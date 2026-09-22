/**
 * Streaming LTX Compactor.
 *
 * The in-memory {@link Compactor} requires each input LTX file as a single
 * `Uint8Array`, which defeats bounded-memory compaction of large databases.
 * This compactor instead merges {@link StreamLtxReader}s (which read pages
 * incrementally) into a single output stream, so peak memory stays bounded by
 * one page buffer per input plus the (small) page index.
 *
 * Semantics match Go's `ltx.Compactor`: later files override earlier files for
 * the same page number, and the output is written with `HeaderFlagNoChecksum`.
 */

import {
  HeaderFlagNoChecksum,
} from './constants.ts';
import type { ByteSink } from './sink.ts';
import { Encoder } from './encoder.ts';
import type { StreamLtxReader } from './stream.ts';
import type { Header, PageHeader, Trailer } from './types.ts';
import { isContiguous } from './types.ts';

interface Input {
  reader: StreamLtxReader;
  data: Uint8Array;
  hdr: PageHeader | null;
  ended: boolean;
}

/** StreamingCompactor merges LTX readers (page-by-page) into one writer. */
export class StreamingCompactor {
  private enc: Encoder;
  private inputs: Input[];
  private _headerFlags: number;

  constructor(sink: ByteSink, readers: StreamLtxReader[], headerFlags: number = HeaderFlagNoChecksum) {
    this.enc = new Encoder(sink);
    this._headerFlags = headerFlags;
    this.inputs = readers.map((reader) => ({
      reader,
      data: new Uint8Array(0),
      hdr: null,
      ended: false,
    }));
  }

  /** Header returns the output header (valid after `compact`). */
  Header(): Header {
    return this.enc.Header();
  }

  /** Trailer returns the output trailer (valid after `compact`). */
  Trailer(): Trailer {
    return this.enc.Trailer();
  }

  /** Compact merges all input readers into a single LTX stream. */
  async compact(): Promise<void> {
    if (this.inputs.length === 0) {
      throw new Error('at least one input reader required');
    }

    // Read headers from all inputs.
    for (const input of this.inputs) {
      await input.reader.readHeader();
    }

    // Validate page sizes & contiguous TXIDs.
    for (let i = 1; i < this.inputs.length; i++) {
      const prevHdr = this.inputs[i - 1].reader.Header();
      const hdr = this.inputs[i].reader.Header();
      if (prevHdr.pageSize !== hdr.pageSize) {
        throw new Error(
          `input files have mismatched page sizes: ${prevHdr.pageSize} != ${hdr.pageSize}`,
        );
      }
      if (!isContiguous(prevHdr.maxTXID, hdr.minTXID, hdr.maxTXID)) {
        throw new Error(
          `non-contiguous transaction ids in input files: (${prevHdr.minTXID},${prevHdr.maxTXID}) -> (${hdr.minTXID},${hdr.maxTXID})`,
        );
      }
    }

    const minHdr = this.inputs[0].reader.Header();
    const maxHdr = this.inputs[this.inputs.length - 1].reader.Header();
    const pageSize = minHdr.pageSize;

    for (const input of this.inputs) {
      input.data = new Uint8Array(pageSize);
    }

    // Propagate the WAL position of the last (newest) input so that a later
    // sync can resume reading the WAL from where the merged range ended,
    // even after the level-0 sources have been pruned.
    this.enc.encodeHeader({
      version: 3,
      flags: this._headerFlags,
      pageSize,
      commit: maxHdr.commit,
      minTXID: minHdr.minTXID,
      maxTXID: maxHdr.maxTXID,
      timestamp: maxHdr.timestamp,
      preApplyChecksum: minHdr.preApplyChecksum,
      walOffset: maxHdr.walOffset,
      walSize: maxHdr.walSize,
      walSalt1: maxHdr.walSalt1,
      walSalt2: maxHdr.walSalt2,
      nodeID: maxHdr.nodeID,
    });

    await this.writePageBlock();

    // Verify every input's index/trailer (catches a corrupt source) and
    // propagate the final post-apply checksum.
    const last = this.inputs[this.inputs.length - 1];
    for (const input of this.inputs) {
      await input.reader.verify();
    }
    this.enc.setPostApplyChecksum(last.reader.Trailer().postApplyChecksum);
    this.enc.close();
  }

  private async writePageBlock(): Promise<void> {
    for (;;) {
      const pgno = await this.fillPageBuffers();
      if (pgno === 0) break;
      await this.writePageBuffer(pgno);
    }
  }

  /** Fills buffers as needed and returns the smallest available page number. */
  private async fillPageBuffers(): Promise<number> {
    let pgno = 0;
    for (const input of this.inputs) {
      if (input.hdr === null && !input.ended) {
        input.hdr = await input.reader.readPage(input.data);
        if (input.hdr === null) input.ended = true;
      }
      if (input.hdr !== null && (pgno === 0 || input.hdr.pgno < pgno)) {
        pgno = input.hdr.pgno;
      }
    }
    return pgno;
  }

  /** Writes the page with matching pgno from the latest input (highest index). */
  private async writePageBuffer(pgno: number): Promise<void> {
    let emitIndex = -1;
    for (let i = 0; i < this.inputs.length; i++) {
      const hdr = this.inputs[i].hdr;
      if (hdr !== null && hdr.pgno === pgno) emitIndex = i;
    }
    if (emitIndex < 0) {
      throw new Error(`internal: no input holds page ${pgno}`);
    }

    // Emit from the latest matching input, then consume every matching input
    // so a page at the same pgno is only written once.
    for (let i = 0; i < this.inputs.length; i++) {
      const input = this.inputs[i];
      if (input.hdr === null || input.hdr.pgno !== pgno) continue;
      if (i === emitIndex) {
        this.enc.encodePage(input.hdr, input.data);
      }
      input.hdr = null;
    }
  }
}
