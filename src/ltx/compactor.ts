/**
 * LTX Compactor merges multiple LTX files (readers) into a single stream,
 * where later files override earlier ones for the same page number.
 *
 * Mirrors Go's `ltx.Compactor`. Used by restore (snapshot + incremental L0
 * files) and by multi-level compaction.
 */

import type { Header, PageHeader, Trailer } from './types.ts';
import { isZeroPageHeader, isContiguous } from './types.ts';
import type { ByteSink } from './sink.ts';
import { Encoder } from './encoder.ts';
import { Decoder } from './decoder.ts';

interface CompactorInput {
  dec: Decoder;
  hdr: PageHeader;
  data: Uint8Array;
}

/** Compactor merges LTX readers into a single writer. */
export class Compactor {
  private enc: Encoder;
  private inputs: CompactorInput[] = [];
  private _headerFlags = 0;
  private allowNonContiguousTXIDs = false;

  constructor(sink: ByteSink, readers: Uint8Array[]) {
    this.enc = new Encoder(sink);
    for (const r of readers) {
      const dec = new Decoder(r);
      dec.setRetainPageIndex(false);
      this.inputs.push({ dec, hdr: { pgno: 0, flags: 0 }, data: new Uint8Array(0) });
    }
  }

  get headerFlags(): number {
    return this._headerFlags;
  }

  set headerFlags(flags: number) {
    this._headerFlags = flags;
  }

  /** AllowNonContiguousTXIDs disables contiguous-TXID validation between inputs. */
  setAllowNonContiguousTXIDs(v: boolean): void {
    this.allowNonContiguousTXIDs = v;
  }

  /** Header returns the output header (after Compact). */
  Header(): Header {
    return this.enc.Header();
  }

  /** Trailer returns the output trailer (after Compact). */
  Trailer(): Trailer {
    return this.enc.Trailer();
  }

  /** Compact merges all input readers into a single LTX stream. */
  compact(): void {
    if (this.inputs.length === 0) {
      throw new Error('at least one input reader required');
    }

    // Read headers from all inputs.
    for (let i = 0; i < this.inputs.length; i++) {
      this.inputs[i].dec.decodeHeader();
    }

    // Validate page sizes & contiguous TXIDs.
    for (let i = 1; i < this.inputs.length; i++) {
      const prevHdr = this.inputs[i - 1].dec.Header();
      const hdr = this.inputs[i].dec.Header();
      if (prevHdr.pageSize !== hdr.pageSize) {
        throw new Error(
          `input files have mismatched page sizes: ${prevHdr.pageSize} != ${hdr.pageSize}`,
        );
      }
      if (
        !this.allowNonContiguousTXIDs &&
        !isContiguous(prevHdr.maxTXID, hdr.minTXID, hdr.maxTXID)
      ) {
        throw new Error(
          `non-contiguous transaction ids in input files: (${prevHdr.minTXID},${prevHdr.maxTXID}) -> (${hdr.minTXID},${hdr.maxTXID})`,
        );
      }
    }

    const minHdr = this.inputs[0].dec.Header();
    const maxHdr = this.inputs[this.inputs.length - 1].dec.Header();

    this.enc.encodeHeader({
      version: 3,
      flags: this.headerFlags,
      pageSize: minHdr.pageSize,
      commit: maxHdr.commit,
      minTXID: minHdr.minTXID,
      maxTXID: maxHdr.maxTXID,
      timestamp: maxHdr.timestamp,
      preApplyChecksum: minHdr.preApplyChecksum,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    });

    this.writePageBlock();

    for (const input of this.inputs) {
      input.dec.close();
    }

    const last = this.inputs[this.inputs.length - 1];
    this.enc.setPostApplyChecksum(last.dec.Trailer().postApplyChecksum);
    this.enc.close();
  }

  private writePageBlock(): void {
    const pageSize = this.enc.Header().pageSize;
    for (const input of this.inputs) {
      input.data = new Uint8Array(pageSize);
    }
    for (;;) {
      const pgno = this.fillPageBuffers();
      if (pgno === 0) break;
      this.writePageBuffer(pgno);
    }
  }

  /** Fills input buffers and returns the smallest available page number. */
  private fillPageBuffers(): number {
    let pgno = 0;
    for (const input of this.inputs) {
      if (isZeroPageHeader(input.hdr)) {
        const hdr = input.dec.decodePage(input.data);
        if (hdr === null) continue; // end of page block for this input
        input.hdr = hdr;
      }
      if (pgno === 0 || input.hdr.pgno < pgno) {
        pgno = input.hdr.pgno;
      }
    }
    return pgno;
  }

  /** Writes the buffer with a matching pgno from the latest input. */
  private writePageBuffer(pgno: number): void {
    const commit = this.enc.Header().commit;
    let pageWritten = false;
    for (let i = this.inputs.length - 1; i >= 0; i--) {
      const input = this.inputs[i];
      if (input.hdr.pgno !== pgno) continue;

      const hdr = input.hdr;
      const data = input.data;
      input.hdr = { pgno: 0, flags: 0 };

      if (pageWritten) continue;
      if (pgno > commit) continue;
      pageWritten = true;
      this.enc.encodePage(hdr, data);
    }
  }
}
