/**
 * LTX encoder.
 *
 * Mirrors Go's `ltx.Encoder`. The encoder streams header, page frames, page
 * index, and trailer to a `ByteSink`, maintaining a running CRC-64 ISO file
 * checksum over all logical content (header, page headers, compressed-size
 * prefixes, *decompressed* page data, page index bytes, and the trailer's
 * post-apply checksum) plus a byte counter for page-index offsets.
 */

import {
  ChecksumFlag,
  PageHeaderFlagSize,
  PageHeaderSize,
  TrailerChecksumOffset,
  TrailerSize,
  lockPgno,
} from './constants.ts';
import { Crc64 } from './crc64.ts';
import { u32BE, u64BE } from './bytes.ts';
import type { ByteSink } from './sink.ts';
import type { Header, PageHeader, Trailer } from './types.ts';
import { marshalHeader, validateHeader } from './header.ts';
import { marshalPageHeader, validatePageHeader } from './page.ts';
import { marshalTrailer, validateTrailer } from './trailer.ts';
import { encodePageIndexRecord, encodeUvarint } from './page-index.ts';
import { compressBlock } from './lz4.ts';
import { LTXFormatError } from '../errors.ts';

const stateHeader = 'header';
const statePage = 'page';
const stateClosed = 'closed';

interface IndexEntry {
  pgno: number;
  offset: number;
  size: number;
}

/** Encoder writes an LTX file to a ByteSink. */
export class Encoder {
  private sink: ByteSink;
  private state: string = stateHeader;
  private header!: Header;
  private trailer: Trailer = { postApplyChecksum: 0n, fileChecksum: 0n };
  private hash = new Crc64();
  private index: IndexEntry[] = [];
  private n = 0;
  private prevPgno = 0;
  private pagesWritten = 0;

  constructor(sink: ByteSink) {
    this.sink = sink;
  }

  /** Returns the number of bytes written to the sink so far. */
  N(): number {
    return this.n;
  }

  /** Returns a shallow copy of the header. */
  Header(): Header {
    return this.header;
  }

  /** Returns the trailer (file checksum valid after Close()). */
  Trailer(): Trailer {
    return this.trailer;
  }

  /** Returns the replication position after applying this file. */
  PostApplyPos() {
    return {
      txID: this.header.maxTXID,
      postApplyChecksum: this.trailer.postApplyChecksum,
    };
  }

  /** SetPostApplyChecksum sets the database checksum to record in the trailer. */
  setPostApplyChecksum(chk: bigint): void {
    this.trailer.postApplyChecksum = chk;
  }

  /** EncodeHeader writes the header frame and initializes the checksum. */
  encodeHeader(h: Header): void {
    if (this.state === stateClosed) throw new LTXFormatError('ltx encoder closed');
    if (this.state !== stateHeader) {
      throw new LTXFormatError(`cannot encode header frame, expected ${this.state}`);
    }
    validateHeader(h);
    this.header = h;
    this.hash = new Crc64();
    this.write(marshalHeader(h));
    this.state = statePage;
  }

  /**
   * EncodePage writes a single page frame. `data` must be exactly the page
   * size and `hdr.pgno` must be in ascending order and not the lock page.
   */
  encodePage(hdr: PageHeader, data: Uint8Array): void {
    if (this.state === stateClosed) throw new LTXFormatError('ltx encoder closed');
    if (this.state !== statePage) {
      throw new LTXFormatError(`cannot encode page header, expected ${this.state}`);
    }
    if (hdr.pgno > this.header.commit) {
      throw new LTXFormatError(
        `page number ${hdr.pgno} out-of-bounds for commit size ${this.header.commit}`,
      );
    }
    validatePageHeader(hdr);
    if (data.length !== this.header.pageSize) {
      throw new LTXFormatError(
        `invalid page buffer size: ${data.length}, expecting ${this.header.pageSize}`,
      );
    }

    const lockPg = lockPgno(this.header.pageSize);
    if (hdr.pgno === lockPg) {
      throw new LTXFormatError(`cannot encode lock page: pgno=${hdr.pgno}`);
    }

    // Enforce page ordering.
    if (this.header.minTXID === 1n) {
      // Snapshot files must include all pages up to commit, in order.
      if (this.prevPgno === 0 && hdr.pgno !== 1) {
        throw new LTXFormatError('snapshot transaction file must start with page number 1');
      }
      if (this.prevPgno === lockPg - 1) {
        if (hdr.pgno !== this.prevPgno + 2) {
          throw new LTXFormatError(
            `nonsequential page numbers in snapshot transaction (skip lock page): ${this.prevPgno},${hdr.pgno}`,
          );
        }
      } else if (this.prevPgno !== 0 && hdr.pgno !== this.prevPgno + 1) {
        throw new LTXFormatError(
          `nonsequential page numbers in snapshot transaction: ${this.prevPgno},${hdr.pgno}`,
        );
      }
    } else {
      if (this.prevPgno >= hdr.pgno) {
        throw new LTXFormatError(`out-of-order page numbers: ${this.prevPgno},${hdr.pgno}`);
      }
    }

    const offset = this.n;
    const compressed = compressBlock(data);

    const fhdr: PageHeader = { pgno: hdr.pgno, flags: hdr.flags | PageHeaderFlagSize };
    this.write(marshalPageHeader(fhdr));
    this.write(u32BE(compressed.length));
    this.sink.write(compressed);
    this.hash.update(data); // hash the raw (uncompressed) page data
    this.n += compressed.length;

    this.pagesWritten++;
    this.prevPgno = hdr.pgno;
    this.index.push({ pgno: hdr.pgno, offset, size: this.n - offset });
  }

  /** Close writes the end-of-pages marker, page index, and trailer. */
  close(): void {
    if (this.state === stateClosed) return;
    if (this.state !== statePage) {
      throw new LTXFormatError(`cannot close, expected ${this.state}`);
    }

    // Empty page header terminates the page block.
    const zero = new Uint8Array(PageHeaderSize);
    this.write(zero);

    // Write the page index.
    const start = this.n;
    for (const e of this.index) {
      this.write(encodePageIndexRecord(e.pgno, e.offset, e.size));
    }
    this.write(encodeUvarint(0n)); // end marker
    this.write(u64BE(BigInt(this.n - start))); // size of index varint bytes

    // Marshal trailer (post-apply checksum set, file checksum pending).
    const t = marshalTrailer(this.trailer);
    this.hash.update(t.subarray(0, TrailerChecksumOffset));
    this.n += TrailerChecksumOffset;
    this.trailer.fileChecksum = ChecksumFlag | this.hash.sum64();
    validateTrailer(this.trailer, this.header);

    // Remarshal with the file checksum and write to the sink (not hashed).
    this.sink.write(marshalTrailer(this.trailer));
    this.n += TrailerSize - TrailerChecksumOffset;

    this.state = stateClosed;
  }

  /** Writes to the sink, feeding the checksum and byte counter. */
  private write(b: Uint8Array): void {
    this.sink.write(b);
    this.hash.update(b);
    this.n += b.length;
  }
}
