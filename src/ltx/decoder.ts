/**
 * LTX decoder.
 *
 * Mirrors Go's `ltx.Decoder`. Reads header, page frames, page index, and
 * trailer from an in-memory buffer, verifying the file checksum and (for
 * checksum-tracking snapshots) the post-apply rolling checksum.
 */

import {
  ChecksumFlag,
  HeaderSize,
  PageHeaderFlagSize,
  PageHeaderSize,
  TrailerChecksumOffset,
  TrailerSize,
  lockPgno,
} from './constants.ts';
import { Crc64 } from './crc64.ts';
import { u32BE } from './bytes.ts';
import { ByteReader } from './byte-reader.ts';
import type { ByteSink } from './sink.ts';
import type { Header, PageHeader, PageIndex, Trailer } from './types.ts';
import { isSnapshot } from './types.ts';
import { unmarshalHeader, validateHeader } from './header.ts';
import { unmarshalPageHeader } from './page.ts';
import { unmarshalTrailer } from './trailer.ts';
import { parsePageIndex } from './page-index.ts';
import { decompressBlock } from './lz4.ts';
import { checksumPage } from './checksum.ts';

const stateHeader = 'header';
const statePage = 'page';
const stateClose = 'close';
const stateClosed = 'closed';

function noChecksum(h: Header): boolean {
  return (h.flags & 0x00000002) !== 0;
}

/** Decoder reads an LTX file from an in-memory buffer. */
export class Decoder {
  private r: ByteReader;
  private header!: Header;
  private trailer: Trailer = { postApplyChecksum: 0n, fileChecksum: 0n };
  private pageIndex: PageIndex | null = null;
  private state = stateHeader;
  private hash = new Crc64();
  private pageSeq = new Crc64();
  private chksum = 0n;
  private pageN = 0;
  private retainPageIndex: boolean;

  constructor(buf: Uint8Array) {
    this.r = new ByteReader(buf);
    this.retainPageIndex = true;
  }

  /** SetRetainPageIndex controls whether Close materializes the page-index map. */
  setRetainPageIndex(retain: boolean): void {
    this.retainPageIndex = retain;
  }

  /** N returns the number of source bytes read. */
  N(): number {
    return this.r.pos;
  }

  /** PageN returns the number of pages decoded. */
  PageN(): number {
    return this.pageN;
  }

  /** Header returns a copy of the header. */
  Header(): Header {
    return this.header;
  }

  /** Trailer returns the trailer. File checksum valid after Close(). */
  Trailer(): Trailer {
    return this.trailer;
  }

  /** PageIndex returns the page index (after Close), or null if not retained. */
  PageIndex(): PageIndex | null {
    return this.pageIndex;
  }

  /** PostApplyPos returns the replication position after applying this file. */
  PostApplyPos() {
    return {
      txID: this.header.maxTXID,
      postApplyChecksum: this.trailer.postApplyChecksum,
    };
  }

  /** DecodeHeader reads and validates the header frame. */
  decodeHeader(): void {
    const b = this.r.readFull(HeaderSize);
    this.header = unmarshalHeader(b);
    this.hash.update(b);
    this.state = statePage;
    validateHeader(this.header);
    if (!noChecksum(this.header)) {
      this.chksum = ChecksumFlag;
    }
  }

  /**
   * DecodePage reads the next page frame into `data`. Returns the page header,
   * or null when the end-of-pages marker is reached.
   */
  decodePage(data: Uint8Array): PageHeader | null {
    if (this.state === stateClosed) throw new Error('ltx decoder closed');
    if (this.state === stateClose) return null;
    if (this.state !== statePage) {
      throw new Error(`cannot read page header, expected ${this.state}`);
    }
    if (data.length !== this.header.pageSize) {
      throw new Error(`invalid page buffer size: ${data.length}, expecting ${this.header.pageSize}`);
    }

    const b = this.r.readFull(PageHeaderSize);
    const hdr = unmarshalPageHeader(b);
    this.hash.update(b);

    // An empty page header marks the end of the page block.
    if (hdr.pgno === 0 && hdr.flags === 0) {
      this.state = stateClose;
      return null;
    }

    if (hdr.pgno > this.header.commit) {
      throw new Error(`page number ${hdr.pgno} out-of-bounds for commit size ${this.header.commit}`);
    }

    if (hdr.flags & PageHeaderFlagSize) {
      // New block format: read size prefix, then LZ4 block data.
      const sizeBuf = this.r.readFull(4);
      this.hash.update(sizeBuf);
      const dataSize = (sizeBuf[0] << 24 | sizeBuf[1] << 16 | sizeBuf[2] << 8 | sizeBuf[3]) >>> 0;
      const compressed = this.r.readFull(dataSize);
      const decompressed = decompressBlock(compressed, this.header.pageSize);
      data.set(decompressed);
    } else {
      throw new Error('legacy LZ4 frame format not supported');
    }

    this.hash.update(data);
    this.hashPgo(this.pageSeq, hdr.pgno);
    this.pageN++;

    // Track the rolling DB checksum for checksum-tracking snapshots.
    if (isSnapshot(this.header) && !noChecksum(this.header)) {
      if (hdr.pgno !== lockPgno(this.header.pageSize)) {
        this.chksum = ChecksumFlag | (this.chksum ^ checksumPage(hdr.pgno, data));
      }
    }

    return hdr;
  }

  /** Verify reads the entire file, discarding page data (for validation). */
  verify(): void {
    this.decodeHeader();
    const data = new Uint8Array(this.header.pageSize);
    for (;;) {
      if (this.decodePage(data) === null) break;
    }
    this.close();
  }

  /** DecodeDatabaseTo writes the snapshot as a SQLite database to `sink`. */
  decodeDatabaseTo(sink: ByteSink): void {
    this.decodeHeader();
    const hdr = this.header;
    if (!isSnapshot(hdr)) {
      throw new Error('cannot decode non-snapshot LTX file to SQLite database');
    }
    const lockPg = lockPgno(hdr.pageSize);
    const data = new Uint8Array(hdr.pageSize);
    for (let pgno = 1; pgno <= hdr.commit; pgno++) {
      if (pgno === lockPg) {
        sink.write(new Uint8Array(hdr.pageSize));
        continue;
      }
      const ph = this.decodePage(data);
      if (ph === null) {
        throw new Error(`unexpected end of pages at pgno ${pgno}`);
      }
      if (ph.pgno !== pgno) {
        throw new Error(`unexpected pgno while decoding page: read ${ph.pgno}, expected ${pgno}`);
      }
      sink.write(data);
    }

    // Expect one final read to be the end-of-pages marker.
    if (this.decodePage(data) !== null) {
      throw new Error('unexpected page after end of database');
    }
    this.close();
  }

  /** Close verifies the page index and trailer, then checks the file checksum. */
  close(): void {
    if (this.state === stateClosed) return;
    if (this.state !== stateClose) {
      throw new Error(`cannot close, expected ${this.state}`);
    }

    const index: PageIndex = new Map();
    let n = 0;
    let nextOffset = HeaderSize;
    const seq = new Crc64();
    const self = this;

    parsePageIndex(
      this.r,
      (buf) => self.hash.update(buf),
      (pgno, offset, size) => {
        if (pgno > self.header.commit) {
          throw new Error(`page index pgno ${pgno} exceeds commit ${self.header.commit}`);
        }
        if (offset < nextOffset) {
          throw new Error(
            `page index pgno ${pgno} offset ${offset} overlaps previous frame ending at ${nextOffset}`,
          );
        }
        if (size <= PageHeaderSize) {
          throw new Error(`page index pgno ${pgno} has invalid frame size ${size}`);
        }
        nextOffset = offset + size;
        n++;
        self.hashPgo(seq, pgno);
        if (index) {
          index.set(pgno, {
            level: 0,
            minTXID: self.header.minTXID,
            maxTXID: self.header.maxTXID,
            offset,
            size,
          });
        }
      },
    );

    if (n !== this.pageN) {
      throw new Error(`page index has ${n} entries but ${this.pageN} pages were decoded`);
    }
    if (seq.sum64() !== this.pageSeq.sum64()) {
      throw new Error('page index does not match the decoded page numbers');
    }
    this.pageIndex = this.retainPageIndex ? index : null;

    // Read the trailer; everything except the file checksum field is hashed.
    const t = this.r.readFull(TrailerSize);
    this.hash.update(t.subarray(0, TrailerChecksumOffset));
    this.trailer = unmarshalTrailer(t);
    if (this.r.remaining > 0) {
      throw new Error('unexpected data after trailer');
    }

    const computed = ChecksumFlag | this.hash.sum64();
    if (computed !== this.trailer.fileChecksum) {
      throw new Error('file checksum mismatch');
    }

    if (isSnapshot(this.header) && !noChecksum(this.header)) {
      if (this.trailer.postApplyChecksum !== this.chksum) {
        throw new Error(
          `post-apply checksum in trailer (${this.trailer.postApplyChecksum.toString(16)}) does not match calculated checksum (${this.chksum.toString(16)})`,
        );
      }
    }

    this.state = stateClosed;
  }

  private hashPgo(h: Crc64, pgno: number): void {
    h.update(u32BE(pgno));
  }
}
