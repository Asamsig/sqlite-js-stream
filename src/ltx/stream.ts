/**
 * Streaming LTX reader.
 *
 * The in-memory {@link Decoder} is convenient but requires the whole LTX file in
 * a single buffer. For large databases we read files from a streaming source (a
 * `Readable`) and decode page-by-page, so peak memory stays bounded by one page
 * buffer plus the small page index, never by the full database size.
 *
 * {@link StreamLtxReader} mirrors the synchronous `Decoder` in shape: read the
 * header, then read each page into a reusable buffer, then `verify()` the page
 * index + trailer (including the rolling checksum) once the end-of-pages marker
 * is reached. It is used by streaming restore and compact.
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
import { parsePageIndex } from './page-index.ts';
import { unmarshalHeader, validateHeader } from './header.ts';
import { unmarshalPageHeader } from './page.ts';
import { unmarshalTrailer } from './trailer.ts';
import { decompressBlockInto } from './lz4.ts';
import { checksumPage } from './checksum.ts';
import type { Header, PageHeader, Trailer } from './types.ts';
import { isSnapshot } from './types.ts';

const stateHeader = 'header';
const statePage = 'page';
const stateClose = 'close';
const stateClosed = 'closed';

function noChecksum(h: Header): boolean {
  return (h.flags & 0x00000002) !== 0;
}

/**
 * Pulls bytes from an async iterable of chunks into an exact-bytes reader.
 * Handles chunk boundaries that split logical records (page headers, LZ4
 * blocks, varints) without buffering more than one chunk at a time.
 */
export class AsyncByteReader {
  private it: AsyncIterator<Uint8Array>;
  private buf: Uint8Array = new Uint8Array(0);
  private bufPos = 0;
  pos = 0;

  constructor(source: AsyncIterable<Uint8Array>) {
    this.it = source[Symbol.asyncIterator]();
  }

  /** Fills the internal buffer from the source. Returns false on EOF. */
  private async fill(): Promise<boolean> {
    if (this.bufPos < this.buf.length) return true;
    const next = await this.it.next();
    if (next.done) return false;
    const v = next.value;
    this.buf = v instanceof Uint8Array ? v : new Uint8Array(v);
    this.bufPos = 0;
    return true;
  }

  /** Reads exactly `n` bytes, advancing the position. Throws on short input. */
  async readFull(n: number): Promise<Uint8Array> {
    const out = new Uint8Array(n);
    let o = 0;
    while (o < n) {
      if (this.bufPos >= this.buf.length) {
        if (!(await this.fill())) throw new Error('unexpected end of input');
      }
      const take = Math.min(n - o, this.buf.length - this.bufPos);
      out.set(this.buf.subarray(this.bufPos, this.bufPos + take), o);
      this.bufPos += take;
      o += take;
    }
    this.pos += n;
    return out;
  }

  /** Reads and returns every remaining byte in the source. */
  async readAllRemaining(): Promise<Uint8Array> {
    const parts: Uint8Array[] = [];
    let total = 0;
    if (this.bufPos < this.buf.length) {
      const rest = this.buf.subarray(this.bufPos);
      parts.push(rest);
      total += rest.length;
    }
    for (;;) {
      const next = await this.it.next();
      if (next.done) break;
      const v = next.value;
      const c = v instanceof Uint8Array ? v : new Uint8Array(v);
      parts.push(c);
      total += c.length;
    }
    this.pos += total;
    const out = new Uint8Array(total);
    let o = 0;
    for (const p of parts) {
      out.set(p, o);
      o += p.length;
    }
    return out;
  }
}

/**
 * Streams an LTX file from an async iterable of byte chunks, decoding the
 * header and each page without holding the whole file in memory.
 */
export class StreamLtxReader {
  private r: AsyncByteReader;
  private header!: Header;
  private trailer: Trailer = { postApplyChecksum: 0n, fileChecksum: 0n };
  private state = stateHeader;
  private hash = new Crc64();
  private pageSeq = new Crc64();
  private chksum = 0n;
  private pageN = 0;

  constructor(source: AsyncIterable<Uint8Array>) {
    this.r = new AsyncByteReader(source);
  }

  /** Header returns the decoded header (after `readHeader`). */
  Header(): Header {
    return this.header;
  }

  /** Trailer returns the trailer (valid after `verify`). */
  Trailer(): Trailer {
    return this.trailer;
  }

  /** PageN returns the number of pages decoded. */
  PageN(): number {
    return this.pageN;
  }

  /** Reads and validates the LTX header frame. */
  async readHeader(): Promise<void> {
    if (this.state === stateClosed) throw new Error('ltx reader closed');
    if (this.state !== stateHeader) {
      throw new Error(`cannot read header, expected ${this.state}`);
    }
    const b = await this.r.readFull(HeaderSize);
    this.header = unmarshalHeader(b);
    this.hash.update(b);
    this.state = statePage;
    validateHeader(this.header);
    if (!noChecksum(this.header)) this.chksum = ChecksumFlag;
  }

  /**
   * Reads the next page frame into `data`. Returns the page header, or null
   * when the end-of-pages marker is reached.
   */
  async readPage(data: Uint8Array): Promise<PageHeader | null> {
    if (this.state === stateClosed) throw new Error('ltx reader closed');
    if (this.state === stateClose) return null;
    if (this.state !== statePage) {
      throw new Error(`cannot read page header, expected ${this.state}`);
    }
    if (data.length !== this.header.pageSize) {
      throw new Error(`invalid page buffer size: ${data.length}, expecting ${this.header.pageSize}`);
    }

    const b = await this.r.readFull(PageHeaderSize);
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
      const sizeBuf = await this.r.readFull(4);
      this.hash.update(sizeBuf);
      const dataSize = (sizeBuf[0] << 24 | sizeBuf[1] << 16 | sizeBuf[2] << 8 | sizeBuf[3]) >>> 0;
      const compressed = await this.r.readFull(dataSize);
      const n = decompressBlockInto(compressed, data);
      if (n !== this.header.pageSize) {
        throw new Error(`lz4 block decompressed to ${n} bytes, expected ${this.header.pageSize}`);
      }
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

  /** Reads the page index + trailer and verifies the file checksum. */
  async verify(): Promise<void> {
    if (this.state === stateClosed) return;
    if (this.state !== stateClose) {
      throw new Error(`cannot verify, expected ${this.state}`);
    }

    const remaining = await this.r.readAllRemaining();
    const br = new ByteReader(remaining);

    let n = 0;
    let nextOffset = HeaderSize;
    const seq = new Crc64();
    parsePageIndex(
      br,
      (buf) => this.hash.update(buf),
      (pgno, offset, size) => {
        if (pgno > this.header.commit) {
          throw new Error(`page index pgno ${pgno} exceeds commit ${this.header.commit}`);
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
        this.hashPgo(seq, pgno);
      },
    );

    if (n !== this.pageN) {
      throw new Error(`page index has ${n} entries but ${this.pageN} pages were decoded`);
    }
    if (seq.sum64() !== this.pageSeq.sum64()) {
      throw new Error('page index does not match the decoded page numbers');
    }

    const t = br.readFull(TrailerSize);
    this.hash.update(t.subarray(0, TrailerChecksumOffset));
    this.trailer = unmarshalTrailer(t);
    if (br.remaining > 0) {
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
