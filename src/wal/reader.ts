/**
 * WALReader parses a SQLite WAL file and verifies salt & checksum integrity
 * while reading. It does not enforce transaction boundaries; the caller is
 * responsible for handling uncommitted frames.
 *
 * Mirrors litestream's `WALReader` (wal_reader.go).
 */

import { WALFrameHeaderSize, WALHeaderSize, WALMagicBE, WALMagicLE, WALVersion } from './constants.ts';
import type { ByteOrder } from './checksum.ts';
import { walChecksum } from './checksum.ts';
import { readU32BE } from '../ltx/bytes.ts';

/** Raised when the WAL ends, or when a salt/checksum mismatch is detected. */
export class WALEOFError extends Error {}

/** Raised when starting mid-WAL and the previous frame does not match. */
export class PrevFrameMismatchError extends Error {
  readonly cause: Error;
  constructor(cause: Error) {
    super(`prev frame mismatch: ${cause.message}`);
    this.cause = cause;
  }
}

export interface WALFrame {
  pgno: number;
  commit: number;
}

export interface WALPageMap {
  /** Latest committed offset (file offset) for each page. */
  pageIndex: Map<number, number>;
  /** Offset just past the last read frame. */
  maxOffset: number;
  /** Final database size in pages (from the last commit frame). */
  commit: number;
  /** Whether the scan was bounded by maxBytes. */
  limited: boolean;
}

/** WALReader reads and validates frames from an in-memory WAL byte buffer. */
export class WALReader {
  private data: Uint8Array;
  private bo!: ByteOrder;
  private pageSizeVal = 0;
  private seqVal = 0;
  private salt1 = 0;
  private salt2 = 0;
  private chksum1 = 0;
  private chksum2 = 0;
  private frameN = 0;

  constructor(data: Uint8Array) {
    this.data = data;
    this.readHeader();
  }

  /** Creates a reader positioned at `offset`, loading salts from the caller. */
  static withOffset(data: Uint8Array, offset: number, salt1: number, salt2: number): WALReader {
    if (offset <= WALHeaderSize) {
      throw new Error(
        `offset (${offset}) must be greater than the wal header size (${WALHeaderSize})`,
      );
    }
    const r = new WALReader(data);

    // Load the salt from the caller; the beginning of the file may be gone.
    r.salt1 = salt1;
    r.salt2 = salt2;

    // Ensure the offset is positioned on a frame start.
    const frameSize = r.pageSizeVal + WALFrameHeaderSize;
    if ((offset - WALHeaderSize) % frameSize !== 0) {
      throw new Error(`unaligned wal offset ${offset} for page size ${r.pageSizeVal}`);
    }
    r.frameN = Math.floor((offset - WALHeaderSize) / frameSize);

    // Read the previous page to load the checksum state.
    r.frameN--;
    try {
      r.readFrameInternal(new Uint8Array(r.pageSizeVal), false);
    } catch (err) {
      if (err instanceof WALEOFError) {
        throw new PrevFrameMismatchError(err);
      }
      throw err;
    }

    return r;
  }

  /** PageSize returns the page size from the header. */
  pageSize(): number {
    return this.pageSizeVal;
  }

  /** Seq returns the checkpoint sequence from the header. */
  seq(): number {
    return this.seqVal;
  }

  /** Offset returns the file offset of the last read frame (0 if none). */
  offset(): number {
    if (this.frameN === 0) return 0;
    return WALHeaderSize + (this.frameN - 1) * (WALFrameHeaderSize + this.pageSizeVal);
  }

  /** ReadFrame reads the next frame, verifying salt & checksum. */
  readFrame(data: Uint8Array): WALFrame {
    return this.readFrameInternal(data, true);
  }

  /** PageMap scans committed frames and returns the latest page offsets. */
  pageMap(maxBytes = 0): WALPageMap {
    const m = new Map<number, number>();
    const txMap = new Map<number, number>();
    const data = new Uint8Array(this.pageSizeVal);
    const frameSize = WALFrameHeaderSize + this.pageSizeVal;
    const startOffset = WALHeaderSize + this.frameN * frameSize;
    let commit = 0;
    let limited = false;

    for (;;) {
      let frame: WALFrame;
      try {
        frame = this.readFrame(data);
      } catch (err) {
        if (err instanceof WALEOFError) break;
        throw err;
      }

      // Buffer the latest offset for each page in the current transaction.
      const offset = this.offset();
      txMap.set(frame.pgno, offset);

      // On a commit record, flush pending pages into the full map.
      if (frame.commit !== 0) {
        for (const [pgno, off] of txMap) m.set(pgno, off);
        txMap.clear();
        commit = frame.commit;

        if (maxBytes > 0 && this.offset() + frameSize - startOffset >= maxBytes) {
          limited = true;
          break;
        }
      }
    }

    // Drop pages beyond the final commit size (DB may have shrunk via VACUUM).
    for (const pgno of [...m.keys()]) {
      if (pgno > commit) m.delete(pgno);
    }

    if (m.size === 0) {
      return { pageIndex: m, maxOffset: 0, commit: 0, limited };
    }

    // Compute the highest frame offset and extend past its data.
    let end = 0;
    for (const offset of m.values()) {
      if (end === 0 || offset > end) end = offset;
    }
    end += frameSize;

    return { pageIndex: m, maxOffset: end, commit, limited };
  }

  private readHeader(): void {
    if (this.data.length < WALHeaderSize) {
      throw new WALEOFError();
    }
    const hdr = this.data.subarray(0, WALHeaderSize);

    const magic = readU32BE(hdr, 0);
    if (magic === WALMagicLE) this.bo = 'le';
    else if (magic === WALMagicBE) this.bo = 'be';
    else throw new Error(`invalid wal header magic: ${magic.toString(16)}`);

    const chksum1 = readU32BE(hdr, 24);
    const chksum2 = readU32BE(hdr, 28);
    const [v0, v1] = walChecksum(this.bo, 0, 0, hdr.subarray(0, 24));
    if (v0 !== chksum1 || v1 !== chksum2) {
      throw new WALEOFError();
    }

    const version = readU32BE(hdr, 4);
    if (version !== WALVersion) {
      throw new Error(`unsupported wal version: ${version}`);
    }

    this.pageSizeVal = readU32BE(hdr, 8);
    this.seqVal = readU32BE(hdr, 12);
    this.salt1 = readU32BE(hdr, 16);
    this.salt2 = readU32BE(hdr, 20);
    this.chksum1 = chksum1;
    this.chksum2 = chksum2;
  }

  private readFrameInternal(data: Uint8Array, verifyChecksum: boolean): WALFrame {
    if (data.length !== this.pageSizeVal) {
      throw new Error(
        `buffer size (${data.length}) must match page size (${this.pageSizeVal})`,
      );
    }

    const frameSize = this.pageSizeVal + WALFrameHeaderSize;
    const offset = WALHeaderSize + this.frameN * frameSize;
    if (offset + frameSize > this.data.length) {
      throw new WALEOFError();
    }

    const hdr = this.data.subarray(offset, offset + WALFrameHeaderSize);
    const page = this.data.subarray(offset + WALFrameHeaderSize, offset + frameSize);

    // Verify the frame salt matches the header salt.
    const salt1 = readU32BE(hdr, 8);
    const salt2 = readU32BE(hdr, 12);
    if (this.salt1 !== salt1 || this.salt2 !== salt2) {
      throw new WALEOFError();
    }

    const fchksum1 = readU32BE(hdr, 16);
    const fchksum2 = readU32BE(hdr, 20);

    if (verifyChecksum) {
      [this.chksum1, this.chksum2] = walChecksum(
        this.bo,
        this.chksum1,
        this.chksum2,
        hdr.subarray(0, 8),
      );
      [this.chksum1, this.chksum2] = walChecksum(
        this.bo,
        this.chksum1,
        this.chksum2,
        page,
      );
      if (this.chksum1 !== fchksum1 || this.chksum2 !== fchksum2) {
        throw new WALEOFError();
      }
    } else {
      this.chksum1 = fchksum1;
      this.chksum2 = fchksum2;
    }

    const pgno = readU32BE(hdr, 0);
    const commit = readU32BE(hdr, 4);
    this.frameN++;

    return { pgno, commit };
  }
}
