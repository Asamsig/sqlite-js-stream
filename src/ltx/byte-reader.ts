/**
 * A sequential byte reader over an in-memory buffer. Used by the LTX decoder
 * to walk the page block, page index, and trailer while tracking position.
 */

import { assertFull } from './bytes.ts';

export class ByteReader {
  private buf: Uint8Array;
  pos: number;

  constructor(buf: Uint8Array, start = 0) {
    this.buf = buf;
    this.pos = start;
  }

  get length(): number {
    return this.buf.length;
  }

  get remaining(): number {
    return this.buf.length - this.pos;
  }

  /** Reads one byte, advancing the position. Throws on EOF. */
  readByte(): number {
    if (this.pos >= this.buf.length) {
      throw new Error('unexpected end of input');
    }
    return this.buf[this.pos++];
  }

  /** Reads exactly `n` bytes, advancing the position. Throws on short input. */
  readFull(n: number): Uint8Array {
    assertFull(this.buf, this.pos, n);
    const out = this.buf.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }

  /** Returns a view of the bytes in [start, end) without moving the position. */
  slice(start: number, end: number): Uint8Array {
    return this.buf.subarray(start, end);
  }
}
