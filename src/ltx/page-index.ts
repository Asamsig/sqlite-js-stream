/**
 * LTX page index encoding/decoding.
 *
 * The page index is a sequence of varint triples `(pgno, offset, size)` in
 * ascending page order, terminated by a zero varint, followed by an 8-byte
 * big-endian size of the varint bytes (excluding the size field itself).
 */

import { readU64BE } from './bytes.ts';
import type { ByteReader } from './byte-reader.ts';
import type { PageIndex, TXID } from './types.ts';
import { LTXFormatError } from '../errors.ts';

const MAX_INT64 = 0x7fffffffffffffffn;

/** Encodes a u64 as a little-endian LEB128 varint (same as Go's uvarint). */
export function encodeUvarint(v: bigint): Uint8Array {
  const out: number[] = [];
  for (;;) {
    const b = Number(v & 0x7fn);
    v >>= 7n;
    if (v !== 0n) {
      out.push(b | 0x80);
    } else {
      out.push(b);
      break;
    }
  }
  return Uint8Array.from(out);
}

/** Decodes a u64 varint from r, advancing its position. */
export function readUvarint(r: ByteReader): bigint {
  let x = 0n;
  let s = 0n;
  for (let i = 0; i < 10; i++) {
    const b = r.readByte();
    if (b < 0x80) {
      if (i === 9 && b > 1) {
        throw new LTXFormatError('uvarint overflows 64 bits');
      }
      return x | (BigInt(b) << s);
    }
    x |= BigInt(b & 0x7f) << s;
    s += 7n;
  }
  throw new LTXFormatError('uvarint overflows 64 bits');
}

/**
 * Parses the page index section from r. observe, if non-null, receives every
 * byte consumed (for checksumming); fn receives each record in file order.
 */
export function parsePageIndex(
  r: ByteReader,
  observe: ((buf: Uint8Array) => void) | null,
  fn: (pgno: number, offset: number, size: number) => void,
): void {
  let consumed = 0;
  let prevPgno = 0;

  for (;;) {
    const start = r.pos;
    const pgno = readUvarint(r);
    if (pgno === 0n) {
      // End marker.
      observe?.(r.slice(start, r.pos));
      consumed += r.pos - start;
      break;
    }
    if (pgno > 0xffffffffn) {
      throw new LTXFormatError(`page index pgno ${pgno} out of range`);
    }
    if (pgno <= BigInt(prevPgno)) {
      throw new LTXFormatError(`page index out of order: ${pgno} after ${prevPgno}`);
    }
    const offset = readUvarint(r);
    const size = readUvarint(r);
    if (offset > MAX_INT64 || size > MAX_INT64) {
      throw new LTXFormatError(`page index pgno ${pgno} offset/size out of range`);
    }
    observe?.(r.slice(start, r.pos));
    consumed += r.pos - start;
    prevPgno = Number(pgno);
    fn(Number(pgno), Number(offset), Number(size));
  }

  // Read trailing size field.
  const sizeBuf = r.readFull(8);
  observe?.(sizeBuf);
  const indexSize = readU64BE(sizeBuf, 0);
  if (indexSize !== BigInt(consumed)) {
    throw new LTXFormatError(`page index size mismatch: field=${indexSize} read=${consumed}`);
  }
}

/**
 * DecodePageIndex decodes the page index from r, starting at the current
 * position, into a Map keyed by page number.
 */
export function decodePageIndex(
  r: ByteReader,
  level: number,
  minTXID: TXID,
  maxTXID: TXID,
): PageIndex {
  const index: PageIndex = new Map();
  parsePageIndex(
    r,
    null,
    (pgno, offset, size) => {
      index.set(pgno, {
        level,
        minTXID,
        maxTXID,
        offset,
        size,
      });
    },
  );
  return index;
}

/** Encodes a single page index record `(pgno, offset, size)` as a varint buffer. */
export function encodePageIndexRecord(pgno: number, offset: number, size: number): Uint8Array {
  const parts = [
    encodeUvarint(BigInt(pgno)),
    encodeUvarint(BigInt(offset)),
    encodeUvarint(BigInt(size)),
  ];
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}
