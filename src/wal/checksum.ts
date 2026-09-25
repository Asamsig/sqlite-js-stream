/**
 * SQLite WAL checksum.
 *
 * Matches litestream's `WALChecksum` and SQLite's wal-checksum algorithm.
 * The byte order is chosen from the WAL magic: 0x377f0682 => little-endian,
 * 0x377f0683 => big-endian. The checksum runs over 8-byte units:
 *   s0 += u32[a] + s1 ; s1 += u32[a+4] + s0
 */

import { readU32BE, readU32LE } from '../ltx/bytes.ts';
import { WALFormatError } from '../errors.ts';

export type ByteOrder = 'le' | 'be';

/** Reads a u32 from `buf` at `off` using the given byte order. */
export function readWalU32(bo: ByteOrder, buf: Uint8Array, off: number): number {
  if (bo === 'le') return readU32LE(buf, off);
  return readU32BE(buf, off);
}

/**
 * walChecksum computes a running SQLite WAL checksum over a byte slice whose
 * length is a multiple of 8. Returns the updated (s0, s1) pair.
 */
export function walChecksum(
  bo: ByteOrder,
  s0: number,
  s1: number,
  b: Uint8Array,
): [number, number] {
  if (b.length % 8 !== 0) {
    throw new WALFormatError('misaligned checksum byte slice');
  }
  for (let i = 0; i < b.length; i += 8) {
    s0 = (s0 + readWalU32(bo, b, i) + s1) >>> 0;
    s1 = (s1 + readWalU32(bo, b, i + 4) + s0) >>> 0;
  }
  return [s0, s1];
}
