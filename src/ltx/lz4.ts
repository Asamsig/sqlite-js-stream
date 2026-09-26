/**
 * LZ4 block-codec adapter.
 *
 * The LTX format uses LZ4 **block** format (not frame format), matching Go's
 * `github.com/pierrec/lz4/v4` `CompressBlock`/`UncompressBlock`.
 *
 * `compressBlock` is a direct TypeScript port of `pierrec/lz4/v4`'s internal
 * `lz4block.Compressor.CompressBlock` (the default Fast compressor used by
 * litestream). It is **byte-identical** to Go's output for the same input, so
 * LTX files we write match Go's `superfly/ltx` encoder exactly (validated
 * against the Go fixtures in `test/ltx/go-interop.test.ts` and CI's
 * `interop-go` job).
 *
 * `decompressBlock`/`decompressBlockInto` delegate to the pure-JS `lz4js`
 * implementation, which decodes any conforming LZ4 block (decompression does
 * not need to be byte-identical).
 */

import lz4 from 'lz4js';
import { LTXFormatError } from '../errors.ts';

// Compressor constants, mirrored from `internal/lz4block/block.go`.
const HASH_LOG = 16;
const HT_SIZE = 1 << HASH_LOG; // 65536 hash-table entries.
const WIN_SIZE = 1 << 16; // 64 KiB window.
const WIN_MASK = WIN_SIZE - 1;
const MIN_MATCH = 4;
const MF_LIMIT = 10 + MIN_MATCH; // Last match cannot start within the last 14 bytes.
const ADAPT_SKIP_LOG = 7;
const PRIME6 = 227718039650203n;
const U64_MASK = 0xffffffffffffffffn;
const U32_MASK = 0xffffffffn;

// Hash table state. `inUse` is a bitmap so we can clear the table cheaply.
const hashTable = new Uint16Array(HT_SIZE);
const inUse = new Uint32Array(HT_SIZE / 32);

/**
 * compressBlockBound returns the maximum compressed size for `n` input bytes
 * using the Fast compressor (`n + n/255 + 16`).
 */
export function compressBlockBound(n: number): number {
  return n + ((n / 255) | 0) + 16;
}

/** Hashes the lower 6 bytes of a 64-bit little-endian value into a hash index. */
function blockHash(x: bigint): number {
  return Number((((x << 16n) * PRIME6) & U64_MASK) >> 48n);
}

/** Reads the 64-bit little-endian value at `o` (used by the Fast scan). */
function readU64(b: Uint8Array, o: number): bigint {
  let x = 0n;
  for (let i = 7; i >= 0; i--) x = (x << 8n) | BigInt(b[o + i]);
  return x;
}

/** Reads the 32-bit little-endian value at `o`. */
function readU32(b: Uint8Array, o: number): number {
  return (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;
}

/** Returns the number of leading zero bytes in a 64-bit little-endian value. */
function trailingZeroBytes(x: bigint): number {
  let n = 0;
  while ((x & 0xffn) === 0n && n < 8) {
    x >>= 8n;
    n++;
  }
  return n;
}

/**
 * Look up the two 16-bit halves of a stored match offset for a hash. Mirrors
 * `Compressor.get`: the table stores 16-bit offsets relative to a 64 KiB block
 * boundary. Returns a position that may be before the start of the window (in
 * which case `offset >= WIN_SIZE` short-circuits the candidate check).
 */
function tableGet(h: number, si: number): number {
  h &= HT_SIZE - 1;
  let i = 0;
  if (inUse[h >>> 5] & (1 << (h & 31))) i = hashTable[h];
  i += si & ~WIN_MASK;
  if (i >= si) i -= WIN_SIZE;
  return i;
}

function tablePut(h: number, si: number): void {
  h &= HT_SIZE - 1;
  hashTable[h] = si & 0xffff;
  inUse[h >>> 5] |= 1 << (h & 31);
}

/**
 * Compresses `src` into `dst` as a Fast-mode LZ4 block, writing at offset 0.
 * Mirrors `lz4block.Compressor.CompressBlock`. Returns the number of bytes
 * written, or 0 if `dst` is too small (which happens only when the caller does
 * not size `dst` with `compressBlockBound`).
 */
function compressBlockInto(src: Uint8Array, dst: Uint8Array): number {
  // Clear the reused table so output is deterministic (issue #65).
  inUse.fill(0);

  // In normal use `dst` is sized by `compressBlockBound`, so the compressor
  // always emits a valid block (even for incompressible input). An undersized
  // `dst` is caught by the `di >= dst.length` guards below and returns 0.

  let si = 0;
  let di = 0;
  let anchor = 0;
  const sn = src.length - MF_LIMIT;

  if (sn > 0) {
    for (; si < sn;) {
      const match = readU64(src, si);
      let h = blockHash(match);
      const h2 = blockHash(match >> 8n);
      const ref = tableGet(h, si);
      const ref2 = tableGet(h2, si + 1);
      tablePut(h, si);
      tablePut(h2, si + 1);

      let offset = si - ref;

      if (offset <= 0 || offset >= WIN_SIZE || (match & U32_MASK) !== BigInt(readU32(src, ref))) {
        h = blockHash(match >> 16n);
        const ref3 = tableGet(h, si + 2);
        si += 1;
        offset = si - ref2;
        if (offset <= 0 || offset >= WIN_SIZE || ((match >> 8n) & U32_MASK) !== BigInt(readU32(src, ref2))) {
          si += 1;
          offset = si - ref3;
          tablePut(h, si);
          if (offset <= 0 || offset >= WIN_SIZE || ((match >> 16n) & U32_MASK) !== BigInt(readU32(src, ref3))) {
            si += 2 + ((si - anchor) >>> ADAPT_SKIP_LOG);
            continue;
          }
        }
      }

      // Match found.
      let lLen = si - anchor;
      let mLen = 4;

      // Extend backwards, reducing literals.
      let tOff = si - offset - 1;
      while (lLen > 0 && tOff >= 0 && src[si - 1] === src[tOff]) {
        si--;
        tOff--;
        lLen--;
        mLen++;
      }

      // Continue search at the end of the match. `matchBase` is the position
      // just past the first 4 matched bytes.
      const matchBase = si + MIN_MATCH;
      si = si + mLen;

      // Find the longest match by comparing 8 bytes at a time.
      while (si + 8 <= sn) {
        const x = readU64(src, si) ^ readU64(src, si - offset);
        if (x === 0n) {
          si += 8;
        } else {
          si += trailingZeroBytes(x);
          break;
        }
      }

      mLen = si - matchBase;

      if (di >= dst.length) return 0;
      if (mLen < 0xf) dst[di] = mLen;
      else dst[di] = 0xf;

      // Encode literal length.
      if (lLen < 0xf) {
        dst[di] |= lLen << 4;
      } else {
        dst[di] |= 0xf0;
        di++;
        let l = lLen - 0xf;
        while (l >= 0xff && di < dst.length) {
          dst[di] = 0xff;
          di++;
          l -= 0xff;
        }
        if (di >= dst.length) return 0;
        dst[di] = l;
      }
      di++;

      if (di + lLen > dst.length) return 0;
      dst.set(src.subarray(anchor, anchor + lLen), di);
      di += lLen + 2;
      anchor = si;

      if (di > dst.length) return 0;
      dst[di - 2] = offset & 0xff;
      dst[di - 1] = (offset >> 8) & 0xff;

      // Encode match length part 2.
      if (mLen >= 0xf) {
        let m = mLen - 0xf;
        while (m >= 0xff && di < dst.length) {
          dst[di] = 0xff;
          di++;
          m -= 0xff;
        }
        if (di >= dst.length) return 0;
        dst[di] = m;
        di++;
      }
      if (si >= sn) break;

      // Hash match end-2.
      h = blockHash(readU64(src, si - 2));
      tablePut(h, si - 2);
    }
  }

  // Last literals.
  if (di >= dst.length) return 0;
  const lLen = src.length - anchor;
  if (lLen < 0xf) {
    dst[di] = lLen << 4;
  } else {
    dst[di] = 0xf0;
    di++;
    let l = lLen - 0xf;
    while (l >= 0xff && di < dst.length) {
      dst[di] = 0xff;
      di++;
      l -= 0xff;
    }
    if (di >= dst.length) return 0;
    dst[di] = l;
  }
  di++;

  if (di + (src.length - anchor) > dst.length) return 0;
  dst.set(src.subarray(anchor), di);
  di += src.length - anchor;
  return di;
}

/**
 * compressBlock compresses `data` as a Fast-mode LZ4 block and returns the
 * compressed bytes. The result is byte-identical to Go's `pierrec/lz4/v4`
 * `CompressBlock` for the same input.
 */
export function compressBlock(data: Uint8Array): Uint8Array {
  const dst = new Uint8Array(compressBlockBound(data.length));
  const n = compressBlockInto(data, dst);
  return dst.slice(0, n);
}

/**
 * decompressBlock decompresses an LZ4 block into a buffer of exactly
 * `uncompressedSize` bytes. Throws if the block is invalid or produces a
 * different size.
 */
export function decompressBlock(data: Uint8Array, uncompressedSize: number): Uint8Array {
  const dst = new Uint8Array(uncompressedSize);
  const n = lz4.decompressBlock(data, dst, 0, data.length, 0);
  if (n !== uncompressedSize) {
    throw new LTXFormatError(`lz4 block decompressed to ${n} bytes, expected ${uncompressedSize}`);
  }
  return dst;
}

/**
 * decompressBlockInto decompresses an LZ4 block into `dst` and returns the
 * number of bytes written. Does not require knowing the uncompressed size
 * ahead of time.
 */
export function decompressBlockInto(data: Uint8Array, dst: Uint8Array): number {
  return lz4.decompressBlock(data, dst, 0, data.length, 0);
}
