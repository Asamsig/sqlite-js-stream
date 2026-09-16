/**
 * LZ4 block-codec adapter.
 *
 * The LTX format uses LZ4 **block** format (not frame format), matching Go's
 * `github.com/pierrec/lz4/v4` `CompressBlock`/`UncompressBlock`. This adapter
 * wraps the pure-JS `lz4js` implementation. For higher throughput a native
 * binding (e.g. `lz4-napi`) can be swapped in behind this same interface
 * later.
 *
 * Robustness note: when a page is not compressible, Go's `CompressBlock`
 * returns 0 and litestream treats it as an error. We instead emit a valid
 * literal-only LZ4 block, which any conforming LZ4 block decoder (including
 * Go's) can read, so replication never fails on incompressible data.
 */

import lz4 from 'lz4js';

const HASH_SIZE = 1 << 16;
const hashTable: number[] = new Array(HASH_SIZE);

/**
 * compressBlock compresses `data` as an LZ4 block and returns the compressed
 * bytes. The result is always a valid LZ4 block.
 */
export function compressBlock(data: Uint8Array): Uint8Array {
  if (data.length === 0) {
    return literalOnlyBlock(data);
  }
  const bound = lz4.compressBound(data.length);
  const dst = new Uint8Array(bound);
  hashTable.fill(0);
  const n = lz4.compressBlock(data, dst, 0, data.length, hashTable);
  if (n === 0) {
    return literalOnlyBlock(data);
  }
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
    throw new Error(`lz4 block decompressed to ${n} bytes, expected ${uncompressedSize}`);
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

/** Encodes a literal-only LZ4 block (no matches). Always valid. */
function literalOnlyBlock(data: Uint8Array): Uint8Array {
  const literals = data.length;
  let lengthBytes = 0;
  if (literals >= 15) {
    lengthBytes = 1 + Math.floor((literals - 15) / 255);
  }
  const out = new Uint8Array(1 + lengthBytes + literals);
  let o = 0;
  if (literals >= 15) {
    out[o++] = 0xf0;
    let rem = literals - 15;
    while (rem >= 255) {
      out[o++] = 0xff;
      rem -= 255;
    }
    out[o++] = rem;
  } else {
    out[o++] = literals << 4;
  }
  out.set(data, o);
  return out;
}
