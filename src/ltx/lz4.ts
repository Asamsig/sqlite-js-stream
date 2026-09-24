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

const LZ4_FRAME_MAGIC = 0x184d2204;

/** Block max sizes indexed by the LZ4 frame block-size field. */
const FRAME_BLOCK_SIZES: Record<number, number> = {
  4: 0x10000,
  5: 0x40000,
  6: 0x100000,
  7: 0x400000,
};

/** Reads a little-endian u32. */
function readU32LE(d: Uint8Array, off: number): number {
  return d[off] | (d[off + 1] << 8) | (d[off + 2] << 16) | (d[off + 3] << 24);
}

/**
 * compressFrameLz4 wraps `data` in an LZ4 **frame** (v0.3.x snapshot/WAL
 * format). Frame format is distinct from the LTX block codec used above.
 */
export function compressFrameLz4(data: Uint8Array): Uint8Array {
  const dst = new Uint8Array(data.length + 4096 + Math.floor(data.length / 255));
  const n = lz4.compressFrame(data, dst);
  return dst.slice(0, n);
}

/**
 * decompressFrameLz4 decodes an LZ4 **frame** into its raw bytes. Handles
 * compressed and uncompressed blocks, and both content-size and unknown-size
 * headers. Pure JS (lz4js), so it is interoperable with Go's LZ4 frame writer.
 */
export function decompressFrameLz4(data: Uint8Array): Uint8Array {
  if (data.length < 7 || readU32LE(data, 0) !== LZ4_FRAME_MAGIC) {
    throw new Error('invalid LZ4 frame: bad magic');
  }

  let sIndex = 4;
  const flg = data[sIndex++];
  const bd = data[sIndex++];
  const useBlockSum = (flg & 0x10) !== 0;
  const useContentSize = (flg & 0x08) !== 0;
  if (useContentSize) sIndex += 8;
  sIndex++; // header checksum

  const bsIdx = (bd >> 4) & 0x07;
  const blockMax = FRAME_BLOCK_SIZES[bsIdx] ?? 0x10000;
  const blockBuf = new Uint8Array(blockMax);
  const parts: Uint8Array[] = [];

  while (sIndex + 4 <= data.length) {
    let compSize = readU32LE(data, sIndex);
    sIndex += 4;
    if (compSize === 0) break; // end mark
    if (useBlockSum) sIndex += 4;

    if ((compSize & 0x80000000) !== 0) {
      // Uncompressed block.
      compSize &= ~0x80000000;
      parts.push(data.slice(sIndex, sIndex + compSize));
      sIndex += compSize;
    } else {
      const n = lz4.decompressBlock(data, blockBuf, sIndex, compSize, 0);
      if (n < 0) throw new Error('LZ4 block decompression failed');
      parts.push(blockBuf.slice(0, n));
      sIndex += compSize;
    }
  }

  // Content checksum (4 bytes) may trail the end mark; ignore it.
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
