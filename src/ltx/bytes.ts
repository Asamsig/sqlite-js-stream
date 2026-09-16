/**
 * Big-endian byte helpers for LTX marshalling.
 */

export function writeU8(buf: Uint8Array, off: number, v: number): void {
  buf[off] = v & 0xff;
}

export function writeU16BE(buf: Uint8Array, off: number, v: number): void {
  buf[off] = (v >>> 8) & 0xff;
  buf[off + 1] = v & 0xff;
}

export function writeU32BE(buf: Uint8Array, off: number, v: number): void {
  buf[off] = (v >>> 24) & 0xff;
  buf[off + 1] = (v >>> 16) & 0xff;
  buf[off + 2] = (v >>> 8) & 0xff;
  buf[off + 3] = v & 0xff;
}

export function writeU64BE(buf: Uint8Array, off: number, v: bigint): void {
  let n = v;
  for (let i = 0; i < 8; i++) {
    buf[off + (7 - i)] = Number(n & 0xffn);
    n >>= 8n;
  }
}

export function readU16BE(buf: Uint8Array, off: number): number {
  return (buf[off] << 8) | buf[off + 1];
}

export function readU32BE(buf: Uint8Array, off: number): number {
  return (
    ((buf[off] | 0) << 24) >>> 0 |
    (buf[off + 1] << 16) |
    (buf[off + 2] << 8) |
    buf[off + 3]
  ) >>> 0;
}

export function readU32LE(buf: Uint8Array, off: number): number {
  return (
    buf[off] |
    (buf[off + 1] << 8) |
    (buf[off + 2] << 16) |
    ((buf[off + 3] | 0) << 24)
  ) >>> 0;
}

export function readU64BE(buf: Uint8Array, off: number): bigint {
  let n = 0n;
  for (let i = 0; i < 8; i++) {
    n = (n << 8n) | BigInt(buf[off + i]);
  }
  return n;
}

/** Converts a number to a big-endian 4-byte array. */
export function u32BE(v: number): Uint8Array {
  const b = new Uint8Array(4);
  writeU32BE(b, 0, v);
  return b;
}

/** Converts a number to a big-endian 8-byte array. */
export function u64BE(v: bigint): Uint8Array {
  const b = new Uint8Array(8);
  writeU64BE(b, 0, v);
  return b;
}

/** Converts a number to a big-endian 2-byte array. */
export function u16BE(v: number): Uint8Array {
  const b = new Uint8Array(2);
  writeU16BE(b, 0, v);
  return b;
}

/** Throws unless exactly `n` bytes are available to read. */
export function assertFull(buf: Uint8Array, off: number, n: number): void {
  if (off + n > buf.length) {
    throw new RangeError(
      `short buffer: need ${n} bytes at offset ${off}, have ${buf.length - off}`,
    );
  }
}
