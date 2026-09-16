/**
 * CRC-64 ISO implementation, byte-compatible with Go's `hash/crc64` using
 * `crc64.MakeTable(crc64.ISO)`. This is what superfly/ltx uses for all LTX
 * checksums (page checksums, rolling DB checksum, file checksum).
 *
 * NOTE: LTX uses CRC-64 ISO, NOT ECMA. The litestream docs are misleading;
 * the actual code (`crc64.New(crc64.MakeTable(crc64.ISO))`) uses ISO.
 *
 * Algorithm matches Go's reflected `crc64.Update`:
 *   - init / xorout = all ones (0xFFFFFFFFFFFFFFFF)
 *   - polynomial register value = 0xD800000000000000
 *   - table built by right-shifting (reflected), update as
 *     `tab[byte(crc) ^ v] ^ (crc >> 8)`
 */

const ISO_POLY = 0xd800000000000000n;
const MASK64 = 0xffffffffffffffffn;

/** Builds the 256-entry CRC table for a reflected polynomial. */
function makeTable(poly: bigint): bigint[] {
  const table: bigint[] = new Array(256);
  for (let i = 0; i < 256; i++) {
    let crc = BigInt(i);
    for (let j = 0; j < 8; j++) {
      crc = (crc & 1n) !== 0n ? (crc >> 1n) ^ poly : crc >> 1n;
    }
    table[i] = crc;
  }
  return table;
}

const ISO_TABLE = makeTable(ISO_POLY);

/**
 * Crc64 is a streaming CRC-64 hasher, equivalent to Go's `hash/crc64`
 * (ISO table). Call `update()` with successive byte chunks and `sum64()` to
 * obtain the running checksum. `reset()` re-initializes the hasher.
 */
export class Crc64 {
  private crc: bigint = MASK64;

  update(buf: Uint8Array): void {
    let crc = this.crc;
    for (let i = 0; i < buf.length; i++) {
      crc = ISO_TABLE[Number((crc ^ BigInt(buf[i])) & 0xffn)] ^ (crc >> 8n);
    }
    this.crc = crc;
  }

  reset(): void {
    this.crc = MASK64;
  }

  /** Returns the digest of all data written so far. */
  sum64(): bigint {
    return this.crc ^ MASK64;
  }
}

/** Convenience: computes the CRC-64 ISO of a single buffer. */
export function crc64ISO(buf: Uint8Array): bigint {
  const h = new Crc64();
  h.update(buf);
  return h.sum64();
}
