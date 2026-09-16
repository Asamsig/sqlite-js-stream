/**
 * LTX checksum helpers: per-page checksums and the rolling DB checksum.
 */

import { ChecksumFlag, lockPgno } from './constants.ts';
import { Crc64 } from './crc64.ts';
import { u32BE } from './bytes.ts';

/**
 * ChecksumPage returns a CRC-64 checksum that combines the page number with
 * the page data: `ChecksumFlag | CRC64-ISO(pgno_be4 ++ data)`.
 */
export function checksumPage(pgno: number, data: Uint8Array): bigint {
  const h = new Crc64();
  h.update(u32BE(pgno));
  h.update(data);
  return ChecksumFlag | h.sum64();
}

export function checksumPageWithHasher(h: Crc64, pgno: number, data: Uint8Array): bigint {
  h.reset();
  h.update(u32BE(pgno));
  h.update(data);
  return ChecksumFlag | h.sum64();
}

/**
 * ChecksumReader computes the rolling DB checksum over a byte buffer split
 * into pages of `pageSize`. The "lock page" is skipped entirely (its checksum
 * contributes nothing), matching Go's `ChecksumReader`.
 *
 * For large databases this should later read page-by-page from a file rather
 * than buffering the whole DB; provided as a buffer for Milestone 1.
 */
export function checksumReader(data: Uint8Array, pageSize: number): bigint {
  const lockPg = lockPgno(pageSize);
  let chksum = 0n;
  let offset = 0;
  for (let pgno = 1; offset < data.length; pgno++, offset += pageSize) {
    const page = data.subarray(offset, offset + pageSize);
    if (pgno !== lockPg) {
      chksum = ChecksumFlag | (chksum ^ checksumPage(pgno, page));
    }
  }
  return chksum;
}
