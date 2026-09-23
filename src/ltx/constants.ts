/**
 * LTX format constants.
 *
 * Ground truth: superfly/ltx v0.5.2 (vendored by litestream). The byte layout
 * here must match Go's `ltx` package exactly so files interoperate.
 */

/** Magic is the first 4 bytes of every LTX file. */
export const Magic = 'LTX1';

/** Version is the current version of the LTX file format. */
export const Version = 3;

/** Size constants. */
export const HeaderSize = 100;
export const PageHeaderSize = 6;
export const TrailerSize = 16;

/** Checksum size & positions. */
export const ChecksumSize = 8;
export const TrailerChecksumOffset = TrailerSize - ChecksumSize;

/** ChecksumFlag ensures a checksum is non-zero. */
export const ChecksumFlag = 1n << 63n;

/** Entry byte offset of the PENDING_BYTE within a database. */
export const PENDING_BYTE = 0x40000000;

/** Header flags. */
export const HeaderFlagMask = 0x00000002;
export const HeaderFlagNoChecksum = 0x00000002;

/** Page header flags. */
/** PageHeaderFlagSize indicates a 4-byte size field follows the page header. */
export const PageHeaderFlagSize = 0x0001;

/** MaxPageSize is the maximum allowed SQLite page size. */
export const MaxPageSize = 65536;

/** SnapshotLevel is the LTX level reserved for full-database snapshots. */
export const SnapshotLevel = 9;

/** Error messages / codes. */
export const ErrInvalidFile = 'invalid LTX file';
export const ErrDecoderClosed = 'ltx decoder closed';
export const ErrEncoderClosed = 'ltx encoder closed';
export const ErrEncoderAborted = 'ltx encoder aborted';
export const ErrNoChecksum = 'no file checksum';
export const ErrInvalidChecksumFormat = 'invalid file checksum format';
export const ErrChecksumMismatch = 'file checksum mismatch';

/**
 * LockPgno returns the page number at which the PENDING_BYTE exists.
 * This page is never part of the LTX payload.
 */
export function lockPgno(pageSize: number): number {
  return Math.floor(PENDING_BYTE / pageSize) + 1;
}

/** IsValidPageSize returns true if sz is between 512 and 64K and a power of two. */
export function isValidPageSize(sz: number): boolean {
  for (let i = 512; i <= MaxPageSize; i *= 2) {
    if (sz === i) return true;
  }
  return false;
}

/** IsValidHeaderFlags returns true unless flags outside the valid mask are set. */
export function isValidHeaderFlags(flags: number): boolean {
  return flags === (flags & HeaderFlagMask);
}
