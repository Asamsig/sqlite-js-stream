/**
 * LTX page header marshalling.
 */

import { PageHeaderSize, PageHeaderFlagSize } from './constants.ts';
import { assertFull, readU16BE, readU32BE, writeU16BE, writeU32BE } from './bytes.ts';
import type { PageHeader } from './types.ts';

/** MarshalBinary encodes a PageHeader to its 6-byte representation. */
export function marshalPageHeader(h: PageHeader): Uint8Array {
  const b = new Uint8Array(PageHeaderSize);
  writeU32BE(b, 0, h.pgno);
  writeU16BE(b, 4, h.flags);
  return b;
}

/** UnmarshalBinary decodes a PageHeader from a byte slice. */
export function unmarshalPageHeader(b: Uint8Array): PageHeader {
  assertFull(b, 0, PageHeaderSize);
  return {
    pgno: readU32BE(b, 0),
    flags: readU16BE(b, 4),
  };
}

/** Validate returns an error (thrown) if h is invalid. */
export function validatePageHeader(h: PageHeader): void {
  if (h.pgno === 0) {
    throw new Error('page number required');
  }
  if ((h.flags & ~PageHeaderFlagSize) !== 0) {
    throw new Error(`invalid page header flags: 0x${h.flags.toString(16)}`);
  }
}
