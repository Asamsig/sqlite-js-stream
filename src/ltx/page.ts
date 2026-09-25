/**
 * LTX page header marshalling.
 */

import { PageHeaderSize, PageHeaderFlagSize, MaxPageSize } from './constants.ts';
import { assertFull, readU16BE, readU32BE, writeU16BE, writeU32BE } from './bytes.ts';
import { decompressBlockInto } from './lz4.ts';
import type { PageHeader } from './types.ts';
import { LTXFormatError } from '../errors.ts';

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
    throw new LTXFormatError('page number required');
  }
  if ((h.flags & ~PageHeaderFlagSize) !== 0) {
    throw new LTXFormatError(`invalid page header flags: 0x${h.flags.toString(16)}`);
  }
}

/** The decoded result of a single LTX page frame. */
export interface DecodedPage {
  header: PageHeader;
  data: Uint8Array;
}

/**
 * DecodePageData decodes a single page frame from `b` (which must start at a
 * page header). Matches Go's `ltx.DecodePageData`. For an empty (zero) page
 * header, `data` is an empty buffer.
 *
 * Note: the uncompressed byte length is unknown here; LTX pages are at most
 * `MaxPageSize`, so we decompress into a 64KiB buffer and slice to the bytes
 * written. Callers that know the page size should use `DecodePage` in the
 * decoder for the size-accurate path.
 */
export function decodePageData(b: Uint8Array): DecodedPage {
  const header = unmarshalPageHeader(b);
  if (header.pgno === 0 && header.flags === 0) {
    return { header, data: new Uint8Array(0) };
  }
  validatePageHeader(header);

  if ((header.flags & PageHeaderFlagSize) !== 0) {
    if (b.length < PageHeaderSize + 4) {
      throw new LTXFormatError('buffer too small for size prefix');
    }
    const dataSize = readU32BE(b, PageHeaderSize);
    const offset = PageHeaderSize + 4;
    if (b.length < offset + dataSize) {
      throw new LTXFormatError(`buffer too small for data: need ${offset + dataSize}, have ${b.length}`);
    }
    const compressed = b.subarray(offset, offset + dataSize);
    const buf = new Uint8Array(MaxPageSize);
    const n = decompressBlockInto(compressed, buf);
    if (n <= 0) {
      throw new LTXFormatError('decompress block failed');
    }
    return { header, data: buf.subarray(0, n) };
  }

  throw new LTXFormatError('legacy LZ4 frame format not supported');
}
