/**
 * LTX trailer frame marshalling.
 */

import {
  ChecksumFlag,
  TrailerChecksumOffset,
  TrailerSize,
} from './constants.ts';
import { assertFull, readU64BE, writeU64BE } from './bytes.ts';
import type { Header, Trailer } from './types.ts';
import { LTXFormatError } from '../errors.ts';

/** MarshalBinary encodes a Trailer to its 16-byte representation. */
export function marshalTrailer(t: Trailer): Uint8Array {
  const b = new Uint8Array(TrailerSize);
  writeU64BE(b, 0, t.postApplyChecksum);
  writeU64BE(b, 8, t.fileChecksum);
  return b;
}

/** UnmarshalBinary decodes a Trailer from a byte slice. */
export function unmarshalTrailer(b: Uint8Array): Trailer {
  assertFull(b, 0, TrailerSize);
  return {
    postApplyChecksum: readU64BE(b, 0),
    fileChecksum: readU64BE(b, 8),
  };
}

function noChecksum(h: Header): boolean {
  return (h.flags & 0x00000002) !== 0;
}

/** Validate returns an error (thrown) if t is invalid for header h. */
export function validateTrailer(t: Trailer, h: Header): void {
  if (noChecksum(h)) {
    if (t.postApplyChecksum !== 0n) {
      throw new LTXFormatError('post-apply checksum not allowed');
    }
  } else {
    if (t.postApplyChecksum === 0n) {
      throw new LTXFormatError('post-apply checksum required');
    }
    if ((t.postApplyChecksum & ChecksumFlag) === 0n) {
      throw new LTXFormatError('invalid post-apply checksum format');
    }
  }

  if (t.fileChecksum === 0n) {
    throw new LTXFormatError('file checksum required');
  }
  if ((t.fileChecksum & ChecksumFlag) === 0n) {
    throw new LTXFormatError('invalid file checksum format');
  }
}

export const TRAILER_CHECKSUM_OFFSET = TrailerChecksumOffset;
