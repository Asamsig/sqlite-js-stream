/**
 * LTX header frame marshalling.
 */

import { HeaderSize, Magic, Version } from './constants.ts';
import {
  ChecksumFlag,
  isValidHeaderFlags,
  isValidPageSize,
} from './constants.ts';
import { assertFull, readU32BE, readU64BE, writeU32BE, writeU64BE } from './bytes.ts';
import type { Header } from './types.ts';
import { isSnapshot } from './types.ts';
import { lockPgno } from './constants.ts';

/**
 * MarshalBinary encodes a header to its 100-byte representation.
 */
export function marshalHeader(h: Header): Uint8Array {
  const b = new Uint8Array(HeaderSize);
  for (let i = 0; i < 4; i++) b[i] = Magic.charCodeAt(i);
  writeU32BE(b, 4, h.flags);
  writeU32BE(b, 8, h.pageSize);
  writeU32BE(b, 12, h.commit);
  writeU64BE(b, 16, h.minTXID);
  writeU64BE(b, 24, h.maxTXID);
  writeU64BE(b, 32, h.timestamp);
  writeU64BE(b, 40, h.preApplyChecksum);
  writeU64BE(b, 48, h.walOffset);
  writeU64BE(b, 56, h.walSize);
  writeU32BE(b, 64, h.walSalt1);
  writeU32BE(b, 68, h.walSalt2);
  writeU64BE(b, 72, h.nodeID);
  return b;
}

/**
 * UnmarshalBinary decodes a header from a byte slice.
 * Sets `version` from the magic and returns the header.
 */
export function unmarshalHeader(b: Uint8Array): Header {
  assertFull(b, 0, HeaderSize);
  const magic = String.fromCharCode(b[0], b[1], b[2], b[3]);
  if (magic !== Magic) {
    throw new Error('invalid LTX file');
  }
  return {
    version: Version,
    flags: readU32BE(b, 4),
    pageSize: readU32BE(b, 8),
    commit: readU32BE(b, 12),
    minTXID: readU64BE(b, 16),
    maxTXID: readU64BE(b, 24),
    timestamp: readU64BE(b, 32),
    preApplyChecksum: readU64BE(b, 40),
    walOffset: readU64BE(b, 48),
    walSize: readU64BE(b, 56),
    walSalt1: readU32BE(b, 64),
    walSalt2: readU32BE(b, 68),
    nodeID: readU64BE(b, 72),
  };
}

function noChecksum(flags: number): boolean {
  return (flags & 0x00000002) !== 0;
}

/** Validate returns an error (thrown) if h is invalid. */
export function validateHeader(h: Header): void {
  if (h.version !== Version) {
    throw new Error('invalid version');
  }
  if (!isValidHeaderFlags(h.flags)) {
    throw new Error(`invalid flags: 0x${h.flags.toString(16)}`);
  }
  if (!isValidPageSize(h.pageSize)) {
    throw new Error(`invalid page size: ${h.pageSize}`);
  }
  if (h.minTXID === 0n) {
    throw new Error('minimum transaction id required');
  }
  if (h.maxTXID === 0n) {
    throw new Error('maximum transaction id required');
  }
  if (h.minTXID > h.maxTXID) {
    throw new Error(`transaction ids out of order: (${h.minTXID},${h.maxTXID})`);
  }

  if (h.walOffset < 0n) {
    throw new Error(`wal offset cannot be negative: ${h.walOffset}`);
  }
  if (h.walSize < 0n) {
    throw new Error(`wal size cannot be negative: ${h.walSize}`);
  }

  if ((h.walSalt1 !== 0 || h.walSalt2 !== 0) && h.walOffset === 0n) {
    throw new Error('wal offset required if salt exists');
  }
  if (h.walOffset === 0n && h.walSize !== 0n) {
    throw new Error('wal offset required if wal size exists');
  }

  const snap = isSnapshot(h);
  const nc = noChecksum(h.flags);
  if (snap) {
    if (h.preApplyChecksum !== 0n) {
      throw new Error('pre-apply checksum must be zero on snapshots');
    }
  } else if (nc) {
    if (h.preApplyChecksum !== 0n) {
      throw new Error('pre-apply checksum not allowed');
    }
  } else {
    if (h.preApplyChecksum === 0n) {
      throw new Error('pre-apply checksum required on non-snapshot files');
    }
    if ((h.preApplyChecksum & ChecksumFlag) === 0n) {
      throw new Error('invalid pre-apply checksum format');
    }
  }
}

export function headerLockPgno(h: Header): number {
  return lockPgno(h.pageSize);
}
