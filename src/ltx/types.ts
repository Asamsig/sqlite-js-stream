/**
 * LTX shared types and small value helpers.
 *
 * 64-bit unsigned values (TXID, Checksum) are represented as `bigint` to keep
 * exact semantics. Page numbers and file offsets/sizes fit within JS safe
 * integers for any realistic database, so they are `number`.
 */

export type TXID = bigint;
export type Checksum = bigint;

/** Pos is the transactional position of a database. */
export interface Pos {
  txID: TXID;
  postApplyChecksum: Checksum;
}

/** Formats a TXID as a fixed-width 16-char lowercase hex string. */
export function formatTXID(txID: bigint): string {
  return txID.toString(16).padStart(16, '0');
}

/** Parses a 16-character hex string into a TXID. */
export function parseTXID(s: string): bigint {
  if (s.length !== 16) {
    throw new Error(`invalid formatted transaction id length: ${JSON.stringify(s)}`);
  }
  if (!/^[0-9a-fA-F]{16}$/.test(s)) {
    throw new Error(`invalid transaction id format: ${JSON.stringify(s)}`);
  }
  return BigInt(`0x${s}`);
}

/** Formats a Checksum as a fixed-width 16-char lowercase hex string. */
export function formatChecksum(chk: bigint): string {
  return chk.toString(16).padStart(16, '0');
}

/** Parses a 16-character hex string into a Checksum. */
export function parseChecksum(s: string): bigint {
  if (s.length !== 16) {
    throw new Error(`invalid formatted checksum length: ${JSON.stringify(s)}`);
  }
  if (!/^[0-9a-fA-F]{16}$/.test(s)) {
    throw new Error(`invalid checksum format: ${JSON.stringify(s)}`);
  }
  return BigInt(`0x${s}`);
}

export function parsePos(s: string): Pos {
  if (s.length !== 33) {
    throw new Error(`invalid formatted position length: ${JSON.stringify(s)}`);
  }
  return {
    txID: parseTXID(s.slice(0, 16)),
    postApplyChecksum: parseChecksum(s.slice(17)),
  };
}

export function formatPos(p: Pos): string {
  return `${formatTXID(p.txID)}/${formatChecksum(p.postApplyChecksum)}`;
}

export function isZeroPos(p: Pos): boolean {
  return p.txID === 0n && p.postApplyChecksum === 0n;
}

/** Header flags. */
export interface Header {
  version: number;
  flags: number;
  pageSize: number;
  commit: number;
  minTXID: TXID;
  maxTXID: TXID;
  timestamp: bigint; // milliseconds since Unix epoch
  preApplyChecksum: Checksum;
  walOffset: bigint;
  walSize: bigint;
  walSalt1: number;
  walSalt2: number;
  nodeID: bigint;
}

export function isSnapshot(h: Header): boolean {
  return h.minTXID === 1n;
}

export function noChecksum(h: Header): boolean {
  return (h.flags & HeaderFlagNoChecksumBit) !== 0;
}

const HeaderFlagNoChecksumBit = 0x00000002;

/** PageHeader represents the header for a single page in an LTX file. */
export interface PageHeader {
  pgno: number; // uint32
  flags: number; // uint16
}

export function isZeroPageHeader(h: PageHeader): boolean {
  return h.pgno === 0 && h.flags === 0;
}

/** Trailer represents the ending frame of an LTX file. */
export interface Trailer {
  postApplyChecksum: Checksum;
  fileChecksum: Checksum;
}

/** PageIndexElem describes a single page entry in the file's page index. */
export interface PageIndexElem {
  level: number;
  minTXID: TXID;
  maxTXID: TXID;
  offset: number;
  size: number;
}

export type PageIndex = Map<number, PageIndexElem>;

/** FileInfo represents file information about an LTX file. */
export interface FileInfo {
  level: number;
  minTXID: TXID;
  maxTXID: TXID;
  preApplyChecksum?: Checksum;
  postApplyChecksum?: Checksum;
  size: number;
  createdAt: number; // ms since Unix epoch
  checksum?: Checksum;
}

export function preApplyPos(info: FileInfo): Pos {
  return {
    txID: info.minTXID - 1n,
    postApplyChecksum: info.preApplyChecksum ?? 0n,
  };
}

export function postApplyPos(info: FileInfo): Pos {
  return {
    txID: info.maxTXID,
    postApplyChecksum: info.postApplyChecksum ?? 0n,
  };
}

/** IsContiguous reports whether a transaction range is contiguous with prevMaxTXID. */
export function isContiguous(prevMaxTXID: TXID, minTXID: TXID, maxTXID: TXID): boolean {
  return minTXID <= prevMaxTXID + 1n && maxTXID > prevMaxTXID;
}
