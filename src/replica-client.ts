/**
 * ReplicaClient interface and shared helpers for interacting with a replica
 * (an LTX destination). Mirrors litestream's `replica_client.go`.
 *
 * The interface is async/iterable oriented rather than Go's iterator/readcloser
 * model, but the semantics match:
 *  - ltxFiles returns LTX file metadata for a level, optionally seeking by TXID.
 *  - openLtxFile returns a byte stream for a range of an LTX file.
 *  - writeLtxFile writes an LTX file, preserving its creation timestamp.
 */

import nodePath from 'node:path';
import type { Readable } from 'node:stream';

import type { FileInfo, Header, PageIndex, TXID } from './ltx/index.ts';
import {
  decodePageData,
  decodePageIndex,
  formatFilename,
  HeaderSize,
  parseFilename,
  TrailerSize,
  type DecodedPage,
  unmarshalHeader,
} from './ltx/index.ts';
import { ByteReader } from './ltx/index.ts';
import { LTXFormatError, SqliteJsStreamError } from './errors.ts';
import { readU64BE } from './ltx/index.ts';
import { readAll } from './util/stream.ts';
import type { Logger } from './util/logger.ts';

/** Sentinel error to stop early iteration in FindLTXFiles. */
export const ErrStopIter = Symbol('stop iterator');

import { SnapshotLevel } from './ltx/constants.ts';

/** SnapshotLevel is the LTX level reserved for full snapshots. */
export { SnapshotLevel };

export interface ReplicaClient {
  /** Type returns the type of client (e.g. "file"). */
  type(): string;
  /** Init initializes the replica client connection (idempotent). */
  init(): Promise<void>;
  /**
   * ltxFiles returns all LTX files for a level. If seek is non-zero, only files
   * with MinTXID >= seek are returned. useMetadata controls timestamp accuracy.
   */
  ltxFiles(opts: LTXFilesOptions): AsyncIterable<FileInfo>;
  /**
   * openLtxFile returns a reader over an LTX file range. If size is 0 the range
   * extends to the end of the file. Throws if the file does not exist.
   */
  openLtxFile(opts: OpenLTXFileOptions): Promise<Readable>;
  /** writeLtxFile writes an LTX file to the replica and returns its metadata. */
  writeLtxFile(
    opts: WriteLTXFileOptions,
    body: AsyncIterable<Uint8Array | string>,
  ): Promise<FileInfo>;
  /** deleteLtxFiles deletes one or more LTX files. */
  deleteLtxFiles(infos: FileInfo[]): Promise<void>;
  /** deleteAll deletes all files on the replica. */
  deleteAll(): Promise<void>;
  /** setLogger sets the logger for the client. */
  setLogger(logger: Logger): void;
}

export interface LTXFilesOptions {
  level: number;
  seek: TXID;
  useMetadata: boolean;
}

export interface OpenLTXFileOptions {
  level: number;
  minTXID: TXID;
  maxTXID: TXID;
  offset: number;
  size: number;
}

export interface WriteLTXFileOptions {
  level: number;
  minTXID: TXID;
  maxTXID: TXID;
}

/** LTXDir returns the path to an LTX directory (root/ltx). */
export function ltxDir(root: string): string {
  return nodePath.join(root, 'ltx');
}

/** LTXLevelDir returns the path to an LTX level directory. */
export function ltxLevelDir(root: string, level: number): string {
  return nodePath.join(ltxDir(root), String(level));
}

/** LTXFilePath returns the path to a single LTX file. */
export function ltxFilePath(
  root: string,
  level: number,
  minTXID: TXID,
  maxTXID: TXID,
): string {
  return nodePath.join(ltxLevelDir(root, level), formatFilename(minTXID, maxTXID));
}

/** LTXError wraps errors reading/writing a specific LTX file. */
export class LTXError extends SqliteJsStreamError {
  readonly op: string;
  readonly path: string;
  readonly level: number;
  readonly minTXID: TXID;
  readonly maxTXID: TXID;
  readonly cause: unknown;

  constructor(op: string, path: string, level: number, minTXID: TXID, maxTXID: TXID, err: unknown) {
    super('ERR_LTX', `${op} ltx file: ${err instanceof Error ? err.message : String(err)}`);
    this.name = 'LTXError';
    this.op = op;
    this.path = path;
    this.level = level;
    this.minTXID = minTXID;
    this.maxTXID = maxTXID;
    this.cause = err;
  }
}

const DEFAULT_ESTIMATED_PAGE_INDEX_SIZE = 32 * 1024;

/**
 * FindLTXFiles returns a list of LTX files on a level matching `filter`.
 * The filter may throw ErrStopIter to end iteration early.
 */
export async function findLtxFiles(
  _ctx: unknown,
  client: ReplicaClient,
  level: number,
  useMetadata: boolean,
  filter: (info: FileInfo) => boolean | Promise<boolean>,
): Promise<FileInfo[]> {
  const out: FileInfo[] = [];
  for await (const info of client.ltxFiles({ level, seek: 0n, useMetadata })) {
    const match = await filter(info);
    if (match) out.push(info);
  }
  return out;
}

/** FetchLTXHeader reads & returns the LTX header for the given file info. */
export async function fetchLtxHeader(client: ReplicaClient, info: FileInfo): Promise<Header> {
  const rc = await client.openLtxFile({
    level: info.level,
    minTXID: info.minTXID,
    maxTXID: info.maxTXID,
    offset: 0,
    size: HeaderSize,
  });
  const b = await readAll(rc);
  return unmarshalHeader(b);
}

/** FetchPageIndex decodes the page index from the tail of an LTX file. */
export async function fetchPageIndex(
  client: ReplicaClient,
  info: FileInfo,
): Promise<PageIndex> {
  const b = await fetchPageIndexData(client, info);
  const r = new ByteReader(b, 0);
  return decodePageIndex(r, info.level, info.minTXID, info.maxTXID);
}

/** Streaming fetchTail for the page index; sizes and reopens as needed. */
export async function fetchPageIndexData(client: ReplicaClient, info: FileInfo): Promise<Uint8Array> {
  let offset = info.size - DEFAULT_ESTIMATED_PAGE_INDEX_SIZE;
  if (offset < 0) offset = 0;

  const f = await client.openLtxFile({
    level: info.level,
    minTXID: info.minTXID,
    maxTXID: info.maxTXID,
    offset,
    size: 0,
  });
  const b = await readAll(f);

  if (b.length < TrailerSize + 8) {
    throw new LTXFormatError(`ltx file too short to contain a page index: ${b.length} bytes`);
  }

  const size = readU64BE(b, b.length - TrailerSize - 8);
  const relativeStart = b.length - Number(size) - TrailerSize - 8;
  if (relativeStart > 0) {
    return b.subarray(relativeStart);
  }

  const f2 = await client.openLtxFile({
    level: info.level,
    minTXID: info.minTXID,
    maxTXID: info.maxTXID,
    offset: info.size - TrailerSize - 8 - Number(size),
    size: 0,
  });
  return readAll(f2);
}

/** FetchPage fetches and decodes a single page frame from an LTX file. */
export async function fetchPage(
  client: ReplicaClient,
  level: number,
  minTXID: TXID,
  maxTXID: TXID,
  offset: number,
  size: number,
): Promise<DecodedPage> {
  const f = await client.openLtxFile({ level, minTXID, maxTXID, offset, size });
  const b = await readAll(f);
  return decodePageData(b);
}

// Re-exported for convenience in the file backend.
export { formatFilename, parseFilename };
