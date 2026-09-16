/**
 * Replica manages replication of a single SQLite database (identified by its
 * `path`) to a ReplicaClient.
 *
 * It reads the database + WAL directly from disk, writes level-0/level-9 LTX
 * files into a local cache, uploads them to the client, tracks the replicated
 * position, and can restore a database by merging LTX files. Mirrors
 * litestream's `replica.go`, but is agnostic to the SQLite driver the
 * application uses (it never touches a connection).
 */

import { access, mkdir, rename, writeFile } from 'node:fs/promises';
import nodePath from 'node:path';

import {
  BufferSink,
  Compactor,
  Decoder,
  HeaderFlagNoChecksum,
  type FileInfo,
} from './ltx/index.ts';
import { LocalDatabase } from './db.ts';
import type { ReplicaClient } from './replica-client.ts';
import { readAll } from './util/stream.ts';
import type { Logger } from './util/logger.ts';
import { noopLogger } from './util/logger.ts';

export interface RestoreOptions {
  outputPath: string;
  txID?: bigint;
  timestamp?: Date;
}

export interface SyncStatus {
  localTXID: bigint;
  remoteTXID: bigint;
  inSync: boolean;
}

function chunks(data: Uint8Array, size = 256 * 1024): AsyncIterable<Uint8Array> {
  return (async function* () {
    for (let i = 0; i < data.length; i += size) {
      yield data.subarray(i, i + size);
    }
  })();
}

/** Replica replicates a single database (by path) to a ReplicaClient. */
export class Replica {
  readonly path: string;
  readonly client: ReplicaClient;
  private logger: Logger;
  private posValue: bigint | null = null;
  private db: LocalDatabase | null = null;

  constructor(path: string, client: ReplicaClient, logger: Logger = noopLogger) {
    this.path = path;
    this.client = client;
    this.logger = logger;
  }

  /** Lazily opens the local database reader (cached). */
  private async localDb(): Promise<LocalDatabase> {
    if (this.db === null) {
      this.db = await LocalDatabase.open(this.path);
    }
    return this.db;
  }

  /** Pos returns the current replicated position (max level-0 maxTXID). */
  async pos(): Promise<bigint> {
    if (this.posValue === null) {
      this.posValue = await this.calcPos();
    }
    return this.posValue;
  }

  /** CalcPos finds the maximum TXID on the replica for level 0. */
  async calcPos(): Promise<bigint> {
    let max = 0n;
    for await (const info of this.client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
      if (info.maxTXID > max) max = info.maxTXID;
    }
    return max;
  }

  /** LocalPos returns the local cached position (max local level-0 maxTXID). */
  async localPos(): Promise<bigint> {
    return (await this.localDb()).pos();
  }

  /**
   * Sync builds a level-0 LTX file locally (if there are new WAL changes) and
   * uploads any pending local files to the client. Returns the file that was
   * written locally, or null if there was nothing new to do.
   */
  async sync(): Promise<FileInfo | null> {
    await this.client.init();
    const db = await this.localDb();
    const info = await db.sync();
    const dbPos = await db.pos();
    const remotePos = await this.pos();

    for (let txID = remotePos + 1n; txID <= dbPos; txID = (await this.pos()) + 1n) {
      await this.uploadLtxFile(0, txID, txID);
      this.posValue = txID;
    }
    return info;
  }

  /** SyncAndWait uploads all pending LTX files; harmless if already synced. */
  async syncAndWait(): Promise<void> {
    await this.sync();
  }

  /** Snapshot writes and uploads a level-9 full-database LTX file. */
  async snapshot(): Promise<void> {
    const info = await (await this.localDb()).snapshot();
    if (info) {
      await this.uploadLtxFile(9, info.minTXID, info.maxTXID);
    }
  }

  /** SyncStatus reports the local and remote positions. */
  async syncStatus(): Promise<SyncStatus> {
    const localTXID = await this.localPos();
    const remoteTXID = await this.calcPos();
    return { localTXID, remoteTXID, inSync: localTXID === remoteTXID };
  }

  /** Uploads a single local LTX file to the client. */
  private async uploadLtxFile(level: number, minTXID: bigint, maxTXID: bigint): Promise<FileInfo> {
    const p = (await this.localDb()).ltxPath(level, minTXID, maxTXID);
    const buf = new Uint8Array(await import('node:fs/promises').then((f) => f.readFile(p)));
    const info = await this.client.writeLtxFile({ level, minTXID, maxTXID }, chunks(buf));
    this.logger.debug('ltx file uploaded', { level, minTXID, maxTXID, size: info.size });
    return info;
  }

  /** EnsureExists restores the local database if it is missing. */
  async ensureExists(): Promise<void> {
    try {
      await access(this.path);
      return;
    } catch {
      /* missing; restore below */
    }
    await this.restore({ outputPath: this.path });
  }

  /** Restore reconstructs a SQLite database at `outputPath` from LTX files. */
  async restore(opt: RestoreOptions): Promise<void> {
    if (!opt.outputPath) throw new Error('output path required');

    const plan = await calcRestorePlan(this.client, opt.txID ?? 0n);
    if (plan.length === 0) throw new Error('no matching backup files available');

    // Read each LTX file into memory (MVP; streaming readers come later).
    const readers: Uint8Array[] = [];
    for (const info of plan) {
      const rc = await this.client.openLtxFile({
        level: info.level,
        minTXID: info.minTXID,
        maxTXID: info.maxTXID,
        offset: 0,
        size: 0,
      });
      readers.push(await readAll(rc));
    }

    // Compact all files into one LTX snapshot stream, then decode to a DB.
    const merged = new BufferSink();
    const compactor = new Compactor(merged, readers);
    compactor.headerFlags = HeaderFlagNoChecksum;
    compactor.compact();

    const dbBytes = new BufferSink();
    const dec = new Decoder(merged.toBuffer());
    dec.decodeDatabaseTo(dbBytes);

    // Atomically write the output database.
    await mkdir(nodePath.dirname(opt.outputPath), { recursive: true });
    const tmp = `${opt.outputPath}.tmp`;
    await writeFile(tmp, dbBytes.toBuffer());
    await rename(tmp, opt.outputPath);
  }
}

/**
 * CalcRestorePlan returns LTX files to apply, in order, to reconstruct a
 * database up to `txID` (0 = latest). It prefers the latest level-9 snapshot
 * before the target as the base, then appends contiguous level-0 files.
 */
export async function calcRestorePlan(
  client: ReplicaClient,
  txID: bigint,
): Promise<FileInfo[]> {
  const plan: FileInfo[] = [];
  let currentMax = 0n;

  // Pick the latest level-9 snapshot at or before the target as the base.
  let base: FileInfo | null = null;
  for await (const info of client.ltxFiles({ level: 9, seek: 0n, useMetadata: false })) {
    if (txID !== 0n && info.maxTXID > txID) continue;
    if (!base || info.maxTXID > base.maxTXID) base = info;
  }
  if (base) {
    plan.push(base);
    currentMax = base.maxTXID;
  }

  // Gather level-0 files and extend the contiguous range from currentMax.
  const l0: FileInfo[] = [];
  for await (const info of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
    if (txID !== 0n && info.maxTXID > txID) continue;
    l0.push(info);
  }
  l0.sort((a, b) => (a.minTXID < b.minTXID ? -1 : a.minTXID > b.minTXID ? 1 : 0));

  for (const info of l0) {
    if (info.maxTXID <= currentMax) continue; // already covered by the base
    if (info.minTXID > currentMax + 1n) break; // non-contiguous
    plan.push(info);
    currentMax = info.maxTXID;
  }

  if (plan.length === 0) {
    throw new Error('no matching backup files available');
  }
  if (txID !== 0n && currentMax < txID) {
    throw new Error('no matching backup files available');
  }
  return plan;
}
