/**
 * Replica manages replication of a single database to a ReplicaClient.
 *
 * It uploads local level-0 LTX files to the client, tracks the replicated
 * position (the max maxTXID of level-0 files on the client), and can restore a
 * database by merging LTX files. Mirrors litestream's `replica.go`.
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
import type { DB } from './db.ts';
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

/** Replica replicates a single database to a ReplicaClient. */
export class Replica {
  readonly db: DB;
  readonly client: ReplicaClient;
  private logger: Logger;
  private posValue: bigint | null = null;

  constructor(db: DB, client: ReplicaClient, logger: Logger = noopLogger) {
    this.db = db;
    this.client = client;
    this.logger = logger;
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

  /** Sync uploads local L0 LTX files to the client, tracking position. */
  async sync(): Promise<void> {
    await this.client.init();
    const pos = await this.pos();
    const dbPos = await this.db.pos();

    for (let txID = pos + 1n; txID <= dbPos; txID = (await this.pos()) + 1n) {
      await this.uploadLtxFile(0, txID, txID);
      this.posValue = txID;
    }
  }

  /** SyncAndWait uploads all pending LTX files; harmless if already synced. */
  async syncAndWait(): Promise<void> {
    await this.sync();
  }

  /** Snapshot writes and uploads a level-9 full-database LTX file. */
  async snapshot(): Promise<void> {
    const info = await this.db.snapshot();
    if (info) {
      await this.uploadLtxFile(9, info.minTXID, info.maxTXID);
    }
  }

  /** SyncStatus reports the local and remote positions. */
  async syncStatus(): Promise<SyncStatus> {
    const localTXID = await this.db.pos();
    const remoteTXID = await this.calcPos();
    return { localTXID, remoteTXID, inSync: localTXID === remoteTXID };
  }

  /** Uploads a single local LTX file to the client. */
  private async uploadLtxFile(level: number, minTXID: bigint, maxTXID: bigint): Promise<FileInfo> {
    const p = this.db.ltxPath(level, minTXID, maxTXID);
    const buf = new Uint8Array(await import('node:fs/promises').then((f) => f.readFile(p)));
    const info = await this.client.writeLtxFile({ level, minTXID, maxTXID }, chunks(buf));
    this.logger.debug('ltx file uploaded', { level, minTXID, maxTXID, size: info.size });
    return info;
  }

  /** EnsureExists restores the local database if it is missing. */
  async ensureExists(): Promise<void> {
    try {
      await access(this.db.path);
      return;
    } catch {
      /* missing; restore below */
    }
    await this.restore({ outputPath: this.db.path });
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
