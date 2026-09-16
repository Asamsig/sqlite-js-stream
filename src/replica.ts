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
  Decoder,
  lockPgno,
  type FileInfo,
  type Header,
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

    // Merge pages across the plan (later files override earlier ones).
    const pages = new Map<number, Uint8Array>();
    let commit = 0;
    let pageSize = 0;
    for (const info of plan) {
      const { header, pageData } = await this.decodeFile(info);
      commit = Math.max(commit, header.commit);
      pageSize = header.pageSize;
      for (const [pgno, data] of pageData) pages.set(pgno, data);
    }

    const lockPg = lockPgno(pageSize);
    const out = new Uint8Array(commit * pageSize);
    for (let pgno = 1; pgno <= commit; pgno++) {
      if (pgno === lockPg) continue; // leave as zeros
      const data = pages.get(pgno);
      if (data) out.set(data, (pgno - 1) * pageSize);
    }

    // Atomically write the output database.
    await mkdir(nodePath.dirname(opt.outputPath), { recursive: true });
    const tmp = `${opt.outputPath}.tmp`;
    await writeFile(tmp, out);
    await rename(tmp, opt.outputPath);
  }

  /** Decodes a single LTX file from the client into its pages. */
  private async decodeFile(info: FileInfo): Promise<{ header: Header; pageData: Map<number, Uint8Array> }> {
    const rc = await this.client.openLtxFile({
      level: info.level,
      minTXID: info.minTXID,
      maxTXID: info.maxTXID,
      offset: 0,
      size: 0,
    });
    const buf = await readAll(rc);
    const dec = new Decoder(buf);
    dec.decodeHeader();
    const header = dec.Header();
    const pageData = new Map<number, Uint8Array>();
    const data = new Uint8Array(header.pageSize);
    for (;;) {
      const ph = dec.decodePage(data);
      if (ph === null) break;
      pageData.set(ph.pgno, new Uint8Array(data));
    }
    dec.close();
    return { header, pageData };
  }
}

/**
 * CalcRestorePlan returns LTX files (level 0) to apply, in order, to
 * reconstruct a database up to `txID`. For the MVP each level-0 file is a full
 * snapshot so the latest eligible file supersedes the rest; merging remains
 * correct if incremental files are later introduced.
 */
export async function calcRestorePlan(
  client: ReplicaClient,
  txID: bigint,
): Promise<FileInfo[]> {
  const files: FileInfo[] = [];
  for await (const info of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
    if (txID !== 0n && info.maxTXID > txID) continue;
    files.push(info);
  }
  files.sort((a, b) => (a.minTXID < b.minTXID ? -1 : a.minTXID > b.minTXID ? 1 : 0));
  if (files.length === 0) {
    throw new Error('no matching backup files available');
  }
  return files;
}
