/**
 * Replica manages replication of a single SQLite database (identified by its
 * `path`) to a ReplicaClient.
 *
 * It reads the database + WAL directly from disk, writes level-0/level-9 LTX
 * files into a local cache, uploads them to the client, tracks the replicated
 * position, and can restore a database by merging LTX files. Mirrors
 * litestream's `replica.go`, but is agnostic to the SQLite driver the
 * application uses.
 *
 * For snapshot consistency it opens its own **independent read-only** SQLite
 * connection (never the application's writable one) to pin the WAL while it
 * reads. A single SQLite connection holds one transaction at a time, so
 * reusing an application `DatabaseSync` handle would merge the read snapshot
 * with the app's writes (and could read uncommitted rows). Keep the app's
 * writer and the library's reader on separate connections to the same file.
 */

import { createReadStream } from 'node:fs';
import { access, mkdir, open, rename, rm, type FileHandle } from 'node:fs/promises';
import nodePath from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

import { lockPgno, StreamLtxReader, type FileInfo } from './ltx/index.ts';
import { LocalDatabase } from './db.ts';
import type { ReplicaClient } from './replica-client.ts';
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

export interface ReplicaRunOptions {
  /** Milliseconds between syncs. Defaults to 1000. */
  intervalMs?: number;
  /** Abort to stop continuous syncing. */
  signal?: AbortSignal;
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

  /**
   * Run continuously syncs the replica until `signal` is aborted. It syncs
   * immediately, then waits `intervalMs` and repeats. Errors during a sync are
   * logged and the loop keeps going, so a transient failure does not stop
   * replication. Resolves once the signal is aborted.
   */
  async run(opt: ReplicaRunOptions = {}): Promise<void> {
    const { intervalMs = 1000, signal } = opt;
    while (!signal?.aborted) {
      try {
        await this.sync();
      } catch (err) {
        this.logger.error('replica sync error', err);
      }
      if (signal?.aborted) break;
      try {
        await sleep(intervalMs, undefined, { signal });
      } catch {
        break; // aborted during the sleep
      }
    }
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
    // Stream the local LTX file straight to the client (no whole-file buffer).
    const body = createReadStream(p) as unknown as AsyncIterable<Uint8Array>;
    const info = await this.client.writeLtxFile({ level, minTXID, maxTXID }, body);
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

    await mkdir(nodePath.dirname(opt.outputPath), { recursive: true });
    const tmp = `${opt.outputPath}.tmp`;

    let fh: FileHandle | null = null;
    const sources: Array<{ destroy?: () => void }> = [];
    try {
      fh = await open(tmp, 'w');

      // The first plan file is the base (a snapshot). Stream its pages
      // sequentially into the output, then overlay each later file's pages by
      // offset, so a later version of a page wins. Peak memory stays bounded by
      // one page buffer + the (small) page index, never the whole DB.
      const openReader = (info: FileInfo): Promise<StreamLtxReader> => this.openStreamReader(info, sources);

      const base = await openReader(plan[0]);
      await base.readHeader();
      let pageSize = base.Header().pageSize;
      let commit = base.Header().commit;
      await writeSnapshotToFile(fh, base, pageSize, commit);
      await base.verify();

      for (let i = 1; i < plan.length; i++) {
        const reader = await openReader(plan[i]);
        await reader.readHeader();
        const hdr = reader.Header();
        if (hdr.pageSize !== pageSize) {
          throw new Error(`mismatched page sizes: ${pageSize} != ${hdr.pageSize}`);
        }
        commit = hdr.commit;
        await overlayPagesToFile(fh, reader, pageSize);
        await reader.verify();
      }

      // The final database size is the last file's committed page count.
      await fh.truncate(commit * pageSize);
      await fh.sync();
      await fh.close();
      fh = null;

      await rename(tmp, opt.outputPath);
    } catch (err) {
      if (fh !== null) await fh.close().catch(() => {});
      await rm(tmp, { force: true }).catch(() => {});
      throw err;
    } finally {
      // Release any not-yet-drained input streams (avoids leaking fds/connections
      // on a corrupt/truncated backup). Destroying an ended stream is a no-op.
      for (const s of sources) {
        try {
          s.destroy?.();
        } catch {
          /* ignore */
        }
      }
    }
  }

  /** Opens a streaming LTX reader over a plan file from the client. */
  private async openStreamReader(
    info: FileInfo,
    sources?: Array<{ destroy?: () => void }>,
  ): Promise<StreamLtxReader> {
    const rc = await this.client.openLtxFile({
      level: info.level,
      minTXID: info.minTXID,
      maxTXID: info.maxTXID,
      offset: 0,
      size: 0,
    });
    if (sources && typeof (rc as { destroy?: unknown }).destroy === 'function') {
      sources.push({ destroy: () => (rc as { destroy: () => void }).destroy() });
    }
    return new StreamLtxReader(rc as unknown as AsyncIterable<Uint8Array>);
  }

  /** Close releases any internal database connection. */
  close(): void {
    if (this.db !== null) {
      this.db.close();
      this.db = null;
    }
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

/**
 * Streams a snapshot's pages (1..commit) into the output file. The lock page
 * is written as a zeroed page (SQLite requires it to exist but never uses it).
 */
async function writeSnapshotToFile(
  fh: FileHandle,
  reader: StreamLtxReader,
  pageSize: number,
  commit: number,
): Promise<void> {
  const lockPg = lockPgno(pageSize);
  const page = new Uint8Array(pageSize);
  const zero = new Uint8Array(pageSize);

  for (let pgno = 1; pgno <= commit; pgno++) {
    if (pgno === lockPg) {
      await writeAllAt(fh, zero, (pgno - 1) * pageSize);
      continue;
    }
    const hdr = await reader.readPage(page);
    if (hdr === null) throw new Error(`unexpected end of pages at pgno ${pgno}`);
    if (hdr.pgno !== pgno) {
      throw new Error(`unexpected pgno while decoding page: read ${hdr.pgno}, expected ${pgno}`);
    }
    await writeAllAt(fh, page, (pgno - 1) * pageSize);
  }

  // Expect one final read to be the end-of-pages marker.
  if ((await reader.readPage(page)) !== null) {
    throw new Error('unexpected page after end of database');
  }
}

/** Overlays an incremental file's pages into the output file at their offsets. */
async function overlayPagesToFile(
  fh: FileHandle,
  reader: StreamLtxReader,
  pageSize: number,
): Promise<void> {
  const page = new Uint8Array(pageSize);
  for (;;) {
    const hdr = await reader.readPage(page);
    if (hdr === null) break;
    await writeAllAt(fh, page, (hdr.pgno - 1) * pageSize);
  }
}

/** Writes all of `bytes` to `handle` at an absolute `position`, looping on partial writes. */
async function writeAllAt(
  handle: FileHandle,
  bytes: Uint8Array,
  position: number,
): Promise<void> {
  let offset = 0;
  while (offset < bytes.length) {
    const { bytesWritten } = await handle.write(
      bytes,
      offset,
      bytes.length - offset,
      position + offset,
    );
    offset += bytesWritten;
  }
}
