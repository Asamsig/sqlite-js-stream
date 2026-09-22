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
import { access, mkdir, open, rename, rm, stat, type FileHandle } from 'node:fs/promises';
import nodePath from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

import {
  lockPgno,
  StreamLtxReader,
  StreamingCompactor,
  type FileInfo,
} from './ltx/index.ts';
import { LocalDatabase } from './db.ts';
import type { ReplicaClient } from './replica-client.ts';
import { SnapshotLevel } from './replica-client.ts';
import { FileSink } from './util/fs.ts';
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

/** Default highest LTX compaction level (L0 -> L1 -> ... -> L{max}). */
export const DefaultMaxCompactionLevel = 3;

export interface CompactOptions {
  /**
   * Destination level to compact into (1..maxLevel). When omitted, the first
   * level (1..maxLevel) that has pending source files is compacted.
   */
  dstLevel?: number;
}

/** Retention/GC policy for pruning old snapshots and compacted files. */
export interface RetentionOptions {
  /** Keep this many newest level-9 snapshots. Defaults to 2. */
  snapshotRetain?: number;
  /** Delete level-9 snapshots older than this many milliseconds. */
  snapshotRetentionMs?: number;
  /**
   * Minimum age (ms) before a file that has been compacted into a higher level
   * is deleted. Defaults to 0 (delete as soon as it is covered).
   */
  l0RetentionMs?: number;
}

export interface RetentionResult {
  snapshotsDeleted: number;
  filesDeleted: number;
  /** maxTXID of the oldest retained snapshot; files below it are removed. */
  boundary: bigint;
}

export interface ReplicaRunOptions {
  /** Milliseconds between syncs. Defaults to 1000. */
  intervalMs?: number;
  /** Abort to stop continuous syncing. */
  signal?: AbortSignal;
  /** Run compaction every N ms while running (0 disables). Defaults to 0. */
  compactionIntervalMs?: number;
  /** Run retention GC every N ms while running (0 disables). Defaults to 0. */
  retentionIntervalMs?: number;
  /** Retention policy used by the retention loop. */
  retention?: RetentionOptions;
  /** Max compaction level. Defaults to {@link DefaultMaxCompactionLevel}. */
  maxLevel?: number;
}

/** Replica replicates a single database (by path) to a ReplicaClient. */
export class Replica {
  readonly path: string;
  readonly client: ReplicaClient;
  private logger: Logger;
  private posValue: bigint | null = null;
  private db: LocalDatabase | null = null;

  /** Highest LTX compaction level this replica will produce. */
  maxCompactionLevel = DefaultMaxCompactionLevel;

  constructor(
    path: string,
    client: ReplicaClient,
    logger: Logger = noopLogger,
    options: { maxLevel?: number } = {},
  ) {
    this.path = path;
    this.client = client;
    this.logger = logger;
    if (options.maxLevel !== undefined) {
      this.maxCompactionLevel = options.maxLevel;
    }
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

  /**
   * CalcPos finds the maximum TXID on the replica across all levels. Since
   * compaction moves the newest data to higher levels and prunes level-0, the
   * position must be derived from every level (and any snapshot).
   */
  async calcPos(): Promise<bigint> {
    let max = 0n;
    for (let level = 0; level <= SnapshotLevel; level++) {
      for await (const info of this.client.ltxFiles({ level, seek: 0n, useMetadata: false })) {
        if (info.maxTXID > max) max = info.maxTXID;
      }
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
    if (opt.maxLevel !== undefined) this.maxCompactionLevel = opt.maxLevel;
    const compactionIntervalMs = opt.compactionIntervalMs ?? 0;
    const retentionIntervalMs = opt.retentionIntervalMs ?? 0;

    let lastCompaction = 0;
    let lastRetention = 0;

    while (!signal?.aborted) {
      try {
        await this.sync();
      } catch (err) {
        this.logger.error('replica sync error', err);
      }
      if (signal?.aborted) break;

      const now = Date.now();
      if (compactionIntervalMs > 0 && now - lastCompaction >= compactionIntervalMs) {
        try {
          await this.compact();
        } catch (err) {
          this.logger.error('replica compaction error', err);
        }
        lastCompaction = Date.now();
      }
      if (retentionIntervalMs > 0 && now - lastRetention >= retentionIntervalMs) {
        try {
          await this.enforceRetention(opt.retention ?? {});
        } catch (err) {
          this.logger.error('replica retention error', err);
        }
        lastRetention = Date.now();
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

  /**
   * Compact merges source LTX files into the next level. If `dstLevel` is
   * given, compacts that level; otherwise compacts the first level (1..max)
   * that has pending source files. Returns the written file, or null if there
   * was nothing to compact.
   */
  async compact(options: CompactOptions = {}): Promise<FileInfo | null> {
    if (options.dstLevel !== undefined) {
      if (options.dstLevel < 1 || options.dstLevel > this.maxCompactionLevel) return null;
      return this.compactLevel(options.dstLevel);
    }
    for (let dst = 1; dst <= this.maxCompactionLevel; dst++) {
      const info = await this.compactLevel(dst);
      if (info) return info;
    }
    return null;
  }

  /**
   * CompactLevel compacts pending files at `dstLevel - 1` into a single file at
   * `dstLevel`. It streams readers (local cache preferred, client fallback),
   * writes the result locally via a file-backed sink, uploads it to the client,
   * and returns the new file's metadata. Source files are left in place; GC is
   * performed by {@link enforceRetention}.
   */
  async compactLevel(dstLevel: number): Promise<FileInfo | null> {
    if (dstLevel < 1 || dstLevel > this.maxCompactionLevel) return null;
    const srcLevel = dstLevel - 1;

    // Determine the max TXID already present at the destination level; only
    // source files after it remain to be compacted.
    let dstMax = 0n;
    for await (const info of this.client.ltxFiles({ level: dstLevel, seek: 0n, useMetadata: false })) {
      if (info.maxTXID > dstMax) dstMax = info.maxTXID;
    }
    const seek = dstMax + 1n;

    const sources: FileInfo[] = [];
    for await (const info of this.client.ltxFiles({ level: srcLevel, seek, useMetadata: false })) {
      sources.push(info);
    }
    if (sources.length === 0) return null;

    let minTXID = sources[0].minTXID;
    let maxTXID = sources[0].maxTXID;
    for (const info of sources) {
      if (info.minTXID < minTXID) minTXID = info.minTXID;
      if (info.maxTXID > maxTXID) maxTXID = info.maxTXID;
    }
    if (minTXID < seek) {
      throw new Error(
        `compaction source ${minTXID}..${maxTXID} overlaps the destination max ${dstMax}`,
      );
    }

    const cleanup: Array<{ destroy?: () => void }> = [];
    const readers: StreamLtxReader[] = [];
    const db = await this.localDb();
    const outPath = db.ltxPath(dstLevel, minTXID, maxTXID);
    const tmpPath = `${outPath}.tmp`;
    await mkdir(nodePath.dirname(outPath), { recursive: true });
    const fh = await open(tmpPath, 'w');
    try {
      for (const info of sources) {
        readers.push(await this.openSourceReader(info, cleanup));
      }

      const sink = new FileSink(fh);
      const compactor = new StreamingCompactor(sink, readers);
      await compactor.compact();
      const cHeader = compactor.Header();
      if (cHeader.pageSize !== (await this.localDb()).pageSize) {
        throw new Error(
          `compacted page size ${cHeader.pageSize} does not match database ${(await this.localDb()).pageSize}`,
        );
      }
      await sink.flush();
      await fh.close();

      await rename(tmpPath, outPath);

      const size = (await stat(outPath)).size;
      await this.uploadLtxFile(dstLevel, minTXID, maxTXID);
      return { level: dstLevel, minTXID, maxTXID, size, createdAt: Date.now() };
    } catch (err) {
      await fh.close().catch(() => {});
      await rm(tmpPath, { force: true }).catch(() => {});
      throw err;
    } finally {
      for (const s of cleanup) {
        try {
          s.destroy?.();
        } catch {
          /* ignore */
        }
      }
    }
  }

  /**
   * EnforceRetention prunes old snapshots and compacted files.
   *
   * - Level-9 snapshots beyond the retention policy are deleted.
   * - Files (any level) whose maxTXID is below the oldest retained snapshot's
   *   maxTXID are deleted, since a retained snapshot supersedes them.
   * - Files covered by a higher-level file and older than `l0RetentionMs` are
   *   deleted.
   *
   * It never deletes files that the retained data needs to restore the current
   * state or any point-in-time target within the retention window.
   */
  async enforceRetention(options: RetentionOptions = {}): Promise<RetentionResult> {
    const now = Date.now();
    const snapshotRetain = options.snapshotRetain ?? 2;
    const snapshotRetentionMs = options.snapshotRetentionMs;
    const l0RetentionMs = options.l0RetentionMs ?? 0;

    const snapshots = await this.listRemoteLevel(SnapshotLevel);
    snapshots.sort((a, b) => (a.maxTXID < b.maxTXID ? -1 : a.maxTXID > b.maxTXID ? 1 : 0));

    let deletedSnapshots: FileInfo[] = [];
    let boundary = 0n;
    if (snapshots.length > 0) {
      let retained: FileInfo[];
      if (snapshotRetentionMs !== undefined) {
        const cutoff = now - snapshotRetentionMs;
        retained = snapshots.filter((s) => s.createdAt >= cutoff);
      } else {
        retained = [...snapshots];
      }
      if (snapshotRetain > 0 && retained.length > snapshotRetain) {
        retained = retained.slice(retained.length - snapshotRetain);
      }
      // Always keep at least the newest snapshot so we never lose the base.
      if (retained.length === 0) retained = [snapshots[snapshots.length - 1]];

      const retainedSet = new Set(retained);
      deletedSnapshots = snapshots.filter((s) => !retainedSet.has(s));
      boundary = retained.reduce(
        (m, s) => (s.maxTXID < m ? s.maxTXID : m),
        retained[0].maxTXID,
      );
      await this.deleteFiles(deletedSnapshots);
    }

    // Gather every file at compaction levels 1..max as potential "coverers".
    const coverers: FileInfo[] = [];
    for (let level = 1; level <= this.maxCompactionLevel; level++) {
      for await (const info of this.client.ltxFiles({ level, seek: 0n, useMetadata: false })) {
        coverers.push(info);
      }
    }

    const toDelete: FileInfo[] = [];
    for (let level = 0; level <= this.maxCompactionLevel; level++) {
      for await (const info of this.client.ltxFiles({ level, seek: 0n, useMetadata: false })) {
        // Below the oldest retained snapshot -> superseded by that snapshot.
        if (info.maxTXID < boundary) {
          toDelete.push(info);
          continue;
        }
        // Covered by a higher-level file and old enough.
        if (now - info.createdAt >= l0RetentionMs && this.isCoveredByHigherLevel(info, coverers)) {
          toDelete.push(info);
        }
      }
    }
    await this.deleteFiles(toDelete);

    return { snapshotsDeleted: deletedSnapshots.length, filesDeleted: toDelete.length, boundary };
  }

  /** Returns true if `info` is fully contained in a file at a higher level. */
  private isCoveredByHigherLevel(info: FileInfo, coverers: FileInfo[]): boolean {
    for (const g of coverers) {
      if (g.level <= info.level) continue;
      if (g.minTXID <= info.minTXID && info.maxTXID <= g.maxTXID) return true;
    }
    return false;
  }

  /** Lists LTX files at a remote level. */
  private async listRemoteLevel(level: number): Promise<FileInfo[]> {
    const out: FileInfo[] = [];
    for await (const info of this.client.ltxFiles({ level, seek: 0n, useMetadata: false })) {
      out.push(info);
    }
    return out;
  }

  /** Deletes files from the client and the local cache. */
  private async deleteFiles(files: FileInfo[]): Promise<void> {
    if (files.length === 0) return;
    this.logger.debug('deleting ltx files', { count: files.length });
    await this.client.deleteLtxFiles(files);
    const db = await this.localDb();
    for (const f of files) {
      const p = db.ltxPath(f.level, f.minTXID, f.maxTXID);
      await rm(p, { force: true }).catch(() => {});
    }
  }

  /**
   * Opens a streaming LTX reader for a source file, preferring the local cache
   * and falling back to the client. Collects cleanup callbacks for the streams.
   */
  private async openSourceReader(
    info: FileInfo,
    cleanup: Array<{ destroy?: () => void }>,
  ): Promise<StreamLtxReader> {
    const db = await this.localDb();
    const localPath = db.ltxPath(info.level, info.minTXID, info.maxTXID);
    try {
      await access(localPath);
      const s = createReadStream(localPath);
      cleanup.push({ destroy: () => s.destroy() });
      return new StreamLtxReader(s as unknown as AsyncIterable<Uint8Array>);
    } catch {
      /* fall back to the client */
    }
    const rc = await this.client.openLtxFile({
      level: info.level,
      minTXID: info.minTXID,
      maxTXID: info.maxTXID,
      offset: 0,
      size: 0,
    });
    if (typeof (rc as { destroy?: unknown }).destroy === 'function') {
      cleanup.push({ destroy: () => (rc as { destroy: () => void }).destroy() });
    }
    return new StreamLtxReader(rc as unknown as AsyncIterable<Uint8Array>);
  }
}

/**
 * CalcRestorePlan returns LTX files to apply, in order, to reconstruct a
 * database up to `txID` (0 = latest). It prefers the latest level-9 snapshot
 * before the target as the base, then appends contiguous files across all
 * compaction levels (0..maxLevel) that extend the range.
 */
export async function calcRestorePlan(
  client: ReplicaClient,
  txID: bigint,
): Promise<FileInfo[]> {
  const plan: FileInfo[] = [];
  let currentMax = 0n;

  // Pick the latest level-9 snapshot at or before the target as the base.
  let base: FileInfo | null = null;
  for await (const info of client.ltxFiles({ level: SnapshotLevel, seek: 0n, useMetadata: false })) {
    if (txID !== 0n && info.maxTXID > txID) continue;
    if (!base || info.maxTXID > base.maxTXID) base = info;
  }
  if (base) {
    plan.push(base);
    currentMax = base.maxTXID;
  }

  // Collect candidate files from every compaction level (0..SnapshotLevel-1).
  // Compaction moves data to higher levels, so a TXID range may be represented
  // by a file at any level; gather them all and prefer the highest level first.
  const candidates: FileInfo[] = [];
  for (let level = 0; level < SnapshotLevel; level++) {
    for await (const info of client.ltxFiles({ level, seek: 0n, useMetadata: false })) {
      if (txID !== 0n && info.maxTXID > txID) continue;
      candidates.push(info);
    }
  }
  // Order by minTXID, then by higher level (more compacted) first, so a file
  // at a higher level supersedes lower-level files spanning the same range.
  candidates.sort((a, b) => {
    if (a.minTXID !== b.minTXID) return a.minTXID < b.minTXID ? -1 : 1;
    return b.level - a.level;
  });

  const seenRanges: Array<[bigint, bigint]> = [];
  for (const info of candidates) {
    if (info.maxTXID <= currentMax) continue; // covered by the base or earlier
    if (info.minTXID > currentMax + 1n) break; // non-contiguous gap
    // Skip a candidate entirely covered by a higher-level file already in the plan.
    if (seenRanges.some(([lo, hi]) => lo <= info.minTXID && info.maxTXID <= hi)) continue;
    plan.push(info);
    currentMax = info.maxTXID;
    seenRanges.push([info.minTXID, info.maxTXID]);
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
