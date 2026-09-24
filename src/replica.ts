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
  CompactionLevels,
  nextCompactionAt,
  prevCompactionAt,
  defaultCompactionLevels,
  DefaultL0RetentionCheckIntervalMs,
  type FileInfo,
  type CompactionLevel,
} from './ltx/index.ts';
import { LocalDatabase } from './db.ts';
import type { ReplicaClient } from './replica-client.ts';
import { SnapshotLevel } from './replica-client.ts';
import { restoreV3, type GenerationClientV3 } from './restore-v3.ts';
import {
  DefaultLeaseRenewalBuffer,
  ErrLeaseNotHeld,
  Lease,
  LeaseExistsError,
  type Leaser,
} from './leaser.ts';
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

/**
 * Default highest LTX compaction level (L0 -> L1 -> ... -> L{max}) for the
 * legacy {@link CompactOptions.maxLevel} path. Prefer {@link CompactionLevels}.
 */
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
  /**
   * Delete level-9 snapshots older than this many milliseconds. The newest
   * snapshot is always kept. Defaults to 0 (keep all snapshots).
   */
  snapshotRetentionMs?: number;
  /**
   * Keep at least this many newest level-9 snapshots even if they are older
   * than {@link snapshotRetentionMs}. Defaults to 1 (always keep the newest).
   */
  snapshotRetain?: number;
  /**
   * Minimum age (ms) before a level-0 file that has been compacted into level-1
   * is deleted. Defaults to 0 (no L0 time-based GC).
   */
  l0RetentionMs?: number;
}

export interface RetentionResult {
  snapshotsDeleted: number;
  filesDeleted: number;
  /** MaxTXID of the last snapshot that was deleted; files below it are removed. */
  boundary: bigint;
}

/** Returned by compaction scheduling when there is nothing to compact. */
export const ErrNoCompaction = new Error('no files to compact');
/** Returned when a level was already compacted within its current time bucket. */
export const ErrCompactionTooEarly = new Error('compaction scheduled too early');

export interface ReplicaRunOptions {
  /** Milliseconds between syncs. Defaults to 1000. */
  intervalMs?: number;
  /** Abort to stop continuous syncing. */
  signal?: AbortSignal;
  /**
   * Per-level compaction schedule. Defaults to {@link defaultCompactionLevels}.
   * Levels are compacted at most once per wall-clock bucket.
   */
  compactionLevels?: CompactionLevels;
  /**
   * Legacy: max compaction level when {@link compactionLevels} is not given.
   * Defaults to {@link DefaultMaxCompactionLevel}.
   */
  maxLevel?: number;
  /**
   * Legacy: cascade-compact all levels every N ms (disables per-level buckets
   * when set). Defaults to 0 (disabled).
   */
  compactionIntervalMs?: number;
  /** Legacy: run {@link Replica.enforceRetention} every N ms. Defaults to 0. */
  retentionIntervalMs?: number;
  /** Retention policy used by the legacy retention loop. */
  retention?: RetentionOptions;
  /**
   * Milliseconds between level-9 snapshots while running (0 disables).
   * Defaults to 0.
   */
  snapshotIntervalMs?: number;
  /** Time-based snapshot retention (ms) applied after each snapshot. */
  snapshotRetentionMs?: number;
  /** Keep at least this many newest snapshots (default 1). */
  snapshotRetain?: number;
  /**
   * Min age (ms) before a compacted level-0 file is deleted. When > 0 an L0
   * retention pass runs on {@link l0RetentionCheckIntervalMs}. Defaults to 0.
   */
  l0RetentionMs?: number;
  /** How often (ms) to run the L0 time-retention pass. Defaults to 15s. */
  l0RetentionCheckIntervalMs?: number;
}

/** Constant intervals for compaction levels beyond the 4-level default. */
const HIGHER_LEVEL_INTERVAL_MS = 3_600_000;

/** Default intervals for levels 1..3 (matches litestream's defaults). */
const DEFAULT_LEVEL_INTERVALS = [30_000, 300_000, 3_600_000];

/** Builds a CompactionLevels covering levels 0..max with sane intervals. */
function compactionLevelsForMax(max: number): CompactionLevels {
  const arr: CompactionLevel[] = [];
  for (let i = 0; i <= max; i++) {
    const intervalMs =
      i === 0 ? 0 : (DEFAULT_LEVEL_INTERVALS[i - 1] ?? HIGHER_LEVEL_INTERVAL_MS);
    arr.push({ level: i, intervalMs });
  }
  return new CompactionLevels(arr);
}

/** Replica replicates a single database (by path) to a ReplicaClient. */
export class Replica {
  readonly path: string;
  readonly client: ReplicaClient;
  private logger: Logger;
  private posValue: bigint | null = null;
  private db: LocalDatabase | null = null;
  private compactionLevelsValue: CompactionLevels = defaultCompactionLevels();
  private leaserValue: Leaser | null = null;
  private leasePrimary = true;
  private leaseCurrent: Lease | null = null;
  private leaseTimer: NodeJS.Timeout | null = null;
  private runActive = false;

  constructor(
    path: string,
    client: ReplicaClient,
    logger: Logger = noopLogger,
    options: { maxLevel?: number; compactionLevels?: CompactionLevels; leaser?: Leaser } = {},
  ) {
    this.path = path;
    this.client = client;
    this.logger = logger;
    if (options.compactionLevels) {
      this.compactionLevelsValue = options.compactionLevels;
    } else if (options.maxLevel !== undefined) {
      this.maxCompactionLevel = options.maxLevel;
    }
    if (options.leaser) {
      this.leaserValue = options.leaser;
      // A leaser is not held until acquired, so a fresh replica starts as a
      // follower until {@link run} (or an explicit acquire) makes it primary.
      this.leasePrimary = false;
    }
  }

  /** Leaser optionally bounds this replica to a single primary across hosts. */
  get leaser(): Leaser | null {
    return this.leaserValue;
  }

  set leaser(leaser: Leaser | null) {
    this.leaserValue = leaser;
  }

  /**
   * IsPrimary reports whether this replica may currently write. Always true
   * when no {@link Leaser} is configured; false while another host holds the
   * lease. Updated by {@link run}.
   */
  isPrimary(): boolean {
    return this.leasePrimary;
  }

  /** @internal Throws if a leaser is set but this replica does not hold it. */
  private assertCanWrite(): void {
    if (this.leaserValue !== null && !this.leasePrimary) {
      throw new ErrLeaseNotHeld();
    }
  }

  /** Per-level compaction schedule. Defaults to {@link defaultCompactionLevels}. */
  get compactionLevels(): CompactionLevels {
    return this.compactionLevelsValue;
  }

  set compactionLevels(levels: CompactionLevels) {
    this.compactionLevelsValue = levels;
  }

  /** Highest non-snapshot compaction level this replica will produce. */
  get maxCompactionLevel(): number {
    return this.compactionLevelsValue.maxLevel();
  }

  set maxCompactionLevel(level: number) {
    this.compactionLevelsValue = compactionLevelsForMax(level);
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
    this.assertCanWrite();
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
   * Run continuously drives the replica until `signal` is aborted. It syncs on
   * `intervalMs`, compacts per level on each level's wall-clock bucket, takes
   * snapshots on `snapshotIntervalMs`, and enforces retention. Errors are logged
   * and the loop keeps going, so a transient failure does not stop replication.
   *
   * Compaction mirrors litestream: each level is compacted at most once per
   * bucket (see {@link CompactionLevels}); the legacy `compactionIntervalMs`
   * cascades all levels on a fixed interval instead.
   */
  async run(opt: ReplicaRunOptions = {}): Promise<void> {
    const { intervalMs = 1000, signal } = opt;
    if (opt.maxLevel !== undefined) this.maxCompactionLevel = opt.maxLevel;

    const levels = opt.compactionLevels ?? this.compactionLevels;
    const useLegacyCompaction = (opt.compactionIntervalMs ?? 0) > 0;
    const compactionIntervalMs = opt.compactionIntervalMs ?? 0;
    const retentionIntervalMs = opt.retentionIntervalMs ?? 0;
    const snapshotIntervalMs = opt.snapshotIntervalMs ?? 0;
    const l0RetentionMs = opt.l0RetentionMs ?? 0;
    const l0RetentionCheckIntervalMs =
      opt.l0RetentionCheckIntervalMs ?? DefaultL0RetentionCheckIntervalMs;

    // Try to acquire the lease up front. If held by another host we become a
    // follower and poll until it is available. Without a leaser we are primary.
    this.runActive = true;
    const leaser = this.leaserValue;
    let primary = leaser === null;
    if (leaser !== null) {
      try {
        this.leaseCurrent = await leaser.acquireLease();
        primary = true;
        this.scheduleLeaseRenewal();
      } catch (err) {
        if (err instanceof LeaseExistsError) {
          this.logger.debug('lease held elsewhere; running as follower', err);
          primary = false;
        } else {
          this.runActive = false;
          throw err;
        }
      }
    }
    this.leasePrimary = primary;

    let lastCompaction = 0;
    let lastRetention = 0;
    let snapshotNext =
      snapshotIntervalMs > 0 ? nextCompactionAt(Date.now(), snapshotIntervalMs) : Infinity;
    let l0RetentionNext = l0RetentionMs > 0 ? Date.now() + l0RetentionCheckIntervalMs : Infinity;

    // Per-level next-bucket times (litestream-style scheduling).
    const levelNext = new Map<number, number>();
    if (!useLegacyCompaction) {
      for (const lvl of levels.toArray()) {
        if (lvl.level > 0 && lvl.intervalMs > 0) {
          levelNext.set(lvl.level, nextCompactionAt(Date.now(), lvl.intervalMs));
        }
      }
    }

    while (!signal?.aborted) {
      const now = Date.now();

      // Follower management: poll to acquire the lease. The primary never
      // renews here — a dedicated timer ({@link scheduleLeaseRenewal}) renews it
      // independently of the loop cadence, so a large `intervalMs` cannot let the
      // lease expire.
      if (leaser !== null && !primary) {
        try {
          this.leaseCurrent = await leaser.acquireLease();
          primary = true;
          this.leasePrimary = true;
          this.scheduleLeaseRenewal();
        } catch (err) {
          if (!(err instanceof LeaseExistsError)) {
            this.logger.error('lease acquire error', err);
          }
          await sleep(intervalMs, undefined, { signal });
          continue;
        }
      }

      // Only the primary (or a leaser-less replica) performs writes.
      if (leaser === null || primary) {
        try {
          await this.sync();
        } catch (err) {
          this.logger.error('replica sync error', err);
        }
      }
      if (signal?.aborted) break;

      if (leaser !== null && !primary) {
        await sleep(intervalMs, undefined, { signal });
        continue;
      }

      // Compaction.
      if (useLegacyCompaction) {
        if (compactionIntervalMs > 0 && now - lastCompaction >= compactionIntervalMs) {
          try {
            await this.compact();
          } catch (err) {
            this.logger.error('replica compaction error', err);
          }
          lastCompaction = Date.now();
        }
      } else {
        for (const [level, next] of levelNext) {
          if (now >= next) {
            try {
              await this.compactDB(level, now);
            } catch (err) {
              if (err !== ErrNoCompaction && err !== ErrCompactionTooEarly) {
                this.logger.error('replica compaction error', err);
              }
            }
            levelNext.set(level, nextCompactionAt(now, levels.level(level).intervalMs));
          }
        }
      }

      // Snapshot + snapshot retention.
      if (snapshotIntervalMs > 0 && now >= snapshotNext) {
        try {
          await this.compactSnapshot(now, snapshotIntervalMs);
          const retention = opt.snapshotRetentionMs ?? 0;
          if (retention > 0 || (opt.snapshotRetain ?? 0) > 0) {
            await this.enforceSnapshotRetention(now, retention, opt.snapshotRetain ?? 0);
          }
        } catch (err) {
          this.logger.error('replica snapshot error', err);
        }
        snapshotNext = nextCompactionAt(now, snapshotIntervalMs);
      }

      // Retention (legacy interval-driven or L0 time-based).
      if (retentionIntervalMs > 0 && now - lastRetention >= retentionIntervalMs) {
        try {
          await this.enforceRetention(opt.retention ?? {});
        } catch (err) {
          this.logger.error('replica retention error', err);
        }
        lastRetention = Date.now();
      }
      if (l0RetentionMs > 0 && now >= l0RetentionNext) {
        try {
          await this.enforceL0RetentionByTime(now, l0RetentionMs);
        } catch (err) {
          this.logger.error('replica l0 retention error', err);
        }
        l0RetentionNext = now + l0RetentionCheckIntervalMs;
      }

      if (signal?.aborted) break;
      try {
        await sleep(intervalMs, undefined, { signal });
      } catch {
        break; // aborted during the sleep
      }
    }

    // Stop the renewal timer and release the lease so another host can take
    // over promptly.
    this.stopLeaseRenewal();
    if (leaser !== null && this.leaseCurrent !== null) {
      try {
        await leaser.releaseLease(this.leaseCurrent);
      } catch (err) {
        this.logger.debug('lease release error', err);
      }
      this.leaseCurrent = null;
      this.leasePrimary = false;
    }
    this.runActive = false;
  }

  /**
   * Schedules the next lease renewal to fire just before expiry, independent of
   * the run-loop cadence. Reschedules after every successful renewal.
   */
  private scheduleLeaseRenewal(): void {
    this.stopLeaseRenewal();
    if (this.leaserValue === null || this.leaseCurrent === null) return;
    const delay = Math.max(
      100,
      this.leaseCurrent.expiresAt - DefaultLeaseRenewalBuffer - Date.now(),
    );
    this.leaseTimer = setTimeout(() => {
      this.leaseTimer = null;
      void this.renewLeaseLoop();
    }, delay);
  }

  /** Renews the lease and reschedules; reverts to follower if we lost it. */
  private async renewLeaseLoop(): Promise<void> {
    if (!this.runActive || this.leaserValue === null || this.leaseCurrent === null) return;
    try {
      this.leaseCurrent = await this.leaserValue.renewLease(this.leaseCurrent);
      this.leasePrimary = true;
      this.scheduleLeaseRenewal();
    } catch (err) {
      if (err instanceof ErrLeaseNotHeld) {
        this.logger.debug('lost lease; reverting to follower', err);
        this.leasePrimary = false;
        this.leaseCurrent = null;
        this.stopLeaseRenewal();
      } else {
        // Transient error: retry shortly rather than letting the lease expire.
        this.logger.error('lease renew error', err);
        this.scheduleLeaseRenewal();
      }
    }
  }

  /** Cancels any pending lease renewal. */
  private stopLeaseRenewal(): void {
    if (this.leaseTimer !== null) {
      clearTimeout(this.leaseTimer);
      this.leaseTimer = null;
    }
  }

  /** Snapshot writes and uploads a level-9 full-database LTX file. */
  async snapshot(): Promise<FileInfo | null> {
    this.assertCanWrite();
    const info = await (await this.localDb()).snapshot();
    if (info) {
      await this.uploadLtxFile(9, info.minTXID, info.maxTXID);
    }
    return info;
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

    // A replica may contain no LTX files (e.g. it only holds legacy v0.3.x
    // generation backups). calcRestorePlan throws in that case; catch it so we
    // can fall back to reading the v0.3.x generations.
    let plan: FileInfo[];
    try {
      plan = await calcRestorePlan(this.client, opt.txID ?? 0n);
    } catch {
      plan = [];
    }
    if (plan.length === 0) {
      if (opt.txID === undefined && isGenerationClientV3(this.client)) {
        await restoreV3(this.client as unknown as GenerationClientV3, opt.outputPath, opt.timestamp);
        return;
      }
      throw new Error('no matching backup files available');
    }

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
    this.assertCanWrite();
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
    this.assertCanWrite();
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
   * CompactDB compacts `dstLevel` if it is time to do so and there is pending
   * source data. Mirrors litestream's `Store.CompactDB`: it refuses to re-compact
   * within the level's current time bucket (throws {@link ErrCompactionTooEarly})
   * and skips when there is nothing to do (throws {@link ErrNoCompaction}).
   */
  async compactDB(dstLevel: number, now: number): Promise<FileInfo | null> {
    this.assertCanWrite();
    if (dstLevel === SnapshotLevel) return this.compactSnapshot(now, 0);
    const lvl = this.compactionLevels.level(dstLevel);
    const prevAt = prevCompactionAt(now, lvl.intervalMs);
    const dstInfo = await this.maxLTXFileInfo(dstLevel);
    if (dstInfo && dstInfo.createdAt > prevAt) throw ErrCompactionTooEarly;

    const srcLevel = this.compactionLevels.prevLevel(dstLevel);
    const srcInfo = await this.maxLTXFileInfo(srcLevel);
    if (srcInfo && dstInfo && srcInfo.maxTXID <= dstInfo.minTXID) throw ErrNoCompaction;

    const info = await this.compactLevel(dstLevel);
    if (info === null) throw ErrNoCompaction;
    return info;
  }

  /**
   * CompactSnapshot takes a level-9 snapshot if it is time to do so and the
   * database has advanced past the current snapshot. Mirrors `Store.CompactDB`
   * for the snapshot level.
   */
  async compactSnapshot(now: number, intervalMs: number): Promise<FileInfo | null> {
    this.assertCanWrite();
    const prevAt = prevCompactionAt(now, intervalMs);
    const dstInfo = await this.maxLTXFileInfo(SnapshotLevel);
    if (dstInfo && dstInfo.createdAt > prevAt) throw ErrCompactionTooEarly;
    const pos = await (await this.localDb()).pos();
    if (dstInfo && dstInfo.maxTXID !== 0n && dstInfo.maxTXID >= pos) throw ErrNoCompaction;
    return this.snapshot();
  }

  /** MaxLTXFileInfo returns the highest-TXID file at a level, or null. */
  async maxLTXFileInfo(level: number): Promise<FileInfo | null> {
    let max: FileInfo | null = null;
    for await (const info of this.client.ltxFiles({ level, seek: 0n, useMetadata: false })) {
      if (max === null || info.maxTXID > max.maxTXID) max = info;
    }
    return max;
  }

  /**
   * EnforceRetention prunes old snapshots and superseded files.
   *
   * 1. Snapshots older than `snapshotRetentionMs` are deleted (the newest is
   *    always kept; `snapshotRetain` may cap how many newest are kept).
   * 2. The resulting snapshot boundary is cascaded down through levels 0..max:
   *    any file whose maxTXID is below the oldest retained snapshot's maxTXID
   *    is deleted (it is superseded by that snapshot).
   * 3. Level-0 files that have been compacted into a higher level and are older
   *    than `l0RetentionMs` are deleted (except the newest, the live head).
   */
  async enforceRetention(options: RetentionOptions = {}): Promise<RetentionResult> {
    this.assertCanWrite();
    const now = Date.now();
    const snapshotRetentionMs = options.snapshotRetentionMs ?? 0;
    const snapshotRetain = options.snapshotRetain ?? 0;
    const l0RetentionMs = options.l0RetentionMs ?? 0;

    const { deleted: snapshotsDeleted, boundary } = await this.enforceSnapshotRetention(
      now,
      snapshotRetentionMs,
      snapshotRetain,
    );

    let filesDeleted = 0;
    if (boundary > 0n) {
      for (let level = 0; level <= this.maxCompactionLevel; level++) {
        filesDeleted += await this.enforceRetentionByTXID(level, boundary);
      }
    }
    filesDeleted += await this.enforceL0RetentionByTime(now, l0RetentionMs);

    return { snapshotsDeleted, filesDeleted, boundary };
  }

  /**
   * EnforceSnapshotRetention deletes snapshots older than `retentionMs` and
   * returns the boundary (max maxTXID of the deleted snapshots). The newest
   * snapshot is always kept; `retain` may keep additional newest snapshots.
   */
  async enforceSnapshotRetention(
    now: number,
    retentionMs: number,
    retain = 0,
  ): Promise<{ deleted: number; boundary: bigint }> {
    this.assertCanWrite();
    const snapshots = await this.listRemoteLevel(SnapshotLevel);
    snapshots.sort((a, b) => (a.maxTXID < b.maxTXID ? -1 : a.maxTXID > b.maxTXID ? 1 : 0));
    if (snapshots.length === 0) return { deleted: 0, boundary: 0n };

    const cutoff = retentionMs > 0 ? now - retentionMs : 0;
    let retained = snapshots.filter((s) => !(retentionMs > 0 && s.createdAt < cutoff));
    const newest = snapshots[snapshots.length - 1];
    if (!retained.includes(newest)) retained.push(newest);
    retained.sort((a, b) => (a.maxTXID < b.maxTXID ? -1 : a.maxTXID > b.maxTXID ? 1 : 0));
    if (retain > 0 && retained.length > retain) retained = retained.slice(retained.length - retain);

    const retainedSet = new Set(retained);
    const deletedFiles = snapshots.filter((s) => !retainedSet.has(s));
    await this.deleteFiles(deletedFiles);

    let boundary = 0n;
    for (const d of deletedFiles) if (d.maxTXID > boundary) boundary = d.maxTXID;
    return { deleted: deletedFiles.length, boundary };
  }

  /**
   * EnforceRetentionByTXID deletes files at `level` with maxTXID below `txID`,
   * always keeping at least one file. Returns the number deleted.
   */
  async enforceRetentionByTXID(level: number, txID: bigint): Promise<number> {
    this.assertCanWrite();
    if (txID <= 0n) return 0;
    const files = await this.listRemoteLevel(level);
    files.sort((a, b) => (a.maxTXID < b.maxTXID ? -1 : a.maxTXID > b.maxTXID ? 1 : 0));
    if (files.length === 0) return 0;

    const last = files[files.length - 1];
    const deletedFiles = files.filter((f) => f.maxTXID < txID);
    if (deletedFiles.length > 0 && deletedFiles[deletedFiles.length - 1] === last) {
      deletedFiles.pop(); // always keep at least one file
    }
    await this.deleteFiles(deletedFiles);
    return deletedFiles.length;
  }

  /**
   * EnforceL0RetentionByTime deletes level-0 files that have been compacted into
   * a higher level and are older than `l0RetentionMs`. It never deletes the
   * newest level-0 file (the live head) and preserves contiguous L0 coverage by
   * stopping at the first file that is not old enough.
   */
  async enforceL0RetentionByTime(now: number, l0RetentionMs: number): Promise<number> {
    this.assertCanWrite();
    const l0 = await this.listRemoteLevel(0);
    l0.sort((a, b) => (a.maxTXID < b.maxTXID ? -1 : a.maxTXID > b.maxTXID ? 1 : 0));
    if (l0.length === 0) return 0;

    const newest = l0[l0.length - 1];
    const coverers: FileInfo[] = [];
    for (let level = 1; level <= this.maxCompactionLevel; level++) {
      for await (const info of this.client.ltxFiles({ level, seek: 0n, useMetadata: false })) {
        coverers.push(info);
      }
    }

    const threshold = now - l0RetentionMs;
    const deletedFiles: FileInfo[] = [];
    for (const info of l0) {
      if (info === newest) continue; // never delete the live head
      if (info.createdAt > threshold) break; // preserve contiguous L0 coverage
      if (this.isCoveredByHigherLevel(info, coverers)) deletedFiles.push(info);
    }
    await this.deleteFiles(deletedFiles);
    return deletedFiles.length;
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

/** Returns true if `client` implements the legacy v0.3.x generation read API. */
function isGenerationClientV3(client: ReplicaClient): boolean {
  const c = client as unknown as Record<string, unknown>;
  return (
    typeof c.generations === 'function' &&
    typeof c.snapshotsV3 === 'function' &&
    typeof c.walSegmentsV3 === 'function' &&
    typeof c.readSnapshotV3 === 'function' &&
    typeof c.readWALSegmentV3 === 'function'
  );
}
