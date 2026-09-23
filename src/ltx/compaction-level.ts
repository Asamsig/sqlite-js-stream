/**
 * Multi-level compaction configuration.
 *
 * Mirrors litestream's `compaction_level.go`. Litestream stores raw LTX files
 * at level 0 and progressively merges them into higher levels at increasing
 * time intervals. Each level is compacted on its own wall-clock bucket; a level
 * is compacted at most once per bucket so repeated writes are coalesced.
 *
 * Defaults (matching litestream):
 *   - L0: raw LTX (no interval)
 *   - L1: every 30s
 *   - L2: every 5m
 *   - L3: every 1h
 */

import { SnapshotLevel } from './constants.ts';

/** CompactionLevel describes a single compaction tier. */
export interface CompactionLevel {
  /** The numeric level. Level 0 is raw, never compacted. */
  level: number;
  /** Frequency (ms) that files are compacted from the previous level. 0 for level 0. */
  intervalMs: number;
}

/** CompactionLevels is an ordered set of non-snapshot compaction levels. */
export class CompactionLevels {
  private levels: CompactionLevel[];

  constructor(levels: CompactionLevel[]) {
    this.levels = [...levels].sort((a, b) => a.level - b.level);
    this.validate();
  }

  /** Returns the level configuration at `level`. */
  level(level: number): CompactionLevel {
    if (level === SnapshotLevel) {
      throw new Error('snapshot level has no compaction interval');
    }
    if (level < 0 || level > this.maxLevel()) {
      throw new Error(`level out of bounds: ${level}`);
    }
    return this.levels[level];
  }

  /** MaxLevel returns the highest non-snapshot compaction level. */
  maxLevel(): number {
    return this.levels.length - 1;
  }

  /** SnapshotLevel returns a pseudo level for full snapshots. */
  snapshotLevel(): CompactionLevel {
    return { level: SnapshotLevel, intervalMs: 0 };
  }

  /** PrevLevel returns the level that compacts into `level` (-1 if none). */
  prevLevel(level: number): number {
    if (level === SnapshotLevel) return this.maxLevel();
    return level - 1;
  }

  /** NextLevel returns the level that `level` compacts into (-1 at the top). */
  nextLevel(level: number): number {
    if (level === SnapshotLevel) return -1;
    if (level === this.maxLevel()) return SnapshotLevel;
    return level + 1;
  }

  /** Validate checks the levels are ordered, bounded, and have sane intervals. */
  validate(): void {
    if (this.levels.length === 0) {
      throw new Error('at least one compaction level is required');
    }
    for (let i = 0; i < this.levels.length; i++) {
      const lvl = this.levels[i];
      if (lvl.level !== i) {
        throw new Error(
          `compaction level number out of order: ${lvl.level}, expected ${i}`,
        );
      }
      if (lvl.level > SnapshotLevel - 1) {
        throw new Error(`compaction level cannot exceed ${SnapshotLevel - 1}`);
      }
      if (lvl.level === 0 && lvl.intervalMs !== 0) {
        throw new Error('cannot set interval on compaction level zero');
      }
      if (lvl.level !== 0 && lvl.intervalMs <= 0) {
        throw new Error(`interval required for level ${lvl.level}`);
      }
    }
  }

  /** IsValidLevel returns true if `level` is a valid compaction level. */
  isValidLevel(level: number): boolean {
    if (level === SnapshotLevel) return true;
    return level >= 0 && level < this.levels.length;
  }

  /** Returns a copy of the underlying levels. */
  toArray(): CompactionLevel[] {
    return this.levels.map((l) => ({ ...l }));
  }
}

/** Canonical default compaction configuration (matches litestream). */
export function defaultCompactionLevels(): CompactionLevels {
  return new CompactionLevels([
    { level: 0, intervalMs: 0 },
    { level: 1, intervalMs: 30_000 },
    { level: 2, intervalMs: 300_000 },
    { level: 3, intervalMs: 3_600_000 },
  ]);
}

/**
 * PrevCompactionAt returns the start of the current compaction bucket for
 * `intervalMs`. Compaction for a level only proceeds once per bucket, so
 * repeated syncs across a window collapse into one compacted file.
 */
export function prevCompactionAt(now: number, intervalMs: number): number {
  if (intervalMs <= 0) return now;
  return Math.floor(now / intervalMs) * intervalMs;
}

/** NextCompactionAt returns the start of the next compaction bucket. */
export function nextCompactionAt(now: number, intervalMs: number): number {
  return prevCompactionAt(now, intervalMs) + intervalMs;
}

/** Interval (ms) constants matching litestream's defaults. */
export const DefaultSnapshotIntervalMs = 24 * 60 * 60 * 1000; // 24h
export const DefaultSnapshotRetentionMs = 24 * 60 * 60 * 1000; // 24h
export const DefaultL0RetentionMs = 5 * 60 * 1000; // 5m
export const DefaultL0RetentionCheckIntervalMs = 15 * 1000; // 15s
