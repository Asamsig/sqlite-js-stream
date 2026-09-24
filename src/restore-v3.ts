/**
 * v0.3.x generation-based backup restore (legacy read support).
 *
 * Litestream v0.3.x stored backups as a directory of *generations*, each with
 * an LZ4-frame snapshot of the raw SQLite database plus LZ4-frame WAL segments:
 *
 *   {root}/generations/{generation}/snapshots/{index:08x}.snapshot.lz4
 *   {root}/generations/{generation}/wal/{index:08x}_{offset:08x}.wal.lz4
 *
 * This module can restore a database from such a backup. Modern litestream
 * writes LTX files instead, but it still *reads* these legacy generations, so
 * we mirror that capability here (and expose byte-level helpers for addon
 * backends to list/open them). The LZ4 frame decompression is pure JS
 * (`lz4js`), so it interoperates with the Go `pierrec/lz4` frame writer.
 */

import { createWriteStream } from 'node:fs';
import { open, rename, rm, mkdir } from 'node:fs/promises';
import nodePath from 'node:path';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { DatabaseSync } from 'node:sqlite';

import { decompressFrameLz4 } from './ltx/index.ts';

/** Metadata about a v0.3.x snapshot. */
export interface SnapshotInfoV3 {
  generation: string;
  index: number;
  size: number;
  createdAt: number; // ms since epoch
}

/** Metadata about a v0.3.x WAL segment. */
export interface WALSegmentInfoV3 {
  generation: string;
  index: number;
  offset: number;
  size: number;
  createdAt: number; // ms since epoch
}

/**
 * GenerationClientV3 is implemented by backends that can read legacy v0.3.x
 * generation backups. Implementations return raw `.lz4` bytes; the restore
 * helper decompresses them.
 */
export interface GenerationClientV3 {
  /** Generation IDs in ascending order (empty if no v0.3.x backups). */
  generations(): Promise<string[]>;
  /** Snapshots for a generation, sorted by index ascending. */
  snapshotsV3(generation: string): Promise<SnapshotInfoV3[]>;
  /** WAL segments for a generation, sorted by (index, offset) ascending. */
  walSegmentsV3(generation: string): Promise<WALSegmentInfoV3[]>;
  /** Raw (compressed) bytes of a snapshot `.lz4` file. */
  readSnapshotV3(generation: string, index: number): Promise<Uint8Array>;
  /** Raw (compressed) bytes of a WAL segment `.lz4` file. */
  readWALSegmentV3(generation: string, index: number, offset: number): Promise<Uint8Array>;
}

const SNAPSHOT_RE = /^([0-9a-f]{8})\.snapshot\.lz4$/;
const WAL_SEGMENT_RE = /^([0-9a-f]{8})_([0-9a-f]{8,16})\.wal\.lz4$/;

/** Formats a v0.3.x snapshot filename. */
export function formatSnapshotFilenameV3(index: number): string {
  return `${index.toString(16).padStart(8, '0')}.snapshot.lz4`;
}

/** Parses a v0.3.x snapshot filename, returning its index. */
export function parseSnapshotFilenameV3(name: string): number {
  const m = SNAPSHOT_RE.exec(name);
  if (m === null) throw new Error(`invalid v0.3.x snapshot filename: ${name}`);
  return parseInt(m[1], 16);
}

/** Formats a v0.3.x WAL segment filename. */
export function formatWALSegmentFilenameV3(index: number, offset: number): string {
  return `${index.toString(16).padStart(8, '0')}_${offset.toString(16).padStart(8, '0')}.wal.lz4`;
}

/** Parses a v0.3.x WAL segment filename, returning the index and offset. */
export function parseWALSegmentFilenameV3(name: string): { index: number; offset: number } {
  const m = WAL_SEGMENT_RE.exec(name);
  if (m === null) throw new Error(`invalid v0.3.x WAL segment filename: ${name}`);
  return { index: parseInt(m[1], 16), offset: parseInt(m[2], 16) };
}

/** Returns true if `s` is a valid 16-hex-digit generation ID. */
export function isGenerationIDV3(s: string): boolean {
  return /^[0-9a-f]{16}$/.test(s);
}

/** Restores a database from the best available v0.3.x generation snapshot. */
export async function restoreV3(
  client: GenerationClientV3,
  outputPath: string,
  timestamp?: Date,
): Promise<void> {
  const generations = await client.generations();
  if (generations.length === 0) throw new Error('no snapshots found');

  const snapshots: SnapshotInfoV3[] = [];
  for (const gen of generations) {
    const snaps = await client.snapshotsV3(gen);
    for (const s of snaps) snapshots.push(s);
  }
  if (snapshots.length === 0) throw new Error('no snapshots found');
  snapshots.sort((a, b) => a.createdAt - b.createdAt);

  let selected = snapshots[snapshots.length - 1];
  if (timestamp) {
    const ts = timestamp.getTime();
    for (let i = snapshots.length - 1; i >= 0; i--) {
      if (snapshots[i].createdAt <= ts) {
        selected = snapshots[i];
        break;
      }
    }
  }

  let segments = await client.walSegmentsV3(selected.generation);
  segments = segments.filter((s) => s.index >= selected.index);
  if (timestamp) {
    const ts = timestamp.getTime();
    segments = segments.filter((s) => s.createdAt <= ts);
  }
  segments.sort((a, b) => a.index - b.index || a.offset - b.offset);

  await mkdir(nodePath.dirname(outputPath), { recursive: true });
  const tmpPath = `${outputPath}.tmp`;
  try {
    await writeSnapshot(client, selected, tmpPath);
    await applyWALSegmentsV3(client, selected.generation, selected.index, segments, tmpPath);
    await rename(tmpPath, outputPath);
  } catch (err) {
    await rm(tmpPath, { force: true }).catch(() => {});
    throw err;
  }
}

/** Downloads and decompresses a v0.3.x snapshot directly to `destPath`. */
async function writeSnapshot(
  client: GenerationClientV3,
  snapshot: SnapshotInfoV3,
  destPath: string,
): Promise<void> {
  const raw = await client.readSnapshotV3(snapshot.generation, snapshot.index);
  const data = decompressFrameLz4(raw);
  await pipeline(Readable.from([data]), createWriteStream(destPath));
}

/**
 * Reconstructs `dbPath-wal` from the WAL segments and checkpoints it into the
 * database. Segments with offset 0 start a new WAL file (so each generation
 * index is a separate WAL that is applied and truncated in order).
 */
async function applyWALSegmentsV3(
  client: GenerationClientV3,
  generation: string,
  snapshotIndex: number,
  segments: WALSegmentInfoV3[],
  dbPath: string,
): Promise<void> {
  if (segments.length === 0) return;

  const walPath = `${dbPath}-wal`;
  let fh: Awaited<ReturnType<typeof open>> | null = null;
  let expectedIndex = snapshotIndex;
  let offset = 0;

  const applyLast = async () => {
    if (fh !== null) {
      await fh.close();
      fh = null;
      checkpointV3(dbPath);
    }
  };

  for (const seg of segments) {
    if (seg.offset === 0) {
      await applyLast();
      if (seg.index !== expectedIndex) {
        throw new Error(
          `missing WAL index: expected ${expectedIndex}/0, got ${seg.index}/${seg.offset}`,
        );
      }
      offset = 0;
      fh = await open(walPath, 'w');
      expectedIndex++;
    } else if (seg.offset !== offset) {
      throw new Error(
        `missing WAL segment: expected ${seg.index}/${offset}, got ${seg.index}/${seg.offset}`,
      );
    }

    const data = decompressFrameLz4(await client.readWALSegmentV3(generation, seg.index, seg.offset));
    await fh!.write(data);
    offset += data.length;
  }

  await applyLast();
}

/** Runs a TRUNCATE checkpoint so the reconstructed WAL is folded into the DB. */
function checkpointV3(dbPath: string): void {
  const db = new DatabaseSync(dbPath);
  try {
    db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  } finally {
    db.close();
  }
}
