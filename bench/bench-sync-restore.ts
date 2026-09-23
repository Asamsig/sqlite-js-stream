/**
 * sqlite-js-stream benchmark.
 *
 * Measures the two paths that matter for a disaster-recovery tool:
 *   1. sync()    — replicate a WAL-mode database to a file replica (LTX files).
 *   2. restore() — rebuild a database from those LTX files.
 *
 * Both report wall time, throughput, and peak process RSS (via sampling). The
 * whole pipeline is streaming, so peak memory should stay bounded by a page
 * buffer rather than scale with the database.
 *
 * Tunables (env):
 *   ROWS       rows to insert into the source DB        (default 100_000)
 *   BATCH      rows per transaction (=> LTX files)      (default 1_000)
 *   DB_ENGINE  'sync' | 'restore' | 'both'              (default 'both')
 *   SCALE      run sync at ROWS and ROWS*SCALE and compare peak RSS to
 *              demonstrate the streaming bound (off by default)
 *
 * Run:
 *   pnpm bench
 *   ROWS=200000 BATCH=1000 pnpm bench
 *   SCALE=4 pnpm bench
 */

import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { rm } from 'node:fs/promises';

import { FileReplicaClient, Replica } from '../src/index.ts';

const ROWS = Number(process.env.ROWS ?? 100_000);
const BATCH = Number(process.env.BATCH ?? 1_000);
const DB_ENGINE = (process.env.DB_ENGINE ?? 'both') as 'sync' | 'restore' | 'both';
const SCALE = Number(process.env.SCALE ?? 0);

/** Builds a WAL-mode DB with `rows` rows across `rows/batch` transactions. */
function makeSourceDB(path: string, rows: number, batch: number): DatabaseSync {
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA wal_autocheckpoint=0');
  db.exec('PRAGMA synchronous=OFF');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)');
  const ins = db.prepare('INSERT INTO t (name, value) VALUES (?, ?)');
  db.exec('BEGIN');
  for (let i = 0; i < rows; i++) {
    ins.run(`row-${i}`, Math.random());
    if ((i + 1) % batch === 0) {
      db.exec('COMMIT');
      db.exec('BEGIN');
    }
  }
  db.exec('COMMIT');
  return db;
}

/** Measures wall ms + peak RSS (sampled) around an async operation. */
async function measure(fn: () => Promise<void>): Promise<{ ms: number; peakRssMB: number }> {
  let peak = process.memoryUsage().rss;
  const sampler = setInterval(() => {
    const rss = process.memoryUsage().rss;
    if (rss > peak) peak = rss;
  }, 20);
  const t0 = process.hrtime.bigint();
  try {
    await fn();
  } finally {
    clearInterval(sampler);
  }
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  return { ms, peakRssMB: peak / (1024 * 1024) };
}

function fmt(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

/** Runs a single sync() workload, returning the measured stats + WAL bytes. */
async function runSync(rows: number): Promise<{ walBytes: number; ms: number; peakRssMB: number }> {
  const dir = mkdtempSync(join(tmpdir(), 'sqlite-js-bench-'));
  const src = join(dir, 'src.db');
  const db = makeSourceDB(src, rows, BATCH);
  const walBytes = statSync(`${src}-wal`).size;
  try {
    const replica = new Replica(src, new FileReplicaClient(join(dir, 'replica')));
    const { ms, peakRssMB } = await measure(() => replica.sync());
    return { walBytes, ms, peakRssMB };
  } finally {
    db.close();
    await rm(dir, { recursive: true, force: true }).catch(() => {});
  }
}

async function main(): Promise<void> {
  // Streaming-bound demonstration: compare peak RSS across two DB sizes.
  if (SCALE > 1) {
    console.log('sqlite-js-stream benchmark — streaming memory bound (sync())');
    console.log('  peak RSS is process-wide (includes Node baseline); the streaming');
    console.log('  invariant is that it does NOT grow with database size.\n');
    const a = await runSync(ROWS);
    const b = await runSync(ROWS * SCALE);
    console.log(`  rows=${ROWS}          WAL=${fmt(a.walBytes)}  sync=${a.ms.toFixed(1)} ms  peak RSS=${a.peakRssMB.toFixed(1)} MB`);
    console.log(`  rows=${ROWS * SCALE}     WAL=${fmt(b.walBytes)}  sync=${b.ms.toFixed(1)} ms  peak RSS=${b.peakRssMB.toFixed(1)} MB`);
    console.log(`\n  WAL grew ${(b.walBytes / a.walBytes).toFixed(1)}x; peak RSS grew ${(b.peakRssMB / a.peakRssMB).toFixed(2)}x.\n`);
    return;
  }

  const dir = mkdtempSync(join(tmpdir(), 'sqlite-js-bench-'));
  const src = join(dir, 'src.db');
  const restored = join(dir, 'restored.db');

  const db = makeSourceDB(src, ROWS, BATCH);
  const transactions = Math.ceil(ROWS / BATCH);
  const walBytes = statSync(`${src}-wal`).size;

  console.log('sqlite-js-stream benchmark');
  console.log(`  workload: rows=${ROWS}, batch=${BATCH}, transactions=${transactions}`);
  console.log(`  source WAL: ${fmt(walBytes)}, source file: ${fmt(statSync(src).size)}`);
  console.log('');

  let client: FileReplicaClient | null = null;
  let replica: Replica | null = null;
  try {
    client = new FileReplicaClient(join(dir, 'replica'));
    replica = new Replica(src, client);

    if (DB_ENGINE === 'sync' || DB_ENGINE === 'both') {
      const { ms, peakRssMB } = await measure(() => replica!.sync());
      const mbs = walBytes / (1024 * 1024) / (ms / 1000);
      console.log('sync():');
      console.log(`  wall: ${ms.toFixed(1)} ms`);
      console.log(
        `  throughput: ${mbs.toFixed(2)} MB/s (WAL), ${(ROWS / (ms / 1000)).toFixed(0)} rows/s`,
      );
      console.log(`  peak RSS: ${peakRssMB.toFixed(1)} MB`);
      console.log('');
    }

    if (DB_ENGINE === 'restore' || DB_ENGINE === 'both') {
      // Ensure LTX files exist first.
      await replica!.sync();
      const { ms, peakRssMB } = await measure(() => replica!.restore({ outputPath: restored }));
      const dbBytes = statSync(restored).size;
      const mbs = dbBytes / (1024 * 1024) / (ms / 1000);
      console.log('restore():');
      console.log(`  wall: ${ms.toFixed(1)} ms`);
      console.log(
        `  throughput: ${mbs.toFixed(2)} MB/s (restored DB), ${(ROWS / (ms / 1000)).toFixed(0)} rows/s`,
      );
      console.log(`  restored DB: ${fmt(dbBytes)}`);
      console.log(`  peak RSS: ${peakRssMB.toFixed(1)} MB`);
      console.log('');

      const check = new DatabaseSync(restored);
      try {
        const ic = check.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
        const count = check.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
        console.log(`  integrity: ${ic.integrity_check}, rows: ${count.n}`);
      } finally {
        check.close();
      }
    }
  } finally {
    replica?.close();
    db.close();
    await rm(dir, { recursive: true, force: true }).catch(() => {});
  }
}

void main();
