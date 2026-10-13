import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

import {
  CompactionLevels,
  ErrCompactionTooEarly,
  ErrNoCompaction,
  FileReplicaClient,
  Replica,
} from '../../src/index.ts';
import type { FileInfo } from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

async function countLevel(client: FileReplicaClient, level: number): Promise<FileInfo[]> {
  const out: FileInfo[] = [];
  for await (const f of client.ltxFiles({ level, seek: 0n, useMetadata: false })) out.push(f);
  return out;
}

test('compactDB respects the per-level time bucket', async () => {
  const t = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  // Use a very large bucket so two immediate calls land in the same bucket.
  replica.compactionLevels = new CompactionLevels([
    { level: 0, intervalMs: 0 },
    { level: 1, intervalMs: 600_000 },
    { level: 2, intervalMs: 1_200_000 },
    { level: 3, intervalMs: 1_800_000 },
  ]);
  try {
    await replica.sync();
    t.db.exec("INSERT INTO t (name) VALUES ('b')");
    await replica.sync();

    const info = await replica.compactDB(1, Date.now());
    assert.ok(info, 'first compaction should produce a file');

    // Same bucket -> refuse to re-compact.
    await assert.rejects(() => replica.compactDB(1, Date.now()), ErrCompactionTooEarly);
  } finally {
    replica.close();
    t.close();
  }
});

test('compactDB returns ErrNoCompaction when nothing is pending', async () => {
  const t = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  replica.compactionLevels = new CompactionLevels([
    { level: 0, intervalMs: 0 },
    { level: 1, intervalMs: 600_000 },
    { level: 2, intervalMs: 1_200_000 },
    { level: 3, intervalMs: 1_800_000 },
  ]);
  try {
    await replica.sync();
    await replica.compactDB(1, Date.now()); // L0 -> L1

    // A future bucket passes the time check; the destination already covers the
    // source, so there is nothing to compact.
    await assert.rejects(() => replica.compactDB(1, Date.now() + 600_000), ErrNoCompaction);
  } finally {
    replica.close();
    t.close();
  }
});

test('compactSnapshot refuses to re-snapshot within the same bucket', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    await replica.sync();
    await replica.snapshot();

    // Re-snapshotting immediately lands in the same bucket -> too early.
    await assert.rejects(() => replica.compactSnapshot(Date.now(), 300_000), ErrCompactionTooEarly);
  } finally {
    replica.close();
    t.close();
  }
});

test('snapshot retention is time-based and keeps the newest snapshot', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    await replica.sync();
    await replica.snapshot(); // snapshot A
    await sleep(80);
    t.db.exec("INSERT INTO t (name) VALUES ('c')");
    await replica.sync();
    await replica.snapshot(); // snapshot B (newer)

    // Anything older than 50ms is deleted; B is retained, A is removed.
    const result = await replica.enforceRetention({ snapshotRetentionMs: 50 });
    assert.equal(result.snapshotsDeleted, 1);
    const snapshots = await countLevel(client, 9);
    assert.equal(snapshots.length, 1, 'only the newest snapshot remains');
  } finally {
    replica.close();
    t.close();
  }
});

test('L0 time retention deletes compacted L0 once it ages out, keeping the head', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    await replica.sync(); // L0 (1..1)
    t.db.exec("INSERT INTO t (name) VALUES ('c')");
    await replica.sync(); // L0 (2..2)
    await replica.compact({ dstLevel: 1 }); // L0 covered by L1 (1..2)

    // Wait for the files to age past the threshold, then enforce.
    await sleep(120);
    const deleted = await replica.enforceL0RetentionByTime(Date.now(), 50);
    assert.equal(deleted, 1, 'the older, compacted L0 file is deleted');

    const l0 = await countLevel(client, 0);
    assert.equal(l0.length, 1, 'the live head L0 is retained');
    assert.equal(l0[0].maxTXID, 2n);
  } finally {
    replica.close();
    t.close();
  }
});

test('run() compacts per level using time-bucketed scheduling', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const ac = new AbortController();
  const outPath = join(makeTempDir('restore-'), 's.db');
  const levels = new CompactionLevels([
    { level: 0, intervalMs: 0 },
    { level: 1, intervalMs: 30 },
    { level: 2, intervalMs: 60 },
    { level: 3, intervalMs: 120 },
  ]);

  const runPromise = replica.run({ intervalMs: 10, compactionLevels: levels, signal: ac.signal });
  try {

    for (let i = 0; i < 5; i++) {
      t.db.exec("INSERT INTO t (name) VALUES ('x')");
      await sleep(30);
    }

    // Wait for all rows to be replicated, then for a higher-level file to appear.
    await waitForRestoredRows(replica, outPath, 7, 8000);
    const deadline = Date.now() + 8000;
    for (;;) {
      let anyCompacted = 0;
      for (let level = 1; level <= 3; level++) {
        anyCompacted += (await countLevel(client, level)).length;
      }
      if (anyCompacted >= 1) break;
      if (Date.now() > deadline) throw new Error('timed out waiting for compaction');
      await sleep(100);
    }

    ac.abort();
    await runPromise;

    let compacted = 0;
    for (let level = 1; level <= 3; level++) compacted += (await countLevel(client, level)).length;
    assert.ok(compacted >= 1, 'per-level scheduling produced a higher-level file');
  } finally {
    // Always stop the run loop, or a failed assertion leaves it (and the test
    // process) running forever.
    ac.abort();
    await runPromise;
    replica.close();
    t.close();
  }
});

async function waitForRestoredRows(
  replica: Replica,
  outPath: string,
  expected: number,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let last = 'no restore attempted';
  for (;;) {
    // The run loop is live: before the first sync lands the replica is empty
    // (NoMatchingBackupError), and compaction/retention may change the file set
    // mid-restore. Treat any failure as "not there yet" and retry.
    try {
      await replica.restore({ outputPath: outPath });
      const d = new DatabaseSync(outPath);
      const n = (d.prepare('SELECT count(*) AS n FROM t').get() as { n: number }).n;
      const ic = (d.prepare('PRAGMA integrity_check').get() as { integrity_check: string })
        .integrity_check;
      d.close();
      if (n === expected && ic === 'ok') return;
      last = `got ${n} rows, integrity_check=${ic}`;
    } catch (err) {
      last = `restore failed: ${(err as Error).message}`;
    }
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${expected} rows (${last})`);
    await sleep(100);
  }
}
