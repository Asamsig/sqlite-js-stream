import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

import { FileReplicaClient, Replica, calcRestorePlan } from '../../src/index.ts';
import type { FileInfo } from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

async function countLevel(client: FileReplicaClient, level: number): Promise<FileInfo[]> {
  const out: FileInfo[] = [];
  for await (const f of client.ltxFiles({ level, seek: 0n, useMetadata: false })) out.push(f);
  return out;
}

async function assertRestored(replica: Replica, outPath: string, expected: number) {
  await replica.restore({ outputPath: outPath });
  const d = new DatabaseSync(outPath);
  const n = (d.prepare('SELECT count(*) AS n FROM t').get() as { n: number }).n;
  const ic = (d.prepare('PRAGMA integrity_check').get() as { integrity_check: string })
    .integrity_check;
  d.close();
  assert.equal(n, expected, 'row count after restore');
  assert.equal(ic, 'ok', 'integrity_check after restore');
}

test('compact moves L0 files into a level-1 file and GC can prune the source', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    await replica.sync(); // L0 (1..1)
    t.db.exec("INSERT INTO t (name) VALUES ('c')");
    await replica.sync(); // L0 (2..2)
    t.db.exec("INSERT INTO t (name) VALUES ('d')");
    await replica.sync(); // L0 (3..3)
    t.db.exec("INSERT INTO t (name) VALUES ('e')");
    await replica.sync(); // L0 (4..4)

    assert.equal((await countLevel(client, 0)).length, 4);

    const info = await replica.compact({ dstLevel: 1 });
    assert.ok(info, 'compaction should produce a file');
    assert.equal(info.level, 1);
    assert.equal(info.minTXID, 1n);
    assert.equal(info.maxTXID, 4n);

    const l1 = await countLevel(client, 1);
    assert.equal(l1.length, 1);
    assert.equal(l1[0].minTXID, 1n);
    assert.equal(l1[0].maxTXID, 4n);

    // Even before GC, the restore plan should prefer the compacted level-1 file.
    const plan = await calcRestorePlan(client, 0n);
    assert.equal(plan[0].level, 1);
    assert.equal(plan[0].maxTXID, 4n);

    // GC removes the L0 source files now that L1 covers them.
    const gc = await replica.enforceRetention({ l0RetentionMs: 0 });
    assert.ok(gc.filesDeleted >= 4);
    assert.equal((await countLevel(client, 0)).length, 0);
    assert.equal((await countLevel(client, 1)).length, 1);

    await assertRestored(replica, join(makeTempDir('restore-'), 'c.db'), 5);
  } finally {
    replica.close();
    t.close();
  }
});

test('enforceRetention prunes old snapshots and keeps the newest', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    await replica.sync();
    await replica.snapshot(); // snapshot at tx2 (maxTXID = 2)

    t.db.exec("INSERT INTO t (name) VALUES ('c')");
    await replica.sync();
    await replica.snapshot(); // snapshot at tx3

    t.db.exec("INSERT INTO t (name) VALUES ('d')");
    await replica.sync();
    await replica.snapshot(); // snapshot at tx4

    assert.equal((await countLevel(client, 9)).length, 3);

    const result = await replica.enforceRetention({ snapshotRetain: 1 });
    assert.equal(result.snapshotsDeleted, 2);

    const snapshots = await countLevel(client, 9);
    assert.equal(snapshots.length, 1);
    // The newest snapshot should have the highest maxTXID among the kept one.
    assert.equal(snapshots[0].maxTXID, 3n);

    // Restore still yields the latest state.
    await assertRestored(replica, join(makeTempDir('restore-'), 'r.db'), 4);
  } finally {
    replica.close();
    t.close();
  }
});

test('incremental sync resumes after compaction and GC without reusing TXIDs', async () => {
  const t = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    await replica.sync(); // L0 (1..1)
    t.db.exec("INSERT INTO t (name) VALUES ('b')");
    await replica.sync(); // L0 (2..2)

    // Compact L0 -> L1 and prune the (now covered) level-0 files.
    await replica.compact({ dstLevel: 1 });
    await replica.enforceRetention({ l0RetentionMs: 0 });
    assert.equal((await countLevel(client, 0)).length, 0);
    assert.equal((await countLevel(client, 1)).length, 1);

    // A later write must continue the sequence (TXID 3), not reuse 1..2.
    t.db.exec("INSERT INTO t (name) VALUES ('c')");
    await replica.sync();

    const l0 = await countLevel(client, 0);
    assert.equal(l0.length, 1, 'a new level-0 file should be created');
    assert.equal(l0[0].minTXID, 3n, 'new L0 starts at TXID 3');
    assert.equal(l0[0].maxTXID, 3n);
    assert.equal(await replica.calcPos(), 3n);

    await assertRestored(replica, join(makeTempDir('restore-'), 'r.db'), 3);
  } finally {
    replica.close();
    t.close();
  }
});

test('run() schedules compaction and retention while replicating', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const ac = new AbortController();
  const outPath = join(makeTempDir('restore-'), 'w.db');

  try {
    const runPromise = replica.run({
      intervalMs: 10,
      compactionIntervalMs: 40,
      retentionIntervalMs: 80,
      retention: { snapshotRetain: 1, l0RetentionMs: 0 },
      signal: ac.signal,
    });

    // New writes land as additional L0 files over the next several ticks.
    for (let i = 0; i < 5; i++) {
      t.db.exec("INSERT INTO t (name) VALUES ('x')");
      await sleep(50);
    }

    // Wait until all rows are replicated, then let the scheduling loop
    // compact the pending L0 files and GC them (settled state).
    await waitForRestoredRows(replica, outPath, 7, 8000);
    const deadline = Date.now() + 8000;
    while ((await countLevel(client, 0)).length > 0) {
      if (Date.now() > deadline) {
        throw new Error('timed out waiting for compaction to prune level-0 files');
      }
      await sleep(150);
    }
    ac.abort();
    await runPromise;

    // Compaction should have lifted data into at least one higher level.
    let compacted = 0;
    for (let level = 1; level <= 3; level++) compacted += (await countLevel(client, level)).length;
    assert.ok(compacted >= 1, 'compaction produced files at a higher level');

    // Coverage-based GC should have pruned the compacted L0 files.
    assert.equal((await countLevel(client, 0)).length, 0, 'no leftover level-0 files after GC');
  } finally {
    replica.close();
    t.close();
  }
});

/** Restores repeatedly until `expected` rows are present (deterministic wait). */
async function waitForRestoredRows(
  replica: Replica,
  outPath: string,
  expected: number,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    await replica.restore({ outputPath: outPath });
    const d = new DatabaseSync(outPath);
    const n = (d.prepare('SELECT count(*) AS n FROM t').get() as { n: number }).n;
    const ic = (d.prepare('PRAGMA integrity_check').get() as { integrity_check: string })
      .integrity_check;
    d.close();
    if (n === expected && ic === 'ok') return;
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${expected} rows (got ${n})`);
    }
    await sleep(150);
  }
}
