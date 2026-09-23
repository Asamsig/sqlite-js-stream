import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';

import {
  FileReplicaClient,
  Replica,
  Store,
  calcRestorePlan,
} from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';
import type { FileInfo } from '../../src/index.ts';

async function fileCount(client: FileReplicaClient, level: number): Promise<number> {
  let n = 0;
  const files: FileInfo[] = [];
  for await (const f of client.ltxFiles({ level, seek: 0n, useMetadata: false })) {
    files.push(f);
    n++;
  }
  return n;
}

test('level-9 snapshot becomes the restore base', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    await replica.sync();
    t.db.exec("INSERT INTO t (name) VALUES ('d')");
    await replica.sync();
    t.db.exec("INSERT INTO t (name) VALUES ('e')");
    await replica.sync();

    await replica.snapshot();

    // The restore plan should start with the level-9 snapshot base.
    const plan = await calcRestorePlan(client, 0n);
    assert.ok(plan.length >= 1);
    assert.equal(plan[0].level, 9, 'first file is the snapshot');
    assert.equal(plan[0].minTXID, 1n);

    const out = join(makeTempDir('restore-'), 'snap.db');
    await replica.restore({ outputPath: out });

    const restored = new DatabaseSync(out);
    const count = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 5);
    const ic = restored.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
    assert.equal(ic.integrity_check, 'ok');
    restored.close();
  } finally {
    replica.close();
    t.close();
  }
});

test('Store opens, syncs, and closes multiple databases', async () => {
  const t1 = createWALDB(['a', 'b']);
  const t2 = createWALDB(['x', 'y', 'z']);
  const store = new Store();
  const client1 = new FileReplicaClient(makeTempDir('repl1-'));
  const client2 = new FileReplicaClient(makeTempDir('repl2-'));
  try {
    await store.openDB(t1.path, client1);
    await store.openDB(t2.path, client2);

    await store.syncAll();

    const r1 = store.replicas()[0];
    const r2 = store.replicas()[1];
    assert.equal((await r1.syncStatus()).inSync, true);
    assert.equal((await r2.syncStatus()).inSync, true);

    let n1 = 0;
    for await (const _ of client1.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) n1++;
    let n2 = 0;
    for await (const _ of client2.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) n2++;
    assert.ok(n1 >= 1);
    assert.ok(n2 >= 1);

    const out = join(makeTempDir('restore-'), 's1.db');
    await r1.restore({ outputPath: out });
    const restored = new DatabaseSync(out);
    const c = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(c.n, 2);
    restored.close();
  } finally {
    t1.close();
    t2.close();
    store.close();
  }
});

test('Store compacts and enforces retention across databases', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const store = new Store();
  try {
    const replica = await store.openDB(t.path, client);
    await store.syncAll();
    t.db.exec("INSERT INTO t (name) VALUES ('d')");
    await store.syncAll();

    // Compact pending L0 files into a higher level, then GC the sources.
    await store.compactAll();
    await store.enforceRetentionAll({ l0RetentionMs: 0 });

    // Only the live-head level-0 file is retained after GC (like litestream).
    const l0 = await fileCount(client, 0);
    assert.equal(l0, 1, 'only the live-head level-0 file remains after compaction + GC');

    const out = join(makeTempDir('restore-'), 'store.db');
    await replica.restore({ outputPath: out });
    const restored = new DatabaseSync(out);
    const c = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(c.n, 4);
    restored.close();
  } finally {
    t.close();
    store.close();
  }
});
