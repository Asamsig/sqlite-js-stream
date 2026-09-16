import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';

import {
  openDatabase,
  FileReplicaClient,
  Replica,
  Store,
  calcRestorePlan,
} from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

test('level-9 snapshot becomes the restore base', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  const db = await openDatabase(t.path);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(db, client);
  try {
    await db.sync();
    t.db.exec("INSERT INTO t (name) VALUES ('d')");
    await db.sync();
    t.db.exec("INSERT INTO t (name) VALUES ('e')");
    await db.sync();

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
    t.close();
    db.close();
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

    const r1 = store.replicas()[0].replica;
    const r2 = store.replicas()[1].replica;
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
