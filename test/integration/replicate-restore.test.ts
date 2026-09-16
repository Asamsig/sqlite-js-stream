import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { rm } from 'node:fs/promises';
import { join } from 'node:path';

import { DB, FileReplicaClient, Replica } from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

test('end-to-end: replicate a WAL database to a file replica and restore', async () => {
  const t = createWALDB(['a', 'b', 'c']); // connection stays open (WAL retained)
  const db = await DB.open(t.path);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(db, client);

  try {
    // First sync produces TXID=1.
    const info1 = await db.sync();
    assert.equal(info1?.minTXID, 1n);
    assert.equal(info1?.maxTXID, 1n);
    await replica.sync();

    const files1: bigint[] = [];
    for await (const f of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
      files1.push(f.maxTXID);
    }
    assert.deepEqual(files1, [1n]);

    // Insert more rows and sync again -> TXID=2.
    t.db.exec("INSERT INTO t (name) VALUES ('d'), ('e')");
    const info2 = await db.sync();
    assert.equal(info2?.minTXID, 2n);
    assert.equal(info2?.maxTXID, 2n);
    await replica.sync();

    const files2: bigint[] = [];
    for await (const f of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
      files2.push(f.maxTXID);
    }
    files2.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    assert.deepEqual(files2, [1n, 2n]);

    const status = await replica.syncStatus();
    assert.equal(status.inSync, true);
    assert.equal(status.localTXID, 2n);
    assert.equal(status.remoteTXID, 2n);

    // Restore into a fresh path and verify integrity + row count.
    const out = join(makeTempDir('restore-'), 'restored.db');
    await replica.restore({ outputPath: out });

    const restored = new DatabaseSync(out);
    const ic = restored.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
    assert.equal(ic.integrity_check, 'ok');
    const count = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 5);
    restored.close();
  } finally {
    t.close();
    db.close();
  }
});

test('ensureExists restores a missing local database', async () => {
  const t = createWALDB(['x', 'y']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  let db: DB | null = await DB.open(t.path);
  const replica = new Replica(db, client);

  try {
    await db.sync();
    await replica.sync();
    db.close();
    db = null;

    // Remove the local database file (+ wal/shm) so it is "missing".
    await rm(t.path, { force: true });
    await rm(t.walPath, { force: true });
    await rm(`${t.path}-shm`, { force: true });

    await replica.ensureExists();

    const restored = new DatabaseSync(t.path);
    const count = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 2);
    restored.close();
  } finally {
    t.close();
    db?.close();
  }
});

test('sync replicates a header-only (empty) database', async () => {
  const dir = makeTempDir('empty-');
  const path = join(dir, 'empty.db');
  const db = await DB.open(path);
  const client = new FileReplicaClient(join(dir, 'repl'));
  const replica = new Replica(db, client);
  try {
    // Even an empty SQLite DB has a header page (page_count >= 1).
    const info = await db.sync();
    assert.equal(info?.minTXID, 1n);
    await replica.sync();
    let n = 0;
    for await (const _ of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) n++;
    assert.equal(n, 1);
  } finally {
    db.close();
  }
});
