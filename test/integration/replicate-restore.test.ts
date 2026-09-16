import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { rm } from 'node:fs/promises';
import { join } from 'node:path';

import { FileReplicaClient, Replica } from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

test('end-to-end: replicate a WAL database to a file replica and restore', async () => {
  const t = createWALDB(['a', 'b', 'c']); // connection stays open (WAL retained)
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    // First sync produces TXID=1 (local write + upload).
    const info1 = await replica.sync();
    assert.equal(info1?.minTXID, 1n);
    assert.equal(info1?.maxTXID, 1n);

    const files1: bigint[] = [];
    for await (const f of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
      files1.push(f.maxTXID);
    }
    assert.deepEqual(files1, [1n]);

    // Insert more rows and sync again -> TXID=2.
    t.db.exec("INSERT INTO t (name) VALUES ('d'), ('e')");
    const info2 = await replica.sync();
    assert.equal(info2?.minTXID, 2n);
    assert.equal(info2?.maxTXID, 2n);

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
    replica.close();
    t.close();
  }
});

test('ensureExists restores a missing local database', async () => {
  const t = createWALDB(['x', 'y']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    await replica.sync();

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
    replica.close();
    t.close();
  }
});

test('sync replicates a header-only (empty) database', async () => {
  const dir = makeTempDir('empty-');
  const path = join(dir, 'empty.db');

  // The library is driver-agnostic and never creates the database itself, so
  // create an (empty) SQLite DB here with the application's own driver.
  const conn = new DatabaseSync(path);
  conn.exec('PRAGMA journal_mode=WAL');
  conn.close();

  const client = new FileReplicaClient(join(dir, 'repl'));
  const replica = new Replica(path, client);
  try {
    // Even an empty SQLite DB has a header page (page_count >= 1).
    const info = await replica.sync();
    assert.equal(info?.minTXID, 1n);
    let n = 0;
    for await (const _ of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) n++;
    assert.equal(n, 1);
  } finally {
    replica.close();
  }
});
