import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { existsSync } from 'node:fs';
import { join } from 'node:path';

import { Replica, FileReplicaClient } from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

/**
 * The replication layer pins a WAL snapshot with a read-only transaction while
 * it reads the DB + WAL files. These tests assert (a) that pin actually blocks
 * `wal_checkpoint(TRUNCATE)`, and (b) that sync/snapshot remain consistent even
 * when a writer attempts a truncating checkpoint concurrently.
 */

test('a pinned read transaction blocks WAL truncation', () => {
  const dir = makeTempDir('pin-');
  const path = join(dir, 't.db');
  const writer = new DatabaseSync(path);
  writer.exec('PRAGMA journal_mode=WAL');
  writer.exec('PRAGMA wal_autocheckpoint=0');
  writer.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
  writer.exec("INSERT INTO t (name) VALUES ('a'), ('b'), ('c')");
  const wal = `${path}-wal`;
  assert.ok(existsSync(wal), 'WAL should exist after writes');

  // Exactly what the library does: BEGIN then a read that touches the database.
  const reader = new DatabaseSync(path, { readOnly: true });
  reader.exec('BEGIN');
  reader.prepare('SELECT count(*) FROM sqlite_master').get();

  const whilePinned = writer.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get() as {
    busy: number;
  };
  assert.equal(whilePinned.busy, 1, 'checkpoint should be blocked while the snapshot is pinned');
  assert.ok(existsSync(wal), 'WAL should not be truncated while pinned');

  reader.exec('COMMIT');
  assert.equal(
    (writer.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get() as { busy: number }).busy,
    0,
    'checkpoint should succeed after the snapshot is released',
  );

  writer.close();
  reader.close();
});

test('sync captures uncheckpointed WAL growth pages', async () => {
  // The connection stays open and autocheckpoint is off, so the extra rows live
  // only in the WAL. Without a pinned reader, the DB file header page_count is
  // stale and the snapshot would drop those pages.
  const t = createWALDB(['a', 'b', 'c']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    t.db.exec("INSERT INTO t (name) VALUES ('d'), ('e'), ('f'), ('g')");

    await replica.sync();

    const out = join(makeTempDir('restore-'), 'db.db');
    await replica.restore({ outputPath: out });
    const r = new DatabaseSync(out);
    const count = r.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 7);
    const ic = r.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
    assert.equal(ic.integrity_check, 'ok');
    r.close();
  } finally {
    replica.close();
    t.close();
  }
});

test('sync stays valid under a concurrent truncating checkpoint', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    for (let i = 0; i < 5; i++) {
      t.db.exec(`INSERT INTO t (name) VALUES ('row${i}')`);

      const syncP = replica.sync();
      const checkpointP = (async () => {
        for (let k = 0; k < 3; k++) {
          try {
            t.db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
          } catch {
            /* checkpoint may fail transiently if the pin is held */
          }
          await new Promise((r) => setTimeout(r, 1));
        }
      })();
      await Promise.all([syncP, checkpointP]);
    }

    const out = join(makeTempDir('restore-'), 'db.db');
    await replica.restore({ outputPath: out });
    const r = new DatabaseSync(out);
    const count = r.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 8);
    const ic = r.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
    assert.equal(ic.integrity_check, 'ok');
    r.close();
  } finally {
    replica.close();
    t.close();
  }
});
