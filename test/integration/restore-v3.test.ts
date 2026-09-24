import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { compressFrameLz4, FileReplicaClient, Replica } from '../../src/index.ts';
import { makeTempDir } from '../helpers/make-db.ts';

const GEN = '0123456789abcdef';

async function rowCount(path: string): Promise<number> {
  const db = new DatabaseSync(path);
  const n = (db.prepare('SELECT count(*) AS n FROM t').get() as { n: number }).n;
  const ic = (db.prepare('PRAGMA integrity_check').get() as { integrity_check: string })
    .integrity_check;
  db.close();
  assert.equal(ic, 'ok');
  return n;
}

test('restore reads a legacy v0.3.x generation snapshot', async () => {
  // Build a non-WAL source DB (rollback journal) so the main file is complete.
  const dbPath = join(makeTempDir('src-'), 'src.db');
  const db = new DatabaseSync(dbPath);
  db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
  db.exec("INSERT INTO t (name) VALUES ('a'), ('b'), ('c')");
  db.close();

  // Snapshot = raw DB bytes, LZ4-frame compressed.
  const root = makeTempDir('replica-');
  const snapDir = join(root, 'generations', GEN, 'snapshots');
  await mkdir(snapDir, { recursive: true });
  const raw = new Uint8Array(await readFile(dbPath));
  await writeFile(join(snapDir, '00000001.snapshot.lz4'), compressFrameLz4(raw));

  const client = new FileReplicaClient(root);
  const replica = new Replica(dbPath, client);
  try {
    const out = join(makeTempDir('restore-'), 'restored.db');
    await replica.restore({ outputPath: out });
    assert.equal(await rowCount(out), 3);
  } finally {
    replica.close();
  }
});

test('restore applies a legacy v0.3.x WAL segment on top of a snapshot', async () => {
  // Build a WAL-mode DB, checkpoint a base (a,b), then write c without
  // checkpointing so it lives in the WAL.
  const dbPath = join(makeTempDir('src-'), 'src.db');
  const db = new DatabaseSync(dbPath);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
  db.exec("INSERT INTO t (name) VALUES ('a'), ('b')");
  db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  db.close();

  const db2 = new DatabaseSync(dbPath);
  db2.exec("INSERT INTO t (name) VALUES ('c')");
  // Read the main DB (a,b) and the WAL (c) while the writer is still open so
  // SQLite has not auto-checkpointed the -wal away.
  const baseBytes = new Uint8Array(await readFile(dbPath));
  const walBytes = new Uint8Array(await readFile(`${dbPath}-wal`));
  db2.close();

  const root = makeTempDir('replica-');
  const snapDir = join(root, 'generations', GEN, 'snapshots');
  const walDir = join(root, 'generations', GEN, 'wal');
  await mkdir(snapDir, { recursive: true });
  await mkdir(walDir, { recursive: true });
  await writeFile(join(snapDir, '00000001.snapshot.lz4'), compressFrameLz4(baseBytes));
  await writeFile(join(walDir, '00000001_00000000.wal.lz4'), compressFrameLz4(walBytes));

  const client = new FileReplicaClient(root);
  const replica = new Replica(dbPath, client);
  try {
    const out = join(makeTempDir('restore-'), 'restored.db');
    await replica.restore({ outputPath: out });
    assert.equal(await rowCount(out), 3);
  } finally {
    replica.close();
  }
});
