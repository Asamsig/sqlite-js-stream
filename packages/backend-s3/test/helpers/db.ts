/**
 * Test helper: create a SQLite database in WAL mode with known rows.
 * The returned connection must be kept open while reading the `-wal` file,
 * because SQLite removes/checkpoints the WAL on clean close.
 */

import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export interface TestDB {
  path: string;
  walPath: string;
  db: DatabaseSync;
  close(): void;
}

/** Creates a WAL-mode DB and inserts one row per name in its own autocommit txn. */
export function createWALDB(names: string[]): TestDB {
  const dir = mkdtempSync(join(tmpdir(), 'sqlite-js-stream-s3-'));
  const path = join(dir, 'test.db');
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA wal_autocheckpoint=0');
  db.exec('PRAGMA synchronous=OFF');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');

  const insert = db.prepare('INSERT INTO t (name) VALUES (?)');
  for (const name of names) {
    insert.run(name);
  }

  return { path, walPath: `${path}-wal`, db, close() { db.close(); } };
}
