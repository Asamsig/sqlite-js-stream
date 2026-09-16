/**
 * Test helper: create a SQLite database in WAL mode with known rows.
 * The returned connection must be kept open while reading the `-wal` file,
 * because SQLite removes/checkpoints the WAL on clean close.
 */

import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export interface TestDB {
  path: string;
  walPath: string;
  db: DatabaseSync;
  /** Number of pages in the database (from PRAGMA page_count). */
  pageCount(): number;
  close(): void;
}

/**
 * Creates a WAL-mode database and inserts one row per name, each in its own
 * autocommit transaction (so the WAL contains one commit frame per row).
 */
export function createWALDB(names: string[]): TestDB {
  const dir = mkdtempSync(join(tmpdir(), 'sqlite-js-stream-'));
  const path = join(dir, 'test.db');
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA wal_autocheckpoint=0');
  db.exec('PRAGMA synchronous=OFF');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');

  const insert = db.prepare('INSERT INTO t (name) VALUES (?)');
  for (const name of names) {
    insert.run(name); // autocommit
  }

  return {
    path,
    walPath: `${path}-wal`,
    db,
    pageCount() {
      const r = db.prepare('PRAGMA page_count').get() as { page_count: number };
      return r.page_count;
    },
    close() {
      db.close();
    },
  };
}

/** Creates a unique temporary directory path (used when a dir is needed). */
export function makeTempDir(prefix = 'sqlite-js-stream-'): string {
  const dir = join(tmpdir(), prefix + Math.random().toString(36).slice(2));
  mkdirSync(dir, { recursive: true });
  return dir;
}
