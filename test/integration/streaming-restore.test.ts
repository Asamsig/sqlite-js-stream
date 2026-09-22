import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';

import { FileReplicaClient, Replica } from '../../src/index.ts';
import { makeTempDir } from '../helpers/make-db.ts';

/** Number of rows in the large database (each ~4 KB page of data). */
const BIG_ROWS = 2000;

interface BigDB {
  path: string;
  db: DatabaseSync;
  close(): void;
}

/** Builds a WAL-mode database with a large, deterministic table. */
function buildLargeDB(rows: number): BigDB {
  const dir = makeTempDir('stream-');
  const path = join(dir, 'big.db');
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA wal_autocheckpoint=0');
  db.exec('PRAGMA synchronous=OFF');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)');

  const insert = db.prepare('INSERT INTO t (id, data) VALUES (?, ?)');
  const blob = new Uint8Array(4096);
  for (let i = 0; i < blob.length; i++) blob[i] = (i * 31) & 0xff;
  for (let i = 1; i <= rows; i++) {
    const b = Uint8Array.from(blob);
    b[0] = i & 0xff;
    b[1] = (i >> 8) & 0xff;
    insert.run(i, b);
  }

  return { path, db, close: () => db.close() };
}

/** SHA-256 over every row's id + blob, in id order. */
function blobHash(db: DatabaseSync): string {
  const h = createHash('sha256');
  const rows = db.prepare('SELECT id, data FROM t ORDER BY id').all() as Array<{
    id: number;
    data: Uint8Array | null;
  }>;
  for (const r of rows) {
    h.update(String(r.id));
    h.update(r.data ?? Buffer.alloc(0));
  }
  return h.digest('hex');
}

test('restore streams a large database across a snapshot base + incremental overlay', async () => {
  const src = buildLargeDB(BIG_ROWS);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(src.path, client);
  try {
    // Base L0 sync, then a level-9 snapshot becomes the restore base.
    await replica.sync();
    await replica.snapshot();

    // Modify many existing pages and grow the DB so a later L0 must overlay
    // changed + new pages onto the snapshot.
    src.db.exec('UPDATE t SET data = randomblob(4096) WHERE id % 3 = 0');
    src.db.exec(
      'INSERT INTO t (id, data) SELECT id + 100000, randomblob(4096) FROM t WHERE id <= 100',
    );
    await replica.sync();

    const out = join(makeTempDir('restore-'), 'big.db');
    await replica.restore({ outputPath: out });

    const restored = new DatabaseSync(out);
    try {
      const ic = restored.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
      assert.equal(ic.integrity_check, 'ok');

      // The streamed restore must reproduce the exact page contents.
      assert.equal(blobHash(src.db), blobHash(restored));

      const count = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
      assert.ok(count.n > BIG_ROWS, `expected >${BIG_ROWS} rows, got ${count.n}`);
    } finally {
      restored.close();
    }
  } finally {
    replica.close();
    src.close();
  }
});

test('restore streams a single large snapshot with no incremental files', async () => {
  const src = buildLargeDB(BIG_ROWS);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(src.path, client);
  try {
    await replica.sync();

    const out = join(makeTempDir('restore-'), 'big.db');
    await replica.restore({ outputPath: out });

    const restored = new DatabaseSync(out);
    try {
      const ic = restored.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
      assert.equal(ic.integrity_check, 'ok');
      assert.equal(blobHash(src.db), blobHash(restored));
    } finally {
      restored.close();
    }
  } finally {
    replica.close();
    src.close();
  }
});
