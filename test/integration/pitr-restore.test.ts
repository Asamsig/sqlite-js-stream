import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';

import {
  FileReplicaClient,
  InvalidArgumentError,
  NoMatchingBackupError,
  Replica,
  RestoreGapError,
} from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

/** Counts rows in a restored (standalone) SQLite database. */
function countRows(path: string): number {
  const db = new DatabaseSync(path);
  try {
    const r = db.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    return r.n;
  } finally {
    db.close();
  }
}

/** Reads the highest `createdAt` (LTX header timestamp) across level-0 files. */
async function latestCreatedAt(client: FileReplicaClient): Promise<number> {
  let max = 0;
  for await (const f of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
    if (f.createdAt > max) max = f.createdAt;
  }
  return max;
}

test('pitr: restore to a specific txID boundary', async () => {
  const t = createWALDB(['a', 'b', 'c']); // txID 1 -> 3 rows
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    await replica.sync(); // txID 1
    t.db.exec("INSERT INTO t (name) VALUES ('d'), ('e')");
    await replica.sync(); // txID 2 -> 5 rows
    t.db.exec("INSERT INTO t (name) VALUES ('f')");
    await replica.sync(); // txID 3 -> 6 rows

    const outTxid1 = join(makeTempDir('restore-'), 'r1.db');
    await replica.restore({ outputPath: outTxid1, txID: 1n });
    assert.equal(countRows(outTxid1), 3, 'restore at txID=1 has first batch only');

    const outTxid2 = join(makeTempDir('restore-'), 'r2.db');
    await replica.restore({ outputPath: outTxid2, txID: 2n });
    assert.equal(countRows(outTxid2), 5, 'restore at txID=2 has first two batches');

    const outLatest = join(makeTempDir('restore-'), 'r3.db');
    await replica.restore({ outputPath: outLatest }); // txID=0 => latest
    assert.equal(countRows(outLatest), 6, 'latest restore has all rows');
  } finally {
    t.close();
  }
});

test('pitr: restore to a timestamp picks the latest backup at/before it', async () => {
  const t = createWALDB(['a', 'b', 'c']); // txID 1 -> 3 rows
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    await replica.sync(); // txID 1, createdAt = t1
    // Guarantee distinct timestamps between the two syncs.
    await new Promise((r) => setTimeout(r, 5));
    const t1 = await latestCreatedAt(client);
    assert.ok(t1 > 0);

    t.db.exec("INSERT INTO t (name) VALUES ('d'), ('e')");
    await replica.sync(); // txID 2, createdAt = t2 > t1
    const t2 = await latestCreatedAt(client);
    assert.ok(t2 > t1, 'expected strictly increasing timestamps');

    // Target just after the first backup but before the second.
    const outEarly = join(makeTempDir('restore-'), 'early.db');
    await replica.restore({ outputPath: outEarly, timestamp: new Date(t1 + 1) });
    assert.equal(countRows(outEarly), 3, 'timestamp before second sync keeps first batch only');

    // Target after the second backup => latest.
    const outLate = join(makeTempDir('restore-'), 'late.db');
    await replica.restore({ outputPath: outLate, timestamp: new Date(t2 + 1000) });
    assert.equal(countRows(outLate), 5, 'timestamp after second sync includes all rows');
  } finally {
    t.close();
  }
});

test('pitr: timestamp before the earliest backup throws NoMatchingBackupError', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    await replica.sync();
    const first = await latestCreatedAt(client);
    await assert.rejects(
      replica.restore({ outputPath: join(makeTempDir('restore-'), 'out.db'), timestamp: new Date(first - 10_000) }),
      NoMatchingBackupError,
    );
  } finally {
    t.close();
  }
});

test('pitr: txID beyond the latest backup throws NoMatchingBackupError', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    await replica.sync();
    await assert.rejects(
      replica.restore({ outputPath: join(makeTempDir('restore-'), 'out.db'), txID: 999n }),
      NoMatchingBackupError,
    );
  } finally {
    t.close();
  }
});

test('pitr: txID and timestamp are mutually exclusive', async () => {
  const t = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    await replica.sync();
    await assert.rejects(
      replica.restore({
        outputPath: join(makeTempDir('restore-'), 'out.db'),
        txID: 1n,
        timestamp: new Date(),
      }),
      InvalidArgumentError,
    );
  } finally {
    t.close();
  }
});

test('pitr: negative txID is rejected', async () => {
  const t = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    await replica.sync();
    await assert.rejects(
      replica.restore({ outputPath: join(makeTempDir('restore-'), 'out.db'), txID: -1n }),
      InvalidArgumentError,
    );
  } finally {
    t.close();
  }
});

test('pitr: latest restore raises RestoreGapError on non-contiguous files', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    await replica.sync(); // 1..1
    t.db.exec("INSERT INTO t (name) VALUES ('c')");
    await replica.sync(); // 2..2
    t.db.exec("INSERT INTO t (name) VALUES ('d')");
    await replica.sync(); // 3..3

    // Delete the level-0 file for TXID 2 to leave a gap: 1..1, 3..3.
    const toDelete = [];
    for await (const f of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
      if (f.maxTXID === 2n) toDelete.push(f);
    }
    assert.equal(toDelete.length, 1);
    await client.deleteLtxFiles(toDelete);

    await assert.rejects(
      replica.restore({ outputPath: join(makeTempDir('restore-'), 'out.db') }),
      RestoreGapError,
    );
  } finally {
    t.close();
  }
});
