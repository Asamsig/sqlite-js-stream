import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';

import { FileReplicaClient, Replica, type ReaderConnection } from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

/** Wraps a read-only node:sqlite connection, recording how it is used. */
function trackingReader(path: string, calls: string[]): ReaderConnection {
  const conn = new DatabaseSync(path, { readOnly: true });
  return {
    exec(sql) {
      calls.push(`exec:${sql}`);
      conn.exec(sql);
    },
    prepare(sql) {
      calls.push(`prepare:${sql}`);
      return conn.prepare(sql);
    },
    close() {
      calls.push('close');
      conn.close();
    },
  };
}

test('openReader: the supplied connection pins sync snapshots', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const opened: string[] = [];
  const calls: string[] = [];
  const replica = new Replica(t.path, client, undefined, {
    openReader: (path) => {
      opened.push(path);
      return trackingReader(path, calls);
    },
  });

  try {
    await replica.sync();
    t.db.exec("INSERT INTO t (name) VALUES ('d'), ('e')");
    await replica.sync();
    await replica.snapshot();

    assert.deepEqual(opened, [t.path], 'reader is opened once and cached');
    assert.ok(calls.includes('exec:BEGIN'));
    assert.ok(calls.includes('exec:COMMIT'));

    const out = join(makeTempDir('restore-'), 'restored.db');
    await replica.restore({ outputPath: out });
    const restored = new DatabaseSync(out);
    const count = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 5);
    restored.close();
  } finally {
    replica.close();
    t.close();
  }

  assert.equal(calls.at(-1), 'close', 'replica.close() closes the reader');
});
