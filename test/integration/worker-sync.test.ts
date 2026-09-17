import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Worker } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';

import { FileReplicaClient, Replica } from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

async function waitUntil(fn: () => Promise<boolean>, timeoutMs = 8000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error('timed out waiting for condition');
}

/** Restores from the replica into `out` and returns the number of rows. */
async function restoredRowCount(out: string, clientPath: string): Promise<number | null> {
  try {
    const replica = new Replica(out, new FileReplicaClient(clientPath));
    await replica.restore({ outputPath: out });
    const r = new DatabaseSync(out);
    const n = r.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    r.close();
    return n.n;
  } catch {
    return null; // replica not ready yet (no files) or partial
  }
}

test('runs continuous replication from a worker thread', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  const clientPath = join(makeTempDir('worker-'), 'repl');

  const worker = new Worker(new URL('../helpers/continuous-worker.ts', import.meta.url));
  const started = new Promise<void>((resolve, reject) => {
    worker.on('message', (m) => {
      if (m?.type === 'started') resolve();
    });
    worker.on('error', reject);
  });

  try {
    worker.postMessage({ type: 'start', dbPath: t.path, clientPath, intervalMs: 25 });
    await started;

    // The application keeps writing on its own connection (main thread).
    t.db.exec("INSERT INTO t (name) VALUES ('d'), ('e'), ('f')");

    // The worker thread should pick up the new rows and upload them. Poll by
    // restoring: the worker may capture everything in its first sync or over a
    // few iterations, so assert on the replicated data, not a specific TXID.
    const out = join(makeTempDir('restore-'), 'db.db');
    await waitUntil(async () => (await restoredRowCount(out, clientPath)) === 6);

    // Stop the worker and wait for it to finish gracefully.
    const done = new Promise<void>((resolve) => {
      worker.on('message', (m) => {
        if (m?.type === 'done') resolve();
      });
    });
    worker.postMessage({ type: 'stop' });
    await done;

    // Final restore in the main thread and verify integrity.
    const replica = new Replica(out, new FileReplicaClient(clientPath));
    await replica.restore({ outputPath: out });
    const r = new DatabaseSync(out);
    const count = r.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 6);
    const ic = r.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
    assert.equal(ic.integrity_check, 'ok');
    r.close();
  } finally {
    await worker.terminate();
    t.close();
  }
});
