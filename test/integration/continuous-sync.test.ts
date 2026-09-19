import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import type { Readable } from 'node:stream';
import { join } from 'node:path';

import {
  Replica,
  FileReplicaClient,
  type ReplicaClient,
  type FileInfo,
  type LTXFilesOptions,
  type OpenLTXFileOptions,
  type WriteLTXFileOptions,
} from '../../src/index.ts';
import type { Logger } from '../../src/util/logger.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

/** A ReplicaClient that fails the first write, then delegates. */
class FlakyOnceClient implements ReplicaClient {
  private inner: FileReplicaClient;
  private failed = false;
  constructor(inner: FileReplicaClient) {
    this.inner = inner;
  }
  type(): string {
    return this.inner.type();
  }
  init(): Promise<void> {
    return this.inner.init();
  }
  ltxFiles(opts: LTXFilesOptions): AsyncIterable<FileInfo> {
    return this.inner.ltxFiles(opts);
  }
  openLtxFile(opts: OpenLTXFileOptions): Promise<Readable> {
    return this.inner.openLtxFile(opts);
  }
  async writeLtxFile(
    opts: WriteLTXFileOptions,
    body: AsyncIterable<Uint8Array | string>,
  ): Promise<FileInfo> {
    if (!this.failed) {
      this.failed = true;
      throw new Error('transient write failure');
    }
    return this.inner.writeLtxFile(opts, body);
  }
  deleteLtxFiles(infos: FileInfo[]): Promise<void> {
    return this.inner.deleteLtxFiles(infos);
  }
  deleteAll(): Promise<void> {
    return this.inner.deleteAll();
  }
  setLogger(logger: Logger): void {
    this.inner.setLogger(logger);
  }
}

/**
 * Restores the replica to `outPath` and returns the row count in `t`, or null if
 * the replica isn't ready yet (no/partial backup files). This asserts on the
 * replicated *data* rather than a TXID position: the continuous loop may capture
 * new rows on its first sync or on a later one, and both are valid outcomes, but
 * the recovered row count must be deterministic in every case.
 */
async function restoredRowCount(replica: Replica, outPath: string): Promise<number | null> {
  try {
    await replica.restore({ outputPath: outPath });
    const r = new DatabaseSync(outPath);
    const n = (r.prepare('SELECT count(*) AS n FROM t').get() as { n: number }).n;
    r.close();
    return n;
  } catch {
    return null; // replica not ready yet (no files) or a partial/corrupt read
  }
}

/** Polls until the replica recovers exactly `expected` rows, then returns. */
async function waitForRows(
  replica: Replica,
  outPath: string,
  expected: number,
  timeoutMs = 8000,
): Promise<void> {
  const start = Date.now();
  let last = 0;
  while (Date.now() - start < timeoutMs) {
    const n = await restoredRowCount(replica, outPath);
    if (n !== null) last = n;
    if (n === expected) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`timed out waiting for ${expected} rows on the replica (last=${last})`);
}

/** Restores, asserts the row count and integrity, and cleans up the output dir. */
async function assertRestored(replica: Replica, outDir: string, expected: number): Promise<void> {
  const outPath = join(outDir, 'db.db');
  await replica.restore({ outputPath: outPath });
  const r = new DatabaseSync(outPath);
  const count = r.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
  const ic = r.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
  r.close();
  assert.equal(count.n, expected);
  assert.equal(ic.integrity_check, 'ok');
}

test('run() continuously syncs new writes until aborted', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const ac = new AbortController();
  const outDir = makeTempDir('restore-');

  const runP = replica.run({ intervalMs: 25, signal: ac.signal });
  try {
    // Let the first sync run, then add more data.
    await new Promise((r) => setTimeout(r, 30));
    t.db.exec("INSERT INTO t (name) VALUES ('d'), ('e'), ('f')");

    // The continuous loop must eventually recover all 6 rows regardless of
    // whether the new rows land in the first L0 file or a later one.
    await waitForRows(replica, join(outDir, 'db.db'), 6);
  } finally {
    ac.abort();
    await runP.catch(() => {});
    replica.close();
    t.close();
  }

  await assertRestored(replica, outDir, 6);
});

test('run() keeps replicating after a transient write failure', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FlakyOnceClient(new FileReplicaClient(makeTempDir('repl-')));
  const replica = new Replica(t.path, client);
  const ac = new AbortController();
  const outDir = makeTempDir('restore-');

  const runP = replica.run({ intervalMs: 25, signal: ac.signal });
  try {
    // Add data; the first upload will fail, but the loop retries and recovers.
    await new Promise((r) => setTimeout(r, 30));
    t.db.exec("INSERT INTO t (name) VALUES ('c'), ('d')");

    // The loop must eventually recover all 4 rows, surviving the transient error.
    await waitForRows(replica, join(outDir, 'db.db'), 4);
  } finally {
    ac.abort();
    await runP.catch(() => {});
    replica.close();
    t.close();
  }

  await assertRestored(replica, outDir, 4);
});
