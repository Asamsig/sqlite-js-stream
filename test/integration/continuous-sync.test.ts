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

async function waitUntil(fn: () => Promise<boolean>, timeoutMs = 8000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error('timed out waiting for condition');
}

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

test('run() continuously syncs new writes until aborted', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const ac = new AbortController();

  const runP = replica.run({ intervalMs: 25, signal: ac.signal });
  try {
    // Let the first sync run, then add more data.
    await new Promise((r) => setTimeout(r, 30));
    t.db.exec("INSERT INTO t (name) VALUES ('d'), ('e'), ('f')");

    // The continuous loop should pick up the new rows on a later iteration.
    await waitUntil(async () => (await replica.syncStatus()).localTXID >= 2n);

    ac.abort();
    await runP;

    const out = join(makeTempDir('restore-'), 'db.db');
    await replica.restore({ outputPath: out });
    const r = new DatabaseSync(out);
    const count = r.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 6);
    const ic = r.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
    assert.equal(ic.integrity_check, 'ok');
    r.close();
  } finally {
    ac.abort();
    replica.close();
    t.close();
  }
});

test('run() keeps replicating after a transient write failure', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FlakyOnceClient(new FileReplicaClient(makeTempDir('repl-')));
  const replica = new Replica(t.path, client);
  const ac = new AbortController();

  const runP = replica.run({ intervalMs: 25, signal: ac.signal });
  try {
    // Add data; the first upload will fail, but the loop retries and recovers.
    await new Promise((r) => setTimeout(r, 30));
    t.db.exec("INSERT INTO t (name) VALUES ('c'), ('d')");

    // localTXID advances on the local write; remoteTXID >= 2 means the retry
    // eventually uploaded everything despite the transient failure.
    await waitUntil(async () => (await replica.syncStatus()).remoteTXID >= 2n);

    ac.abort();
    await runP;

    const out = join(makeTempDir('restore-'), 'db.db');
    await replica.restore({ outputPath: out });
    const r = new DatabaseSync(out);
    const count = r.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 4);
    const ic = r.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
    assert.equal(ic.integrity_check, 'ok');
    r.close();
  } finally {
    ac.abort();
    replica.close();
    t.close();
  }
});
