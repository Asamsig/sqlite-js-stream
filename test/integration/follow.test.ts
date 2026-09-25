import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

import {
  FileReplicaClient,
  FollowAheadOfLatestError,
  FollowDataGapError,
  FollowInvalidTxidError,
  FollowMissingTxidError,
  Replica,
} from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

/** Reads the value written to a follow `-txid` sidecar, or null. */
async function readTxid(path: string): Promise<bigint | null> {
  try {
    const txt = (await readFile(path, 'utf8')).trim();
    return /^[0-9a-fA-F]{1,16}$/.test(txt) ? BigInt(`0x${txt}`) : null;
  } catch {
    return null;
  }
}

/** Writes a follow `-txid` sidecar value. */
async function writeTxid(path: string, txid: bigint): Promise<void> {
  await writeFile(path, txid.toString(16).padStart(16, '0') + '\n');
}

/** Polls `predicate` until true or the timeout elapses. */
async function waitFor(predicate: () => Promise<boolean>, ms = 4000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < ms) {
    if (await predicate()) return;
    await sleep(20);
  }
  throw new Error('timed out waiting for condition');
}

test('follow: fresh restore waits for first backup, then applies new files', async () => {
  const t = createWALDB(['a', 'b']); // writer stays open (WAL retained)
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const out = join(makeTempDir('follow-'), 'db.db');
  const ac = new AbortController();

  try {
    // Start follow on an empty replica: it must wait for the first backup.
    const pending = replica.follow({ outputPath: out, intervalMs: 40, signal: ac.signal });

    await replica.sync(); // TXID 1
    await waitFor(async () => (await readTxid(`${out}-txid`)) === 1n);

    // New data appears later; follow should apply it without re-restoring.
    t.db.exec("INSERT INTO t (name) VALUES ('c'), ('d')");
    await replica.sync(); // TXID 2
    await waitFor(async () => (await readTxid(`${out}-txid`)) === 2n);

    ac.abort();
    await pending;
  } finally {
    ac.abort();
    replica.close();
    t.close();
  }

  const restored = new DatabaseSync(out);
  try {
    const ic = restored.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
    assert.equal(ic.integrity_check, 'ok');
    const count = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 4);
  } finally {
    restored.close();
  }
});

test('follow: resumes from the -txid sidecar across two runs', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const out = join(makeTempDir('follow-'), 'db.db');

  try {
    await replica.sync(); // TXID 1

    // Run 1: fresh follow restores the base and records txid 1.
    const ac1 = new AbortController();
    const p1 = replica.follow({ outputPath: out, intervalMs: 40, signal: ac1.signal });
    await waitFor(async () => (await readTxid(`${out}-txid`)) === 1n);
    ac1.abort();
    await p1;

    // Run 2: a new txid exists; a fresh follow must resume (no re-restore) and
    // advance the sidecar to 2.
    t.db.exec("INSERT INTO t (name) VALUES ('c')");
    await replica.sync(); // TXID 2

    const ac2 = new AbortController();
    const p2 = replica.follow({ outputPath: out, intervalMs: 20, signal: ac2.signal });
    await waitFor(async () => (await readTxid(`${out}-txid`)) === 2n);
    ac2.abort();
    await p2;
  } finally {
    replica.close();
    t.close();
  }

  const restored = new DatabaseSync(out);
  try {
    const count = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 3);
  } finally {
    restored.close();
  }
});

test('follow: errors when the DB exists but the -txid sidecar is missing', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const out = join(makeTempDir('follow-'), 'db.db');

  try {
    await replica.sync();
    await replica.restore({ outputPath: out }); // DB exists, no -txid sidecar
    await assert.rejects(
      replica.follow({ outputPath: out, intervalMs: 20 }),
      (err: unknown) => {
        assert.ok(err instanceof FollowMissingTxidError);
        assert.equal((err as FollowMissingTxidError).code, 'ERR_FOLLOW_DB_WITHOUT_TXID');
        return true;
      },
    );
  } finally {
    replica.close();
    t.close();
  }
});

test('follow: errors when the saved TXID is ahead of the latest snapshot', async () => {
  const t = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const out = join(makeTempDir('follow-'), 'db.db');

  try {
    await replica.sync(); // TXID 1
    await replica.restore({ outputPath: out });
    await writeTxid(`${out}-txid`, 100n); // DB claims to be far ahead of the replica
    await assert.rejects(
      replica.follow({ outputPath: out, intervalMs: 20 }),
      (err: unknown) => {
        assert.ok(err instanceof FollowAheadOfLatestError);
        assert.equal((err as FollowAheadOfLatestError).code, 'ERR_FOLLOW_AHEAD_OF_LATEST');
        return true;
      },
    );
  } finally {
    replica.close();
    t.close();
  }
});

test('follow: errors when the saved TXID is below the retained files (oldest pruned)', async () => {
  const t = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const out = join(makeTempDir('follow-'), 'db.db');

  try {
    // Produce four contiguous L0 files [1..1]..[4..4].
    await replica.sync();
    for (const name of ['b', 'c', 'd']) {
      t.db.exec(`INSERT INTO t (name) VALUES ('${name}')`);
      await replica.sync();
    }

    // Restore the DB, then delete the two oldest files to simulate retention.
    await replica.restore({ outputPath: out });
    const toDelete: import('../../src/index.ts').FileInfo[] = [];
    for await (const f of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
      if (f.minTXID <= 2n) toDelete.push(f);
    }
    await client.deleteLtxFiles(toDelete);

    // Pretend we previously resumed from TXID 1, which is now below the retained
    // files (3, 4 remain) => the next needed file is gone.
    await writeTxid(`${out}-txid`, 1n);

    await assert.rejects(
      replica.follow({ outputPath: out, intervalMs: 20 }),
      (err: unknown) => {
        assert.ok(err instanceof FollowDataGapError);
        assert.equal((err as FollowDataGapError).code, 'ERR_FOLLOW_OLDER_THAN_EARLIEST');
        return true;
      },
    );
  } finally {
    replica.close();
    t.close();
  }
});

test('follow: errors on a malformed -txid sidecar', async () => {
  const t = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const out = join(makeTempDir('follow-'), 'db.db');

  try {
    await replica.sync();
    await replica.restore({ outputPath: out });
    await writeFile(`${out}-txid`, 'not-a-txid\n');
    await assert.rejects(
      replica.follow({ outputPath: out, intervalMs: 20 }),
      (err: unknown) => {
        assert.ok(err instanceof FollowInvalidTxidError);
        assert.equal((err as FollowInvalidTxidError).code, 'ERR_FOLLOW_INVALID_TXID');
        return true;
      },
    );
  } finally {
    replica.close();
    t.close();
  }
});
