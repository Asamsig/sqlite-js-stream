import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { rm } from 'node:fs/promises';
import nodePath from 'node:path';

import { FileLeaser } from '../../src/backends/file/leaser.ts';
import { FileReplicaClient } from '../../src/backends/file/replica-client.ts';
import { ErrLeaseNotHeld, LeaseExistsError } from '../../src/leaser.ts';
import { Replica } from '../../src/replica.ts';
import { makeTempDir, createWALDB } from '../helpers/make-db.ts';

describe('FileLeaser', () => {
  test('acquire → held → renew → release → reacquire', async () => {
    const dir = makeTempDir('lease-');
    const leaser = new FileLeaser(dir);

    const lease = await leaser.acquireLease();
    assert.equal(lease.generation, 1);
    assert.ok(!lease.isExpired());
    assert.ok(lease.etag);

    // Held by us → a second acquire fails.
    await assert.rejects(leaser.acquireLease(), LeaseExistsError);

    // Renew extends but keeps the generation; use the renewed lease henceforth.
    const renewed = await leaser.renewLease(lease);
    assert.equal(renewed.generation, 1);
    assert.ok(!renewed.isExpired());

    await leaser.releaseLease(renewed);

    // Released → the object is gone, so a fresh acquire starts at generation 1.
    const lease2 = await leaser.acquireLease();
    assert.equal(lease2.generation, 1);

    await leaser.releaseLease(lease2);
    await rm(dir, { recursive: true, force: true });
  });

  test('renew/release after losing the lease throw ErrLeaseNotHeld', async () => {
    const dir = makeTempDir('lease-');
    const leaser = new FileLeaser(dir);

    const lease = await leaser.acquireLease();

    // Simulate another host taking the lease by overwriting the lock file.
    const { writeFile } = await import('node:fs/promises');
    await writeFile(
      nodePath.join(dir, 'lock.json'),
      JSON.stringify({
        generation: 1,
        expires_at: new Date(Date.now() + 60_000).toISOString(),
        owner: 'other-host',
      }),
    );

    await assert.rejects(leaser.renewLease(lease), ErrLeaseNotHeld);
    await assert.rejects(leaser.releaseLease(lease), ErrLeaseNotHeld);

    await rm(dir, { recursive: true, force: true });
  });

  test('acquiring an expired lease succeeds with an incremented generation', async () => {
    const dir = makeTempDir('lease-');
    const leaser = new FileLeaser(dir);

    await leaser.acquireLease();
    // Force-expire by writing a lease with a past expiry.
    const expired = new FileLeaser(dir);
    // Overwrite the file directly so its expiry is in the past.
    const { writeFile } = await import('node:fs/promises');
    const lockPath = nodePath.join(dir, 'lock.json');
    await writeFile(
      lockPath,
      JSON.stringify({ generation: 1, expires_at: new Date(Date.now() - 1000).toISOString(), owner: 'x' }),
    );

    const lease2 = await leaser.acquireLease();
    assert.equal(lease2.generation, 2);
    assert.ok(!lease2.isExpired());

    await leaser.releaseLease(lease2);
    void expired;
    await rm(dir, { recursive: true, force: true });
  });
});

describe('Replica.run() with a leaser (primary/follower)', () => {
  test('one replica is primary and the other follows; takeover on release', async () => {
    const dir = makeTempDir('lease-run-');
    const db = createWALDB(['a', 'b', 'c']);
    const dbPath = db.path;

    const client = new FileReplicaClient(dir);
    await client.init();

    // Use two independent leasers on the same lock path.
    const leaserA = new FileLeaser(dir);
    const leaserB = new FileLeaser(dir);

    const replicaA = new Replica(dbPath, client);
    replicaA.leaser = leaserA;
    const replicaB = new Replica(dbPath, client);
    replicaB.leaser = leaserB;

    // A starts first → becomes primary.
    const acA = new AbortController();
    const runA = replicaA.run({ intervalMs: 20, signal: acA.signal });
    await waitFor(() => replicaA.isPrimary());
    assert.ok(replicaA.isPrimary());

    // B starts while A holds the lease → follower.
    const acB = new AbortController();
    const runB = replicaB.run({ intervalMs: 20, signal: acB.signal });
    await waitFor(() => !replicaB.isPrimary(), 2000);
    assert.ok(!replicaB.isPrimary());

    // A follower must not be able to write directly.
    await assert.rejects(replicaB.sync(), ErrLeaseNotHeld);

    // Stop the primary → it releases the lease → B becomes primary.
    acA.abort();
    await runA;
    await waitFor(() => replicaB.isPrimary(), 3000);
    assert.ok(replicaB.isPrimary());

    acB.abort();
    await runB;
    db.close();
    await rm(dir, { recursive: true, force: true });
  });
});

/** Polls `predicate` until true or the timeout (ms) elapses. */
async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error('timeout waiting for condition');
}
