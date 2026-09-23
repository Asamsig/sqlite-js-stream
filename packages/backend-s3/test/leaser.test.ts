import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { PutObjectCommand } from '@aws-sdk/client-s3';

import { S3Leaser } from '../src/leaser.ts';
import { ErrLeaseNotHeld, LeaseExistsError } from 'sqlite-js-stream';
import { MemoryS3 } from './helpers/memory-s3.ts';

describe('S3Leaser', () => {
  test('acquire → held → renew → release → reacquire', async () => {
    const s3 = new MemoryS3();
    const leaser = new S3Leaser({ bucket: 'b', prefix: 'p', client: s3 });

    const lease = await leaser.acquireLease();
    assert.equal(lease.generation, 1);
    assert.ok(!lease.isExpired());
    assert.ok(lease.etag);

    await assert.rejects(leaser.acquireLease(), LeaseExistsError);

    const renewed = await leaser.renewLease(lease);
    assert.equal(renewed.generation, 1);
    assert.ok(!renewed.isExpired());

    await leaser.releaseLease(renewed);

    const lease2 = await leaser.acquireLease();
    assert.equal(lease2.generation, 1);
    await leaser.releaseLease(lease2);
  });

  test('a second leaser cannot acquire while held; can after release', async () => {
    const s3 = new MemoryS3();
    const a = new S3Leaser({ bucket: 'b', prefix: 'p', client: s3, owner: 'a' });
    const b = new S3Leaser({ bucket: 'b', prefix: 'p', client: s3, owner: 'b' });

    const leaseA = await a.acquireLease();
    await assert.rejects(b.acquireLease(), LeaseExistsError);

    await a.releaseLease(leaseA);
    const leaseB = await b.acquireLease();
    assert.equal(leaseB.owner, 'b');
    await b.releaseLease(leaseB);
  });

  test('renew after another host takes over throws ErrLeaseNotHeld', async () => {
    const s3 = new MemoryS3();
    const a = new S3Leaser({ bucket: 'b', prefix: 'p', client: s3, owner: 'a' });
    const b = new S3Leaser({ bucket: 'b', prefix: 'p', client: s3, owner: 'b' });

    const leaseA = await a.acquireLease();

    // Force-expire the lease object, then let `b` take it over.
    await s3.send(
      new PutObjectCommand({
        Bucket: 'b',
        Key: 'p/lock.json',
        Body: JSON.stringify({ generation: 1, expires_at: new Date(Date.now() - 1000).toISOString() }),
      }),
    );
    const leaseB = await b.acquireLease();

    await assert.rejects(a.renewLease(leaseA), ErrLeaseNotHeld);
    assert.equal(leaseB.generation, 2);
    await b.releaseLease(leaseB);
  });

  test('release is a no-op when the lease is already gone', async () => {
    const s3 = new MemoryS3();
    const leaser = new S3Leaser({ bucket: 'b', prefix: 'p', client: s3 });
    const lease = await leaser.acquireLease();
    await leaser.releaseLease(lease);
    // Second release is already released → no error.
    await leaser.releaseLease(lease);
  });
});
