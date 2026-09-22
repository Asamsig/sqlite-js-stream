import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  Replica,
  marshalHeader,
  newReplicaClientFromURL,
  type Header,
} from 'sqlite-js-stream';

import {
  S3ReplicaClient,
  type S3ReplicaClientOptions,
} from '../src/index.ts';
import { MemoryS3 } from './helpers/memory-s3.ts';
import { createWALDB } from './helpers/db.ts';

function concatBytes(chunks: readonly Uint8Array[]): Uint8Array {
  let total = 0;
  for (const c of chunks) total += c.length;
  const out = new Uint8Array(total);
  let o = 0;
  for (const c of chunks) {
    out.set(c, o);
    o += c.length;
  }
  return out;
}

function makeLtxBody(size = 3): { body: Uint8Array; timestamp: number } {
  const timestamp = 1700000000000;
  const hdr: Header = {
    version: 3,
    flags: 0,
    pageSize: 4096,
    commit: 1,
    minTXID: 1n,
    maxTXID: 1n,
    timestamp: BigInt(timestamp),
    preApplyChecksum: 0n,
    walOffset: 0n,
    walSize: 0n,
    walSalt1: 0,
    walSalt2: 0,
    nodeID: 0n,
  };
  // Fill the page-payload tail with an increasing byte pattern so byte-range
  // reads can be asserted against known values.
  const tail = Uint8Array.from({ length: size }, (_, i) => i);
  const body = concatBytes([marshalHeader(hdr), tail]);
  return { body, timestamp };
}

async function* iter(data: Uint8Array): AsyncIterable<Uint8Array> {
  yield data;
}

/** Yields `data` in fixed-size slices, so multipart uploads span parts. */
async function* iterChunks(data: Uint8Array, chunkSize: number): AsyncIterable<Uint8Array> {
  for (let i = 0; i < data.length; i += chunkSize) {
    yield data.subarray(i, i + chunkSize);
  }
}

function makeS3(opts: Partial<S3ReplicaClientOptions> & { bucket: string }): {
  client: S3ReplicaClient;
  s3: MemoryS3;
} {
  const s3 = new MemoryS3();
  return { client: new S3ReplicaClient({ ...opts, client: s3 }), s3 };
}

test('type() reports s3', async () => {
  const { client } = makeS3({ bucket: 'b' });
  assert.equal(client.type(), 's3');
});

test('prefix normalization strips leading/trailing slashes', () => {
  const { client } = makeS3({ bucket: 'b', prefix: '/a/b/' });
  assert.equal(client.prefix, 'a/b');
});

test('writeLtxFile uploads an object with ltx-timestamp metadata', async () => {
  const { client, s3 } = makeS3({ bucket: 'b', prefix: 'backup' });
  const { body, timestamp } = makeLtxBody();
  const info = await client.writeLtxFile(
    { level: 0, minTXID: 1n, maxTXID: 1n },
    iter(body),
  );

  assert.equal(info.level, 0);
  assert.equal(info.minTXID, 1n);
  assert.equal(info.maxTXID, 1n);
  assert.equal(info.size, body.length);
  assert.equal(info.createdAt, timestamp);

  const stored = s3.objects.get('b/backup/ltx/0/0000000000000001-0000000000000001.ltx');
  assert.ok(stored, 'object should be stored');
  assert.equal(stored.metadata['ltx-timestamp'], String(timestamp));
  assert.equal(stored.body.length, body.length);
});

test('ltxFiles lists uploaded objects, skipping non-conforming keys', async () => {
  const { client, s3 } = makeS3({ bucket: 'b' });
  const { body } = makeLtxBody();
  s3.objects.set('b/ltx/0/0000000000000001-0000000000000001.ltx', {
    body: Buffer.from(body),
    metadata: {},
    lastModified: new Date(1700000000000),
  });
  s3.objects.set('b/ltx/0/0000000000000002-0000000000000002.ltx', {
    body: Buffer.from(body),
    metadata: {},
    lastModified: new Date(1700000005000),
  });
  s3.objects.set('b/ltx/0/not-a-ltx-file.txt', {
    body: Buffer.from('x'),
    metadata: {},
    lastModified: new Date(),
  });

  const infos = [];
  for await (const info of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
    infos.push(info);
  }
  assert.equal(infos.length, 2);
  assert.equal(infos[0].maxTXID, 1n);
  assert.equal(infos[1].maxTXID, 2n);
  assert.equal(infos[0].size, body.length);
});

test('ltxFiles honors seek by skipping earlier TXIDs', async () => {
  const { client, s3 } = makeS3({ bucket: 'b' });
  const { body } = makeLtxBody();
  s3.objects.set('b/ltx/0/0000000000000001-0000000000000001.ltx', {
    body: Buffer.from(body),
    metadata: {},
    lastModified: new Date(),
  });
  const infos = [];
  for await (const info of client.ltxFiles({ level: 0, seek: 2n, useMetadata: false })) {
    infos.push(info);
  }
  assert.equal(infos.length, 0);
});

test('openLtxFile reads a full object', async () => {
  const { client, s3 } = makeS3({ bucket: 'b' });
  const { body } = makeLtxBody(5);
  s3.objects.set('b/ltx/0/0000000000000001-0000000000000001.ltx', {
    body: Buffer.from(body),
    metadata: {},
    lastModified: new Date(),
  });
  const stream = await client.openLtxFile({
    level: 0,
    minTXID: 1n,
    maxTXID: 1n,
    offset: 0,
    size: 0,
  });
  const buf = new Uint8Array(await collect(stream));
  assert.deepEqual(buf, body);
});

test('openLtxFile reads a byte range', async () => {
  const { client, s3 } = makeS3({ bucket: 'b' });
  const { body } = makeLtxBody(5);
  s3.objects.set('b/ltx/0/0000000000000001-0000000000000001.ltx', {
    body: Buffer.from(body),
    metadata: {},
    lastModified: new Date(),
  });
  const stream = await client.openLtxFile({
    level: 0,
    minTXID: 1n,
    maxTXID: 1n,
    offset: 100,
    size: 3,
  });
  const buf = new Uint8Array(await collect(stream));
  assert.deepEqual([...buf], [0, 1, 2]);
});

test('openLtxFile returns empty stream when offset >= size', async () => {
  const { client, s3 } = makeS3({ bucket: 'b' });
  const { body } = makeLtxBody(5);
  s3.objects.set('b/ltx/0/0000000000000001-0000000000000001.ltx', {
    body: Buffer.from(body),
    metadata: {},
    lastModified: new Date(),
  });
  const stream = await client.openLtxFile({
    level: 0,
    minTXID: 1n,
    maxTXID: 1n,
    offset: 1000,
    size: 10,
  });
  const buf = await collect(stream);
  assert.equal(buf.length, 0);
});

test('openLtxFile throws for a missing object', async () => {
  const { client } = makeS3({ bucket: 'b' });
  await assert.rejects(
    client.openLtxFile({
      level: 0,
      minTXID: 99n,
      maxTXID: 99n,
      offset: 0,
      size: 0,
    }),
    /open ltx file/,
  );
});

test('deleteLtxFiles removes only the requested files', async () => {
  const { client, s3 } = makeS3({ bucket: 'b' });
  s3.objects.set('b/ltx/0/0000000000000001-0000000000000001.ltx', {
    body: Buffer.from('a'),
    metadata: {},
    lastModified: new Date(),
  });
  s3.objects.set('b/ltx/0/0000000000000002-0000000000000002.ltx', {
    body: Buffer.from('b'),
    metadata: {},
    lastModified: new Date(),
  });
  await client.deleteLtxFiles([
    { level: 0, minTXID: 1n, maxTXID: 1n, size: 1, createdAt: 0 },
  ]);
  assert.equal(s3.objects.size, 1);
  assert.ok(s3.objects.has('b/ltx/0/0000000000000002-0000000000000002.ltx'));
});

test('deleteAll removes every object under the prefix', async () => {
  const { client, s3 } = makeS3({ bucket: 'b', prefix: 'backup' });
  s3.objects.set('b/backup/ltx/0/0000000000000001-0000000000000001.ltx', {
    body: Buffer.from('a'),
    metadata: {},
    lastModified: new Date(),
  });
  s3.objects.set('b/backup/ltx/9/0000000000000001-0000000000000001.ltx', {
    body: Buffer.from('b'),
    metadata: {},
    lastModified: new Date(),
  });
  // An object under a different prefix must survive.
  s3.objects.set('b/other/ltx/0/0000000000000001-0000000000000001.ltx', {
    body: Buffer.from('c'),
    metadata: {},
    lastModified: new Date(),
  });
  await client.deleteAll();
  assert.equal(s3.objects.size, 1);
  assert.ok(s3.objects.has('b/other/ltx/0/0000000000000001-0000000000000001.ltx'));
});

test('newReplicaClientFromURL parses s3 URLs and registers the scheme', () => {
  // Importing the addon registers 's3'; make sure a URL round-trips.
  const rc = newReplicaClientFromURL('s3://mybucket/some/prefix?region=eu-west-1');
  assert.ok(rc instanceof S3ReplicaClient);
  const s3 = rc as S3ReplicaClient;
  assert.equal(s3.bucket, 'mybucket');
  assert.equal(s3.prefix, 'some/prefix');
  assert.equal(s3.type(), 's3');
});

test('writeLtxFile streams a large object via multipart upload', async () => {
  const { client, s3 } = makeS3({ bucket: 'b', prefix: 'backup' });
  // Larger than the 5 MiB multipart threshold, fed in 1 MiB chunks so the
  // upload spans multiple parts (5 MiB part + remainder).
  const { body, timestamp } = makeLtxBody(5 * 1024 * 1024 + 777);
  const info = await client.writeLtxFile(
    { level: 9, minTXID: 1n, maxTXID: 99n },
    iterChunks(body, 1024 * 1024),
  );

  assert.equal(info.size, body.length);
  assert.equal(info.createdAt, timestamp);
  assert.equal(s3.multiparts.size, 0, 'multipart upload should have completed');

  const stored = s3.objects.get('b/backup/ltx/9/0000000000000001-0000000000000063.ltx');
  assert.ok(stored, 'large object should be stored');
  assert.equal(stored.body.length, body.length);
  assert.equal(stored.metadata['ltx-timestamp'], String(timestamp));
  // The concatenated parts must reproduce the exact input bytes.
  assert.deepEqual(stored.body, Buffer.from(body));
});

test('integrates with Replica.sync() then restore() via S3', async () => {
  const source = createWALDB(['a', 'b', 'c', 'd', 'e', 'f']);
  const { client, s3 } = makeS3({ bucket: 'mybucket', prefix: 'db' });

  const replica = new Replica(source.path, client);
  try {
    await replica.sync();
    assert.ok(
      s3.objects.has('mybucket/db/ltx/0/0000000000000001-0000000000000001.ltx'),
      'a level-0 LTX file should be uploaded',
    );

    const outDir = mkdtempSync(join(tmpdir(), 'sqlite-js-stream-restore-'));
    const outPath = join(outDir, 'restored.db');
    await replica.restore({ outputPath: outPath });

    const r = new DatabaseSync(outPath);
    const n = (r.prepare('SELECT count(*) AS n FROM t').get() as { n: number }).n;
    const integrity = (r.prepare('PRAGMA integrity_check').get() as { integrity_check: string })
      .integrity_check;
    r.close();
    assert.equal(n, 6);
    assert.equal(integrity, 'ok');
    rmSync(outDir, { recursive: true, force: true });
  } finally {
    replica.close();
    source.close();
  }
});

async function collect(stream: AsyncIterable<Uint8Array | string>): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const c of stream) {
    chunks.push(typeof c === 'string' ? new TextEncoder().encode(c) : Uint8Array.from(c));
  }
  return concatBytes(chunks);
}
