import { test } from 'node:test';
import assert from 'node:assert/strict';
import { stat } from 'node:fs/promises';
import { join } from 'node:path';

import {
  FileReplicaClient,
  fetchLtxHeader,
  fetchPageIndex,
  newReplicaClientFromURL,
} from '../../../src/index.ts';
import { readAll } from '../../../src/util/stream.ts';
import { makeSnapshot, writeFileSpec } from '../../helpers/make-ltx.ts';
import { byteEq } from '../../helpers/byte.ts';
import { makeTempDir } from '../../helpers/make-db.ts';

function chunks(data: Uint8Array, size = 257): AsyncIterable<Uint8Array> {
  return (async function* () {
    for (let i = 0; i < data.length; i += size) {
      yield data.subarray(i, i + size);
    }
  })();
}

test('write then read back an LTX file (atomic, no .tmp left)', async () => {
  const dir = makeTempDir('replica-');
  const client = new FileReplicaClient(dir);
  const spec = makeSnapshot(5, 512);
  const buf = writeFileSpec(spec);

  const info = await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 4n }, chunks(buf));
  assert.equal(info.size, buf.length);
  assert.equal(info.level, 0);
  assert.equal(info.minTXID, 1n);
  assert.equal(info.maxTXID, 4n);

  const rc = await client.openLtxFile({ level: 0, minTXID: 1n, maxTXID: 4n, offset: 0, size: 0 });
  const back = await readAll(rc);
  assert.ok(byteEq(back, buf));

  // No temp file should remain after a successful rename.
  await assert.rejects(() => stat(`${dir}/ltx/0/0000000000000001-0000000000000004.ltx.tmp`), /ENOENT/);
});

test('ltxFiles lists files sorted and skips non-conforming names', async () => {
  const dir = makeTempDir('replica-');
  const client = new FileReplicaClient(dir);
  const spec = makeSnapshot(2, 512);
  const buf = writeFileSpec(spec);

  await client.writeLtxFile({ level: 0, minTXID: 5n, maxTXID: 6n }, chunks(buf));
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 4n }, chunks(buf));
  await client.writeLtxFile({ level: 0, minTXID: 2n, maxTXID: 2n }, chunks(buf));

  // Drop a non-conforming file into the level dir.
  const { writeFile } = await import('node:fs/promises');
  await writeFile(join(dir, 'ltx', '0', 'notes.txt'), 'not an ltx file');

  const infos: { minTXID: bigint }[] = [];
  for await (const i of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
    infos.push({ minTXID: i.minTXID });
  }
  assert.deepEqual(infos.map((i) => i.minTXID), [1n, 2n, 5n]);

  const after: bigint[] = [];
  for await (const i of client.ltxFiles({ level: 0, seek: 3n, useMetadata: false })) {
    after.push(i.minTXID);
  }
  assert.deepEqual(after, [5n]);
});

test('openLtxFile honours offset/size range reads', async () => {
  const dir = makeTempDir('replica-');
  const client = new FileReplicaClient(dir);
  const spec = makeSnapshot(3, 512);
  const buf = writeFileSpec(spec);

  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 3n }, chunks(buf));

  // Read bytes [100, 160).
  const rc = await client.openLtxFile({ level: 0, minTXID: 1n, maxTXID: 3n, offset: 100, size: 60 });
  const part = await readAll(rc);
  assert.ok(byteEq(part, buf.subarray(100, 100 + 60)));

  // Reading a range that extends past EOF returns just the remainder.
  const rc2 = await client.openLtxFile({ level: 0, minTXID: 1n, maxTXID: 3n, offset: buf.length - 10, size: 100 });
  const tail = await readAll(rc2);
  assert.ok(byteEq(tail, buf.subarray(buf.length - 10)));
});

test('openLtxFile throws for a missing file', async () => {
  const dir = makeTempDir('replica-');
  const client = new FileReplicaClient(dir);
  await assert.rejects(
    () => client.openLtxFile({ level: 0, minTXID: 1n, maxTXID: 9n, offset: 0, size: 0 }),
    /ltx file/,
  );
});

test('fetchLtxHeader and fetchPageIndex work on a written file', async () => {
  const dir = makeTempDir('replica-');
  const client = new FileReplicaClient(dir);
  const spec = makeSnapshot(5, 512);
  const buf = writeFileSpec(spec);
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 4n }, chunks(buf));

  const info = {
    level: 0,
    minTXID: 1n,
    maxTXID: 4n,
    size: buf.length,
    createdAt: 0,
  };
  const hdr = await fetchLtxHeader(client, info);
  assert.equal(hdr.commit, 5);
  assert.equal(hdr.pageSize, 512);
  assert.equal(hdr.minTXID, 1n);
  assert.equal(hdr.maxTXID, 4n);

  const index = await fetchPageIndex(client, info);
  assert.equal(index.size, 5);
  assert.ok(index.has(1));
  assert.ok(index.has(5));
});

test('writeLtxFile preserves the header timestamp as file mtime', async () => {
  const dir = makeTempDir('replica-');
  const client = new FileReplicaClient(dir);
  const spec = makeSnapshot(3, 512);
  const buf = writeFileSpec(spec);
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 3n }, chunks(buf));

  const p = join(dir, 'ltx', '0', '0000000000000001-0000000000000003.ltx');
  const st = await stat(p);
  // Header timestamp is 1000ms; allow filesystem resolution slack.
  assert.ok(Math.abs(st.mtimeMs - 1000) < 2000, `mtimeMs=${st.mtimeMs}`);
});

test('deleteLtxFiles and deleteAll', async () => {
  const dir = makeTempDir('replica-');
  const client = new FileReplicaClient(dir);
  const spec = makeSnapshot(2, 512);
  const buf = writeFileSpec(spec);
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 2n }, chunks(buf));

  await client.deleteLtxFiles([{ level: 0, minTXID: 1n, maxTXID: 2n, size: buf.length, createdAt: 0 }]);
  await assert.rejects(() => stat(join(dir, 'ltx', '0', '0000000000000001-0000000000000002.ltx')), /ENOENT/);

  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 2n }, chunks(buf));
  await client.deleteAll();
  await assert.rejects(() => stat(dir), /ENOENT/);
});

test('newReplicaClientFromURL parses file:// URLs', () => {
  const client = newReplicaClientFromURL('file:///data/repl') as FileReplicaClient;
  assert.equal(client.type(), 'file');
  assert.ok(client instanceof FileReplicaClient);
  // URL pathname is /data/repl.
  assert.ok(client.path().endsWith('data/repl') || client.path() === '/data/repl');
});

test('unsupported replica URL scheme is rejected', () => {
  assert.throws(() => newReplicaClientFromURL('s3://bucket/key'), /unsupported replica URL scheme/);
});

