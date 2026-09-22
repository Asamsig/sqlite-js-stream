import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';

import { BufferSink, Encoder, StreamLtxReader } from '../../src/ltx/index.ts';
import { makePage, makeSnapshot, writeFileSpec } from '../helpers/make-ltx.ts';

/** Splits bytes into small chunks to exercise cross-boundary reads. */
function intoChunks(bytes: Uint8Array, size: number): Uint8Array[] {
  const out: Uint8Array[] = [];
  for (let i = 0; i < bytes.length; i += size) {
    out.push(Uint8Array.from(bytes.subarray(i, i + size)));
  }
  return out;
}

test('StreamLtxReader decodes a snapshot across chunk boundaries', async () => {
  const commit = 50;
  const pageSize = 512;
  const spec = makeSnapshot(commit, pageSize);
  const bytes = writeFileSpec(spec);

  // Feed the file through tiny chunks so every logical record is split.
  const r = new StreamLtxReader(Readable.from(intoChunks(bytes, 37)));

  await r.readHeader();
  const hdr = r.Header();
  assert.equal(hdr.pageSize, pageSize);
  assert.equal(hdr.commit, commit);
  assert.equal(hdr.minTXID, 1n);
  assert.equal(hdr.maxTXID, 4n);

  const data = new Uint8Array(pageSize);
  let pgno = 1;
  for (;;) {
    const ph = await r.readPage(data);
    if (ph === null) break;
    assert.equal(ph.pgno, pgno);
    assert.deepEqual(data, makePage(pgno, pageSize));
    pgno++;
  }
  assert.equal(pgno - 1, commit);
  assert.equal(r.PageN(), commit);

  // The snapshot carries a rolling post-apply checksum; verify must pass.
  await r.verify();
});

test('StreamLtxReader surfaces a file checksum mismatch', async () => {
  const spec = makeSnapshot(10, 512);
  const bytes = writeFileSpec(spec);

  // Corrupt the stored file checksum (last 8 bytes of the trailer).
  const corrupted = Uint8Array.from(bytes);
  corrupted[corrupted.length - 1] ^= 0xff;

  const r = new StreamLtxReader(Readable.from(intoChunks(corrupted, 64)));
  await r.readHeader();
  const data = new Uint8Array(512);
  for (;;) {
    if ((await r.readPage(data)) === null) break;
  }
  await assert.rejects(() => r.verify(), /checksum/);
});

test('StreamLtxReader decodes an incremental (non-snapshot) file', async () => {
  const pageSize = 512;
  // A non-snapshot LTX: minTXID != 1, sparse pages (1 and 3).
  const sink = new BufferSink();
  const enc = new Encoder(sink);
  enc.encodeHeader({
    version: 3,
    flags: 0x2, // HeaderFlagNoChecksum
    pageSize,
    commit: 5,
    minTXID: 2n,
    maxTXID: 4n,
    timestamp: 2000n,
    preApplyChecksum: 0n,
    walOffset: 0n,
    walSize: 0n,
    walSalt1: 0,
    walSalt2: 0,
    nodeID: 0n,
  });
  enc.encodePage({ pgno: 1, flags: 0 }, makePage(1, pageSize));
  enc.encodePage({ pgno: 3, flags: 0 }, makePage(3, pageSize));
  enc.close();
  const bytes = sink.toBuffer();

  const r = new StreamLtxReader(Readable.from(intoChunks(bytes, 13)));
  await r.readHeader();
  assert.equal(r.Header().minTXID, 2n);
  assert.equal(r.Header().commit, 5);

  const data = new Uint8Array(pageSize);
  const seen: number[] = [];
  for (;;) {
    const ph = await r.readPage(data);
    if (ph === null) break;
    seen.push(ph.pgno);
    assert.deepEqual(data, makePage(ph.pgno, pageSize));
  }
  assert.deepEqual(seen, [1, 3]);
  await r.verify();
});
