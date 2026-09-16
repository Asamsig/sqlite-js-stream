import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  BufferSink,
  ChecksumFlag,
  Decoder,
  HeaderFlagNoChecksum,
  lockPgno,
  type Header,
  type PageHeader,
} from '../../src/ltx/index.ts';
import { makeSnapshot, writeFileSpec, type FileSpec } from '../helpers/make-ltx.ts';
import { byteEq } from '../helpers/byte.ts';

test('snapshot encoder/decoder round-trips pages and trailer', () => {
  const spec = makeSnapshot(6, 512);
  const buf = writeFileSpec(spec);

  const dec = new Decoder(buf);
  dec.decodeHeader();
  assert.equal(dec.Header().commit, 6);
  assert.ok(dec.Header().minTXID === 1n);

  const data = new Uint8Array(512);
  const seen: PageHeader[] = [];
  for (;;) {
    const ph = dec.decodePage(data);
    if (ph === null) break;
    seen.push(ph);
    assert.ok(byteEq(data, spec.pages[ph.pgno - 1].data), `page ${ph.pgno}`);
  }
  assert.equal(seen.length, 6);
  dec.close();

  assert.equal(dec.Trailer().postApplyChecksum, spec.trailer.postApplyChecksum);
  assert.equal(dec.Trailer().fileChecksum, spec.trailer.fileChecksum);
});

test('decoder verifies a complete snapshot file', () => {
  const spec = makeSnapshot(6, 512);
  const buf = writeFileSpec(spec);
  const dec = new Decoder(buf);
  dec.verify();
  assert.equal(dec.PageN(), 6);
  assert.ok(dec.PageIndex()!.size === 6);
});

test('decoder detects a changed file checksum field', () => {
  const spec = makeSnapshot(6, 512);
  const buf = writeFileSpec(spec);
  // Overwrite the trailing file-checksum field (last 8 bytes) with a wrong value.
  const wrong = ChecksumFlag | 0x1234n;
  new DataView(buf.buffer, buf.byteOffset, buf.byteLength).setBigUint64(
    buf.length - 8,
    wrong,
    false,
  );
  const dec = new Decoder(buf);
  // Header still validates; the checksum computed over the content now differs.
  assert.throws(() => dec.verify(), /file checksum mismatch/);
});

test('decodeDatabaseTo reconstructs the full database', () => {
  const spec = makeSnapshot(6, 512);
  const buf = writeFileSpec(spec);
  const dec = new Decoder(buf);

  const sink = new BufferSink();
  dec.decodeDatabaseTo(sink);

  const expected = new Uint8Array(6 * 512);
  for (let i = 0; i < 6; i++) expected.set(spec.pages[i].data, i * 512);
  assert.ok(byteEq(sink.toBuffer(), expected));
});

test('non-snapshot file with HeaderFlagNoChecksum round-trips', () => {
  const header: Header = {
    version: 3,
    flags: HeaderFlagNoChecksum,
    pageSize: 512,
    commit: 16,
    minTXID: 2n,
    maxTXID: 3n,
    timestamp: 1000n,
    preApplyChecksum: 0n,
    walOffset: 0n,
    walSize: 0n,
    walSalt1: 0,
    walSalt2: 0,
    nodeID: 0n,
  };
  const spec: FileSpec = {
    header,
    pages: [2, 5, 9, 16].map((pgno) => ({
      header: { pgno, flags: 0 },
      data: makePageBytes(pgno),
    })),
    trailer: { postApplyChecksum: 0n, fileChecksum: 0n },
  };
  const buf = writeFileSpec(spec);
  const dec = new Decoder(buf);
  dec.verify();
  assert.equal(dec.Trailer().postApplyChecksum, 0n);
  assert.notEqual(dec.Trailer().fileChecksum, 0n);
});

test('the lock page is rejected by the encoder', () => {
  const spec = makeSnapshot(6, 512);
  const lockPg = lockPgno(512);
  spec.header.commit = lockPg; // ensure the page is in range
  spec.pages.push({ header: { pgno: lockPg, flags: 0 }, data: new Uint8Array(512) });
  assert.throws(() => writeFileSpec(spec), /cannot encode lock page/);
});

function makePageBytes(seed: number): Uint8Array {
  const b = new Uint8Array(512);
  for (let i = 0; i < 512; i++) b[i] = (seed + (i % 7)) & 0xff;
  return b;
}

