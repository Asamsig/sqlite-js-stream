import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  BufferSink,
  Compactor,
  Decoder,
  Encoder,
  HeaderFlagNoChecksum,
} from '../../src/ltx/index.ts';
import { byteEq } from '../helpers/byte.ts';

/** Builds an LTX file with the given pages at level-0, NoChecksum. */
function buildLtx(
  minTXID: bigint,
  maxTXID: bigint,
  commit: number,
  pages: Array<{ pgno: number; data: Uint8Array }>,
): Uint8Array {
  const sink = new BufferSink();
  const enc = new Encoder(sink);
  enc.encodeHeader({
    version: 3,
    flags: HeaderFlagNoChecksum,
    pageSize: 512,
    commit,
    minTXID,
    maxTXID,
    timestamp: 1000n,
    preApplyChecksum: 0n,
    walOffset: 0n,
    walSize: 0n,
    walSalt1: 0,
    walSalt2: 0,
    nodeID: 0n,
  });
  for (const p of pages) enc.encodePage({ pgno: p.pgno, flags: 0 }, p.data);
  enc.close();
  return sink.toBuffer();
}

function pageData(seed: number): Uint8Array {
  const b = new Uint8Array(512);
  for (let i = 0; i < 512; i++) b[i] = (seed + i) & 0xff;
  return b;
}

test('compactor merges a base snapshot with an incremental, later wins', () => {
  // Base: all pages 1..5 (min=1, max=1).
  const base = buildLtx(
    1n,
    1n,
    5,
    [1, 2, 3, 4, 5].map((pgno) => ({ pgno, data: pageData(10 + pgno) })),
  );
  // Incremental: pages 2 and 4 changed (min=2, max=2).
  const inc = buildLtx(
    2n,
    2n,
    5,
    [
      { pgno: 2, data: pageData(200) },
      { pgno: 4, data: pageData(400) },
    ],
  );

  const out = new BufferSink();
  const compactor = new Compactor(out, [base, inc]);
  compactor.headerFlags = HeaderFlagNoChecksum;
  compactor.compact();

  const merged = out.toBuffer();
  const dec = new Decoder(merged);
  dec.decodeHeader();
  assert.equal(dec.Header().minTXID, 1n);
  assert.equal(dec.Header().maxTXID, 2n);
  assert.equal(dec.Header().commit, 5);

  const data = new Uint8Array(512);
  const pages = new Map<number, Uint8Array>();
  for (;;) {
    const ph = dec.decodePage(data);
    if (ph === null) break;
    pages.set(ph.pgno, new Uint8Array(data));
  }
  dec.close();

  assert.ok(byteEq(pages.get(1)!, pageData(11)));
  assert.ok(byteEq(pages.get(2)!, pageData(200)));
  assert.ok(byteEq(pages.get(3)!, pageData(13)));
  assert.ok(byteEq(pages.get(4)!, pageData(400)));
  assert.ok(byteEq(pages.get(5)!, pageData(15)));
});

test('coalesced output can be decoded as a database (snapshot)', () => {
  const base = buildLtx(
    1n,
    1n,
    5,
    [1, 2, 3, 4, 5].map((pgno) => ({ pgno, data: pageData(pgno) })),
  );
  const out = new BufferSink();
  const compactor = new Compactor(out, [base]);
  compactor.headerFlags = HeaderFlagNoChecksum;
  compactor.compact();

  const dec = new Decoder(out.toBuffer());
  const db = new BufferSink();
  dec.decodeDatabaseTo(db);
  assert.equal(db.length, 5 * 512);
  const buf = db.toBuffer();
  assert.ok(byteEq(buf.subarray(0, 512), pageData(1)));
  assert.ok(byteEq(buf.subarray(4 * 512, 5 * 512), pageData(5)));
});

test('compactor rejects mismatched page sizes', () => {
  const small = (() => {
    const sink = new BufferSink();
    const enc = new Encoder(sink);
    enc.encodeHeader({
      version: 3,
      flags: HeaderFlagNoChecksum,
      pageSize: 512,
      commit: 1,
      minTXID: 1n,
      maxTXID: 1n,
      timestamp: 0n,
      preApplyChecksum: 0n,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    });
    enc.encodePage({ pgno: 1, flags: 0 }, pageData(1));
    enc.close();
    return sink.toBuffer();
  })();
  const big = (() => {
    const sink = new BufferSink();
    const enc = new Encoder(sink);
    enc.encodeHeader({
      version: 3,
      flags: HeaderFlagNoChecksum,
      pageSize: 1024,
      commit: 1,
      minTXID: 2n,
      maxTXID: 2n,
      timestamp: 0n,
      preApplyChecksum: 0n,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    });
    enc.encodePage({ pgno: 1, flags: 0 }, new Uint8Array(1024));
    enc.close();
    return sink.toBuffer();
  })();

  const out = new BufferSink();
  const compactor = new Compactor(out, [small, big]);
  assert.throws(() => compactor.compact(), /mismatched page sizes/);
});

