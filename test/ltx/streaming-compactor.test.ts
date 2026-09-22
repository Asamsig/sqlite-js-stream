import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  BufferSink,
  Decoder,
  HeaderFlagNoChecksum,
  StreamingCompactor,
  StreamLtxReader,
  type Header,
} from '../../src/ltx/index.ts';
import {
  makeIncrementalSpec,
  makePage,
  writeFileSpec,
} from '../helpers/make-ltx.ts';

/** Decodes an LTX byte buffer, returning the header and each page. */
function decode(buf: Uint8Array): { header: Header; pages: Array<{ pgno: number; data: Uint8Array }> } {
  const d = new Decoder(buf);
  d.decodeHeader();
  const header = d.Header();
  const pages: Array<{ pgno: number; data: Uint8Array }> = [];
  const page = new Uint8Array(header.pageSize);
  for (;;) {
    const h = d.decodePage(page);
    if (h === null) break;
    pages.push({ pgno: h.pgno, data: Uint8Array.from(page) });
  }
  return { header, pages };
}

/** Adds a snapshot of a single supplied chunk; yields whole-file async iterable. */
async function toAsyncIterable(buf: Uint8Array): Promise<AsyncIterable<Uint8Array>> {
  return (async function* () {
    // Split across small chunk boundaries to exercise the streaming reader.
    for (let i = 0; i < buf.length; i += 7) {
      yield buf.subarray(i, i + 7);
    }
  })();
}

test('merges incremental files, later page wins', async () => {
  const a = makeIncrementalSpec({
    minTXID: 2n,
    maxTXID: 2n,
    commit: 10,
    pages: [
      { header: { pgno: 1, flags: 0 }, data: makePage(1, 512) },
      { header: { pgno: 5, flags: 0 }, data: makePage(5, 512) },
    ],
  });
  const b = makeIncrementalSpec({
    minTXID: 3n,
    maxTXID: 3n,
    commit: 10,
    pages: [
      { header: { pgno: 5, flags: 0 }, data: makePage(50, 512) }, // overrides a's pgno 5
      { header: { pgno: 9, flags: 0 }, data: makePage(9, 512) },
    ],
  });

  const r1 = new StreamLtxReader(await toAsyncIterable(writeFileSpec(a)));
  const r2 = new StreamLtxReader(await toAsyncIterable(writeFileSpec(b)));

  const sink = new BufferSink();
  const comp = new StreamingCompactor(sink, [r1, r2]);
  await comp.compact();

  const { header, pages } = decode(sink.toBuffer());
  assert.equal(header.minTXID, 2n);
  assert.equal(header.maxTXID, 3n);
  assert.equal(header.commit, 10);
  assert.equal(header.flags & HeaderFlagNoChecksum, HeaderFlagNoChecksum);
  assert.equal(pages.length, 3);
  assert.equal(pages[0].pgno, 1);
  assert.deepEqual(pages[0].data, makePage(1, 512));
  assert.equal(pages[1].pgno, 5);
  assert.deepEqual(pages[1].data, makePage(50, 512)); // later file wins
  assert.equal(pages[2].pgno, 9);
  assert.deepEqual(pages[2].data, makePage(9, 512));
});

test('rejects non-contiguous TXIDs between inputs', async () => {
  const a = makeIncrementalSpec({
    minTXID: 2n,
    maxTXID: 3n,
    commit: 10,
    pages: [{ header: { pgno: 1, flags: 0 }, data: makePage(1, 512) }],
  });
  const b = makeIncrementalSpec({
    minTXID: 5n, // gap: 3 -> 5 is not contiguous
    maxTXID: 6n,
    commit: 10,
    pages: [{ header: { pgno: 1, flags: 0 }, data: makePage(2, 512) }],
  });

  const sink = new BufferSink();
  const comp = new StreamingCompactor(
    sink,
    [new StreamLtxReader(await toAsyncIterable(writeFileSpec(a))), new StreamLtxReader(await toAsyncIterable(writeFileSpec(b)))],
  );
  await assert.rejects(comp.compact(), /non-contiguous transaction ids/);
});

test('round-trips the compacted output through a StreamLtxReader', async () => {
  const a = makeIncrementalSpec({
    minTXID: 2n,
    maxTXID: 2n,
    commit: 4,
    pages: [
      { header: { pgno: 2, flags: 0 }, data: makePage(100, 512) },
      { header: { pgno: 3, flags: 0 }, data: makePage(3, 512) },
    ],
  });

  const sink = new BufferSink();
  const comp = new StreamingCompactor(
    sink,
    [new StreamLtxReader(await toAsyncIterable(writeFileSpec(a)))],
  );
  await comp.compact();

  const out = sink.toBuffer();
  const reader = new StreamLtxReader(await toAsyncIterable(out));
  await reader.readHeader();
  const hdr = reader.Header();
  assert.equal(hdr.minTXID, 2n);
  const page = new Uint8Array(512);
  const seen: Array<{ pgno: number; data: Uint8Array }> = [];
  for (;;) {
    const h = await reader.readPage(page);
    if (h === null) break;
    seen.push({ pgno: h.pgno, data: Uint8Array.from(page) });
  }
  await reader.verify();
  assert.deepEqual(seen.map((p) => p.pgno), [2, 3]);
  assert.deepEqual(seen[0].data, makePage(100, 512));
});
