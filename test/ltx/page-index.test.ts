import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  ByteReader,
  decodePageIndex,
  encodePageIndexRecord,
  encodeUvarint,
  readUvarint,
} from '../../src/ltx/index.ts';

test('uvarint round-trips simple and large values', () => {
  for (const v of [0n, 1n, 127n, 128n, 255n, 300n, 0xffffffffn, 0xffffffffffffffffn]) {
    const enc = encodeUvarint(v);
    const r = new ByteReader(enc);
    assert.equal(readUvarint(r), v, `value ${v}`);
    assert.equal(r.pos, enc.length);
  }
});

test('uvarint encoding uses little-endian LEB128 (Go-compatible)', () => {
  // 300 = 0x12c -> [0xac, 0x02]
  const enc = encodeUvarint(300n);
  assert.deepEqual(Array.from(enc), [0xac, 0x02]);
});

test('page index record encodes as (pgno, offset, size) varints', () => {
  const rec = encodePageIndexRecord(5, 100, 10);
  const r = new ByteReader(rec);
  assert.equal(readUvarint(r), 5n);
  assert.equal(readUvarint(r), 100n);
  assert.equal(readUvarint(r), 10n);
});

test('decodePageIndex round-trips a full index with terminator + size field', () => {
  const entries = [
    { pgno: 2, offset: 100, size: 10 },
    { pgno: 5, offset: 150, size: 12 },
    { pgno: 9, offset: 200, size: 8 },
  ];

  const parts: Uint8Array[] = [];
  let bytes = 0;
  for (const e of entries) {
    const rec = encodePageIndexRecord(e.pgno, e.offset, e.size);
    parts.push(rec);
    bytes += rec.length;
  }
  const term = encodeUvarint(0n);
  parts.push(term);
  bytes += term.length;

  // size field (big-endian u64) follows.
  const sizeField = new Uint8Array(8);
  new DataView(sizeField.buffer).setBigUint64(0, BigInt(bytes));
  parts.push(sizeField);

  const buf = concatParts(parts);
  const r = new ByteReader(buf);
  const index = decodePageIndex(r, 0, 2n, 3n);

  assert.equal(index.size, 3);
  assert.deepEqual(index.get(5), { level: 0, minTXID: 2n, maxTXID: 3n, offset: 150, size: 12 });
  assert.equal(r.pos, buf.length);
});

test('decodePageIndex rejects non-ascending page numbers', () => {
  const entries = [
    { pgno: 5, offset: 100, size: 10 },
    { pgno: 2, offset: 150, size: 12 },
  ];
  const parts: Uint8Array[] = [];
  let bytes = 0;
  for (const e of entries) {
    const rec = encodePageIndexRecord(e.pgno, e.offset, e.size);
    parts.push(rec);
    bytes += rec.length;
  }
  const term = encodeUvarint(0n);
  parts.push(term);
  bytes += term.length;
  const sizeField = new Uint8Array(8);
  new DataView(sizeField.buffer).setBigUint64(0, BigInt(bytes));
  parts.push(sizeField);

  const r = new ByteReader(concatParts(parts));
  assert.throws(() => decodePageIndex(r, 0, 2n, 3n), /out of order/);
});

test('decodePageIndex rejects a mismatched size field', () => {
  const entries = [{ pgno: 2, offset: 100, size: 10 }];
  const parts: Uint8Array[] = [];
  for (const e of entries) {
    parts.push(encodePageIndexRecord(e.pgno, e.offset, e.size));
  }
  parts.push(encodeUvarint(0n));
  const sizeField = new Uint8Array(8);
  new DataView(sizeField.buffer).setBigUint64(0, 9999n); // wrong size
  parts.push(sizeField);

  const r = new ByteReader(concatParts(parts));
  assert.throws(() => decodePageIndex(r, 0, 2n, 3n), /size mismatch/);
});

function concatParts(parts: Uint8Array[]): Uint8Array {
  const len = parts.reduce((a, p) => a + p.length, 0);
  const out = new Uint8Array(len);
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}
