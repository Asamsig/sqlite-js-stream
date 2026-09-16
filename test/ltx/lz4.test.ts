import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { compressBlock, decompressBlock } from '../../src/ltx/index.ts';
import { byteEq } from '../helpers/byte.ts';

test('lz4 block round-trips compressible data', () => {
  const data = new Uint8Array(512);
  for (let i = 0; i < 512; i++) data[i] = (5 + (i % 7)) & 0xff;

  const compressed = compressBlock(data);
  assert.ok(compressed.length < data.length, `expected some compression, got ${compressed.length}`);
  const out = decompressBlock(compressed, data.length);
  assert.ok(byteEq(out, data));
});

test('lz4 block round-trips random data (likely literal-only path)', () => {
  const data = new Uint8Array(randomBytes(512));

  const compressed = compressBlock(data);
  const out = decompressBlock(compressed, data.length);
  assert.ok(byteEq(out, data));
});

test('lz4 block round-trips empty data', () => {
  const data = new Uint8Array(0);
  const compressed = compressBlock(data);
  assert.equal(compressed.length, 1);
  const out = decompressBlock(compressed, 0);
  assert.equal(out.length, 0);
});

test('lz4 block round-trips a 4KiB page', () => {
  const data = new Uint8Array(4096);
  for (let i = 0; i < 4096; i++) data[i] = (i & 0xff);
  const compressed = compressBlock(data);
  const out = decompressBlock(compressed, data.length);
  assert.ok(byteEq(out, data));
});
