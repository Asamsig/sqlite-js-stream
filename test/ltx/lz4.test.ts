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

function toHex(b: Uint8Array): string {
  return Buffer.from(b).toString('hex');
}

test('lz4 compressBlock is byte-identical to Go pierrec/lz4/v4', () => {
  // Vectors produced by Go's `lz4.CompressBlock` (internal/lz4block Fast mode),
  // captured by running `tools/golden` / a minimal Go program.
  const vectors: Array<[string, string]> = [
    ['hello world hello world hello world hello world', 'cf68656c6c6f20776f726c64200c0001001800b068656c6c6f20776f726c64'],
    ['a'.repeat(47), '1f61010009000200e06161616161616161616161616161'],
    [
      'The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog.',
      'ff1e54686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e202d0009002d00c06865206c617a7920646f672e',
    ],
  ];
  for (const [input, expectedHex] of vectors) {
    const compressed = compressBlock(new Uint8Array(Buffer.from(input, 'utf8')));
    assert.equal(toHex(compressed), expectedHex, `Go byte-identity for: ${JSON.stringify(input.slice(0, 24))}`);
  }
});
