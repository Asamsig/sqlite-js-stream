import { test } from 'node:test';
import assert from 'node:assert/strict';
import { formatFilename, parseFilename } from '../../src/ltx/index.ts';

test('formatFilename produces zero-padded hex names', () => {
  assert.equal(formatFilename(1n, 100n), '0000000000000001-0000000000000064.ltx');
});

test('parseFilename round-trips', () => {
  const name = formatFilename(0xdeadbeefn, 0xfeedfacecafen);
  assert.deepEqual(parseFilename(name), { minTXID: 0xdeadbeefn, maxTXID: 0xfeedfacecafen });
});

test('parseFilename rejects malformed names', () => {
  assert.throws(() => parseFilename('foo.ltx'), /invalid ltx filename/);
  assert.throws(() => parseFilename('00000000000000-0000000000000064.ltx'), /invalid ltx filename/);
  assert.throws(() => parseFilename('0000000000000001-00000000000000G4.ltx'), /invalid ltx filename/);
});
