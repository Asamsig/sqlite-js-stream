import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  compressFrameLz4,
  decompressFrameLz4,
  formatSnapshotFilenameV3,
  formatWALSegmentFilenameV3,
  isGenerationIDV3,
  parseSnapshotFilenameV3,
  parseWALSegmentFilenameV3,
} from '../src/index.ts';

test('v0.3.x snapshot filenames round-trip', () => {
  assert.equal(formatSnapshotFilenameV3(1), '00000001.snapshot.lz4');
  assert.equal(parseSnapshotFilenameV3('00000001.snapshot.lz4'), 1);
  assert.equal(parseSnapshotFilenameV3('0000abcd.snapshot.lz4'), 0xabcd);
});

test('v0.3.x WAL segment filenames round-trip', () => {
  assert.equal(formatWALSegmentFilenameV3(1, 0), '00000001_00000000.wal.lz4');
  assert.deepEqual(parseWALSegmentFilenameV3('00000003_00000100.wal.lz4'), {
    index: 3,
    offset: 0x100,
  });
});

test('v0.3.x generation IDs are 16 hex chars', () => {
  assert.ok(isGenerationIDV3('0123456789abcdef'));
  assert.ok(!isGenerationIDV3('short'));
  assert.ok(!isGenerationIDV3('0123456789abcdeG'));
});

test('LZ4 frame compress/decompress round-trips binary data', () => {
  const src = new Uint8Array(300_000);
  for (let i = 0; i < src.length; i++) src[i] = (i * 7) & 0xff;
  const frame = compressFrameLz4(src);
  const out = decompressFrameLz4(frame);
  assert.equal(out.length, src.length);
  for (let i = 0; i < src.length; i++) {
    if (out[i] !== src[i]) {
      assert.fail(`mismatch at ${i}`);
      break;
    }
  }
});

test('LZ4 frame decoder rejects a non-frame buffer', () => {
  assert.throws(() => decompressFrameLz4(new Uint8Array([1, 2, 3, 4, 5, 6, 7])), /bad magic/);
});
