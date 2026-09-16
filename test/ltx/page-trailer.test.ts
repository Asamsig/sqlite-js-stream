import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PageHeaderSize,
  TrailerSize,
  marshalPageHeader,
  unmarshalPageHeader,
  validatePageHeader,
  marshalTrailer,
  unmarshalTrailer,
  validateTrailer,
  ChecksumFlag,
  type Header,
} from '../../src/ltx/index.ts';

test('page header marshal/unmarshal round-trips', () => {
  const h = { pgno: 42, flags: 0x0001 };
  const b = marshalPageHeader(h);
  assert.equal(b.length, PageHeaderSize);
  assert.deepEqual(unmarshalPageHeader(b), h);
});

test('page header big-endian byte layout', () => {
  const b = marshalPageHeader({ pgno: 0x01020304, flags: 0x0001 });
  assert.equal(b[0], 0x01);
  assert.equal(b[1], 0x02);
  assert.equal(b[2], 0x03);
  assert.equal(b[3], 0x04);
  assert.equal(b[4], 0x00);
  assert.equal(b[5], 0x01);
});

test('page header validation rejects zero page number', () => {
  assert.throws(() => validatePageHeader({ pgno: 0, flags: 1 }), /page number required/);
});

test('page header validation rejects unknown flags', () => {
  assert.throws(() => validatePageHeader({ pgno: 1, flags: 0x0002 }), /invalid page header flags/);
});

test('trailer marshal/unmarshal round-trips', () => {
  const t = { postApplyChecksum: ChecksumFlag | 1n, fileChecksum: ChecksumFlag | 2n };
  const b = marshalTrailer(t);
  assert.equal(b.length, TrailerSize);
  assert.deepEqual(unmarshalTrailer(b), t);
});

function noChecksumHeader(): Header {
  return {
    version: 3,
    flags: 0x00000002,
    pageSize: 4096,
    commit: 10,
    minTXID: 5n,
    maxTXID: 6n,
    timestamp: 0n,
    preApplyChecksum: 0n,
    walOffset: 0n,
    walSize: 0n,
    walSalt1: 0,
    walSalt2: 0,
    nodeID: 0n,
  };
}

test('trailer validation for no-checksum files requires empty post-apply checksum', () => {
  const h = noChecksumHeader();
  assert.throws(() => validateTrailer({ postApplyChecksum: 1n, fileChecksum: ChecksumFlag }, h), /post-apply checksum not allowed/);
});

test('trailer validation requires a file checksum with the flag set', () => {
  const h = noChecksumHeader();
  assert.throws(() => validateTrailer({ postApplyChecksum: 0n, fileChecksum: 0n }, h), /file checksum required/);
  assert.throws(() => validateTrailer({ postApplyChecksum: 0n, fileChecksum: 1n }, h), /invalid file checksum format/);
});
