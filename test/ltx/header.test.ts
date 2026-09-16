import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  HeaderSize,
  Magic,
  marshalHeader,
  unmarshalHeader,
  validateHeader,
  type Header,
} from '../../src/ltx/index.ts';

function sampleHeader(): Header {
  return {
    version: 3,
    flags: 0x00000002,
    pageSize: 4096,
    commit: 10,
    minTXID: 5n,
    maxTXID: 6n,
    timestamp: 1726000000000n,
    preApplyChecksum: 0n,
    walOffset: 0n,
    walSize: 0n,
    walSalt1: 0,
    walSalt2: 0,
    nodeID: 0n,
  };
}

test('header marshal/unmarshal round-trips', () => {
  const h = sampleHeader();
  const b = marshalHeader(h);
  assert.equal(b.length, HeaderSize);
  const got = unmarshalHeader(b);
  assert.deepEqual(got, h);
});

test('header has the LTX1 magic in the first 4 bytes', () => {
  const b = marshalHeader(sampleHeader());
  const magic = String.fromCharCode(b[0], b[1], b[2], b[3]);
  assert.equal(magic, Magic);
});

test('header unmarshalling rejects a bad magic', () => {
  const b = marshalHeader(sampleHeader());
  b[0] = b[0] ^ 0xff;
  assert.throws(() => unmarshalHeader(b), /invalid LTX file/);
});

test('header validation rejects invalid page size', () => {
  const h = sampleHeader();
  h.pageSize = 100;
  assert.throws(() => validateHeader(h), /invalid page size/);
});

test('header validation rejects out-of-order TXIDs', () => {
  const h = sampleHeader();
  h.minTXID = 7n;
  h.maxTXID = 6n;
  assert.throws(() => validateHeader(h), /out of order/);
});

test('header validation enforces snapshot pre-apply checksum', () => {
  const h = sampleHeader();
  h.minTXID = 1n; // snapshot
  h.preApplyChecksum = 1n; // must be zero for snapshots
  assert.throws(() => validateHeader(h), /pre-apply checksum must be zero/);
});

test('header validation rejects negative WAL offset', () => {
  const h = sampleHeader();
  h.walOffset = -1n;
  assert.throws(() => validateHeader(h), /wal offset cannot be negative/);
});

test('header validation requires WAL offset when salt set', () => {
  const h = sampleHeader();
  h.walSalt1 = 0x01020304;
  h.walSalt2 = 0x05060708;
  h.walOffset = 0n;
  assert.throws(() => validateHeader(h), /wal offset required if salt exists/);
});
