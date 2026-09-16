import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import {
  WALFrameHeaderSize,
  WALHeaderSize,
  WALMagicLE,
  WALReader,
  WALEOFError,
  walChecksum,
} from '../../src/wal/index.ts';
import { readU32BE } from '../../src/ltx/index.ts';
import { createWALDB } from '../helpers/make-db.ts';

test('walChecksum computes the SQLite rolling checksum (LE)', () => {
  // 8 bytes representing 0x01020304, 0x05060708 in little-endian.
  const b = Uint8Array.of(0x04, 0x03, 0x02, 0x01, 0x08, 0x07, 0x06, 0x05);
  const [s0, s1] = walChecksum('le', 0, 0, b);
  assert.equal(s0, 0x01020304);
  assert.equal(s1, 0x05060708 + s0);
});

test('WALReader reads a real WAL produced by node:sqlite', () => {
  const t = createWALDB(['a', 'b', 'c', 'd', 'e']);
  try {
    const wal = new Uint8Array(readFileSync(t.walPath));
    const r = new WALReader(wal);
    assert.equal(r.pageSize(), 4096);

    const frames: { pgno: number; commit: number }[] = [];
    const data = new Uint8Array(r.pageSize());
    for (;;) {
      let fr;
      try {
        fr = r.readFrame(data);
      } catch (err) {
        if (err instanceof WALEOFError) break;
        throw err;
      }
      frames.push(fr);
      assert.ok(fr.pgno >= 1);
    }

    assert.ok(frames.length > 0, 'expected at least one frame');
    assert.ok(frames.some((f) => f.commit !== 0), 'expected a commit frame');

    const pm = new WALReader(wal).pageMap();
    assert.ok(pm.commit > 0);
    assert.equal(pm.commit, t.pageCount());
    assert.ok(pm.pageIndex.size > 0);
    assert.ok(pm.maxOffset > WALHeaderSize);
    for (const pgno of pm.pageIndex.keys()) {
      assert.ok(pgno <= pm.commit, `page ${pgno} exceeds commit ${pm.commit}`);
    }
  } finally {
    t.close();
  }
});

test('WALReader.withOffset resumes from a known frame offset', () => {
  const t = createWALDB(['a', 'b', 'c', 'd', 'e']);
  try {
    const wal = new Uint8Array(readFileSync(t.walPath));

    // Read all frames from the start.
    const reader = new WALReader(wal);
    const data = new Uint8Array(reader.pageSize());
    const all: { pgno: number; commit: number }[] = [];
    for (;;) {
      try {
        all.push(reader.readFrame(data));
      } catch (err) {
        if (err instanceof WALEOFError) break;
        throw err;
      }
    }
    assert.ok(all.length > 2);

    const frameSize = reader.pageSize() + WALFrameHeaderSize;
    const salt1 = readU32BE(wal, 16);
    const salt2 = readU32BE(wal, 20);
    const offset = WALHeaderSize + 2 * frameSize; // start at frame index 2

    const resume = WALReader.withOffset(wal, offset, salt1, salt2);
    const resumed: { pgno: number; commit: number }[] = [];
    const d = new Uint8Array(resume.pageSize());
    for (;;) {
      try {
        resumed.push(resume.readFrame(d));
      } catch (err) {
        if (err instanceof WALEOFError) break;
        throw err;
      }
    }

    assert.equal(resumed.length, all.length - 2);
    for (let i = 0; i < resumed.length; i++) {
      assert.deepEqual(resumed[i], all[2 + i]);
    }
  } finally {
    t.close();
  }
});

test('WALReader detects a header checksum mismatch', () => {
  const t = createWALDB(['a']);
  try {
    const wal = new Uint8Array(readFileSync(t.walPath));
    const bad = Uint8Array.from(wal);
    bad[24] ^= 0xff;
    assert.throws(() => new WALReader(bad), WALEOFError);
  } finally {
    t.close();
  }
});

test('WALReader rejects an invalid magic', () => {
  const t = createWALDB(['a']);
  try {
    const wal = new Uint8Array(readFileSync(t.walPath));
    const bad = Uint8Array.from(wal);
    bad[0] = 0xff;
    assert.throws(() => new WALReader(bad), /invalid wal header magic/);
  } finally {
    t.close();
  }
});

test('WALReader.pageMap drops pages beyond the final commit (shrunk DB)', () => {
  const pageSize = 512;
  const frameSize = WALFrameHeaderSize + pageSize;
  const wal = new Uint8Array(WALHeaderSize + 2 * frameSize);

  // Header (little-endian checksums).
  const hdr = wal.subarray(0, 24);
  setU32(hdr, 0, WALMagicLE);
  setU32(hdr, 4, 3007000);
  setU32(hdr, 8, pageSize);
  setU32(hdr, 12, 1);
  setU32(hdr, 16, 0x11111111);
  setU32(hdr, 20, 0x22222222);
  const [h0, h1] = walChecksum('le', 0, 0, hdr);
  setU32(wal, 24, h0);
  setU32(wal, 28, h1);

  let s0 = h0;
  let s1 = h1;

  // Frame 1: page 5, not a commit.
  const f1 = wal.subarray(WALHeaderSize, WALHeaderSize + frameSize);
  setU32(f1, 0, 5);
  setU32(f1, 4, 0);
  setU32(f1, 8, 0x11111111);
  setU32(f1, 12, 0x22222222);
  [s0, s1] = walChecksum('le', s0, s1, f1.subarray(0, 8));
  [s0, s1] = walChecksum('le', s0, s1, f1.subarray(WALFrameHeaderSize));
  setU32(f1, 16, s0);
  setU32(f1, 20, s1);

  // Frame 2: page 1, commit=4 (db size 4 pages). Page 5 must be dropped.
  const f2 = wal.subarray(WALHeaderSize + frameSize, WALHeaderSize + 2 * frameSize);
  setU32(f2, 0, 1);
  setU32(f2, 4, 4);
  setU32(f2, 8, 0x11111111);
  setU32(f2, 12, 0x22222222);
  [s0, s1] = walChecksum('le', s0, s1, f2.subarray(0, 8));
  [s0, s1] = walChecksum('le', s0, s1, f2.subarray(WALFrameHeaderSize));
  setU32(f2, 16, s0);
  setU32(f2, 20, s1);

  const pm = new WALReader(wal).pageMap();
  assert.equal(pm.commit, 4);
  assert.equal(pm.pageIndex.size, 1);
  assert.ok(pm.pageIndex.has(1));
  assert.ok(!pm.pageIndex.has(5));
  assert.equal(pm.maxOffset, WALHeaderSize + 2 * frameSize);
});

function setU32(buf: Uint8Array, off: number, v: number): void {
  buf[off] = (v >>> 24) & 0xff;
  buf[off + 1] = (v >>> 16) & 0xff;
  buf[off + 2] = (v >>> 8) & 0xff;
  buf[off + 3] = v & 0xff;
}
