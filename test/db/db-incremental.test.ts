import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

import { DB, Decoder } from '../../src/index.ts';
import { createWALDB } from '../helpers/make-db.ts';

function countPages(dec: Decoder): number {
  const data = new Uint8Array(dec.Header().pageSize);
  let n = 0;
  for (;;) {
    const ph = dec.decodePage(data);
    if (ph === null) break;
    n++;
  }
  dec.close();
  return n;
}

test('first sync is a base snapshot; later syncs are incremental', async () => {
  const t = createWALDB(['a', 'b', 'c', 'd', 'e']);
  const db = await DB.open(t.path);
  try {
    const info1 = await db.sync();
    assert.equal(info1?.minTXID, 1n);
    assert.equal(info1?.maxTXID, 1n);

    // Base snapshot: minTXID=1, contains all pages.
    const dec1 = new Decoder(new Uint8Array(await readFile(db.ltxPath(0, 1n, 1n))));
    dec1.decodeHeader();
    assert.equal(dec1.Header().minTXID, 1n, 'base file is a snapshot');
    const pages1 = countPages(dec1);
    assert.ok(pages1 > 0, 'base snapshot has pages');
    assert.equal(pages1, dec1.Header().commit - (dec1.Header().commit > 2097153 ? 1 : 0));

    // Insert more rows and sync again.
    t.db.exec("INSERT INTO t (name) VALUES ('f'), ('g')");
    const info2 = await db.sync();
    assert.equal(info2?.minTXID, 2n);
    assert.equal(info2?.maxTXID, 2n);

    // Incremental file: minTXID=2 (not a snapshot) and smaller than the base.
    const dec2 = new Decoder(new Uint8Array(await readFile(db.ltxPath(0, 2n, 2n))));
    dec2.decodeHeader();
    assert.equal(dec2.Header().minTXID, 2n, 'incremental file is not a snapshot');
    const pages2 = countPages(dec2);
    assert.ok(pages2 < pages1, `incremental (${pages2}) should have fewer pages than base (${pages1})`);
    assert.ok(info2!.size < info1!.size, 'incremental file should be smaller');
  } finally {
    t.close();
    db.close();
  }
});

test('txID files are contiguous (each min-1 equals previous max)', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  const db = await DB.open(t.path);
  try {
    await db.sync();
    t.db.exec("INSERT INTO t (name) VALUES ('d')");
    await db.sync();
    t.db.exec("INSERT INTO t (name) VALUES ('e')");
    await db.sync();

    const files = await db.l0Files();
    assert.deepEqual(files.map((f) => f.minTXID), [1n, 2n, 3n]);
    assert.deepEqual(files.map((f) => f.maxTXID), [1n, 2n, 3n]);
  } finally {
    t.close();
    db.close();
  }
});
