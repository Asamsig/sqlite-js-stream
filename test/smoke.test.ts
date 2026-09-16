import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { VERSION } from '../src/index.ts';

test('type stripping runs .ts directly', () => {
  assert.equal(VERSION, '0.1.0');
});

test('node:sqlite is available', () => {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
  db.exec("INSERT INTO t (name) VALUES ('a'), ('b')");
  const row = db.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
  assert.equal(row.n, 2);
  db.close();
});
