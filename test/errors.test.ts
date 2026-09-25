import { test } from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';

import {
  ErrCompactionTooEarly,
  ErrCompactionTooEarlyError,
  ErrLeaseNotHeld,
  ErrNoCompaction,
  ErrNoCompactionError,
  FileReplicaClient,
  FollowAheadOfLatestError,
  FollowDataGapError,
  FollowInvalidTxidError,
  FollowMissingTxidError,
  InvalidArgumentError,
  LeaseExistsError,
  LTXError,
  NoMatchingBackupError,
  PageSizeMismatchError,
  Replica,
  SqliteJsStreamError,
} from '../src/index.ts';
import { makeTempDir } from './helpers/make-db.ts';

test('library error classes share the common base and carry .code/name', () => {
  // Every typed error must extend SqliteJsStreamError (the single catch-all base).
  const cases: Array<{ err: unknown; code: string; name: string }> = [
    { err: new NoMatchingBackupError(), code: 'ERR_NO_MATCHING_BACKUP', name: 'NoMatchingBackupError' },
    { err: new PageSizeMismatchError('mismatch'), code: 'ERR_PAGE_SIZE_MISMATCH', name: 'PageSizeMismatchError' },
    { err: new InvalidArgumentError('bad arg'), code: 'ERR_INVALID_ARGUMENT', name: 'InvalidArgumentError' },
    { err: ErrNoCompaction, code: 'ERR_NO_COMPACTION', name: 'ErrNoCompactionError' },
    { err: ErrCompactionTooEarly, code: 'ERR_COMPACTION_TOO_EARLY', name: 'ErrCompactionTooEarlyError' },
    {
      err: new LTXError('read', '/x.ltx', 0, 1n, 1n, new Error('boom')),
      code: 'ERR_LTX',
      name: 'LTXError',
    },
    { err: new LeaseExistsError('owner', Date.now() + 1000), code: 'ERR_LEASE_EXISTS', name: 'LeaseExistsError' },
    { err: new ErrLeaseNotHeld(), code: 'ERR_LEASE_NOT_HELD', name: 'ErrLeaseNotHeld' },
    { err: new FollowMissingTxidError('x'), code: 'ERR_FOLLOW_DB_WITHOUT_TXID', name: 'FollowMissingTxidError' },
    { err: new FollowInvalidTxidError('x'), code: 'ERR_FOLLOW_INVALID_TXID', name: 'FollowInvalidTxidError' },
    { err: new FollowAheadOfLatestError('x'), code: 'ERR_FOLLOW_AHEAD_OF_LATEST', name: 'FollowAheadOfLatestError' },
    { err: new FollowDataGapError('x'), code: 'ERR_FOLLOW_OLDER_THAN_EARLIEST', name: 'FollowDataGapError' },
  ];

  for (const { err, code, name } of cases) {
    assert.ok(err instanceof SqliteJsStreamError, `${name} should extend SqliteJsStreamError`);
    assert.ok(err instanceof Error, `${name} should extend Error`);
    assert.equal((err as { code: string }).code, code);
    assert.equal((err as { name: string }).name, name);
  }

  // The compaction singletons are instances of their classes, so both
  // `err === ErrNoCompaction` and `err instanceof ErrNoCompactionError` work.
  assert.ok(ErrNoCompaction instanceof ErrNoCompactionError);
  assert.ok(ErrCompactionTooEarly instanceof ErrCompactionTooEarlyError);
});

test('restore with an empty replica throws a coded NoMatchingBackupError', async () => {
  const client = new FileReplicaClient(makeTempDir('empty-'));
  const replica = new Replica('/tmp/does-not-matter.db', client);
  try {
    await assert.rejects(
      replica.restore({ outputPath: join(makeTempDir('restore-'), 'x.db') }),
      (err: unknown) => {
        assert.ok(err instanceof NoMatchingBackupError);
        assert.equal((err as NoMatchingBackupError).code, 'ERR_NO_MATCHING_BACKUP');
        assert.equal((err as NoMatchingBackupError).message, 'no matching backup files available');
        return true;
      },
    );
  } finally {
    replica.close();
  }
});

test('restore validates the output path with a coded InvalidArgumentError', async () => {
  const client = new FileReplicaClient(makeTempDir('x-'));
  const replica = new Replica('/tmp/does-not-matter.db', client);
  try {
    await assert.rejects(
      replica.restore({ outputPath: '' }),
      (err: unknown) => {
        assert.ok(err instanceof InvalidArgumentError);
        assert.equal((err as InvalidArgumentError).code, 'ERR_INVALID_ARGUMENT');
        return true;
      },
    );
  } finally {
    replica.close();
  }
});
