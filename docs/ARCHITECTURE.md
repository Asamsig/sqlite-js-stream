# Architecture

*Adapted from [Litestream](https://github.com/benbjohnson/litestream)'s `docs/ARCHITECTURE.md`
(Apache-2.0). Describes the Node.js/TypeScript port in `sqlite-js-stream`.*

This document explains how `sqlite-js-stream` replicates a SQLite database. It is the high-level
design; see [`LTX_FORMAT.md`](LTX_FORMAT.md) for the on-disk format and [`PLAN.md`](../PLAN.md) for
the milestone status.

---

## Overview

SQLite databases are a single file plus (in WAL mode) a `-wal` and a `-shm` file. Replication must
capture the write-ahead log before it is checkpointed away, record enough to reconstruct any point
in time, and upload it somewhere durable.

The core insight (from Litestream): **do not use SQLite's replication or backup API to read the
data.** Instead:

1. Read the `-wal` file and the main database file **directly** (via `fs`),
2. Serialize the changed pages into an **LTX file** (immutable, chronological, checksummed),
3. Upload LTX files to a **replica** destination,
4. **Restore** by replaying a snapshot + level-0 files.

A single long-lived **read transaction** on a `node:sqlite` connection pins a consistent snapshot and
prevents the WAL from being checkpointed past our position.

---

## Components

| Component | Responsibility |
|-----------|----------------|
| **LTX module** (`src/ltx/`) | Encode/decode LTX files; CRC-64 ISO checksums; LZ4 *block* codec; page index; in-memory `Compactor` and streaming `StreamingCompactor`. |
| **WAL module** (`src/wal/`) | Parse SQLite WAL header/frames; verify salt + checksum; produce `pageMap()` (latest committed page offsets). |
| **LocalDatabase** (`src/db.ts`, internal) | Reads a database + `-wal` directly from disk, builds LTX files (base snapshot, incremental L0, level-9 snapshot), and tracks the local position. It holds a short-lived **read-only** `node:sqlite` read transaction to **pin the WAL snapshot** (SQLite won't checkpoint/truncate past an active reader), then reads the files via `fs`. It is an internal implementation detail, not part of the public API. |
| **Replica** (`src/replica.ts`) | Upload level-0/level-9 LTX files to a client; track the replicated position; `restore()`, `follow()`, `ensureExists()`, `syncStatus()`, `compact()`, `enforceRetention()`, `run()`. |
| **ReplicaClient** (`src/replica-client.ts`) | An interface to a storage backend (`LTXFiles`, `OpenLTXFile`, `WriteLTXFile`, `DeleteLTXFiles`, `DeleteAll`). Addons implement this to add upload strategies. |
| **File backend** (`src/backends/file/`) | The simplest backend: store LTX files in `<root>/ltx/<level>/<min>-<max>.ltx`, atomic writes. |
| **Store** (`src/store.ts`) | Coordinate multiple `Replica`s: `syncAll()`, `snapshotAll()`, `compactAll()`, `enforceRetentionAll()`, `run()`. |
| **Addons** (`packages/`) | Upload strategies live as separate workspace packages. Each addon depends on the core `sqlite-js-stream` and registers a URL scheme (e.g. `packages/backend-s3` registers `s3://`). |

> The repository is a **pnpm workspace**: the core package is at the root and
> upload-strategy addons live under `packages/*`. Addons import the `ReplicaClient`
> contract (and `registerReplicaClientFactory`) from the core.

---

## Replication flow

### Replica.sync()

1. Compute the next **transaction ID**: `txID = localPos + 1`, where `localPos` is the max
   `MaxTXID` among local level-0 LTX files.
2. Read the current database commit size (`PRAGMA page_count`) and the WAL file.
3. **First sync** (no local files): write a **full-snapshot base** LTX file with every committed
   page (`MinTXID = MaxTXID = 1`).
4. **Later syncs**: resume the WAL reader from the previous sync's `WALOffset + WALSize` and salts
   (`WALReader.withOffset`). Build a **page map** of committed pages since then and write an
   **incremental** LTX file (only the changed pages, plus any DB-growth pages). If the WAL has been
   reset (salt/checksum mismatch → `PrevFrameMismatchError`), fall back to a new base snapshot.
5. Write the LTX file atomically to `<db>-litestream/ltx/0/<txID>-<txID>.ltx`.

> **Reading pages.** Each page is read from the WAL (at `pageMap[pgno] + WALFrameHeaderSize`) if it
> appears in the committed WAL page map, otherwise from the main database file. This is what makes
> the read consistent with the pinned read snapshot without copying the whole database.

### Replica.snapshot()

Writes a **level-9** full snapshot LTX file (`MinTXID = 1`, `MaxTXID = current position`) containing
every committed page. Combined with incremental level-0 files this gives point-in-time restore.

### Replica.run()

`Replica.sync()` writes a local level-0 LTX file and then uploads every pending L0 file from
`replicaPos + 1` to `dbPos`, advancing the replicated position (which is the max `MaxTXID` among
level-0 files on the client). `Replica.run({ intervalMs, signal })` loops this continuously until
the `AbortSignal` is aborted: it syncs immediately, waits `intervalMs`, then repeats. A transient
sync error is logged and the loop keeps going.

### Replica.restore()

1. `CalcRestorePlan` picks the latest **level-9 snapshot** at or before the target TXID, then the
   contiguous files across all **compaction levels** (0..max) after it, preferring the highest level
   for any given TXID range.
2. Each file is opened via `ReplicaClient.openLtxFile` and decoded with a **streaming** LTX reader
   (`StreamLtxReader`), so the whole file is never held in memory.
3. The snapshot's pages are streamed into the output file in order (the lock page is written as
   zeros). Each later file's pages are then **overlaid** at their page offsets, so a later version of
   a page wins.
4. The output file is truncated to the last file's committed page count and atomically renamed.

Because restore never materializes the full merged LTX (or the whole DB) in memory, peak memory is
bounded by one page buffer plus the (small) page index, regardless of the database size.

### Replica.follow()

`follow()` is litestream's `restore -f` continuous mode. It first restores the latest backup to
`outputPath`, then polls (default every 1s) and overlays any newly replicated LTX files into that
database, keeping a **read-only** replica up to date with the primary.

- It records the last applied TXID in a `-txid` sidecar (atomically temp+rename) after each batch, so
  a later `follow()` resumes from that point instead of re-restoring. LTX files are immutable and
  page content is deterministic, so re-applying a batch after a crash is idempotent (the stale
  sidecar simply replays the same pages).
- It refuses to resume if the DB exists but the `-txid` sidecar is missing (it can't prove the file
  matches the replica), throws on a malformed sidecar, and throws on a TXID gap (e.g. retention
  pruned the next needed file).
- Resume/inconsistency failures are typed: `FollowMissingTxidError` (`ERR_FOLLOW_DB_WITHOUT_TXID`),
  `FollowInvalidTxidError` (`ERR_FOLLOW_INVALID_TXID`), `FollowAheadOfLatestError`
  (`ERR_FOLLOW_AHEAD_OF_LATEST`), `FollowDataGapError` (`ERR_FOLLOW_OLDER_THAN_EARLIEST`), and
  `FollowAbortedError` (`ERR_FOLLOW_ABORTED`) — all extend `FollowError` and carry a `.code`.
- The restored DB must be opened **read-only**; a writer would conflict with the overlaid pages.

---

## Compaction & retention

Replication writes fresh **level-0** files per sync. To keep the replica from growing without bound
and the restore plan from spanning thousands of tiny files, `Replica.compact()` merges a level's
files into the next level using the streaming compactor (`StreamingCompactor`), and
`Replica.enforceRetention()` prunes superseded files.

### Levels

- **Level 0**: fresh incremental files written by each `sync()`.
- **Levels 1..N** (`maxCompactionLevel`, default 3): compacted files. `compact(dstLevel)` reads the
  pending files at `dstLevel - 1` (after the current max TXID at `dstLevel`) and writes a single
  merged file at `dstLevel` covering the whole TXID range. Later files win for equal page numbers.
- **Level 9**: full-database snapshots written by `snapshot()`.

Compaction is **streaming**: each source is read page-by-page and the merged output is written
straight to a file, so peak memory stays bounded. The output carries the WAL position of the newest
input, so sync can continue incrementally even after level-0 files are pruned.

### Retention / GC

`Replica.enforceRetention({ snapshotRetain, snapshotRetentionMs, l0RetentionMs })`:

1. **Snapshots** past the policy are deleted (keep newest `snapshotRetain`, and/or delete those older
   than `snapshotRetentionMs`; the newest snapshot is always kept).
2. Files whose `MaxTXID` is below the **oldest retained snapshot** are deleted — a snapshot at TXID N
   supersedes every file below N, so those are no longer needed to restore any target within the
   retention window.
3. Files at a lower level that are fully covered by a file at a higher level and are older than
   `l0RetentionMs` are deleted (for example, a compacted level-1 file supersedes its level-0
   sources).

Deletion happens on both the client and the local cache, so the two stay consistent. Because
compaction only deletes a source file after its destination file is written *and uploaded*, a
failure mid-compaction never loses data.

### Scheduling

`Replica.run()` can run `sync()`, `compact()`, and `enforceRetention()` on independent intervals via
`compactionIntervalMs` / `retentionIntervalMs`; `Store.run()` drives the same loop across all
replicas. `Store.compactAll()` / `enforceRetentionAll()` run a single pass over every database.

---

## Position tracking

A **transaction ID** is a sequential counter, not derived from SQLite. The LTX file header carries
`WALOffset`, `WALSize`, `WALSalt1`, `WALSalt2`, so a sync can resume exactly where the previous one
left off, even after a restart.

---

## Distributed leasing

When several replicas share a storage path, only one may write. A `Leaser` (from `src/leaser.ts`)
acquires/renews/releases a `lock.json` lease; `Replica.run()` uses it to choose a **primary**
(writes) vs a **follower** (idle, polls). All storage-write methods (`sync`, `snapshot`, `compact*`,
`enforceRetention*`) are gated on lease ownership, so a secondary can never write.

- `Lease` = `{ generation, expiresAt, owner, etag }`; `etag` is the conditional-write token.
- `acquireLease` uses `If-None-Match: *` when no lease exists and `If-Match: <etag>` when replacing an
  expired one — two racy acquirers cannot both win (one gets `412 PreconditionFailed`).
- `renewLease` uses `If-Match: <etag>` and throws `ErrLeaseNotHeld` on mismatch (we lost it).
- `releaseLease` deletes with `If-Match: <etag>`; missing object = already released.
- A follower polls `acquireLease` each loop; a **dedicated timer** renews the lease before expiry,
  independent of the loop cadence, so a large `intervalMs` cannot let it expire. Losing the lease
  reverts the node to follower. On shutdown the primary stops the timer and releases the lease.

- **File backend** (`FileLeaser`): exclusive-create (`wx`) + content-hash etag — fine for
  local/single-host leases.
- **S3 backend** (`S3Leaser`, addon): real conditional writes via S3 `If-None-Match`/`If-Match`.

### What the lease does *not* do (single-writer semantics)

**This is a backup & disaster-recovery tool for one SQLite database. It is not a distributed
database.** The lease elects a single *writer to storage*, but it never merges, compares, or reconciles
the contents of your databases.

The layer the lease protects is **only** the replication/storage path (so two hosts can't both push LTX
files to the same prefix). It does **not** coordinate writes inside your SQLite files. Each `Replica`
reads **its own local `-wal`**; there is no cross-host write ordering, no consensus, and no merge.

Concretely, if host A has `row id=1 → "X"` and host B has `row id=1 → "Y"` in their *separate* local
databases, and both point at one prefix:

1. Only the **primary** host pushes to S3.
2. The backup records the primary's value (`"X"` if A is primary).
3. The follower's value (`"Y"`) is **never uploaded and never merged** — restore yields exactly one
   host's state, and `"Y"` is unrecoverable from the backup.

SQLite itself serializes writes *within a single DB file* (last commit wins via the WAL). But there is
**no** ordering or coordination across two separate SQLite files on two hosts. A single SQLite file has
one writer by design, so pointing several hosts at one backup prefix only makes sense when they all hold
the **same** logical data.

### When multi-host + lease is correct

- **Standby / DR failover**: every host holds the *same* logical DB (the standby is restored from the
  same backup). The lease picks one writer and gives automatic takeover when the primary's lease
expires.

  > On failover, the standby has a **gap**: it only contains data up to the last backup, not writes the
  > primary made in the last few seconds before it died. Always restore the standby from S3 *before*
  > it takes the lease.

- **Preventing split-brain in the backup**: with a lease, a secondary can never write, so it cannot
  corrupt the shared prefix.

### When it is *not* correct

- **Different data on different hosts sharing one prefix.** Each host should get its own prefix
  (`app/${tenantId}`) so the backups stay separate. Sharing one prefix across divergent DBs interleaves
  their LTX files and corrupts the replica.
- **Horizontal write scaling of one logical dataset.** That requires a client-server database, not
  SQLite + LTX replication.

---

## Checksums

- **LTX page checksums & file checksum** use **CRC-64 ISO** (`crc64.MakeTable(crc64.ISO)`), matching
  Go's `superfly/ltx`. (The litestream docs' mention of CRC-64 ECMA is stale; the code uses ISO.)
- **SQLite WAL checksums** use the rolling `s0/s1` algorithm, with the byte order selected by the WAL
  magic (`0x377f0682` ⇒ little-endian, `0x377f0683` ⇒ big-endian).

## Errors

Every typed library error extends one base class, [`SqliteJsStreamError`](../src/errors.ts), and
carries a stable, Node-style `code` string. Callers can catch any library error with a single
`instanceof SqliteJsStreamError`, and branch on the precise failure via `err.code` (or `instanceof`
on the specific subclass) instead of matching message text.

| Error | `code` | Meaning |
|-------|--------|---------|
| `NoMatchingBackupError` | `ERR_NO_MATCHING_BACKUP` | No LTX files at/below the target TXID. |
| `PageSizeMismatchError` | `ERR_PAGE_SIZE_MISMATCH` | An incremental file's page size differs from the base. |
| `InvalidArgumentError` | `ERR_INVALID_ARGUMENT` | A required argument is missing/invalid. |
| `LTXError` | `ERR_LTX` | An LTX file could not be read/decoded (carries `op`, `path`, `level`, TXIDs). |
| `LeaseExistsError` | `ERR_LEASE_EXISTS` | The lease is held by another host. |
| `ErrLeaseNotHeld` | `ERR_LEASE_NOT_HELD` | The caller lost/never held the lease. |
| `ErrNoCompactionError` (`ErrNoCompaction`) | `ERR_NO_COMPACTION` | Nothing to compact (singleton; use `===`). |
| `ErrCompactionTooEarlyError` (`ErrCompactionTooEarly`) | `ERR_COMPACTION_TOO_EARLY` | The level was already compacted this bucket (singleton). |
| `FollowError` subclasses | `ERR_FOLLOW_*` | `follow()` resume/inconsistency failures (see [Replica.follow()](#replica-follow)). |
| `LTXFormatError` | `ERR_LTX_FORMAT` | An LTX file is malformed/corrupt (decode/encode/parse/checksum). |
| `WALFormatError` | `ERR_WAL_FORMAT` | A SQLite WAL file is malformed/corrupt. |
| `ReplicaURLError` | `ERR_REPLICA_URL` | A replica URL is malformed or uses an unsupported scheme. |
| `FileReplicaError` | `ERR_FILE_REPLICA` | A local file/backend replica operation failed. |
| `DatabaseError` | `ERR_DATABASE` | A SQLite database file is missing/invalid or unreadable. |

---

## Restrictions & simplifications

- **Snapshot consistency.** `sync()`/`snapshot()` pin a read-only snapshot via a `node:sqlite` read
  transaction on an independent read-only connection so the WAL cannot be checkpointed/truncated
  while the DB + WAL files are read; the pinned reader's `PRAGMA page_count` also reports the commit
  including uncheckpointed WAL pages.
- **Connection sharing constraint.** The library uses a separate read-only connection for pinned
  snapshots and never consumes the application's writable connection. A single SQLite connection
  holds one transaction at a time: reusing the application's `DatabaseSync` handle would merge the
  library's read snapshot with the app's writes (breaking snapshot isolation and letting the library
  read uncommitted rows) and would prevent it from `BEGIN`-ing its pin if the app already had a
  transaction open. The app's writer and the library's reader must be separate connections to the
  same database file.
- **Streaming for large databases.** `sync()`/`snapshot()` read database pages from a file handle one
  at a time and stream the encoded LTX straight to a temp file (`FileSink`), instead of buffering the
  whole DB/WAL. `restore()` streams pages into the output DB rather than buffering every LTX file. The
  S3 addon uploads objects larger than 5 MiB with the multipart API so the upload is never buffered
  either. The WAL is still read in full (it is small relative to the database).
- LZ4 uses `lz4js` (pure JS **block** format), which is interoperable for **decompression** but not
  byte-identical to Go's `pierrec/lz4/v4`. Files written here are readable by Litestream and
  vice-versa.
- **Write format is modern LTX.** We write `superfly/ltx v0.5.2` LTX files with compaction levels
  0..9 (snapshots at level 9) under `ltx/{level}/{min}-{max}.ltx`, exactly matching current
  litestream. We deliberately do **not** read legacy v0.3.x generation backups
  (`generations/…/snapshots|wal/*.lz4`); the LZ4 frame codec and generation-listing interfaces that
  would be needed are intentionally omitted.
- **Single-writer semantics (important).** This is a backup/DR tool for **one** SQLite database, not
  a distributed database. The lease elects a single writer to storage but never merges or reconciles
  data across hosts. Pointing several hosts at one prefix is only correct when they hold the **same**
  logical data (see [Distributed leasing](#distributed-leasing)); separate databases must use separate
  prefixes.
- Cloud backends beyond S3 (GCS/Azure/SFTP/NATS), VFS mirroring, and the IPC server are not yet
  implemented. Distributed leasing is implemented for the file and S3 backends (see above).
