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
| **LTX module** (`src/ltx/`) | Encode/decode LTX files; CRC-64 ISO checksums; LZ4 *block* codec; page index; compactor. |
| **WAL module** (`src/wal/`) | Parse SQLite WAL header/frames; verify salt + checksum; produce `pageMap()` (latest committed page offsets). |
| **LocalDatabase** (`src/db.ts`, internal) | Reads a database + `-wal` directly from disk, builds LTX files (base snapshot, incremental L0, level-9 snapshot), and tracks the local position. It holds a short-lived **read-only** `node:sqlite` read transaction to **pin the WAL snapshot** (SQLite won't checkpoint/truncate past an active reader), then reads the files via `fs`. It is an internal implementation detail, not part of the public API. |
| **Replica** (`src/replica.ts`) | Upload level-0/level-9 LTX files to a client; track the replicated position; `restore()`, `ensureExists()`, `syncStatus()`. |
| **ReplicaClient** (`src/replica-client.ts`) | An interface to a storage backend (`LTXFiles`, `OpenLTXFile`, `WriteLTXFile`, `DeleteLTXFiles`, `DeleteAll`). Addons implement this to add upload strategies. |
| **File backend** (`src/backends/file/`) | The simplest backend: store LTX files in `<root>/ltx/<level>/<min>-<max>.ltx`, atomic writes. |
| **Store** (`src/store.ts`) | Coordinate multiple `Replica`s. |
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
   contiguous **level-0** files after it.
2. Each file is opened via `ReplicaClient.openLtxFile` and decoded with a **streaming** LTX reader
   (`StreamLtxReader`), so the whole file is never held in memory.
3. The snapshot's pages are streamed into the output file in order (the lock page is written as
   zeros). Each later level-0 file's pages are then **overlaid** at their page offsets, so a later
   version of a page wins.
4. The output file is truncated to the last file's committed page count and atomically renamed.

Because restore never materializes the full merged LTX (or the whole DB) in memory, peak memory is
bounded by one page buffer plus the (small) page index, regardless of the database size.

---

## Position tracking

A **transaction ID** is a sequential counter, not derived from SQLite. The LTX file header carries
`WALOffset`, `WALSize`, `WALSalt1`, `WALSalt2`, so a sync can resume exactly where the previous one
left off, even after a restart.

---

## Checksums

- **LTX page checksums & file checksum** use **CRC-64 ISO** (`crc64.MakeTable(crc64.ISO)`), matching
  Go's `superfly/ltx`. (The litestream docs' mention of CRC-64 ECMA is stale; the code uses ISO.)
- **SQLite WAL checksums** use the rolling `s0/s1` algorithm, with the byte order selected by the WAL
  magic (`0x377f0682` ⇒ little-endian, `0x377f0683` ⇒ big-endian).

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
- Multi-level compaction (L0→L1→L2 + retention), cloud backends, distributed leasing, VFS
  mirroring, and the IPC server are not yet implemented.
