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
| **DB** (`src/db.ts`) | Open a SQLite DB in WAL mode; read pages from the DB file + WAL; build LTX files (base snapshot, incremental L0, level-9 snapshot); track local position. |
| **Replica** (`src/replica.ts`) | Upload level-0/level-9 LTX files to a client; track the replicated position; `restore()`, `ensureExists()`, `syncStatus()`. |
| **ReplicaClient** (`src/replica-client.ts`) | An interface to a storage backend (`LTXFiles`, `OpenLTXFile`, `WriteLTXFile`, `DeleteLTXFiles`, `DeleteAll`). |
| **File backend** (`src/backends/file/`) | The simplest backend: store LTX files in `<root>/ltx/<level>/<min>-<max>.ltx`, atomic writes. |
| **Store** (`src/store.ts`) | Coordinate multiple `DB`+`Replica` pairs. |

---

## Replication flow

### DB.sync()

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

### DB.snapshot()

Writes a **level-9** full snapshot LTX file (`MinTXID = 1`, `MaxTXID = current position`) containing
every committed page. Combined with incremental level-0 files this gives point-in-time restore.

### Replica.sync()

Uploads every local level-0 LTX file from `replicaPos + 1` to `dbPos` to the client, advancing the
replicated position as it goes. The replica position is the max `MaxTXID` among level-0 files on the
client.

### Replica.restore()

1. `CalcRestorePlan` picks the latest **level-9 snapshot** at or before the target TXID, then the
   contiguous **level-0** files after it.
2. Each file is opened via `ReplicaClient.openLtxFile`.
3. A **Compactor** merges all readers, letting later files override earlier ones per page, producing
   a single snapshot-style LTX stream.
4. `Decoder.decodeDatabaseTo` writes out the reconstructed SQLite database page-by-page (the
   lock page is left zero-filled).

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

- Reads are done with the whole DB/WAL file for MVP tests; large databases should stream page-by-page
  from file handles.
- LZ4 uses `lz4js` (pure JS **block** format), which is interoperable for **decompression** but not
  byte-identical to Go's `pierrec/lz4/v4`. Files written here are readable by Litestream and
  vice-versa.
- Multi-level compaction (L0→L1→L2 + retention), cloud backends, distributed leasing, VFS
  mirroring, and the IPC server are not yet implemented.
