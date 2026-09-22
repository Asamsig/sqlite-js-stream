# PLAN — Port Litestream to Node.js as a Library

**Goal:** Port [`benbjohnson/litestream`](https://github.com/benbjohnson/litestream) (a SQLite streaming
replication + disaster-recovery tool) to a **Node.js library**, written in **TypeScript with type
stripping** (no build step at runtime), developed **test-first (TDD)**.

**Guiding principle:** achieve **minimum viable functionality first** — a working end-to-end
`SQLite → local LTX files → restore` pipeline — then layer on advanced features (cloud backends,
compaction, leasing, VFS mirroring).

**Runtime requirement:** Node.js `>= 24` (native `.ts` type stripping + built-in `node:sqlite`).
Verified on Node `v24.21.0`.

---

## 0. Status

- [x] **Milestone 0 — scaffolding**: `package.json`, `tsconfig.json` (type-stripping friendly, `noEmit`),
      `node:test` running `.test.ts` directly, `tsc --noEmit` clean. `node:sqlite` + `lz4js` installed.
- [x] **Milestone 1 — LTX format core**: `Crc64` (ISO), `ChecksumPage`/`ChecksumReader`, header/page/
      trailer/page-index/filename codecs, LZ4 block adapter, `Encoder`/`Decoder`, `BufferSink`,
      `FileInfoSliceIterator`. Unit + round-trip + golden tests green (48 tests).
- [x] **Milestone 2 — WAL reader**: `src/wal/` — `WALReader` (header/frame parse, salt + checksum
      validation, `pageMap` with committed-page-tracking + shrunk-DB page drop), `WALChecksum`,
      `withOffset` resume, `PrevFrameMismatchError`. Verified against real `node:sqlite` WAL files.
- [x] **Milestone 3 — ReplicaClient + file backend**: `ReplicaClient` interface + `findLtxFiles`/
      `fetchLtxHeader`/`fetchPageIndex`/`fetchPage` helpers, `LTXError`, path helpers, replica URL
      parsing + factory registration, and `FileReplicaClient` (atomic temp+rename+fsync writes,
      mtime from LTX header, seek/range reads, delete).
- [x] **Milestone 4 — DB + Replica end-to-end (MVP)**: `DB` (open WAL, `sync()` builds a
      **full-snapshot L0 file** from DB+WAL page map, local position from L0 files) and `Replica`
      (upload L0 to client, position tracking, `CalcRestorePlan`, page-merge `restore()`,
      `ensureExists`, `syncStatus`).
- [x] **Milestone 5 — Incremental + snapshots + compactor + Store (core)**: `Compactor`, level-9
      `DB.snapshot()`, incremental L0 sync (resume from WAL offset, snapshot fallback),
      compactor-based `Restore`, and `Store` (multi-DB open/sync/close).

> **Milestone 5 completed (incremental + snapshots + compactor):** `DB.sync()` now writes a
> **level-0 base snapshot** on the first sync and then **incremental level-0 files** (only the pages
> whose latest committed version is in the WAL since the last synced offset, resuming via
> `WALReader.withOffset`; falls back to a new base snapshot if the WAL was reset). `DB.snapshot()`
> writes **level-9 full snapshots** (`min=1`). `Restore` and `Store` use the `Compactor` to merge a
> level-9 snapshot with subsequent level-0 files. See `test/integration/snapshot-store.test.ts`.

> **Interop note (Milestone 1):** Go's `superfly/ltx` golden file-checksum vectors
> (`0xe27f4c18f0da2c10`, `0xd926967672f7ca53`) are pinned in `test/ltx/golden.test.ts`. Our `lz4js`
> produces *valid* but not *byte-identical* LZ4 block bytes to Go's `pierrec/lz4/v4`, so our file
> checksums differ. LZ4 block **decompression is fully interoperable** (files we write are readable by
> Go and vice-versa); only byte-identical encoding requires a liblz4-compatible codec. Validate Go
> byte-parity in CI with Go goldens if we later switch to `lz4-napi`/WASM liblz4.

---

## 1. What Litestream Is (and what we port)

Litestream runs as a background process that watches a SQLite database's **WAL file**, converts the
changing pages into immutable **LTX (Log Transaction) files**, and replicates them to a replica
destination (S3, GCS, Azure, SFTP, or local files). It also produces **snapshots** (full DB → LTX)
and supports **restore** (reconstruct a database from a set of LTX files).

It never writes to the database through the SQLite WAL/VFS as part of normal replication — it reads
the DB file and `-wal` file directly via file descriptors, and uses the SQLite API only to hold a
long-running read transaction and to trigger checkpoints. This is critical to our Node port: **we
read pages/WAL via `fs.promises`, and use `node:sqlite` only for control + read transaction.**

### Core components (Go → port)

| Go file (litestream)          | Responsibility                                                            | Port target                    |
|-------------------------------|---------------------------------------------------------------------------|--------------------------------|
| `internal` + `superfly/ltx`   | LTX format: encode/decode, checksums, compactor (external pkg)            | `src/ltx/`                     |
| `wal_reader.go`               | SQLite WAL parser (header, frames, checksums, page map)                   | `src/wal/`                     |
| `replica_client.go`           | `ReplicaClient` interface + helpers (page index fetch, header fetch)      | `src/replica-client.ts`        |
| `file/replica_client.go`      | Local-filesystem backend (simplest backend)                               | `src/backends/file/`           |
| `db.go`                       | Single DB lifecycle: open, page size, read txn, monitor, sync, snapshot, restore | `src/db.ts`              |
| `replica.go`                  | Replication mechanics: position tracking, sync to client, restore plan    | `src/replica.ts`               |
| `store.go`                    | Multi-DB coordination, snapshot/compaction scheduling, retention          | `src/store.ts`                 |
| `compactor.go`                | Merge LTX readers into one stream (used for compaction & restore)         | `src/ltx/compactor.ts`         |
| `leaser.go`, `s3/leaser.go`   | Distributed lease (lock.json) — later phase                               | `src/leaser.ts` (later)        |
| `server.go`                   | Unix-socket IPC control server — out of scope for MVP                     | —                              |
| `vfs.go`                      | Real-time SQLite VFS mirroring — out of scope for MVP                     | —                              |

### Layer boundaries (from `AGENTS.md`) — respect these

- **DB layer** owns database state (restoration, WAL monitoring, checkpoint, snapshot).
- **Replica layer** owns replication mechanics only (position, upload, download).
- **Storage layer** owns backend-specific reads/writes (one replica client per backend).
- **Single replica per database.** `LTX files are immutable` — never modify after creation.

---

## 2. Critical format facts (must be exact)

These are the ground truth we must match for interoperability with existing litestream replicas.
Source of truth: `superfly/ltx` v0.5.2 code (vendored by litestream), **not** the sometimes-stale
`docs/LTX_FORMAT.md`.

### 2.1 LTX layout

```
[ Header 100 bytes ][ page block (zero-terminated) ][ page index ][ Trailer 16 bytes ]
```

- **Magic** `<4 bytes>` = `"LTX1"`; `Version` = `3`.
- **Size constants:** `HeaderSize = 100`, `PageHeaderSize = 6`, `TrailerSize = 16`.

#### Header (100 bytes, big-endian)

| Off | Size | Field | Notes |
|----|----|-------|-------|
| 0 | 4 | Magic | `LTX1` |
| 4 | 4 | Flags | `HeaderFlagNoChecksum = 1<<1 = 0x2` |
| 8 | 4 | PageSize | DB page size |
| 12 | 4 | Commit | DB size after applying file, in pages |
| 16 | 8 | MinTXID | |
| 24 | 8 | MaxTXID | |
| 32 | 8 | Timestamp | ms since Unix epoch |
| 40 | 8 | PreApplyChecksum | 0 when NoChecksum |
| 48 | 8 | WALOffset | 0 for snapshot/journal |
| 56 | 8 | WALSize | 0 for snapshot/journal |
| 64 | 4 | WALSalt1 | 0 for journal/compaction |
| 68 | 4 | WALSalt2 | 0 for journal/compaction |
| 72 | 8 | NodeID | 0 if unset |
| 80 | 20 | Reserved | zeros |

`IsSnapshot()` ⇔ `MinTXID == 1`.

#### Page frame (current v3 encoder)

```
[ Pgno u32 ][ Flags u16 ][ CompressedSize u32 ][ LZ4-block payload ]
```

- `PageHeaderFlagSize = 0x1` — always set by the current encoder; a 4-byte compressed-size
  prefix follows. Payload must decompress to exactly `PageSize` bytes.
- A **zero 6-byte page header** terminates the page block (no size prefix, no data).
- Pages are written in **ascending page-number order**; the **lock page is always skipped**.

#### Page index

Three consecutive **unsigned varints** per entry: `(pageNo, absolute-file-offset, encoded-frame-size)`.
Terminated by a zero page-number varint, then an **8-byte big-endian** value = total byte size of the
varint entries (including the zero terminator, excluding the size field itself).

#### Trailer (16 bytes)

```
[ PostApplyChecksum u64 ][ FileChecksum u64 ]
```

#### Checksums

- **Page checksum** `ChecksumPage(pgno, data)` = `(ChecksumFlag | CRC64-ISO(pgno_be ++ data))`, where
  `ChecksumFlag = 1<<63`.
- **File checksum** = `(ChecksumFlag | CRC64-ISO(header_bytes ++ per-page (page_header_bytes ++
  compressed_size_prefix ++ **decompressed** page data) ++ zero_6byte_page_header ++ page_index_bytes
  ++ trailer_post_apply_checksum))`. The final file-checksum field is **not** included.
- DB rolling checksum = `ChecksumFlag | (prev ^ ChecksumPage(pgno, data))`, skipping the lock page.

#### Lock page

`PENDING_BYTE = 0x40000000` (1 GB); `LockPgno(pageSize) = floor(PENDING_BYTE / pageSize) + 1`.
Skip this page number everywhere.

#### Filenames

`"{minTXID:016x}-{maxTXID:016x}.ltx"`, e.g. `0000000000000001-0000000000000064.ltx`.

### 2.2 SQLite WAL format

- **WAL header (32 bytes):** magic `0x377f0682` ⇒ **little-endian** checksum order,
  `0x377f0683` ⇒ **big-endian**; version `3007000`; page size; checkpoint seq; salt1/salt2;
  header checksum at offset 24 (`s0`, `s1` 4 bytes each).
- **Frame header (24 bytes):** page no, db size (commit flag = nonzero), salt1/salt2 (must match
  header), cumulative checksum (2×u32). Followed by `pageSize` bytes of data.
- **WAL checksum** `WALChecksum(bo, s0, s1, bytes)`: for each 8-byte unit,
  `s0 += u32(a) + s1; s1 += u32(a+4) + s0`.
- **`WALReader.pageMap()`** returns the latest-offset per page among **committed** transactions,
  the max frame offset, and the final db size (commit). Frames whose page no exceeds the final
  commit are dropped (DB shrank via VACUUM).

### 2.3 TXID derivation

TXIDs are **sequential**, not derived from SQLite. On each sync, `txID = prevPos.TXID + 1`. Position
persists by reading the highest local L0 LTX file (its header carries `WALOffset`, `WALSize`,
`WALSalt1`, `WALSalt2` to resume reading mid-WAL after restart).

---

## 3. Tech stack & dependencies

| Concern | Choice | Why |
|---------|--------|-----|
| SQLite | **`node:sqlite`** (`DatabaseSync`) | Built-in (Node 24), no native compile. Confirm WAL/PRAGMA/read-txn/checkpoint behavior in tests. |
| LZ4 **block** codec | **`lz4-napi` (native) with `lz4js` (pure-JS) fallback** | LTX uses block format. Follow `@platformatic/kafka`'s native→JS fallback pattern (`crc32c.ts`). |
| CRC-64 ISO | **Implement in TS** (`src/ltx/crc64.ts`) | No good npm pkg; table-based, test against Go vectors (`"123456789"` ⇒ `b90956c775a41001`). |
| Binary buffer accumulation | `@platformatic/dynamic-buffer` *(optional)* | Nice for streaming WAL/page/index; not required for MVP (plain `Buffer` is fine). |
| Test runner | **`node:test`** + `node:assert/strict` in `.test.ts` | Built-in, runs `.ts` directly via type stripping. |
| Type-check only | **`tsc --noEmit`** | Emit deferred; runtime runs `.ts` directly. |
| Config | Plain TS objects; YAML via `yaml` (optional, later) | MVP uses code/tests; CLI/config later. |

**Install:** `npm i lz4js` (baseline) and optionally `lz4-napi`, `@platformatic/dynamic-buffer`,
`yaml`. No other heavyweight runtime deps.

### Type-stripping project settings (mirroring `@platformatic/kafka`)

`tsconfig.json`:
```jsonc
{
  "compilerOptions": {
    "target": "ESNext",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "skipLibCheck": true,
    "noImplicitAny": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "allowImportingTsExtensions": true,
    "rewriteRelativeImportExtensions": true,
    "verbatimModuleSyntax": true,
    "allowSyntheticDefaultImports": true,
    "esModuleInterop": true,
    "resolveJsonModule": true,
    "useUnknownInCatchVariables": false,
    "noEmit": true
  },
  "include": ["src/**/*.ts", "test/**/*.ts"]
}
```
Rules for type stripping: **no `enum`**, no legacy decorators, no parameter properties, import with
explicit `.ts` extensions, use `import type` for type-only imports.

`package.json` (dev):
```jsonc
{
  "type": "module",
  "engines": { "node": ">=24" },
  "scripts": {
    "test": "node --test 'test/**/*.test.ts'",
    "typecheck": "tsc -p . --noEmit"
  }
}
```

---

## 4. Project layout

```
sqlite-js-stream/
  package.json
  tsconfig.json
  PLAN.md
  README.md
  src/
    index.ts                 # public entry: Litestream, DB, Replica, ReplicaClient, helpers
    types.ts                 # TXID, Checksum, Pos, SyncStatus, Config types
    db.ts                    # DB class
    replica.ts               # Replica class
    store.ts                 # Store class (multi-DB, scheduling)
    replica-client.ts        # ReplicaClient interface + Fetch helpers
    config.ts                # config parsing / replica URL
    ltx/
      index.ts
      crc64.ts               # CRC-64 ISO (table-based, reflected, init/xorout all-ones)
      checksum.ts            # ChecksumPage, rolling checksum, ChecksumReader
      constants.ts           # Magic, sizes, flags, LockPgno, filename regex
      types.ts               # Header, PageHeader, PageIndexElem, Trailer, FileInfo, Pos
      header.ts              # Header marshal/unmarshal/validate
      page.ts                # PageHeader marshal/unmarshal
      page-index.ts          # varint encode/decode, DecodePageIndex/EncodePageIndex
      trailer.ts             # Trailer marshal/unmarshal/validate
      filename.ts            # FormatFilename / ParseFilename / ParseTXID / ParseChecksum
      lz4.ts                 # LZ4 block adapter (native → js fallback)
      encoder.ts             # Encoder: header, page, index, trailer, file checksum
      decoder.ts             # Decoder: header, page, index, trailer, DecodeDatabaseTo
      compactor.ts           # Compactor: merge readers
      file-info.ts           # FileInfo, FileIterator, FileInfoSliceIterator
    wal/
      index.ts
      reader.ts              # WALReader: header/frames/checksum/pageMap, with-offset
      checksum.ts            # WALChecksum
    backends/
      file/
        index.ts
        replica-client.ts    # FileReplicaClient
      s3/                    # later
    util/
      atomic-file.ts         # write-to-temp + rename + fsync
      limit-read-closer.ts   # partial reads
      resumable-reader.ts    # resumable stream reads (later)
      sqlite.ts              # node:sqlite helpers (open w/ WAL, page size, read txn)
  test/
    helpers/
      make-db.ts             # create a WAL db with known rows via node:sqlite
      make-ltx.ts            # build a FileSpec / random pages
    ltx/*.test.ts
    wal/*.test.ts
    backends/file/*.test.ts
    db/*.test.ts
    replica/*.test.ts
    store/*.test.ts
    integration/*.test.ts    # end-to-end replicate + restore
  testdata/
    golden/                  # *.ltx files produced by Go (authoritative fixtures)
```

---

## 5. Public API we expose (MVP)

```ts
import { Litestream, DB, Replica, ReplicaClient, FileReplicaClient, Config } from '...'

type Config = {
  dbs: Array<{
    path: string
    replicas: Array<{ url: string }>         // 'file:///data/repl'
  }>
  snapshotInterval?: number
  snapshotRetention?: number
  l0Retention?: number
}

const ls = new Litestream(config)
await ls.open()                              // opens all DBs, starts monitors

const db = ls.getDB('/data/app.db')          // or DB.open(path)
await db.sync()                               // WAL → LTX, uploads via ReplicaClient
const status = await db.syncStatus()          // { localTXID, remoteTXID, inSync }
await db.syncAndWait()
await db.ensureExists()                       // restore local DB if missing, before open

const replica = db.replica                    // Replica
await replica.restore({ outputPath: '/data/restored.db' })  // apply snapshot + L0
await db.close()
```

`ReplicaClient` interface (port of `replica_client.go`):
```ts
interface ReplicaClient {
  type(): string
  init(ctx: AbortSignal): Promise<void>
  ltxFiles(opts: { level: number; seek: bigint; useMetadata: boolean }): AsyncIterable<FileInfo>
  openLtxFile(opts: { level: number; minTXID: bigint; maxTXID: bigint; offset: bigint; size: bigint }): Promise<Readable>
  writeLtxFile(opts: { level: number; minTXID: bigint; maxTXID: bigint }, body: Readable): Promise<FileInfo>
  deleteLtxFiles(infos: FileInfo[]): Promise<void>
  deleteAll(): Promise<void>
  setLogger(l: Logger): void
}
```

---

## 6. TDD methodology

- **Test runner:** `node:test` + `node:assert/strict`, run on `.ts` files directly.
- **Unit tests** for each pure module (crc64, checksum, header, page-index, filename, wal reader).
- **Round-trip tests:** encode → decode → assert identical header/pages/trailer; verify file
  checksum.
- **Interop tests (/ goldens):** the most important. Generate authoritative goldens with the Go
  `superfly/ltx` (`go run` harness, committed in CI under `testdata/golden/`), then assert our
  encoder produces **byte-identical** files for the same logical content, and our decoder reads
  Go-produced files. Because Go is not installed in this dev env, we:
  1. Hand-derive minimal golden byte layouts from the spec for MVP.
  2. Add a Go golden generator script (`scripts/gen-goldens/`) to run in CI.
- **Integration tests:** building a real WAL database with `node:sqlite`, replicating to a temp
  `file://` client, then restoring and running `PRAGMA integrity_check` + comparing row counts.
- **Fuzz/property:** random pages → encode → decode → identical pages (page order preserved).

**Fixture helpers:** `test/helpers/make-db.ts` (create WAL DB, insert `n` rows, flush commits),
`test/helpers/make-ltx.ts` (deterministic page payloads).

---

## 7. Milestones (minimum functionality first)

### Milestone 0 — Project scaffolding (TDD infra) ✅ DONE

- `git init`, `package.json`, `tsconfig.json`, `.gitignore`.
- Verify: `node --test` runs a `.test.ts`; `tsc --noEmit` passes; `.ts` runs directly.
- Add a trivial passing test + `npm test` / `npm run typecheck` scripts.

### Milestone 1 — LTX format core (pure, no DB) — **foundation** ✅ DONE

Implement and unit-test, in order:

1. **`crc64.ts`** — CRC-64 ISO table + update (reflected, init/xorout all-ones).
   ✔ Vector: `"123456789"` ⇒ `b90956c775a41001`.
2. **`checksum.ts`** — `ChecksumPage(pgno,data)`, rolling DB checksum (`ChecksumReader`), lock-page
   skip.
3. **`constants.ts` / `filename.ts` / `types.ts`** — sizes, flags, `LockPgno`, filename format/parse,
   `TXID`/`Checksum` string/JSON formats.
4. **`header.ts` / `page.ts` / `trailer.ts`** — marshalling/unmarshalling + `Validate()`.
5. **`page-index.ts`** — varint encode/decode; `EncodePageIndex`/`DecodePageIndex`.
6. **`lz4.ts`** — block codec adapter (`lz4-napi`→`lz4js`, both expose block functions).
7. **`encoder.ts`** — `EncodeHeader`, `EncodePage` (compress + size prefix), `EncodePageIndex`,
   `Close` (compute file checksum). Support `HeaderFlagNoChecksum`.
8. **`decoder.ts`** — `DecodeHeader`, `DecodePage`, `DecodePageIndex`, `Close` (verify file
   checksum), `DecodeDatabaseTo`.
9. **`file-info.ts`** — `FileInfo`, `FileIterator`, `FileInfoSliceIterator`.

**Acceptance tests:**
- Round-trip a hand-built `FileSpec` (header + pages + trailer) ⇒ identical bytes.
- File-checksum verification fails on a single flipped byte, passes otherwise.
- `DecodePageIndex` returns a map keyed by page no with correct offsets/sizes.
- Lock page (`pgno == LockPgno(pageSize)`) is skipped on encode and `ChecksumReader`.
- Golden byte-layout tests for a minimal file.

### Milestone 2 — WAL reader (pure, SQLite-driven fixtures) ✅ DONE

- **`wal/checksum.ts`** — `walChecksum(byteOrder, s0, s1, bytes)`.
- **`wal/reader.ts`** — `WALReader`: header parse + validation, frame read + salt + checksum
  verification, `pageMap()` (committed pages only, db size, max offset), `newWALReaderWithOffset`
  (resume at offset with known salts).

**Acceptance tests:**
- Build a real WAL DB with `node:sqlite` (multiple commits), read all frames; assert page nos/data
  match SQLite's view and final commit size.
- `pageMap()` returns latest page versions and the correct end offset/db size.
- Corrupt a salt / checksum ⇒ `EOF` (frame mismatch) as litestream does.
- `withOffset` resumes correctly after a crafted multi-frame WAL.

### Milestone 3 — ReplicaClient interface + File backend ✅ DONE

- **`src/replica-client.ts`** — the interface + `FetchLTXHeader`, `FetchPageIndex`, `FetchPage`
  helpers (partial-read + trailer/varint logic).
- **`src/backends/file/replica-client.ts`** — layout `<root>/ltx/<level>/<min>-<max>.ltx`,
  atomic write (temp + rename + fsync), preserve file `mtime` = LTX header timestamp, iterator,
  delete, delete-all.
- **`src/util/atomic-file.ts`**, `src/util/limit-read-closer.ts`.

**Acceptance tests:**
- `writeLtxFile` then `openLtxFile` returns identical bytes.
- `ltxFiles` lists sorted by TXID, skips non-conforming names, honors `seek`.
- Atomic write leaves no `.tmp` on failure; `deleteLTXFiles` and `deleteAll` behave.
- `FetchPageIndex` on our written file decodes a correct index map.

### Milestone 4 — DB + Replica end-to-end replication (the MVP) ✅ DONE

This is the core. Implement:

- **`src/util/sqlite.ts`** — open DB via `node:sqlite`, `PRAGMA journal_mode=WAL`, read `page_size`,
  `page_count`; manage a long-running **read transaction** (pin a snapshot; prevent WAL truncation
  past our sync point) and a separate **control** connection for checkpoints. *(Verify concurrency
  behavior: read txn on connection A, `PRAGMA wal_checkpoint` on connection B.)*
- **`src/db.ts`** — `open` (page size, meta dir `<dir>/.<name>-litestream`, read txn, local L0 cache),
  `sync()` (compute next TXID, `pageMap`, write LTX via `Encoder`, upload via client, update local
  L0 cache), `checkpoint()` (PASSIVE/TRUNCATE with thresholds), snapshot-on-salt-reset
  (`writeLTXFromDB`), `restore`, `ensureExists`, `syncStatus`, `syncAndWait`, `close`.
- **`src/replica.ts`** — holds `ReplicaClient`, tracks `Pos` from local L0, `sync()` uploads,
  `restore()` = `CalcRestorePlan` (snapshot + contiguous L0) → `Compactor` → `DecodeDatabaseTo`.
- **`src/replica-client.ts` helper** `CalcRestorePlan` (choose snapshot + subsequent files by
  TXID/timestamp).

**Acceptance (integration) tests:**
- Create DB, insert N rows → `sync()` → LTX files appear on the `file://` replica with contiguous
  TXIDs; `syncStatus().inSync` true.
- Insert more rows → `sync()` → next contiguous TXID; old LTX still present (immutable).
- Restart `DB` (re-open) → reproduces position from local L0 → new rows produce next TXID.
- `replica.restore({ outputPath })` → resulting DB opens with `node:sqlite`, `PRAGMA
  integrity_check` = ok, row count matches, no `-wal`/`-shm` leftover.
- `db.ensureExists()` restores when local file is absent, no-ops when present.
- Salt reset forces a snapshot (full DB → LTX) and resume.

### Milestone 5 — Store + config + monitoring + compaction (post-MVP, still core) ✅ DONE (core)

- **`src/store.ts`** — open many DBs concurrently, start per-DB monitor loops, schedule snapshot
  interval, run retention (`syncAll`, `snapshotAll`, `compactAll`, `enforceRetentionAll`, `run`).
- **`src/ltx/compactor.ts`** — in-memory merge of LTX readers (page dedup, lock-page skip, latest
  page wins) used by restore; the streaming `StreamingCompactor` (page-by-page, bounded memory) is
  used by multi-level compaction.
- **`src/replica.ts`** — **compaction scheduling** (`compact()`/`compactLevel()` merging a level's
  pending files into the next, up to `maxCompactionLevel`), **retention/GC**
  (`enforceRetention()`) that prunes old snapshots and superseded files, and interval-driven
  `compactionIntervalMs`/`retentionIntervalMs` inside `run()`.
- **`src/config.ts`** — parse replica URLs (`file://`, later `s3://`), DB config.

**Acceptance:** multi-DB store; snapshot rotation; compaction merges L0→L1→…→max and verifies
contiguous TXIDs; retention prunes old L0/snapshots; restore still correct after compaction; position
and WAL-resumption survive compaction + GC (incremental sync continues without reusing TXIDs).

✅ **Implemented (post-MVP hardening):** multi-level streaming compaction (L0→L1→L2→L3 via
`StreamingCompactor`), snapshot + coverage-based retention/GC, and interval scheduling in
`Replica.run()`/`Store.run()`. Restore now builds its plan across all compaction levels, preferring
the highest level for a TXID range.

### Milestone 6+ — Advanced / optional (NOT required for MVP)

- **Cloud backends:** `s3/`, `gs/`, `abs/`, `sftp/`, `nats/` replica clients + their leaser
  (`lock.json` conditional writes).
- **Distributed leasing** (`src/leaser.ts`).
- **VFS real-time mirroring** (`vfs.go` port — hook into SQLite VFS; likely deferred).
- **IPC Unix-socket control server** (`server.go`).
- **`litestream ltx`/CLI wrapper** only if a CLI is desired.

---

## 8. Known risks & open questions

1. **`node:sqlite` concurrency semantics.** Whether a long-running read txn on one
   `DatabaseSync` genuinely blocks WAL truncation while a **separate** connection runs
   `wal_checkpoint`, and whether `PRAGMA wal_autocheckpoint` must be raised (litestream disables
   SQLite's auto-checkpoint so it can control checkpoints). Must be verified in Milestone 4 with
   explicit tests; if `node:sqlite` can't hold cross-connection snapshot pins the way Go/`modernc`
   can, we fall back to **reading WAL before SQLite checkpoints** opportunistically and only
   truncating after we've synced (matching litestream's `checkpoint` ordering) plus accepting a
   snapshot window.
2. **Golden interoperability.** Go is not installed in the dev environment. We hand-derive fixtures
   and add a Go golden generator for CI. Until goldens are committed, interop is validated by
   round-trip + spec conformance only.
3. **LZ4 block vs frame.** LTX requires **block** format; `@platformatic/wasm-utils` is frame-only.
   `lz4-napi` (block) and `lz4js` (block) both verified. Prefer `lz4-napi` for perf with `lz4js`
   fallback, but ensure the fallback path is tested for identical decode results.
4. **CRC-64 ISO vs ECMA.** **ISO must be used** (mismatch = broken interop). Provide a direct vector
   test to lock this in.
5. **TXID/position persistence.** Position is derived from local L0 LTX headers (no separate pos
   file). Ensure we reconstruct `WALOffset`/salts on restart exactly.
6. **Type stripping limits.** No `enum`/decorators/parameter properties; use `import type`; explicit
   `.ts` extensions. Add a lint guard (eslint rule or tsconfig) to prevent violations.
7. **Memory/perf on large DBs.** ✅ **Streaming implemented (post-MVP).** `restore()` streams each
   LTX file page-by-page into the output DB (snapshot base + incremental overlay) and
   `sync()`/`snapshot()` read pages from a file handle and stream the encoded LTX straight to disk
   (`FileSink`); the S3 addon uses multipart upload for objects > 5 MiB. Peak memory is bounded by
   one page buffer + the smallest page index. The WAL is still read in full (it is small relative
   to the DB), and a >1 GB lock-page validation is a further stretch goal.

---

## 9. Out of scope for MVP (documented, deferred)

- Cloud object-store backends (S3/GCS/Azure/SFTP/NATS).
- Distributed leasing / lock.json.
- VFS real-time mirroring.
- Unix-socket IPC server.
- Full CLI (`litestream replicate`, `restore`, `ltx`, `reset`, `snapshots`).
- Metrics/telemetry (Prometheus) and rich diagnostics.
- `--txid`/timestamp point-in-time restore edge cases and `follow` mode.

---

## 10. Reference mapping (Go → TS) quick index

| Go | TS |
|----|----|
| `internal/ltx` (superfly) | `src/ltx/*` |
| `wal_reader.go`, `WALChecksum` | `src/wal/*` |
| `replica_client.go` interface | `src/replica-client.ts` |
| `file/replica_client.go` | `src/backends/file/replica-client.ts` |
| `db.go` | `src/db.ts` |
| `replica.go` | `src/replica.ts` |
| `store.go`, `compactor.go` | `src/store.ts`, `src/ltx/compactor.ts` |
| `leaser.go`, `s3/leaser.go` | `src/leaser.ts` (later) |
| `vfs.go` | deferred |
| `server.go` | deferred |

---

## 11. Definition of done for MVP (Milestones 0–4)

- [ ] `npm test` green; `tsc --noEmit` clean.
- [ ] LTX encoder/decoder pass round-trip + golden + checksum-verification tests.
- [ ] WAL reader parses real `node:sqlite` WAL files, `pageMap` correct.
- [ ] `FileReplicaClient` writes/reads/deletes in the correct directory layout.
- [ ] End-to-end test: SQLite DB → `sync()` → `file://` replica → `restore()` → valid DB.
- [ ] `syncStatus`/`syncAndWait`/`ensureExists` work.
- [ ] No checkpoint truncates WAL before we've synced it.

---

*Authoritative references:* `superfly/ltx` source (`/tmp/ltx`), litestream source (`/tmp/litestream`)
and its `docs/ARCHITECTURE.md`, `docs/SQLITE_INTERNALS.md`, `docs/LTX_FORMAT.md` (note: LTX_FORMAT.md
is the *only* doc that incorrectly says CRC-64 ECMA — the code uses ISO), and
`@platformatic/kafka` + `@platformatic/wasm-utils` for the native→WASM→JS utility pattern.
