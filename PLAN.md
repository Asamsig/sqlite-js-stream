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

> **Post-MVP hardening & features completed:** streaming for large DBs (`StreamLtxReader` /
> `AsyncByteReader` / `FileSink`, S3 multipart ≥5 MiB), multi-level compaction + retention/GC
> (L0→…→L9 with litestream-style time-bucketed scheduling), a **pnpm monorepo** with an **S3 addon**
> (`packages/backend-s3`), distributed **leasing** (`lock.json` conditional writes), and continuous
> **follow-restore** (`Replica.follow()`), and a typed-error convention (`SqliteJsStreamError` + `.code`).
> Test suite: **135 tests (117 core + 18 S3)**, green on Node 24 & 26.

> **Publishing + linting completed:** packages are scoped to `@platformatic/*` and published from a
> **type-stripping build pipeline** — `tsc` transpiles `src/*.ts` → `dist/*.js` (types erased) plus
> `dist/*.d.ts` declarations, with relative specifiers rewritten to `.js` for consumers. This is
> required because Node refuses to type-strip `.ts` files under `node_modules`. Dev/test still run
> `.ts` directly via native type stripping. A release workflow publishes with `--provenance`. Linting
> uses **ESLint + neostandard** (`ts: true`) as the baseline plus a type-stripping guard
> (`no-restricted-syntax` rejecting `enum`, parameter properties, `namespace`/`module`, and
> `import x = require(...)`) and a `require-ts-extension` rule.

> **Interop note (Milestone 1):** Our `compressBlock` is a byte-identical TypeScript port of Go's
> `pierrec/lz4/v4` Fast block compressor, so the LTX files we write match Go's `superfly/ltx` encoder
> exactly. The canonical Go golden file-checksum vectors
> (`0xf61cfb1887ed139e`, `0x8433a627b12d0fd4`) are pinned in `test/ltx/golden.test.ts` and are
> re-encoded byte-for-byte in `test/ltx/go-interop.test.ts`, so any drift in our encoder, decoder, or
> the Go format is caught by the `interop-go` CI job. `lz4js` is retained only for LZ4 block
> **decompression** (which need not be byte-identical).

> **Remaining / next steps.** The in-scope library is functionally complete. The last *operational*
> step is the actual **npm release**: create a `v0.1.0` tag to trigger `release.yml` and configure the
> `NPM_TOKEN` repository secret (the tag can be created by an agent; the secret and the publish itself
> are operator-owned). Deferred/optional features remain documented in §9 below — a CLI + YAML config,
> metrics/telemetry, point-in-time restore edge cases, VFS real-time mirroring, and an IPC control
> server.

---

## 1. What Litestream Is (and what we port)

Litestream runs as a background process that watches a SQLite database's **WAL file**, converts the
changing pages into immutable **LTX (Log Transaction) files**, and replicates them to a replica
destination (S3, or local files). It also produces **snapshots** (full DB → LTX)
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
| `leaser.go`, `s3/leaser.go`   | Distributed lease (lock.json) — ✅ implemented                            | `src/leaser.ts` + `src/backends/file/leaser.ts` + `packages/backend-s3/src/leaser.ts` |
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
| LZ4 codec | **`lz4js` for decode + a TS port of Go's `pierrec/lz4/v4` for encode**, LTX **block** format only | LTX uses block format. Our `compressBlock` is a **byte-identical** TS port of Go's Fast block compressor, so file bytes/checksums match Go exactly; `lz4js` handles **decompression** (which need not be byte-identical). |
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
    index.ts                 # public entry: VERSION, Replica, Store, ReplicaClient, helpers,
                             #   LTX/WAL module re-exports, leasers, logger
    replica.ts               # Replica class (driver-agnostic: takes a path + client)
    store.ts                 # Store class (multi-DB, scheduling)
    replica-client.ts        # ReplicaClient interface + ltxDir/ltxFilePath/fetch* helpers
    replica-url.ts           # parseReplicaURLWithQuery + registerReplicaClientFactory +
                             #   newReplicaClientFromURL (scheme registry)
    db.ts                    # internal: node:sqlite snapshot pinning (NOT exported)
    leaser.ts                # Leaser interface
    ltx/
      index.ts               # re-exports the LTX module
      crc64.ts               # CRC-64 ISO (table-based, reflected, init/xorout all-ones)
      checksum.ts            # ChecksumPage, rolling checksum, ChecksumReader
      constants.ts           # Magic, sizes, flags, LockPgno, SnapshotLevel, filename regex
      types.ts               # Header, PageHeader, PageIndexElem, Trailer, FileInfo, Pos
      header.ts              # Header marshal/unmarshal/validate
      page.ts                # PageHeader marshal/unmarshal
      page-index.ts          # varint encode/decode, DecodePageIndex/EncodePageIndex
      trailer.ts             # Trailer marshal/unmarshal/validate
      filename.ts            # FormatFilename / ParseFilename / ParseTXID / ParseChecksum
      lz4.ts                 # LZ4 block adapter (lz4js)
      encoder.ts             # Encoder: header, page, index, trailer, file checksum
      decoder.ts             # Decoder: header, page, index, trailer, DecodeDatabaseTo
      compactor.ts           # in-memory Compactor: merge readers
      streaming-compactor.ts # page-by-page bounded-memory compactor (multi-level)
      stream.ts              # StreamLtxReader / AsyncByteReader / FileSink
      file-info.ts           # FileInfo, FileIterator, FileInfoSliceIterator
      compaction-level.ts    # CompactionLevels, defaultCompactionLevels, bucket helpers
      byte-reader.ts         # ByteReader helpers
      bytes.ts               # byte-vector read/write helpers
      sink.ts                # output sink for encoded LTX
    wal/
      index.ts               # re-exports the WAL module
      reader.ts              # WALReader: header/frames/checksum/pageMap, with-offset
      checksum.ts            # WALChecksum
      constants.ts           # WAL magic / salts / version constants
    backends/
      file/
        index.ts             # re-exports the file backend
        replica-client.ts    # FileReplicaClient
        leaser.ts            # FileLeaser
    util/
      fs.ts                  # write-to-temp + rename + fsync, mkdir, rm
      stream.ts              # stream plumbing / AsyncByteReader / FileSink
      logger.ts              # Logger interface + noopLogger
  packages/
    backend-s3/              # monorepo addon: S3ReplicaClient + S3Leaser (+ scheme registry)
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

The library is **driver-agnostic.** `Replica`/`Store` take a database **path** and read the DB file
+ `-wal` directly from disk, so the application can use any SQLite driver (or none).
`DB`/`Database`/`openDatabase`/`Litestream`/`Config` are intentionally **not** exported;
`node:sqlite` is used internally only to pin a read-only snapshot.

```ts
import {
  Replica, Store, FileReplicaClient, FileLeaser,
  newReplicaClientFromURL, registerReplicaClientFactory,
  ReplicaClient, CompactionLevels, defaultCompactionLevels, calcRestorePlan,
  VERSION,
} from '@platformatic/sqlite-js-stream'

// File-backed replica: `path` is the SQLite DB; the client is a storage backend.
const client = new FileReplicaClient('/data/repl')
const replica = new Replica('/data/app.db', client)   // reads app.db + app.db-wal directly

await replica.sync()                        // WAL → LTX, uploads via client
await replica.snapshot()                    // full level-9 snapshot
const status = await replica.syncStatus()   // { localTXID, remoteTXID, inSync }
await replica.syncAndWait()
await replica.ensureExists()                // restore local DB if missing, before open
await replica.run({ intervalMs: 5000, signal })   // continuous sync (primary/follower w/ leaser)
await replica.compact()                     // litestream-style per-level compaction schedule
await replica.enforceRetention()            // time-based snapshot + L0 retention / GC

await replica.restore({ outputPath: '/data/restored.db', timestamp })  // snapshot + L0 overlay
await replica.follow({ outputPath: '/data/replica.db', signal })   // continuous read-only restore
replica.close()

// Multi-DB coordination
const store = new Store()
await store.openDB('/data/app.db', client)
await store.run({ intervalMs })             // syncAll + snapshotAll + compactAll + retention
store.close()

// URL-based factory + addon scheme registry
registerReplicaClientFactory('s3', s3Factory)   // S3 addon auto-registers 's3' on import
const c = newReplicaClientFromURL('s3://bucket/prefix')
```

`ReplicaClient` interface (port of `replica_client.go`):
```ts
interface ReplicaClient {
  type(): string
  init(): Promise<void>
  ltxFiles(opts: { level: number; seek: TXID; useMetadata: boolean }): AsyncIterable<FileInfo>
  openLtxFile(opts: { level: number; minTXID: TXID; maxTXID: TXID; offset: number; size: number }): Promise<Readable>
  writeLtxFile(opts: { level: number; minTXID: TXID; maxTXID: TXID }, body: AsyncIterable<Uint8Array | string>): Promise<FileInfo>
  deleteLtxFiles(infos: FileInfo[]): Promise<void>
  deleteAll(): Promise<void>
  setLogger(l: Logger): void
}
```

The **S3 backend is a monorepo addon** (`@platformatic/sqlite-js-stream-backend-s3`)
registered via `registerReplicaClientFactory('s3', …)` on import.

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
6. **`lz4.ts`** — LZ4 **block** codec adapter (`lz4js`).
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
`StreamingCompactor`), litestream-style compaction scheduling and retention, and interval scheduling
in `Replica.run()`/`Store.run()`. Restore now builds its plan across all compaction levels,
preferring the highest level for a TXID range.

**Litestream-alignment (committed):** per-level **time-bucketed** compaction (`CompactionLevels`,
`compactDB`/`compactSnapshot`, L1=30s/L2=5m/L3=1h defaults); time-based snapshot retention with an
always-kept-newest guard; TXID-boundary cascade through levels; L0 **time retention** that never
deletes the live-head L0 file. The legacy single-interval `compactionIntervalMs` path is preserved.

**Deliberately out of scope:** legacy v0.3.x **generation** backup reads (`generations/…`). Our write
format is already the modern `superfly/ltx v0.5.2` LTX (same as current litestream), and we do not
wish to add the LZ4-frame codec + generation-listing interfaces needed to recover old v0.3.x
backups.

### Milestone 6+ — Advanced / optional (NOT required for MVP)

- **S3 backend** — ✅ **implemented** as a monorepo addon `packages/backend-s3`:
  `S3ReplicaClient` (multipart upload ≥5 MiB, `If-None-Match`/`If-Match` conditional writes) + `S3Leaser`. Importing it registers the `s3` scheme so
  `newReplicaClientFromURL('s3://bucket/prefix')` resolves.
- **Distributed leasing** — ✅ **implemented** (`src/leaser.ts` + file/S3 leasers): a `Leaser`
  acquires/renews/releases a `lock.json` lease; `Replica.run()` becomes a **primary** (writes) or a
  **follower** (polls), gating all storage-write methods on lease ownership. File backend uses
  exclusive-create + content-hash etag; S3 uses `If-None-Match`/`If-Match` conditional writes.
- **Follow mode** — ✅ **implemented** (`Replica.follow()`): continuously restores new LTX files into
  a read-only replica, resuming from a `-txid` sidecar (see §5 and ARCHITECTURE `Replica.follow()`).
- **VFS real-time mirroring** (`vfs.go` port — hook into SQLite VFS; likely deferred).
- **IPC Unix-socket control server** (`server.go`).
- **CLI** (`litestream replicate/restore/ltx/reset/snapshots`) + YAML config — *not built*; library
  only for now.

---

## 8. Known risks & open questions

1. **`node:sqlite` concurrency semantics.** ✅ **resolved** (verified in `test/integration/checkpoint-concurrency.test.ts`). Whether a long-running read txn on one
   `DatabaseSync` genuinely blocks WAL truncation while a **separate** connection runs
   `wal_checkpoint`, and whether `PRAGMA wal_autocheckpoint` must be raised (litestream disables
   SQLite's auto-checkpoint so it can control checkpoints). Must be verified in Milestone 4 with
   explicit tests; if `node:sqlite` can't hold cross-connection snapshot pins the way Go/`modernc`
   can, we fall back to **reading WAL before SQLite checkpoints** opportunistically and only
   truncating after we've synced (matching litestream's `checkpoint` ordering) plus accepting a
   snapshot window.
2. **Golden interoperability.** ✅ **resolved** — `tools/golden/main.go` generates canonical
   `superfly/ltx` (v0.5.3) LTX fixtures into `testdata/go/`, and `test/ltx/go-interop.test.ts` decodes
   them with our TS decoder (header, pages, page index, trailer, LZ4 block decompression, CRC-64 ISO
   file + post-apply checksums). The `interop-go` CI job regenerates the fixtures and fails if they
   drift (`git diff --exit-code -- testdata/go`). `test/ltx/go-interop.test.ts` also **re-encodes** the
   same logical content and asserts **byte-identical** output, so drift in the format, the decoder, or
   the encoder is caught in both directions.
3. **LZ4 block format.** ✅ **resolved** — `compressBlock` is a **byte-identical** TypeScript port of
   Go's `pierrec/lz4/v4` Fast block compressor (validated against Go's own `CompressBlock` vectors in
   `test/ltx/lz4.test.ts`), so the files we write match Go exactly. `lz4js` is retained only for LZ4
   block **decompression**.
4. **CRC-64 ISO vs ECMA.** ✅ **resolved** – **ISO is used** (mismatch = broken interop). Provide a direct vector
   test to lock this in.
5. **TXID/position persistence.** ✅ **resolved** – position is derived from local L0 LTX headers (no separate pos
   file). Ensure we reconstruct `WALOffset`/salts on restart exactly.
6. **Type stripping limits.** ✅ **resolved** — no `enum`/decorators/parameter properties; use `import type`;
   explicit `.ts` extensions. Guarded by **ESLint + neostandard** (`no-restricted-syntax` rejecting `enum`,
   parameter properties, `namespace`/`module`, `import x = require(...)`) plus a `require-ts-extension` rule.
   Note Node will not type-strip `.ts` under `node_modules`, so the published package ships transpiled `.js` + `.d.ts`.
7. **Memory/perf on large DBs.** ✅ **resolved** – **streaming implemented (post-MVP).** `restore()` streams each
   LTX file page-by-page into the output DB (snapshot base + incremental overlay) and
   `sync()`/`snapshot()` read pages from a file handle and stream the encoded LTX straight to disk
   (`FileSink`); the S3 addon uses multipart upload for objects > 5 MiB. Peak memory is bounded by
   one page buffer + the smallest page index. The WAL is still read in full (it is small relative
   to the DB), and a >1 GB lock-page validation is a further stretch goal.
8. **Single-writer semantics / divergence (important).** This is a backup/DR tool for **one**
   SQLite database, not a distributed database. The lease elects one writer *to storage* but does
   **not** merge or reconcile data across hosts. Multiple hosts may share one prefix **only** when
   they hold the **same** logical data (DR/standby, restored from the same backup); different DBs
   must use different prefixes. Documented in README + ARCHITECTURE.

---

## 9. Out of scope for MVP (documented, deferred)

- Cloud object-store backends — **S3 done** (addon `packages/backend-s3`); further backends are deliberately out of scope.
- Distributed leasing / lock.json — **done** (`src/leaser.ts` + file/S3 leasers).
- VFS real-time mirroring.
- Unix-socket IPC server.
- CLI (`litestream replicate`, `restore`, `ltx`, `reset`, `snapshots`) + YAML config.
- Metrics/telemetry (Prometheus) and rich diagnostics.
- `--txid`/timestamp point-in-time restore edge cases.

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
| `leaser.go`, `s3/leaser.go` | `src/leaser.ts` + `src/backends/file/leaser.ts` + `packages/backend-s3/src/leaser.ts` |
| `vfs.go` | deferred |
| `server.go` | deferred |

---

## 11. Definition of done for MVP (Milestones 0–4)

- [x] `npm test` green (135 tests: 117 core + 18 S3); `tsc --noEmit` clean; `eslint .` clean.
- [x] LTX encoder/decoder pass round-trip + golden + checksum-verification tests.
- [x] WAL reader parses real `node:sqlite` WAL files, `pageMap` correct.
- [x] `FileReplicaClient` writes/reads/deletes in the correct directory layout.
- [x] End-to-end test: SQLite DB → `sync()` → `file://` replica → `restore()` → valid DB.
- [x] `syncStatus`/`syncAndWait`/`ensureExists` work.
- [x] No checkpoint truncates WAL before we've synced it.

---

*Authoritative references:* `superfly/ltx` source (`/tmp/ltx`), litestream source (`/tmp/litestream`)
and its `docs/ARCHITECTURE.md`, `docs/SQLITE_INTERNALS.md`, `docs/LTX_FORMAT.md` (note: LTX_FORMAT.md
is the *only* doc that incorrectly says CRC-64 ECMA — the code uses ISO), and
`@platformatic/kafka` + `@platformatic/wasm-utils` for the native→WASM→JS utility pattern.
