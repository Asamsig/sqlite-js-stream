# Changelog

All notable changes to `@platformatic/sqlite-js-stream` are documented here. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-01

### Added

- Initial release.
- LTX format core: CRC-64 (ISO) checksums, header/page-frame/page-index/trailer
  encoding and decoding, LZ4 block codec (`lz4js`), validated against the
  [superfly/ltx](https://github.com/superfly/ltx) spec.
- SQLite WAL reader that parses header + frames and builds a page map.
- `ReplicaClient` interface plus a local **file** backend.
- `Replica` end-to-end replication: `sync()` (incremental WAL → LTX),
  `snapshot()` (full DB → LTX), `restore()`, streaming for large databases,
  multi-level compaction with retention/GC, and continuous `follow()` restore.
- Distributed leasing (`Leaser`) so `Replica.run()` elects a single writer.
- S3 backend as a separate addon (`@platformatic/sqlite-js-stream-s3`).
- Typed errors: every library error extends `SqliteJsStreamError` with a stable
  Node-style `code`.
- Distributed directly as type-stripped TypeScript source (Node ≥ 24, no build
  step) with accompanying `.d.ts` declarations.

### Notes

- Requires **Node >= 24** (native type stripping + `node:sqlite`).
- This library is a disaster-recovery / backup tool for a **single** SQLite
  database; it does not merge or reconcile data across hosts.
