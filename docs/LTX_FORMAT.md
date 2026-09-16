# LTX File Format

*The LTX (Log Transaction) format is from [`superfly/ltx`](https://github.com/superfly/ltx) and
Litestream (Apache-2.0). Documented here for the Node.js port `sqlite-js-stream`.*

An LTX file is an immutable, self-contained sequence of SQLite pages with header and checksum
metadata. It is logically a **set of page overrides** applied to a database in ascending page order,
tagged with a transaction range `(MinTXID, MaxTXID)`.

```
[ Header (100 bytes) ][ page block (zero-terminated) ][ page index ][ Trailer (16 bytes) ]
```

## Header frame (100 bytes, big-endian)

| Offset | Size | Field | Notes |
|-------:|-----:|-------|-------|
| 0 | 4 | Magic | `"LTX1"` |
| 4 | 4 | Flags | `HeaderFlagNoChecksum = 1<<1` (`0x2`) |
| 8 | 4 | PageSize | SQLite page size |
| 12 | 4 | Commit | DB size after applying this file, in pages |
| 16 | 8 | MinTXID | |
| 24 | 8 | MaxTXID | |
| 32 | 8 | Timestamp | milliseconds since Unix epoch |
| 40 | 8 | PreApplyChecksum | rolling DB checksum before applying (0 when `NoChecksum`) |
| 48 | 8 | WALOffset | file offset in the original WAL; 0 for journal/compaction |
| 56 | 8 | WALSize | size of the original WAL segment; 0 for journal/compaction |
| 64 | 4 | WALSalt1 | WAL salt; 0 for journal/compaction |
| 68 | 4 | WALSalt2 | WAL salt; 0 for journal/compaction |
| 72 | 8 | NodeID | node that created the file; 0 if unset |
| 80 | 20 | Reserved | zero |

`IsSnapshot()` ⇔ `MinTXID == 1`.

## Page frame

```
[ Pgno (u32) ][ Flags (u16) ][ CompressedSize (u32) ][ LZ4-block payload ]
```

- `PageHeaderFlagSize = 0x1` — when set (always, in the current writer), a 4-byte size prefix
  follows the 6-byte page header and the payload is an **LZ4 block**.
- The payload decompresses to exactly `PageSize` bytes.
- A **zero 6-byte page header** terminates the page block.
- Pages are in **ascending page-number order**; the **lock page** is always skipped.
- `LockPgno(pageSize) = 0x40000000 / pageSize + 1` (the page at the 1 GB `PENDING_BYTE`).

## Page index

Three unsigned **LEB128 varints** per entry: `(pageNo, absolute-file-offset, encoded-frame-size)`,
in ascending page order. Terminated by a zero page-number varint, then an **8-byte big-endian**
value equal to the total byte size of the varint entries (including the zero terminator, excluding
the size field itself).

## Trailer frame (16 bytes, big-endian)

```
[ PostApplyChecksum (u64) ][ FileChecksum (u64) ]
```

## Checksums

The checksum is **CRC-64 ISO** (`crc64.ISO`), *not* ECMA. (The litestream docs describe ECMA, but the
`superfly/ltx` code uses `crc64.MakeTable(crc64.ISO)`. This port follows the code.)

- **Page checksum**: `ChecksumFlag | CRC64-ISO( pgno_be_4 ++ page_data )`, where `ChecksumFlag = 1<<63`.
- **Rolling DB checksum**: `ChecksumFlag | (prev ^ ChecksumPage(pgno, data))` over each page,
  **skipping the lock page**. Used as `PreApplyChecksum` (before) and `PostApplyChecksum` (after).
- **File checksum**: `ChecksumFlag | CRC64-ISO( header_bytes ++ per-page( page_header_bytes ++
  compressed_size_prefix ++ **decompressed** page_data ) ++ zero_6byte_page_header ++
  page_index_bytes ++ trailer_post_apply_checksum )`. The file checksum field itself is excluded.

When `HeaderFlagNoChecksum` is set, the pre/post-apply checksums must be `0` and only the **file
checksum** is required.

## Names

Files are named `{minTXID:016x}-{maxTXID:016x}.ltx`, e.g. `0000000000000001-0000000000000064.ltx`,
and stored at `<root>/ltx/<level>/<name>`.

## CRC-64 ISO reference (JS)

The port implements CRC-64 ISO with a reflected table (init/xorout all-ones, polynomial register
value `0xD800000000000000`), verifying against the published check value:

```
crc64ISO("123456789") === 0xb90956c775a41001n
```
