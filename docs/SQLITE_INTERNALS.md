# SQLite Internals (as used by sqlite-js-stream)

*Adapted from [Litestream](https://github.com/benbjohnson/litestream)'s
`docs/SQLITE_INTERNALS.md` (Apache-2.0).*

Replication depends on the on-disk layout of a SQLite database and its WAL. This page summarizes
what this library relies on.

---

## Database file

A SQLite database is a single file split into fixed-size **pages**. Unused space between pages is
ignored; the file can be sparse. Common page sizes are 512, 1024, 2048, 4096 (default), 8192,
16384, 32768, and 65536 bytes.

The database header (first 100 bytes of page 1) contains the page size, the file change counter, and
the database size in pages (which may differ from the physical file size).

### Lock page

SQLite reserves a **1 GiB offset** (`0x40000000`, the `PENDING_BYTE`) for file locking. For a given
page size, the page containing that byte is:

```
lockPgno(pageSize) = 0x40000000 / pageSize + 1
```

This page is never stored in an LTX file. It is always treated as zero-filled.

---

## Write-Ahead Log (WAL)

In WAL mode, writes go to a `-wal` file instead of the main database, so readers are not blocked by
writers. The WAL is a sequence of **frames**, each containing a 24-byte frame header and a
`pageSize`-byte page image. Committed frames are marked with the database size they commit to.

If you hold a read transaction open (as `sqlite-js-stream` does), SQLite cannot checkpoint the WAL
beyond that transaction — which is what lets us read the WAL directly and still see a consistent
snapshot.

### WAL header (32 bytes)

| Offset | Size | Field |
|-------:|-----:|-------|
| 0 | 4 | Magic (`0x377f0682` => little-endian checksums, `0x377f0683` => big-endian) |
| 4 | 4 | Version (`3007000`) |
| 8 | 4 | Page size |
| 12 | 4 | Checkpoint sequence |
| 16 | 4 | Salt-1 |
| 20 | 4 | Salt-2 |
| 24 | 8 | Header checksum |

### WAL frame header (24 bytes)

| Offset | Size | Field |
|-------:|-----:|-------|
| 0 | 4 | Page number |
| 4 | 4 | Database size in pages (nonzero = commit frame) |
| 8 | 4 | Salt-1 |
| 12 | 4 | Salt-2 |
| 16 | 8 | Cumulative checksum |

### WAL checksum

SQLite's WAL checksum is a rolling pair. For each 8-byte unit:

```
s0 += u32[a] + s1
s1 += u32[a+4] + s0
```

The byte order (`le`/`be`) is chosen by the WAL magic. Frame checksums chain from the header
checksum and from the previous frame.

---

## Checkboxes / checkpointing

Checkpointing copies the WAL's committed pages back into the main database file and truncates the
WAL (in `TRUNCATE` mode) or replaces it, resetting the salt. After a checkpoint the WAL's salt
changes, so a reader that resumes from an old offset/salt detects a **prev-frame mismatch** and must
start a fresh snapshot.

---

## What this library reads

- **Page data** comes from the main database file *or* the WAL (a committed WAL page takes
  precedence). This is how a consistent snapshot is reconstructed without copying the whole file.
- The **transaction ID** is a sequence number maintained by this library, *not* from SQLite.
- Pages written into LTX files are LZ4-**block**-compressed and CRC-64 **ISO** checksummed.

---

## References

- SQLite WAL documentation: <https://www.sqlite.org/wal.html>
- SQLite file format: <https://www.sqlite.org/fileformat.html>
