# sqlite-js-stream

A **Node.js port of [Litestream](https://github.com/benbjohnson/litestream)** — SQLite streaming
replication and disaster recovery — written in **TypeScript** using Node's native type stripping
(no build step) and the built-in `node:sqlite`.

SQLite is the most widely deployed database on the planet but it only lives on a single machine.
If that machine dies, your database is gone. This library replicates a SQLite database's **WAL
(Write-Ahead Log)** file into immutable, chronological **LTX (Log Transaction)** files on a remote
replica (S3, a local directory, etc.), and can **restore** a database from those files.

> **Status:** work in progress. Milestones 0–5 are complete (LTX format, WAL reader, file backend,
> end-to-end DB+Replica sync/restore, incremental sync + level-9 snapshots + compactor + store).
> Cloud backends, multi-level compaction/retention, distributed leasing, VFS mirroring, and the
> IPC server are not yet implemented.

---

## Requirements

- **Node.js `>= 24`** — required for native `.ts` type stripping and the built-in `node:sqlite`.
- Runtime dependency: `lz4js` (pure-JS LZ4 **block** codec, matching the LTX format).

No build step. Test files and source run directly under Node.

---

## Installation

```bash
npm install sqlite-js-stream
```

*(Not yet published; install from the repository.)*

---

## Quick start

```ts
import { openDatabase, Replica, FileReplicaClient } from 'sqlite-js-stream';

// 1. Open a database handle (driver-agnostic, returns a `Database`).
const db = await openDatabase('/data/app.db');

// 2. Choose a replica destination. A local directory is simplest.
const client = new FileReplicaClient('/data/replica');

// 3. Wire the replica and replicate.
const replica = new Replica(db, client);
await db.sync();         // writes a local LTX file into <db>-litestream/ltx/0/
await replica.sync();    // uploads it to the replica (file://.../ltx/0/...)
```

The database is written to by your own SQLite connection (or the same `DatabaseSync`). After more
writes, just `sync()` again:

```ts
// ... your app writes rows via node:sqlite ...
await db.sync();
await replica.sync();

const status = await replica.syncStatus();
console.log(status.inSync, status.localTXID, status.remoteTXID); // true 3n 3n
```

### Take a full snapshot

Normal `sync()` writes **incremental** level-0 files. For a complete, point-in-time backup, take a
**level-9 snapshot** (a full database copy as an LTX file with `MinTXID = 1`):

```ts
await replica.snapshot();
```

### Restore a database

Restore reconstructs a SQLite database by compacting a level-9 snapshot with the subsequent
level-0 files:

```ts
await replica.restore({ outputPath: '/data/restored.db' });
```

Restore to a specific transaction ID:

```ts
await replica.restore({ outputPath: '/data/app-at-txid.db', txID: 42n });
```

### Restore the local database if it is missing

```ts
await replica.ensureExists();
```

### Manage many databases

```ts
import { Store } from 'sqlite-js-stream';

const store = new Store();
await store.openDB('/data/a.db', new FileReplicaClient('/data/replica-a'));
await store.openDB('/data/b.db', new FileReplicaClient('/data/replica-b'));

await store.syncAll();      // local sync + upload for every database
await store.snapshotAll();  // level-9 snapshots for every database

store.close();
```

### Replica URL

Replica clients are created from URLs via the factory registry:

```ts
import { newReplicaClientFromURL, openDatabase, Replica } from 'sqlite-js-stream';

const client = newReplicaClientFromURL('file:///data/replica');
const replica = new Replica(await openDatabase('/data/app.db'), client);
```

---

## How it works

Litestream (and this port) never touches the database through the SQLite WAL/VFS as part of
replication. Instead it:

1. **Reads the `-wal` file directly** (parsing frames, verifying the WAL salt + checksum) and the
   **database file directly** via `fs`.
2. **Builds an LTX file** per sync — a sequence of LZ4-compressed, checksummed pages that capture
   the changed pages, tagged with a sequential **transaction ID**.
3. **Uploads** those immutable LTX files to a replica.
4. **Restores** by compacting a snapshot with the stream of level-0 files and writing a valid
   SQLite file.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design, and
[`PLAN.md`](PLAN.md) for the milestone plan and format details.

---

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — system design (DB, Replica, ReplicaClient,
  LTX, WAL, backends).
- [`docs/LTX_FORMAT.md`](docs/LTX_FORMAT.md) — the LTX binary format.
- [`PLAN.md`](PLAN.md) — the porting plan, format tables, milestone status, and known risks.
- [`test/`](test) — TDD test suite (`npm test`).

---

## Credits

This project is a **port of [Litestream](https://github.com/benbjohnson/litestream)**
by [Ben Johnson](https://github.com/benbjohnson) (Apache-2.0). The LTX file format and much of the
design come from the [`superfly/ltx`](https://github.com/superfly/ltx) library and the litestream
documentation (`docs/ARCHITECTURE.md`, `docs/SQLITE_INTERNALS.md`, `docs/LTX_FORMAT.md`,
`docs/PATTERNS.md`), which are adapted here with attribution.

The SQLite **WAL** and **LTX** formats are replicated faithfully so that this library can read LTX
files produced by Litestream and vice-versa.

---

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
