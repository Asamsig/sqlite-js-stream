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

This repository is a **pnpm workspace**. The core library lives at the root and
upload strategies (replica backends) live under `packages/`:

```bash
pnpm install       # link the workspace
```

There is no build step: we run TypeScript directly via Node's native type
stripping.

The core package is published as `sqlite-js-stream` *(not yet published; install
from the repository)*, and each addon is a separate package:

```bash
pnpm add sqlite-js-stream sqlite-js-stream-backend-s3
```

---

## Quick start

```ts
import { Replica, FileReplicaClient } from 'sqlite-js-stream';

// 1. Point the replica at your database path and a replica destination.
//    The library reads the database + `-wal` directly and is agnostic to
//    whichever SQLite driver your application uses.
const client = new FileReplicaClient('/data/replica');
const replica = new Replica('/data/app.db', client);

// 2. Sync builds a local LTX file (into <db>-litestream/ltx/0/) and uploads it.
await replica.sync();
```

The database is written to by your **own** SQLite connection (using `node:sqlite`, `better-sqlite3`,
or anything else). After more writes, just `sync()` again:

```ts
// ... your app writes rows with whatever driver you use ...
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

### Continuously restore (follow)

`follow()` restores the latest backup, then keeps the database up to date as new backups are
replicated — maintaining a read-only replica (litestream's `restore -f`):

```ts
const ac = new AbortController();
await replica.follow({ outputPath: '/data/replica.db', intervalMs: 1000, signal: ac.signal });
// later: ac.abort() to stop
```

The applied TXID is recorded in a `-txid` sidecar (default `<outputPath>-txid`) so a later `follow()`
resumes instead of re-restoring. The output database should only be opened **read-only**.

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
import { newReplicaClientFromURL, Replica } from 'sqlite-js-stream';

const client = newReplicaClientFromURL('file:///data/replica');
const replica = new Replica('/data/app.db', client);
```

The `file` scheme is built into the core. Additional schemes (S3, etc.) are
registered by importing the matching addon (see below).

### S3 upload strategy

The `sqlite-js-stream-backend-s3` addon provides an S3 replica client. Import it
once to register the `s3://` scheme, then create clients from URLs or directly:

```ts
import { Replica, newReplicaClientFromURL } from 'sqlite-js-stream';
import { S3ReplicaClient } from 'sqlite-js-stream-backend-s3';

// Via a URL (bucket from the host, prefix from the path).
const client = newReplicaClientFromURL('s3://my-bucket/backup?region=us-east-1');

// Or directly, with explicit AWS config.
const s3 = new S3ReplicaClient({
  bucket: 'my-bucket',
  prefix: 'backup',
  region: 'us-east-1',
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const replica = new Replica('/data/app.db', client);
await replica.run({ intervalMs: 1000 });
```

Objects are stored as `{prefix}/ltx/{level}/{minTXID}-{maxTXID}.ltx` — the same
layout as the file backend, so a replica can move between strategies. Creation
timestamps are preserved as the `ltx-timestamp` object metadata.

> **Testing without AWS:** `S3ReplicaClient` accepts an injected `client`
> implementing `send(command)`. Pass an in-memory fake (see
> `packages/backend-s3/test/helpers/memory-s3.ts`) to exercise the addon without
> network access.

### Continuous sync

For ongoing replication, `replica.run()` syncs immediately and then repeats on an
interval until the supplied `AbortSignal` is aborted. Sync errors are logged and the
loop keeps going, so a transient failure doesn't stop replication:

```ts
import { Replica, FileReplicaClient } from 'sqlite-js-stream';

const replica = new Replica('/data/app.db', new FileReplicaClient('/data/replica'));
const ac = new AbortController();

// Run continuously (sync now, then every 1s); resolves when aborted.
const runP = replica.run({ intervalMs: 1000, signal: ac.signal });

// ... later, to stop:
ac.abort();
await runP;
```

If you want to keep replicating in the background without awaiting, call `run()` and
`catch` its promise (it resolves on abort):

```ts
replica.run({ intervalMs: 1000, signal: ac.signal }).catch((err) => {
  console.error('continuous sync stopped', err);
});
```

`Store` also exposes `syncAll()`/`snapshotAll()` for one-shot operations; a continuous
loop can be driven by calling `replica.run()` per database, or via your own scheduler.

### Replication from a worker thread

Run `replica.run()` in a **worker thread** so continuous replication doesn't compete with
your application's main thread. The worker opens its own **read-only** connection (via the
library) and reads the database + `-wal` files, while your app keeps writing from the main
thread on its own connection. Only the **path**s need to cross threads.

`replica-worker.ts`:

```ts
import { parentPort } from 'node:worker_threads';
import { Replica, FileReplicaClient } from 'sqlite-js-stream';

parentPort.on('message', async (msg) => {
  if (msg?.type !== 'start') return;
  const { dbPath, clientPath, intervalMs = 1000 } = msg;

  const replica = new Replica(dbPath, new FileReplicaClient(clientPath));
  const ac = new AbortController();
  const runP = replica.run({ intervalMs, signal: ac.signal });

  parentPort.postMessage({ type: 'started' });
  parentPort.on('message', async (m) => {
    if (m?.type === 'stop') {
      ac.abort();
      await runP;
      parentPort.postMessage({ type: 'done' });
    }
  });
});
```

Main thread:

```ts
import { Worker } from 'node:worker_threads';

const worker = new Worker(new URL('./replica-worker.ts', import.meta.url));
const started = new Promise((resolve) => worker.on('message', (m) => m?.type === 'started' && resolve()));
const done = new Promise((resolve) => worker.on('message', (m) => m?.type === 'done' && resolve()));

worker.postMessage({ type: 'start', dbPath: '/data/app.db', clientPath: '/data/replica', intervalMs: 1000 });
await started;

// Your app keeps using node:sqlite (or better-sqlite3) from the main thread.
// ... write rows ...

// Shut the replication worker down gracefully.
worker.postMessage({ type: 'stop' });
await done;
await worker.terminate();
```

> **Important:** the worker and your app each use their **own** connection to the same database
> file. Never hand the worker the same `DatabaseSync` handle your app writes with — a single
> connection holds one transaction at a time, so sharing a writable handle would merge the read
> snapshot with your writes (see *Concurrency & snapshot consistency* above).

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

### Concurrency & snapshot consistency

To keep snapshots consistent, `sync()`/`snapshot()` open a **separate, read-only** SQLite
connection to the same database and hold a short-lived read transaction while they read the
`-wal` + database files. SQLite will not checkpoint/truncate the WAL past an active reader, and the
pinned reader's `PRAGMA page_count` reports the commit including uncheckpointed WAL pages.

Your application is free to use its own SQLite driver and connections (`node:sqlite`,
`better-sqlite3`, …) independently — the library reads only the files and never touches them.

> **Do not point the library at your application's writable connection.**
>
> A single SQLite connection holds **one transaction at a time**. If the library were to use the
> same `DatabaseSync` handle as your writer:
> - its read snapshot would **not be isolated** from concurrent app writes (they'd merge into one
>   transaction, and the library could read *uncommitted* rows or commit the app's writes), and
> - if your app had a transaction open, the library could not `BEGIN` its snapshot pin at all.
>
> The library has no API to accept an app-owned writable connection, and it deliberately opens its
> own read-only reader instead. Keep the library's reader and your app's writer on separate
> connections to the same database file.

### Large databases & streaming

The replication path is **streaming** end-to-end, so peak memory is bounded by one page buffer
(plus the small LTX page index) rather than the database size:

- `sync()`/`snapshot()` read database **pages one at a time from a file handle** and stream the
  encoded LTX straight to a temp file (never buffering the whole DB/WAL).
- `restore()` streams each LTX file's pages into the output database, overlaying incremental files
  on the snapshot base — it never materializes a full merged file or the whole DB in memory.
- The S3 addon uploads objects larger than **5 MiB** with the **multipart** API, so a large snapshot
  is never buffered on the upload side either.

### Benchmarks

Reproducible, configurable benchmarks live in [`bench/bench-sync-restore.ts`](bench/bench-sync-restore.ts)
and measure `sync()`/`restore()` throughput plus peak process RSS:

```bash
pnpm bench
# another size / batch:
ROWS=200000 BATCH=1000 pnpm bench
# demonstrate the streaming memory bound (peak RSS vs DB size):
SCALE=4 pnpm bench
```

Conditions are disclosed in the output (rows, batch/transactions, source WAL bytes). Peak RSS is
process-wide (it includes the Node baseline), so the meaningful claim is that it stays **flat** as the
database grows — see the `SCALE` mode, which shows WAL bytes and peak RSS at two sizes.

### Compaction & retention

Over time `sync()` writes one level-0 LTX file per transaction. To keep the replica bounded and
restore fast, compact those into higher levels and prune data that has been superseded:

```ts
// Merge pending level-0 files into a level-1 file (and 1→2→3 up to maxCompactionLevel).
await replica.compact({ dstLevel: 1 });

// Time-based retention: delete snapshots older than 24h (always keeping the
// newest), cascade the boundary down through the other levels, and delete
// level-0 files that were compacted into level-1 once they age past 5m.
const result = await replica.enforceRetention({
  snapshotRetentionMs: 24 * 60 * 60 * 1000,
  l0RetentionMs: 5 * 60 * 1000,
});
```

`Replica.run()` can drive all of this on their own schedules, and `Store` orchestrates it across
every replica.

```ts
import { CompactionLevels } from 'sqlite-js-stream';

// Each level is compacted at most once per wall-clock bucket (L1 every 30s,
// L2 every 5m, L3 every 1h) — mirrors litestream's per-level pacing.
const levels = new CompactionLevels([
  { level: 0, intervalMs: 0 },
  { level: 1, intervalMs: 30_000 },
  { level: 2, intervalMs: 300_000 },
  { level: 3, intervalMs: 3_600_000 },
]);

await store.run({
  intervalMs: 1000,
  compactionLevels: levels,
  snapshotIntervalMs: 24 * 60 * 60 * 1000,
  snapshotRetentionMs: 24 * 60 * 60 * 1000,
  l0RetentionMs: 5 * 60 * 1000,
  signal,
});
```

Compaction is **streaming** (bounded memory) and only deletes a lower-level file after the higher
level is written *and* uploaded, so a mid-compaction failure never loses data. Retention never deletes
files needed to restore any point within the retention window, and the live-head level-0 file is
always retained.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#compaction--retention) for the full model.

### Distributed leasing

When several replicas point at the same storage, only one may write at a time. Give the replica a
`Leaser` (a `lock.json` lease) and `run()` will make exactly one replica the active **primary**;
the others become idle **followers** that poll until the lease is free. Storage writes are gated on
lease ownership, so a secondary can never split the brain.

```ts
import { FileLeaser } from 'sqlite-js-stream';

replica.leaser = new FileLeaser('/data/replica'); // becomes primary if the lease is free
await replica.run({ intervalMs: 1000, signal });

// Query which mode we are in:
if (replica.isPrimary()) {
  // we are writing
}
```

The file backend uses an exclusive-create lease with a content-hash token; the S3 addon
(`import { S3Leaser } from 'sqlite-js-stream-backend-s3'`) uses S3 `If-None-Match`/`If-Match`
conditional writes so racy acquirers cannot both win.

### ⚠️ This is a backup tool, not a distributed database

The lease coordinates **which host writes to storage**. It does **not** merge, compare, or reconcile
the actual data in your databases. Be very clear about what you're pointing at the same prefix.

**Multiple hosts are only safe if they hold the *same* database content** (e.g. a DR/standby host that
was restored from the same backup). The lease then prevents two hosts from both writing, and gives
you automatic failover when the primary stops.

**Do NOT point hosts that hold *different* data at the same prefix.** SQLite is a
single-writer, single-file database. If host A has `row id=1 → "X"` and host B has
`row id=1 → "Y"` and they share one prefix, here is what happens:

| Host A local DB      | Host B local DB      | What lands in S3 |
|----------------------|---------------------|------------------|
| `row id=1 → "X"`     | `row id=1 → "Y"`     | `"X"` (only if **A** is the primary) |

- Whichever host is the **primary** gets backed up. The other host's value is **never uploaded**, is
  not merged, and is effectively **lost** from the backup's perspective.
- When you restore, you get exactly one host's state. There is **no conflict resolution**. `"Y"` is
  simply gone.

**Choose the right setup:**

| If you need…                                            | Do this |
|---------------------------------------------------------|----------|
| Each host owns a **different** database                 | Give each its own prefix, e.g. `app/${tenantId}` — separate backup per DB, no conflict. |
| Multiple app instances sharing one consistent dataset | Don't use SQLite + this replication. Use a client-server DB (Postgres/MySQL) or a real shared-file lock + single writer. |
| Same data on every host (failover / DR)                 | Keep exactly one writer. On failover, **restore the standby from S3 first**, then let it take the lease — don't let a stale local file start writing. |

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
