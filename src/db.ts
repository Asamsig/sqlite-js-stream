/**
 * DB manages a single SQLite database's replication state.
 *
 * It holds a `node:sqlite` connection (for page_size/page_count and to pin a
 * read snapshot), and reads the database and `-wal` files directly via fs so it
 * can build LTX files. This mirrors litestream's `db.go`.
 *
 * `sync()` writes a level-0 LTX file (`<meta>/ltx/0/<txID>-<txID>.ltx`):
 *  - The first sync (no local files) writes a **full-snapshot** base.
 *  - Later syncs write an **incremental** file containing only the pages whose
 *    latest committed version is in the WAL since the last synced offset.
 *  - If the WAL has been reset (checkpointed) so we can no longer resume, sync
 *    falls back to another full-snapshot base.
 */

import { DatabaseSync } from 'node:sqlite';
import { readFile, readdir, rename, stat, writeFile } from 'node:fs/promises';
import nodePath from 'node:path';

import {
  BufferSink,
  Decoder,
  Encoder,
  HeaderFlagNoChecksum,
  formatFilename,
  lockPgno,
  parseFilename,
  type FileInfo,
  type Header,
} from './ltx/index.ts';
import {
  PrevFrameMismatchError,
  WALFrameHeaderSize,
  WALHeaderSize,
  WALReader,
  WALEOFError,
} from './wal/index.ts';
import { mkdirAll } from './util/fs.ts';

/** DB wraps a SQLite database and its LTX replication state. */
export class DB {
  readonly path: string;
  readonly metaPath: string;
  readonly pageSize: number;
  private conn: DatabaseSync;

  private constructor(path: string, metaPath: string, pageSize: number, conn: DatabaseSync) {
    this.path = path;
    this.metaPath = metaPath;
    this.pageSize = pageSize;
    this.conn = conn;
  }

  /** Opens (and creates, if needed) a SQLite database in WAL mode. */
  static async open(path: string): Promise<DB> {
    const conn = new DatabaseSync(path);
    conn.exec('PRAGMA journal_mode=WAL');
    conn.exec('PRAGMA wal_autocheckpoint=0');
    conn.exec('PRAGMA busy_timeout=5000');

    const p = conn.prepare('PRAGMA page_size').get() as { page_size: number };
    const pageSize = p.page_size || 4096;

    const metaPath = `${path}-litestream`;
    await mkdirAll(nodePath.join(metaPath, 'ltx', '0'));

    return new DB(path, metaPath, pageSize, conn);
  }

  /** WALPath returns the path to the database's WAL file. */
  walPath(): string {
    return `${this.path}-wal`;
  }

  /** LTXDir returns the local LTX root directory. */
  ltxDir(): string {
    return nodePath.join(this.metaPath, 'ltx');
  }

  /** LTXLevelDir returns the local LTX directory for a level. */
  ltxLevelDir(level: number): string {
    return nodePath.join(this.ltxDir(), String(level));
  }

  /** LTXPath returns the local path of a single LTX file. */
  ltxPath(level: number, minTXID: bigint, maxTXID: bigint): string {
    return nodePath.join(this.ltxLevelDir(level), formatFilename(minTXID, maxTXID));
  }

  /** pageCount returns the current committed database size, in pages. */
  pageCount(): number {
    const r = this.conn.prepare('PRAGMA page_count').get() as { page_count: number };
    return r.page_count;
  }

  /** Lists local level-0 LTX files, sorted by TXID. */
  async l0Files(): Promise<FileInfo[]> {
    const dir = this.ltxLevelDir(0);
    const out: FileInfo[] = [];
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return out;
      throw err;
    }
    for (const entry of entries) {
      if (!entry.isFile()) continue;
      let minTXID: bigint;
      let maxTXID: bigint;
      try {
        ({ minTXID, maxTXID } = parseFilename(entry.name));
      } catch {
        continue;
      }
      const st = await stat(nodePath.join(dir, entry.name));
      out.push({ level: 0, minTXID, maxTXID, size: st.size, createdAt: st.mtimeMs });
    }
    out.sort((a, b) => (a.minTXID < b.minTXID ? -1 : a.minTXID > b.minTXID ? 1 : 0));
    return out;
  }

  /** Pos returns the maximum TXID in the local level-0 cache (0 if none). */
  async pos(): Promise<bigint> {
    const files = await this.l0Files();
    if (files.length === 0) return 0n;
    let max = files[0].maxTXID;
    for (const f of files) if (f.maxTXID > max) max = f.maxTXID;
    return max;
  }

  /** Sync builds a level-0 LTX file and writes it to the local cache. */
  async sync(): Promise<FileInfo | null> {
    const pos = await this.pos();
    const txID = pos + 1n;
    const commit = this.pageCount();
    if (commit === 0) return null;

    const pageSize = this.pageSize;
    const lockPg = lockPgno(pageSize);

    const walBuf = await this.readWalOrEmpty();
    const dbBuf = new Uint8Array(await readFile(this.path));

    // Determine the previous sync state to resume incremental reading.
    const prev = await this.prevSyncInfo();
    let snapshotting = prev === null;

    let reader: WALReader | null = null;
    let offset = WALHeaderSize;
    let salt1 = 0;
    let salt2 = 0;
    let pageMap = new Map<number, number>();
    let maxOffset = 0;
    let walCommit = 0;
    let prevCommit = 0;

    if (!snapshotting && prev !== null) {
      reader = makeResumeReader(walBuf, prev.walOffset + prev.walSize, prev.salt1, prev.salt2);
      if (reader === null) {
        snapshotting = true; // WAL was reset; fall back to a snapshot base
      } else {
        const pm = reader.pageMap();
        pageMap = pm.pageIndex;
        maxOffset = pm.maxOffset;
        walCommit = pm.commit;
        offset = prev.walOffset + prev.walSize;
        salt1 = prev.salt1;
        salt2 = prev.salt2;
        prevCommit = prev.commit;
      }
    }

    const walSize = snapshotting ? 0 : maxOffset > 0 ? maxOffset - offset : 0;

    if (!snapshotting && walSize === 0) {
      return null; // no new WAL pages since the last sync
    }

    if (snapshotting) {
      // Read the WAL from the start to build a full page map for the snapshot.
      try {
        const full = new WALReader(walBuf);
        const pm = full.pageMap();
        pageMap = pm.pageIndex;
        walCommit = pm.commit;
        salt1 = full.salt1();
        salt2 = full.salt2();
      } catch (err) {
        if (!(err instanceof WALEOFError)) throw err;
      }
      if (walCommit > 0) {
        offset = WALHeaderSize;
      }
    } else if (walCommit > 0) {
      // Commit reflects the DB size at the last committed frame.
      // `commit` is already from PRAGMA; keep the larger for safety.
      void walCommit;
    }

    const finalCommit = Math.max(commit, walCommit);

    // Encode the LTX file.
    const sink = new BufferSink();
    const enc = new Encoder(sink);
    enc.encodeHeader({
      version: 3,
      flags: HeaderFlagNoChecksum,
      pageSize,
      commit: finalCommit,
      minTXID: txID,
      maxTXID: txID,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: BigInt(offset),
      walSize: BigInt(snapshotting ? Math.max(0, walBuf.length - WALHeaderSize) : walSize),
      walSalt1: salt1,
      walSalt2: salt2,
      nodeID: 0n,
    });

    if (snapshotting) {
      this.writePagesFromDB(enc, pageSize, finalCommit, lockPg, pageMap, walBuf, dbBuf);
    } else {
      this.writePagesFromWAL(enc, pageSize, finalCommit, prevCommit, lockPg, pageMap, walBuf, dbBuf);
    }
    enc.close();

    const buf = sink.toBuffer();
    const dir = this.ltxLevelDir(0);
    await mkdirAll(dir);
    const filename = this.ltxPath(0, txID, txID);
    const tmp = `${filename}.tmp`;
    await writeFile(tmp, buf);
    await rename(tmp, filename);

    return { level: 0, minTXID: txID, maxTXID: txID, size: buf.length, createdAt: Date.now() };
  }

  /** Writes a full snapshot: every committed page, preferring WAL pages. */
  private writePagesFromDB(
    enc: Encoder,
    pageSize: number,
    commit: number,
    lockPg: number,
    pageMap: Map<number, number>,
    walBuf: Uint8Array,
    dbBuf: Uint8Array,
  ): void {
    for (let pgno = 1; pgno <= commit; pgno++) {
      if (pgno === lockPg) continue;
      enc.encodePage({ pgno, flags: 0 }, this.readPage(pgno, pageSize, pageMap, walBuf, dbBuf));
    }
  }

  /** Writes an incremental file: changed WAL pages + DB growth pages. */
  private writePagesFromWAL(
    enc: Encoder,
    pageSize: number,
    commit: number,
    prevCommit: number,
    lockPg: number,
    pageMap: Map<number, number>,
    walBuf: Uint8Array,
    dbBuf: Uint8Array,
  ): void {
    // Sorted page numbers present in the WAL change-set.
    const pgnos = [...pageMap.keys()].sort((a, b) => a - b);
    const lockPgno = lockPg;

    // Add DB growth pages (new pages beyond prevCommit not already in pageMap).
    if (commit > prevCommit) {
      for (let pgno = prevCommit + 1; pgno <= commit; pgno++) {
        if (pgno === lockPgno) continue;
        if (pageMap.has(pgno)) continue;
        pgnos.push(pgno);
      }
    }

    for (const pgno of pgnos) {
      if (pgno === lockPgno) continue;
      // Growth pages (not in pageMap) come directly from the database file.
      if (!pageMap.has(pgno)) {
        const start = (pgno - 1) * pageSize;
        enc.encodePage({ pgno, flags: 0 }, new Uint8Array(dbBuf.subarray(start, start + pageSize)));
        continue;
      }
      enc.encodePage({ pgno, flags: 0 }, this.readPage(pgno, pageSize, pageMap, walBuf, dbBuf));
    }
  }

  /**
   * Snapshot writes a full-database LTX file at `SnapshotLevel` (9) with
   * MinTXID=1 and MaxTXID equal to the current local position. Returns null if
   * a snapshot would cover no data (empty database).
   */
  async snapshot(): Promise<FileInfo | null> {
    const pos = await this.pos();
    const maxTXID = pos === 0n ? 1n : pos;
    const commit = this.pageCount();
    if (commit === 0) return null;

    const pageSize = this.pageSize;
    const lockPg = lockPgno(pageSize);
    const walBuf = await this.readWalOrEmpty();
    const dbBuf = new Uint8Array(await readFile(this.path));

    // Build the committed WAL page map for the overlay (may be empty).
    let pageMap = new Map<number, number>();
    let salt1 = 0;
    let salt2 = 0;
    if (walBuf.length > 0) {
      try {
        const full = new WALReader(walBuf);
        const pm = full.pageMap();
        pageMap = pm.pageIndex;
        salt1 = full.salt1();
        salt2 = full.salt2();
      } catch (err) {
        if (!(err instanceof WALEOFError)) throw err;
      }
    }

    const sink = new BufferSink();
    const enc = new Encoder(sink);
    enc.encodeHeader({
      version: 3,
      flags: HeaderFlagNoChecksum,
      pageSize,
      commit,
      minTXID: 1n,
      maxTXID,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: BigInt(WALHeaderSize),
      walSize: BigInt(Math.max(0, walBuf.length - WALHeaderSize)),
      walSalt1: salt1,
      walSalt2: salt2,
      nodeID: 0n,
    });
    this.writePagesFromDB(enc, pageSize, commit, lockPg, pageMap, walBuf, dbBuf);
    enc.close();

    const buf = sink.toBuffer();
    const dir = this.ltxLevelDir(9);
    await mkdirAll(dir);
    const filename = this.ltxPath(9, 1n, maxTXID);
    const tmp = `${filename}.tmp`;
    await writeFile(tmp, buf);
    await rename(tmp, filename);

    return { level: 9, minTXID: 1n, maxTXID, size: buf.length, createdAt: Date.now() };
  }

  /** Reads a single page, preferring the WAL over the database file. */
  private readPage(
    pgno: number,
    pageSize: number,
    pageMap: Map<number, number>,
    walBuf: Uint8Array,
    dbBuf: Uint8Array,
  ): Uint8Array {
    const off = pageMap.get(pgno);
    if (off !== undefined) {
      const start = off + WALFrameHeaderSize;
      return new Uint8Array(walBuf.subarray(start, start + pageSize));
    }
    const start = (pgno - 1) * pageSize;
    return new Uint8Array(dbBuf.subarray(start, start + pageSize));
  }

  /** Reads the WAL file bytes, returning empty if it does not exist. */
  private async readWalOrEmpty(): Promise<Uint8Array> {
    try {
      return new Uint8Array(await readFile(this.walPath()));
    } catch {
      return new Uint8Array(0);
    }
  }

  /** Decodes the header of the max-TXID local L0 file (or null if none). */
  private async prevSyncInfo(): Promise<{
    walOffset: number;
    walSize: number;
    salt1: number;
    salt2: number;
    commit: number;
  } | null> {
    const files = await this.l0Files();
    if (files.length === 0) return null;
    let max = files[0];
    for (const f of files) if (f.maxTXID > max.maxTXID) max = f;
    try {
      const buf = new Uint8Array(await readFile(this.ltxPath(0, max.minTXID, max.maxTXID)));
      const hdr = new Decoder(buf);
      hdr.decodeHeader();
      const h: Header = hdr.Header();
      return {
        walOffset: Number(h.walOffset),
        walSize: Number(h.walSize),
        salt1: h.walSalt1,
        salt2: h.walSalt2,
        commit: h.commit,
      };
    } catch {
      return null;
    }
  }

  /** close releases the SQLite connection. */
  close(): void {
    this.conn.close();
  }
}

/** Creates a WALReader positioned at `offset`, or null if it cannot resume. */
function makeResumeReader(walBuf: Uint8Array, offset: number, salt1: number, salt2: number): WALReader | null {
  if (walBuf.length === 0) return null;
  if (offset <= WALHeaderSize) {
    // Starting fresh (or before any frames): read from the header.
    try {
      return new WALReader(walBuf);
    } catch {
      return null;
    }
  }
  try {
    return WALReader.withOffset(walBuf, offset, salt1, salt2);
  } catch (err) {
    if (err instanceof PrevFrameMismatchError) return null;
    throw err;
  }
}


