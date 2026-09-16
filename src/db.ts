/**
 * DB manages a single SQLite database's replication state.
 *
 * It holds a `node:sqlite` connection (for page_size/page_count and to pin a
 * read snapshot), and reads the database and `-wal` files directly via fs so it
 * can build LTX files. This mirrors litestream's `db.go`.
 *
 * MVP simplification: `sync()` writes a **full-snapshot** LTX file at level 0
 * (`<meta>/ltx/0/<txID>-<txID>.ltx`) whose pages are the union of the
 * committed WAL pages (overlaid on the database file). This is a valid,
 * contiguous, restorable replication stream; true incremental level-0 files and
 * level-9 snapshots (with compaction) are a later milestone.
 */

import { DatabaseSync } from 'node:sqlite';
import { readFile, rename, writeFile } from 'node:fs/promises';
import nodePath from 'node:path';

import {
  BufferSink,
  Encoder,
  HeaderFlagNoChecksum,
  formatFilename,
  lockPgno,
  parseFilename,
  type FileInfo,
} from './ltx/index.ts';
import { WALFrameHeaderSize, WALReader, WALEOFError } from './wal/index.ts';
import { mkdirAll } from './util/fs.ts';

const DefaultPageSize = 4096;

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
    const pageSize = p.page_size || DefaultPageSize;

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
      entries = await import('node:fs/promises').then((f) => f.readdir(dir, { withFileTypes: true }));
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
      const st = await import('node:fs/promises').then((f) => f.stat(nodePath.join(dir, entry.name)));
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

  /**
   * Sync builds a full-snapshot LTX file at level 0 and writes it to the local
   * cache. Returns the new file's metadata, or null if no pages exist (e.g. an
   * empty database).
   */
  async sync(): Promise<FileInfo | null> {
    const pos = await this.pos();
    const txID = pos + 1n;

    const commit = this.pageCount();
    const pageSize = this.pageSize;
    if (commit === 0) return null; // empty database

    // Read the WAL (may not exist after a checkpoint) and its committed page map.
    let walBuf = new Uint8Array(0);
    let pageMap = new Map<number, number>();
    try {
      walBuf = new Uint8Array(await readFile(this.walPath()));
    } catch {
      walBuf = new Uint8Array(0);
    }
    if (walBuf.length > 0) {
      try {
        const wr = new WALReader(walBuf);
        pageMap = wr.pageMap().pageIndex;
      } catch (err) {
        if (err instanceof WALEOFError) pageMap = new Map();
        else throw err;
      }
    }

    // Read the database file for non-WAL pages.
    const dbBuf = new Uint8Array(await readFile(this.path));
    const lockPg = lockPgno(pageSize);

    // Encode a full snapshot (all committed pages) as an LTX file.
    const sink = new BufferSink();
    const enc = new Encoder(sink);
    enc.encodeHeader({
      version: 3,
      flags: HeaderFlagNoChecksum,
      pageSize,
      commit,
      minTXID: txID,
      maxTXID: txID,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    });

    for (let pgno = 1; pgno <= commit; pgno++) {
      if (pgno === lockPg) continue;
      const data = this.readPage(pgno, pageSize, pageMap, walBuf, dbBuf);
      enc.encodePage({ pgno, flags: 0 }, data);
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



  /** close releases the SQLite connection. */
  close(): void {
    this.conn.close();
  }
}
