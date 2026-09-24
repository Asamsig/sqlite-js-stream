/**
 * File replica client — replicates LTX files to a local directory.
 *
 * Layout: `<root>/ltx/<level>/<minTXID>-<maxTXID>.ltx`
 * Writes are atomic: data is streamed to a `.tmp` file, fsync'd, then renamed,
 * the directory is fsync'd, and the file mtime is set from the LTX header
 * timestamp to preserve original creation time.
 */

import { createReadStream } from 'node:fs';
import { open, readdir, readFile, rename, rm, stat, utimes, type FileHandle } from 'node:fs/promises';
import nodePath from 'node:path';
import { Readable } from 'node:stream';

import {
  ltxFilePath as sharedLTXFilePath,
  ltxLevelDir as sharedLTXLevelDir,
  LTXError,
  parseFilename,
  type OpenLTXFileOptions,
  type ReplicaClient,
  type WriteLTXFileOptions,
} from '../../replica-client.ts';
import type { FileInfo, Header } from '../../ltx/index.ts';
import { HeaderSize, unmarshalHeader } from '../../ltx/index.ts';
import {
  formatSnapshotFilenameV3,
  formatWALSegmentFilenameV3,
  isGenerationIDV3,
  parseSnapshotFilenameV3,
  parseWALSegmentFilenameV3,
  type GenerationClientV3,
  type SnapshotInfoV3,
  type WALSegmentInfoV3,
} from '../../restore-v3.ts';
import { concatBytes } from '../../util/stream.ts';
import { fsyncDir, mkdirAll } from '../../util/fs.ts';
import type { Logger } from '../../util/logger.ts';
import { noopLogger } from '../../util/logger.ts';
import { registerReplicaClientFactory } from '../../replica-url.ts';

export const ReplicaClientType = 'file';

/** FileReplicaClient writes LTX files to a local directory. */
export class FileReplicaClient implements ReplicaClient, GenerationClientV3 {
  private pathVal: string;
  private logger: Logger = noopLogger;

  constructor(path: string) {
    this.pathVal = path;
  }

  /** GenerationsV3 returns legacy v0.3.x generation IDs in ascending order. */
  async generations(): Promise<string[]> {
    const dir = nodePath.join(this.pathVal, 'generations');
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return [];
      throw err;
    }
    return entries
      .filter((e) => e.isDirectory() && isGenerationIDV3(e.name))
      .map((e) => e.name)
      .sort();
  }

  /** SnapshotsV3 returns v0.3.x snapshots for a generation, sorted by index. */
  async snapshotsV3(generation: string): Promise<SnapshotInfoV3[]> {
    const dir = nodePath.join(this.pathVal, 'generations', generation, 'snapshots');
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return [];
      throw err;
    }
    const out: SnapshotInfoV3[] = [];
    for (const e of entries) {
      if (!e.isFile()) continue;
      let index: number;
      try {
        index = parseSnapshotFilenameV3(e.name);
      } catch {
        continue;
      }
      const st = await stat(nodePath.join(dir, e.name));
      out.push({ generation, index, size: st.size, createdAt: st.mtimeMs });
    }
    out.sort((a, b) => a.index - b.index);
    return out;
  }

  /** WALSegmentsV3 returns v0.3.x WAL segments, sorted by (index, offset). */
  async walSegmentsV3(generation: string): Promise<WALSegmentInfoV3[]> {
    const dir = nodePath.join(this.pathVal, 'generations', generation, 'wal');
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return [];
      throw err;
    }
    const out: WALSegmentInfoV3[] = [];
    for (const e of entries) {
      if (!e.isFile()) continue;
      let index: number, offset: number;
      try {
        ({ index, offset } = parseWALSegmentFilenameV3(e.name));
      } catch {
        continue;
      }
      const st = await stat(nodePath.join(dir, e.name));
      out.push({ generation, index, offset, size: st.size, createdAt: st.mtimeMs });
    }
    out.sort((a, b) => a.index - b.index || a.offset - b.offset);
    return out;
  }

  /** Reads the raw (compressed) bytes of a v0.3.x snapshot. */
  async readSnapshotV3(generation: string, index: number): Promise<Uint8Array> {
    return readFile(
      nodePath.join(
        this.pathVal,
        'generations',
        generation,
        'snapshots',
        formatSnapshotFilenameV3(index),
      ),
    );
  }

  /** Reads the raw (compressed) bytes of a v0.3.x WAL segment. */
  async readWALSegmentV3(generation: string, index: number, offset: number): Promise<Uint8Array> {
    return readFile(
      nodePath.join(
        this.pathVal,
        'generations',
        generation,
        'wal',
        formatWALSegmentFilenameV3(index, offset),
      ),
    );
  }

  type(): string {
    return ReplicaClientType;
  }

  async init(): Promise<void> {
    /* no initialization required */
  }

  setLogger(logger: Logger): void {
    this.logger = logger;
  }

  /** Returns the destination root path. */
  path(): string {
    return this.pathVal;
  }

  ltxLevelDir(level: number): string {
    return sharedLTXLevelDir(this.pathVal, level);
  }

  ltxFilePath(level: number, minTXID: bigint, maxTXID: bigint): string {
    return sharedLTXFilePath(this.pathVal, level, minTXID, maxTXID);
  }

  async *ltxFiles(opts: { level: number; seek: bigint; useMetadata: boolean }): AsyncIterable<FileInfo> {
    const dir = this.ltxLevelDir(opts.level);
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return;
      throw err;
    }

    const infos: FileInfo[] = [];
    for (const entry of entries) {
      if (!entry.isFile()) continue;
      let minTXID: bigint;
      let maxTXID: bigint;
      try {
        ({ minTXID, maxTXID } = parseFilename(entry.name));
      } catch {
        continue; // skip non-conforming files
      }
      if (minTXID < opts.seek) continue;

      const full = nodePath.join(dir, entry.name);
      const st = await stat(full);
      infos.push({
        level: opts.level,
        minTXID,
        maxTXID,
        size: st.size,
        createdAt: st.mtimeMs,
      });
    }

    infos.sort((a, b) => {
      if (a.minTXID !== b.minTXID) return a.minTXID < b.minTXID ? -1 : 1;
      return a.maxTXID < b.maxTXID ? -1 : a.maxTXID > b.maxTXID ? 1 : 0;
    });

    for (const info of infos) yield info;
  }

  async openLtxFile(opts: OpenLTXFileOptions): Promise<Readable> {
    const p = this.ltxFilePath(opts.level, opts.minTXID, opts.maxTXID);
    let st;
    try {
      st = await stat(p);
    } catch (err) {
      throw new LTXError('open', p, opts.level, opts.minTXID, opts.maxTXID, err);
    }

    if (opts.offset >= st.size) {
      return Readable.from([]);
    }

    const end =
      opts.size > 0
        ? Math.min(opts.offset + opts.size - 1, st.size - 1)
        : st.size - 1;

    return createReadStream(p, { start: opts.offset, end });
  }

  async writeLtxFile(opts: WriteLTXFileOptions, body: AsyncIterable<Uint8Array | string>): Promise<FileInfo> {
    const dir = this.ltxLevelDir(opts.level);
    await mkdirAll(dir);

    const filename = this.ltxFilePath(opts.level, opts.minTXID, opts.maxTXID);
    const tmp = `${filename}.tmp`;

    let fh: FileHandle | null = null;
    try {
      fh = await open(tmp, 'w');
    } catch (err) {
      throw new LTXError('create', tmp, opts.level, opts.minTXID, opts.maxTXID, err);
    }

    let headerBytes: Uint8Array | null = null;
    let collected: Uint8Array[] = [];
    let collectedLen = 0;
    let size = 0;

    try {
      for await (const chunk of body) {
        const bytes =
          typeof chunk === 'string' ? new TextEncoder().encode(chunk) : Uint8Array.from(chunk);
        if (headerBytes === null) {
          collected.push(bytes);
          collectedLen += bytes.length;
          if (collectedLen >= HeaderSize) {
            const concat = concatBytes(collected);
            headerBytes = concat.subarray(0, HeaderSize);
            collected = [];
            collectedLen = 0;
          }
        }
        await writeAll(fh, bytes);
        size += bytes.length;
      }

      if (headerBytes === null) {
        throw new Error('write ltx file: input too short to contain a header');
      }
      const hdr: Header = unmarshalHeader(headerBytes);
      const timestamp = Number(hdr.timestamp);

      await fh.sync();
      await fh.close();
      fh = null;

      await rename(tmp, filename);
      await fsyncDir(dir);
      await utimes(filename, new Date(timestamp), new Date(timestamp));

      return {
        level: opts.level,
        minTXID: opts.minTXID,
        maxTXID: opts.maxTXID,
        size,
        createdAt: timestamp,
      };
    } catch (err) {
      if (fh !== null) await fh.close().catch(() => {});
      await rm(tmp, { force: true }).catch(() => {});
      throw err;
    }
  }

  async deleteLtxFiles(infos: FileInfo[]): Promise<void> {
    for (const info of infos) {
      const p = this.ltxFilePath(info.level, info.minTXID, info.maxTXID);
      this.logger.debug('deleting ltx file', {
        level: info.level,
        minTXID: info.minTXID,
        maxTXID: info.maxTXID,
        path: p,
      });
      try {
        await rm(p);
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
      }
    }
  }

  async deleteAll(): Promise<void> {
    try {
      await rm(this.pathVal, { recursive: true, force: true });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
    }
  }
}

/** NewReplicaClientFromURL creates a FileReplicaClient from a URL path. */
export function NewReplicaClientFromURL(
  _scheme: string,
  _host: string,
  urlPath: string,
  _query: URLSearchParams | null,
  _userinfo: string | null,
): ReplicaClient {
  if (!urlPath) {
    throw new Error('file replica path required');
  }
  return new FileReplicaClient(urlPath);
}

// Mirrors Go's init(): register the file factory when this module loads.
registerReplicaClientFactory('file', NewReplicaClientFromURL);

/** Writes all of `bytes` to a FileHandle, handling partial writes. */
async function writeAll(handle: FileHandle, bytes: Uint8Array): Promise<void> {
  let offset = 0;
  while (offset < bytes.length) {
    const { bytesWritten } = await handle.write(bytes, offset, bytes.length - offset);
    offset += bytesWritten;
  }
}
