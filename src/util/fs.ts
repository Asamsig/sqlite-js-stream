/** File-system helpers for atomic writes and directory fsync. */

import { writeSync } from 'node:fs';
import { mkdir, open, type FileHandle } from 'node:fs/promises';
import { dirname } from 'node:path';
import type { ByteSink } from '../ltx/sink.ts';

/** Ensures a directory (and parents) exists; no-op if already present. */
export async function mkdirAll(path: string): Promise<void> {
  await mkdir(path, { recursive: true });
}

/** Fsyncs a directory so a preceding rename within it is durable. */
export async function fsyncDir(path: string): Promise<void> {
  const handle = await open(path, 'r');
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

/** Returns the parent directory of a path (portable). */
export function parentDir(path: string): string {
  return dirname(path);
}

/**
 * A file-backed {@link ByteSink}. It batches bytes into fixed-size blocks and
 * writes them synchronously, so the LTX encoder can stream a large file to disk
 * without buffering the whole file (or the whole database) in memory.
 */
export class FileSink implements ByteSink {
  private fh: FileHandle;
  private buf: Uint8Array;
  private pos = 0;
  private filePos = 0;

  constructor(fh: FileHandle, chunkSize = 256 * 1024) {
    this.fh = fh;
    this.buf = new Uint8Array(chunkSize);
  }

  write(bytes: Uint8Array): void {
    let o = 0;
    while (o < bytes.length) {
      if (this.pos === this.buf.length) this.flushSync();
      const take = Math.min(this.buf.length - this.pos, bytes.length - o);
      this.buf.set(bytes.subarray(o, o + take), this.pos);
      this.pos += take;
      o += take;
    }
  }

  /** Returns the number of bytes written so far. */
  get length(): number {
    return this.filePos + this.pos;
  }

  private flushSync(): void {
    if (this.pos === 0) return;
    writeAllSync(this.fh, this.buf, this.pos, this.filePos);
    this.filePos += this.pos;
    this.pos = 0;
  }

  /** Flushes any remaining buffered bytes and fsyncs the file. */
  async flush(): Promise<void> {
    this.flushSync();
    await this.fh.sync();
  }
}

/** Writes all of `buf[0..len)` to `handle`'s fd starting at `position`. */
function writeAllSync(handle: FileHandle, buf: Uint8Array, len: number, position: number): void {
  const fd = handle.fd;
  let offset = 0;
  while (offset < len) {
    const written = writeSync(fd, buf, offset, len - offset, position + offset);
    offset += written;
  }
}
