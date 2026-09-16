/** File-system helpers for atomic writes and directory fsync. */

import { mkdir, open } from 'node:fs/promises';
import { dirname } from 'node:path';

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
