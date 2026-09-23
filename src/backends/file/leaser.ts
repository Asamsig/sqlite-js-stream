/**
 * File-backend Leaser.
 *
 * Stores the lease as `<path>/lock.json` and uses an exclusive create + a
 * content hash as the conditional-write token. The file backend is primarily
 * for local/single-host use; for real multi-host safety use the S3 leaser
 * (which uses S3 `If-None-Match`/`If-Match` conditional writes).
 */

import { createHash } from 'node:crypto';
import { open, readFile, rename, rm, writeFile } from 'node:fs/promises';
import nodePath from 'node:path';

import {
  DefaultLeasePath,
  DefaultLeaseTTL,
  ErrLeaseNotHeld,
  Lease,
  LeaseExistsError,
  type Leaser,
} from '../../leaser.ts';

function sha256(data: Uint8Array): string {
  return createHash('sha256').update(data).digest('hex');
}

function encodeLease(l: Lease): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify({
      generation: l.generation,
      expires_at: new Date(l.expiresAt).toISOString(),
      owner: l.owner,
    }),
  );
}

function decodeLease(data: Uint8Array, etag: string): Lease {
  const parsed = JSON.parse(new TextDecoder().decode(data)) as {
    generation?: number;
    expires_at?: string;
    owner?: string;
  };
  const expiresAt = Date.parse(parsed.expires_at ?? '');
  if (Number.isNaN(expiresAt)) {
    throw new Error('invalid lease file: bad expires_at');
  }
  return new Lease(parsed.generation ?? 1, expiresAt, parsed.owner ?? '', etag);
}

/** Reads and decodes the lease file, returning null if it does not exist. */
async function readLease(lockPath: string): Promise<Lease | null> {
  try {
    const data = await readFile(lockPath);
    return decodeLease(data, sha256(data));
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw err;
  }
}

/** FileLeaser implements {@link Leaser} for a local directory. */
export class FileLeaser implements Leaser {
  private lockPath: string;
  private ttlMs: number;

  constructor(path: string, lockName = DefaultLeasePath, ttlMs = DefaultLeaseTTL) {
    this.lockPath = nodePath.join(path, lockName);
    this.ttlMs = ttlMs;
  }

  async acquireLease(): Promise<Lease> {
    const existing = await readLease(this.lockPath);
    if (existing !== null && !existing.isExpired()) {
      throw new LeaseExistsError(existing.owner, existing.expiresAt);
    }

    let generation = 1;
    if (existing !== null) generation = existing.generation + 1;

    const lease = new Lease(generation, Date.now() + this.ttlMs, this.owner());
    const bytes = encodeLease(lease);

    if (existing === null) {
      // Exclusive create so we do not clobber a concurrent acquirer.
      let fh;
      try {
        fh = await open(this.lockPath, 'wx');
      } catch (err) {
        const code = (err as NodeJS.ErrnoException).code;
        if (code === 'EEXIST') {
          const current = await readLease(this.lockPath);
          if (current !== null && !current.isExpired()) {
            throw new LeaseExistsError(current.owner, current.expiresAt);
          }
          // The other writer lost the race; retry the expired-overwrite path.
          return this.acquireLease();
        }
        throw err;
      }
      await fh.write(bytes);
      await fh.close();
    } else {
      // Existing lease expired: atomically replace it.
      const tmp = `${this.lockPath}.tmp-${process.pid}-${Date.now()}`;
      await writeFile(tmp, bytes);
      await rename(tmp, this.lockPath);
    }

    const written = await readLease(this.lockPath);
    return written ?? lease;
  }

  async renewLease(lease: Lease): Promise<Lease> {
    const current = await readLease(this.lockPath);
    if (current === null) throw new ErrLeaseNotHeld();
    if (current.etag !== lease.etag) throw new ErrLeaseNotHeld();

    const renewed = new Lease(lease.generation, Date.now() + this.ttlMs, lease.owner);
    const bytes = encodeLease(renewed);
    const tmp = `${this.lockPath}.tmp-${process.pid}-${Date.now()}`;
    await writeFile(tmp, bytes);
    await rename(tmp, this.lockPath);

    const written = await readLease(this.lockPath);
    return written ?? renewed;
  }

  async releaseLease(lease: Lease): Promise<void> {
    const current = await readLease(this.lockPath);
    if (current === null) return; // already released
    if (current.etag !== lease.etag) throw new ErrLeaseNotHeld();
    await rm(this.lockPath, { force: true });
  }

  private owner(): string {
    return `${process.pid}`;
  }
}
