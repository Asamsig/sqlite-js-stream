/**
 * Distributed leasing.
 *
 * When multiple replicas point at the same storage path, only one may be the
 * active writer at a time. A `Leaser` acquires a lease on the replica's storage
 * (a `lock.json` file / object); only the lease holder is the *primary* and may
 * write. Other replicas become idle *followers* that poll until the lease is
 * available.
 *
 * This mirrors litestream's `leaser.go`/`Lease` (a storage-agnostic interface)
 * and `s3/leaser.go` (a conditional-write S3 implementation). Acquire uses
 * `If-None-Match: *` when no lease exists and `If-Match: <etag>` (renew) or
 * `If-Match: <etag>` (release) otherwise, so a stale writer can never clobber a
 * live one.
 */

/** A held lease. `etag` is the conditional-write token for renew/release. */
export class Lease {
  generation: number;
  expiresAt: number; // ms since epoch
  owner: string;
  etag: string | null = null;

  constructor(generation: number, expiresAt: number, owner: string, etag: string | null = null) {
    this.generation = generation;
    this.expiresAt = expiresAt;
    this.owner = owner;
    this.etag = etag;
  }

  /** IsExpired reports whether the lease has passed its expiration. */
  isExpired(now = Date.now()): boolean {
    return now > this.expiresAt;
  }

  /** TTL returns the remaining time before expiration, in ms. */
  ttl(now = Date.now()): number {
    return this.expiresAt - now;
  }
}

/** The lease is already held by another owner. */
export class LeaseExistsError extends Error {
  owner: string;
  expiresAt: number;

  constructor(owner: string, expiresAt: number) {
    super(
      owner !== ''
        ? `lease already held by ${owner} until ${new Date(expiresAt).toISOString()}`
        : `lease already held until ${new Date(expiresAt).toISOString()}`,
    );
    this.name = 'LeaseExistsError';
    this.owner = owner;
    this.expiresAt = expiresAt;
  }
}

/** The caller no longer holds the lease (lost it or never acquired it). */
export class ErrLeaseNotHeld extends Error {
  constructor() {
    super('lease not held');
    this.name = 'ErrLeaseNotHeld';
  }
}

/**
 * Leaser acquires, renews, and releases a lease on a storage path. The
 * implementation must use conditional writes so concurrent acquirers cannot
 * both win.
 */
export interface Leaser {
  /** Acquires the lease, throwing {@link LeaseExistsError} if held elsewhere. */
  acquireLease(): Promise<Lease>;
  /** Renews the lease, throwing {@link ErrLeaseNotHeld} if we lost it. */
  renewLease(lease: Lease): Promise<Lease>;
  /** Releases the lease, throwing {@link ErrLeaseNotHeld} if we lost it. */
  releaseLease(lease: Lease): Promise<void>;
}

export const DefaultLeaseTTL = 30_000; // 30s
export const DefaultLeasePath = 'lock.json';
export const DefaultLeaseRenewalBuffer = 5_000; // renew before it expires
