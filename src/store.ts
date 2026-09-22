/**
 * Store manages one or more Replicas (a litestream instance).
 *
 * MVP: a thin orchestration layer that opens replicas for database paths with
 * their replica clients, syncs/snapshots them, and closes. Monitor loops,
 * scheduling, and retention are future work.
 */

import type { Replica, RetentionOptions, ReplicaRunOptions } from './replica.ts';
import { Replica as ReplicaClass } from './replica.ts';
import type { ReplicaClient } from './replica-client.ts';

/** Store coordinates multiple replicated databases. */
export class Store {
  private items: Replica[] = [];

  /** OpenDB opens a replica for a database path and wires it to a client. */
  async openDB(path: string, client: ReplicaClient): Promise<Replica> {
    const replica = new ReplicaClass(path, client);
    this.items.push(replica);
    return replica;
  }

  /** Replicas returns all open replicas. */
  replicas(): Replica[] {
    return [...this.items];
  }

  /** SyncAll syncs every database (local writes then replica upload). */
  async syncAll(): Promise<void> {
    for (const replica of this.items) {
      await replica.sync();
    }
  }

  /** SnapshotAll writes & uploads level-9 snapshots for every database. */
  async snapshotAll(): Promise<void> {
    for (const replica of this.items) {
      await replica.snapshot();
    }
  }

  /** CompactAll compacts pending LTX files for every database. */
  async compactAll(): Promise<void> {
    for (const replica of this.items) {
      await replica.compact();
    }
  }

  /** EnforceRetentionAll prunes old snapshots/files for every database. */
  async enforceRetentionAll(options: RetentionOptions = {}): Promise<void> {
    for (const replica of this.items) {
      await replica.enforceRetention(options);
    }
  }

  /**
   * Run starts each replica's continuous sync/compaction/retention loop and
   * resolves once `options.signal` is aborted. Pass per-replica intervals and a
   * retention policy via {@link ReplicaRunOptions}.
   */
  async run(options: ReplicaRunOptions = {}): Promise<void> {
    await Promise.all(this.items.map((replica) => replica.run(options)));
  }

  /** Close releases all replicas' internal connections. */
  close(): void {
    for (const replica of this.items) replica.close();
    this.items = [];
  }
}
