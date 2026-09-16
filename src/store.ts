/**
 * Store manages one or more DB + Replica pairs (a litestream instance).
 *
 * MVP: a thin orchestration layer that opens databases with their replica
 * clients, syncs them, and closes them. Monitor loops, scheduling, and
 * retention are future work.
 */

import { openDatabase } from './db.ts';
import type { Database } from './db.ts';
import type { Replica } from './replica.ts';
import { Replica as ReplicaClass } from './replica.ts';
import type { ReplicaClient } from './replica-client.ts';

export interface StoreReplica {
  db: Database;
  replica: Replica;
}

/** Store coordinates multiple replicated databases. */
export class Store {
  private items: StoreReplica[] = [];

  /** OpenDB opens a database (via `openDatabase`) and wires it to a replica client. */
  async openDB(path: string, client: ReplicaClient): Promise<Replica> {
    const db = await openDatabase(path);
    const replica = new ReplicaClass(db, client);
    this.items.push({ db, replica });
    return replica;
  }

  /** Replicas returns all open DB+Replica pairs. */
  replicas(): StoreReplica[] {
    return [...this.items];
  }

  /** SyncAll syncs every database (local writes then replica upload). */
  async syncAll(): Promise<void> {
    for (const item of this.items) {
      await item.db.sync();
      await item.replica.sync();
    }
  }

  /** SnapshotAll writes & uploads level-9 snapshots for every database. */
  async snapshotAll(): Promise<void> {
    for (const item of this.items) {
      await item.replica.snapshot();
    }
  }

  /** Close closes all database connections. */
  close(): void {
    for (const item of this.items) item.db.close();
    this.items = [];
  }
}
