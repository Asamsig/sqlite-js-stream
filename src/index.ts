/**
 * sqlite-js-stream — a Node.js port of Litestream.
 *
 * This is the public entry point. For now it only exports the version marker
 * and placeholder types; the LTX/WAL/DB layers are added in later milestones.
 */

export const VERSION = '0.1.0';

export type TXID = bigint;

export * from './errors.ts';
export * from './ltx/index.ts';
export * from './wal/index.ts';
export * from './replica-client.ts';
export * from './replica-url.ts';
export * from './backends/file/index.ts';
// The DB layer (`src/db.ts`) is intentionally NOT part of the public API.
// `Replica`/`Store` take a database path and read the DB + WAL directly, so the
// library is agnostic to whatever SQLite driver the application uses.
export * from './replica.ts';
export * from './store.ts';
export * from './leaser.ts';
// Logger is part of the public ReplicaClient contract; expose it so addons
// (e.g. the S3 backend) can implement the interface consistently.
export * from './util/logger.ts';
