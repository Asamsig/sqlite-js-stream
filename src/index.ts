/**
 * sqlite-js-stream — a Node.js port of Litestream.
 *
 * This is the public entry point. For now it only exports the version marker
 * and placeholder types; the LTX/WAL/DB layers are added in later milestones.
 */

export const VERSION = '0.1.0';

export type TXID = bigint;

export * from './ltx/index.ts';
export * from './wal/index.ts';
export * from './replica-client.ts';
export * from './replica-url.ts';
export * from './backends/file/index.ts';
// The `Database` interface keeps the replication layer driver-agnostic. The
// concrete `node:sqlite`-backed `SQLiteDatabase` is intentionally NOT exported;
// obtain a default via `openDatabase`.
export type { Database } from './db.ts';
export { openDatabase } from './db.ts';
export * from './replica.ts';
export * from './store.ts';
