/**
 * sqlite-js-stream — a Node.js port of Litestream.
 *
 * This is the public entry point. For now it only exports the version marker
 * and placeholder types; the LTX/WAL/DB layers are added in later milestones.
 */

export const VERSION = '0.1.0';

export type TXID = bigint;

export * from './ltx/index.ts';
