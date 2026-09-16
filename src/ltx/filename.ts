/**
 * LTX filename parsing/formatting.
 *
 * Names are `{minTXID:016x}-{maxTXID:016x}.ltx`.
 */

import { formatTXID } from './types.ts';

const FILENAME_RE = /^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$/;

/** FormatFilename returns an LTX filename representing a TXID range. */
export function formatFilename(minTXID: bigint, maxTXID: bigint): string {
  return `${formatTXID(minTXID)}-${formatTXID(maxTXID)}.ltx`;
}

/** ParseFilename parses a transaction range from an LTX filename. */
export function parseFilename(name: string): { minTXID: bigint; maxTXID: bigint } {
  const m = FILENAME_RE.exec(name);
  if (m === null) {
    throw new Error(`invalid ltx filename: ${name}`);
  }
  return {
    minTXID: BigInt(`0x${m[1]}`),
    maxTXID: BigInt(`0x${m[2]}`),
  };
}
