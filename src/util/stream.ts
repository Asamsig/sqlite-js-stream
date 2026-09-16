/** Stream helpers for consuming/capturing LTX byte streams. */

/** Concatenates an array of byte chunks into a single Uint8Array. */
export function concatBytes(chunks: readonly Uint8Array[]): Uint8Array {
  let total = 0;
  for (const c of chunks) total += c.length;
  const out = new Uint8Array(total);
  let o = 0;
  for (const c of chunks) {
    out.set(c, o);
    o += c.length;
  }
  return out;
}

/**
 * Reads an entire async iterable of byte chunks into a single Uint8Array.
 * String chunks are encoded as UTF-8.
 */
export async function readAll(
  stream: AsyncIterable<Uint8Array | string> | { [Symbol.asyncIterator](): AsyncIterator<Uint8Array | string> },
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of stream) {
    chunks.push(typeof chunk === 'string' ? new TextEncoder().encode(chunk) : Uint8Array.from(chunk));
  }
  return concatBytes(chunks);
}
