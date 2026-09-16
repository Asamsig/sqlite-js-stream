/**
 * Test helpers to construct LTX files and deterministic page payloads.
 */

import {
  BufferSink,
  checksumReader,
  Encoder,
  type Header,
  type PageHeader,
  type Trailer,
} from '../../src/ltx/index.ts';

export interface PageSpec {
  header: PageHeader;
  data: Uint8Array;
}

export interface FileSpec {
  header: Header;
  pages: PageSpec[];
  trailer: Trailer;
}

/** Creates a deterministic page of `size` bytes filled with `seed + (i % 7)`. */
export function makePage(seed: number, size: number): Uint8Array {
  const b = new Uint8Array(size);
  for (let i = 0; i < size; i++) b[i] = (seed + (i % 7)) & 0xff;
  return b;
}

/** Encodes a FileSpec to bytes; mutates `spec.trailer` with the file checksum. */
export function writeFileSpec(spec: FileSpec): Uint8Array {
  const sink = new BufferSink();
  const enc = new Encoder(sink);
  enc.encodeHeader(spec.header);
  for (const p of spec.pages) enc.encodePage(p.header, p.data);
  enc.setPostApplyChecksum(spec.trailer.postApplyChecksum);
  enc.close();
  spec.trailer = enc.Trailer();
  return sink.toBuffer();
}

/** Builds a minimal snapshot FileSpec with pages 1..commit (pageSize = 512). */
export function makeSnapshot(commit: number, pageSize = 512): FileSpec {
  const pages = Array.from({ length: commit }, (_, i) => ({
    header: { pgno: i + 1, flags: 0 },
    data: makePage(i + 1, pageSize),
  }));
  // Compute the real rolling DB checksum so the decoder's snapshot checksum
  // validation passes (the snapshot's post-apply checksum must match).
  const db = new Uint8Array(commit * pageSize);
  for (let i = 0; i < commit; i++) db.set(pages[i].data, i * pageSize);
  const postApplyChecksum = checksumReader(db, pageSize);

  return {
    header: {
      version: 3,
      flags: 0,
      pageSize,
      commit,
      minTXID: 1n,
      maxTXID: 4n,
      timestamp: 1000n,
      preApplyChecksum: 0n,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    },
    pages,
    trailer: { postApplyChecksum, fileChecksum: 0n },
  };
}
