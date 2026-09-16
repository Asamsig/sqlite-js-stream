/**
 * LTX file metadata and iteration over a set of LTX files.
 */

import type { FileInfo, TXID } from './types.ts';
import { isContiguous } from './types.ts';

/** Sorts FileInfo by (level, minTXID, maxTXID), in place. */
export function sortFileInfo(a: FileInfo[]): FileInfo[] {
  a.sort((x, y) => {
    if (x.level !== y.level) return x.level - y.level;
    if (x.minTXID !== y.minTXID) return x.minTXID < y.minTXID ? -1 : 1;
    return x.maxTXID < y.maxTXID ? -1 : x.maxTXID > y.maxTXID ? 1 : 0;
  });
  return a;
}

/** MaxTXID returns the largest maxTXID in a slice, or 0n if empty. */
export function maxTXID(a: readonly FileInfo[]): TXID {
  if (a.length === 0) return 0n;
  let m = a[0].maxTXID;
  for (const info of a) {
    if (info.maxTXID > m) m = info.maxTXID;
  }
  return m;
}

/** SliceFileIterator iterates over a sorted slice of LTX file metadata. */
export class FileInfoSliceIterator {
  private a: FileInfo[];
  private init = false;
  private current: FileInfo | null = null;

  constructor(a: FileInfo[]) {
    this.a = sortFileInfo([...a]);
  }

  next(): boolean {
    if (!this.init) {
      this.init = true;
      if (this.a.length === 0) {
        this.current = null;
        return false;
      }
      this.current = this.a[0];
      return true;
    }
    this.a = this.a.slice(1);
    if (this.a.length === 0) {
      this.current = null;
      return false;
    }
    this.current = this.a[0];
    return true;
  }

  err(): Error | null {
    return null;
  }

  item(): FileInfo | null {
    return this.current;
  }

  close(): void {
    /* nothing to release */
  }
}

/** Returns all LTX files from an iterator as an array. */
export function sliceFileIterator(itr: FileInfoSliceIterator): FileInfo[] {
  const out: FileInfo[] = [];
  while (itr.next()) {
    const item = itr.item();
    if (item) out.push(item);
  }
  itr.close();
  return out;
}

export { isContiguous };
