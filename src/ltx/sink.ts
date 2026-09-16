/**
 * Output sink abstraction for the LTX encoder and other writers.
 * A sink must accept ordered byte chunks; the encoder counts total bytes via
 * its own `n` counter so sinks do not need to be seekable.
 */
export interface ByteSink {
  write(bytes: Uint8Array): void;
}

/** In-memory sink that accumulates bytes into a single growable buffer. */
export class BufferSink implements ByteSink {
  private chunks: Uint8Array[] = [];
  private _length = 0;

  write(bytes: Uint8Array): void {
    this.chunks.push(Uint8Array.from(bytes));
    this._length += bytes.length;
  }

  get length(): number {
    return this._length;
  }

  toBuffer(): Uint8Array {
    const out = new Uint8Array(this._length);
    let o = 0;
    for (const c of this.chunks) {
      out.set(c, o);
      o += c.length;
    }
    return out;
  }
}
