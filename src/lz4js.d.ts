/**
 * Ambient type declarations for the `lz4js` package (no upstream types).
 * We only use the block-format functions required by the LTX format.
 */
declare module 'lz4js' {
  export function compressBound(n: number): number;
  export function decompressBound(src: Uint8Array): number;
  export function compressBlock(
    src: Uint8Array,
    dst: Uint8Array,
    sIndex?: number,
    sLength?: number,
    hashTable?: number[],
  ): number;
  export function decompressBlock(
    src: Uint8Array,
    dst: Uint8Array,
    sIndex?: number,
    sLength?: number,
    dIndex?: number,
  ): number;
  export function compressFrame(src: Uint8Array, dst: Uint8Array): number;
  export function decompressFrame(src: Uint8Array, dst: Uint8Array): number;
  export function compress(src: Uint8Array, maxSize?: number): Uint8Array;
  export function decompress(src: Uint8Array, maxSize?: number): Uint8Array;
  export function makeBuffer(n: number): Uint8Array;
}
