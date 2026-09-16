/** SQLite WAL (Write-Ahead Log) file constants. */

export const WALHeaderSize = 32;
export const WALFrameHeaderSize = 24;

/** WAL magic values (always read big-endian; determines checksum byte order). */
export const WALMagicLE = 0x377f0682; // checksums are computed little-endian
export const WALMagicBE = 0x377f0683; // checksums are computed big-endian

export const WALVersion = 3007000;
