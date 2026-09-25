/**
 * Shared error base for sqlite-js-stream.
 *
 * Every typed library error extends {@link SqliteJsStreamError} and carries a
 * stable, Node-style `code` string (e.g. `ERR_NO_MATCHING_BACKUP`). This lets
 * callers catch any library error with a single `instanceof`, and branch on the
 * precise failure via `err.code` without matching message text.
 */
export class SqliteJsStreamError extends Error {
  /** Stable, machine-readable error code (Node.js convention). */
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = 'SqliteJsStreamError';
    this.code = code;
  }
}
