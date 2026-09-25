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

/** A required function argument is missing or invalid (`ERR_INVALID_ARGUMENT`). */
export class InvalidArgumentError extends SqliteJsStreamError {
  constructor(message: string) {
    super('ERR_INVALID_ARGUMENT', message);
    this.name = 'InvalidArgumentError';
  }
}

/** An incremental LTX file's page size differs from the base database. */
export class PageSizeMismatchError extends SqliteJsStreamError {
  constructor(message: string) {
    super('ERR_PAGE_SIZE_MISMATCH', message);
    this.name = 'PageSizeMismatchError';
  }
}

/** An LTX file is malformed/corrupt or violates the format (`ERR_LTX_FORMAT`). */
export class LTXFormatError extends SqliteJsStreamError {
  constructor(message: string) {
    super('ERR_LTX_FORMAT', message);
    this.name = 'LTXFormatError';
  }
}

/** A SQLite WAL file is malformed/corrupt (`ERR_WAL_FORMAT`). */
export class WALFormatError extends SqliteJsStreamError {
  constructor(message: string) {
    super('ERR_WAL_FORMAT', message);
    this.name = 'WALFormatError';
  }
}

/** A replica URL is malformed or uses an unsupported scheme (`ERR_REPLICA_URL`). */
export class ReplicaURLError extends SqliteJsStreamError {
  constructor(message: string) {
    super('ERR_REPLICA_URL', message);
    this.name = 'ReplicaURLError';
  }
}

/** A file/backend replica operation failed (`ERR_FILE_REPLICA`). */
export class FileReplicaError extends SqliteJsStreamError {
  constructor(message: string) {
    super('ERR_FILE_REPLICA', message);
    this.name = 'FileReplicaError';
  }
}

/** A SQLite database file is missing/invalid or could not be read (`ERR_DATABASE`). */
export class DatabaseError extends SqliteJsStreamError {
  constructor(message: string) {
    super('ERR_DATABASE', message);
    this.name = 'DatabaseError';
  }
}
