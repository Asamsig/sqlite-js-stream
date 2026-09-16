/** Minimal structured logger interface (stand-in for Go's slog.Logger). */
export interface Logger {
  debug(msg: string, ...args: unknown[]): void;
  info(msg: string, ...args: unknown[]): void;
  warn(msg: string, ...args: unknown[]): void;
  error(msg: string, ...args: unknown[]): void;
}

/** A no-op logger used when none is provided. */
export const noopLogger: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
};
