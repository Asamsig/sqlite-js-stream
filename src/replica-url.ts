/**
 * Replica URL parsing and replica-client factory registration.
 * Mirrors litestream's `replica_url.go` (simplified: file scheme only for now).
 */

import type { ReplicaClient } from './replica-client.ts';

export type ReplicaClientFactory = (
  scheme: string,
  host: string,
  urlPath: string,
  query: URLSearchParams | null,
  userinfo: string | null,
) => ReplicaClient;

export interface ParsedReplicaURL {
  scheme: string;
  host: string;
  urlPath: string;
  query: URLSearchParams | null;
  userinfo: string | null;
}

const factories = new Map<string, ReplicaClientFactory>();

/** RegisterReplicaClientFactory registers a factory for a URL scheme. */
export function registerReplicaClientFactory(scheme: string, factory: ReplicaClientFactory): void {
  factories.set(scheme, factory);
}

/** NewReplicaClientFromURL creates a ReplicaClient from a URL string. */
export function newReplicaClientFromURL(rawURL: string): ReplicaClient {
  const parsed = parseReplicaURLWithQuery(rawURL);
  const factory = factories.get(parsed.scheme);
  if (!factory) {
    throw new Error(`unsupported replica URL scheme: ${parsed.scheme}`);
  }
  return factory(parsed.scheme, parsed.host, parsed.urlPath, parsed.query, parsed.userinfo);
}

/** ParseReplicaURLWithQuery parses a replica URL into components. */
export function parseReplicaURLWithQuery(s: string): ParsedReplicaURL {
  let u: URL;
  try {
    u = new URL(s);
  } catch (err) {
    throw new Error(`parse replica url ${JSON.stringify(s)}: ${err instanceof Error ? err.message : String(err)}`);
  }

  const scheme = u.protocol.replace(/:$/, '');
  if (scheme === 'file') {
    return {
      scheme: 'file',
      host: '',
      urlPath: u.pathname,
      query: null,
      userinfo: null,
    };
  }
  if (scheme === '') {
    throw new Error(`replica url scheme required: ${s}`);
  }

  return {
    scheme,
    host: u.host,
    urlPath: decodeURIComponent(u.pathname),
    query: u.searchParams,
    userinfo: u.username ? `${u.username}:${u.password}` : null,
  };
}
