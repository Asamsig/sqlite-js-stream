/**
 * S3 replica client — replicates LTX files to an Amazon S3 bucket.
 *
 * Object layout mirrors the file backend:
 *   `{prefix}ltx/{level}/{minTXID}-{maxTXID}.ltx`
 *
 * The implementation talks to S3 through the AWS SDK v3 {@link S3Client}. A
 * minimal `send`-shaped client can be injected (e.g. an in-memory fake in
 * tests) so the upload strategy is testable without network access.
 *
 * Creation timestamps are preserved on upload as the `ltx-timestamp` object
 * metadata; listing falls back to the object's `LastModified`.
 */

import { Readable } from 'node:stream';

import {
  S3Client,
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  UploadPartCommand,
} from '@aws-sdk/client-s3';

import {
  HeaderSize,
  LTXError,
  formatFilename,
  noopLogger,
  parseFilename,
  unmarshalHeader,
  type FileInfo,
  type Logger,
  type LTXFilesOptions,
  type OpenLTXFileOptions,
  type ReplicaClient,
  type WriteLTXFileOptions,
} from 'sqlite-js-stream';

/** The strategy identifier reported by {@link S3ReplicaClient.type}. */
export const ReplicaClientType = 's3';

/**
 * Minimal shape of a client used to send commands. The AWS SDK v3 `S3Client`
 * satisfies this; tests inject an in-memory fake instead of hitting the network.
 */
export interface S3ClientLike {
  send(command: any): Promise<any>;
}

/** Configuration for {@link S3ReplicaClient}. */
export interface S3ReplicaClientOptions {
  /** S3 bucket name. Required. */
  bucket: string;
  /** Object key prefix (e.g. `backup`). Trailing slash is added as needed. */
  prefix?: string;
  /** AWS region. Defaults to the AWS SDK region resolution chain. */
  region?: string;
  /** Custom endpoint URL (e.g. for MinIO / LocalStack). */
  endpoint?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  sessionToken?: string;
  /** Inject a preconfigured client. Mostly for tests. */
  client?: S3ClientLike;
}

/** Normalized base prefix (no leading/trailing slash, `''` when empty). */
function normalizePrefix(prefix: string): string {
  return prefix.replace(/^\/+/, '').replace(/\/+$/, '');
}

/** Concatenate byte chunks into a single Uint8Array. */
function concatBytes(chunks: readonly Uint8Array[]): Uint8Array {
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

/** S3 requires every multipart part (except the last) to be at least 5 MiB. */
const MULTIPART_MIN_PART_SIZE = 5 * 1024 * 1024;

/** S3ReplicaClient writes LTX files to an Amazon S3 bucket. */
export class S3ReplicaClient implements ReplicaClient {
  readonly bucket: string;
  readonly prefix: string;
  private client: S3ClientLike;
  private logger: Logger = noopLogger;

  constructor(opts: S3ReplicaClientOptions) {
    if (!opts.bucket) {
      throw new Error('S3 replica client: bucket is required');
    }
    this.bucket = opts.bucket;
    this.prefix = normalizePrefix(opts.prefix ?? '');
    this.client =
      opts.client ??
      new S3Client({
        region: opts.region,
        endpoint: opts.endpoint,
        credentials: opts.accessKeyId
          ? {
              accessKeyId: opts.accessKeyId,
              secretAccessKey: opts.secretAccessKey ?? '',
              sessionToken: opts.sessionToken,
            }
          : undefined,
      });
  }

  type(): string {
    return ReplicaClientType;
  }

  /** Best-effort connectivity check; real I/O errors surface on first use. */
  async init(): Promise<void> {
    try {
      await this.client.send(new HeadBucketCommand({ Bucket: this.bucket }));
    } catch (err) {
      this.logger.debug('s3 head bucket failed', err);
    }
  }

  setLogger(logger: Logger): void {
    this.logger = logger;
  }

  /** Returns the object key prefix (empty string if none). */
  prefixKey(): string {
    return this.prefix;
  }

  /** Returns `{prefix}ltx/{level}/`. */
  private levelPrefix(level: number): string {
    return `${this.prefix ? `${this.prefix}/` : ''}ltx/${level}/`;
  }

  /** Returns the object key for a level and TXID range. */
  private fileKey(level: number, minTXID: bigint, maxTXID: bigint): string {
    return `${this.levelPrefix(level)}${formatFilename(minTXID, maxTXID)}`;
  }

  async *ltxFiles(opts: LTXFilesOptions): AsyncIterable<FileInfo> {
    const prefix = this.levelPrefix(opts.level);
    let token: string | undefined;
    do {
      const res = await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          Prefix: prefix,
          ContinuationToken: token,
        }),
      );
      for (const obj of res.Contents ?? []) {
        if (!obj.Key) continue;
        const name = obj.Key.slice(prefix.length);
        let minTXID: bigint;
        let maxTXID: bigint;
        try {
          ({ minTXID, maxTXID } = parseFilename(name));
        } catch {
          continue; // skip non-conforming keys
        }
        if (minTXID < opts.seek) continue;
        yield {
          level: opts.level,
          minTXID,
          maxTXID,
          size: obj.Size ?? 0,
          createdAt: (obj.LastModified ?? new Date(0)).getTime(),
        };
      }
      token = res.IsTruncated ? (res.NextContinuationToken ?? undefined) : undefined;
    } while (token);
  }

  async openLtxFile(opts: OpenLTXFileOptions): Promise<Readable> {
    const key = this.fileKey(opts.level, opts.minTXID, opts.maxTXID);

    // A zero offset/size is a full read; no HeadObject round-trip needed.
    if (opts.offset === 0 && opts.size === 0) {
      try {
        const res = await this.client.send(new GetObjectCommand({ Bucket: this.bucket, Key: key }));
        return res.Body as Readable;
      } catch (err) {
        throw new LTXError('open', key, opts.level, opts.minTXID, opts.maxTXID, err);
      }
    }

    // Need the object size to build a range and to handle offset >= size.
    let head;
    try {
      head = await this.client.send(new HeadObjectCommand({ Bucket: this.bucket, Key: key }));
    } catch (err) {
      throw new LTXError('open', key, opts.level, opts.minTXID, opts.maxTXID, err);
    }
    const len = Number(head.ContentLength ?? 0);
    if (opts.offset >= len) return Readable.from([]);

    const end = opts.size > 0 ? Math.min(opts.offset + opts.size - 1, len - 1) : len - 1;
    if (end < opts.offset) return Readable.from([]);

    try {
      const res = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Range: `bytes=${opts.offset}-${end}`,
        }),
      );
      return res.Body as Readable;
    } catch (err) {
      throw new LTXError('open', key, opts.level, opts.minTXID, opts.maxTXID, err);
    }
  }

  async writeLtxFile(opts: WriteLTXFileOptions, body: AsyncIterable<Uint8Array | string>): Promise<FileInfo> {
    const key = this.fileKey(opts.level, opts.minTXID, opts.maxTXID);

    // Capture the LTX header (timestamp) from the leading bytes, and stream the
    // rest. Small objects use a single PutObject; large objects (> 5 MiB) are
    // uploaded with the multipart API so the whole file is never buffered.
    let headerBytes: Uint8Array | null = null;
    let head: Uint8Array[] = [];
    let headLen = 0;

    let acc: Uint8Array[] = [];
    let accLen = 0;
    let size = 0;

    let uploadId: string | null = null;
    let partNumber = 0;
    const uploadedParts: Array<{ PartNumber: number; ETag: string }> = [];

    const uploadPart = async (buf: Uint8Array): Promise<void> => {
      partNumber++;
      const res = await this.client.send(
        new UploadPartCommand({
          Bucket: this.bucket,
          Key: key,
          UploadId: uploadId!,
          PartNumber: partNumber,
          Body: buf,
        }),
      );
      uploadedParts.push({ PartNumber: partNumber, ETag: res.ETag ?? `etag-${partNumber}` });
    };

    try {
      for await (const chunk of body) {
        const bytes =
          typeof chunk === 'string' ? new TextEncoder().encode(chunk) : Uint8Array.from(chunk);
        if (bytes.length === 0) continue;
        size += bytes.length;

        if (headerBytes === null) {
          head.push(bytes);
          headLen += bytes.length;
          if (headLen >= HeaderSize) {
            headerBytes = concatBytes(head).subarray(0, HeaderSize);
            head = [];
            headLen = 0;
          }
        }

        acc.push(bytes);
        accLen += bytes.length;

        if (uploadId === null && accLen >= MULTIPART_MIN_PART_SIZE) {
          const res = await this.client.send(
            new CreateMultipartUploadCommand({
              Bucket: this.bucket,
              Key: key,
              Metadata: { 'ltx-timestamp': String(Number(unmarshalHeader(headerBytes!).timestamp)) },
            }),
          );
          uploadId = res.UploadId;
          await uploadPart(concatBytes(acc));
          acc = [];
          accLen = 0;
        } else if (uploadId !== null && accLen >= MULTIPART_MIN_PART_SIZE) {
          await uploadPart(concatBytes(acc));
          acc = [];
          accLen = 0;
        }
      }

      if (headerBytes === null) {
        throw new Error('write ltx file: input too short to contain a header');
      }
      const timestamp = Number(unmarshalHeader(headerBytes).timestamp);
      const metadata = { 'ltx-timestamp': String(timestamp) };

      if (uploadId === null) {
        // Single PutObject for small objects.
        const buf = concatBytes(acc);
        await this.client.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: key,
            Body: buf,
            ContentLength: size,
            Metadata: metadata,
          }),
        );
      } else {
        // Multipart: upload any remaining bytes as the final part, then complete.
        if (accLen > 0) await uploadPart(concatBytes(acc));
        await this.client.send(
          new CompleteMultipartUploadCommand({
            Bucket: this.bucket,
            Key: key,
            UploadId: uploadId,
            MultipartUpload: { Parts: uploadedParts },
          }),
        );
      }

      return {
        level: opts.level,
        minTXID: opts.minTXID,
        maxTXID: opts.maxTXID,
        size,
        createdAt: timestamp,
      };
    } catch (err) {
      if (uploadId !== null) {
        await this.client
          .send(
            new AbortMultipartUploadCommand({
              Bucket: this.bucket,
              Key: key,
              UploadId: uploadId,
            }),
          )
          .catch(() => {});
      }
      throw new LTXError('write', key, opts.level, opts.minTXID, opts.maxTXID, err);
    }
  }

  async deleteLtxFiles(infos: FileInfo[]): Promise<void> {
    const objects = infos.map((info) => ({
      Key: this.fileKey(info.level, info.minTXID, info.maxTXID),
    }));
    for (let i = 0; i < objects.length; i += 1000) {
      await this.client.send(
        new DeleteObjectsCommand({
          Bucket: this.bucket,
          Delete: { Objects: objects.slice(i, i + 1000), Quiet: true },
        }),
      );
    }
  }

  async deleteAll(): Promise<void> {
    const prefix = this.prefix ? `${this.prefix}/` : '';
    let token: string | undefined;
    do {
      const res = await this.client.send(
        new ListObjectsV2Command({ Bucket: this.bucket, Prefix: prefix, ContinuationToken: token }),
      );
      const contents = (res.Contents ?? []) as Array<{ Key?: string }>;
      const objects = contents.filter((o) => o.Key).map((o) => ({ Key: o.Key! }));
      for (let i = 0; i < objects.length; i += 1000) {
        await this.client.send(
          new DeleteObjectsCommand({
            Bucket: this.bucket,
            Delete: { Objects: objects.slice(i, i + 1000), Quiet: true },
          }),
        );
      }
      token = res.IsTruncated ? (res.NextContinuationToken ?? undefined) : undefined;
    } while (token);
  }
}

/**
 * Builds an S3ReplicaClient from parsed replica URL parts. `host` is the bucket,
 * `urlPath` is the prefix, `query` may carry `region`/`endpoint`, and `userinfo`
 * carries `accessKeyId:secretAccessKey`.
 */
export function newReplicaClientFromURL(
  _scheme: string,
  host: string,
  urlPath: string,
  query: URLSearchParams | null,
  userinfo: string | null,
): ReplicaClient {
  const opts: S3ReplicaClientOptions = {
    bucket: host,
    prefix: normalizePrefix(urlPath),
    region: query?.get('region') ?? undefined,
    endpoint: query?.get('endpoint') ?? undefined,
  };
  if (userinfo) {
    const [accessKeyId, secretAccessKey] = userinfo.split(':');
    opts.accessKeyId = accessKeyId;
    opts.secretAccessKey = secretAccessKey;
  }
  return new S3ReplicaClient(opts);
}
