/**
 * In-memory S3 fake for tests. Implements the minimal `S3ClientLike` contract:
 * a `send(command)` method that handles the AWS SDK v3 commands used by
 * {@link S3ReplicaClient}. Objects live in a Map keyed by `bucket/key`.
 */

import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import {
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

interface StoredObject {
  body: Buffer;
  metadata: Record<string, string>;
  lastModified: Date;
}

interface MultipartUpload {
  bucket: string;
  key: string;
  parts: Map<number, Buffer>;
  metadata: Record<string, string>;
}

/** Normalizes an SDK Body value to a Buffer. */
function toBuffer(raw: unknown): Buffer {
  if (Buffer.isBuffer(raw)) return Buffer.from(raw);
  if (raw instanceof Uint8Array) return Buffer.from(raw);
  if (raw !== undefined && raw !== null) return Buffer.from(String(raw));
  return Buffer.alloc(0);
}

export class MemoryS3 {
  readonly objects = new Map<string, StoredObject>();
  readonly multiparts = new Map<string, MultipartUpload>();

  async send(command: any): Promise<any> {
    if (command instanceof ListObjectsV2Command) {
      return this.list(command);
    }
    if (command instanceof GetObjectCommand) {
      return this.get(command);
    }
    if (command instanceof PutObjectCommand) {
      return this.put(command);
    }
    if (command instanceof DeleteObjectsCommand) {
      return this.delete(command);
    }
    if (command instanceof HeadObjectCommand) {
      return this.head(command);
    }
    if (command instanceof HeadBucketCommand) {
      return {};
    }
    if (command instanceof CreateMultipartUploadCommand) {
      return this.createMultipart(command);
    }
    if (command instanceof UploadPartCommand) {
      return this.uploadPart(command);
    }
    if (command instanceof CompleteMultipartUploadCommand) {
      return this.completeMultipart(command);
    }
    if (command instanceof AbortMultipartUploadCommand) {
      return this.abortMultipart(command);
    }
    throw new Error(`MemoryS3: unsupported command ${command?.constructor?.name}`);
  }

  private createMultipart(cmd: any) {
    const uploadId = `upload-${this.multiparts.size + 1}`;
    this.multiparts.set(uploadId, {
      bucket: cmd.input.Bucket,
      key: cmd.input.Key,
      parts: new Map(),
      metadata: cmd.input.Metadata ?? {},
    });
    return { UploadId: uploadId };
  }

  private uploadPart(cmd: any) {
    const mp = this.multiparts.get(cmd.input.UploadId);
    if (!mp) throw notFound(cmd.input.UploadId);
    const body = toBuffer(cmd.input.Body);
    mp.parts.set(cmd.input.PartNumber, body);
    return { ETag: `fake-etag-${cmd.input.PartNumber}` };
  }

  private completeMultipart(cmd: any) {
    const mp = this.multiparts.get(cmd.input.UploadId);
    if (!mp) throw notFound(cmd.input.UploadId);
    const partNumbers = [...mp.parts.keys()].sort((a, b) => a - b);
    const chunks = partNumbers.map((n) => mp.parts.get(n)!);
    const body = Buffer.concat(chunks);
    this.objects.set(this.key(mp.bucket, mp.key), {
      body,
      metadata: mp.metadata,
      lastModified: new Date(),
    });
    this.multiparts.delete(cmd.input.UploadId);
    return { ETag: fakeEtag(body), Key: mp.key };
  }

  private abortMultipart(cmd: any) {
    this.multiparts.delete(cmd.input.UploadId);
    return {};
  }

  private key(bucket: string, key: string): string {
    return `${bucket}/${key}`;
  }

  private list(cmd: any) {
    const prefix = cmd.input.Prefix ?? '';
    const contents: any[] = [];
    for (const [k, obj] of this.objects) {
      const slash = k.indexOf('/');
      const bucket = k.slice(0, slash);
      if (bucket !== cmd.input.Bucket) continue;
      const key = k.slice(slash + 1);
      if (!key.startsWith(prefix)) continue;
      contents.push({
        Key: key,
        Size: obj.body.length,
        LastModified: obj.lastModified,
      });
    }
    return { Contents: contents, IsTruncated: false };
  }

  private get(cmd: any) {
    const k = this.key(cmd.input.Bucket, cmd.input.Key);
    const obj = this.objects.get(k);
    if (!obj) throw notFound(cmd.input.Key);
    let body = obj.body;
    if (cmd.input.Range) {
      const m = /^bytes=(\d+)-(\d*)$/.exec(cmd.input.Range);
      if (m) {
        const start = Number(m[1]);
        const end = m[2] === '' ? obj.body.length - 1 : Number(m[2]);
        body = obj.body.subarray(start, end + 1);
      }
    }
    return {
      Body: Readable.from(body),
      ContentLength: body.length,
      LastModified: obj.lastModified,
      Metadata: obj.metadata,
    };
  }

  private put(cmd: any) {
    const k = this.key(cmd.input.Bucket, cmd.input.Key);
    const raw = cmd.input.Body;
    const body = Buffer.isBuffer(raw)
      ? Buffer.from(raw)
      : raw instanceof Uint8Array
        ? Buffer.from(raw)
        : raw !== undefined
          ? Buffer.from(String(raw))
          : Buffer.alloc(0);
    this.objects.set(k, {
      body,
      metadata: cmd.input.Metadata ?? {},
      lastModified: new Date(),
    });
    return { ETag: 'fake', ContentLength: body.length };
  }

  private delete(cmd: any) {
    for (const obj of cmd.input.Delete.Objects ?? []) {
      this.objects.delete(this.key(cmd.input.Bucket, obj.Key));
    }
    return { Deleted: cmd.input.Delete.Objects ?? [] };
  }

  private head(cmd: any) {
    const k = this.key(cmd.input.Bucket, cmd.input.Key);
    const obj = this.objects.get(k);
    if (!obj) throw notFound(cmd.input.Key);
    return {
      ContentLength: obj.body.length,
      Metadata: obj.metadata,
      LastModified: obj.lastModified,
    };
  }
}

function notFound(key: string): Error {
  const err = new Error(`Not Found: ${key}`);
  (err as any).name = 'NotFound';
  return err;
}

function fakeEtag(body: Buffer): string {
  return `"${createHash('md5').update(body).digest('hex')}"`;
}
