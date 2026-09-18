/**
 * In-memory S3 fake for tests. Implements the minimal `S3ClientLike` contract:
 * a `send(command)` method that handles the AWS SDK v3 commands used by
 * {@link S3ReplicaClient}. Objects live in a Map keyed by `bucket/key`.
 */

import { Readable } from 'node:stream';
import {
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
} from '@aws-sdk/client-s3';

interface StoredObject {
  body: Buffer;
  metadata: Record<string, string>;
  lastModified: Date;
}

export class MemoryS3 {
  readonly objects = new Map<string, StoredObject>();

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
    throw new Error(`MemoryS3: unsupported command ${command?.constructor?.name}`);
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
