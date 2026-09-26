/**
 * @platformatic/sqlite-js-stream-s3 — Amazon S3 upload strategy.
 *
 * Importing this module registers the `s3` scheme with the core's replica URL
 * factory, so `newReplicaClientFromURL('s3://bucket/prefix')` resolves after
 * this addon is loaded.
 */

export {
  S3ReplicaClient,
  newReplicaClientFromURL,
  ReplicaClientType,
  type S3ClientLike,
  type S3ReplicaClientOptions,
} from './replica-client.ts';

export { S3Leaser } from './leaser.ts';

import { registerReplicaClientFactory } from '@platformatic/sqlite-js-stream';
import { newReplicaClientFromURL } from './replica-client.ts';

// Register the 's3' URL scheme when the addon is imported.
registerReplicaClientFactory('s3', newReplicaClientFromURL);
