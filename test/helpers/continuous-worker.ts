/**
 * A minimal worker-thread entry point that runs continuous replication for a
 * database path + replica client path, demonstrating how to keep `Replica.run()`
 * out of the application's main thread.
 *
 * Protocol (messages on parentPort):
 *   in : { type: 'start', dbPath, clientPath, intervalMs? }
 *   out: { type: 'started' }
 *   in : { type: 'stop' }
 *   out: { type: 'done' }
 */

import { parentPort } from 'node:worker_threads';
import { Replica, FileReplicaClient } from '../../src/index.ts';

if (parentPort) {
  parentPort.on('message', async (msg) => {
    if (msg?.type !== 'start') return;
    const { dbPath, clientPath, intervalMs = 25 } = msg as {
      dbPath: string;
      clientPath: string;
      intervalMs?: number;
    };
    try {
      const replica = new Replica(dbPath, new FileReplicaClient(clientPath));
      const ac = new AbortController();
      const runP = replica.run({ intervalMs, signal: ac.signal });
      parentPort!.postMessage({ type: 'started' });

      parentPort!.on('message', async (m) => {
        if (m?.type === 'stop') {
          ac.abort();
          await runP;
          parentPort!.postMessage({ type: 'done' });
        }
      });
    } catch (err) {
      parentPort!.postMessage({ type: 'error', message: String(err) });
    }
  });
}
