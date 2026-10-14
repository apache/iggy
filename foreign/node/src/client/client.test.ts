// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import assert from 'node:assert/strict';
import { once, type EventEmitter } from 'node:events';
import {
  createServer,
  type AddressInfo,
  type Server,
  type Socket
} from 'node:net';
import { describe, it, before, after } from 'node:test';
import { Client } from './client.js';
import type {
  ClientConfig,
  ClientEventMap,
  ReconnectOption
} from './client.type.js';

const NO_RECONNECT: ReconnectOption = {
  enabled: false, interval: 0, maxRetries: 0
};
const FAST_RECONNECT: ReconnectOption = {
  enabled: true, interval: 10, maxRetries: 3
};

/** Plain TCP peer: the lifecycle events are transport-level and need no protocol. */
const startServer = async (): Promise<{ server: Server, peers: Socket[] }> => {
  const server = createServer();
  const peers: Socket[] = [];
  server.on('connection', (socket) => peers.push(socket));
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return { server, peers };
};

const portOf = (server: Server): number =>
  (server.address() as AddressInfo).port;

const closeServer = (server: Server, peers: Socket[] = []): Promise<void> => {
  for (const peer of peers)
    peer.destroy();
  return new Promise<void>((resolve) => server.close(() => resolve()));
};

/** A port that refuses connections. */
const refusedPort = async (): Promise<number> => {
  const { server } = await startServer();
  const port = portOf(server);
  await closeServer(server);
  return port;
};

const clientConfig = (
  port: number,
  reconnect: ReconnectOption = NO_RECONNECT
): ClientConfig => ({
  transport: 'TCP',
  options: { host: '127.0.0.1', port },
  credentials: { username: 'iggy', password: 'iggy' },
  reconnect,
  heartbeatInterval: 0
});

const record = (client: Client): string[] => {
  const log: string[] = [];
  client
    .on('connecting', () => log.push('connecting'))
    .on('connected', () => log.push('connected'))
    .on('end', () => log.push('end'));
  return log;
};

const next = (client: Client, event: keyof ClientEventMap): Promise<void> =>
  new Promise<void>((resolve) => client.once(event, () => resolve()));

/** Gives a duplicate event that would trail the awaited one time to arrive. */
const settle = (ms = 20): Promise<void> =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

let keepAlive: NodeJS.Timeout;

describe('Client lifecycle events', () => {

  // The reconnect backoff timer is unref'd. Before node v24 that lets the
  // event loop exit while a test still waits on a redial.
  before(() => { keepAlive = setInterval(() => {}, 10000) });

  it('announces one connecting, then one connected, for the first dial',
    async () => {
      const { server, peers } = await startServer();
      const client = new Client(clientConfig(portOf(server)));
      const log = record(client);
      const noop = () => undefined;
      try {
        assert.equal(client.on('connected', noop), client);
        assert.equal(client.once('connected', noop), client);
        assert.equal(client.off('connected', noop), client);
        await next(client, 'connected');
        await settle();
        assert.deepEqual(log, ['connecting', 'connected']);
      } finally {
        await client.destroy();
        await closeServer(server, peers);
      }
    }
  );

  it('emits end once, after destroy, as the last event', async () => {
    const { server, peers } = await startServer();
    const client = new Client(clientConfig(portOf(server)));
    const log = record(client);
    try {
      await next(client, 'connected');
      await client.destroy();
      assert.deepEqual(log, ['connecting', 'connected', 'end']);

      await client.destroy();
      await settle();
      assert.deepEqual(log, ['connecting', 'connected', 'end']);
    } finally {
      await closeServer(server, peers);
    }
  });

  it('emits end when destroyed after the connection already closed',
    async () => {
      const { server, peers } = await startServer();
      const client = new Client(clientConfig(portOf(server)));
      const log = record(client);
      try {
        await next(client, 'connected');
        const socket = client['events'].getReadStream() as Socket;
        peers[0].destroy();
        if (!socket.closed)
          await once(socket, 'close');

        await client.destroy();
        assert.deepEqual(log, ['connecting', 'connected', 'end']);
      } finally {
        await closeServer(server, peers);
      }
    }
  );

  it('runs once listeners once and stops calling removed listeners',
    async () => {
      const { server, peers } = await startServer();
      const client = new Client(clientConfig(portOf(server), FAST_RECONNECT));
      const calls = { kept: 0, once: 0, off: 0, removedOnce: 0 };
      const offListener = () => { calls.off += 1; };
      const removedOnceListener = () => { calls.removedOnce += 1; };
      client.on('connected', () => { calls.kept += 1; });
      client.once('connected', () => { calls.once += 1; });
      client.on('connected', offListener);
      client.once('connected', removedOnceListener);
      client.removeListener('connected', removedOnceListener);
      try {
        await next(client, 'connected');
        client.off('connected', offListener);
        const reconnected = next(client, 'connected');
        peers[0].destroy();
        await reconnected;
        assert.deepEqual(calls, { kept: 2, once: 1, off: 1, removedOnce: 0 });
      } finally {
        await client.destroy();
        await closeServer(server, peers);
      }
    }
  );

  it('works with events.once and leaves no listener behind', async () => {
    const { server, peers } = await startServer();
    const client = new Client(clientConfig(portOf(server)));
    try {
      await once(client as unknown as EventEmitter, 'connected');
      assert.equal(client['events'].listenerCount('connected'), 0);
      assert.equal(client['events'].listenerCount('error'), 0);
    } finally {
      await client.destroy();
      await closeServer(server, peers);
    }
  });

  it('keeps listeners across a reconnect without duplicate events',
    async () => {
      const { server, peers } = await startServer();
      const client = new Client(clientConfig(portOf(server), FAST_RECONNECT));
      const log = record(client);
      try {
        await next(client, 'connected');
        const reconnected = next(client, 'connected');
        peers[0].destroy();
        await reconnected;
        await settle();
        assert.deepEqual(
          log,
          ['connecting', 'connected', 'connecting', 'connected']
        );
      } finally {
        await client.destroy();
        await closeServer(server, peers);
      }
    }
  );

  it('announces each failed dial of a reconnect pass once', async () => {
    const { server, peers } = await startServer();
    const client = new Client(clientConfig(
      portOf(server),
      { enabled: true, interval: 10, maxRetries: 2 }
    ));
    const log = record(client);
    const gaveUp = new Promise<void>((resolve) => {
      client.on('error', (error) => {
        if (error.message.includes('reconnect maxRetries exceeded'))
          resolve();
      });
    });
    try {
      await next(client, 'connected');
      // Closed before the drop, so every redial is refused.
      const closed = closeServer(server, peers);
      await gaveUp;
      await closed;
      await settle();
      // The first dial, then one per retry.
      assert.deepEqual(
        log,
        ['connecting', 'connected', 'connecting', 'connecting']
      );
    } finally {
      await client.destroy();
    }
  });

  it('delivers a dial failure to error listeners and still fails the command',
    async () => {
      const client = new Client(clientConfig(await refusedPort()));
      const errors: Error[] = [];
      client.on('error', (error) => errors.push(error));
      try {
        let commandError: unknown;
        await assert.rejects(client.system.ping(), (error) => {
          commandError = error;
          return true;
        });
        assert.equal((commandError as { code?: string }).code, 'ECONNREFUSED');
        assert.ok(errors.includes(commandError as Error),
          'the listener did not see the error the command failed with');
      } finally {
        await client.destroy();
      }
    }
  );

  it('does not throw when nobody listens for error', async () => {
    const uncaught: unknown[] = [];
    const onUncaught = (error: unknown) => uncaught.push(error);
    process.on('uncaughtException', onUncaught);
    const client = new Client(clientConfig(await refusedPort()));
    try {
      await assert.rejects(client.system.ping(), { code: 'ECONNREFUSED' });
      await settle(50);
      assert.deepEqual(uncaught, []);
    } finally {
      process.removeListener('uncaughtException', onUncaught);
      await client.destroy();
    }
  });

  it('stops delivering errors once the once listener has run', async () => {
    const uncaught: unknown[] = [];
    const onUncaught = (error: unknown) => uncaught.push(error);
    process.on('uncaughtException', onUncaught);
    const client = new Client(clientConfig(
      await refusedPort(),
      { enabled: true, interval: 10, maxRetries: 2 }
    ));
    let errorCalls = 0;
    client.once('error', () => { errorCalls += 1; });
    // The first dial and both retries fail, each with an error of its own.
    const dialedThrice = new Promise<void>((resolve) => {
      let dials = 0;
      client.on('connecting', () => {
        dials += 1;
        if (dials === 3)
          resolve();
      });
    });
    try {
      await dialedThrice;
      await settle(50);
      assert.equal(errorCalls, 1);
      assert.deepEqual(uncaught, []);
    } finally {
      process.removeListener('uncaughtException', onUncaught);
      await client.destroy();
    }
  });

  it('adds no internal listeners when it reconnects', async () => {
    const { server, peers } = await startServer();
    const client = new Client(clientConfig(portOf(server), FAST_RECONNECT));
    const stream = client['events'];
    const connection = stream['connection'];
    const counts = () => [
      ...['connecting', 'connect', 'disconnected', 'end', 'error', 'eviction']
        .map((event) => connection.listenerCount(event)),
      ...['connecting', 'connected', 'end', 'error']
        .map((event) => stream.listenerCount(event))
    ];
    try {
      await next(client, 'connected');
      const before = counts();
      for (let drop = 0; drop < 3; drop += 1) {
        const reconnected = next(client, 'connected');
        peers[peers.length - 1].destroy();
        await reconnected;
      }
      assert.deepEqual(counts(), before);
    } finally {
      await client.destroy();
      await closeServer(server, peers);
    }
  });

  after(() => clearInterval(keepAlive));
});
