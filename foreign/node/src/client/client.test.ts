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
import { createServer, type AddressInfo, type Server, type Socket } from 'node:net';
import { setImmediate as nextTick } from 'node:timers/promises';
import { describe, it } from 'node:test';
import { Client, SimpleClient, SingleClient } from './client.js';
import { CommandResponseStream, NotConnectedError } from './client.socket.js';
import type { ClientConfig } from './client.type.js';
import { ResponseFrameDecoder } from './client.frame.js';
import { ResponseError } from '../wire/error.utils.js';
import { Command, HEADER_SIZE, REPLY_OFFSET, REQUEST_OFFSET } from '../wire/vsr/header.js';

const startServer = async (reply = true) => {
  const peers = new Set<Socket>();
  let received!: () => void;
  const requestReceived = new Promise<void>((resolve) => { received = resolve; });
  const server = createServer((socket) => {
    peers.add(socket);
    socket.on('error', () => undefined);
    socket.on('close', () => peers.delete(socket));
    const decoder = new ResponseFrameDecoder(64 * 1024 * 1024);
    socket.on('data', (data) => {
      for (const frame of decoder.push(Buffer.isBuffer(data) ? data : Buffer.from(data))) {
        received();
        if (!reply) continue;
        const response = Buffer.alloc(HEADER_SIZE);
        response.writeUInt32LE(response.length, REPLY_OFFSET.size);
        response[REPLY_OFFSET.command] = Command.Reply;
        response[REPLY_OFFSET.operation] = frame[REQUEST_OFFSET.operation];
        response.writeBigUInt64LE(frame.readBigUInt64LE(REQUEST_OFFSET.request), REPLY_OFFSET.request);
        socket.write(response);
      }
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return {
    server, peers, requestReceived,
    close: async () => {
      for (const peer of peers) peer.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  };
};

const clientConfig = (server: Server): ClientConfig => ({
  transport: 'TCP',
  options: { host: '127.0.0.1', port: (server.address() as AddressInfo).port },
  credentials: { username: 'iggy', password: 'iggy' },
  reconnect: { enabled: false, interval: 0, maxRetries: 0 },
  heartbeatInterval: 0
});

const record = (client: Client): string[] => {
  const events: string[] = [];
  for (const event of ['connecting', 'connected', 'error', 'end'] as const)
    client.on(event, () => events.push(event));
  return events;
};

describe('Client lifecycle events', () => {
  for (const ClientClass of [Client, SingleClient]) {
    it(`${ClientClass.name} reports the first dial lazily with typed chainable listeners`, async () => {
      const server = await startServer();
      const client = new ClientClass(clientConfig(server.server));
      const events = record(client);
      const noop = () => undefined;
      try {
        assert.equal(client.on('connected', noop), client);
        assert.equal(client.once('connected', noop), client);
        assert.equal(client.off('connected', noop), client);
        assert.equal(client.removeListener('connected', noop), client);
        assert.equal('_pool' in client, false);
        assert.deepEqual(events, []);
        const ping = client.system.ping();
        assert.deepEqual(events, []);
        await ping;
        assert.deepEqual(events, ['connecting', 'connected']);
        await client.destroy();
        assert.deepEqual(events, ['connecting', 'connected', 'end']);
      } finally {
        await client.destroy();
        await server.close();
      }
    });
  }

  it('emits end once after socket closure, including repeated destroy calls', async () => {
    const server = await startServer();
    const client = new Client(clientConfig(server.server));
    const events = record(client);
    try {
      await client.system.ping();
      const socket = client['events'].getReadStream() as Socket;
      let closed = false;
      socket.once('close', () => { closed = true; });
      client.on('end', () => assert.ok(closed));
      const ended = client.destroy();
      assert.equal(client.destroy(), ended);
      await ended;
      await client.destroy();
      await nextTick();
      assert.deepEqual(events, ['connecting', 'connected', 'end']);
    } finally {
      await client.destroy();
      await server.close();
    }
  });

  it('cancels an in-flight request immediately before waiting for socket closure', async () => {
    const server = await startServer(false);
    const client = new Client(clientConfig(server.server));
    const events = record(client);
    try {
      const ping = client.system.ping();
      const rejected = assert.rejects(ping,
        (error: unknown) => error instanceof ResponseError && error.errorCode === 58);
      await server.requestReceived;
      const socket = client['events'].getReadStream() as Socket;
      const ended = client.destroy();
      assert.ok(socket.destroyed);
      await rejected;
      await ended;
      assert.deepEqual(events, ['connecting', 'connected', 'end']);
    } finally {
      await client.destroy();
      await server.close();
    }
  });

  it('waits for a socket that was already closing when destroy was called', async () => {
    const server = await startServer();
    const client = new Client(clientConfig(server.server));
    try {
      await client.system.ping();
      const socket = client['events'].getReadStream() as Socket;
      let closed = false;
      socket.once('close', () => { closed = true; });
      client.once('end', () => assert.ok(closed));
      socket.destroy();
      await client.destroy();
      assert.ok(closed);
    } finally {
      await client.destroy();
      await server.close();
    }
  });

  it('does not dial a queued request after destruction', async () => {
    const server = await startServer();
    const client = new Client(clientConfig(server.server));
    const events = record(client);
    try {
      const ping = client.system.ping();
      const rejected = assert.rejects(ping,
        (error: unknown) => error instanceof ResponseError && error.errorCode === 58);
      await client.destroy();
      await rejected;
      await nextTick();
      assert.equal(server.peers.size, 0);
      assert.deepEqual(events, ['end']);
    } finally {
      await client.destroy();
      await server.close();
    }
  });

  it('awaits an asynchronous raw client destroy through SimpleClient', async () => {
    const server = await startServer();
    const raw = new CommandResponseStream(clientConfig(server.server));
    const destroy = raw.destroy.bind(raw);
    let release!: () => void;
    const pending = new Promise<void>((resolve) => { release = resolve; });
    raw.destroy = async () => { await pending; await destroy(); };
    const client = new SimpleClient(raw);
    let finished = false;
    try {
      const ended = client.destroy().then(() => { finished = true; });
      await nextTick();
      assert.equal(finished, false);
      release();
      await ended;
      assert.equal(finished, true);
    } finally {
      release();
      await destroy();
      await server.close();
    }
  });

  it('keeps listeners across replacement links without reattaching them', async () => {
    const server = await startServer();
    const client = new Client(clientConfig(server.server));
    const calls = { kept: 0, once: 0, removed: 0 };
    const removed = () => { calls.removed += 1; };
    client.on('connected', () => { calls.kept += 1; });
    client.once('connected', () => { calls.once += 1; });
    client.on('connected', removed);
    try {
      await client.system.ping();
      client.off('connected', removed);
      client.once('connected', removed);
      client.removeListener('connected', removed);
      const counts = Object.fromEntries(
        ['connecting', 'connected', 'connectionError', 'end'].map((name) => [name, client['events'].listenerCount(name)])
      );
      for (let replacement = 0; replacement < 3; replacement += 1) {
        const socket = client['events'].getReadStream() as Socket;
        const closed = once(socket, 'close');
        for (const peer of server.peers) peer.destroy();
        await closed;
        await client.system.ping();
      }
      assert.deepEqual(calls, { kept: 4, once: 1, removed: 1 });
      for (const [name, count] of Object.entries(counts))
        assert.equal(client['events'].listenerCount(name), count);
    } finally {
      await client.destroy();
      await server.close();
    }
  });

  it('supports node:events.once and removes its temporary listeners', async () => {
    const server = await startServer();
    const client = new Client(clientConfig(server.server));
    try {
      const connected = once(client as unknown as EventEmitter, 'connected');
      await client.system.ping();
      await connected;
      assert.equal(client['events'].listenerCount('connected'), 0);
      assert.equal(client['events'].listenerCount('connectionError'), 0);
    } finally {
      await client.destroy();
      await server.close();
    }
  });

  for (const listen of [true, false]) {
    it(`preserves a refused command and tolerates ${listen ? 'a one-shot' : 'no'} error listener`, async () => {
      const server = await startServer();
      const config = clientConfig(server.server);
      await server.close();
      config.reconnect = { enabled: true, interval: 1, maxRetries: 2 };
      const client = new Client(config);
      const errors: Error[] = [];
      const dials: string[] = [];
      client.on('connecting', () => dials.push('connecting'));
      if (listen) client.once('error', (error) => errors.push(error));
      try {
        await assert.rejects(client.session.login({ username: 'iggy', password: 'iggy' }), (error: unknown) => {
          assert.ok(error instanceof NotConnectedError);
          assert.equal((error.cause as NodeJS.ErrnoException).code, 'ECONNREFUSED');
          return true;
        });
        assert.equal(dials.length, 3);
        assert.equal(errors.length, listen ? 1 : 0);
      } finally {
        await client.destroy();
      }
    });
  }
});
