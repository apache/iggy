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
import { spawnSync } from 'node:child_process';
import { once, type EventEmitter } from 'node:events';
import { createServer, Socket, type AddressInfo, type Server } from 'node:net';
import { setImmediate as nextTick } from 'node:timers/promises';
import { describe, it } from 'node:test';
import { Client, SimpleClient, SingleClient } from './client.js';
import { CommandResponseStream, getRawClient, NotConnectedError } from './client.socket.js';
import type { ClientConfig } from './client.type.js';
import { ResponseFrameDecoder } from './client.frame.js';
import { ResponseError } from '../wire/error.utils.js';
import { COMMAND_CODE } from '../wire/command.code.js';
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

  it('completes facade destruction after the raw client already emitted end', async () => {
    const server = await startServer();
    const client = new Client(clientConfig(server.server));
    const raw = await client.clientProvider();
    try {
      await client.system.ping();
      const ended = new Promise<void>((resolve) => raw.once('end', () => resolve()));
      raw.destroy();
      await ended;
      let finished = false;
      const destroyed = client.destroy();
      void destroyed.then(() => { finished = true; });
      await nextTick();
      assert.ok(finished, 'facade must observe the completed raw shutdown');
      assert.equal(client.destroy(), destroyed);
    } finally {
      raw.destroy();
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
      const rejected = assert.rejects(ping, { message: 'client destroyed' });
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

  it('accepts custom raw clients whose synchronous destroy returns a socket', async () => {
    const socket = new Socket();
    const client = new SimpleClient({
      sendCommand: async () => { throw new Error('No command expected'); },
      isAuthenticated: false,
      authenticate: async () => false,
      destroy: () => socket.destroy(),
      on: () => undefined,
      once: () => undefined,
      getReadStream: () => socket
    });
    await client.destroy();
    assert.ok(socket.destroyed);
  });

  it('keeps raw destroy synchronous and reports socket closure through end', async () => {
    const server = await startServer();
    const raw = getRawClient(clientConfig(server.server));
    const client = new SimpleClient(raw);
    try {
      await client.system.ping();
      const socket = raw.getReadStream() as Socket;
      let closed = false;
      socket.once('close', () => { closed = true; });
      const ended = new Promise<void>((resolve) => raw.once('end', () => {
        assert.ok(closed);
        resolve();
      }));
      const result: void = raw.destroy();
      assert.equal(result, undefined);
      assert.ok(socket.destroyed);
      assert.equal(raw.destroy(), undefined);
      await ended;
    } finally {
      await raw.destroy();
      await server.close();
    }
  });

  it('does not turn an end listener exception into an unhandled rejection', () => {
    const result = spawnSync(process.execPath, [
      '--import', '@swc-node/register/esm-register', '--input-type=module', '--eval', `
        import { CommandResponseStream } from ${JSON.stringify(new URL('./client.socket.ts', import.meta.url).href)};
        process.on('unhandledRejection', () => process.exit(2));
        process.once('uncaughtException', (error, origin) => {
          process.exit(error.message === 'listener failed' && origin === 'uncaughtException' ? 0 : 3);
        });
        const client = new CommandResponseStream('iggy://iggy:iggy@127.0.0.1:1');
        client.on('end', () => { throw new Error('listener failed'); });
        client.destroy();
        setTimeout(() => process.exit(4), 2000);
      `
    ], { encoding: 'utf8', timeout: 10_000 });
    assert.equal(result.status, 0, result.stderr || String(result.error));
  });

  it('announces one connecting phase across retry bursts until a link succeeds', async () => {
    const server = await startServer();
    const config = clientConfig(server.server);
    await server.close();
    config.reconnect = { enabled: true, interval: 1, maxRetries: 2 };
    const client = new CommandResponseStream(config);
    let connecting = 0;
    let failures = 0;
    client.on('connecting', () => { connecting += 1; });
    client.on('connectionError', () => { failures += 1; });
    const failThreeDials = async () => {
      const target = failures + 3;
      await assert.rejects(client.sendCommand(COMMAND_CODE.Ping, Buffer.alloc(0), {
        deadline: Date.now() + 5000
      }), NotConnectedError);
      assert.equal(failures, target, 'bootstrap must use its configured retry budget');
    };
    try {
      await failThreeDials();
      assert.equal(connecting, 1);
      await failThreeDials();
      assert.equal(connecting, 1);
      server.server.listen(config.options.port, '127.0.0.1');
      await once(server.server, 'listening');
      await client.sendCommand(COMMAND_CODE.Ping, Buffer.alloc(0));
      assert.equal(connecting, 1);
      const socket = client.getReadStream() as Socket;
      const closed = once(socket, 'close');
      socket.destroy();
      await closed;
      await client.sendCommand(COMMAND_CODE.Ping, Buffer.alloc(0));
      assert.equal(connecting, 2);
    } finally {
      const ended = once(client, 'end');
      client.destroy();
      await ended;
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

  it('uses the public client as the listener receiver and allows self-removal', async () => {
    const client = new Client('iggy://iggy:iggy@127.0.0.1:1');
    let calls = 0;
    function listener(this: Client) {
      assert.equal(this, client);
      calls += 1;
      this.off('error', listener);
    }
    try {
      client.on('error', listener);
      client['events'].emit('connectionError', new Error('first'));
      client['events'].emit('connectionError', new Error('second'));
      assert.equal(calls, 1);
      client.once('error', listener);
      client['events'].emit('connectionError', new Error('third'));
      client['events'].emit('connectionError', new Error('fourth'));
      assert.equal(calls, 2);
      client.on('error', listener).on('error', listener);
      client.off('error', listener);
      client['events'].emit('connectionError', new Error('fifth'));
      assert.equal(calls, 3, 'off removes only the latest duplicate registration');
      assert.equal(client['events'].listenerCount('connectionError'), 0);
    } finally {
      await client.destroy();
    }
  });

  it('keeps connected listener exceptions outside transport retry handling', () => {
    const result = spawnSync(process.execPath, [
      '--import', '@swc-node/register/esm-register', '--input-type=module', '--eval', `
        import { createServer } from 'node:net';
        import { once } from 'node:events';
        import { CommandResponseStream } from ${JSON.stringify(new URL('./client.socket.ts', import.meta.url).href)};
        const server = createServer();
        server.listen(0, '127.0.0.1');
        await once(server, 'listening');
        const client = new CommandResponseStream({
          transport: 'TCP',
          options: { host: '127.0.0.1', port: server.address().port },
          reconnect: { enabled: false, interval: 0, maxRetries: 0 },
          heartbeatInterval: 0
        });
        let calls = 0;
        process.once('uncaughtException', (error, origin) => {
          const socket = client.getReadStream();
          process.exit(error.message === 'listener failed' && origin === 'uncaughtException'
            && calls === 1 && socket && !socket.destroyed ? 0 : 3);
        });
        process.on('unhandledRejection', () => process.exit(4));
        client.on('connected', () => { calls += 1; throw new Error('listener failed'); });
        client.sendCommand(${COMMAND_CODE.Ping}, Buffer.alloc(0)).catch(() => process.exit(2));
        setTimeout(() => process.exit(5), 2000);
      `
    ], { encoding: 'utf8', timeout: 10_000 });
    assert.equal(result.status, 0, result.stderr || String(result.error));
  });

  it('supports JavaScript node:events.once and removes its temporary listeners', async () => {
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
        assert.equal(dials.length, 1);
        assert.equal(errors.length, listen ? 1 : 0);
      } finally {
        await client.destroy();
      }
    });
  }
});
