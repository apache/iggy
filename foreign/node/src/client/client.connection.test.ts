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
import { once } from 'node:events';
import { readFileSync } from 'node:fs';
import {
  createServer,
  type AddressInfo,
  type Server,
  type Socket
} from 'node:net';
import {
  createServer as createTlsServer,
  type TLSSocket
} from 'node:tls';
import { describe, it, before, after } from 'node:test';
import { ProtocolFrameError } from './client.frame.js';
import { IggyConnection } from './client.connection.js';
import type { ClientConfig } from './client.type.js';
import { Command, HEADER_SIZE, REPLY_OFFSET } from '../wire/vsr/header.js';

const FRAME_LIMIT = 2 * HEADER_SIZE;

const TLS_CERTIFICATE = readFileSync(
  new URL('../../../../core/certs/iggy_cert.pem', import.meta.url)
);
const TLS_KEY = readFileSync(
  new URL('../../../../core/certs/iggy_key.pem', import.meta.url)
);
const TLS_CA_CERTIFICATE = readFileSync(
  new URL('../../../../core/certs/iggy_ca_cert.pem', import.meta.url)
);

const startTlsServer = async (): Promise<Server> => {
  const server = createTlsServer({
    cert: TLS_CERTIFICATE,
    key: TLS_KEY
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server;
};

const startServer = async (): Promise<Server> => {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server;
};

const connectionConfig = (server: Server): ClientConfig => ({
  transport: 'TCP',
  options: {
    host: '127.0.0.1',
    port: (server.address() as AddressInfo).port
  },
  credentials: { username: 'iggy', password: 'iggy' },
  reconnect: { enabled: false, interval: 0, maxRetries: 0 },
  maxResponseFrameSize: FRAME_LIMIT
});

const replyFrame = (body: Buffer): Buffer => {
  const frame = Buffer.alloc(HEADER_SIZE + body.length);
  frame.writeUInt32LE(frame.length, REPLY_OFFSET.size);
  frame.writeUInt8(Command.Reply, REPLY_OFFSET.command);
  body.copy(frame, HEADER_SIZE);
  return frame;
};

const closeConnection = async (
  connection: IggyConnection,
  server: Server
): Promise<void> => {
  connection._destroy();
  if (!connection.socket.destroyed)
    await once(connection.socket, 'close');
  await new Promise<void>((resolve) => server.close(() => resolve()));
};

let keepAlive: NodeJS.Timeout;

describe('IggyConnection', () => {

  // Note:
  // before node v24 Timeout.unref() would let eventloop exit before test end
  // (tested against 22.x 23.x -> fail vs 24.x 26.x -> pass)
  // this timeout prevent eventloop exit before this test end
  before(() => { keepAlive = setInterval(() => {}, 10000) });

  it('recognizes a connection established before connect is called',
    async () => {
      const server = await startServer();
      const accepted = once(server, 'connection');
      const connection = new IggyConnection(connectionConfig(server));
      try {
        await accepted;
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(await connection.connect(), connection);
        assert.equal(connection.connected, true);
      } finally {
        await closeConnection(connection, server);
      }
    }
  );

  it('shares connection attempts, recognizes endpoints, and writes frames',
    async () => {
      const server = await startServer();
      const received = new Promise<Buffer>((resolve) => {
        server.once('connection', (socket) => {
          socket.once('data', (data) => resolve(Buffer.from(data)));
        });
      });
      const connection = new IggyConnection(connectionConfig(server));
      try {
        const first = connection.connect();
        assert.equal(connection.connect(), first);
        await first;
        assert.equal(await connection.connect(), connection);
        assert.equal(
          connection.isConnectedTo(
            'localhost',
            (server.address() as AddressInfo).port
          ),
          true
        );

        connection.config.options = {
          ...connection.config.options,
          host: 'broker.example'
        };
        assert.equal(
          connection.isConnectedTo(
            'broker.example',
            (server.address() as AddressInfo).port
          ),
          true
        );

        const frame = replyFrame(Buffer.from('payload'));
        connection.writeFrame(frame);
        assert.deepEqual(await received, frame);
      } finally {
        await closeConnection(connection, server);
      }
    }
  );

  it('emits complete buffered responses and rejects malformed frames',
    async () => {
      const server = await startServer();
      const connection = new IggyConnection(connectionConfig(server));
      try {
        await connection.connect();
        const frame = replyFrame(Buffer.from('response'));
        const response = once(connection, 'response');
        connection._onData(frame.subarray(0, 6));
        connection._onData(frame.subarray(6));
        assert.deepEqual((await response)[0], frame);

        const malformed = replyFrame(Buffer.alloc(0));
        malformed.writeUInt32LE(FRAME_LIMIT + 1, REPLY_OFFSET.size);
        const error = once(connection, 'error');
        connection._onData(malformed);
        assert.ok((await error)[0] instanceof ProtocolFrameError);
      } finally {
        await closeConnection(connection, server);
      }
    }
  );

  it('suppresses expected reset errors during intentional shutdown',
    async () => {
      const server = await startServer();
      const connection = new IggyConnection(connectionConfig(server));
      try {
        await connection.connect();
        let emitted = false;
        connection.on('error', () => { emitted = true; });
        connection.ending = true;
        connection.socket.emit(
          'error',
          Object.assign(new Error('reset'), { code: 'ECONNRESET' })
        );
        assert.equal(emitted, false);
      } finally {
        await closeConnection(connection, server);
      }
    }
  );

  it('does not reconnect or reopen after destruction', async () => {
    const server = await startServer();
    const config = {
      ...connectionConfig(server),
      reconnect: { enabled: true, interval: 20, maxRetries: 1 }
    };
    let connections = 0;
    let serverSocket: Socket | undefined;
    server.on('connection', (socket) => {
      connections += 1;
      serverSocket = socket;
    });
    const connection = new IggyConnection(config);
    try {
      await connection.connect();
      const disconnected = once(connection, 'disconnected');
      serverSocket?.destroy();
      await disconnected;
      connection._destroy();

      await assert.rejects(
        () => connection.connect(),
        /connection is closed/
      );
      await new Promise<void>((resolve) => setTimeout(resolve, 40));
      assert.equal(connections, 1);
    } finally {
      await closeConnection(connection, server);
    }
  });

  it('shares reconnect backoff with callers',
    async () => {
      const server = await startServer();
      const config = {
        ...connectionConfig(server),
        reconnect: { enabled: true, interval: 10, maxRetries: 2 }
      };
      let serverSocket: Socket | undefined;
      let connections = 0;
      server.on('connection', (socket) => {
        connections += 1;
        serverSocket = socket;
      });
      const connection = new IggyConnection(config);
      connection.on('error', () => undefined);
      try {
        await connection.connect();
        const oldSocket = connection.socket;
        connection._onData(Buffer.alloc(3));
        const disconnected = once(connection, 'disconnected');
        serverSocket?.destroy();
        await disconnected;

        const first = connection.connect();
        assert.equal(connection.connect(), first);
        assert.equal(await first, connection);
        assert.equal(connection.connected, true);
        assert.equal(connections, 2);

        const frame = replyFrame(Buffer.from('fresh'));
        const response = once(connection, 'response');
        oldSocket.emit('data', Buffer.alloc(8));
        connection._onData(frame);
        assert.deepEqual((await response)[0], frame);
      } finally {
        await closeConnection(connection, server);
      }
    }
  );

  it('keeps the seed endpoint when a redirect fails', async () => {
    const server = await startServer();
    const unavailable = await startServer();
    const unavailablePort = (unavailable.address() as AddressInfo).port;
    await new Promise<void>((resolve) => unavailable.close(() => resolve()));
    const connection = new IggyConnection({
      ...connectionConfig(server),
      reconnect: { enabled: true, interval: 10, maxRetries: 1 }
    });
    connection.on('error', () => undefined);
    try {
      await connection.connect();
      const seedPort = (server.address() as AddressInfo).port;
      const reconnected = new Promise<void>((resolve) => {
        connection.once('connect', () => resolve());
      });
      await assert.rejects(
        () => connection.redirect('127.0.0.1', unavailablePort)
      );
      await reconnected;
      assert.equal(connection.config.options.host, '127.0.0.1');
      assert.equal(connection.config.options.port, seedPort);
      assert.equal(connection.connected, true);
      assert.equal(
        connection.isConnectedTo('127.0.0.1', unavailablePort),
        false
      );
    } finally {
      await closeConnection(connection, server);
    }
  });

  it('stops a pending reconnect after a redirect replaces the socket',
    async () => {
      const seed = await startServer();
      const target = await startServer();
      const targetPort = (target.address() as AddressInfo).port;
      let seedConnections = 0;
      let seedSocket: Socket | undefined;
      seed.on('connection', (socket) => {
        seedConnections += 1;
        seedSocket = socket;
      });
      let targetConnections = 0;
      target.on('connection', () => {
        targetConnections += 1;
      });
      const connection = new IggyConnection({
        ...connectionConfig(seed),
        reconnect: { enabled: true, interval: 50, maxRetries: 3 }
      });
      connection.on('error', () => undefined);
      try {
        await connection.connect();
        const disconnected = once(connection, 'disconnected');
        seedSocket?.destroy();
        await disconnected;
        await connection.redirect('127.0.0.1', targetPort);
        assert.equal(connection.connected, true);
        await new Promise<void>((resolve) => setTimeout(resolve, 150));
        assert.equal(connection.connected, true);
        assert.equal(connection.isConnectedTo('127.0.0.1', targetPort), true);
        assert.equal(seedConnections, 1);
        assert.equal(targetConnections, 1);
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => seed.close(() => resolve()));
        await new Promise<void>((resolve) => target.close(() => resolve()));
      }
    }
  );

  it('exhausts consecutive failed retries without unhandled rejections',
    async () => {
      const server = await startServer();
      let serverSocket: Socket | undefined;
      server.on('connection', (socket) => {
        serverSocket = socket;
      });
      const rejections: unknown[] = [];
      const onUnhandled = (reason: unknown) => rejections.push(reason);
      process.on('unhandledRejection', onUnhandled);
      const connection = new IggyConnection({
        ...connectionConfig(server),
        reconnect: { enabled: true, interval: 10, maxRetries: 3 }
      });
      try {
        await connection.connect();
        const exhausted = new Promise<Error>((resolve) => {
          const onError = (error: Error) => {
            if (!error.message.includes('reconnect maxRetries exceeded'))
              return;
            connection.removeListener('error', onError);
            resolve(error);
          };
          connection.on('error', onError);
        });
        const closed = new Promise<void>(
          (resolve) => server.close(() => resolve())
        );
        serverSocket?.destroy();
        const retryStartedAt = Date.now();
        await closed;
        const error = await exhausted;
        // Three retries at a 10 ms interval must spend at least 30 ms in
        // backoff; a broken wait would redial back to back.
        assert.ok(
          Date.now() - retryStartedAt >= 30,
          'reconnect backoff did not elapse between retries'
        );
        assert.match(error.message, /reconnect maxRetries exceeded/);
        await new Promise<void>((resolve) => setTimeout(resolve, 20));
        assert.deepEqual(rejections, []);
      } finally {
        process.removeListener('unhandledRejection', onUnhandled);
        connection._destroy();
      }
    }
  );

  it('falls back to the seed endpoint when a redirected leader dies',
    async () => {
      const seed = await startServer();
      const seedPort = (seed.address() as AddressInfo).port;
      const target = await startServer();
      const targetPort = (target.address() as AddressInfo).port;
      let targetSocket: Socket | undefined;
      target.on('connection', (socket) => {
        targetSocket = socket;
      });
      const connection = new IggyConnection({
        ...connectionConfig(seed),
        reconnect: { enabled: true, interval: 10, maxRetries: 4 }
      });
      connection.on('error', () => undefined);
      try {
        await connection.connect();
        await connection.redirect('127.0.0.1', targetPort);
        assert.equal(connection.config.options.port, targetPort);

        const reconnected = new Promise<void>((resolve) => {
          connection.once('connect', () => resolve());
        });
        const targetClosed = new Promise<void>(
          (resolve) => target.close(() => resolve())
        );
        targetSocket?.destroy();
        await targetClosed;
        await reconnected;
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(connection.connected, true);
        assert.equal(connection.isConnectedTo('127.0.0.1', seedPort), true);
        assert.equal(connection.config.options.port, seedPort);
      } finally {
        await closeConnection(connection, seed);
      }
    }
  );

  it('rotates a redial through the roster it learned while connected',
    async () => {
      const seed = await startServer();
      const seedPort = (seed.address() as AddressInfo).port;
      const connection = new IggyConnection(connectionConfig(seed));
      connection.on('error', () => undefined);
      try {
        connection.rememberRoster([
          { host: '127.0.0.1', port: seedPort },
          { host: '127.0.0.1', port: seedPort + 1 },
          { host: '127.0.0.1', port: seedPort + 2 }
        ]);
        // The endpoint the client is on leads, the roster follows, and the
        // roster's copy of that endpoint does not earn a second attempt.
        assert.deepEqual(
          connection._redialCandidates().map((options) => options.port),
          [seedPort, seedPort + 1, seedPort + 2]
        );
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => seed.close(() => resolve()));
      }
    }
  );

  it('dials the endpoint it is on, then the seed, then the roster',
    async () => {
      const seed = await startServer();
      const seedPort = (seed.address() as AddressInfo).port;
      const connection = new IggyConnection(connectionConfig(seed));
      connection.on('error', () => undefined);
      try {
        // A redirect moves the client off its seed; the seed is still the one
        // endpoint the caller vouched for, so it comes before a roster the
        // cluster may have reshaped since.
        connection.config.options = {
          ...connection.config.options,
          port: seedPort + 9
        };
        connection.rememberRoster([{ host: '127.0.0.1', port: seedPort + 5 }]);

        assert.deepEqual(
          connection._redialCandidates().map((options) => options.port),
          [seedPort + 9, seedPort, seedPort + 5]
        );
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => seed.close(() => resolve()));
      }
    }
  );

  it('counts endpoints that only differ in spelling once',
    async () => {
      const seed = await startServer();
      const seedPort = (seed.address() as AddressInfo).port;
      const connection = new IggyConnection(connectionConfig(seed));
      connection.on('error', () => undefined);
      try {
        // The loopback aliases and an IPv4-mapped address all name the endpoint
        // the client is already on, so none of them earns a dial of its own.
        connection.rememberRoster([
          { host: 'localhost', port: seedPort },
          { host: '::1', port: seedPort },
          { host: '::ffff:127.0.0.1', port: seedPort },
          { host: '127.0.0.1', port: seedPort + 1 }
        ]);

        assert.deepEqual(
          connection._redialCandidates().map((options) => options.port),
          [seedPort, seedPort + 1]
        );
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => seed.close(() => resolve()));
      }
    }
  );

  it('does not redial at all when reconnection is disabled',
    async () => {
      // `enabled: false` is what a caller says to opt out. The retry budget is
      // whatever the defaults hold, so a loop that reads it without checking
      // this flag would run every one of those passes - and with the backoff
      // gated on the same flag, back to back.
      //
      // The endpoint accepts and hangs up, so the drop that would start a
      // redial happens and every dial of it is counted.
      const hangup = await startServer();
      const hangupPort = (hangup.address() as AddressInfo).port;
      let accepted = 0;
      hangup.on('connection', (socket) => {
        accepted += 1;
        socket.destroy();
      });

      const connection = new IggyConnection({
        transport: 'TCP',
        options: { host: '127.0.0.1', port: hangupPort },
        credentials: { username: 'iggy', password: 'iggy' },
        reconnect: { enabled: false, interval: 10, maxRetries: 12 },
        maxResponseFrameSize: FRAME_LIMIT
      });
      connection.on('error', () => undefined);
      try {
        await connection.connect().catch(() => undefined);
        await new Promise<void>((resolve) => setTimeout(resolve, 300));

        assert.equal(accepted, 1,
          'a client that turned reconnection off redialed anyway'
        );
        assert.equal(connection.connected, false);
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => hangup.close(() => resolve()));
      }
    }
  );

  it('sweeps the endpoints it knows once when reconnection is disabled',
    async () => {
      // Opting out of retries is not opting out of the endpoints: with more
      // than one known, they get exactly one pass and no backoff, as in the
      // other SDKs.
      const dead = await startServer();
      const deadPort = (dead.address() as AddressInfo).port;
      await new Promise<void>((resolve) => dead.close(() => resolve()));
      const live = await startServer();
      const livePort = (live.address() as AddressInfo).port;
      let accepted = 0;
      live.on('connection', () => { accepted += 1; });

      const connection = new IggyConnection({
        transport: 'TCP',
        options: { host: '127.0.0.1', port: deadPort },
        credentials: { username: 'iggy', password: 'iggy' },
        reconnect: { enabled: false, interval: 10, maxRetries: 12 },
        maxResponseFrameSize: FRAME_LIMIT
      });
      connection.on('error', () => undefined);
      try {
        connection.rememberRoster([{ host: '127.0.0.1', port: livePort }]);
        await connection.connect().catch(() => undefined);
        await new Promise<void>((resolve) => setTimeout(resolve, 200));

        assert.equal(accepted, 1,
          'the known endpoints got either no pass or more than one'
        );
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => live.close(() => resolve()));
      }
    }
  );

  it('makes one pass when every endpoint is down and reconnection is disabled',
    async () => {
      // One pass, not the whole retry budget: with the budget read but the
      // flag ignored, a client that opted out of retries dials every endpoint
      // once per pass for all of them -- and with the backoff gated on the same
      // flag, back to back.
      //
      // Plain TCP behind a TLS client, closed at once: the dial fails, so the
      // pass moves on, and every dial is counted where it lands.
      const first = await startServer();
      const firstPort = (first.address() as AddressInfo).port;
      let firstDials = 0;
      first.on('connection', (socket) => {
        firstDials += 1;
        socket.destroy();
      });
      const second = await startServer();
      const secondPort = (second.address() as AddressInfo).port;
      let secondDials = 0;
      second.on('connection', (socket) => {
        secondDials += 1;
        socket.destroy();
      });

      const connection = new IggyConnection({
        transport: 'TLS',
        options: {
          host: '127.0.0.1',
          port: firstPort,
          rejectUnauthorized: false
        },
        credentials: { username: 'iggy', password: 'iggy' },
        reconnect: { enabled: false, interval: 10, maxRetries: 12 },
        maxResponseFrameSize: FRAME_LIMIT
      });
      connection.on('error', () => undefined);
      try {
        connection.rememberRoster([{ host: '127.0.0.1', port: secondPort }]);
        await connection.connect().catch(() => undefined);
        await new Promise<void>((resolve) => setTimeout(resolve, 300));

        assert.equal(connection.connected, false);
        // The connect's own dial of the configured endpoint, then one pass over
        // both: the endpoint the client starts on is dialed twice, the one
        // behind it once.
        assert.deepEqual([firstDials, secondDials], [2, 1],
          'a client that opted out of retries swept more than once'
        );
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => first.close(() => resolve()));
        await new Promise<void>((resolve) => second.close(() => resolve()));
      }
    }
  );

  it('skips the first backoff when another endpoint is known',
    async () => {
      // The endpoint the client is on is dead and a live one sits behind it in
      // the roster: waiting out the interval before the first pass would push
      // the failover past what the caller waits for, and the node just lost may
      // be gone for good.
      const dead = await startServer();
      const deadPort = (dead.address() as AddressInfo).port;
      await new Promise<void>((resolve) => dead.close(() => resolve()));
      const live = await startServer();
      const livePort = (live.address() as AddressInfo).port;

      const interval = 3000;
      const connection = new IggyConnection({
        transport: 'TCP',
        options: { host: '127.0.0.1', port: deadPort },
        credentials: { username: 'iggy', password: 'iggy' },
        reconnect: { enabled: true, interval, maxRetries: 3 },
        maxResponseFrameSize: FRAME_LIMIT
      });
      connection.on('error', () => undefined);
      try {
        connection.rememberRoster([{ host: '127.0.0.1', port: livePort }]);
        const dialed = once(live, 'connection');
        const started = Date.now();
        void connection.connect().catch(() => undefined);
        await dialed;

        assert.ok(Date.now() - started < interval,
          'the failover waited out the backoff before its first pass'
        );
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => live.close(() => resolve()));
      }
    }
  );

  it('bounds a dial that never becomes usable when others are queued behind it',
    async () => {
      // Plain TCP behind a TLS client: the socket connects, so only a bound on
      // the handshake ends the attempt. The endpoint behind it is dead, so the
      // pass has to end on its own rather than hang on the first one.
      const silent = await startServer();
      const silentPort = (silent.address() as AddressInfo).port;
      const held: Socket[] = [];
      silent.on('connection', (socket) => { held.push(socket); });
      const dead = await startServer();
      const deadPort = (dead.address() as AddressInfo).port;
      await new Promise<void>((resolve) => dead.close(() => resolve()));

      const connection = new IggyConnection({
        transport: 'TLS',
        options: {
          host: '127.0.0.1',
          port: silentPort,
          rejectUnauthorized: false
        },
        credentials: { username: 'iggy', password: 'iggy' },
        reconnect: { enabled: true, interval: 10, maxRetries: 3 },
        maxResponseFrameSize: FRAME_LIMIT
      });
      connection.on('error', () => undefined);
      try {
        connection.rememberRoster([{ host: '127.0.0.1', port: deadPort }]);
        void connection.connect().catch(() => undefined);

        // Unbounded, the first dial never ends and this endpoint is dialed
        // exactly once, forever.
        const deadline = Date.now() + 8_000;
        while (held.length < 2 && Date.now() < deadline)
          await new Promise<void>((resolve) => setTimeout(resolve, 50));

        assert.ok(held.length >= 2,
          'a dial that never became usable held the pass'
        );
        assert.equal(connection.connected, false);
      } finally {
        connection._destroy();
        held.forEach((socket) => socket.destroy());
        await new Promise<void>((resolve) => silent.close(() => resolve()));
      }
    }
  );

  it('stops a redial pass that is destroyed part-way through',
    async () => {
      // The endpoint the client is on is dead, so every dial to it is refused
      // - and the live roster endpoint behind it is what the pass would reach
      // next, unless the destroy in between stops the pass.
      const dead = await startServer();
      const deadPort = (dead.address() as AddressInfo).port;
      await new Promise<void>((resolve) => dead.close(() => resolve()));
      const live = await startServer();
      const livePort = (live.address() as AddressInfo).port;
      let accepted = 0;
      live.on('connection', () => { accepted += 1; });

      const connection = new IggyConnection({
        transport: 'TCP',
        options: { host: '127.0.0.1', port: deadPort },
        credentials: { username: 'iggy', password: 'iggy' },
        reconnect: { enabled: true, interval: 10, maxRetries: 3 },
        maxResponseFrameSize: FRAME_LIMIT
      });
      let destroyed = false;
      let connectsAfterDestroy = 0;
      connection.on('connect', () => {
        if (destroyed)
          connectsAfterDestroy += 1;
      });
      try {
        connection.rememberRoster([{ host: '127.0.0.1', port: livePort }]);

        // The first failure is the initial connect, which is what starts the
        // redial pass; the next one is that pass's first candidate, so
        // destroying there lands between two candidates rather than before the
        // pass.
        const destroyedMidPass = new Promise<void>((resolve) => {
          let failures = 0;
          connection.on('error', () => {
            failures += 1;
            if (failures < 2 || destroyed)
              return;
            connection._destroy();
            destroyed = true;
            resolve();
          });
        });

        await connection.connect().catch(() => undefined);
        await destroyedMidPass;
        await new Promise<void>((resolve) => setTimeout(resolve, 100));

        assert.equal(accepted, 0,
          'a destroyed connection must not keep dialing the rest of the pass'
        );
        assert.equal(connectsAfterDestroy, 0,
          'a destroyed connection must not announce a connection'
        );
        assert.equal(connection.connected, false);
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => live.close(() => resolve()));
      }
    }
  );

  it('settles a dial in flight when a redirect replaces the socket',
    async () => {
      const seed = await startServer();
      const target = await startServer();
      const targetPort = (target.address() as AddressInfo).port;
      const connection = new IggyConnection(connectionConfig(seed));
      connection.on('error', () => undefined);
      try {
        const pending = connection.connect();
        const redirected = connection.redirect('127.0.0.1', targetPort);
        await assert.rejects(
          () => pending,
          /connection closed before it was established/
        );
        await redirected;
        assert.equal(connection.connected, true);
        assert.equal(connection.isConnectedTo('127.0.0.1', targetPort), true);
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => seed.close(() => resolve()));
        await new Promise<void>((resolve) => target.close(() => resolve()));
      }
    }
  );

  it('shares the redirect dial with a disconnected listener',
    async () => {
      const seed = await startServer();
      const target = await startServer();
      const targetPort = (target.address() as AddressInfo).port;
      let seedConnections = 0;
      let targetConnections = 0;
      seed.on('connection', () => {
        seedConnections += 1;
      });
      target.on('connection', () => {
        targetConnections += 1;
      });
      const connection = new IggyConnection(connectionConfig(seed));
      connection.on('error', () => undefined);
      let listenerConnection: Promise<IggyConnection> | undefined;
      try {
        await connection.connect();
        connection.once('disconnected', () => {
          listenerConnection = connection.connect();
        });

        await connection.redirect('127.0.0.1', targetPort);
        assert.ok(listenerConnection);
        assert.equal(await listenerConnection, connection);
        assert.equal(connection.connected, true);
        assert.equal(connection.isConnectedTo('127.0.0.1', targetPort), true);
        assert.equal(seedConnections, 1);
        assert.equal(targetConnections, 1);
      } finally {
        connection._destroy();
        await new Promise<void>((resolve) => seed.close(() => resolve()));
        await new Promise<void>((resolve) => target.close(() => resolve()));
      }
    }
  );

  it('rejects an unreadable tls_ca_file with a TypeError at socket creation',
    () => {
      assert.throws(
        () =>
          new IggyConnection({
            transport: 'TLS',
            options: {
              host: '127.0.0.1',
              port: 8090,
              caFile: '/does/not/exist.pem'
            },
            credentials: { username: 'iggy', password: 'iggy' },
            reconnect: { enabled: false, interval: 0, maxRetries: 0 }
          }),
        /cannot read tls_ca_file/
      );
    }
  );

  it('sends a DNS host as the SNI server name when none is set',
    async () => {
      const server = await startTlsServer();
      const secureConnection =
        once(server, 'secureConnection') as Promise<[TLSSocket]>;
      const connection = new IggyConnection({
        transport: 'TLS',
        options: {
          host: 'localhost',
          port: (server.address() as AddressInfo).port,
          ca: TLS_CA_CERTIFICATE
        },
        credentials: { username: 'iggy', password: 'iggy' },
        reconnect: { enabled: false, interval: 0, maxRetries: 0 }
      });
      try {
        await connection.connect();
        assert.equal(connection.connected, true);
        const [tlsSocket] = await secureConnection;
        assert.equal(tlsSocket.servername, 'localhost');
      } finally {
        await closeConnection(connection, server);
      }
    }
  );

  it('omits SNI for IP literal hosts', async () => {
    const server = await startTlsServer();
    const secureConnection =
      once(server, 'secureConnection') as Promise<[TLSSocket]>;
    const connection = new IggyConnection({
      transport: 'TLS',
      options: {
        host: '127.0.0.1',
        port: (server.address() as AddressInfo).port,
        rejectUnauthorized: false
      },
      credentials: { username: 'iggy', password: 'iggy' },
      reconnect: { enabled: false, interval: 0, maxRetries: 0 }
    });
    try {
      await connection.connect();
      assert.equal(connection.connected, true);
      const [tlsSocket] = await secureConnection;
      // Node reports a missing SNI name as false on the server side.
      assert.ok(!tlsSocket.servername);
    } finally {
      await closeConnection(connection, server);
    }
  });

  after(() => clearInterval(keepAlive));
});
