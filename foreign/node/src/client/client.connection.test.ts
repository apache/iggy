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
//

import assert from 'node:assert/strict';
import { once } from 'node:events';
import {
  createServer,
  type AddressInfo,
  type Server,
  type Socket,
} from 'node:net';
import { describe, it } from 'node:test';
import { ProtocolFrameError } from './client.frame.js';
import { IggyConnection } from './client.connection.js';
import type { ClientConfig } from './client.type.js';

const startServer = async (): Promise<Server> => {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server;
};

const connectionConfig = (server: Server): ClientConfig => ({
  protocol: 'classic',
  transport: 'TCP',
  options: {
    host: '127.0.0.1',
    port: (server.address() as AddressInfo).port
  },
  credentials: { username: 'iggy', password: 'iggy' },
  reconnect: { enabled: false, interval: 0, maxRetries: 0 },
  maxResponseFrameSize: 256
});

const closeConnection = async (
  connection: IggyConnection,
  server: Server
): Promise<void> => {
  connection._destroy();
  if (!connection.socket.destroyed)
    await once(connection.socket, 'close');
  await new Promise<void>((resolve) => server.close(() => resolve()));
};

describe('IggyConnection', () => {
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

  it('shares connection attempts, recognizes endpoints, and writes commands',
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

        connection.writeCommand(1, Buffer.from('payload'));
        const command = await received;
        assert.equal(command.readUInt32LE(4), 1);
        assert.deepEqual(command.subarray(8), Buffer.from('payload'));
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
        const body = Buffer.from('response');
        const frame = Buffer.alloc(8 + body.length);
        frame.writeUInt32LE(body.length, 4);
        body.copy(frame, 8);
        const response = once(connection, 'response');
        connection._onData(frame.subarray(0, 6));
        connection._onData(frame.subarray(6));
        assert.deepEqual((await response)[0], frame);

        const malformed = Buffer.alloc(8);
        malformed.writeUInt32LE(256, 4);
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

        const body = Buffer.from('fresh');
        const frame = Buffer.alloc(8 + body.length);
        frame.writeUInt32LE(body.length, 4);
        body.copy(frame, 8);
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
});
