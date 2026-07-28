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
import { createServer, type AddressInfo, type Server } from 'node:net';
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
        connection.connecting = true;
        await assert.rejects(
          () => connection.connect(),
          /connection attempt already in progress/
        );
        connection.connecting = false;
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
});
