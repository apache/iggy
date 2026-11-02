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
import { createServer, Socket, type AddressInfo, type Server } from 'node:net';
import { createServer as createTlsServer, type TLSSocket } from 'node:tls';
import { describe, it } from 'node:test';
import { IggyConnection, LINK_WRITE_QUEUE, MAX_PENDING_PER_LINK } from './client.connection.js';
import { ResponseFrameDecoder, ProtocolFrameError } from './client.frame.js';
import type { ClientConfig } from './client.type.js';
import { Command, HEADER_SIZE, REPLY_OFFSET, REQUEST_OFFSET, encodeRequestHeader } from '../wire/vsr/header.js';
import { Operation } from '../wire/vsr/operation.js';
import { FrozenFrame, OPERATION_TIMEOUT_MS, VsrResponseTimeoutError } from './client.operation.js';
import { ConnectionClosedError } from '../index.js';

const TLS_CERTIFICATE = readFileSync(new URL('../../../../core/certs/iggy_cert.pem', import.meta.url));
const TLS_KEY = readFileSync(new URL('../../../../core/certs/iggy_key.pem', import.meta.url));
const TLS_CA_CERTIFICATE = readFileSync(new URL('../../../../core/certs/iggy_ca_cert.pem', import.meta.url));
const FRAME_LIMIT = 2 * HEADER_SIZE;
const connectionConfig = (server: Server): ClientConfig => ({
  transport: 'TCP', options: { host: '127.0.0.1', port: (server.address() as AddressInfo).port },
  credentials: { username: 'iggy', password: 'iggy' }, heartbeatInterval: 0,
  reconnect: { enabled: false, interval: 0, maxRetries: 0 }, maxResponseFrameSize: FRAME_LIMIT
});
const requestFrame = (id: bigint): Buffer => encodeRequestHeader({
  size: HEADER_SIZE, client: 1n, request: id, operation: Operation.NonReplicated, session: 1n, nonReplicatedCode: 1
});
const replyFrame = (request: Buffer, body = Buffer.alloc(0)): Buffer => {
  const response = Buffer.alloc(HEADER_SIZE + body.length);
  response.writeUInt32LE(response.length, REPLY_OFFSET.size);
  response[REPLY_OFFSET.command] = Command.Reply;
  response[REPLY_OFFSET.operation] = request[REQUEST_OFFSET.operation];
  response.writeBigUInt64LE(request.readBigUInt64LE(REQUEST_OFFSET.request), REPLY_OFFSET.request);
  body.copy(response, HEADER_SIZE);
  return response;
};
const startServer = async (handler?: (frame: Buffer, socket: Socket) => void, tls = false): Promise<Server> => {
  const accept = (socket: Socket) => {
    const decoder = new ResponseFrameDecoder(FRAME_LIMIT);
    socket.on('error', () => undefined);
    socket.on('data', (bytes) => { if (!handler) return; for (const frame of decoder.push(Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes))) handler?.(frame, socket); });
  };
  const server = tls ? createTlsServer({ cert: TLS_CERTIFICATE, key: TLS_KEY }, accept) : createServer(accept);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server;
};
const closeConnection = async (connection: IggyConnection, server: Server): Promise<void> => {
  connection._destroy();
  await new Promise<void>((resolve) => server.close(() => resolve()));
};
const exchange = (connection: IggyConnection, id: bigint, signal = new AbortController().signal) =>
  connection.exchange(() => requestFrame(id), Date.now() + 1_000, signal);

describe('IggyConnection multiplexing', () => {
  it('matches normalized peer addresses and ports with a configured-host fallback', () => {
    const host = 'bootstrap.example.test';
    const configuredPort = 8090;
    const peerPort = configuredPort + 1;
    const connection = new IggyConnection({ transport: 'TCP', options: { host, port: configuredPort },
      credentials: { username: 'iggy', password: 'iggy' } });
    assert.equal(connection.isConnectedTo(host.toUpperCase(), configuredPort), true);
    assert.equal(connection.isConnectedTo('192.0.2.10', peerPort), false);
    connection.socket = new Socket();
    Object.defineProperties(connection.socket, {
      remoteAddress: { value: '::ffff:192.0.2.10' },
      remotePort: { value: peerPort }
    });
    try {
      assert.equal(connection.isConnectedTo('192.0.2.10', peerPort), true);
      assert.equal(connection.isConnectedTo('::FFFF:192.0.2.10', peerPort), true);
      assert.equal(connection.isConnectedTo('192.0.2.10', configuredPort), false);
      assert.equal(connection.isConnectedTo('192.0.2.11', peerPort), false);
      assert.equal(connection.isConnectedTo(host.toUpperCase(), configuredPort), true);
      assert.equal(connection.isConnectedTo(host, peerPort), false);
    } finally { connection._destroy(); }
  });

  it('recognizes an IP roster endpoint for a DNS TLS connection', async () => {
    const server = await startServer((frame, socket) => socket.write(replyFrame(frame)), true);
    const secured = once(server, 'secureConnection') as Promise<[TLSSocket]>;
    const host = 'bootstrap.example.test';
    const port = (server.address() as AddressInfo).port;
    let accepted = 0;
    server.on('secureConnection', () => { accepted += 1; });
    const connection = new IggyConnection({ ...connectionConfig(server), transport: 'TLS',
      options: { host, port, family: 4, rejectUnauthorized: false,
        lookup: (_hostname, _options, callback) => callback(null, '127.0.0.1', 4) } });
    try {
      await connection.connect();
      const [peer] = await secured;
      const socket = connection.socket;
      assert.equal(peer.servername, host);
      assert.equal(socket.remoteAddress, '127.0.0.1');
      assert.equal(connection.isConnectedTo('127.0.0.1', port), true);
      assert.equal(connection.isConnectedTo('::ffff:127.0.0.1', port), true);
      assert.equal(connection.isConnectedTo(host.toUpperCase(), port), true);
      assert.equal((await exchange(connection, 1n)).error, undefined);
      await connection.connect();
      assert.equal(connection.socket, socket);
      assert.equal(accepted, 1);
    } finally { await closeConnection(connection, server); }
  });

  it('dials lazily and shares an in-flight connection attempt', async () => {
    const server = await startServer((frame, socket) => socket.write(replyFrame(frame)));
    let accepted = 0;
    server.on('connection', () => { accepted += 1; });
    const connection = new IggyConnection(connectionConfig(server));
    try {
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(accepted, 0);
      const pending = connection.connect();
      assert.equal(connection.connect(), pending);
      await pending;
      assert.equal(accepted, 1);
      assert.equal(connection.isConnectedTo('localhost', (server.address() as AddressInfo).port), true);
      assert.equal((await exchange(connection, 1n)).error, undefined);
    } finally { await closeConnection(connection, server); }
  });

  it('correlates out-of-order replies on one socket', async () => {
    const requests: Buffer[] = [];
    const server = await startServer((frame, socket) => {
      requests.push(frame);
      if (requests.length === 2)
        socket.write(Buffer.concat([replyFrame(requests[1], Buffer.from('two')), replyFrame(requests[0], Buffer.from('one'))]));
    });
    const connection = new IggyConnection(connectionConfig(server));
    try {
      await connection.connect();
      const [first, second] = await Promise.all([exchange(connection, 1n), exchange(connection, 2n)]);
      assert.equal(first.response?.subarray(HEADER_SIZE).toString(), 'one');
      assert.equal(second.response?.subarray(HEADER_SIZE).toString(), 'two');
    } finally { await closeConnection(connection, server); }
  });

  it('isolates cancellation and discards a late reply without closing the socket', async () => {
    const canceled = new AbortController();
    let first: Buffer | undefined;
    const server = await startServer((frame, socket) => {
      if (frame.readBigUInt64LE(REQUEST_OFFSET.request) === 1n) { first = frame; canceled.abort(); }
      else socket.write(Buffer.concat([replyFrame(first!), replyFrame(frame, Buffer.from('alive'))]));
    });
    const connection = new IggyConnection(connectionConfig(server));
    try {
      await connection.connect();
      assert.ok((await exchange(connection, 1n, canceled.signal)).error);
      assert.equal((await exchange(connection, 2n)).response?.subarray(HEADER_SIZE).toString(), 'alive');
      assert.equal(connection.connected, true);
    } finally { await closeConnection(connection, server); }
  });

  it('times out an expired deadline before building a frame', async () => {
    const server = await startServer();
    const connection = new IggyConnection(connectionConfig(server));
    let built = 0;
    try {
      await connection.connect();
      const result = await connection.exchange(() => { built += 1; return requestFrame(1n); },
        Date.now() - 1, new AbortController().signal);
      assert.ok(result.error instanceof VsrResponseTimeoutError);
      assert.match(String(result.error), /after 0 ms/);
      assert.equal(result.started, false);
      assert.equal(built, 0);
    } finally { await closeConnection(connection, server); }
  });

  it('reports a caller abort ahead of an expired deadline', async () => {
    const server = await startServer();
    const connection = new IggyConnection(connectionConfig(server));
    const cancel = new AbortController();
    const reason = new Error('caller canceled');
    let built = 0;
    try {
      await connection.connect();
      cancel.abort(reason);
      const result = await connection.exchange(() => { built += 1; return requestFrame(1n); },
        Date.now() - 1, cancel.signal);
      assert.equal(result.error, reason);
      assert.equal(result.started, false);
      assert.equal(built, 0);
    } finally { await closeConnection(connection, server); }
  });

  it('assembles partial frames and rejects an oversized declared frame', async () => {
    let count = 0;
    const server = await startServer((frame, socket) => {
      const response = replyFrame(frame, Buffer.from('complete'));
      if (++count === 1) {
        socket.write(response.subarray(0, 6));
        setImmediate(() => socket.write(response.subarray(6)));
      } else {
        response.writeUInt32LE(FRAME_LIMIT + 1, REPLY_OFFSET.size);
        socket.write(response);
      }
    });
    const connection = new IggyConnection(connectionConfig(server));
    try {
      await connection.connect();
      assert.equal((await exchange(connection, 1n)).response?.subarray(HEADER_SIZE).toString(), 'complete');
      assert.ok((await exchange(connection, 2n)).error instanceof ProtocolFrameError);
      assert.equal(connection.connected, false);
    } finally { await closeConnection(connection, server); }
  });

  it('bounds pending calls before frame allocation and releases capacity after cancellation', async () => {
    const server = await startServer();
    const connection = new IggyConnection(connectionConfig(server));
    const cancel = new AbortController();
    let allocated = 0;
    try {
      await connection.connect();
      const calls = Array.from({ length: MAX_PENDING_PER_LINK + 1 }, (_, index) =>
        connection.exchange(() => { allocated += 1; return requestFrame(BigInt(index + 1)); }, Date.now() + 2_000, cancel.signal));
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      assert.equal(allocated, MAX_PENDING_PER_LINK);
      cancel.abort();
      await Promise.all(calls);
      assert.equal(allocated, MAX_PENDING_PER_LINK);
      assert.equal(connection.connected, true);
    } finally { cancel.abort(); await closeConnection(connection, server); }
  });

  it('bounds queued writers and pins canceled frame bytes until the write callback completes', async () => {
    const received: Buffer[] = [];
    const server = await startServer((frame) => { received.push(frame); });
    const connection = new IggyConnection(connectionConfig(server));
    const cancel = new AbortController();
    let released = false;
    const owner = new FrozenFrame(requestFrame(1n), () => { released = true; });
    let completeWrite: (() => void) | undefined;
    let writing!: () => void;
    const started = new Promise<void>((resolve) => { writing = resolve; });
    try {
      await connection.connect();
      const write = connection.socket.write.bind(connection.socket);
      connection.socket.write = ((chunk: Buffer, callback: (error?: Error | null) => void) =>
        write(chunk, (error) => { completeWrite = () => callback(error); writing(); })) as Socket['write'];
      const first = connection.exchange(() => owner.buffer, Date.now() + 2_000, cancel.signal, owner);
      await started;
      owner.release();
      const waiting = Array.from({ length: LINK_WRITE_QUEUE + 8 }, (_, index) =>
        exchange(connection, BigInt(index + 2), cancel.signal));
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal((connection as unknown as { writes: unknown[] }).writes.length, LINK_WRITE_QUEUE);
      cancel.abort();
      assert.equal((await first).started, true);
      assert.ok((await Promise.all(waiting)).every((result) => !result.started));
      assert.equal(released, false);
      assert.equal(owner.buffer.readBigUInt64LE(REQUEST_OFFSET.request), 1n);
      connection.socket.write = write;
      assert.ok(completeWrite);
      completeWrite();
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(released, true);
      assert.throws(() => owner.buffer, /released/);
      assert.equal(received.length, 1);
    } finally { cancel.abort(); completeWrite?.(); await closeConnection(connection, server); }
  });

  it('expires an incomplete frame independently of the caller deadline', async (context) => {
    const server = await startServer((frame, socket) => socket.write(replyFrame(frame, Buffer.from('body')).subarray(0, HEADER_SIZE + 1)));
    const connection = new IggyConnection(connectionConfig(server));
    try {
      await connection.connect();
      context.mock.timers.enable({ apis: ['setTimeout'] });
      const received = once(connection.socket, 'data');
      const pending = connection.exchange(() => requestFrame(1n), Date.now() + OPERATION_TIMEOUT_MS * 2,
        new AbortController().signal);
      await received;
      context.mock.timers.tick(OPERATION_TIMEOUT_MS);
      const result = await pending;
      assert.ok(result.error instanceof ProtocolFrameError);
      assert.match(String(result.error), /incomplete consensus frame timed out/);
      assert.equal(connection.connected, false);
    } finally { context.mock.timers.reset(); await closeConnection(connection, server); }
  });

  it('never reconnects a failed physical link', async () => {
    let accepted = 0;
    const server = await startServer((_frame, socket) => socket.destroy());
    server.on('connection', () => { accepted += 1; });
    const connection = new IggyConnection(connectionConfig(server));
    try {
      await connection.connect();
      assert.ok((await exchange(connection, 1n)).error instanceof ConnectionClosedError);
      await assert.rejects(connection.connect());
      assert.equal(accepted, 1);
    } finally { await closeConnection(connection, server); }
  });

  it('bounds TCP plus TLS establishment when the peer never completes TLS', async () => {
    const server = await startServer();
    const connection = new IggyConnection({ ...connectionConfig(server), transport: 'TLS',
      options: { ...connectionConfig(server).options, rejectUnauthorized: false } });
    const started = Date.now();
    try {
      await assert.rejects(connection.connect(), /timed out/);
      assert.ok(Date.now() - started < 3_000);
      assert.equal(connection.connected, false);
    } finally { await closeConnection(connection, server); }
  });

  it('rejects an unreadable CA file when connecting without leaking a socket', async () => {
    const connection = new IggyConnection({ transport: 'TLS', options: { host: '127.0.0.1', port: 8090,
      caFile: '/does/not/exist.pem' }, credentials: { username: 'iggy', password: 'iggy' } });
    await assert.rejects(connection.connect(), /cannot read tls_ca_file/);
    connection._destroy();
  });

  for (const host of ['localhost', '127.0.0.1']) {
    it(`uses the correct TLS server name for ${host}`, async () => {
      const server = await startServer(undefined, true);
      const secured = once(server, 'secureConnection') as Promise<[TLSSocket]>;
      const connection = new IggyConnection({ ...connectionConfig(server), transport: 'TLS',
        options: { host, port: (server.address() as AddressInfo).port,
          ...(host === 'localhost' ? { ca: TLS_CA_CERTIFICATE } : { rejectUnauthorized: false }) } });
      try {
        await connection.connect();
        const [socket] = await secured;
        assert.equal(socket.servername || undefined, host === 'localhost' ? host : undefined);
      } finally { await closeConnection(connection, server); }
    });
  }
});
