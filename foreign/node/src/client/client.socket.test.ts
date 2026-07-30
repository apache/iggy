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
import type { AddressInfo, Socket } from 'node:net';
import { createServer, type Server } from 'node:net';
import { describe, it } from 'node:test';
import { COMMAND_CODE } from '../wire/command.code.js';
import { ResponseError } from '../wire/error.utils.js';
import {
  Command2,
  EVICTION_OFFSET,
  EvictionReason,
  HEADER_SIZE,
  REPLY_OFFSET,
  REQUEST_OFFSET
} from '../wire/vsr/header.js';
import { Operation } from '../wire/vsr/operation.js';
import { VsrEvictionError } from '../wire/vsr/reply.js';
import { CommandResponseStream } from './client.socket.js';
import type { ClientConfig } from './client.type.js';

const TEST_SESSION = 42n;

type FrameHandler = (frame: Buffer, socket: Socket) => void;

type VsrTestServer = {
  port: number,
  frames: Buffer[],
  close: () => Promise<void>
};

/** Loopback server speaking just enough VSR framing for the client tests. */
const startVsrServer = async (
  handler: FrameHandler
): Promise<VsrTestServer> => {
  const frames: Buffer[] = [];
  const server: Server = createServer((socket) => {
    let pending = Buffer.alloc(0);
    socket.on('data', (data: Buffer) => {
      pending = Buffer.concat([pending, data]);
      while (pending.length >= HEADER_SIZE) {
        const size = pending.readUInt32LE(REQUEST_OFFSET.size);
        if (pending.length < size) break;
        const frame = pending.subarray(0, size);
        pending = pending.subarray(size);
        frames.push(Buffer.from(frame));
        handler(Buffer.from(frame), socket);
      }
    });
    socket.on('error', () => {});
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return {
    port: (server.address() as AddressInfo).port,
    frames,
    close: () => new Promise((resolve) => server.close(() => resolve()))
  };
};

const replyFrame = (
  operation: number,
  body: Buffer = Buffer.alloc(0),
  status = 0
): Buffer => {
  const frame = Buffer.alloc(HEADER_SIZE + body.length);
  frame.writeUInt32LE(frame.length, REPLY_OFFSET.size);
  frame.writeUInt8(Command2.Reply, REPLY_OFFSET.command);
  frame.writeUInt8(operation, REPLY_OFFSET.operation);
  frame.writeUInt32LE(status, REPLY_OFFSET.status);
  body.copy(frame, HEADER_SIZE);
  return frame;
};

const evictionFrame = (reason: number): Buffer => {
  const frame = Buffer.alloc(HEADER_SIZE);
  frame.writeUInt32LE(HEADER_SIZE, REPLY_OFFSET.size);
  frame.writeUInt8(Command2.Eviction, REPLY_OFFSET.command);
  frame.writeUInt8(reason, EVICTION_OFFSET.reason);
  return frame;
};

const registerReplyBody = (): Buffer => {
  const serverVersion = Buffer.from('0.0.0');
  const body = Buffer.alloc(4 + 17 + serverVersion.length);
  body.writeUInt32LE(0, 0);
  body.writeUInt32LE(7, 4);
  body.writeBigUInt64LE(TEST_SESSION, 8);
  body.writeUInt32LE(0, 16);
  body.writeUInt8(serverVersion.length, 20);
  serverVersion.copy(body, 21);
  return body;
};

const singleNodeMetadataBody = (port: number): Buffer => {
  const name = Buffer.from('single-node');
  const nodeName = Buffer.from('iggy-node');
  const ip = Buffer.from('127.0.0.1');
  const body = Buffer.alloc(
    4 + name.length + 4 + 4 + nodeName.length + 4 + ip.length + 8 + 2
  );
  let offset = 0;
  body.writeUInt32LE(name.length, offset); offset += 4;
  name.copy(body, offset); offset += name.length;
  body.writeUInt32LE(1, offset); offset += 4;
  body.writeUInt32LE(nodeName.length, offset); offset += 4;
  nodeName.copy(body, offset); offset += nodeName.length;
  body.writeUInt32LE(ip.length, offset); offset += 4;
  ip.copy(body, offset); offset += ip.length;
  body.writeUInt16LE(port, offset); offset += 8;
  body.writeUInt8(0, offset); offset += 1;
  body.writeUInt8(0, offset);
  return body;
};

const twoNodeMetadataBody = (
  followerPort: number,
  leaderPort: number,
  leaderRole = 0,
  leaderStatus = 0
): Buffer => {
  const node = (
    nodeName: Buffer,
    port: number,
    role: number,
    status = 0
  ): Buffer => {
    const ip = Buffer.from('127.0.0.1');
    const encoded = Buffer.alloc(4 + nodeName.length + 4 + ip.length + 8 + 2);
    let offset = 0;
    encoded.writeUInt32LE(nodeName.length, offset); offset += 4;
    nodeName.copy(encoded, offset); offset += nodeName.length;
    encoded.writeUInt32LE(ip.length, offset); offset += 4;
    ip.copy(encoded, offset); offset += ip.length;
    encoded.writeUInt16LE(port, offset); offset += 8;
    encoded.writeUInt8(role, offset); offset += 1;
    encoded.writeUInt8(status, offset);
    return encoded;
  };
  const name = Buffer.from('iggy-cluster');
  const header = Buffer.alloc(4 + name.length + 4);
  header.writeUInt32LE(name.length, 0);
  name.copy(header, 4);
  header.writeUInt32LE(2, 4 + name.length);
  return Buffer.concat([
    header,
    node(Buffer.from('iggy-node-1'), leaderPort, leaderRole, leaderStatus),
    node(Buffer.from('iggy-node-2'), followerPort, 1)
  ]);
};

/** Register, metadata, and echo behavior of a healthy single VSR node. */
const singleNodeHandler = (port: number): FrameHandler =>
  (frame, socket) => {
    const operation = frame.readUInt8(REQUEST_OFFSET.operation);
    if (operation === Operation.Register) {
      socket.write(replyFrame(Operation.Register, registerReplyBody()));
      return;
    }
    const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
    if (code === COMMAND_CODE.GetClusterMetadata) {
      socket.write(
        replyFrame(Operation.NonReplicated, singleNodeMetadataBody(port))
      );
      return;
    }
    socket.write(replyFrame(operation));
  };

const vsrConfig = (port: number): ClientConfig => ({
  protocol: 'vsr',
  transport: 'TCP',
  options: { host: '127.0.0.1', port },
  credentials: { username: 'iggy', password: 'iggy' },
  reconnect: { enabled: false, interval: 100, maxRetries: 1 }
});

describe('VSR client socket', () => {
  it('registers one session before the first authenticated command', async () => {
    const server = await startVsrServer(
      (frame, socket) => singleNodeHandler(server.port)(frame, socket)
    );
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      const response = await client.sendCommand(
        60_001,
        Buffer.from('opaque')
      );
      assert.equal(response.status, 0);

      const operations = server.frames.map(
        (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
      );
      assert.deepEqual(operations, [
        Operation.NonReplicated,
        Operation.Register,
        Operation.NonReplicated
      ]);
      const register = server.frames[1];
      assert.equal(register.readBigUInt64LE(REQUEST_OFFSET.request), 0n);
      assert.equal(register.readBigUInt64LE(REQUEST_OFFSET.session), 0n);
      const request = server.frames[2];
      assert.equal(
        request.readBigUInt64LE(REQUEST_OFFSET.session),
        TEST_SESSION
      );
      assert.equal(request.readUInt32LE(REQUEST_OFFSET.reserved), 60_001);
      assert.equal(client.isAuthenticated, true);
      assert.equal(client.userId, 7);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('replays the identical frame on a transient response', async () => {
    let attempts = 0;
    const server = await startVsrServer((frame, socket) => {
      const operation = frame.readUInt8(REQUEST_OFFSET.operation);
      if (operation !== Operation.NonReplicated ||
          frame.readUInt32LE(REQUEST_OFFSET.reserved) !== 60_002)
        return singleNodeHandler(server.port)(frame, socket);
      attempts += 1;
      socket.write(
        attempts === 1
          ? replyFrame(operation, Buffer.alloc(0), 57)
          : replyFrame(operation, Buffer.from('done'))
      );
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      const response = await client.sendCommand(
        60_002,
        Buffer.from('retry-me')
      );
      assert.equal(attempts, 2);
      assert.deepEqual(response.data, Buffer.from('done'));
      const sent = server.frames.filter(
        (frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_002
      );
      assert.equal(sent.length, 2);
      assert.deepEqual(sent[0], sent[1]);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('routes eviction out of band without desynchronizing replies', async () => {
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_003) {
        socket.write(Buffer.concat([
          evictionFrame(EvictionReason.NoSession),
          replyFrame(Operation.NonReplicated, Buffer.from('stale'))
        ]));
        return;
      }
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_014) {
        socket.write(
          replyFrame(Operation.NonReplicated, Buffer.from('fresh'))
        );
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      let sessionResets = 0;
      let evictions = 0;
      client.on('sessionReset', () => { sessionResets += 1; });
      client.on('eviction', () => { evictions += 1; });
      await assert.rejects(
        () => client.sendCommand(
          60_003,
          Buffer.alloc(0)
        ),
        (error: unknown) =>
          error instanceof VsrEvictionError && error.errorCode === 40
      );
      assert.ok(sessionResets >= 1);
      assert.equal(evictions, 1);
      assert.equal(client.isAuthenticated, false);
      const response = await client.sendCommand(60_014, Buffer.alloc(0));
      assert.deepEqual(response.data, Buffer.from('fresh'));
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('fails an in-flight request when the connection drops', async () => {
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_004) {
        socket.destroy();
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await assert.rejects(
        () => client.sendCommand(
          60_004,
          Buffer.alloc(0)
        )
      );
      assert.equal(client.isAuthenticated, false);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('redirects a direct login to the advertised leader before registering',
    async () => {
      const leader = await startVsrServer(
        (frame, socket) => singleNodeHandler(leader.port)(frame, socket)
      );
      const follower = await startVsrServer((frame, socket) => {
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (code === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(
            Operation.NonReplicated,
            twoNodeMetadataBody(follower.port, leader.port)
          ));
          return;
        }
        socket.write(replyFrame(
          frame.readUInt8(REQUEST_OFFSET.operation),
          Buffer.alloc(0),
          3
        ));
      });
      const client = new CommandResponseStream(vsrConfig(follower.port));
      try {
        const response = await client.sendCommand(COMMAND_CODE.LoginUser,
          Buffer.concat([
            Buffer.from([4]),
            Buffer.from('iggy'),
            Buffer.from([4]),
            Buffer.from('iggy')
          ]));
        assert.equal(response.status, 0);
        assert.equal(client.isAuthenticated, true);
        const followerOperations = follower.frames.map(
          (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
        );
        assert.deepEqual(followerOperations, [Operation.NonReplicated]);
        const leaderOperations = leader.frames.map(
          (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
        );
        assert.deepEqual(leaderOperations, [
          Operation.NonReplicated,
          Operation.Register
        ]);
      } finally {
        client.destroy();
        await leader.close();
        await follower.close();
      }
    });

  it('rejects instead of hanging while a connection attempt is unresolved', async () => {
    const server = await startVsrServer(() => {});
    const port = server.port;
    await server.close();
    const client = new CommandResponseStream({
      ...vsrConfig(port),
      reconnect: { enabled: true, interval: 300, maxRetries: 1 }
    });
    try {
      await assert.rejects(
        () => client.sendCommand(
          60_006,
          Buffer.alloc(0)
        )
      );
      await assert.rejects(
        () => client.sendCommand(
          60_006,
          Buffer.alloc(0)
        )
      );
    } finally {
      client.destroy();
    }
  });

  it('resets the session before an authenticated VSR login', async () => {
    const server = await startVsrServer(
      (frame, socket) => singleNodeHandler(server.port)(frame, socket)
    );
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.sendCommand(60_007, Buffer.alloc(0));
      let sessionResets = 0;
      client.on('sessionReset', () => { sessionResets += 1; });

      await client.sendCommand(
        COMMAND_CODE.LoginUser,
        Buffer.concat([
          Buffer.from([4]),
          Buffer.from('iggy'),
          Buffer.from([4]),
          Buffer.from('iggy')
        ])
      );

      const operations = server.frames.map(
        (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
      );
      assert.equal(
        operations.filter((operation) => operation === Operation.Register)
          .length,
        2
      );
      assert.ok(operations.includes(Operation.Logout));
      assert.equal(sessionResets, 1);
      assert.equal(client.isAuthenticated, true);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('keeps logout and replacement login adjacent in the queue', async () => {
    const server = await startVsrServer((frame, socket) => {
      const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
      if (code === 60_015) {
        setTimeout(() => {
          socket.write(
            replyFrame(Operation.NonReplicated, Buffer.from('first'))
          );
        }, 10);
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      const first = client.sendCommand(60_015, Buffer.alloc(0));
      const second = client.sendCommand(60_016, Buffer.alloc(0));
      const login = client.sendCommand(
        COMMAND_CODE.LoginUser,
        Buffer.concat([
          Buffer.from([4]),
          Buffer.from('iggy'),
          Buffer.from([4]),
          Buffer.from('iggy')
        ])
      );
      await Promise.all([first, second, login]);

      const operations = server.frames.map(
        (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
      );
      const logoutIndex = operations.lastIndexOf(Operation.Logout);
      assert.equal(operations[logoutIndex + 1], Operation.Register);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('supports raw VSR responses without decoding', async () => {
    const server = await startVsrServer(
      (frame, socket) => singleNodeHandler(server.port)(frame, socket)
    );
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      const raw = await client.sendCommand(
        60_008,
        Buffer.alloc(0),
        { handleResponse: false }
      );
      assert.ok(Buffer.isBuffer(raw));
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('remaps a terminal VSR response to the original command', async () => {
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_009) {
        socket.write(replyFrame(Operation.NonReplicated, Buffer.alloc(0), 3));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await assert.rejects(
        () => client.sendCommand(60_009, Buffer.alloc(0)),
        (error: unknown) =>
          error instanceof ResponseError &&
          error.commandCode === 60_009 &&
          error.errorCode === 3
      );
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('rejects synchronous writes and timed-out exchanges', async () => {
    const server = await startVsrServer(() => {});
    const client = new CommandResponseStream(vsrConfig(server.port));
    const exchange = (
      client as unknown as {
        _exchange: (write: () => void, timeout?: number) => Promise<Buffer>
      }
    )._exchange.bind(client);
    try {
      const writeError = new Error('write failed');
      await assert.rejects(
        () => exchange(() => { throw writeError; }),
        (error: unknown) => error === writeError
      );
      const connection = (
        client as unknown as {
          connection: { emit: (event: string, error: Error) => boolean }
        }
      ).connection;
      const responseError = new Error('response failed');
      const pending = exchange(() => {});
      connection.emit('error', responseError);
      await assert.rejects(
        () => pending,
        (error: unknown) => error === responseError
      );
      await assert.rejects(
        () => exchange(() => {}, 1),
        /timed out after 1 ms/
      );
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('expires a VSR request before writing after its deadline', async () => {
    const server = await startVsrServer(
      (frame, socket) => singleNodeHandler(server.port)(frame, socket)
    );
    const client = new CommandResponseStream(vsrConfig(server.port));
    const realNow = Date.now;
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      let sessionResets = 0;
      client.on('sessionReset', () => { sessionResets += 1; });
      let now = 0;
      Date.now = () => {
        now += 30_001;
        return now;
      };
      await assert.rejects(
        () => client.sendCommand(60_013, Buffer.alloc(0)),
        /timed out after 30000 ms/
      );
      assert.equal(
        server.frames.some(
          (frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_013
        ),
        false
      );
      assert.equal(client.isAuthenticated, true);
      assert.equal(sessionResets, 0);
    } finally {
      Date.now = realNow;
      client.destroy();
      await server.close();
    }
  });

  it('keeps a typed transient error and session at its retry deadline',
    async () => {
      const server = await startVsrServer((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_017) {
          socket.write(
            replyFrame(Operation.NonReplicated, Buffer.alloc(0), 57)
          );
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      const realNow = Date.now;
      try {
        await client.authenticate(vsrConfig(server.port).credentials);
        let sessionResets = 0;
        client.on('sessionReset', () => { sessionResets += 1; });
        const times = [0, 1, 30_001];
        Date.now = () => times.shift() ?? 30_001;

        await assert.rejects(
          () => client.sendCommand(60_017, Buffer.alloc(0)),
          (error: unknown) =>
            error instanceof ResponseError &&
            error.commandCode === 60_017 &&
            error.errorCode === 57
        );
        assert.equal(client.isAuthenticated, true);
        assert.equal(sessionResets, 0);
      } finally {
        Date.now = realNow;
        client.destroy();
        await server.close();
      }
    }
  );

  it('rejects a cluster without a healthy leader', async () => {
    const server = await startVsrServer((frame, socket) => {
      socket.write(replyFrame(
        frame.readUInt8(REQUEST_OFFSET.operation),
        twoNodeMetadataBody(server.port, server.port, 1)
      ));
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await assert.rejects(
        () => client.sendCommand(60_010, Buffer.alloc(0)),
        /VSR cluster has no healthy leader/
      );
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('shares token authentication between concurrent callers', async () => {
    const server = await startVsrServer(
      (frame, socket) => singleNodeHandler(server.port)(frame, socket)
    );
    const client = new CommandResponseStream({
      ...vsrConfig(server.port),
      credentials: { token: 'secret' }
    });
    try {
      await Promise.all([
        client.sendCommand(60_011, Buffer.alloc(0)),
        client.sendCommand(60_012, Buffer.alloc(0))
      ]);
      assert.equal(
        server.frames.filter(
          (frame) =>
            frame.readUInt8(REQUEST_OFFSET.operation) === Operation.Register
        ).length,
        1
      );
      assert.equal(client.isAuthenticated, true);
    } finally {
      client.destroy();
      await server.close();
    }
  });
});
