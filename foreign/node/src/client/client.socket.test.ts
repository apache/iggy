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
import type { AddressInfo, Socket } from 'node:net';
import { createServer, type Server } from 'node:net';
import { describe, it } from 'node:test';
import { createServer as createTlsServer } from 'node:tls';
import { COMMAND_CODE } from '../wire/command.code.js';
import { ResponseError } from '../wire/error.utils.js';
import {
  Command,
  EVICTION_OFFSET,
  EvictionReason,
  HEADER_SIZE,
  REPLY_OFFSET,
  REQUEST_OFFSET
} from '../wire/vsr/header.js';
import { Operation } from '../wire/vsr/operation.js';
import { VsrEvictionError } from '../wire/vsr/reply.js';
import { CommandResponseStream } from './client.socket.js';
import type { ClientConfig, CommandResponse } from './client.type.js';

const TEST_SESSION = 42n;
const TLS_CERTIFICATE = readFileSync(
  new URL('../../../../core/certs/iggy_cert.pem', import.meta.url)
);
const TLS_KEY = readFileSync(
  new URL('../../../../core/certs/iggy_key.pem', import.meta.url)
);
const TLS_CA_CERTIFICATE = readFileSync(
  new URL('../../../../core/certs/iggy_ca_cert.pem', import.meta.url)
);

type FrameHandler = (frame: Buffer, socket: Socket) => void;

type VsrTestServer = {
  port: number,
  frames: Buffer[],
  close: () => Promise<void>
};

/** Loopback server speaking just enough VSR framing for the client tests. */
const startVsrServer = async (
  handler: FrameHandler,
  transport: 'TCP' | 'TLS' = 'TCP'
): Promise<VsrTestServer> => {
  const frames: Buffer[] = [];
  const handleConnection = (socket: Socket) => {
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
  };
  const server: Server = transport === 'TLS'
    ? createTlsServer(
      { cert: TLS_CERTIFICATE, key: TLS_KEY },
      handleConnection
    )
    : createServer(handleConnection);
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
  frame.writeUInt8(Command.Reply, REPLY_OFFSET.command);
  frame.writeUInt8(operation, REPLY_OFFSET.operation);
  frame.writeUInt32LE(status, REPLY_OFFSET.status);
  body.copy(frame, HEADER_SIZE);
  return frame;
};

const evictionFrame = (reason: number): Buffer => {
  const frame = Buffer.alloc(HEADER_SIZE);
  frame.writeUInt32LE(HEADER_SIZE, REPLY_OFFSET.size);
  frame.writeUInt8(Command.Eviction, REPLY_OFFSET.command);
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

const threeNodeMetadataBody = (
  firstPort: number,
  secondPort: number,
  thirdPort: number
): Buffer => {
  const node = (name: string, port: number, role: number): Buffer => {
    const nodeName = Buffer.from(name);
    const ip = Buffer.from('127.0.0.1');
    const encoded = Buffer.alloc(4 + nodeName.length + 4 + ip.length + 8 + 2);
    let offset = 0;
    encoded.writeUInt32LE(nodeName.length, offset); offset += 4;
    nodeName.copy(encoded, offset); offset += nodeName.length;
    encoded.writeUInt32LE(ip.length, offset); offset += 4;
    ip.copy(encoded, offset); offset += ip.length;
    encoded.writeUInt16LE(port, offset); offset += 8;
    encoded.writeUInt8(role, offset); offset += 1;
    encoded.writeUInt8(0, offset);
    return encoded;
  };
  const name = Buffer.from('iggy-cluster');
  const header = Buffer.alloc(4 + name.length + 4);
  header.writeUInt32LE(name.length, 0);
  name.copy(header, 4);
  header.writeUInt32LE(3, 4 + name.length);
  return Buffer.concat([
    header,
    node('iggy-node-1', firstPort, 0),
    node('iggy-node-2', secondPort, 1),
    node('iggy-node-3', thirdPort, 1)
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
  transport: 'TCP',
  options: { host: '127.0.0.1', port },
  credentials: { username: 'iggy', password: 'iggy' },
  reconnect: { enabled: false, interval: 100, maxRetries: 1 }
});

/** Shrinks the leaderless poll so a test observes it without waiting on it. */
/** The queue a command waits in, for parking one the way the client does. */
const execQueue = (client: CommandResponseStream): {
  command: number,
  payload: Buffer,
  handleResponse: boolean,
  deadline: number,
  resolve: (v: CommandResponse | PromiseLike<CommandResponse>) => void,
  reject: (e: unknown) => void
}[] => (client as unknown as { _execQueue: never[] })._execQueue;

/** The connection under a stream, for driving a redirect the way a move does. */
const connectionOf = (client: CommandResponseStream): {
  redirect: (host: string, port: number) => Promise<void>
} => (client as unknown as {
  connection: { redirect: (host: string, port: number) => Promise<void> }
}).connection;

const compressLeaderlessPoll = (
  client: CommandResponseStream,
  budget: number
): void => {
  const settlement = client as unknown as {
    leaderlessWaitBudget: number,
    leaderlessPollInterval: number
  };
  settlement.leaderlessWaitBudget = budget;
  settlement.leaderlessPollInterval = 1;
};

describe('VSR client socket', () => {
  it('exchanges VSR frames over TLS', async () => {
    const server = await startVsrServer(
      (frame, socket) => singleNodeHandler(server.port)(frame, socket),
      'TLS'
    );
    const client = new CommandResponseStream({
      ...vsrConfig(server.port),
      transport: 'TLS',
      options: {
        host: '127.0.0.1',
        port: server.port,
        servername: 'localhost',
        ca: TLS_CA_CERTIFICATE
      }
    });
    try {
      const response = await client.sendCommand(60_015, Buffer.from('tls'));

      assert.equal(response.status, 0);
      assert.equal(server.frames.length, 3);
    } finally {
      client.destroy();
      await server.close();
    }
  });

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
        Operation.Register,
        Operation.NonReplicated,
        Operation.NonReplicated
      ]);
      const register = server.frames[0];
      assert.equal(register.readBigUInt64LE(REQUEST_OFFSET.request), 0n);
      assert.equal(register.readBigUInt64LE(REQUEST_OFFSET.session), 0n);
      const settlement = server.frames[1];
      assert.equal(
        settlement.readUInt32LE(REQUEST_OFFSET.reserved),
        COMMAND_CODE.GetClusterMetadata
      );
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

  it('redirects a login to the advertised leader and registers there',
    async () => {
      const leader = await startVsrServer(
        (frame, socket) => singleNodeHandler(leader.port)(frame, socket)
      );
      const follower = await startVsrServer((frame, socket) => {
        const operation = frame.readUInt8(REQUEST_OFFSET.operation);
        if (operation === Operation.Register) {
          socket.write(replyFrame(Operation.Register, registerReplyBody()));
          return;
        }
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) ===
            COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(
            Operation.NonReplicated,
            twoNodeMetadataBody(follower.port, leader.port)
          ));
          return;
        }
        socket.write(replyFrame(operation, Buffer.alloc(0), 58));
      });
      const client = new CommandResponseStream(vsrConfig(follower.port));
      try {
        const response = await client.sendCommand(
          COMMAND_CODE.LoginUser,
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
        assert.deepEqual(followerOperations, [
          Operation.Register,
          Operation.NonReplicated
        ]);

        await client.sendCommand(60_018, Buffer.alloc(0));

        const leaderOperations = leader.frames.map(
          (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
        );
        assert.deepEqual(leaderOperations, [
          Operation.Register,
          Operation.NonReplicated,
          Operation.NonReplicated
        ]);
        assert.equal(
          leader.frames[2].readUInt32LE(REQUEST_OFFSET.reserved),
          60_018
        );
        assert.equal(follower.frames.length, 2);
      } finally {
        client.destroy();
        await leader.close();
        await follower.close();
      }
    });

  it('rechecks leadership after a redirected login', async () => {
    const leader = await startVsrServer(
      (frame, socket) => singleNodeHandler(leader.port)(frame, socket)
    );
    const intermediate = await startVsrServer((frame, socket) => {
      const operation = frame.readUInt8(REQUEST_OFFSET.operation);
      if (operation === Operation.Register) {
        socket.write(replyFrame(Operation.Register, registerReplyBody()));
        return;
      }
      socket.write(replyFrame(
        Operation.NonReplicated,
        twoNodeMetadataBody(intermediate.port, leader.port)
      ));
    });
    const follower = await startVsrServer((frame, socket) => {
      const operation = frame.readUInt8(REQUEST_OFFSET.operation);
      if (operation === Operation.Register) {
        socket.write(replyFrame(Operation.Register, registerReplyBody()));
        return;
      }
      socket.write(replyFrame(
        Operation.NonReplicated,
        twoNodeMetadataBody(follower.port, intermediate.port)
      ));
    });
    const client = new CommandResponseStream(vsrConfig(follower.port));
    try {
      await client.authenticate(vsrConfig(follower.port).credentials);
      await client.sendCommand(60_019, Buffer.alloc(0));

      assert.deepEqual(
        follower.frames.map((frame) => frame.readUInt8(REQUEST_OFFSET.operation)),
        [Operation.Register, Operation.NonReplicated]
      );
      assert.deepEqual(
        intermediate.frames.map(
          (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
        ),
        [Operation.Register, Operation.NonReplicated]
      );
      assert.equal(
        leader.frames[2].readUInt32LE(REQUEST_OFFSET.reserved),
        60_019
      );
    } finally {
      client.destroy();
      await follower.close();
      await intermediate.close();
      await leader.close();
    }
  });

  // The node a client authenticated on dies; its next command has to complete
  // on a survivor the roster named, under a session established there.
  // Mirrors `core/integration/tests/cluster/failover_client_continuity.rs`.
  it('resumes on a survivor after the node it authenticated on dies',
    async () => {
      const primarySockets = new Set<Socket>();
      let primaryDead = false;

      const survivor = await startVsrServer((frame, socket) => {
        const operation = frame.readUInt8(REQUEST_OFFSET.operation);
        if (operation === Operation.Register) {
          socket.write(replyFrame(Operation.Register, registerReplyBody()));
          return;
        }
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (code === COMMAND_CODE.GetClusterMetadata) {
          // The survivor leads once the primary is gone.
          socket.write(replyFrame(
            Operation.NonReplicated,
            twoNodeMetadataBody(primary.port, survivor.port)
          ));
          return;
        }
        socket.write(replyFrame(operation));
      });

      const primary = await startVsrServer((frame, socket) => {
        primarySockets.add(socket);
        if (primaryDead) {
          socket.destroy();
          return;
        }
        const operation = frame.readUInt8(REQUEST_OFFSET.operation);
        if (operation === Operation.Register) {
          socket.write(replyFrame(Operation.Register, registerReplyBody()));
          return;
        }
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (code === COMMAND_CODE.GetClusterMetadata) {
          // The primary leads, so the login settles here and the roster is
          // only remembered, not acted on, until the node dies.
          socket.write(replyFrame(
            Operation.NonReplicated,
            twoNodeMetadataBody(survivor.port, primary.port)
          ));
          return;
        }
        socket.write(replyFrame(operation));
      });

      const config: ClientConfig = {
        ...vsrConfig(primary.port),
        reconnect: { enabled: true, interval: 1, maxRetries: 3 }
      };
      const client = new CommandResponseStream(config);
      try {
        await client.authenticate(config.credentials);
        await client.sendCommand(60_021, Buffer.alloc(0));
        assert.ok(
          primary.frames.some(
            (frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_021
          ),
          'the live primary answered the first command'
        );

        primaryDead = true;
        for (const socket of primarySockets)
          socket.destroy();
        await primary.close();

        // The attempt in flight when the socket died is allowed to fail; the
        // one after it has to land on the survivor. Two attempts, not a
        // polling loop: the comment above promises at most one failed
        // submission, and a loop of twenty would pass with nineteen failures.
        let resumed = false;
        let lastError: unknown;
        for (let attempt = 0; attempt < 2 && !resumed; attempt += 1) {
          try {
            await client.sendCommand(60_021, Buffer.alloc(0));
            resumed = true;
          } catch (error) {
            lastError = error;
          }
        }
        assert.ok(resumed, `the client never resumed: ${String(lastError)}`);

        const operations = survivor.frames.map(
          (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
        );
        assert.ok(
          operations.includes(Operation.Register),
          'the client signed in again on the survivor'
        );
        assert.ok(
          survivor.frames.some(
            (frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_021
          ),
          'the command landed on the survivor the roster named'
        );
      } finally {
        client.destroy();
        await survivor.close();
      }
    });

  it('keeps a single-node login on its node', async () => {
    const server = await startVsrServer(
      (frame, socket) => singleNodeHandler(server.port)(frame, socket)
    );
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.authenticate(vsrConfig(server.port).credentials);

      const operations = server.frames.map(
        (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
      );
      assert.deepEqual(operations, [
        Operation.Register,
        Operation.NonReplicated
      ]);
      assert.equal(
        server.frames[1].readUInt32LE(REQUEST_OFFSET.reserved),
        COMMAND_CODE.GetClusterMetadata
      );
      assert.equal(client.isAuthenticated, true);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('polls a leaderless roster before redirecting to the elected leader',
    async () => {
      const leader = await startVsrServer(
        (frame, socket) => singleNodeHandler(leader.port)(frame, socket)
      );
      let rosterReads = 0;
      const follower = await startVsrServer((frame, socket) => {
        const operation = frame.readUInt8(REQUEST_OFFSET.operation);
        if (operation === Operation.Register) {
          socket.write(replyFrame(Operation.Register, registerReplyBody()));
          return;
        }
        rosterReads += 1;
        // The first answer is mid-election: neither node holds the leader role.
        socket.write(replyFrame(
          Operation.NonReplicated,
          rosterReads === 1
            ? twoNodeMetadataBody(follower.port, leader.port, 1)
            : twoNodeMetadataBody(follower.port, leader.port)
        ));
      });
      const client = new CommandResponseStream(vsrConfig(follower.port));
      compressLeaderlessPoll(client, 1_000);
      try {
        await client.authenticate(vsrConfig(follower.port).credentials);

        assert.equal(rosterReads, 2);
        const leaderOperations = leader.frames.map(
          (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
        );
        assert.deepEqual(leaderOperations, [
          Operation.Register,
          Operation.NonReplicated
        ]);
        assert.equal(client.isAuthenticated, true);
      } finally {
        client.destroy();
        await leader.close();
        await follower.close();
      }
    });

  it('keeps a login alive when the session dies mid-poll', async () => {
    let rosterReads = 0;
    const server = await startVsrServer((frame, socket) => {
      const operation = frame.readUInt8(REQUEST_OFFSET.operation);
      if (operation === Operation.Register) {
        socket.write(replyFrame(Operation.Register, registerReplyBody()));
        return;
      }
      rosterReads += 1;
      // The roster stays leaderless and the session dies in the same breath.
      // Polling on without a session would re-enter authentication, which
      // awaits the login being settled, and the login would never return.
      socket.write(Buffer.concat([
        replyFrame(
          Operation.NonReplicated,
          twoNodeMetadataBody(server.port, server.port, 1)
        ),
        evictionFrame(EvictionReason.NoSession)
      ]));
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    compressLeaderlessPoll(client, 1_000);
    try {
      const outcome = await Promise.race([
        client.authenticate(vsrConfig(server.port).credentials)
          .then(() => 'authenticated'),
        // Unreferenced so a passing run is not held open by the stall timer.
        new Promise((resolve) => {
          setTimeout(() => resolve('stalled'), 500).unref();
        })
      ]);

      assert.equal(outcome, 'authenticated');
      assert.equal(rosterReads, 1);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('keeps a login on its node when no leader appears in the budget',
    async () => {
      const unavailable = await startVsrServer(() => {});
      const unavailablePort = unavailable.port;
      await unavailable.close();
      // The roster marks its leader-role node unhealthy on a dead port, so a
      // redirect would fail the dial instead of passing unnoticed.
      const server = await startVsrServer((frame, socket) => {
        const operation = frame.readUInt8(REQUEST_OFFSET.operation);
        if (operation === Operation.Register) {
          socket.write(replyFrame(Operation.Register, registerReplyBody()));
          return;
        }
        socket.write(replyFrame(
          Operation.NonReplicated,
          twoNodeMetadataBody(server.port, unavailablePort, 0, 1)
        ));
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      compressLeaderlessPoll(client, 0);
      try {
        await client.authenticate(vsrConfig(server.port).credentials);

        const operations = server.frames.map(
          (frame) => frame.readUInt8(REQUEST_OFFSET.operation)
        );
        assert.deepEqual(operations, [
          Operation.Register,
          Operation.NonReplicated
        ]);
        assert.equal(client.isAuthenticated, true);
      } finally {
        client.destroy();
        await server.close();
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

  it('re-issues every request refused by a demoted node, not just the first',
    async () => {
      // One demotion, several refused requests: each of them re-checking on
      // its own would move the client once per request, and the first
      // redirect's drop would fail the others' roster reads - reporting a
      // refusal they never had to.
      const leader = await startVsrServer((frame, socket) => {
        singleNodeHandler(leader.port)(frame, socket);
      });
      // Leader at login, so the settlement leaves the client here, then
      // demoted: the refusals below are what tells the client to look again.
      let demotedYet = false;
      const demoted = await startVsrServer((frame, socket) => {
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (code === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(
            Operation.NonReplicated,
            demotedYet
              ? twoNodeMetadataBody(demoted.port, leader.port)
              : twoNodeMetadataBody(leader.port, demoted.port)
          ));
          return;
        }
        if (code === 60_032) {
          socket.write(replyFrame(Operation.NonReplicated, Buffer.alloc(0), 58));
          return;
        }
        singleNodeHandler(demoted.port)(frame, socket);
      });

      const client = new CommandResponseStream(vsrConfig(demoted.port));
      try {
        await client.authenticate(vsrConfig(demoted.port).credentials);
        demotedYet = true;
        const rosterReadsBefore = demoted.frames.filter(
          (frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) ===
            COMMAND_CODE.GetClusterMetadata
        ).length;

        // Two commands, both refused by the demoted node: one re-check between
        // them, and both answered on the node it moved to.
        const answers = await Promise.all([
          client.sendCommand(60_032, Buffer.alloc(0)),
          client.sendCommand(60_032, Buffer.alloc(0))
        ]);

        assert.deepEqual(answers.map((answer) => answer.status), [0, 0]);
        const rosterReads = demoted.frames.filter(
          (frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) ===
            COMMAND_CODE.GetClusterMetadata
        ).length - rosterReadsBefore;
        assert.equal(rosterReads, 1,
          'each refusal re-read the roster on its own'
        );
        const reissued = leader.frames.filter(
          (frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_032
        ).length;
        assert.equal(reissued, 2,
          'a refused command was not re-issued on the node the move landed on'
        );
        const connection = (client as unknown as {
          connection: { isConnectedTo: (host: string, port: number) => boolean }
        }).connection;
        assert.equal(connection.isConnectedTo('127.0.0.1', leader.port), true);
      } finally {
        client.destroy();
        await leader.close();
        await demoted.close();
      }
    }
  );

  it('walks past two refusing replicas to the partition primary',
    async () => {
      const third = await startVsrServer((frame, socket) => {
        singleNodeHandler(third.port)(frame, socket);
      });
      const second = await startVsrServer((frame, socket) => {
        const operation = frame.readUInt8(REQUEST_OFFSET.operation);
        if (operation === Operation.Register) {
          socket.write(replyFrame(Operation.Register, registerReplyBody()));
          return;
        }
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_040) {
          socket.write(replyFrame(Operation.NonReplicated, Buffer.alloc(0), 58));
          return;
        }
        socket.write(replyFrame(operation));
      });
      const metadataLeader = await startVsrServer((frame, socket) => {
        const operation = frame.readUInt8(REQUEST_OFFSET.operation);
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (operation === Operation.Register) {
          socket.write(replyFrame(Operation.Register, registerReplyBody()));
          return;
        }
        if (code === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(
            Operation.NonReplicated,
            threeNodeMetadataBody(
              metadataLeader.port,
              second.port,
              third.port
            )
          ));
          return;
        }
        if (code === 60_040) {
          socket.write(replyFrame(Operation.NonReplicated, Buffer.alloc(0), 58));
          return;
        }
        socket.write(replyFrame(operation));
      });
      const client = new CommandResponseStream(vsrConfig(metadataLeader.port));
      try {
        await client.authenticate(vsrConfig(metadataLeader.port).credentials);

        const response = await client.sendCommand(60_040, Buffer.alloc(0), {
          deadline: Date.now() + 15_000
        });

        assert.equal(response.status, 0);
        assert.ok(
          second.frames.some((frame) =>
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_040),
          'the roster walk skipped the second replica'
        );
        assert.ok(
          third.frames.some((frame) =>
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_040),
          'the roster walk never reached the partition primary'
        );
      } finally {
        client.destroy();
        await metadataLeader.close();
        await second.close();
        await third.close();
      }
    }
  );

  it('holds a queued command instead of writing it to the node being left',
    async () => {
      // A refusal sends its caller to re-read the roster, and the drain that
      // handed it out keeps going. Written in that window, the next queued
      // command goes to the socket the move is about to replace: in flight when
      // that happens, it dies with a lost-connection error nobody can act on
      // instead of being re-issued on the node the move lands on.
      const leader = await startVsrServer((frame, socket) => {
        singleNodeHandler(leader.port)(frame, socket);
      });
      let demotedYet = false;
      const demoted = await startVsrServer((frame, socket) => {
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (code === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(
            Operation.NonReplicated,
            demotedYet
              ? twoNodeMetadataBody(demoted.port, leader.port)
              : twoNodeMetadataBody(leader.port, demoted.port)
          ));
          return;
        }
        if (code === 60_037) {
          socket.write(replyFrame(Operation.NonReplicated, Buffer.alloc(0), 58));
          return;
        }
        if (code === 60_038) {
          // Accepted and never answered: a command written here is stuck until
          // the move replaces the socket under it.
          return;
        }
        singleNodeHandler(demoted.port)(frame, socket);
      });

      const client = new CommandResponseStream(vsrConfig(demoted.port));
      try {
        await client.authenticate(vsrConfig(demoted.port).credentials);
        demotedYet = true;

        // The second command is queued while the first is in flight, which is
        // where a command caught in a move comes from.
        const refused = client.sendCommand(60_037, Buffer.alloc(0));
        const behind = client.sendCommand(60_038, Buffer.alloc(0));
        refused.catch(() => undefined);
        behind.catch(() => undefined);

        const settled = await Promise.race([
          Promise.all([refused, behind]).then(() => 'answered'),
          new Promise((resolve) => {
            setTimeout(() => resolve('stalled'), 10_000).unref();
          })
        ]);
        assert.equal(settled, 'answered',
          'the command behind the refusal went out on the node being left'
        );
        assert.ok(
          !demoted.frames.some((frame) =>
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_038),
          'the command behind the refusal was written to the node being left'
        );
        assert.ok(
          leader.frames.some((frame) =>
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_038),
          'the command behind the refusal never reached the node moved to'
        );
      } finally {
        client.destroy();
        await leader.close();
        await demoted.close();
      }
    }
  );

  it('re-issues a command queued behind a leader move instead of failing it',
    async () => {
      // A move replaces the socket, which looks like a drop to everything
      // waiting in the queue. Nothing queued was written, though, so it belongs
      // on the node the client moves to rather than in a lost-connection error
      // the caller can do nothing about.
      const leader = await startVsrServer((frame, socket) => {
        singleNodeHandler(leader.port)(frame, socket);
      });
      const demoted = await startVsrServer((frame, socket) => {
        singleNodeHandler(demoted.port)(frame, socket);
      });

      const client = new CommandResponseStream(vsrConfig(demoted.port));
      try {
        await client.authenticate(vsrConfig(demoted.port).credentials);

        // Parked the way a command is while something else holds the queue.
        const queued = new Promise<CommandResponse>((resolve, reject) => {
          execQueue(client).push({
            command: 60_034,
            payload: Buffer.alloc(0),
            handleResponse: true,
            deadline: Date.now() + 30_000,
            resolve,
            reject
          });
        });

        await connectionOf(client).redirect('127.0.0.1', leader.port);
        await queued;

        const landedOnLeader = leader.frames.some(
          (frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_034
        );
        assert.ok(landedOnLeader,
          'the queued command never reached the node the client moved to'
        );
      } finally {
        client.destroy();
        await leader.close();
        await demoted.close();
      }
    }
  );

  it('surfaces the refusal rather than a timeout when the budget runs out',
    async () => {
      const server = await startVsrServer((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_036) {
          socket.write(replyFrame(Operation.NonReplicated, Buffer.alloc(0), 58));
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      const realNow = Date.now;
      try {
        await client.authenticate(vsrConfig(server.port).credentials);
        // The request's budget, the first exchange, the window that hands the
        // refusal out, and then a clock 10ms short of the deadline: too little
        // to carry another attempt, so the caller has to see the answer the
        // server gave rather than the timeout a doomed re-issue would produce.
        const times = [0, 1, 2_001, 29_990];
        Date.now = () => times.shift() ?? 30_050;

        await assert.rejects(
          () => client.sendCommand(60_036, Buffer.alloc(0)),
          (error: unknown) =>
            error instanceof ResponseError &&
            error.commandCode === 60_036 &&
            error.errorCode === 58
        );
      } finally {
        Date.now = realNow;
        client.destroy();
        await server.close();
      }
    }
  );

  it('does not suppress leader settlement after a failed roster walk',
    async () => {
      const server = await startVsrServer(
        (frame, socket) => singleNodeHandler(server.port)(frame, socket)
      );
      const client = new CommandResponseStream(vsrConfig(server.port));
      try {
        await client.authenticate(vsrConfig(server.port).credentials);
        const routing = client as unknown as {
          walkSettleSuppressed: boolean,
          _followLeaderMove: (walkPastLeader: boolean) => Promise<unknown>,
          connection: {
            nextRosterEndpoint: () => { host: string, port: number },
            redirect: (host: string, port: number) => Promise<void>
          }
        };
        routing.connection.nextRosterEndpoint = () => ({
          host: '127.0.0.1',
          port: server.port + 1
        });
        routing.connection.redirect = async () => {
          throw new Error('redirect failed');
        };

        assert.deepEqual(await routing._followLeaderMove(true), {
          endpoint: { host: '127.0.0.1', port: server.port + 1 },
          moved: false
        });
        assert.equal(routing.walkSettleSuppressed, false,
          'a failed walk leaked its one-shot suppression into a later login'
        );
      } finally {
        client.destroy();
        await server.close();
      }
    }
  );

  it('surfaces the refusal when the move left too little of the budget',
    async () => {
      // The move itself costs budget: a roster read, an election it waited out,
      // a redial. What is left can be positive and still too small to carry
      // another exchange, and re-issued into it the request times out -- the
      // caller then sees a timeout where the answer was "not admitted".
      const leader = await startVsrServer((frame, socket) => {
        singleNodeHandler(leader.port)(frame, socket);
      });
      let demotedYet = false;
      const demoted = await startVsrServer((frame, socket) => {
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (code === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(
            Operation.NonReplicated,
            demotedYet
              ? twoNodeMetadataBody(demoted.port, leader.port)
              : twoNodeMetadataBody(leader.port, demoted.port)
          ));
          return;
        }
        if (code === 60_039) {
          socket.write(replyFrame(Operation.NonReplicated, Buffer.alloc(0), 58));
          return;
        }
        singleNodeHandler(demoted.port)(frame, socket);
      });

      const client = new CommandResponseStream(vsrConfig(demoted.port));
      const realNow = Date.now;
      let offset = 0;
      try {
        await client.authenticate(vsrConfig(demoted.port).credentials);
        demotedYet = true;

        const deadline = realNow() + 30_000;
        const connection = connectionOf(client);
        const move = connection.redirect.bind(connection);
        connection.redirect = async (host: string, port: number) => {
          await move(host, port);
          // 30ms of budget left the moment the client lands: positive, and
          // below the interval one exchange needs.
          offset = deadline - realNow() - 30;
        };
        Date.now = () => realNow() + offset;

        await assert.rejects(
          () => client.sendCommand(60_039, Buffer.alloc(0), { deadline }),
          (error: unknown) =>
            error instanceof ResponseError &&
            error.commandCode === 60_039 &&
            error.errorCode === 58
        );
        assert.ok(
          !leader.frames.some((frame) =>
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_039),
          'the request was re-issued into a budget too small to answer it'
        );
      } finally {
        Date.now = realNow;
        client.destroy();
        await leader.close();
        await demoted.close();
      }
    }
  );

  it('paces the re-issues while the roster still names this node',
    async () => {
      // Re-issuing is right, spinning is not: the in-connection replay window
      // belongs to the request's budget and is spent after the first pass, so
      // without a wait the client would hammer the node for the whole budget.
      let refusals = 0;
      const server = await startVsrServer((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_035) {
          refusals += 1;
          socket.write(replyFrame(Operation.NonReplicated, Buffer.alloc(0), 58));
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      try {
        await client.authenticate(vsrConfig(server.port).credentials);

        const pending = client.sendCommand(60_035, Buffer.alloc(0));
        pending.catch(() => undefined);
        await new Promise((resolve) => {
          setTimeout(resolve, 5_000).unref();
        });

        // The first 2s window replays on the connection at its own interval;
        // every window after it costs one refusal per pace.
        assert.ok(refusals < 200,
          `the re-issues were not paced: ${refusals} refusals in 5s`
        );
      } finally {
        client.destroy();
        await server.close();
      }
    }
  );

  it('keeps re-issuing a not-admitted request while the roster still names this node',
    async () => {
      const server = await startVsrServer((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_031) {
          socket.write(replyFrame(Operation.NonReplicated, Buffer.alloc(0), 58));
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      try {
        await client.authenticate(vsrConfig(server.port).credentials);

        // A refusal the roster cannot explain is a wait, not a verdict: an
        // election may still be in flight, so the request keeps going for its
        // whole budget instead of failing after the first re-check window.
        const pending = client.sendCommand(60_031, Buffer.alloc(0));
        const outcome = await Promise.race([
          pending.then(() => 'answered', () => 'gave up'),
          new Promise((resolve) => {
            setTimeout(() => resolve('still trying'), 4_000).unref();
          })
        ]);

        assert.equal(outcome, 'still trying');
      } finally {
        client.destroy();
        await server.close();
      }
    }
  );

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
