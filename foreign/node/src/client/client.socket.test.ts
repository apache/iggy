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
import { performance } from 'node:perf_hooks';
import type { AddressInfo, Socket } from 'node:net';
import { createServer, type Server } from 'node:net';
import type { Readable } from 'node:stream';
import { describe, it, type TestContext } from 'node:test';
import { createServer as createTlsServer } from 'node:tls';
import { groupConsumerStream, singleConsumerStream } from '../stream/consumer-stream.js';
import { COMMAND_CODE } from '../wire/command.code.js';
import { LEAVE_GROUP } from '../wire/consumer-group/leave-group.command.js';
import { LOGIN, LOGIN_WITH_TOKEN, LOGOUT } from '../wire/index.js';
import { UPDATE_USER } from '../wire/user/update-user.command.js';
import { UPDATE_TOPIC } from '../wire/topic/update-topic.command.js';
import { UPDATE_STREAM } from '../wire/stream/update-stream.command.js';
import { OptionsScope } from '../wire/system/describe-options.command.js';
import { CHANGE_PASSWORD } from '../wire/user/change-password.command.js';
import { POLL_MESSAGES, pollMessages } from '../wire/message/poll-messages.command.js';
import { SEND_MESSAGES, sendMessages } from '../wire/message/send-messages.command.js';
import { GET_OFFSET } from '../wire/offset/get-offset.command.js';
import { STORE_OFFSET } from '../wire/offset/store-offset.command.js';
import { DELETE_OFFSET } from '../wire/offset/delete-offset.command.js';
import { serializePartitionRoute } from '../wire/partition/partition-route.js';
import { Partitioning } from '../wire/message/partitioning.utils.js';
import { PollingStrategy } from '../wire/message/poll.utils.js';
import { ConsumerKind } from '../wire/offset/offset.utils.js';
import { readWireName } from '../wire/vsr/index.js';
import { BIND_SECRET_BYTES } from '../wire/vsr/register.js';
import { ResponseError } from '../wire/error.utils.js';
import {
  Command,
  EVICTION_OFFSET,
  EvictionReason,
  HEADER_SIZE,
  REPLY_OFFSET,
  REQUEST_OFFSET
} from '../wire/vsr/header.js';
import { Operation, operationForCode } from '../wire/vsr/operation.js';
import { VsrEvictionError } from '../wire/vsr/reply.js';
import { IggyConnection, type LinkAttempt } from './client.connection.js';
import { OPERATION_TIMEOUT_MS } from './client.operation.js';
import {
  CommandResponseStream, LaneBlockedError, NotConnectedError, OperationNotRetainedError, OperationUncertainError
} from './client.socket.js';
import type { ClientConfig } from './client.type.js';
import { SimpleClient } from './client.js';

const TEST_SESSION = 42n;
const HISTORY_UNAVAILABLE = 87;
const LIFECYCLE_BUSY = 88;
const TRANSIENT_NOT_COMMITTED = 57;
const TRANSIENT_NOT_ACCEPTED = 58;
const INVALID_CREDENTIALS = 42;
const STALE_CLIENT = 30;
const UNAUTHENTICATED = 40;
const UNAUTHORIZED = 41;
const WAIT_LIMIT_MS = 2_000;
const PARTITION_NOT_FOUND = 3007;
/** How long a test server holds a logout reply while the client goes on. */
const LOGOUT_DELAY_MS = 50;
/** The error of a request whose link the server closed. */
const CLOSED_LINK = 'replica connection closed';
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
/** Answers a frame in place of the default handler, and says whether it did. */
type FrameOverride = (frame: Buffer, socket: Socket) => boolean;

type VsrTestServer = {
  port: number,
  frames: Buffer[],
  sockets: Set<Socket>,
  close: () => Promise<void>,
  /** Listens again on the same port after `close`. */
  reopen: () => Promise<void>
};

/** Loopback server speaking just enough VSR framing for the client tests. */
const startVsrServer = async (
  handler: FrameHandler,
  transport: 'TCP' | 'TLS' = 'TCP'
): Promise<VsrTestServer> => {
  const frames: Buffer[] = [];
  const sockets = new Set<Socket>();
  const handleConnection = (socket: Socket) => {
    sockets.add(socket);
    socket.once('close', () => sockets.delete(socket));
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
  const port = (server.address() as AddressInfo).port;
  return {
    port, frames, sockets,
    close: () => new Promise((resolve) => server.close(() => resolve())),
    reopen: async () => {
      server.listen(port, '127.0.0.1');
      await once(server, 'listening');
    }
  };
};

const replyFrame = (
  request: Buffer | undefined,
  operation: number,
  body: Buffer = Buffer.alloc(0),
  status = 0
): Buffer => {
  const frame = Buffer.alloc(HEADER_SIZE + body.length);
  frame.writeUInt32LE(frame.length, REPLY_OFFSET.size);
  frame.writeUInt8(Command.Reply, REPLY_OFFSET.command);
  frame.writeUInt8(operation, REPLY_OFFSET.operation);
  frame.writeUInt32LE(status, REPLY_OFFSET.status);
  if (request) {
    frame.writeBigUInt64LE(request.readBigUInt64LE(REQUEST_OFFSET.request), REPLY_OFFSET.request);
    request.copy(frame, REPLY_OFFSET.client, REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16);
    request.copy(frame, REPLY_OFFSET.requestChecksum, REQUEST_OFFSET.requestChecksum, REQUEST_OFFSET.requestChecksum + 16);
    frame.writeBigUInt64LE(request.readBigUInt64LE(REQUEST_OFFSET.partitionIncarnation), REPLY_OFFSET.partitionIncarnation);
  }
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

const bindingIdentity = (frame: Buffer): Buffer => frame.subarray(-64, -32);

const assertSharedBinding = (register: Buffer, binding: Buffer): void => {
  assert.deepEqual(bindingIdentity(binding).subarray(0, 16),
    register.subarray(REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16));
  assert.equal(bindingIdentity(binding).readBigUInt64LE(16), TEST_SESSION);
  const sdkName = readWireName(register, HEADER_SIZE + 4);
  const sdkVersion = readWireName(register, sdkName.next);
  assert.deepEqual(binding.subarray(-BIND_SECRET_BYTES),
    register.subarray(sdkVersion.next, sdkVersion.next + BIND_SECRET_BYTES));
};

const registerReplyBody = (session = TEST_SESSION): Buffer => {
  const serverVersion = Buffer.from('0.0.0');
  const body = Buffer.alloc(4 + 17 + serverVersion.length);
  body.writeUInt32LE(0, 0);
  body.writeUInt32LE(7, 4);
  body.writeBigUInt64LE(session, 8);
  body.writeUInt32LE(0, 16);
  body.writeUInt8(serverVersion.length, 20);
  serverVersion.copy(body, 21);
  return body;
};

const metadataBody = (nodes: { port: number, replicaId: number, role?: number, status?: number }[]): Buffer => {
  const name = Buffer.from('iggy-cluster');
  const header = Buffer.alloc(4 + name.length + 4);
  header.writeUInt32LE(name.length, 0);
  name.copy(header, 4);
  header.writeUInt32LE(nodes.length, 4 + name.length);
  return Buffer.concat([header, ...nodes.map(({ port, replicaId, role = 1, status = 0 }) => {
    const nodeName = Buffer.from(`iggy-node-${replicaId}`);
    const ip = Buffer.from('127.0.0.1');
    const encoded = Buffer.alloc(1 + 4 + nodeName.length + 4 + ip.length + 8 + 2);
    let offset = 0;
    encoded.writeUInt8(replicaId, offset); offset += 1;
    encoded.writeUInt32LE(nodeName.length, offset); offset += 4;
    nodeName.copy(encoded, offset); offset += nodeName.length;
    encoded.writeUInt32LE(ip.length, offset); offset += 4;
    ip.copy(encoded, offset); offset += ip.length;
    encoded.writeUInt16LE(port, offset); offset += 8;
    encoded.writeUInt8(role, offset); offset += 1;
    encoded.writeUInt8(status, offset);
    return encoded;
  })]);
};

const singleNodeMetadataBody = (port: number): Buffer => metadataBody([{ port, replicaId: 0, role: 0 }]);

const twoNodeMetadataBody = (followerPort: number, leaderPort: number, leaderRole = 0, leaderStatus = 0): Buffer =>
  metadataBody([{ port: leaderPort, replicaId: 0, role: leaderRole, status: leaderStatus },
    { port: followerPort, replicaId: 7 }]);

const threeNodeMetadataBody = (firstPort: number, secondPort: number, thirdPort: number): Buffer =>
  metadataBody([{ port: firstPort, replicaId: 0, role: 0 }, { port: secondPort, replicaId: 7 },
    { port: thirdPort, replicaId: 9 }]);

/** Register, metadata, and echo behavior of a healthy single VSR node. */
const singleNodeHandler = (port: number): FrameHandler =>
  (frame, socket) => {
    const operation = frame.readUInt8(REQUEST_OFFSET.operation);
    if (operation === Operation.Register) {
      socket.write(replyFrame(frame, Operation.Register, registerReplyBody()));
      return;
    }
    const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
    if (code === COMMAND_CODE.BindSession) {
      socket.write(replyFrame(frame, Operation.NonReplicated, registerReplyBody().subarray(4)));
      return;
    }
    if (code === COMMAND_CODE.GetClusterMetadata) {
      socket.write(
        replyFrame(frame, Operation.NonReplicated, singleNodeMetadataBody(port))
      );
      return;
    }
    if (code === COMMAND_CODE.GetPartitionRoute) {
      socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(frame.readBigUInt64LE(REQUEST_OFFSET.minimumMetadataOp) || 1n, 0n, 0)));
      return;
    }
    socket.write(replyFrame(frame, operation));
  };

const vsrConfig = (port: number): ClientConfig => ({
  transport: 'TCP',
  options: { host: '127.0.0.1', port },
  credentials: { username: 'iggy', password: 'iggy' },
  reconnect: { enabled: false, interval: 100, maxRetries: 1 }
});

const groupPollPayload = (autocommit = true, partitionId = 0): Buffer =>
  POLL_MESSAGES.serialize({
    streamId: 1,
    topicId: 1,
    consumer: { kind: ConsumerKind.Group, id: 1 },
    partitionId,
    pollingStrategy: PollingStrategy.Next,
    count: 1,
    autocommit
  });

const routingBody = (watermark: bigint, incarnation = 0n,
  primaryReplica = 7): Buffer => {
  const route = Buffer.alloc(25);
  route.writeBigUInt64LE(incarnation, 0);
  route.writeBigUInt64LE(watermark, 8);
  route.writeUInt32LE(Number(watermark), 16);
  route.writeUInt32LE(4, 20);
  route.writeUInt8(primaryReplica, 24);
  return route;
};

const countCommand = (server: VsrTestServer, code: number): number =>
  server.frames.filter((frame) =>
    frame.readUInt8(REQUEST_OFFSET.operation) === Operation.NonReplicated &&
    frame.readUInt32LE(REQUEST_OFFSET.reserved) === code).length;

const registerCredentialsBody = (frame: Buffer): Buffer => {
  const sdkName = readWireName(frame, HEADER_SIZE + 4);
  const sdkVersion = readWireName(frame, sdkName.next);
  return frame.subarray(sdkVersion.next + BIND_SECRET_BYTES);
};

const startPollCluster = async (
  primaryOverride?: (frame: Buffer, socket: Socket) => boolean,
  coordinatorOverride?: (frame: Buffer, socket: Socket) => boolean,
  transport: 'TCP' | 'TLS' = 'TCP',
  overrides: Partial<ClientConfig> = {}
) => {
  const primary = await startVsrServer((frame, socket) => {
    if (primaryOverride?.(frame, socket))
      return;
    if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.PollMessages) {
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.from('primary')));
      return;
    }
    singleNodeHandler(primary.port)(frame, socket);
  }, transport);
  const coordinator = await startVsrServer((frame, socket) => {
    if (coordinatorOverride?.(frame, socket))
      return;
    const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
    if (code === COMMAND_CODE.GetClusterMetadata) {
      socket.write(replyFrame(frame, Operation.NonReplicated,
        twoNodeMetadataBody(primary.port, coordinator.port)));
      return;
    }
    if (code === COMMAND_CODE.GetPartitionRoute) {
      socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(frame.readBigUInt64LE(REQUEST_OFFSET.minimumMetadataOp) || 1n)));
      return;
    }
    singleNodeHandler(coordinator.port)(frame, socket);
  }, transport);
  const config = {
    ...vsrConfig(coordinator.port), transport, heartbeatInterval: 0,
    options: {
      ...vsrConfig(coordinator.port).options,
      ...(transport === 'TLS' ? { ca: TLS_CA_CERTIFICATE, servername: 'localhost' } : {})
    },
    ...overrides
  };
  const client = new CommandResponseStream(config);
  return {
    client, primary, coordinator,
    close: async () => {
      client.destroy();
      await Promise.all([coordinator.close(), primary.close()]);
    }
  };
};

const sendTo = (partition: Partitioning): Buffer => SEND_MESSAGES.serialize({
  streamId: 1, topicId: 2, messages: [{ payload: 'message' }], partition
});

const explicitSend = sendTo(Partitioning.PartitionId(3));

const storeOffsetPayload = STORE_OFFSET.serialize({
  streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Single, id: 1 }, partitionId: 3, offset: 5n
});

const deleteOffsetPayload = DELETE_OFFSET.serialize({
  streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Single, id: 1 }, partitionId: 3
});

const CALLER_CONTEXT = { incarnation: 7n, ownerGeneration: 8n, metadataOp: 9n };

const stampedContext = (frame: Buffer) => ({
  incarnation: frame.readBigUInt64LE(REQUEST_OFFSET.partitionIncarnation),
  ownerGeneration: frame.readBigUInt64LE(REQUEST_OFFSET.ownerGeneration),
  metadataOp: frame.readBigUInt64LE(REQUEST_OFFSET.minimumMetadataOp)
});

const framesOf = (server: VsrTestServer, operation: number): Buffer[] =>
  server.frames.filter((frame) => frame.readUInt8(REQUEST_OFFSET.operation) === operation);

/** Body of a committed metadata reply whose single result is `code`. */
const metadataResult = (code: number): Buffer => {
  const body = Buffer.alloc(12);
  body.writeUInt32LE(1, 0);
  body.writeUInt32LE(code, 8);
  return body;
};

const RESOLVED_UPDATES = new Map<number, number>([
  [OptionsScope.Topic, Operation.UpdateTopicResolved],
  [OptionsScope.Stream, Operation.UpdateStreamResolved],
  [OptionsScope.User, Operation.UpdateUserResolved]
]);

/** The operation a committed reply carries: the primary resolves an update by its scope. */
const committedOperation = (frame: Buffer): number => {
  const operation = frame[REQUEST_OFFSET.operation];
  return operation === Operation.UpdateResource
    ? RESOLVED_UPDATES.get(frame[HEADER_SIZE]) ?? operation : operation;
};

/** A single node that also commits offset writes. */
const offsetCommittingHandler = (port: number): FrameHandler => (frame, socket) => {
  const operation = frame.readUInt8(REQUEST_OFFSET.operation);
  if (operation === Operation.StoreConsumerOffset || operation === Operation.DeleteConsumerOffset) {
    socket.write(replyFrame(frame, operation, metadataResult(0)));
    return;
  }
  singleNodeHandler(port)(frame, socket);
};

const contextServer = async (state: {
  live: bigint, refuseAll: boolean, contextStatus: number, onSend?: () => void
}) => {
  const server: VsrTestServer = await startVsrServer((frame, socket) => {
    if (frame.readUInt8(REQUEST_OFFSET.operation) === Operation.SendMessages) {
      state.onSend?.();
      const sent = frame.readBigUInt64LE(REQUEST_OFFSET.partitionIncarnation);
      socket.write(replyFrame(frame, Operation.SendMessages, Buffer.alloc(0),
        !state.refuseAll && sent === state.live ? 0 : HISTORY_UNAVAILABLE));
      return;
    }
    if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
      const context = routingBody(0n, 0n, 0);
      context.writeBigUInt64LE(state.live, 0);
      socket.write(replyFrame(frame, Operation.NonReplicated, context, state.contextStatus));
      return;
    }
    singleNodeHandler(server.port)(frame, socket);
  });
  return server;
};

describe('unified partition routing', () => {
  const offset = { streamId: 1, topicId: 2, partitionId: 3,
    consumer: { kind: ConsumerKind.Single, id: 1 } };
  const poll = POLL_MESSAGES.serialize({ ...offset, count: 1,
    pollingStrategy: PollingStrategy.Next, autocommit: false });
  const requests = [
    [COMMAND_CODE.SendMessages, explicitSend, Operation.SendMessages],
    [COMMAND_CODE.PollMessages, poll, Operation.NonReplicated],
    [COMMAND_CODE.GetOffset, GET_OFFSET.serialize(offset), Operation.NonReplicated],
    [COMMAND_CODE.StoreOffset, storeOffsetPayload, Operation.StoreConsumerOffset],
    [COMMAND_CODE.DeleteConsumerOffset, deleteOffsetPayload, Operation.DeleteConsumerOffset]
  ] as const;

  for (const [command, payload, operation] of requests) {
    it(`reroutes command ${command} after89 with the identical request`, async () => {
      const replacement = await startVsrServer((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === operation &&
            (operation !== Operation.NonReplicated || frame.readUInt32LE(REQUEST_OFFSET.reserved) === command)) {
          socket.write(replyFrame(frame, operation, operation === Operation.StoreConsumerOffset ||
            operation === Operation.DeleteConsumerOffset ? metadataResult(0) : Buffer.alloc(0)));
          return;
        }
        singleNodeHandler(replacement.port)(frame, socket);
      });
      let routes = 0;
      const cluster = await startPollCluster((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] !== operation ||
            (operation === Operation.NonReplicated && frame.readUInt32LE(REQUEST_OFFSET.reserved) !== command))
          return false;
        socket.write(replyFrame(frame, operation, Buffer.alloc(0), 89));
        return true;
      }, (frame, socket) => {
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (code === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(frame, Operation.NonReplicated,
            threeNodeMetadataBody(cluster.coordinator.port, cluster.primary.port, replacement.port)));
          return true;
        }
        if (code !== COMMAND_CODE.GetPartitionRoute)
          return false;
        routes += 1;
        socket.write(replyFrame(frame, Operation.NonReplicated,
          routingBody(BigInt(routes), BigInt(routes), routes === 1 ? 7 : 9)));
        return true;
      });
      try {
        await cluster.client.sendCommand(command, payload, { deadline: Date.now() + 1_000 });
        const matching = (server: VsrTestServer) => framesOf(server, operation).filter((frame) =>
          operation !== Operation.NonReplicated || frame.readUInt32LE(REQUEST_OFFSET.reserved) === command);
        assert.equal(matching(cluster.primary).length, 1);
        assert.equal(matching(replacement).length, 1);
        assert.deepEqual(matching(cluster.primary)[0], matching(replacement)[0]);
        assert.deepEqual(stampedContext(matching(replacement)[0]),
          { incarnation: 1n, ownerGeneration: 0n, metadataOp: 1n });
        assert.equal(routes, 2);
        assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata), 1);
        assert.equal(framesOf(cluster.coordinator, Operation.Register).length, 1);
        const initial = cluster.coordinator.frames[0];
        for (const server of [cluster.primary, replacement]) {
          const binding = server.frames.find((frame) =>
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession)!;
          assertSharedBinding(initial, binding);
        }
      } finally {
        await cluster.close();
        await replacement.close();
      }
    });
  }

  it('stamps the latest metadata acknowledgement on typed send route discovery', async () => {
    const cluster = await startPollCluster(undefined, (frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] !== Operation.CreateStream)
        return false;
      const response = replyFrame(frame, Operation.CreateStream, metadataResult(0));
      response.writeBigUInt64LE(19n, REPLY_OFFSET.commit);
      response.writeBigUInt64LE(19n, REPLY_OFFSET.op);
      response[REPLY_OFFSET.reserved] = 1;
      socket.write(response);
      return true;
    });
    try {
      await cluster.client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      await sendMessages(async () => cluster.client)({
        streamId: 1, topicId: 2, messages: [{ payload: 'balanced' }]
      });
      const lookups = cluster.coordinator.frames.filter((frame) =>
        frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute);
      assert.equal(lookups.length, 1);
      assert.equal(stampedContext(lookups[0]).metadataOp, 19n);
    } finally {
      await cluster.close();
    }
  });

  for (const command of [COMMAND_CODE.PollMessages, COMMAND_CODE.GetOffset]) {
    it(`refreshes cached history after observing a newer metadata commit for ${command}`, async () => {
      let watermark = 1n;
      let incarnation = 7n;
      const histories: bigint[] = [];
      const cluster = await startPollCluster((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== command)
          return false;
        histories.push(stampedContext(frame).incarnation);
        socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0)));
        return true;
      }, (frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.UpdateResource) {
          watermark = 19n;
          incarnation = 8n;
          const response = replyFrame(frame, Operation.UpdateUserResolved, metadataResult(0));
          response.writeBigUInt64LE(watermark, REPLY_OFFSET.commit);
          response.writeBigUInt64LE(watermark, REPLY_OFFSET.op);
          response[REPLY_OFFSET.reserved] = 1;
          socket.write(response);
          return true;
        }
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute)
          return false;
        socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(watermark, incarnation)));
        return true;
      });
      try {
        const payload = command === COMMAND_CODE.PollMessages
          ? groupPollPayload() : GET_OFFSET.serialize({ streamId: 1, topicId: 2,
            consumer: { kind: ConsumerKind.Single, id: 1 }, partitionId: 3 });
        await cluster.client.sendCommand(command, payload);
        await cluster.client.sendCommand(COMMAND_CODE.UpdateResource, Buffer.alloc(0));
        await cluster.client.sendCommand(command, payload);
        assert.deepEqual(histories, [7n, 8n]);
        assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 2);
        const lookups = cluster.coordinator.frames.filter((frame) =>
          frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute);
        assert.equal(stampedContext(lookups[1]).metadataOp, watermark);
      } finally {
        await cluster.close();
      }
    });
  }

  it('shares one route and request counter across all five partition commands', async () => {
    const cluster = await startPollCluster((frame, socket) => {
      const operation = frame[REQUEST_OFFSET.operation];
      if (operation !== Operation.StoreConsumerOffset && operation !== Operation.DeleteConsumerOffset)
        return false;
      socket.write(replyFrame(frame, operation, metadataResult(0)));
      return true;
    });
    try {
      for (const [command, payload] of requests)
        await cluster.client.sendCommand(command, payload);
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 1);
      const boundFrames = [...cluster.coordinator.frames, ...cluster.primary.frames]
        .filter((frame) => frame.readBigUInt64LE(REQUEST_OFFSET.session) === TEST_SESSION);
      const ids = boundFrames.map((frame) => frame.readBigUInt64LE(REQUEST_OFFSET.request));
      assert.equal(new Set(ids).size, ids.length);
      assert.ok(ids.every((id) => id > 0n));
      assert.equal(framesOf(cluster.primary, Operation.Register).length, 0);
    } finally {
      await cluster.close();
    }
  });
});

describe('primary auto-commit polling', () => {
  for (const transport of ['TCP', 'TLS'] as const) {
    for (const refused of [false, true]) {
      it(`preserves raw poll responses over ${transport}${refused ? ' after a refusal' : ''}`, async () => {
        let expected!: Buffer;
        let polls = 0;
        const cluster = await startPollCluster((frame, socket) => {
          if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages)
            return false;
          polls += 1;
          expected = replyFrame(frame, Operation.NonReplicated, Buffer.from('raw primary'));
          socket.write(refused && polls === 1
            ? replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 58)
            : expected);
          return true;
        }, undefined, transport);
        let resets = 0;
        cluster.client.on('sessionReset', () => { resets += 1; });
        try {
          for (let poll = 0; poll < 2; poll += 1) {
            const response = await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload(),
              { handleResponse: false });
            assert.ok(Buffer.isBuffer(response), 'raw polls must return the complete VSR frame');
            assert.deepEqual(response, expected);
          }
          assert.equal(polls, refused ? 3 : 2);
          assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), refused ? 2 : 1);
          assert.equal(countCommand(cluster.primary, COMMAND_CODE.BindSession), 1);
          assert.equal(resets, 0);
        } finally {
          await cluster.close();
        }
      });
    }
  }

  for (const transport of ['TCP', 'TLS'] as const) {
    it(`retains the coordinator session and reuses authenticated primary polls over ${transport}`, async () => {
      const cluster = await startPollCluster(undefined, undefined, transport);
      const { client, primary, coordinator } = cluster;
      let resets = 0;
      client.on('sessionReset', () => { resets += 1; });
      try {
        const first = await client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
        const second = await client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
        assert.equal(first.data.toString(), 'primary');
        assert.equal(second.data.toString(), 'primary');
        assert.equal(countCommand(coordinator, COMMAND_CODE.GetPartitionRoute), 1);
        assert.equal(countCommand(coordinator, COMMAND_CODE.PollMessages), 0);
        assert.equal(countCommand(primary, COMMAND_CODE.PollMessages), 2);
        assert.equal(countCommand(primary, COMMAND_CODE.BindSession), 1);
        assert.equal(countCommand(primary, COMMAND_CODE.GetClusterMetadata), 0,
          'auxiliary login must stay on the partition primary');
        assert.equal(primary.frames.filter((frame) =>
          frame[REQUEST_OFFSET.operation] === Operation.Register).length, 0);
        const route = coordinator.frames.find((frame) =>
          frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute)!;
        const attach = primary.frames.find((frame) =>
          frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession)!;
        assert.deepEqual(bindingIdentity(attach).subarray(0, 16),
          route.subarray(REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16));
        assert.equal(bindingIdentity(attach).readBigUInt64LE(16), TEST_SESSION);
        assert.deepEqual(attach.subarray(REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16),
          route.subarray(REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16),
          'the auxiliary must bind the coordinator identity');
        assert.equal(resets, 0);
      } finally {
        await cluster.close();
      }
    });
  }

  it('routes manual-commit polls to the primary and keeps the standalone path', async () => {
    const cluster = await startPollCluster();
    const server = await startVsrServer((frame, socket) => singleNodeHandler(server.port)(frame, socket));
    const standalone = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    try {
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload(false));
      await standalone.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.PollMessages), 0);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.PollMessages), 1);
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 1);
      assert.equal(countCommand(server, COMMAND_CODE.PollMessages), 1);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 1);
    } finally {
      standalone.destroy();
      await cluster.close();
      await server.close();
    }
  });

  it('refreshes discovery after metadata acknowledgement while reusing healthy links and ignoring partition commits', async () => {
    const cluster = await startPollCluster((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] !== Operation.SendMessages)
        return false;
      const response = replyFrame(frame, Operation.SendMessages);
      response.writeBigUInt64LE(900n, REPLY_OFFSET.commit);
      response.writeBigUInt64LE(900n, REPLY_OFFSET.op);
      response[REPLY_OFFSET.reserved] = 1;
      socket.write(response);
      return true;
    }, (frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] !== Operation.UpdateResource)
        return false;
      const response = replyFrame(frame, Operation.UpdateTopicResolved, Buffer.alloc(4));
      response.writeBigUInt64LE(15n, REPLY_OFFSET.commit);
      response.writeBigUInt64LE(15n, REPLY_OFFSET.op);
      response[REPLY_OFFSET.reserved] = 1;
      socket.write(response);
      return true;
    });
    try {
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 2);
      await cluster.client.sendCommand(COMMAND_CODE.UpdateResource, Buffer.alloc(0), { handleResponse: false });
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      const attachments = cluster.primary.frames.filter((frame) =>
        frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession);
      assert.deepEqual(attachments.map((frame) => bindingIdentity(frame).readBigUInt64LE(24)), [0n]);
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 3);
      await cluster.client.sendCommand(LOGOUT.code, LOGOUT.serialize());
      // After a logout only the caller signs in again.
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      const replacement = cluster.primary.frames.filter((frame) =>
        frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession).at(-1)!;
      assert.equal(bindingIdentity(replacement).readBigUInt64LE(24), 15n,
        'a new coordinator session must retain previously acknowledged metadata');
    } finally {
      await cluster.close();
    }
  });

  it('refreshes a not-admitted route without moving the coordinator', async () => {
    let polls = 0;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages)
        return false;
      polls += 1;
      if (polls > 1)
        return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 58));
      return true;
    });
    let resets = 0;
    cluster.client.on('sessionReset', () => { resets += 1; });
    try {
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      assert.equal(polls, 2);
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 2);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.BindSession), 1);
      assert.equal(resets, 0);
    } finally {
      await cluster.close();
    }
  });

  for (const refusing of [COMMAND_CODE.GetPartitionRoute, COMMAND_CODE.BindSession,
    COMMAND_CODE.PollMessages]) {
    it(`reports not accepted after command ${refusing} refuses the whole poll budget`, async () => {
      const refuse = (frame: Buffer, socket: Socket): boolean => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== refusing)
          return false;
        socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 58));
        return true;
      };
      const cluster = await startPollCluster(refuse, refuse);
      let resets = 0;
      cluster.client.on('sessionReset', () => { resets += 1; });
      try {
        await assert.rejects(cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload(),
          { deadline: Date.now() + 1_000 }), (error: unknown) =>
          error instanceof ResponseError && error.errorCode === 58);
        if (refusing === COMMAND_CODE.BindSession)
          assert.ok(countCommand(cluster.primary, COMMAND_CODE.BindSession) >= 2);
        else
          assert.ok(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute) >= 2);
        if (refusing === COMMAND_CODE.GetPartitionRoute)
          assert.equal(countCommand(cluster.primary, COMMAND_CODE.PollMessages), 0);
        if (refusing === COMMAND_CODE.BindSession)
          assert.equal(countCommand(cluster.primary, COMMAND_CODE.PollMessages), 0);
        assert.equal(resets, 0);
      } finally {
        await cluster.close();
      }
    });
  }

  it('refreshes the roster once when the primary has no TCP endpoint', async () => {
    const cluster = await startPollCluster(undefined, (frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetClusterMetadata)
        return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, twoNodeMetadataBody(0, cluster.coordinator.port)));
      return true;
    });
    try {
      await assert.rejects(cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload(),
        { deadline: Date.now() + 500 }),
      (error: unknown) => error instanceof ResponseError && error.errorCode === 58);
      assert.equal(cluster.primary.frames.length, 0);
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata), 2);
    } finally {
      await cluster.close();
    }
  });

  it('sends once the next route lookup replaces a route that names an unknown primary', async () => {
    const unknownReplica = 9;
    let lookups = 0;
    const answerLookup = (frame: Buffer, socket: Socket): boolean => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated,
        ++lookups === 1 ? routingBody(1n, 0n, unknownReplica) : routingBody(2n)));
      return true;
    };
    const cluster = await startPollCluster(answerLookup, answerLookup);
    try {
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      assert.equal(lookups, 2);
      // The unknown primary refreshes the roster once, which still lacks it.
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata), 2);
      assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 1);
    } finally {
      await cluster.close();
    }
  });

  for (const standalone of [false, true]) {
    for (const failure of ['empty reply', 'empty roster', 'unavailable']) {
      it(`requires valid topology after ${failure} before ${standalone ? 'standalone' : 'cluster'} polling`, async () => {
        const clusterName = Buffer.from('cluster-without-nodes');
        const emptyRoster = Buffer.alloc(4 + clusterName.length + 4);
        emptyRoster.writeUInt32LE(clusterName.length, 0);
        clusterName.copy(emptyRoster, 4);
        let rejectMetadata = true;
        let coordinatorPort = 0;
        const cluster = await startPollCluster(undefined, (frame, socket) => {
          if (standalone && frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
            socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(0n, 0n, 0)));
            return true;
          }
          if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetClusterMetadata)
            return false;
          if (rejectMetadata) {
            socket.write(replyFrame(frame, Operation.NonReplicated,
              failure === 'empty roster' ? emptyRoster : Buffer.alloc(0),
              failure === 'unavailable' ? 5 : 0));
            return true;
          }
          if (standalone) {
            socket.write(replyFrame(frame, Operation.NonReplicated, singleNodeMetadataBody(coordinatorPort)));
            return true;
          }
          return false;
        });
        coordinatorPort = cluster.coordinator.port;
        let resets = 0;
        cluster.client.on('sessionReset', () => { resets += 1; });
        try {
          await assert.rejects(cluster.client.authenticate(vsrConfig(coordinatorPort).credentials));
          await assert.rejects(cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload()));
          assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.PollMessages), 0);
          assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 0);
          assert.equal(cluster.primary.frames.length, 0);
          rejectMetadata = false;
          await cluster.client.authenticate(vsrConfig(coordinatorPort).credentials);
          await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
          rejectMetadata = true;
          const refresh = cluster.client.sendCommand(COMMAND_CODE.GetClusterMetadata, Buffer.alloc(0));
          if (failure === 'unavailable')
            await assert.rejects(refresh, (error: unknown) => error instanceof ResponseError && error.errorCode === 5);
          else
            await assert.rejects(refresh);
          const reads = countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata);
          await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
          assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata), reads);
          assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.PollMessages), standalone ? 2 : 0);
          assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 1);
          assert.equal(countCommand(cluster.primary, COMMAND_CODE.PollMessages), standalone ? 0 : 2);
          assert.equal(resets, 1);
        } finally {
          await cluster.close();
        }
      });
    }
  }

  it('refreshes a stalled auxiliary TLS dial within the shared poll budget', async () => {
    const held: Socket[] = [];
    const silent = createServer((socket) => {
      held.push(socket);
      socket.resume();
    });
    silent.listen(0, '127.0.0.1');
    await once(silent, 'listening');
    const silentPort = (silent.address() as AddressInfo).port;
    let routes = 0;
    const cluster = await startPollCluster(undefined, (frame, socket) => {
      const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
      if (code === COMMAND_CODE.GetClusterMetadata) {
        socket.write(replyFrame(frame, Operation.NonReplicated,
          threeNodeMetadataBody(cluster.coordinator.port, cluster.primary.port, silentPort)));
        return true;
      }
      if (code !== COMMAND_CODE.GetPartitionRoute)
        return false;
      routes += 1;
      socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(BigInt(routes), 0n, routes === 1 ? 9 : 7)));
      return true;
    }, 'TLS');
    let resets = 0;
    cluster.client.on('sessionReset', () => { resets += 1; });
    try {
      const response = await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload(),
        { deadline: Date.now() + 5_000 });
      assert.equal(response.data.toString(), 'primary');
      assert.equal(routes, 2);
      assert.equal(held.length, 1);
      assert.equal(held[0].destroyed, true);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.PollMessages), 1);
      assert.equal(resets, 0);
    } finally {
      await cluster.close();
      held.forEach((socket) => socket.destroy());
      await new Promise<void>((resolve) => silent.close(() => resolve()));
    }
  });

  for (const operation of [Operation.TruncatePartition, Operation.DeleteSegments]) {
    it(`raises the discovery floor for a DeleteSegments acknowledgement with operation ${operation}`, async () => {
      const cluster = await startPollCluster(undefined, (frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] !== Operation.DeleteSegments)
          return false;
        const response = replyFrame(frame, operation,
          operation === Operation.TruncatePartition ? Buffer.alloc(4) : Buffer.alloc(0));
        response.writeBigUInt64LE(50n, REPLY_OFFSET.commit);
        response.writeBigUInt64LE(50n, REPLY_OFFSET.op);
        response[REPLY_OFFSET.reserved] = 1;
        socket.write(response);
        return true;
      });
      try {
        await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
        await cluster.client.sendCommand(COMMAND_CODE.DeleteSegments, Buffer.alloc(0));
        await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
        const attachments = cluster.primary.frames.filter((frame) =>
          frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession);
        assert.deepEqual(attachments.map((frame) => bindingIdentity(frame).readBigUInt64LE(24)), [0n]);
        const lookup = cluster.coordinator.frames.filter((frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute).at(-1)!;
        assert.equal(stampedContext(lookup).metadataOp, 50n);
        assert.equal(cluster.primary.frames.filter((frame) => frame[REQUEST_OFFSET.operation] === Operation.Register).length, 0);
      } finally {
        await cluster.close();
      }
    });
  }

  it('keeps queued poll context captured and refreshes the next call after metadata acknowledgement', async () => {
    let firstPoll!: (value: { socket: Socket, frame: Buffer }) => void;
    const admitted = new Promise<{ socket: Socket, frame: Buffer }>((resolve) => { firstPoll = resolve; });
    let polls = 0;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages || ++polls !== 1)
        return false;
      firstPoll({ socket, frame });
      return true;
    }, (frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] !== Operation.UpdateResource)
        return false;
      const response = replyFrame(frame, Operation.UpdateTopicResolved, Buffer.alloc(4));
      response.writeBigUInt64LE(21n, REPLY_OFFSET.commit);
      response.writeBigUInt64LE(21n, REPLY_OFFSET.op);
      response[REPLY_OFFSET.reserved] = 1;
      socket.write(response);
      return true;
    });
    try {
      const first = cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      const { socket, frame } = await admitted;
      const second = cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      await cluster.client.sendCommand(COMMAND_CODE.UpdateResource, Buffer.alloc(0));
      assert.equal(polls, 1, 'progress polls on one partition share its mutation lane');
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.from('primary')));
      await Promise.all([first, second]);
      assert.equal(polls, 2);
      const attachments = cluster.primary.frames.filter((frame) =>
        frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession);
      assert.deepEqual(attachments.map((frame) => bindingIdentity(frame).readBigUInt64LE(24)), [0n]);
      const queued = cluster.primary.frames.filter((frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.PollMessages);
      assert.deepEqual(queued.map(stampedContext), [stampedContext(frame), stampedContext(frame)]);
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      const fresh = cluster.primary.frames.filter((frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.PollMessages).at(-1)!;
      assert.equal(stampedContext(fresh).metadataOp, 21n);
    } finally {
      await cluster.close();
    }
  });

  for (const failure of ['disconnect', 'not committed', 'timeout', 'partial reply', 'stale session', 'unauthenticated'] as const) {
    it(`replays only a refused primary poll after ${failure}`, async () => {
      let polls = 0;
      const cluster = await startPollCluster((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages)
          return false;
        polls += 1;
        if (polls > 1)
          return false;
        if (failure === 'disconnect') socket.destroy();
        if (failure === 'not committed')
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 57));
        if (failure === 'stale session')
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 30));
        if (failure === 'unauthenticated')
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 40));
        if (failure === 'partial reply')
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.from('partial')).subarray(0, HEADER_SIZE + 1));
        return true;
      }, undefined, 'TCP', { reconnect: { enabled: true, interval: 10, maxRetries: 2 } });
      let resets = 0;
      cluster.client.on('sessionReset', () => { resets += 1; });
      try {
        const first = cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload(),
          { deadline: Date.now() + 300 });
        if (failure === 'unauthenticated') {
          await first;
          assert.equal(polls, 2);
          assert.equal(resets, 0);
          assert.equal(countCommand(cluster.primary, COMMAND_CODE.BindSession), 2);
          return;
        }
        await assert.rejects(first, (error: unknown) =>
          failure === 'stale session'
            ? error instanceof ResponseError && error.errorCode === STALE_CLIENT
            : error instanceof ResponseError && error.errorCode === TRANSIENT_NOT_COMMITTED);
        assert.equal(polls, 1);
        assert.equal(resets, 0);
        if (failure === 'partial reply') {
          await cluster.client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
          assert.equal(polls, 1);
        } else {
          await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
          assert.equal(polls, 2);
          assert.equal(countCommand(cluster.primary, COMMAND_CODE.BindSession),
            failure === 'disconnect' || failure === 'stale session' ? 2 : 1);
        }
      } finally {
        await cluster.close();
      }
    });
  }

  for (const status of [30, 40]) {
    it(`rebuilds an auxiliary rejected with session status ${status} before poll admission`, async () => {
      let attachments = 0;
      const cluster = await startPollCluster((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.BindSession || ++attachments > 1)
          return false;
        socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), status));
        return true;
      }, undefined, 'TCP', { reconnect: { enabled: true, interval: 10, maxRetries: 1 } });
      let resets = 0;
      cluster.client.on('sessionReset', () => { resets += 1; });
      try {
        // A 30 refusal fails only the link, so the slot binds the same session
        // again. A 40 ends the session, and the poll that never left the
        // client starts again under the next one.
        await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
        await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
        assert.equal(attachments, 2);
        assert.equal(countCommand(cluster.primary, COMMAND_CODE.PollMessages), 2);
        assert.equal(cluster.primary.frames.filter((frame) => frame[REQUEST_OFFSET.operation] === Operation.Register).length, 0);
        assert.equal(resets, status === STALE_CLIENT ? 0 : 1);
      } finally {
        await cluster.close();
      }
    });

    it(`rebinds a detached control link after status ${status}`, async () => {
      const registered = new Set<Socket>();
      let routes = 0;
      const cluster = await startPollCluster(undefined, (frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.Register ||
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession) {
          assert.equal(registered.has(socket), false,
            'session recovery must replace the rejected control transport');
          registered.add(socket);
        }
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute || ++routes > 1)
          return false;
        socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), status));
        return true;
      }, 'TCP', { reconnect: { enabled: true, interval: 10, maxRetries: 1 } });
      let resets = 0;
      cluster.client.on('sessionReset', () => { resets += 1; });
      try {
        const first = cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
        if (status === STALE_CLIENT) {
          await assert.rejects(first, (error: unknown) => error instanceof ResponseError && error.errorCode === status);
          assert.equal(routes, 1);
          assert.equal(countCommand(cluster.primary, COMMAND_CODE.PollMessages), 0);
        } else {
          await first;
        }
        // A lookup on the fake primary routes the poll to replica 0, so wait
        // until the metadata link is bound again on a new socket.
        const replicas = (cluster.client as unknown as { replicas: Map<number, { attached: boolean }> }).replicas;
        await waitUntil(() => registered.size === 2 && replicas.get(0)?.attached === true);
        await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
        assert.equal(registered.size, 2);
        assert.equal(countCommand(cluster.primary, COMMAND_CODE.PollMessages) +
          countCommand(cluster.coordinator, COMMAND_CODE.PollMessages), status === STALE_CLIENT ? 1 : 2);
        assert.equal(resets, 0);
      } finally {
        await cluster.close();
      }
    });
  }

  it('binds primary connections after credential changes without another login', async () => {
    const cluster = await startPollCluster(undefined, (frame, socket) => {
      const operation = frame[REQUEST_OFFSET.operation];
      if (operation !== Operation.UpdateResource && operation !== Operation.ChangePassword)
        return false;
      socket.write(replyFrame(frame, committedOperation(frame), Buffer.alloc(4)));
      return true;
    });
    try {
      await cluster.client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'manual', password: 'first' }));
      await cluster.client.sendCommand(UPDATE_USER.code, UPDATE_USER.serialize({ userId: 7, username: 'renamed' }));
      await cluster.client.sendCommand(CHANGE_PASSWORD.code, CHANGE_PASSWORD.serialize({
        userId: 7, currentPassword: 'first', newPassword: 'second'
      }));
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      const login = cluster.coordinator.frames.find((frame) => frame[REQUEST_OFFSET.operation] === Operation.Register)!;
      const credentials = registerCredentialsBody(login);
      const username = readWireName(credentials, 0);
      const password = readWireName(credentials, username.next);
      assert.equal(username.value, 'manual');
      assert.equal(password.value, 'first');
      const binding = cluster.primary.frames.find((frame) =>
        frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession)!;
      assertSharedBinding(login, binding);
      assert.equal(cluster.primary.frames.filter((frame) =>
        frame[REQUEST_OFFSET.operation] === Operation.Register).length, 0);
    } finally {
      await cluster.close();
    }
  });

  it('binds auxiliaries with the proof from PAT registration', async () => {
    const cluster = await startPollCluster();
    try {
      await cluster.client.sendCommand(LOGIN_WITH_TOKEN.code, LOGIN_WITH_TOKEN.serialize({ token: 'manual-token' }));
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      const login = cluster.coordinator.frames.find((frame) => frame[REQUEST_OFFSET.operation] === Operation.Register)!;
      const sdkName = readWireName(login, HEADER_SIZE + 4);
      const sdkVersion = readWireName(login, sdkName.next);
      assert.equal(readWireName(login, sdkVersion.next + BIND_SECRET_BYTES).value, 'manual-token');
      assertSharedBinding(login, cluster.primary.frames.find((frame) =>
        frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession)!);
    } finally {
      await cluster.close();
    }
  });

  for (const manual of [
    { command: LOGIN.code, payload: LOGIN.serialize({ username: 'manual', password: 'first' }), identity: 'manual' },
    { command: LOGIN_WITH_TOKEN.code, payload: LOGIN_WITH_TOKEN.serialize({ token: 'manual-token' }), identity: 'manual-token' }
  ]) {
    it(`retains ${manual.identity} across recovery and never signs in by itself after logout`, async () => {
      let coordinatorSocket!: Socket;
      const cluster = await startPollCluster(undefined, (_frame, socket) => {
        coordinatorSocket = socket;
        return false;
      });
      try {
        await cluster.client.sendCommand(manual.command, manual.payload);
        await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
        const reset = once(cluster.client, 'sessionReset');
        coordinatorSocket.write(evictionFrame(EvictionReason.SessionError));
        await reset;
        await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
        for (const server of [cluster.coordinator]) {
          const login = server.frames.filter((frame) => frame[REQUEST_OFFSET.operation] === Operation.Register).at(-1)!;
          assert.equal(readWireName(registerCredentialsBody(login), 0).value, manual.identity);
          assertSharedBinding(login, cluster.primary.frames.filter((frame) =>
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession).at(-1)!);
        }
        await cluster.client.sendCommand(LOGOUT.code, LOGOUT.serialize());
        const registers = framesOf(cluster.coordinator, Operation.Register).length;
        await assert.rejects(cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload()), (error: unknown) =>
          error instanceof ResponseError && error.errorCode === UNAUTHENTICATED &&
          error.commandCode === COMMAND_CODE.PollMessages);
        assert.equal(framesOf(cluster.coordinator, Operation.Register).length, registers);
      } finally {
        await cluster.close();
      }
    });
  }

  it('updates the configured identity after its password and username change', async () => {
    let coordinatorSocket!: Socket;
    const cluster = await startPollCluster(undefined, (frame, socket) => {
      coordinatorSocket = socket;
      const operation = frame[REQUEST_OFFSET.operation];
      if (operation !== Operation.UpdateResource && operation !== Operation.ChangePassword)
        return false;
      socket.write(replyFrame(frame, committedOperation(frame), Buffer.alloc(4)));
      return true;
    });
    try {
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      await cluster.client.sendCommand(UPDATE_USER.code, UPDATE_USER.serialize({ userId: 7, username: 'renamed' }));
      await cluster.client.sendCommand(CHANGE_PASSWORD.code, CHANGE_PASSWORD.serialize({
        userId: 7, currentPassword: 'iggy', newPassword: 'second'
      }));
      const reset = once(cluster.client, 'sessionReset');
      coordinatorSocket.write(evictionFrame(EvictionReason.SessionError));
      await reset;
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      for (const server of [cluster.coordinator]) {
        const login = server.frames.filter((frame) => frame[REQUEST_OFFSET.operation] === Operation.Register).at(-1)!;
        const credentials = registerCredentialsBody(login);
        const username = readWireName(credentials, 0);
        assert.equal(username.value, 'renamed');
        assert.equal(readWireName(credentials, username.next).value, 'second');
        assertSharedBinding(login, cluster.primary.frames.filter((frame) =>
          frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession).at(-1)!);
      }
    } finally {
      await cluster.close();
    }
  });

  it('keeps the signed-in identity when a stream or topic rename names it', async () => {
    let coordinatorSocket!: Socket;
    const cluster = await startPollCluster(undefined, (frame, socket) => {
      coordinatorSocket = socket;
      if (frame[REQUEST_OFFSET.operation] !== Operation.UpdateResource)
        return false;
      socket.write(replyFrame(frame, committedOperation(frame), Buffer.alloc(4)));
      return true;
    });
    try {
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      for (const streamId of [7, 'iggy'])
        await cluster.client.sendCommand(UPDATE_TOPIC.code,
          UPDATE_TOPIC.serialize({ streamId, topicId: 'iggy', name: 'renamed' }));
      for (const streamId of [7, 'iggy'])
        await cluster.client.sendCommand(UPDATE_STREAM.code,
          UPDATE_STREAM.serialize({ streamId, name: 'renamed' }));
      const reset = once(cluster.client, 'sessionReset');
      coordinatorSocket.write(evictionFrame(EvictionReason.SessionError));
      await reset;
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      const login = cluster.coordinator.frames.filter((frame) =>
        frame[REQUEST_OFFSET.operation] === Operation.Register).at(-1)!;
      assert.equal(readWireName(registerCredentialsBody(login), 0).value, 'iggy');
    } finally {
      await cluster.close();
    }
  });

  it('keeps the old identity when its rename is refused', async () => {
    let coordinatorSocket!: Socket;
    const cluster = await startPollCluster(undefined, (frame, socket) => {
      coordinatorSocket = socket;
      if (frame[REQUEST_OFFSET.operation] !== Operation.UpdateResource)
        return false;
      const response = replyFrame(frame, committedOperation(frame), metadataResult(UNAUTHORIZED));
      response.writeBigUInt64LE(3n, REPLY_OFFSET.commit);
      response.writeBigUInt64LE(3n, REPLY_OFFSET.op);
      response[REPLY_OFFSET.reserved] = 1;
      socket.write(response);
      return true;
    });
    try {
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      await assert.rejects(
        cluster.client.sendCommand(UPDATE_USER.code, UPDATE_USER.serialize({ userId: 7, username: 'renamed' })),
        (error: unknown) => error instanceof ResponseError && error.errorCode === UNAUTHORIZED
      );
      const reset = once(cluster.client, 'sessionReset');
      coordinatorSocket.write(evictionFrame(EvictionReason.SessionError));
      await reset;
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      const login = cluster.coordinator.frames.filter((frame) =>
        frame[REQUEST_OFFSET.operation] === Operation.Register).at(-1)!;
      assert.equal(readWireName(registerCredentialsBody(login), 0).value, 'iggy');
    } finally {
      await cluster.close();
    }
  });

  it('closes a waiting primary exchange on coordinator logout without replay', async () => {
    let admitted!: () => void;
    const pendingAdmission = new Promise<void>((resolve) => { admitted = resolve; });
    const cluster = await startPollCluster((frame) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages)
        return false;
      admitted();
      return true;
    });
    try {
      const polling = cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload());
      const failed = assert.rejects(polling, (error: unknown) =>
        error instanceof ResponseError && error.errorCode === TRANSIENT_NOT_COMMITTED);
      await pendingAdmission;
      await cluster.client.sendCommand(LOGOUT.code, LOGOUT.serialize());
      await failed;
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.PollMessages), 1);
      assert.equal(cluster.client.isAuthenticated, false);
    } finally {
      await cluster.close();
    }
  });
});

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
          frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetStats)
        return singleNodeHandler(server.port)(frame, socket);
      attempts += 1;
      socket.write(
        attempts === 1
          ? replyFrame(frame, operation, Buffer.alloc(0), 57)
          : replyFrame(frame, operation, Buffer.from('done'))
      );
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      const response = await client.sendCommand(
        COMMAND_CODE.GetStats,
        Buffer.from('retry-me')
      );
      assert.equal(attempts, 2);
      assert.deepEqual(response.data, Buffer.from('done'));
      const sent = server.frames.filter(
        (frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetStats
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
          evictionFrame(EvictionReason.InvalidCredentials),
          replyFrame(frame, Operation.NonReplicated, Buffer.from('stale'))
        ]));
        return;
      }
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === 60_014) {
        socket.write(
          replyFrame(frame, Operation.NonReplicated, Buffer.from('fresh'))
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
          error instanceof VsrEvictionError && error.errorCode === INVALID_CREDENTIALS
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

  it('reports whether a session reset ended a signed-in session', async () => {
    let registers = 0;
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.Register && ++registers === 1) {
        socket.write(evictionFrame(EvictionReason.InvalidCredentials));
        return;
      }
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetStats) {
        socket.write(evictionFrame(EvictionReason.SessionError));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    const resets: unknown[] = [];
    client.on('sessionReset', (signedIn) => { resets.push(signedIn); });
    try {
      await assert.rejects(client.authenticate(vsrConfig(server.port).credentials), VsrEvictionError);
      await client.authenticate(vsrConfig(server.port).credentials);
      // A group poll retries after this reset: the client signs in again by itself.
      await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)), VsrEvictionError);
      await waitUntil(() => client.isAuthenticated);
      assert.equal(registers, 3);
      await client.sendCommand(LOGOUT.code, LOGOUT.serialize());
      // The sign-in after the refused one, the recovery after the session failure, then the logout.
      assert.deepEqual(resets, [false, true, true]);
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
      assert.equal(client.isAuthenticated, true);
    } finally {
      client.destroy();
      await server.close();
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
        if (operation === Operation.Register ||
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession) {
          singleNodeHandler(survivor.port)(frame, socket);
          return;
        }
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (code === COMMAND_CODE.GetClusterMetadata) {
          // The survivor leads once the primary is gone.
          socket.write(replyFrame(frame,
            Operation.NonReplicated,
            twoNodeMetadataBody(primary.port, survivor.port)
          ));
          return;
        }
        socket.write(replyFrame(frame, operation));
      });

      const primary = await startVsrServer((frame, socket) => {
        primarySockets.add(socket);
        if (primaryDead) {
          socket.destroy();
          return;
        }
        const operation = frame.readUInt8(REQUEST_OFFSET.operation);
        if (operation === Operation.Register) {
          socket.write(replyFrame(frame, Operation.Register, registerReplyBody()));
          return;
        }
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (code === COMMAND_CODE.GetClusterMetadata) {
          // The primary leads, so the login settles here and the roster is
          // only remembered, not acted on, until the node dies.
          socket.write(replyFrame(frame,
            Operation.NonReplicated,
            twoNodeMetadataBody(survivor.port, primary.port)
          ));
          return;
        }
        socket.write(replyFrame(frame, operation));
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

        assert.equal(countCommand(survivor, COMMAND_CODE.BindSession), 1);
        assertSharedBinding(primary.frames[0], survivor.frames[0]);
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

  it('redials the configured endpoint of a single node that advertises another', async () => {
    const unreachable = await startVsrServer(() => {});
    const advertisedPort = unreachable.port;
    await unreachable.close();
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetClusterMetadata) {
        socket.write(replyFrame(frame, Operation.NonReplicated, singleNodeMetadataBody(advertisedPort)));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0,
      reconnect: { enabled: true, interval: 1, maxRetries: 3 } });
    try {
      await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0), { deadline: Date.now() + 2_000 });
      assert.equal(countCommand(server, COMMAND_CODE.BindSession), 0);
      const slot = (client as unknown as { replicas: Map<number, { dialEndpoint: unknown }> }).replicas.get(0);
      assert.deepEqual(slot?.dialEndpoint, vsrConfig(server.port).options);

      for (const socket of server.sockets) socket.destroy();
      await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0), { deadline: Date.now() + 2_000 });
      assert.equal(countCommand(server, COMMAND_CODE.BindSession), 1);
      assert.equal(framesOf(server, Operation.Register).length, 1);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('keeps a login alive when the session dies mid-poll', async () => {
    let rosterReads = 0;
    const server = await startVsrServer((frame, socket) => {
      const operation = frame.readUInt8(REQUEST_OFFSET.operation);
      if (operation === Operation.Register) {
        socket.write(replyFrame(frame, Operation.Register, registerReplyBody()));
        return;
      }
      rosterReads += 1;
      // The roster stays leaderless and the session dies in the same breath.
      // Polling on without a session would re-enter authentication, which
      // awaits the login being settled, and the login would never return.
      socket.write(Buffer.concat([
        replyFrame(frame,
          Operation.NonReplicated,
          twoNodeMetadataBody(server.port, server.port, 1)
        ),
        evictionFrame(EvictionReason.NoSession)
      ]));
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
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
          socket.write(replyFrame(frame, Operation.Register, registerReplyBody()));
          return;
        }
        socket.write(replyFrame(frame,
          Operation.NonReplicated,
          twoNodeMetadataBody(server.port, unavailablePort, 0, 1)
        ));
      });
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
        socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 3));
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
        (error: unknown) => error instanceof ResponseError && error.errorCode === TRANSIENT_NOT_ACCEPTED
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
          socket.write(replyFrame(frame,
            Operation.NonReplicated,
            demotedYet
              ? twoNodeMetadataBody(demoted.port, leader.port)
              : twoNodeMetadataBody(leader.port, demoted.port)
          ));
          return;
        }
        if (code === 60_032) {
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 58));
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
        assert.equal(framesOf(leader, Operation.Register).length, 0);
        assert.equal(framesOf(demoted, Operation.Register).length, 1);
      } finally {
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
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 58));
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
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 58));
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

  for (const refusal of [58, 89, 41, 40, 30, 87, 3007]) {
  it(`keeps an uncertain write uncertain after a later refusal ${refusal}`, async () => {
    const EXPIRED_REQUEST_TIME = 30_001;
    let now = 0;
    let attempts = 0;
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt8(REQUEST_OFFSET.operation) === Operation.SendMessages) {
        attempts += 1;
        now = attempts === 1 ? 1 : EXPIRED_REQUEST_TIME;
        socket.write(replyFrame(frame,
          Operation.SendMessages, Buffer.alloc(0), attempts === 1 ? 57 : refusal
        ));
        return;
      }
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
        socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(0n, 0n, 0)));
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
      Date.now = () => now;
      await assert.rejects(
        () => client.sendCommand(COMMAND_CODE.SendMessages, explicitSend),
        (error: unknown) =>
          error instanceof OperationUncertainError && error.operationId !== undefined
      );
      const writes = server.frames.filter(
        (frame) => frame.readUInt8(REQUEST_OFFSET.operation) === Operation.SendMessages
      );
      assert.equal(writes.length, 2);
      assert.deepEqual(writes[1], writes[0]);
      assert.equal(sessionResets, 0);
    } finally {
      Date.now = realNow;
      client.destroy();
      await server.close();
    }
  });
  }

  it('reuses a send context and refreshes it once after a history refusal', async () => {
    const state = { live: 1n, refuseAll: false, contextStatus: 0 };
    const server = await contextServer(state);
    const client = new CommandResponseStream(vsrConfig(server.port));
    const incarnations = () => server.frames
      .filter((frame) => frame.readUInt8(REQUEST_OFFSET.operation) === Operation.SendMessages)
      .map((frame) => frame.readBigUInt64LE(REQUEST_OFFSET.partitionIncarnation));
    try {
      await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 1);
      state.live = 2n;
      await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 2);
      assert.deepEqual(incarnations(), [1n, 1n, 1n, 2n]);
      state.refuseAll = true;
      await assert.rejects(() => client.sendCommand(COMMAND_CODE.SendMessages, explicitSend),
        (error: unknown) => error instanceof ResponseError &&
          error.commandCode === COMMAND_CODE.SendMessages && error.errorCode === HISTORY_UNAVAILABLE);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 3);
      assert.equal(incarnations().length, 6);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('reports a refused context query as an error of the send', async () => {
    const server = await contextServer({ live: 1n, refuseAll: false, contextStatus: 2010 });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await assert.rejects(() => client.sendCommand(COMMAND_CODE.SendMessages, explicitSend),
        (error: unknown) => error instanceof ResponseError &&
          error.commandCode === COMMAND_CODE.SendMessages && error.errorCode === 2010);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('keeps an eviction during the send context query typed', async () => {
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
        socket.write(evictionFrame(EvictionReason.InvalidCredentials));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await assert.rejects(() => client.sendCommand(COMMAND_CODE.SendMessages, explicitSend),
        (error: unknown) => error instanceof VsrEvictionError && error.errorCode === INVALID_CREDENTIALS);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('surfaces a history refusal the remaining budget cannot refresh', async () => {
    const realNow = Date.now;
    const deadline = realNow() + 30_000;
    let offset = 0;
    const server = await contextServer({
      live: 1n, refuseAll: true, contextStatus: 0,
      // Expire the original deadline as the history refusal arrives.
      onSend: () => { offset = deadline - realNow() + 1; }
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      Date.now = () => realNow() + offset;
      await assert.rejects(
        () => client.sendCommand(COMMAND_CODE.SendMessages, explicitSend, { deadline }),
        (error: unknown) => error instanceof ResponseError &&
          error.commandCode === COMMAND_CODE.SendMessages && error.errorCode === HISTORY_UNAVAILABLE);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 1);
    } finally {
      Date.now = realNow;
      client.destroy();
      await server.close();
    }
  });

  for (const partition of [Partitioning.Balanced, Partitioning.MessageKey('key')]) {
    it(`refuses a raw send with partitioning kind ${partition.kind} before writing it`, async () => {
      const server = await startVsrServer((frame, socket) => singleNodeHandler(server.port)(frame, socket));
      const client = new CommandResponseStream(vsrConfig(server.port));
      try {
        await assert.rejects(() => client.sendCommand(COMMAND_CODE.SendMessages, sendTo(partition)),
          (error: unknown) => error instanceof ResponseError &&
            error.commandCode === COMMAND_CODE.SendMessages && error.errorCode === 5 &&
            error.message.includes('explicit partition id') && error.message.includes('typed send API'));
        assert.equal(framesOf(server, Operation.SendMessages).length, 0);
        assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 0);
      } finally {
        client.destroy();
        await server.close();
      }
    });
  }

  for (const [name, code, payload, operation] of [
    ['an explicit-partition send', COMMAND_CODE.SendMessages, explicitSend, Operation.SendMessages],
    ['an offset store', COMMAND_CODE.StoreOffset, storeOffsetPayload, Operation.StoreConsumerOffset],
    ['an offset delete', COMMAND_CODE.DeleteConsumerOffset, deleteOffsetPayload, Operation.DeleteConsumerOffset],
    ['a standalone poll', COMMAND_CODE.PollMessages, groupPollPayload(false), Operation.NonReplicated]
  ] as const) {
    it(`stamps the caller's context on ${name} while looking up its primary`, async () => {
      const server = await startVsrServer((frame, socket) => offsetCommittingHandler(server.port)(frame, socket));
      const client = new CommandResponseStream(vsrConfig(server.port));
      try {
        await client.sendCommand(code, payload, { context: CALLER_CONTEXT });
        const requests = framesOf(server, operation).filter((frame) =>
          operation !== Operation.NonReplicated || frame.readUInt32LE(REQUEST_OFFSET.reserved) === code);
        assert.equal(requests.length, 1);
        assert.deepEqual(stampedContext(requests[0]), CALLER_CONTEXT);
        assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 1);
      } finally {
        client.destroy();
        await server.close();
      }
    });
  }

  it("stamps the caller's context on a primary poll", async () => {
    const cluster = await startPollCluster();
    try {
      await cluster.client.sendCommand(POLL_MESSAGES.code, groupPollPayload(), { context: CALLER_CONTEXT });
      const polls = cluster.primary.frames.filter((frame) =>
        frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.PollMessages);
      assert.equal(polls.length, 1);
      assert.deepEqual(stampedContext(polls[0]), CALLER_CONTEXT);
    } finally {
      await cluster.close();
    }
  });

  it('keeps a caller context on a send refused as history unavailable', async () => {
    const server = await contextServer({ live: 1n, refuseAll: true, contextStatus: 0 });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await assert.rejects(
        () => client.sendCommand(COMMAND_CODE.SendMessages, explicitSend, { context: CALLER_CONTEXT }),
        (error: unknown) => error instanceof ResponseError && error.errorCode === HISTORY_UNAVAILABLE);
      assert.deepEqual(framesOf(server, Operation.SendMessages).map(stampedContext), [CALLER_CONTEXT]);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 1);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('retries primary lifecycle refusals with new ids and the current route context while the coordinator proceeds', async () => {
    const order: string[] = [];
    let refusals = 2;
    let routes = 0;
    let refuseFirst: (() => void) | undefined;
    let firstSent!: () => void;
    const sent = new Promise<void>((resolve) => { firstSent = resolve; });
    let firstRefusal!: () => void;
    const refused = new Promise<void>((resolve) => { firstRefusal = resolve; });
    const otherCommand = 60_041;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt8(REQUEST_OFFSET.operation) !== Operation.SendMessages)
        return false;
      order.push('send');
      const reply = () => {
        socket.write(replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), refusals-- > 0 ? LIFECYCLE_BUSY : 0));
        firstRefusal();
      };
      if (refuseFirst) reply();
      else {
        refuseFirst = reply;
        firstSent();
      }
      return true;
    }, (frame, socket) => {
      const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
      if (code === COMMAND_CODE.GetPartitionRoute) {
        routes += 1;
        socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(BigInt(routes), BigInt(routes))));
        return true;
      }
      if (code === otherCommand)
        order.push('other');
      return false;
    });
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      const started = Date.now();
      const send = cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      await sent;
      // Another lookup replaces the cached route before the first refusal.
      await cluster.client.sendCommand(COMMAND_CODE.GetPartitionRoute, serializePartitionRoute(1, 2, 3));
      refuseFirst!();
      await refused;
      await cluster.client.sendCommand(otherCommand, Buffer.alloc(0));
      await send;
      const sends = framesOf(cluster.primary, Operation.SendMessages);
      assert.equal(sends.length, 3);
      assert.equal(new Set(sends.map((frame) => frame.readBigUInt64LE(REQUEST_OFFSET.request))).size, 3);
      const replaced = { incarnation: 2n, ownerGeneration: 0n, metadataOp: 2n };
      assert.deepEqual(sends.map(stampedContext), [{ incarnation: 1n, ownerGeneration: 0n, metadataOp: 1n },
        replaced, replaced], 'a new request takes the context of the route at that time');
      assert.deepEqual(order, ['send', 'other', 'send', 'send']);
      assert.ok(Date.now() - started >= 145, 'the pauses grow from 50 ms to 100 ms');
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetClusterMetadata), 0);
    } finally {
      await cluster.close();
    }
  });

  for (const code of [COMMAND_CODE.GetPartitionRoute, COMMAND_CODE.BindSession]) {
    it(`does not retry a lifecycle refusal from routing command ${code}`, async () => {
      const override = (frame: Buffer, socket: Socket): boolean => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== code)
          return false;
        socket.write(replyFrame(frame, frame.readUInt8(REQUEST_OFFSET.operation), Buffer.alloc(0), 88));
        return true;
      };
      const cluster = await startPollCluster(override, override);
      try {
        await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
        await assert.rejects(
          () => cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend,
            { deadline: Date.now() + 200 }),
          (error: unknown) => error instanceof ResponseError &&
            error.commandCode === COMMAND_CODE.SendMessages && error.errorCode === 88);
        assert.equal(countCommand(code === COMMAND_CODE.GetPartitionRoute
          ? cluster.coordinator : cluster.primary, code), 1);
        assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 0);
      } finally {
        await cluster.close();
      }
    });
  }

  it('returns a primary lifecycle refusal within the original request deadline', async () => {
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt8(REQUEST_OFFSET.operation) !== Operation.SendMessages)
        return false;
      socket.write(replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), 88));
      return true;
    });
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      const deadline = Date.now() + 400;
      await assert.rejects(
        () => cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend, { deadline }),
        (error: unknown) => error instanceof ResponseError &&
          error.commandCode === COMMAND_CODE.SendMessages && error.errorCode === 88);
      assert.ok(Date.now() <= deadline + 100, 'the original deadline bounds the call');
      assert.ok(framesOf(cluster.primary, Operation.SendMessages).length >= 2);
    } finally {
      await cluster.close();
    }
  });

  it('keeps a lifecycle restart ahead of later mutations in its lane', async () => {
    let refuse!: () => void;
    let received!: () => void;
    const firstReceived = new Promise<void>((resolve) => { received = resolve; });
    const names: string[] = [];
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
        names.push(frame.subarray(HEADER_SIZE).toString());
        if (names.length === 1) {
          refuse = () => socket.write(replyFrame(frame, Operation.CreateStream, metadataResult(88)));
          received();
        } else socket.write(replyFrame(frame, Operation.CreateStream, metadataResult(0)));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      const body = Buffer.from('first');
      const first = client.sendCommand(COMMAND_CODE.CreateStream, body);
      await firstReceived;
      body.write('other');
      const second = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.from('second'));
      await new Promise<void>((resolve) => setImmediate(resolve));
      refuse();
      await Promise.all([first, second]);
      assert.deepEqual(names, ['first', 'first', 'second']);
      const ids = framesOf(server, Operation.CreateStream).map((frame) => frame.readBigUInt64LE(REQUEST_OFFSET.request));
      assert.equal(new Set(ids).size, 3, 'each admitted mutation has a fresh identity');
    } finally { client.destroy(); await server.close(); }
  });

  it('retries a lifecycle refusal as new requests while other commands proceed', async () => {
    let refusals = 2;
    let firstRefusal!: () => void;
    const refused = new Promise<void>((resolve) => { firstRefusal = resolve; });
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt8(REQUEST_OFFSET.operation) === Operation.CreateStream) {
        socket.write(replyFrame(frame, Operation.CreateStream, metadataResult(refusals-- > 0 ? 88 : 0)));
        firstRefusal();
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      const started = Date.now();
      const create = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      await refused;
      const other = client.sendCommand(60_040, Buffer.alloc(0));
      await Promise.all([create, other]);
      const elapsed = Date.now() - started;
      const creates = framesOf(server, Operation.CreateStream);
      assert.equal(creates.length, 3);
      assert.equal(new Set(creates.map((frame) =>
        frame.readBigUInt64LE(REQUEST_OFFSET.request))).size, 3, 'each retry is a new request');
      assert.ok(elapsed >= 145, `the pauses grow from 50 ms, took ${elapsed} ms`);
      const order = server.frames.map((frame) => frame.readUInt8(REQUEST_OFFSET.operation) === Operation.CreateStream
        ? 'create' : frame.readUInt32LE(REQUEST_OFFSET.reserved)).filter((entry) => entry === 'create' || entry === 60_040);
      assert.deepEqual(order, ['create', 60_040, 'create', 'create'], 'the pause must not hold the queue');
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('returns the lifecycle refusal once the request deadline leaves no room for a retry', async () => {
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt8(REQUEST_OFFSET.operation) === Operation.CreateStream) {
        socket.write(replyFrame(frame, Operation.CreateStream, metadataResult(88)));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      const deadline = Date.now() + 400;
      await assert.rejects(
        () => client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0), { deadline }),
        (error: unknown) => error instanceof ResponseError &&
          error.commandCode === COMMAND_CODE.CreateStream && error.errorCode === 88);
      assert.ok(Date.now() <= deadline + 100, 'the original deadline bounds the call');
      assert.ok(framesOf(server, Operation.CreateStream).length >= 2);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('invalidates a refused partition without evicting another partition', async () => {
    const state = { live: 1n, refuseAll: false, contextStatus: 0 };
    const server = await contextServer(state);
    const client = new CommandResponseStream(vsrConfig(server.port));
    const otherPartition = sendTo(Partitioning.PartitionId(4));
    try {
      await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      await client.sendCommand(COMMAND_CODE.SendMessages, otherPartition);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 2);
      state.live = 2n;
      await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 3);
      await client.sendCommand(COMMAND_CODE.SendMessages, otherPartition);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 4,
        'each stale partition refreshes when its own history is refused');
      assert.deepEqual(framesOf(server, Operation.SendMessages).map((frame) =>
        stampedContext(frame).incarnation), [1n, 1n, 1n, 2n, 1n, 2n]);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  for (const code of [COMMAND_CODE.DeleteStream, COMMAND_CODE.DeleteTopic,
    COMMAND_CODE.CreatePartitions, COMMAND_CODE.DeletePartitions]) {
    it(`drops every cached send context after the client's own command ${code}`, async () => {
      const server = await startVsrServer((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === operationForCode(code)) {
          socket.write(replyFrame(frame, frame[REQUEST_OFFSET.operation], metadataResult(0)));
          return;
        }
        offsetCommittingHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      let resets = 0;
      client.on('topicDiscoveryReset', () => { resets += 1; });
      try {
        await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
        await client.sendCommand(code, Buffer.alloc(0), { handleResponse: false });
        await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
        assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 2);
        assert.equal(resets, 1, 'typed sends read the partition count again');
      } finally {
        client.destroy();
        await server.close();
      }
    });
  }

  for (const [name, code, operation, payload] of [
    ['store', COMMAND_CODE.StoreOffset, Operation.StoreConsumerOffset, storeOffsetPayload],
    ['delete', COMMAND_CODE.DeleteConsumerOffset, Operation.DeleteConsumerOffset, deleteOffsetPayload]
  ] as const) {
    it(`writes an offset ${name} on the partition primary and keeps the main connection`, async () => {
      const cluster = await startPollCluster(
        (frame, socket) => {
          if (frame.readUInt8(REQUEST_OFFSET.operation) !== operation)
            return false;
          socket.write(replyFrame(frame, operation, metadataResult(0)));
          return true;
        },
        (frame, socket) => {
          if (frame.readUInt8(REQUEST_OFFSET.operation) !== operation)
            return false;
          socket.write(replyFrame(frame, operation, Buffer.alloc(0), TRANSIENT_NOT_ACCEPTED));
          return true;
        });
      try {
        await cluster.client.sendCommand(code, payload, { deadline: Date.now() + 2_000 });
        assert.equal(framesOf(cluster.coordinator, operation).length, 0);
        assert.equal(framesOf(cluster.primary, operation).length, 1);
        const rosterReads = countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata);
        await cluster.client.sendCommand(COMMAND_CODE.GetClusterMetadata, Buffer.alloc(0));
        assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata), rosterReads + 1,
          'the main connection stays on its node');
        assert.equal(framesOf(cluster.coordinator, Operation.Register).length, 1, 'the session stays');
        assert.equal(framesOf(cluster.primary, Operation.Register).length, 0);
      } finally {
        await cluster.close();
      }
    });
  }

  it('takes the request ids of primary offset writes from the session counter', async () => {
    // The partition deduplicates by client id and request id, so an id the
    // session already used would answer a write with the cached reply of
    // another. Reading routes also consumes ids from the same counter.
    let stores = 0;
    const cluster = await startPollCluster(
      (frame, socket) => {
        if (frame.readUInt8(REQUEST_OFFSET.operation) !== Operation.StoreConsumerOffset)
          return false;
        stores += 1;
        socket.write(replyFrame(frame, Operation.StoreConsumerOffset, metadataResult(0),
          stores === 1 ? HISTORY_UNAVAILABLE : 0));
        return true;
      },
      (frame, socket) => {
        if (frame.readUInt8(REQUEST_OFFSET.operation) !== Operation.CreateStream)
          return false;
        socket.write(replyFrame(frame, Operation.CreateStream, metadataResult(0)));
        return true;
      });
    try {
      await cluster.client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload),
        (error: unknown) => error instanceof ResponseError && error.errorCode === HISTORY_UNAVAILABLE);
      await cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.BindSession), 1,
        'a history refusal retains the authenticated connection');
      const requests = [
        ...framesOf(cluster.coordinator, Operation.CreateStream),
        ...framesOf(cluster.primary, Operation.StoreConsumerOffset)
      ].map((frame) => frame.readBigUInt64LE(REQUEST_OFFSET.request));
      assert.equal(new Set(requests).size, requests.length);
      assert.ok(requests[0] < requests[1] && requests[1] < requests[2]);
    } finally {
      await cluster.close();
    }
  });

  for (const callerContext of [undefined, CALLER_CONTEXT]) {
    it(`keeps one ${callerContext ? 'caller' : 'route'} context across primary retries of an offset store`, async () => {
      let routes = 0;
      let writes = 0;
      const cluster = await startPollCluster(
        (frame, socket) => {
          if (frame.readUInt8(REQUEST_OFFSET.operation) !== Operation.StoreConsumerOffset)
            return false;
          writes += 1;
          socket.write(writes === 1
            ? replyFrame(frame, Operation.StoreConsumerOffset, Buffer.alloc(0), TRANSIENT_NOT_ACCEPTED)
            : replyFrame(frame, Operation.StoreConsumerOffset, metadataResult(0)));
          return true;
        },
        (frame, socket) => {
          if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute)
            return false;
          routes += 1;
          socket.write(replyFrame(frame, Operation.NonReplicated,
            routingBody(1n, BigInt(routes))));
          return true;
        });
      let resets = 0;
      cluster.client.on('sessionReset', () => { resets += 1; });
      try {
        await cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload,
          { context: callerContext });
        assert.equal(routes, 2, 'a refused write routes again');
        assert.equal(resets, 0, 'a primary refusal leaves the main connection alone');
        assert.equal(framesOf(cluster.coordinator, Operation.Register).length, 1);
        assert.deepEqual(framesOf(cluster.primary, Operation.StoreConsumerOffset).map(stampedContext),
          Array(2).fill(callerContext ?? { incarnation: 1n, ownerGeneration: 0n, metadataOp: 1n }));
      } finally {
        await cluster.close();
      }
    });
  }

  it('refreshes standalone routes after metadata commits', async () => {
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt8(REQUEST_OFFSET.operation) === Operation.CreateStream) {
        const reply = replyFrame(frame, Operation.CreateStream, metadataResult(0));
        reply.writeBigUInt64LE(5n, REPLY_OFFSET.commit);
        reply.writeBigUInt64LE(5n, REPLY_OFFSET.op);
        reply[REPLY_OFFSET.reserved] = 1;
        socket.write(reply);
        return;
      }
      offsetCommittingHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      for (let call = 0; call < 2; call += 1) {
        await client.sendCommand(POLL_MESSAGES.code, groupPollPayload(false));
        await client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload);
        await client.sendCommand(COMMAND_CODE.DeleteConsumerOffset, deleteOffsetPayload);
      }
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 2);
      await client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      await client.sendCommand(POLL_MESSAGES.code, groupPollPayload(false));
      await client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 4);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('surfaces a refusal under a stale route once and routes the next call again', async () => {
    let live = 1n;
    const server = await startVsrServer((frame, socket) => {
      const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
      if (code === COMMAND_CODE.GetPartitionRoute) {
        socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(1n, live, 0)));
        return;
      }
      if (code === COMMAND_CODE.PollMessages) {
        socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0),
          stampedContext(frame).incarnation === live ? 0 : HISTORY_UNAVAILABLE));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.sendCommand(POLL_MESSAGES.code, groupPollPayload(false));
      live = 2n;
      await assert.rejects(() => client.sendCommand(POLL_MESSAGES.code, groupPollPayload(false)),
        (error: unknown) => error instanceof ResponseError && error.errorCode === HISTORY_UNAVAILABLE);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 1, 'no silent retry');
      await client.sendCommand(POLL_MESSAGES.code, groupPollPayload(false));
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), 2);
      assert.deepEqual(server.frames
        .filter((frame) => frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.PollMessages)
        .map((frame) => stampedContext(frame).incarnation), [1n, 1n, 2n]);
    } finally {
      client.destroy();
      await server.close();
    }
  });

  it('rediscovers an evicted route when the partition cache is full', async () => {
    const server = await contextServer({ live: 1n, refuseAll: false, contextStatus: 0 });
    const client = new CommandResponseStream(vsrConfig(server.port));
    const partitions = Array.from({ length: 4097 }, (_, partitionId) => sendTo(Partitioning.PartitionId(partitionId)));
    try {
      await Promise.all(partitions.map((payload) => client.sendCommand(COMMAND_CODE.SendMessages, payload)));
      const before = countCommand(server, COMMAND_CODE.GetPartitionRoute);
      await client.sendCommand(COMMAND_CODE.SendMessages, partitions[4096]);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), before, 'capacity eviction preserves the newest route');
      await client.sendCommand(COMMAND_CODE.SendMessages, partitions[0]);
      assert.equal(countCommand(server, COMMAND_CODE.GetPartitionRoute), partitions.length + 1);
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

const provedReply = (request: Buffer, code = 0, marker = 1): Buffer => {
  const operation = request[REQUEST_OFFSET.operation];
  const body = operation === Operation.SendMessages ? Buffer.alloc(0) : metadataResult(code);
  const reply = replyFrame(request, operation, body);
  reply[REPLY_OFFSET.reserved] = marker;
  reply.writeBigUInt64LE(1n, REPLY_OFFSET.op);
  reply.writeBigUInt64LE(1n, REPLY_OFFSET.commit);
  return reply;
};

const waitUntil = async (predicate: () => boolean): Promise<void> => {
  const deadline = Date.now() + WAIT_LIMIT_MS;
  while (!predicate()) {
    if (Date.now() >= deadline) assert.fail('condition was not reached');
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
};

/** Like `waitUntil`, but by the real clock, so it also ends while a test mocks `Date` and `setTimeout`. */
const waitUntilUnmocked = async (predicate: () => boolean): Promise<void> => {
  const deadline = performance.now() + WAIT_LIMIT_MS;
  while (!predicate()) {
    if (performance.now() >= deadline) assert.fail('condition was not reached');
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
};

describe('native mutation recovery', () => {
  it('freezes a raw request before awaiting route discovery', async () => {
    const payload = Buffer.from(explicitSend);
    const submitted = Buffer.from(payload);
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute)
        payload.fill(0xff);
      offsetCommittingHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.sendCommand(COMMAND_CODE.SendMessages, payload);
      assert.deepEqual(framesOf(server, Operation.SendMessages)[0].subarray(HEADER_SIZE), submitted);
    } finally { client.destroy(); await server.close(); }
  });

  it('admits a recreated history while retaining an unresolved write to its old history', async () => {
    let incarnation = 1n;
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
        const route = routingBody(0n, 0n, 0);
        route.writeBigUInt64LE(incarnation, 0);
        socket.write(replyFrame(frame, Operation.NonReplicated, route));
      } else if (frame[REQUEST_OFFSET.operation] === Operation.SendMessages) {
        socket.write(incarnation === 1n
          ? replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), 85)
          : provedReply(frame));
      } else if (frame[REQUEST_OFFSET.operation] === Operation.DeleteTopic) {
        incarnation = 2n;
        socket.write(replyFrame(frame, Operation.DeleteTopic, metadataResult(0)));
      } else singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      let retained!: bigint;
      await assert.rejects(client.sendCommand(COMMAND_CODE.SendMessages, explicitSend), (error: unknown) => {
        assert.ok(error instanceof OperationUncertainError);
        retained = error.operationId!;
        return true;
      });
      await assert.rejects(client.sendCommand(COMMAND_CODE.SendMessages, explicitSend), LaneBlockedError);
      await client.sendCommand(COMMAND_CODE.DeleteTopic, Buffer.alloc(0));
      await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      assert.deepEqual(framesOf(server, Operation.SendMessages).map((frame) => stampedContext(frame).incarnation), [1n, 2n]);
      client.abandonOperation(retained);
    } finally { client.destroy(); await server.close(); }
  });

  for (const resolving of [false, true]) {
    it(`preserves uncertainty when a session resets before ${resolving ? 'a retained replay' : 'a retry'} reaches the wire`, async () => {
      let retain = resolving;
      const server = await startVsrServer((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
          socket.write(replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), retain ? 85 : 57));
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
      let release!: () => void;
      const released = new Promise<void>((resolve) => { release = resolve; });
      try {
        await client.authenticate(vsrConfig(server.port).credentials);
        let operationId!: bigint;
        if (resolving) {
          await assert.rejects(client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0)), (error: unknown) => {
            assert.ok(error instanceof OperationUncertainError);
            operationId = error.operationId!;
            return true;
          });
          retain = false;
        }
        const routing = client as unknown as {
          readyLink: (deadline: number, signal: AbortSignal, avoid?: IggyConnection) => Promise<IggyConnection>
        };
        const readyLink = routing.readyLink.bind(client);
        let selections = 0;
        let retryWaiting = false;
        routing.readyLink = async (...args) => {
          const link = await readyLink(...args);
          if (++selections === (resolving ? 1 : 2)) {
            retryWaiting = true;
            await released;
          }
          return link;
        };
        const pending = resolving ? client.resolveOperation(operationId)
          : client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
        const outcome = pending.then(() => undefined, (error: unknown) => error);
        await waitUntil(() => retryWaiting);
        operationId = framesOf(server, Operation.CreateStream)[0].readBigUInt64LE(REQUEST_OFFSET.request);
        await client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'other', password: 'secret' }));
        release();
        const error = await outcome;
        assert.ok(error instanceof OperationUncertainError, `expected uncertainty, received ${String(error)}`);
        assert.equal(error.operationId, operationId);
        assert.equal(framesOf(server, Operation.CreateStream).length, 1);
        await assert.rejects(client.resolveOperation(operationId), OperationNotRetainedError);
      } finally { release(); client.destroy(); await server.close(); }
    });
  }

  for (const bootstrapOnly of [false, true]) {
    it(`ends a sent mutation uncertain after an eviction on ${bootstrapOnly ? 'the bootstrap link' : 'a replica link'}`, async () => {
      const server = await startVsrServer((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
          socket.write(evictionFrame(EvictionReason.SessionError));
          return;
        }
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(frame, Operation.NonReplicated,
            metadataBody([{ port: bootstrapOnly ? 0 : server.port, replicaId: 0, role: 0 }])));
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
      try {
        await assert.rejects(client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0)), (error: unknown) =>
          error instanceof OperationUncertainError && error.cause instanceof VsrEvictionError);
        assert.equal(framesOf(server, Operation.CreateStream).length, 1);
      } finally { client.destroy(); await server.close(); }
    });
  }

  it('restarts a call that waits in its lane behind an evicted mutation under the next session', async () => {
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
        socket.write(framesOf(server, Operation.CreateStream).length === 1
          ? evictionFrame(EvictionReason.SessionError) : provedReply(frame));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    const identity = (frame: Buffer): Buffer => frame.subarray(REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16);
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      const sent = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      const waiting = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      await assert.rejects(sent, (error: unknown) =>
        error instanceof OperationUncertainError && error.cause instanceof VsrEvictionError);
      await waiting;
      const registers = framesOf(server, Operation.Register);
      const creates = framesOf(server, Operation.CreateStream);
      assert.equal(registers.length, 2);
      assert.equal(creates.length, 2);
      assert.deepEqual(identity(creates[0]), identity(registers[0]));
      assert.deepEqual(identity(creates[1]), identity(registers[1]));
    } finally { client.destroy(); await server.close(); }
  });

  it('ends a call that waits for a lost session with 58 at its deadline', async () => {
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
        socket.write(evictionFrame(EvictionReason.SessionError));
        return;
      }
      // The sign-in that would replace the session never ends.
      if (frame[REQUEST_OFFSET.operation] === Operation.Register && framesOf(server, Operation.Register).length > 1)
        return;
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      const sent = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      const waiting = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0), { deadline: Date.now() + 300 });
      await assert.rejects(sent, OperationUncertainError);
      await assert.rejects(waiting, (error: unknown) => error instanceof ResponseError &&
        error.errorCode === TRANSIENT_NOT_ACCEPTED && error.commandCode === COMMAND_CODE.CreateStream);
      assert.equal(framesOf(server, Operation.CreateStream).length, 1);
    } finally { client.destroy(); await server.close(); }
  });

  it('ends a call parked between attempts with 40 when the caller logs out and signs in as another user', async () => {
    let lookups = 0;
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.SendMessages) {
        socket.write(framesOf(server, Operation.SendMessages).length === 1
          ? replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), TRANSIENT_NOT_ACCEPTED) : provedReply(frame));
        return;
      }
      // The route lookup after the refusal never ends, so the call waits between attempts.
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute && ++lookups === 2) return;
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    try {
      await client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'first', password: 'secret' }));
      // The call ends while the logout runs, so its rejection needs a handler from the start.
      const parked = assert.rejects(client.sendCommand(COMMAND_CODE.SendMessages, explicitSend),
        (error: unknown) => error instanceof ResponseError &&
          error.errorCode === UNAUTHENTICATED && error.commandCode === COMMAND_CODE.SendMessages);
      await waitUntil(() => lookups === 2);
      await client.sendCommand(LOGOUT.code, LOGOUT.serialize());
      await client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'other', password: 'secret' }));
      await parked;
      assert.equal(readWireName(registerCredentialsBody(framesOf(server, Operation.Register).at(-1)!), 0).value, 'other');
      assert.equal(framesOf(server, Operation.SendMessages).length, 1);
    } finally { client.destroy(); await server.close(); }
  });

  it('ends a group poll with 40 when the caller logs out and signs in as another user while it runs', async () => {
    let assignments = 0;
    const server = await startVsrServer((frame, socket) => {
      // The first assignment read never ends, so the poll still runs when the caller signs in again.
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.SyncGroup && ++assignments === 1) return;
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    try {
      await client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'first', password: 'secret' }));
      const poll = assert.rejects(pollMessages(async () => client)({
        streamId: 1, topicId: 1, consumer: { kind: ConsumerKind.Group, id: 1 }, partitionId: null,
        pollingStrategy: PollingStrategy.Next, count: 1, autocommit: true
      }), (error: unknown) => error instanceof ResponseError && error.errorCode === UNAUTHENTICATED);
      await waitUntil(() => assignments === 1);
      await client.sendCommand(LOGOUT.code, LOGOUT.serialize());
      await client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'other', password: 'secret' }));
      await poll;
      assert.equal(assignments, 1);
      assert.equal(countCommand(server, COMMAND_CODE.PollMessages), 0);
    } finally { client.destroy(); await server.close(); }
  });

  it('ends a typed send with 40 when the caller logs out and signs in as another user before it sends again', async () => {
    let refuseFirstSend: (() => void) | undefined;
    let loggingOut = false;
    const server = await startVsrServer((frame, socket) => {
      const operation = frame[REQUEST_OFFSET.operation];
      if (operation === Operation.SendMessages && !refuseFirstSend) {
        refuseFirstSend = () => socket.write(replyFrame(frame, operation, Buffer.alloc(0), PARTITION_NOT_FOUND));
        return;
      }
      if (operation === Operation.Logout) {
        // The refusal starts a new partition lookup, which goes out before the logout ends the session.
        loggingOut = true;
        refuseFirstSend!();
        setTimeout(() => {
          loggingOut = false;
          singleNodeHandler(server.port)(frame, socket);
        }, LOGOUT_DELAY_MS);
        return;
      }
      if (loggingOut && frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) return;
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    try {
      await client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'first', password: 'secret' }));
      const send = assert.rejects(sendMessages(async () => client)({ streamId: 1, topicId: 2, messages: [{ payload: 'balanced' }] }),
        (error: unknown) => error instanceof ResponseError && error.errorCode === UNAUTHENTICATED);
      await waitUntil(() => refuseFirstSend !== undefined);
      await client.sendCommand(LOGOUT.code, LOGOUT.serialize());
      await client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'other', password: 'secret' }));
      await send;
      assert.equal(framesOf(server, Operation.SendMessages).length, 1);
    } finally { client.destroy(); await server.close(); }
  });

  it('ends a call that waits for its first sign-in with 58 at its deadline', async () => {
    // The sign-in never ends.
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] !== Operation.Register) singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    try {
      await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0), { deadline: Date.now() + 300 }),
        (error: unknown) => error instanceof ResponseError && error.errorCode === TRANSIENT_NOT_ACCEPTED &&
          error.commandCode === COMMAND_CODE.GetStats);
      assert.equal(countCommand(server, COMMAND_CODE.GetStats), 0);
    } finally { client.destroy(); await server.close(); }
  });

  it('ends a call that waits for a refused sign-in with 40 and never signs in with those credentials again', async () => {
    const server = await startVsrServer((frame, socket) => {
      const operation = frame[REQUEST_OFFSET.operation];
      if (operation === Operation.CreateStream) {
        socket.write(evictionFrame(EvictionReason.SessionError));
        return;
      }
      // The sign-in that would replace the evicted session is refused.
      if (operation === Operation.Register && framesOf(server, Operation.Register).length > 1) {
        socket.write(replyFrame(frame, Operation.Register, Buffer.alloc(0), INVALID_CREDENTIALS));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      const sent = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      const waiting = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      await assert.rejects(sent, OperationUncertainError);
      await assert.rejects(waiting, (error: unknown) => error instanceof ResponseError &&
        error.errorCode === UNAUTHENTICATED && error.commandCode === COMMAND_CODE.CreateStream &&
        error.cause instanceof ResponseError && error.cause.errorCode === INVALID_CREDENTIALS);
      await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)), (error: unknown) =>
        error instanceof ResponseError && error.errorCode === UNAUTHENTICATED);
      assert.equal(framesOf(server, Operation.Register).length, 2);
      assert.equal(framesOf(server, Operation.CreateStream).length, 1);
    } finally { client.destroy(); await server.close(); }
  });

  for (const [name, command, payload, operation] of [
    ['a send', COMMAND_CODE.SendMessages, explicitSend, Operation.SendMessages],
    ['an offset store', COMMAND_CODE.StoreOffset, storeOffsetPayload, Operation.StoreConsumerOffset]
  ] as const) {
    for (const commits of [true, false]) {
      it(`replays ${name} refused with 57 under its request id until ${commits ? 'it commits' : 'its deadline'}`, async () => {
        const writes: Buffer[] = [];
        const cluster = await startPollCluster((frame, socket) => {
          if (frame[REQUEST_OFFSET.operation] !== operation) return false;
          writes.push(frame);
          socket.write(commits && writes.length === 2 ? provedReply(frame)
            : replyFrame(frame, operation, Buffer.alloc(0), TRANSIENT_NOT_COMMITTED));
          return true;
        });
        try {
          await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
          const pending = cluster.client.sendCommand(command, payload, { deadline: Date.now() + 500 });
          if (commits) {
            await pending;
            assert.equal(writes.length, 2);
          } else {
            await assert.rejects(pending, (error: unknown) => error instanceof OperationUncertainError &&
              error.operationId === writes[0].readBigUInt64LE(REQUEST_OFFSET.request) &&
              error.cause instanceof ResponseError && error.cause.errorCode === TRANSIENT_NOT_COMMITTED);
            assert.ok(writes.length > 1, `${writes.length} writes`);
          }
          for (const write of writes) assert.deepEqual(write, writes[0], 'a replay keeps its request');
        } finally { await cluster.close(); }
      });
    }
  }

  for (const [name, command, payload, operation] of [
    ['store offset', COMMAND_CODE.StoreOffset, storeOffsetPayload, Operation.StoreConsumerOffset],
    ['delete offset', COMMAND_CODE.DeleteConsumerOffset, deleteOffsetPayload, Operation.DeleteConsumerOffset],
    ['progress poll', COMMAND_CODE.PollMessages, groupPollPayload(), Operation.NonReplicated]
  ] as const) {
    it(`stamps the route context of the next session on a restarted ${name} and one queued in its lane`, async () => {
      let incarnation = 1n;
      let bindHeld = false;
      const isCall = (frame: Buffer): boolean => frame[REQUEST_OFFSET.operation] === operation &&
        (operation !== Operation.NonReplicated || frame.readUInt32LE(REQUEST_OFFSET.reserved) === command);
      const cluster = await startPollCluster((frame, socket) => {
        // The first bind never ends, so the call holds its frame until the session ends.
        if (!bindHeld && frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession) {
          bindHeld = true;
          return true;
        }
        if (!isCall(frame)) return false;
        socket.write(stampedContext(frame).incarnation === incarnation
          ? replyFrame(frame, operation, metadataResult(0))
          : replyFrame(frame, operation, Buffer.alloc(0), HISTORY_UNAVAILABLE));
        return true;
      }, (frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
          incarnation = 2n;
          socket.write(evictionFrame(EvictionReason.SessionError));
          return true;
        }
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
        socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(1n, incarnation)));
        return true;
      });
      try {
        await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
        const routed = once(cluster.client, 'partitionRoute');
        const restarted = cluster.client.sendCommand(command, payload);
        await routed;
        await new Promise<void>((resolve) => setImmediate(resolve));
        // The second call takes the cached route and waits in the lane of the first one, which it
        // enters only after the restart.
        const queued = cluster.client.sendCommand(command, payload);
        await new Promise<void>((resolve) => setImmediate(resolve));
        await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0)),
          OperationUncertainError);
        await Promise.all([restarted, queued]);
        assert.deepEqual(cluster.primary.frames.filter(isCall).map((frame) => stampedContext(frame).incarnation),
          [2n, 2n]);
      } finally { await cluster.close(); }
    });
  }

  for (const ending of ['stale client', 'eviction', 'refused sign-in'] as const) {
    it(`ends a mutation uncertain when its reply is lost and the session then ends by ${ending}`, async () => {
      let binds = 0;
      const server = await startVsrServer((frame, socket) => {
        const operation = frame[REQUEST_OFFSET.operation];
        if (operation === Operation.CreateStream) {
          if (framesOf(server, Operation.CreateStream).length === 1) socket.destroy();
          else socket.write(ending === 'stale client'
            ? replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), STALE_CLIENT)
            : evictionFrame(EvictionReason.SessionError));
          return;
        }
        // Only the first bind finds the session, unless the sign-in fails.
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession &&
            (ending === 'refused sign-in' || ++binds > 1)) {
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), UNAUTHENTICATED));
          return;
        }
        if (operation === Operation.Register && ending === 'refused sign-in' &&
            framesOf(server, Operation.Register).length > 1) {
          socket.write(replyFrame(frame, Operation.Register, Buffer.alloc(0), INVALID_CREDENTIALS));
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream({
        ...vsrConfig(server.port), heartbeatInterval: 0, reconnect: { enabled: true, interval: 10, maxRetries: 2 }
      });
      let resets = 0;
      client.on('sessionReset', () => { resets += 1; });
      try {
        await client.authenticate(vsrConfig(server.port).credentials);
        const outcome = await client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0))
          .catch((error: unknown) => error);
        const creates = framesOf(server, Operation.CreateStream);
        const id = creates[0].readBigUInt64LE(REQUEST_OFFSET.request);
        assert.ok(outcome instanceof OperationUncertainError && outcome.operationId === id, `received ${String(outcome)}`);
        assert.equal(creates.length, ending === 'refused sign-in' ? 1 : 2);
        for (const create of creates) assert.deepEqual(create, creates[0]);
        await waitUntil(() => resets === 1);
        await assert.rejects(client.resolveOperation(id), OperationNotRetainedError);
      } finally { client.destroy(); await server.close(); }
    });
  }

  for (const bootstrapOnly of [false, true]) {
    for (const written of ['in-flight', 'retained'] as const) {
      for (const reason of [EvictionReason.StaleClient, EvictionReason.NoSession]) {
      it(`binds the same session and proves the ${written} mutation after eviction ${reason} on ${bootstrapOnly ? 'the bootstrap link' : 'a replica link'}`, async () => {
        let link!: Socket;
        const server = await startVsrServer((frame, socket) => {
          if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
            link = socket;
            if (framesOf(server, Operation.CreateStream).length > 1) socket.write(provedReply(frame));
            else if (written === 'in-flight') socket.write(evictionFrame(reason));
            else socket.write(replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), 85));
            return;
          }
          if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetClusterMetadata) {
            socket.write(replyFrame(frame, Operation.NonReplicated,
              metadataBody([{ port: bootstrapOnly ? 0 : server.port, replicaId: 0, role: 0 }])));
            return;
          }
          singleNodeHandler(server.port)(frame, socket);
        });
        const client = new CommandResponseStream({
          ...vsrConfig(server.port), heartbeatInterval: 0, reconnect: { enabled: true, interval: 10, maxRetries: 1 }
        });
        let resets = 0;
        client.on('sessionReset', () => { resets += 1; });
        try {
          const created = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
          if (written === 'retained') {
            let id!: bigint;
            await assert.rejects(created, (error: unknown) => {
              assert.ok(error instanceof OperationUncertainError); id = error.operationId!; return true;
            });
            const evicted = once(client, 'eviction');
            link.write(evictionFrame(reason));
            await evicted;
            await client.resolveOperation(id);
          } else await created;
          assert.equal(resets, 0);
          const registers = framesOf(server, Operation.Register);
          const binds = server.frames.filter((frame) =>
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession);
          assert.equal(registers.length, 1);
          assert.equal(binds.length, 1);
          assertSharedBinding(registers[0], binds[0]);
          const [original, replay] = framesOf(server, Operation.CreateStream);
          assert.deepEqual(replay, original);
        } finally { client.destroy(); await server.close(); }
      });
    }
  }
  }

  for (const status of [STALE_CLIENT, UNAUTHENTICATED]) {
    const committed = true;
    for (const attempt of ['first attempt', 'retry', 'resolve'] as const) {
      it(`drops the session at once after a ${committed ? 'committed' : 'denied'} ${status} reply to a ${attempt}`, async () => {
        const server = await startVsrServer((frame, socket) => {
          if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
            if (attempt !== 'first attempt' && framesOf(server, Operation.CreateStream).length === 1)
              socket.write(replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), attempt === 'retry' ? 57 : 85));
            else socket.write(committed ? provedReply(frame, status)
              : replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), status));
            return;
          }
          singleNodeHandler(server.port)(frame, socket);
        });
        const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
        let resets = 0;
        client.on('sessionReset', () => { resets += 1; });
        try {
          let outcome = await client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0))
            .catch((error: unknown) => error);
          const id = framesOf(server, Operation.CreateStream)[0].readBigUInt64LE(REQUEST_OFFSET.request);
          if (attempt === 'resolve') {
            assert.ok(outcome instanceof OperationUncertainError && outcome.operationId === id);
            outcome = await client.resolveOperation(id).catch((error: unknown) => error);
          }
          assert.ok(committed || attempt === 'first attempt'
            ? outcome instanceof ResponseError && outcome.errorCode === status && outcome.committed === committed
            : outcome instanceof OperationUncertainError && outcome.operationId === id &&
              outcome.cause instanceof ResponseError && outcome.cause.errorCode === status,
          `received ${String(outcome)}`);
          assert.equal(resets, 1);
          const again = await client.resolveOperation(id).catch((error: unknown) => error);
          if (committed && attempt === 'resolve') assert.equal(again, outcome);
          else assert.ok(again instanceof OperationNotRetainedError, `received ${String(again)}`);
          assert.equal(framesOf(server, Operation.CreateStream).length, attempt === 'first attempt' ? 1 : 2);
          await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
          assert.equal(framesOf(server, Operation.Register).length, 2);
        } finally { client.destroy(); await server.close(); }
      });
    }
  }

  for (const live of [true, false]) {
    for (const attempt of ['first attempt', 'retry', 'resolve'] as const) {
      it(`lets the bind decide after a denied 30 reply to a ${attempt} on ${live ? 'a live' : 'an ended'} session`, async () => {
        const server = await startVsrServer((frame, socket) => {
          if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
            const creates = framesOf(server, Operation.CreateStream).length;
            if (attempt !== 'first attempt' && creates === 1)
              socket.write(replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), attempt === 'retry' ? 57 : 85));
            else if (creates <= 2)
              socket.write(replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), STALE_CLIENT));
            else socket.write(provedReply(frame));
            return;
          }
          if (!live && frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession) {
            socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), UNAUTHENTICATED));
            return;
          }
          singleNodeHandler(server.port)(frame, socket);
        });
        const client = new CommandResponseStream({
          ...vsrConfig(server.port), heartbeatInterval: 0, reconnect: { enabled: true, interval: 10, maxRetries: 1 }
        });
        let resets = 0;
        client.on('sessionReset', () => { resets += 1; });
        try {
          let outcome = await client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0))
            .catch((error: unknown) => error);
          const [original] = framesOf(server, Operation.CreateStream);
          const id = original.readBigUInt64LE(REQUEST_OFFSET.request);
          if (attempt === 'resolve') {
            assert.ok(outcome instanceof OperationUncertainError && outcome.operationId === id);
            outcome = await client.resolveOperation(id).catch((error: unknown) => error);
          }
          assert.ok(attempt === 'first attempt'
            ? outcome instanceof ResponseError && outcome.errorCode === STALE_CLIENT && !outcome.committed
            : outcome instanceof OperationUncertainError && outcome.operationId === id &&
              outcome.cause instanceof ResponseError && outcome.cause.errorCode === STALE_CLIENT,
          `received ${String(outcome)}`);
          assert.equal(resets, 0);
          if (live && attempt !== 'first attempt') {
            await client.resolveOperation(id);
            assert.deepEqual(framesOf(server, Operation.CreateStream)[2], original);
          } else {
            if (!live) await waitUntil(() => resets === 1);
            await assert.rejects(client.resolveOperation(id), OperationNotRetainedError);
          }
          await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
          const registers = framesOf(server, Operation.Register);
          const binds = server.frames.filter((frame) =>
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession);
          assert.equal(resets, live ? 0 : 1);
          assert.equal(registers.length, live ? 1 : 2);
          assert.equal(binds.length, 1);
          if (live) assertSharedBinding(registers[0], binds[0]);
        } finally { client.destroy(); await server.close(); }
      });
    }
  }

  for (const live of [true, false]) {
    it(`uses a bind to check an unauthenticated link on a ${live ? 'live' : 'lost'} session`, async () => {
      let refuses = true;
      const server = await startVsrServer((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
          socket.write(framesOf(server, Operation.CreateStream).length === 1
            ? replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), 85) : provedReply(frame));
          return;
        }
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if ((code === COMMAND_CODE.GetStats && refuses) || (!live && code === COMMAND_CODE.BindSession)) {
          refuses = false;
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), UNAUTHENTICATED));
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream({
        ...vsrConfig(server.port), heartbeatInterval: 0,
        reconnect: { enabled: true, interval: 1, maxRetries: 1 }
      });
      try {
        let id!: bigint;
        await assert.rejects(client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0)), (error: unknown) => {
          assert.ok(error instanceof OperationUncertainError); id = error.operationId!; return true;
        });
        // On a lost session the read waits for the next one.
        await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
        assert.equal(framesOf(server, Operation.Register).length, live ? 1 : 2);
        if (live) {
          await client.resolveOperation(id);
          const [original, replay] = framesOf(server, Operation.CreateStream);
          assert.deepEqual(replay, original);
        } else await assert.rejects(client.resolveOperation(id), OperationNotRetainedError);
      } finally { client.destroy(); await server.close(); }
    });
  }

  it('retains a canceled send, blocks its lane, and resolves the original frame', async () => {
    const cancel = new AbortController();
    const sends: Buffer[] = [];
    const cluster = await startPollCluster((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] !== Operation.SendMessages) return false;
      sends.push(frame);
      if (sends.length === 1) cancel.abort();
      else socket.write(provedReply(frame));
      return true;
    });
    try {
      let id!: bigint;
      await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend,
        { signal: cancel.signal, deadline: Date.now() + 1_000 }), (error: unknown) => {
          assert.ok(error instanceof OperationUncertainError);
          assert.equal(typeof error.operationId, 'bigint');
          id = error.operationId!;
          return true;
        });
      await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend),
        (error: unknown) => error instanceof LaneBlockedError && error.operationId === id);
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, sendTo(Partitioning.PartitionId(4)));
      await cluster.client.resolveOperation(id, { deadline: Date.now() + 1_000 });
      assert.deepEqual(sends[0], sends[2]);
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      assert.ok(sends[3].readBigUInt64LE(REQUEST_OFFSET.request) > id);
      await cluster.client.resolveOperation(id);
      assert.equal(sends.length, 4);
    } finally { await cluster.close(); }
  });

  for (const rejected of [false, true]) {
    it(`gives every resolver the ${rejected ? 'committed rejection' : 'commit'} that one replay proved`, async () => {
      const stores: Buffer[] = [];
      const cluster = await startPollCluster((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] !== Operation.StoreConsumerOffset) return false;
        stores.push(frame);
        socket.write(stores.length === 1 ? replyFrame(frame, Operation.StoreConsumerOffset, Buffer.alloc(0), 85)
          : provedReply(frame, rejected ? HISTORY_UNAVAILABLE : 0));
        return true;
      });
      try {
        let id!: bigint;
        await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload), (error: unknown) => {
          assert.ok(error instanceof OperationUncertainError); id = error.operationId!; return true;
        });
        const racing = await Promise.allSettled([cluster.client.resolveOperation(id), cluster.client.resolveOperation(id)]);
        const outcomes = [...racing, ...await Promise.allSettled([cluster.client.resolveOperation(id)])];
        assert.equal(stores.length, 2);
        assert.deepEqual(stores[0], stores[1]);
        const [first] = outcomes;
        if (rejected)
          assert.ok(first.status === 'rejected' && first.reason instanceof ResponseError &&
            first.reason.errorCode === HISTORY_UNAVAILABLE && first.reason.committed);
        else assert.equal(first.status, 'fulfilled');
        for (const outcome of outcomes) assert.deepEqual(outcome, first);
      } finally { await cluster.close(); }
    });
  }

  it('ends a resolver uncertain when its deadline passes behind a running replay', async () => {
    let answer!: () => void;
    let replayed!: () => void;
    const replaying = new Promise<void>((resolve) => { replayed = resolve; });
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
        if (framesOf(server, Operation.CreateStream).length === 1)
          socket.write(replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), 85));
        else {
          answer = () => socket.write(provedReply(frame));
          replayed();
        }
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    try {
      let id!: bigint;
      await assert.rejects(client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0)), (error: unknown) => {
        assert.ok(error instanceof OperationUncertainError); id = error.operationId!; return true;
      });
      const resolving = client.resolveOperation(id).then(() => undefined, (error: unknown) => error);
      await replaying;
      await assert.rejects(client.resolveOperation(id, { deadline: Date.now() + 20 }), (error: unknown) =>
        error instanceof OperationUncertainError && error.operationId === id);
      answer();
      assert.equal(await resolving, undefined);
      await client.resolveOperation(id);
      assert.equal(framesOf(server, Operation.CreateStream).length, 2);
    } finally { client.destroy(); await server.close(); }
  });

  it('keeps the newest proved outcomes up to the pending operation limit across a session reset', async () => {
    const stores: Buffer[] = [];
    const cluster = await startPollCluster((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] !== Operation.StoreConsumerOffset) return false;
      stores.push(frame);
      socket.write(stores.length % 2 === 1
        ? replyFrame(frame, Operation.StoreConsumerOffset, Buffer.alloc(0), 85) : provedReply(frame));
      return true;
    }, undefined, 'TCP', { maxPendingOperations: 1 });
    try {
      const ids: bigint[] = [];
      for (let operation = 0; operation < 2; operation += 1) {
        await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload), (error: unknown) => {
          assert.ok(error instanceof OperationUncertainError); ids.push(error.operationId!); return true;
        });
        await cluster.client.resolveOperation(ids[operation]);
      }
      await cluster.client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'other', password: 'secret' }));
      await cluster.client.resolveOperation(ids[1]);
      await assert.rejects(cluster.client.resolveOperation(ids[0]), OperationNotRetainedError);
      assert.equal(stores.length, 4);
    } finally { await cluster.close(); }
  });

  for (const proved of [true, false]) {
    it(`keeps an abandoned replay in its lane until it ends ${proved ? 'proved and keeps its outcome' : 'uncertain'}`, async () => {
      let answer!: () => void;
      let replayed!: () => void;
      const replaying = new Promise<void>((resolve) => { replayed = resolve; });
      const server = await startVsrServer((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
          const creates = framesOf(server, Operation.CreateStream).length;
          const uncertain = replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), 85);
          if (creates === 1) socket.write(uncertain);
          else if (creates === 2) {
            answer = () => socket.write(proved ? provedReply(frame) : uncertain);
            replayed();
          } else socket.write(provedReply(frame));
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
      try {
        let id!: bigint;
        await assert.rejects(client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0)), (error: unknown) => {
          assert.ok(error instanceof OperationUncertainError); id = error.operationId!; return true;
        });
        const resolving = client.resolveOperation(id).then(() => undefined, (error: unknown) => error);
        await replaying;
        client.abandonOperation(id);
        const next = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
        await new Promise<void>((resolve) => setTimeout(resolve, 50));
        assert.equal(framesOf(server, Operation.CreateStream).length, 2, 'the replay keeps its lane');
        answer();
        const outcome = await resolving;
        await next;
        if (proved) {
          assert.equal(outcome, undefined);
          await client.resolveOperation(id);
        } else {
          assert.ok(outcome instanceof OperationUncertainError && outcome.operationId === id);
          await assert.rejects(client.resolveOperation(id), OperationNotRetainedError);
        }
        const creates = framesOf(server, Operation.CreateStream);
        assert.equal(creates.length, 3);
        assert.ok(creates[2].readBigUInt64LE(REQUEST_OFFSET.request) > id);
      } finally { client.destroy(); await server.close(); }
    });
  }

  for (const rejected of [false, true]) {
    it(`signs in with the ${rejected ? 'old' : 'new'} password after a resolve proves a ${rejected ? 'rejected' : 'committed'} change`, async () => {
      let session!: Socket;
      const server = await startVsrServer((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.ChangePassword) {
          session = socket;
          socket.write(framesOf(server, Operation.ChangePassword).length === 1
            ? replyFrame(frame, Operation.ChangePassword, Buffer.alloc(0), 85)
            : provedReply(frame, rejected ? INVALID_CREDENTIALS : 0));
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
      try {
        let id!: bigint;
        await assert.rejects(client.sendCommand(CHANGE_PASSWORD.code, CHANGE_PASSWORD.serialize({
          userId: 7, currentPassword: 'iggy', newPassword: 'second'
        })), (error: unknown) => {
          assert.ok(error instanceof OperationUncertainError); id = error.operationId!; return true;
        });
        const resolved = client.resolveOperation(id);
        if (rejected)
          await assert.rejects(resolved, (error: unknown) =>
            error instanceof ResponseError && error.errorCode === INVALID_CREDENTIALS);
        else await resolved;
        const reset = once(client, 'sessionReset');
        session.write(evictionFrame(EvictionReason.SessionError));
        await reset;
        await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
        const registers = framesOf(server, Operation.Register);
        assert.equal(registers.length, 2);
        const credentials = registerCredentialsBody(registers[1]);
        const username = readWireName(credentials, 0);
        assert.equal(username.value, 'iggy');
        assert.equal(readWireName(credentials, username.next).value, rejected ? 'iggy' : 'second');
      } finally { client.destroy(); await server.close(); }
    });
  }

  for (const marker of [0, 2]) {
    it(`keeps an uncertain operation retained after unproved marker ${marker}`, async () => {
      let calls = 0;
      const cluster = await startPollCluster((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] !== Operation.StoreConsumerOffset) return false;
        calls += 1;
        socket.write(calls === 1 ? replyFrame(frame, Operation.StoreConsumerOffset, Buffer.alloc(0), 57)
          : provedReply(frame, 0, calls === 2 ? marker : 1));
        return true;
      });
      try {
        let id!: bigint;
        await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload), (error: unknown) => {
          assert.ok(error instanceof OperationUncertainError); id = error.operationId!; return true;
        });
        await cluster.client.resolveOperation(id);
        assert.equal(calls, 3);
      } finally { await cluster.close(); }
    });
  }

  for (const field of ['client', 'request checksum', 'incarnation', 'op', 'commit', 'zero op'] as const) {
    for (const result of [0, 40]) {
      it(`rejects contradictory ${field} proof with result ${result} before session effects`, async () => {
        let calls = 0;
        const cluster = await startPollCluster((frame, socket) => {
          if (frame[REQUEST_OFFSET.operation] !== Operation.StoreConsumerOffset) return false;
          calls += 1;
          if (calls === 1) socket.write(replyFrame(frame, Operation.StoreConsumerOffset, Buffer.alloc(0), 57));
          else {
            const response = provedReply(frame, calls === 2 ? result : 0);
            if (calls === 2) {
              if (field === 'client') response[REPLY_OFFSET.client] ^= 1;
              if (field === 'request checksum') response[REPLY_OFFSET.requestChecksum] ^= 1;
              if (field === 'incarnation') response[REPLY_OFFSET.partitionIncarnation] ^= 1;
              if (field === 'op') response.writeBigUInt64LE(2n, REPLY_OFFSET.op);
              if (field === 'commit') response.writeBigUInt64LE(2n, REPLY_OFFSET.commit);
              if (field === 'zero op') { response.writeBigUInt64LE(0n, REPLY_OFFSET.op); response.writeBigUInt64LE(0n, REPLY_OFFSET.commit); }
            }
            socket.write(response);
          }
          return true;
        });
        try {
          let id!: bigint;
          await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload), (error: unknown) => {
            assert.ok(error instanceof OperationUncertainError); id = error.operationId!; return true;
          });
          assert.equal(cluster.client.isAuthenticated, true);
          await cluster.client.resolveOperation(id);
          assert.equal(framesOf(cluster.coordinator, Operation.Register).length, 1);
        } finally { await cluster.close(); }
      });
    }
  }

  for (const uncertain of [false, true]) {
    it(`handles proved LifecycleBusy correctly ${uncertain ? 'after' : 'before'} uncertainty`, async () => {
      const mutations: Buffer[] = [];
      const cluster = await startPollCluster((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] !== Operation.StoreConsumerOffset) return false;
        mutations.push(frame);
        if (uncertain && mutations.length === 1)
          socket.write(replyFrame(frame, Operation.StoreConsumerOffset, Buffer.alloc(0), 57));
        else socket.write(provedReply(frame, mutations.length === (uncertain ? 2 : 1) ? 88 : 0));
        return true;
      });
      try {
        const call = cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload);
        if (uncertain) await assert.rejects(call, (error: unknown) => error instanceof ResponseError && error.errorCode === 88);
        else await call;
        assert.equal(mutations.length, 2);
        assert.equal(mutations[0].readBigUInt64LE(REQUEST_OFFSET.request) === mutations[1].readBigUInt64LE(REQUEST_OFFSET.request), uncertain);
        await cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload);
        assert.equal(mutations.length, 3);
      } finally { await cluster.close(); }
    });
  }

  it('never starts a fresh identity for OperationNotRetained without abandonment', async () => {
    const mutations: Buffer[] = [];
    let allow = false;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] !== Operation.StoreConsumerOffset) return false;
      mutations.push(frame);
      socket.write(allow ? provedReply(frame) : replyFrame(frame, Operation.StoreConsumerOffset, Buffer.alloc(0), 85));
      return true;
    });
    try {
      let id!: bigint;
      await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload), (error: unknown) => {
        assert.ok(error instanceof OperationUncertainError); id = error.operationId!; return true;
      });
      await assert.rejects(cluster.client.resolveOperation(id), OperationUncertainError);
      assert.deepEqual(mutations[0], mutations[1]);
      cluster.client.abandonOperation(id);
      allow = true;
      await cluster.client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload);
      assert.ok(mutations[2].readBigUInt64LE(REQUEST_OFFSET.request) > id);
    } finally { await cluster.close(); }
  });

  for (const strategy of ['auto-commit', 'Next'] as const) {
    for (const failure of ['not committed', 'cancel'] as const) {
      it(`ends uncertain ${strategy} polls with 57 after ${failure} and releases the transient lane`, async () => {
        const cancel = new AbortController();
        let polls = 0;
        const cluster = await startPollCluster((frame, socket) => {
          if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages) return false;
          polls += 1;
          if (failure === 'cancel') cancel.abort();
          else socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 57));
          return true;
        });
        const body = POLL_MESSAGES.serialize({ streamId: 1, topicId: 2, partitionId: 3,
          consumer: { kind: ConsumerKind.Single, id: 1 }, count: 1,
          pollingStrategy: strategy === 'Next' ? PollingStrategy.Next : PollingStrategy.First,
          autocommit: strategy === 'auto-commit' });
        try {
          await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.PollMessages, body,
            { signal: cancel.signal }), (error: unknown) => error instanceof ResponseError &&
            error.errorCode === TRANSIENT_NOT_COMMITTED);
          assert.equal(polls, 1);
          await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
        } finally { await cluster.close(); }
      });
    }
  }

  it('keeps pure reads retryable after an uncommitted response', async () => {
    let polls = 0;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.from('read'), ++polls === 1 ? 57 : 0));
      return true;
    });
    try {
      const body = POLL_MESSAGES.serialize({ streamId: 1, topicId: 2, partitionId: 3,
        consumer: { kind: ConsumerKind.Single, id: 1 }, count: 1, pollingStrategy: PollingStrategy.First, autocommit: false });
      assert.equal((await cluster.client.sendCommand(COMMAND_CODE.PollMessages, body)).data.toString(), 'read');
      assert.equal(polls, 2);
    } finally { await cluster.close(); }
  });

  it('attaches every known replica proactively without a partition call', async () => {
    const cluster = await startPollCluster();
    try {
      await cluster.client.authenticate({ username: 'iggy', password: 'iggy' });
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 0);
      assert.equal(framesOf(cluster.primary, Operation.Register).length, 0);
      assertSharedBinding(cluster.coordinator.frames[0], cluster.primary.frames[0]);
    } finally { await cluster.close(); }
  });
});


describe('native replica and selection lifecycle', () => {
  for (const partition of [Partitioning.Balanced, Partitioning.MessageKey('key'), Partitioning.PartitionId(0)]) {
    for (const uncertain of [false, true]) {
      it(`handles partition-not-found for kind ${partition.kind} with uncertainty ${uncertain}`, async () => {
        let count = 1;
        const sends: Buffer[] = [];
        // The metadata primary owns the partition, so its miss is final.
        const cluster = await startPollCluster(undefined, (frame, socket) => {
          if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
            const route = routingBody(1n, 0n, 0);
            route.writeUInt32LE(count, 20);
            socket.write(replyFrame(frame, Operation.NonReplicated, route));
            return true;
          }
          if (frame[REQUEST_OFFSET.operation] !== Operation.SendMessages) return false;
          sends.push(frame);
          count = 2;
          socket.write(uncertain && sends.length === 1
            ? replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), 57)
            : sends.length === (uncertain ? 2 : 1)
              ? replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), 3007)
              : provedReply(frame));
          return true;
        });
        try {
          const pending = sendMessages(async () => cluster.client)({
            streamId: 1, topicId: 2, messages: [{ payload: 'selected' }], partition
          });
          if (uncertain) {
            await assert.rejects(pending, OperationUncertainError);
            assert.equal(sends.length, 2);
            assert.deepEqual(sends[0], sends[1]);
            assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 1);
          } else if (partition.kind === Partitioning.PartitionId(0).kind) {
            await assert.rejects(pending, (error: unknown) => error instanceof ResponseError && error.errorCode === 3007);
            assert.equal(sends.length, 1);
          } else {
            await pending;
            assert.equal(sends.length, 2);
            assert.ok(sends[1].readBigUInt64LE(REQUEST_OFFSET.request) > sends[0].readBigUInt64LE(REQUEST_OFFSET.request));
            assert.ok(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute) >= 2);
          }
        } finally { await cluster.close(); }
      });
    }
  }

  it('reuses another authenticated link when metadata discovery is refused', async () => {
    let refuse = false;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetClusterMetadata) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, twoNodeMetadataBody(cluster.primary.port, cluster.coordinator.port)));
      return true;
    }, (frame, socket) => {
      if (!refuse || frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetClusterMetadata) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 58));
      return true;
    });
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
      refuse = true;
      await cluster.client.sendCommand(COMMAND_CODE.GetClusterMetadata, Buffer.alloc(0), { deadline: Date.now() + 1000 });
      assert.ok(countCommand(cluster.primary, COMMAND_CODE.GetClusterMetadata) >= 1);
      assert.equal(framesOf(cluster.coordinator, Operation.Register).length, 1);
      assert.equal(framesOf(cluster.primary, Operation.Register).length, 0);
    } finally { await cluster.close(); }
  });

  it('binds the same session after losing its first roster response', async () => {
    let reads = 0;
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetClusterMetadata && ++reads === 1) {
        socket.destroy();
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 15 });
    try {
      // SDK recovery may keep its bound session after discovery fails.
      await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)));
      await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
      await waitUntil(() => countCommand(server, COMMAND_CODE.Ping) >= 2);
      assert.equal(framesOf(server, Operation.Register).length, 1);
      assert.equal(countCommand(server, COMMAND_CODE.BindSession), 1);
      assertSharedBinding(framesOf(server, Operation.Register)[0], server.frames.find((frame) =>
        frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession)!);
    } finally { client.destroy(); await server.close(); }
  });

  /**
   * A client that signs in on `configured`, binds its session on `later` and
   * then loses the roster of SDK recovery, so it stays signed in without
   * links. Each later bind on an endpoint goes to its entry in `binds` first.
   */
  const startUnlinkedClient = async (config: Partial<ClientConfig> = {}) => {
    const binds: { configured?: FrameOverride, later?: FrameOverride } = {};
    const isBind = (frame: Buffer): boolean => frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession;
    let rosterReads = 0;
    let endSession = false;
    const later: VsrTestServer = await startVsrServer((frame, socket) => {
      if (!isBind(frame) || !binds.later?.(frame, socket)) singleNodeHandler(later.port)(frame, socket);
    });
    const configured: VsrTestServer = await startVsrServer((frame, socket) => {
      if (endSession && frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetStats) {
        endSession = false;
        socket.write(evictionFrame(EvictionReason.SessionError));
        return;
      }
      if (isBind(frame) && binds.configured?.(frame, socket)) return;
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetClusterMetadata)
        singleNodeHandler(configured.port)(frame, socket);
      else if (++rosterReads === 2) socket.destroy();
      else socket.write(replyFrame(frame, Operation.NonReplicated, twoNodeMetadataBody(later.port, configured.port)));
    });
    const credentials = { username: 'iggy', password: 'iggy' };
    const client = new CommandResponseStream({ ...vsrConfig(configured.port), heartbeatInterval: 0, ...config });
    const close = async (): Promise<void> => {
      client.destroy();
      await Promise.all([later.close(), configured.close()]);
    };
    try {
      await client.authenticate(credentials);
      await waitUntil(() => countCommand(later, COMMAND_CODE.BindSession) === 1);
      endSession = true;
      await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)), VsrEvictionError);
      await waitUntil(() => rosterReads === 2);
      await (client as unknown as { recoveryPromise?: Promise<void> }).recoveryPromise;
    } catch (error) {
      await close();
      throw error;
    }
    return { client, configured, later, binds, close };
  };
  const closesLink = (_frame: Buffer, socket: Socket): boolean => {
    socket.destroy();
    return true;
  };
  const isClosedLink = (error: unknown): boolean => error instanceof Error && error.message === CLOSED_LINK;
  const refusesBind = (frame: Buffer, socket: Socket): boolean => {
    socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), LIFECYCLE_BUSY));
    return true;
  };
  const isBindRefusal = (error: unknown): boolean => error instanceof ResponseError && error.errorCode === LIFECYCLE_BUSY;

  it('names no dial failure when the deadline ends the bind of a later endpoint', async (context) => {
    const unlinked = await startUnlinkedClient();
    try {
      for (const socket of unlinked.configured.sockets) socket.destroy();
      await unlinked.configured.close();
      unlinked.binds.later = () => true;
      context.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
      // The roster read dials the configured endpoint, which refuses, and then binds on the later one.
      const outcome = unlinked.client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0),
        { deadline: Date.now() + 2 * OPERATION_TIMEOUT_MS }).then(() => 'resolved', (error: unknown) => error);
      await waitUntilUnmocked(() => countCommand(unlinked.later, COMMAND_CODE.BindSession) === 2);
      context.mock.timers.tick(OPERATION_TIMEOUT_MS);
      const error = await outcome;
      assert.ok(error instanceof ResponseError && error.errorCode === TRANSIENT_NOT_ACCEPTED, String(error));
    } finally {
      context.mock.timers.reset();
      await unlinked.close();
    }
  });

  it('refuses a call as not accepted when the retry budget ends after the bind deadline of a dialed endpoint', async (context) => {
    const reconnect = { enabled: true, interval: 1, maxRetries: 1 };
    const unlinked = await startUnlinkedClient({ reconnect });
    try {
      for (const socket of unlinked.configured.sockets) socket.destroy();
      await unlinked.configured.close();
      unlinked.binds.later = () => true;
      context.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
      const getStats = () => unlinked.client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0),
        { deadline: Date.now() + 2 * OPERATION_TIMEOUT_MS }).then(() => 'resolved', (error: unknown) => error);
      const first = getStats();
      await waitUntilUnmocked(() => countCommand(unlinked.later, COMMAND_CODE.BindSession) === 2);
      // No endpoint accepts a dial of the second pass.
      void unlinked.later.close();
      // The deadline of the first roster read ends with the bind, whose own deadline ends the first pass.
      context.mock.timers.tick(OPERATION_TIMEOUT_MS);
      await first;
      // A second roster read waits for the same bootstrap until its retry budget ends.
      const second = getStats();
      await new Promise<void>((resolve) => setImmediate(resolve));
      context.mock.timers.tick(reconnect.interval);
      const error = await second;
      assert.ok(error instanceof ResponseError && error.errorCode === TRANSIENT_NOT_ACCEPTED, String(error));
    } finally {
      context.mock.timers.reset();
      await unlinked.close();
    }
  });

  for (const [failure, bind, isFailure] of [
    ['refusal', refusesBind, isBindRefusal],
    ['lost link', closesLink, isClosedLink]
  ] as const) {
    it(`reports the bind ${failure} of a dialed endpoint over the dial failure of a later one`, async () => {
      const unlinked = await startUnlinkedClient();
      try {
        for (const socket of unlinked.later.sockets) socket.destroy();
        await unlinked.later.close();
        unlinked.binds.configured = bind;
        // The roster read dials the configured endpoint, which fails the bind, and then fails to dial the later one.
        await assert.rejects(unlinked.client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0),
          { deadline: Date.now() + 2_000 }), isFailure);
      } finally { await unlinked.close(); }
    });
  }

  it('reports the lost bind of a dialed endpoint when the deadline ends the later dials', async (context) => {
    // The default retry budget outlasts the deadline.
    const unlinked = await startUnlinkedClient({ reconnect: undefined });
    try {
      for (const socket of unlinked.later.sockets) socket.destroy();
      await unlinked.later.close();
      unlinked.binds.configured = (frame, socket) => {
        // No endpoint accepts a later dial, so the lost bind stays the last failure.
        void unlinked.configured.close();
        return closesLink(frame, socket);
      };
      const dials = context.mock.method(IggyConnection.prototype, 'connect');
      context.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
      const outcome = unlinked.client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0),
        { deadline: Date.now() + 2 * OPERATION_TIMEOUT_MS }).then(() => 'resolved', (error: unknown) => error);
      // The bind fails before the dial of the later endpoint starts.
      await waitUntilUnmocked(() => dials.mock.callCount() === 2);
      await dials.mock.calls[1].result?.catch(() => undefined);
      context.mock.timers.tick(OPERATION_TIMEOUT_MS);
      const error = await outcome;
      assert.ok(isClosedLink(error), String(error));
    } finally {
      context.mock.timers.reset();
      await unlinked.close();
    }
  });

  it('logs out a retained metadata lane before relogin and never reuses its operation id', async () => {
    let calls = 0;
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
        socket.write(replyFrame(frame, Operation.CreateStream, Buffer.alloc(0), ++calls === 1 ? 57 : 41));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      let id!: bigint;
      await assert.rejects(client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0)), (error: unknown) => {
        assert.ok(error instanceof OperationUncertainError);
        id = error.operationId!;
        return true;
      });
      await client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'other', password: 'secret' }));
      await assert.rejects(client.resolveOperation(id), OperationNotRetainedError);
      const controls = server.frames.filter((frame) => frame[REQUEST_OFFSET.operation] === Operation.Register || frame[REQUEST_OFFSET.operation] === Operation.Logout);
      assert.deepEqual(controls.map((frame) => frame[REQUEST_OFFSET.operation]), [Operation.Register, Operation.Logout, Operation.Register]);
      assert.ok(controls[1].readBigUInt64LE(REQUEST_OFFSET.request) > id);
    } finally { client.destroy(); await server.close(); }
  });

  it('does not start Register when explicit relogin loses the Logout outcome', async () => {
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.Logout) { socket.destroy(); return; }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      await assert.rejects(client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'other', password: 'secret' })),
        (error: unknown) => error instanceof ResponseError && error.errorCode === TRANSIENT_NOT_COMMITTED);
      assert.equal(framesOf(server, Operation.Register).length, 1);
      assert.equal(framesOf(server, Operation.Logout).length, 1);
    } finally { client.destroy(); await server.close(); }
  });

  it('preserves its watermark across reset and ignores a completed old-session receipt', async () => {
    let commit = 47n;
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.CreateStream) {
        const reply = provedReply(frame);
        reply.writeBigUInt64LE(commit, REPLY_OFFSET.op);
        reply.writeBigUInt64LE(commit, REPLY_OFFSET.commit);
        socket.write(reply);
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    let release!: () => void;
    const released = new Promise<void>((resolve) => { release = resolve; });
    let received!: () => void;
    const completedRead = new Promise<void>((resolve) => { received = resolve; });
    try {
      await client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      const link = (client as unknown as { replicas: Map<number, { connection: IggyConnection }> }).replicas.get(0)!.connection;
      const exchange = link.exchange.bind(link);
      link.exchange = async (...args): Promise<LinkAttempt> => {
        const reply = await exchange(...args);
        if (reply.response?.[REPLY_OFFSET.operation] === Operation.CreateStream) {
          received();
          await released;
        }
        return reply;
      };
      commit = 999n;
      const old = client.sendCommand(COMMAND_CODE.CreateStream, Buffer.alloc(0));
      const rejected = assert.rejects(old, OperationUncertainError);
      await completedRead;
      await client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'other', password: 'secret' }));
      release();
      await rejected;
      await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
      const latest = server.frames.at(-1)!;
      assert.equal(stampedContext(latest).metadataOp, 47n);
      const creates = framesOf(server, Operation.CreateStream);
      assert.ok(latest.readBigUInt64LE(REQUEST_OFFSET.request) > creates[1].readBigUInt64LE(REQUEST_OFFSET.request));
      assert.notDeepEqual(latest.subarray(REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16),
        creates[1].subarray(REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16));
    } finally { release(); client.destroy(); await server.close(); }
  });
});


describe('native retry pacing', () => {
  for (const retry of ['request', 'lookup', 'metadata', 'register'] as const) {
    for (const [sample, delays] of [
      [0, [0, 0, 0, 0, 0, 0, 0]],
      [0.5, [25, 50, 100, 200, 400, 500, 500]],
      [1 - Number.EPSILON, [50, 100, 200, 400, 800, 1_000, 1_000]]
    ] as const) {
      it(`backs off ${retry} retries with capped full jitter at random sample ${sample}`, async (context) => {
        const server = await startVsrServer((frame, socket) => singleNodeHandler(server.port)(frame, socket));
        const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
        const attempts: Buffer[] = [];
        let attempted!: () => void;
        const firstAttempt = new Promise<void>((resolve) => { attempted = resolve; });
        const random = context.mock.method(Math, 'random', () => sample);
        context.mock.method(IggyConnection.prototype, 'exchange', async (encode: () => Buffer): Promise<LinkAttempt> => {
          const frame = encode();
          const operation = frame[REQUEST_OFFSET.operation];
          const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
          const matches = retry === 'request' ? operation === Operation.StoreConsumerOffset
            : retry === 'lookup' ? code === COMMAND_CODE.GetPartitionRoute
              : retry === 'metadata' ? code === COMMAND_CODE.GetClusterMetadata : operation === Operation.Register;
          if (matches) { attempts.push(Buffer.from(frame)); attempted(); }
          const refused = matches && attempts.length <= delays.length;
          const body = operation === Operation.Register ? registerReplyBody()
            : code === COMMAND_CODE.GetClusterMetadata ? singleNodeMetadataBody(server.port)
              : code === COMMAND_CODE.GetPartitionRoute ? routingBody(1n, 1n, 0) : metadataResult(0);
          return { started: true, response: replyFrame(frame, operation,
            refused ? Buffer.alloc(0) : body, refused ? TRANSIENT_NOT_ACCEPTED : 0) };
        });
        try {
          context.mock.timers.enable({ apis: ['setTimeout'] });
          const pending = retry === 'metadata' || retry === 'register'
            ? client.authenticate(vsrConfig(server.port).credentials)
            : client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload);
          void pending.catch(() => undefined);
          await firstAttempt;
          await new Promise<void>((resolve) => setImmediate(resolve));
          for (const [index, delay] of delays.entries()) {
            assert.equal(attempts.length, index + 1);
            if (delay > 0) {
              context.mock.timers.tick(delay - 1);
              await new Promise<void>((resolve) => setImmediate(resolve));
              assert.equal(attempts.length, index + 1, 'a retry cannot start before its sampled delay');
            }
            context.mock.timers.tick(delay > 0 ? 1 : 0);
            await new Promise<void>((resolve) => setImmediate(resolve));
            assert.equal(attempts.length, index + 2, 'a retry starts at its sampled delay');
          }
          await pending;
          assert.equal(random.mock.callCount(), delays.length);
          if (retry !== 'lookup')
            for (const frame of attempts) assert.deepEqual(frame, attempts[0]);
        } finally { context.mock.timers.reset(); client.destroy(); await server.close(); }
      });
    }
  }

  it('refreshes immediately on a first 89 and backs off repeated refusals with the same identity', async (context) => {
    const server = await startVsrServer((frame, socket) => singleNodeHandler(server.port)(frame, socket));
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    const mutations: Buffer[] = [];
    const statuses = [89, 89, TRANSIENT_NOT_ACCEPTED, 89, 0];
    let lookups = 0;
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      const random = context.mock.method(Math, 'random', () => 1 - Number.EPSILON);
      context.mock.method(IggyConnection.prototype, 'exchange', async (encode: () => Buffer): Promise<LinkAttempt> => {
        const frame = encode();
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
          const route = routingBody(1n, 1n, 0);
          route.writeUInt32LE(lookups++, 16);
          return { started: true, response: replyFrame(frame, Operation.NonReplicated, route) };
        }
        assert.equal(frame[REQUEST_OFFSET.operation], Operation.StoreConsumerOffset);
        mutations.push(Buffer.from(frame));
        const status = statuses[mutations.length - 1];
        return { started: true, response: replyFrame(frame, Operation.StoreConsumerOffset,
          status === 0 ? metadataResult(0) : Buffer.alloc(0), status) };
      });
      context.mock.timers.enable({ apis: ['setTimeout'] });
      const pending = client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload);
      void pending.catch(() => undefined);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(mutations.length, 2, 'the first 89 refreshes and retries without a timer');
      assert.equal(lookups, 2);
      assert.equal(random.mock.callCount(), 1);
      for (const [delay, before, after] of [[50, 2, 3], [100, 3, 5]]) {
        context.mock.timers.tick(delay - 1);
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(mutations.length, before);
        context.mock.timers.tick(1);
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(mutations.length, after);
        assert.equal(lookups, after);
      }
      await pending;
      assert.equal(random.mock.callCount(), 2, 'a new 89 after a different refusal is immediate again');
      for (const frame of mutations) assert.deepEqual(frame, mutations[0]);
    } finally { context.mock.timers.reset(); client.destroy(); await server.close(); }
  });

  it('ends a call that gets 89 until its deadline with 58', async (context) => {
    const server = await startVsrServer((frame, socket) => singleNodeHandler(server.port)(frame, socket));
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    let lookups = 0;
    let refusals = 0;
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      context.mock.method(IggyConnection.prototype, 'exchange', async (encode: () => Buffer): Promise<LinkAttempt> => {
        const frame = encode();
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
          const route = routingBody(1n, 1n, 0);
          route.writeUInt32LE(lookups++, 16);
          return { started: true, response: replyFrame(frame, Operation.NonReplicated, route) };
        }
        refusals += 1;
        return { started: true, response: replyFrame(frame, Operation.StoreConsumerOffset, Buffer.alloc(0), 89) };
      });
      await assert.rejects(client.sendCommand(COMMAND_CODE.StoreOffset, storeOffsetPayload,
        { deadline: Date.now() + 300 }), (error: unknown) => error instanceof ResponseError &&
        error.errorCode === TRANSIENT_NOT_ACCEPTED && error.fromServer);
      assert.ok(refusals > 1);
    } finally { client.destroy(); await server.close(); }
  });

  for (const [sample, delays] of [
    [0, [0]],
    [0.5, [500, 1_000, 2_000, 4_000, 5_000, 5_000]],
    [1 - Number.EPSILON, [1_000]]
  ] as const) {
    it(`jitters failed bootstrap passes with a capped exponential delay at sample ${sample}`, async (context) => {
      const server = await startVsrServer((frame, socket) => singleNodeHandler(server.port)(frame, socket));
      const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0,
        reconnect: { enabled: true, interval: 1_000, maxRetries: delays.length } });
      const connect = IggyConnection.prototype.connect;
      let attempts = 0;
      const dials = context.mock.method(IggyConnection.prototype, 'connect', function (this: IggyConnection, signal?: AbortSignal) {
        return ++attempts <= delays.length ? Promise.reject(new Error('bootstrap unavailable')) : connect.call(this, signal);
      });
      const random = context.mock.method(Math, 'random', () => sample);
      try {
        context.mock.timers.enable({ apis: ['setTimeout'] });
        const pending = client.authenticate(vsrConfig(server.port).credentials);
        void pending.catch(() => undefined);
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(dials.mock.callCount(), 1, 'the first bootstrap attempt is immediate');
        for (const [index, delay] of delays.entries()) {
          if (delay > 0) {
            context.mock.timers.tick(delay - 1);
            await new Promise<void>((resolve) => setImmediate(resolve));
            assert.equal(dials.mock.callCount(), index + 1);
          }
          context.mock.timers.tick(delay > 0 ? 1 : 0);
          await new Promise<void>((resolve) => setImmediate(resolve));
          assert.equal(dials.mock.callCount(), index + 2);
        }
        context.mock.timers.reset();
        await pending;
        assert.equal(random.mock.callCount(), delays.length);
        assert.equal(framesOf(server, Operation.Register).length, 1);
      } finally { context.mock.timers.reset(); client.destroy(); await server.close(); }
    });
  }
});

describe('native replica resources and route floors', () => {
  it('reconnects an unused replica independently without registering another session', async () => {
    const cluster = await startPollCluster(undefined, undefined, 'TCP', {
      reconnect: { enabled: true, interval: 10, maxRetries: 2 }
    });
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
      for (const socket of cluster.primary.sockets) socket.destroy();
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 2);
      assert.equal(framesOf(cluster.primary, Operation.Register).length, 0);
      assert.equal(framesOf(cluster.coordinator, Operation.Register).length, 1);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.PollMessages), 0);
      assert.equal(cluster.primary.sockets.size, 1);
      await cluster.client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
    } finally { await cluster.close(); }
  });

  for (const [sample, delay] of [[0, 0], [0.5, 500], [1 - Number.EPSILON, 1_000]]) {
    it(`uses the full jitter range for the first background redial with random sample ${sample}`, async (context) => {
      const interval = 1_000;
      const dials = context.mock.method(IggyConnection.prototype, 'connect');
      const random = context.mock.method(Math, 'random', () => sample);
      const cluster = await startPollCluster(undefined, undefined, 'TCP', {
        reconnect: { enabled: true, interval, maxRetries: 2 }
      });
      try {
        await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
        await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
        assert.equal(dials.mock.callCount(), 2);
        assert.equal(random.mock.callCount(), 0, 'bootstrap and initial replica attachment stay immediate');
        const link = (cluster.client as unknown as {
          replicas: Map<number, { connection: IggyConnection }>
        }).replicas.get(7)!.connection;
        const disconnected = new Promise<void>((resolve) => { link.once('disconnected', resolve); });
        context.mock.timers.enable({ apis: ['setTimeout'] });
        for (const socket of cluster.primary.sockets) socket.destroy();
        await disconnected;
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(random.mock.callCount(), 1);
        assert.equal(dials.mock.callCount(), 2);
        if (delay > 0) {
          context.mock.timers.tick(delay - 1);
          await new Promise<void>((resolve) => setImmediate(resolve));
          assert.equal(dials.mock.callCount(), 2);
        }
        context.mock.timers.tick(delay > 0 ? 1 : 0);
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(dials.mock.callCount(), 3, 'the first redial starts at the sampled delay');
        context.mock.timers.reset();
        await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 2);
        assert.equal(framesOf(cluster.coordinator, Operation.Register).length, 1);
        assert.equal(framesOf(cluster.primary, Operation.Register).length, 0);
      } finally { context.mock.timers.reset(); await cluster.close(); }
    });
  }

  it('replaces one replica endpoint, preserves its session, and closes every link on destroy', async () => {
    let endpoint = 0;
    const replacement = await startVsrServer((frame, socket) => singleNodeHandler(replacement.port)(frame, socket));
    const cluster = await startPollCluster(undefined, (frame, socket) => {
      if (!endpoint || frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetClusterMetadata) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, twoNodeMetadataBody(endpoint, cluster.coordinator.port)));
      return true;
    });
    try {
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      const original = framesOf(cluster.primary, Operation.SendMessages)[0];
      endpoint = replacement.port;
      await cluster.client.sendCommand(COMMAND_CODE.GetClusterMetadata, Buffer.alloc(0));
      await waitUntil(() => countCommand(replacement, COMMAND_CODE.BindSession) === 1 && cluster.primary.sockets.size === 0);
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      const subsequent = framesOf(replacement, Operation.SendMessages)[0];
      assert.deepEqual(stampedContext(subsequent), stampedContext(original));
      assert.deepEqual(subsequent.subarray(REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16),
        original.subarray(REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16));
      assert.equal(framesOf(replacement, Operation.Register).length, 0);
      cluster.client.destroy();
      await waitUntil(() => replacement.sockets.size === 0 && cluster.coordinator.sockets.size === 0);
    } finally { await cluster.close(); await replacement.close(); }
  });

  it('keeps healthy replica heartbeats running while another replica is unavailable', async () => {
    const cluster = await startPollCluster(undefined, undefined, 'TCP', {
      heartbeatInterval: 15, reconnect: { enabled: true, interval: 10, maxRetries: 2 }
    });
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
      for (const socket of cluster.primary.sockets) socket.destroy();
      await cluster.primary.close();
      await waitUntil(() => countCommand(cluster.coordinator, COMMAND_CODE.Ping) >= 2);
      await cluster.client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
      assert.equal(framesOf(cluster.coordinator, Operation.Register).length, 1);
      cluster.client.destroy();
      await waitUntil(() => cluster.coordinator.sockets.size === 0);
      const pings = countCommand(cluster.coordinator, COMMAND_CODE.Ping);
      await new Promise<void>((resolve) => setTimeout(resolve, 45));
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.Ping), pings);
    } finally { await cluster.close(); }
  });

  it('restarts a replica link after its retry budget on each heartbeat until destroy', async () => {
    let down = false;
    const server = await startVsrServer((frame, socket) => {
      if (down) { socket.destroy(); return; }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 20,
      reconnect: { enabled: true, interval: 1, maxRetries: 1 } });
    const bindings = () => countCommand(server, COMMAND_CODE.BindSession);
    const outage = async () => {
      const before = bindings();
      down = true;
      for (const socket of server.sockets) socket.destroy();
      // One budget allows a single redial, so a third bind attempt proves a restart.
      await waitUntil(() => bindings() >= before + 3);
    };
    try {
      await client.authenticate(vsrConfig(server.port).credentials);
      await outage();
      down = false;
      // A call fails at once while every link is stopped, so wait until a restart binds.
      const restarted = bindings();
      await waitUntil(() => bindings() > restarted);
      await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0), { deadline: Date.now() + 2_000 });
      const register = framesOf(server, Operation.Register);
      assert.equal(register.length, 1);
      assertSharedBinding(register[0], server.frames.findLast((frame) =>
        frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession)!);

      await outage();
      client.destroy();
      await waitUntil(() => server.sockets.size === 0);
      const settled = server.frames.length;
      await new Promise<void>((resolve) => setTimeout(resolve, 100));
      assert.equal(server.frames.length, settled);
    } finally { client.destroy(); await server.close(); }
  });

  it('never restarts a stopped replica link when reconnection is disabled', async () => {
    const cluster = await startPollCluster(undefined, undefined, 'TCP', { heartbeatInterval: 20 });
    const pings = () => countCommand(cluster.coordinator, COMMAND_CODE.Ping);
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
      for (const socket of cluster.primary.sockets) socket.destroy();
      const before = pings();
      await waitUntil(() => pings() >= before + 3);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.BindSession), 1);
    } finally { await cluster.close(); }
  });

  it('never restarts a stopped replica link after logout', async (context) => {
    let down = false;
    const cluster = await startPollCluster((_frame, socket) => {
      if (!down) return false;
      socket.destroy();
      return true;
    }, undefined, 'TCP', { heartbeatInterval: 20, reconnect: { enabled: true, interval: 1, maxRetries: 1 } });
    const bindings = () => countCommand(cluster.primary, COMMAND_CODE.BindSession);
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await waitUntil(() => bindings() === 1);
      down = true;
      for (const socket of cluster.primary.sockets) socket.destroy();
      await waitUntil(() => bindings() >= 2);
      // After logout no session is left to bind, so count dials, not binds.
      const dials = context.mock.method(IggyConnection.prototype, 'connect');
      await cluster.client.sendCommand(LOGOUT.code, LOGOUT.serialize());
      const settled = dials.mock.callCount();
      await new Promise<void>((resolve) => setTimeout(resolve, 100));
      assert.equal(dials.mock.callCount(), settled);
    } finally { await cluster.close(); }
  });

  /** A heartbeat interval that never ticks within a test. */
  const idleHeartbeat = 60_000;
  const stoppingClient = (port: number, heartbeatInterval: number): CommandResponseStream =>
    new CommandResponseStream({ ...vsrConfig(port), heartbeatInterval,
      reconnect: { enabled: true, interval: 1, maxRetries: 1 } });
  /** Closes a single node and waits until its link used the retry budget. */
  const stopEveryLink = async (client: CommandResponseStream, server: VsrTestServer): Promise<void> => {
    for (const socket of server.sockets) socket.destroy();
    await server.close();
    const slots = (client as unknown as { replicas: Map<number, { failure?: unknown }> }).replicas;
    await waitUntil(() => slots.size > 0 && [...slots.values()].every((slot) => slot.failure !== undefined));
  };
  const isExhausted = (error: unknown): boolean => error instanceof Error &&
    !(error instanceof NotConnectedError) && error.message === 'replica reconnect attempts exhausted';

  for (const heartbeat of [true, false]) {
    for (const command of [COMMAND_CODE.GetStats, COMMAND_CODE.SendMessages]) {
      it(`fails command ${command} at once when every link used its budget with heartbeats ${heartbeat ? 'on' : 'off'}`,
        async (context) => {
          const firstBackoff = 50;
          const server = await startVsrServer((frame, socket) => singleNodeHandler(server.port)(frame, socket));
          const client = stoppingClient(server.port, heartbeat ? idleHeartbeat : 0);
          try {
            await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
            await stopEveryLink(client, server);
            context.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
            const outcome = client.sendCommand(command, command === COMMAND_CODE.SendMessages ? explicitSend : Buffer.alloc(0))
              .then(() => 'resolved', (error: unknown) => error);
            await new Promise<void>((resolve) => setImmediate(resolve));
            // The send first refreshes the route of its stopped primary after one retry pause.
            context.mock.timers.tick(firstBackoff);
            const settled = await Promise.race([outcome, new Promise((resolve) => setImmediate(() => resolve('pending')))]);
            assert.ok(heartbeat ? settled instanceof NotConnectedError : isExhausted(settled), String(settled));
          } finally { context.mock.timers.reset(); client.destroy(); await server.close(); }
        });
    }
  }

  for (const toggle of ['off after the stop', 'off before the stop', 'off and on again'] as const) {
    const restarted = toggle === 'off and on again';
    it(`fails a call on a stopped link as ${restarted ? 'not connected' : 'exhausted'} when heartbeats go ${toggle}`,
      async () => {
        const server = await startVsrServer((frame, socket) => singleNodeHandler(server.port)(frame, socket));
        const client = stoppingClient(server.port, idleHeartbeat);
        try {
          await client.authenticate(vsrConfig(server.port).credentials);
          if (toggle === 'off before the stop') client.heartbeat(0);
          await stopEveryLink(client, server);
          if (toggle !== 'off before the stop') client.heartbeat(0);
          if (restarted) client.heartbeat(idleHeartbeat);
          await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)),
            restarted ? NotConnectedError : isExhausted);
        } finally { client.destroy(); await server.close(); }
      });
  }

  it('resets a replacement history view floor without accepting older metadata', async () => {
    const original = { incarnation: 17n, metadata: 10n, view: 12 };
    const replacement = { incarnation: 18n, metadata: 11n, view: 0 };
    let replaced = false;
    let lookups = 0;
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
        const current = replaced ? replacement : original;
        const metadata = ++lookups === 2 ? original.metadata - 1n : current.metadata;
        const route = routingBody(metadata, current.incarnation, 0);
        route.writeUInt32LE(current.view, 16);
        socket.write(replyFrame(frame, Operation.NonReplicated, route));
        return;
      }
      if (frame[REQUEST_OFFSET.operation] === Operation.SendMessages) {
        const stale = replaced && stampedContext(frame).incarnation === original.incarnation;
        socket.write(replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), stale ? HISTORY_UNAVAILABLE : 0));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream({ ...vsrConfig(server.port), heartbeatInterval: 0 });
    try {
      await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      replaced = true;
      await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend, { deadline: Date.now() + 500 });
      const sends = framesOf(server, Operation.SendMessages);
      assert.deepEqual(sends.map((frame) => stampedContext(frame).incarnation),
        [original.incarnation, original.incarnation, replacement.incarnation]);
      assert.equal(stampedContext(sends[2]).metadataOp, replacement.metadata);
      assert.equal(lookups, 3);
    } finally { client.destroy(); await server.close(); }
  });

  for (const status of [1009, 1010, 2010, 2011, 3007]) {
    it(`ignores a backup namespace miss ${status} while the primary materializes the route`, async (context) => {
      const lookups: number[] = [];
      let primaryLookups = 0;
      const cluster = await startPollCluster((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
        lookups.push(7);
        socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), status));
        return true;
      }, (frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
        lookups.push(0);
        socket.write(++primaryLookups === 1
          ? replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), TRANSIENT_NOT_ACCEPTED)
          : replyFrame(frame, Operation.NonReplicated, routingBody(1n)));
        return true;
      });
      try {
        await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
        await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
        context.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
        let backoffStarted!: () => void;
        const backoff = new Promise<void>((resolve) => { backoffStarted = resolve; });
        context.mock.method(Math, 'random', () => { backoffStarted(); return 1 - Number.EPSILON; });
        const pending = cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend, { deadline: Date.now() + 500 });
        await Promise.race([backoff, pending]);
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.deepEqual(lookups, [0, 7]);
        context.mock.timers.tick(50);
        await pending;
        assert.deepEqual(lookups, [0, 7, 0]);
        assert.equal(framesOf(cluster.coordinator, Operation.SendMessages).length, 0);
        assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 1);
      } finally { context.mock.timers.reset(); await cluster.close(); }
    });

    it(`returns a known metadata primary namespace miss ${status} without sending`, async (context) => {
      const cluster = await startPollCluster(undefined, (frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
        socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), status));
        return true;
      });
      try {
        await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
        await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
        context.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
        await assert.rejects(
          cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend, { deadline: Date.now() + 500 }),
          (error: unknown) => error instanceof ResponseError && error.errorCode === status &&
            error.commandCode === COMMAND_CODE.SendMessages
        );
        assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 1);
        assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetPartitionRoute), 0);
        assert.equal(framesOf(cluster.coordinator, Operation.SendMessages).length, 0);
        assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 0);
      } finally { context.mock.timers.reset(); await cluster.close(); }
    });
  }

  it('keeps backup namespace misses transient until the route deadline expires', async (context) => {
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), 2010));
      return true;
    }, (frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), TRANSIENT_NOT_ACCEPTED));
      return true;
    });
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
      context.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
      let backoffStarted!: () => void;
      const backoff = new Promise<void>((resolve) => { backoffStarted = resolve; });
      context.mock.method(Math, 'random', () => { backoffStarted(); return 1 - Number.EPSILON; });
      const routeTimeout = 25;
      const pending = cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend, { deadline: Date.now() + routeTimeout });
      await Promise.race([backoff, pending]);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 1);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetPartitionRoute), 1);
      const rejected = assert.rejects(pending,
        (error: unknown) => error instanceof ResponseError && error.errorCode === TRANSIENT_NOT_ACCEPTED
      );
      context.mock.timers.tick(routeTimeout);
      await rejected;
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 1);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetPartitionRoute), 1);
      assert.equal(framesOf(cluster.coordinator, Operation.SendMessages).length, 0);
      assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 0);
    } finally { context.mock.timers.reset(); await cluster.close(); }
  });

  const missingTopic = 2010;
  const isSendRefusal = (status: number) => (error: unknown): boolean =>
    error instanceof ResponseError && error.errorCode === status && error.commandCode === COMMAND_CODE.SendMessages;

  for (const status of [1009, 1010, 2010, 2011, 3007]) {
    it(`retries a backup primary namespace miss ${status} with a route from the metadata primary`, async () => {
      const sends: Buffer[] = [];
      const cluster = await startPollCluster((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] !== Operation.SendMessages) return false;
        sends.push(frame);
        socket.write(replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), sends.length === 1 ? status : 0));
        return true;
      });
      try {
        await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
        assert.equal(sends.length, 2);
        assert.deepEqual(sends[1], sends[0], 'the retry resends the same request');
        assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 2);
        assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetPartitionRoute), 0);
      } finally { await cluster.close(); }
    });
  }

  for (const proved of [true, false]) {
    it(`resends an uncertain send unchanged after backup primary namespace misses until ${
      proved ? 'a proof' : 'its deadline'}`, async () => {
      const sends: Buffer[] = [];
      const cluster = await startPollCluster((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] !== Operation.SendMessages) return false;
        sends.push(frame);
        socket.write(sends.length === 1
          ? replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), TRANSIENT_NOT_COMMITTED)
          : proved && sends.length === 3 ? provedReply(frame)
            : replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), missingTopic));
        return true;
      });
      try {
        const pending = cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend,
          { deadline: Date.now() + (proved ? 2_000 : 500) });
        if (proved) {
          await pending;
          assert.equal(sends.length, 3);
          assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 2);
        } else {
          await assert.rejects(pending, (error: unknown) => error instanceof OperationUncertainError &&
            error.operationId === sends[0].readBigUInt64LE(REQUEST_OFFSET.request) &&
            isSendRefusal(TRANSIENT_NOT_ACCEPTED)(error.cause));
        }
        for (const send of sends) assert.deepEqual(send, sends[0], 'an uncertain send keeps its request');
      } finally { await cluster.close(); }
    });
  }

  it('reports backup primary namespace misses past the deadline as not accepted', async (context) => {
    const cluster = await startPollCluster((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] !== Operation.SendMessages) return false;
      socket.write(replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), missingTopic));
      return true;
    });
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
      context.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
      let backoffStarted!: () => void;
      const nextBackoff = () => new Promise<void>((resolve) => { backoffStarted = resolve; });
      context.mock.method(Math, 'random', () => { backoffStarted(); return 1 - Number.EPSILON; });
      const firstBackoff = 50;
      const sendTimeout = 500;
      let backoff = nextBackoff();
      const pending = cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend,
        { deadline: Date.now() + sendTimeout });
      await Promise.race([backoff, pending]);
      backoff = nextBackoff();
      context.mock.timers.tick(firstBackoff);
      await Promise.race([backoff, pending]);
      assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 2);
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 2);
      const rejected = assert.rejects(pending, isSendRefusal(TRANSIENT_NOT_ACCEPTED));
      context.mock.timers.tick(sendTimeout);
      await rejected;
      assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 2);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetPartitionRoute), 0);
    } finally { context.mock.timers.reset(); await cluster.close(); }
  });

  it('keeps a namespace miss final when the metadata primary owns the partition', async () => {
    const cluster = await startPollCluster(undefined, (frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
        socket.write(replyFrame(frame, Operation.NonReplicated,
          routingBody(frame.readBigUInt64LE(REQUEST_OFFSET.minimumMetadataOp) || 1n, 0n, 0)));
        return true;
      }
      if (frame[REQUEST_OFFSET.operation] !== Operation.SendMessages) return false;
      socket.write(replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), missingTopic));
      return true;
    });
    try {
      await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend,
        { deadline: Date.now() + 2_000 }), isSendRefusal(missingTopic));
      assert.equal(framesOf(cluster.coordinator, Operation.SendMessages).length, 1);
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 1);
      // A roster read after the miss confirms the metadata primary.
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata), 2);
    } finally { await cluster.close(); }
  });

  it('ends with the namespace miss of the metadata primary lookup after a backup primary miss', async () => {
    let lookups = 0;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] !== Operation.SendMessages) return false;
      socket.write(replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), missingTopic));
      return true;
    }, (frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute || ++lookups === 1)
        return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), missingTopic));
      return true;
    });
    try {
      await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend,
        { deadline: Date.now() + 2_000 }), isSendRefusal(missingTopic));
      assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 1);
      assert.equal(lookups, 2);
    } finally { await cluster.close(); }
  });

  for (const cached of [false, true]) {
    it(`ends with the namespace miss of the elected metadata primary while the believed one has no link, ${
      cached ? 'cached' : 'new'} route`, async () => {
      let elected = false;
      const cluster = await startPollCluster((frame, socket) => {
        if (!elected) return false;
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(frame, Operation.NonReplicated, metadataBody([
            { port: cluster.coordinator.port, replicaId: 0 }, { port: cluster.primary.port, replicaId: 7, role: 0 }])));
          return true;
        }
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute &&
            frame[REQUEST_OFFSET.operation] !== Operation.SendMessages) return false;
        socket.write(replyFrame(frame, frame[REQUEST_OFFSET.operation], Buffer.alloc(0), missingTopic));
        return true;
      });
      const slots = (cluster.client as unknown as { replicas: Map<number, { failure?: unknown }> }).replicas;
      try {
        await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
        await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
        if (cached) await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
        for (const socket of cluster.coordinator.sockets) socket.destroy();
        await cluster.coordinator.close();
        await waitUntil(() => slots.get(0)?.failure !== undefined);
        elected = true;
        await assert.rejects(cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend,
          { deadline: Date.now() + 2_000 }), isSendRefusal(missingTopic));
        assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetClusterMetadata), 1);
      } finally { await cluster.close(); }
    });
  }

  it('retries a backup namespace miss to the partition count lookup of a balanced send', async () => {
    let lookups = 0;
    const cluster = await startPollCluster((frame, socket) => {
      const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
      if (code === COMMAND_CODE.GetClusterMetadata) {
        socket.write(replyFrame(frame, Operation.NonReplicated,
          twoNodeMetadataBody(cluster.primary.port, cluster.coordinator.port)));
        return true;
      }
      if (code !== COMMAND_CODE.GetPartitionRoute) return false;
      socket.write(++lookups === 1
        ? replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), missingTopic)
        : replyFrame(frame, Operation.NonReplicated, routingBody(1n)));
      return true;
    });
    const slots = (cluster.client as unknown as { replicas: Map<number, { failure?: unknown }> }).replicas;
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
      for (const socket of cluster.coordinator.sockets) socket.destroy();
      await cluster.coordinator.close();
      await waitUntil(() => slots.get(0)?.failure !== undefined);
      await sendMessages(async () => cluster.client)({ streamId: 1, topicId: 2, messages: [{ payload: 'balanced' }] });
      assert.equal(lookups, 2);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetClusterMetadata), 1);
      assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 1);
    } finally { await cluster.close(); }
  });

  for (const missed of ['lookup', 'send'] as const) {
    it(`asks the elected metadata primary after the deposed one misses the namespace in a ${missed}`, async () => {
      let elected = false;
      const cluster = await startPollCluster((frame, socket) => {
        if (!elected || frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
        socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(2n)));
        return true;
      }, (frame, socket) => {
        if (!elected) return false;
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (code === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(frame, Operation.NonReplicated, metadataBody([
            { port: cluster.coordinator.port, replicaId: 0 }, { port: cluster.primary.port, replicaId: 7, role: 0 }])));
          return true;
        }
        if (code === COMMAND_CODE.GetPartitionRoute && missed === 'send') {
          socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(1n, 0n, 0)));
          return true;
        }
        if (code !== COMMAND_CODE.GetPartitionRoute && frame[REQUEST_OFFSET.operation] !== Operation.SendMessages)
          return false;
        socket.write(replyFrame(frame, frame[REQUEST_OFFSET.operation], Buffer.alloc(0), missingTopic));
        return true;
      });
      try {
        await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
        await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
        elected = true;
        await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend, { deadline: Date.now() + 2_000 });
        assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata), 2);
        assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 1);
      } finally { await cluster.close(); }
    });
  }

  for (const elected of [false, true]) {
    it(elected ? 'asks the elected metadata primary after the deposed one misses a raw route lookup'
      : 'keeps a raw route lookup miss final while the roster of the metadata primary names it', async () => {
      let signedIn = false;
      const cluster = await startPollCluster((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
        socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(1n)));
        return true;
      }, (frame, socket) => {
        if (!signedIn) return false;
        const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
        if (elected && code === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(frame, Operation.NonReplicated, metadataBody([
            { port: cluster.coordinator.port, replicaId: 0 }, { port: cluster.primary.port, replicaId: 7, role: 0 }])));
          return true;
        }
        if (code !== COMMAND_CODE.GetPartitionRoute) return false;
        socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), missingTopic));
        return true;
      });
      try {
        await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
        await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
        signedIn = true;
        const lookup = cluster.client.sendCommand(COMMAND_CODE.GetPartitionRoute, serializePartitionRoute(1, 2, 0),
          { deadline: Date.now() + 2_000 });
        if (elected) await lookup;
        else await assert.rejects(lookup, (error: unknown) => error instanceof ResponseError &&
          error.errorCode === missingTopic && error.commandCode === COMMAND_CODE.GetPartitionRoute);
        // The roster read on the link of the believed metadata primary decides the miss.
        assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata), 2);
        assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetPartitionRoute), elected ? 1 : 0);
      } finally { await cluster.close(); }
    });
  }

  it('reads the roster that decides a namespace miss only once and only on the link that missed', async () => {
    let signedIn = false;
    let rosterRefused = false;
    const cluster = await startPollCluster((frame, socket) => {
      const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
      if (code === COMMAND_CODE.GetClusterMetadata) {
        socket.write(replyFrame(frame, Operation.NonReplicated,
          twoNodeMetadataBody(cluster.primary.port, cluster.coordinator.port)));
        return true;
      }
      if (code !== COMMAND_CODE.GetPartitionRoute) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(1n)));
      return true;
    }, (frame, socket) => {
      if (!signedIn) return false;
      const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
      if (code === COMMAND_CODE.GetClusterMetadata && !rosterRefused) {
        rosterRefused = true;
        socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), TRANSIENT_NOT_ACCEPTED));
        return true;
      }
      if (code !== COMMAND_CODE.GetPartitionRoute) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), missingTopic));
      return true;
    });
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
      signedIn = true;
      // The refused roster read leaves the miss open, so the lookup asks the next replica.
      await cluster.client.sendCommand(COMMAND_CODE.GetPartitionRoute, serializePartitionRoute(1, 2, 0),
        { deadline: Date.now() + 2_000 });
      assert.ok(rosterRefused);
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetClusterMetadata), 2);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetClusterMetadata), 0);
      assert.equal(countCommand(cluster.primary, COMMAND_CODE.GetPartitionRoute), 1);
    } finally { await cluster.close(); }
  });

  it('starts the next lookup pass at the metadata primary that a roster read names', async () => {
    let signedIn = false;
    let primaryLookups = 0;
    const lookups: number[] = [];
    const cluster = await startPollCluster((frame, socket) => {
      if (!signedIn || frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
      lookups.push(7);
      // The first refusal ends the first pass.
      socket.write(++primaryLookups === 1
        ? replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), TRANSIENT_NOT_ACCEPTED)
        : replyFrame(frame, Operation.NonReplicated, routingBody(1n)));
      return true;
    }, (frame, socket) => {
      if (!signedIn) return false;
      const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
      if (code === COMMAND_CODE.GetClusterMetadata) {
        socket.write(replyFrame(frame, Operation.NonReplicated, metadataBody([
          { port: cluster.coordinator.port, replicaId: 0 }, { port: cluster.primary.port, replicaId: 7, role: 0 }])));
        return true;
      }
      if (code !== COMMAND_CODE.GetPartitionRoute) return false;
      lookups.push(0);
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), missingTopic));
      return true;
    });
    try {
      await cluster.client.authenticate(vsrConfig(cluster.coordinator.port).credentials);
      await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.BindSession) === 1);
      signedIn = true;
      // The deposed metadata primary misses the namespace, and its roster names replica 7.
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend, { deadline: Date.now() + 2_000 });
      assert.deepEqual(lookups, [0, 7, 7]);
      assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 1);
    } finally { await cluster.close(); }
  });

  for (const refusal of ['stale', 'conflicting', 'refused'] as const) {
    it(`visits every ready replica when earlier route replies are ${refusal}`, async () => {
      const incarnation = 17n;
      const originalView = 10;
      const primaryReplica = 7;
      const lastReplica = 9;
      const lookups: number[] = [];
      let discover = false;
      let sends = 0;
      const handleRoute = (replica: number, frame: Buffer, socket: Socket): boolean => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
        if (discover) lookups.push(replica);
        if (discover && replica !== lastReplica && refusal === 'refused') {
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), TRANSIENT_NOT_ACCEPTED));
          return true;
        }
        const current = !discover || replica === lastReplica;
        const route = routingBody(1n, incarnation, !current && refusal === 'conflicting' ? 0 : primaryReplica);
        route.writeUInt32LE(!discover ? originalView : current ? originalView + 1
          : refusal === 'stale' ? originalView - 1 : originalView, 16);
        socket.write(replyFrame(frame, Operation.NonReplicated, route));
        return true;
      };
      const last = await startVsrServer((frame, socket) => {
        if (!handleRoute(lastReplica, frame, socket)) singleNodeHandler(last.port)(frame, socket);
      });
      const primary = await startVsrServer((frame, socket) => {
        if (handleRoute(primaryReplica, frame, socket)) return;
        if (frame[REQUEST_OFFSET.operation] === Operation.SendMessages && ++sends === 2) {
          discover = true;
          socket.write(replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), TRANSIENT_NOT_ACCEPTED));
          return;
        }
        singleNodeHandler(primary.port)(frame, socket);
      });
      const coordinator = await startVsrServer((frame, socket) => {
        if (handleRoute(0, frame, socket)) return;
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetClusterMetadata) {
          socket.write(replyFrame(frame, Operation.NonReplicated,
            threeNodeMetadataBody(coordinator.port, primary.port, last.port)));
          return;
        }
        singleNodeHandler(coordinator.port)(frame, socket);
      });
      const client = new CommandResponseStream({ ...vsrConfig(coordinator.port), heartbeatInterval: 0 });
      try {
        await client.authenticate(vsrConfig(coordinator.port).credentials);
        await waitUntil(() => countCommand(primary, COMMAND_CODE.BindSession) === 1 &&
          countCommand(last, COMMAND_CODE.BindSession) === 1);
        await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
        await client.sendCommand(COMMAND_CODE.SendMessages, explicitSend, { deadline: Date.now() + 500 });
        assert.deepEqual(lookups, [0, primaryReplica, lastReplica]);
        assert.equal(sends, 3);
      } finally {
        client.destroy();
        await Promise.all([coordinator.close(), primary.close(), last.close()]);
      }
    });
  }

  it('keeps a newer route usable after a late refusal from an older view', async () => {
    let view = 0;
    const polls: { frame: Buffer, socket: Socket }[] = [];
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetOffset) return false;
      polls.push({ frame, socket });
      if (polls.length > 2) socket.write(replyFrame(frame, Operation.NonReplicated));
      return true;
    }, (frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
      const route = routingBody(1n, 17n);
      route.writeUInt32LE(view, 16);
      socket.write(replyFrame(frame, Operation.NonReplicated, route));
      return true;
    });
    try {
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      const body = GET_OFFSET.serialize({ streamId: 1, topicId: 2, partitionId: 3,
        consumer: { kind: ConsumerKind.Single, id: 1 } });
      const deadline = Date.now() + 1_000;
      const first = cluster.client.sendCommand(COMMAND_CODE.GetOffset, body, { deadline });
      const second = cluster.client.sendCommand(COMMAND_CODE.GetOffset, body, { deadline });
      void first.catch(() => undefined);
      void second.catch(() => undefined);
      await waitUntil(() => polls.length === 2);
      view = 1;
      polls[0].socket.write(replyFrame(polls[0].frame, Operation.NonReplicated, Buffer.alloc(0), 89));
      await first;
      polls[1].socket.write(replyFrame(polls[1].frame, Operation.NonReplicated, Buffer.alloc(0), 89));
      await second;
      assert.equal(polls.length, 4);
      assert.equal(countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute), 3);
    } finally { await cluster.close(); }
  });

  it('shares accepted view floors across aliases and rejects conflicting equal-view primaries', async () => {
    const views = [10, 12, 11, 12, 13];
    let lookups = 0;
    const handleRoute = (frame: Buffer, socket: Socket): boolean => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute) return false;
      const route = routingBody(1n, 17n, lookups === 3 ? 0 : 7);
      route.writeUInt32LE(views[lookups++], 16);
      socket.write(replyFrame(frame, Operation.NonReplicated, route));
      return true;
    };
    const cluster = await startPollCluster(handleRoute, handleRoute);
    try {
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      const alias = SEND_MESSAGES.serialize({ streamId: 'stream', topicId: 'topic',
        messages: [{ payload: 'alias' }], partition: Partitioning.PartitionId(3) });
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, alias);
      await cluster.client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      assert.equal(lookups, 5);
      assert.equal(framesOf(cluster.primary, Operation.SendMessages).length, 3);
      assert.equal(framesOf(cluster.coordinator, Operation.SendMessages).length, 0);
    } finally { await cluster.close(); }
  });

  it('limits concurrent TCP plus TLS dials to four and attaches a healthy later replica', async () => {
    let active = 0;
    let peak = 0;
    const sockets = new Set<Socket>();
    const stalled: Server[] = [];
    for (let index = 0; index < 5; index += 1) {
      const server = createServer((socket) => {
        sockets.add(socket);
        active += 1;
        peak = Math.max(peak, active);
        socket.on('error', () => undefined);
        socket.once('close', () => { sockets.delete(socket); active -= 1; });
        socket.resume();
      });
      server.listen(0, '127.0.0.1');
      await once(server, 'listening');
      stalled.push(server);
    }
    const healthy = await startVsrServer((frame, socket) => singleNodeHandler(healthy.port)(frame, socket), 'TLS');
    const coordinator = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetClusterMetadata) {
        const nodes = [{ port: coordinator.port, replicaId: 0, role: 0 },
          ...stalled.map((server, index) => ({ port: (server.address() as AddressInfo).port, replicaId: index + 1, role: 1 })),
          { port: healthy.port, replicaId: 7, role: 1 }];
        socket.write(replyFrame(frame, Operation.NonReplicated, metadataBody(nodes)));
        return;
      }
      singleNodeHandler(coordinator.port)(frame, socket);
    }, 'TLS');
    const client = new CommandResponseStream({ ...vsrConfig(coordinator.port), transport: 'TLS', heartbeatInterval: 0,
      options: { host: '127.0.0.1', port: coordinator.port, servername: 'localhost', ca: TLS_CA_CERTIFICATE } });
    try {
      await client.authenticate(vsrConfig(coordinator.port).credentials);
      await waitUntil(() => active === 4);
      assert.equal(countCommand(healthy, COMMAND_CODE.BindSession), 0);
      assert.equal(peak, 4);
      const deadline = Date.now() + 3000;
      while (countCommand(healthy, COMMAND_CODE.BindSession) === 0 && Date.now() < deadline)
        await new Promise<void>((resolve) => setTimeout(resolve, 10));
      assert.equal(countCommand(healthy, COMMAND_CODE.BindSession), 1);
      assert.equal(framesOf(healthy, Operation.Register).length, 0);
      await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
    } finally {
      client.destroy();
      for (const socket of sockets) socket.destroy();
      await Promise.all([coordinator.close(), healthy.close(), ...stalled.map((server) =>
        new Promise<void>((resolve) => server.close(() => resolve())))]);
    }
  });
});


describe('committed password refresh ordering', () => {
  const original = { username: 'caller', password: 'first' };
  const passwordChange = { userId: 7, currentPassword: 'first', newPassword: 'second' };

  for (const inFlight of [false, true]) {
    for (const eviction of [false, true]) {
      it(`does not acknowledge a ${inFlight ? 'previously sent' : 'queued'} password change during a held automatic ${eviction ? 'eviction' : 'refusal'}`, async () => {
        let heldRegister: (() => void) | undefined;
        let heldChange: (() => void) | undefined;
        const server = await startVsrServer((frame, socket) => {
          if (frame[REQUEST_OFFSET.operation] === Operation.Register && framesOf(server, Operation.Register).length === 2) {
            heldRegister = () => socket.write(eviction ? evictionFrame(EvictionReason.InvalidCredentials)
              : replyFrame(frame, Operation.Register, Buffer.alloc(0), INVALID_CREDENTIALS));
            return;
          }
          if (frame[REQUEST_OFFSET.operation] === Operation.ChangePassword) {
            heldChange = () => socket.write(provedReply(frame));
            return;
          }
          singleNodeHandler(server.port)(frame, socket);
        });
        const client = new CommandResponseStream(vsrConfig(server.port));
        const facade = new SimpleClient(client);
        let changing: Promise<unknown> | undefined;
        try {
          await facade.session.login(original);
          const lineage = client.lineage;
          if (inFlight) {
            changing = facade.user.changePassword(passwordChange).catch((error: unknown) => error);
            await waitUntil(() => heldChange !== undefined);
          }
          for (const socket of server.sockets) socket.write(evictionFrame(EvictionReason.SessionError));
          await waitUntil(() => heldRegister !== undefined);
          if (inFlight) {
            heldChange!();
            assert.ok(await changing instanceof OperationUncertainError);
          } else {
            changing = facade.user.changePassword(passwordChange).catch((error: unknown) => error);
            await waitUntil(() => client.busy);
          }
          assert.equal(client.lineage, lineage);
          assert.equal(framesOf(server, Operation.ChangePassword).length, inFlight ? 1 : 0);
          heldRegister!();
          if (!inFlight) {
            const error = await changing;
            assert.ok(error instanceof ResponseError && error.errorCode === UNAUTHENTICATED);
            assert.ok(eviction ? error.cause instanceof VsrEvictionError
              : error.cause instanceof ResponseError && error.cause.errorCode === INVALID_CREDENTIALS);
          }
          await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)),
            (error: unknown) => error instanceof ResponseError && error.errorCode === UNAUTHENTICATED);
          assert.equal(framesOf(server, Operation.ChangePassword).length, inFlight ? 1 : 0);
          assert.equal(framesOf(server, Operation.Register).length, 2);
        } finally { client.destroy(); await changing; await server.close(); }
      });
    }
  }

  it('finishes automatic roster discovery before a password change and retains its acknowledged password', async () => {
    let heldRoster: (() => void) | undefined;
    let holdRoster = false;
    let rosterReleased = false;
    const server = await startVsrServer((frame, socket) => {
      if (holdRoster && frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetClusterMetadata) {
        heldRoster = () => {
          rosterReleased = true;
          holdRoster = false;
          singleNodeHandler(server.port)(frame, socket);
        };
        return;
      }
      if (frame[REQUEST_OFFSET.operation] === Operation.ChangePassword) {
        assert.equal(rosterReleased, true, 'the password change crossed unfinished automatic discovery');
        socket.write(provedReply(frame));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    const facade = new SimpleClient(client);
    let changing: Promise<boolean> | undefined;
    try {
      await facade.session.login(original);
      const lineage = client.lineage;
      holdRoster = true;
      for (const socket of server.sockets) socket.write(evictionFrame(EvictionReason.SessionError));
      await waitUntil(() => heldRoster !== undefined);
      assert.equal(client.isAuthenticated, true);
      changing = facade.user.changePassword(passwordChange);
      await waitUntil(() => client.busy);
      assert.equal(framesOf(server, Operation.ChangePassword).length, 0);
      heldRoster!();
      assert.equal(await changing, true);
      for (const socket of server.sockets) socket.write(evictionFrame(EvictionReason.SessionError));
      await waitUntil(() => framesOf(server, Operation.Register).length === 3);
      await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
      assert.equal(client.lineage, lineage);
      assert.deepEqual(framesOf(server, Operation.Register).map((frame) => {
        const body = registerCredentialsBody(frame);
        const username = readWireName(body, 0);
        return [username.value, readWireName(body, username.next).value];
      }), [['caller', 'first'], ['caller', 'first'], ['caller', 'second']]);
    } finally { client.destroy(); await changing?.catch(() => undefined); await server.close(); }
  });

  it('keeps another explicit identity after a delayed old password reply', async () => {
    let heldChange: (() => void) | undefined;
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.ChangePassword) {
        heldChange = () => socket.write(provedReply(frame));
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    const facade = new SimpleClient(client);
    let changing: Promise<unknown> | undefined;
    try {
      await facade.session.login(original);
      changing = facade.user.changePassword(passwordChange).catch((error: unknown) => error);
      await waitUntil(() => heldChange !== undefined);
      await facade.session.login({ username: 'other', password: 'own-secret' });
      heldChange!();
      assert.ok(await changing instanceof OperationUncertainError);
      for (const socket of server.sockets) socket.write(evictionFrame(EvictionReason.SessionError));
      await waitUntil(() => framesOf(server, Operation.Register).length === 3);
      await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
      const recovered = registerCredentialsBody(framesOf(server, Operation.Register).at(-1)!);
      const username = readWireName(recovered, 0);
      assert.equal(username.value, 'other');
      assert.equal(readWireName(recovered, username.next).value, 'own-secret');
    } finally { client.destroy(); await changing; await server.close(); }
  });
});

describe('explicit sign-in ownership', () => {
  const caller = { username: 'caller', password: 'secret' };
  const isSignedOut = (error: unknown): boolean => error instanceof ResponseError && error.errorCode === UNAUTHENTICATED;

  for (const alreadyBound of [false, true]) {
    it(`uses explicit credentials after ${alreadyBound ? 'a bound identity' : 'an automatic register in flight'}`, async () => {
      let held: (() => void) | undefined;
      const server = await startVsrServer((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.Register && framesOf(server, Operation.Register).length === 1 && !alreadyBound) {
          held = () => singleNodeHandler(server.port)(frame, socket);
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      try {
        const earlier = client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)).catch((error: unknown) => error);
        if (alreadyBound) await earlier;
        else await waitUntil(() => held !== undefined);
        const pending = client.authenticate(caller);
        held?.();
        assert.equal(await pending, true);
        if (!alreadyBound) assert.ok(isSignedOut(await earlier), 'the earlier call crossed an explicit sign-in');
        assert.deepEqual(framesOf(server, Operation.Register).map((frame) => readWireName(registerCredentialsBody(frame), 0).value), ['iggy', 'caller']);
      } finally { held?.(); client.destroy(); await server.close(); }
    });
  }

  for (const kind of ['authenticate', 'password', 'token'] as const) {
    it(`withdraws remembered and configured fallback before ${kind} input validation`, async () => {
      let ended = false;
      const server = await startVsrServer((frame, socket) => {
        if (ended && frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession) {
          socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), UNAUTHENTICATED));
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      const facade = new SimpleClient(client);
      try {
        await client.sendCommand(LOGIN.code, LOGIN.serialize(caller));
        const before = client.lineage;
        await assert.rejects(kind === 'authenticate' ? client.authenticate({ username: '', password: 'secret' })
          : kind === 'password' ? facade.session.login({ username: '', password: 'secret' })
          : facade.session.loginWithToken({ token: '' }));
        assert.notEqual(client.lineage, before);
        ended = true;
        for (const socket of server.sockets) socket.write(evictionFrame(EvictionReason.SessionError));
        await waitUntil(() => !client.isAuthenticated);
        await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)), isSignedOut);
        assert.equal(framesOf(server, Operation.Register).length, 1);
        await client.authenticate(caller);
        assert.equal(client.isAuthenticated, true);
      } finally { client.destroy(); await server.close(); }
    });
  }

  for (const failure of [INVALID_CREDENTIALS, 53, 54, 47, 'eviction', 'timeout', 'cancel', 'type error'] as const) {
    it(`withdraws explicit fallback after ${failure} and preserves its cause`, async (context) => {
      const cancellation = new Error('caller cancelled sign-in');
      const configuration = new TypeError('invalid transport configuration');
      const abort = new AbortController();
      let refuse = true;
      const server = await startVsrServer((frame, socket) => {
        if (refuse && frame[REQUEST_OFFSET.operation] === Operation.Register) {
          if (typeof failure === 'number') socket.write(replyFrame(frame, Operation.Register, Buffer.alloc(0), failure));
          else if (failure === 'eviction') socket.write(evictionFrame(EvictionReason.InvalidCredentials));
          else if (failure === 'cancel') abort.abort(cancellation);
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      const dial = failure === 'type error'
        ? context.mock.method(IggyConnection.prototype, 'connect', () => Promise.reject(configuration)) : undefined;
      try {
        const error = await client.sendCommand(LOGIN.code, LOGIN.serialize(caller),
          { deadline: Date.now() + 150, signal: abort.signal }).catch((error: unknown) => error);
        if (typeof failure === 'number') assert.ok(error instanceof ResponseError && error.errorCode === failure);
        else if (failure === 'eviction') assert.ok(error instanceof VsrEvictionError);
        else if (failure === 'timeout') assert.ok(error instanceof ResponseError &&
          error.errorCode === TRANSIENT_NOT_ACCEPTED);
        else assert.equal(error, failure === 'cancel' ? cancellation : configuration);
        dial?.mock.restore();
        refuse = false;
        await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)), isSignedOut);
        assert.equal(framesOf(server, Operation.Register).length, failure === 'type error' ? 0 : 1);
        await client.authenticate(caller);
        assert.equal(client.isAuthenticated, true);
      } finally { dial?.mock.restore(); client.destroy(); await server.close(); }
    });
  }

  for (const automatic of [false, true]) {
    it(`fences old ${automatic ? 'automatic' : 'explicit'} and queued sign-ins after a newer validation failure`, async () => {
      let held: (() => void) | undefined;
      const server = await startVsrServer((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.Register && framesOf(server, Operation.Register).length === 1) {
          held = () => singleNodeHandler(server.port)(frame, socket);
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      try {
        const older = (automatic ? client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0))
          : client.sendCommand(LOGIN.code, LOGIN.serialize(caller))).catch((error: unknown) => error);
        await waitUntil(() => held !== undefined);
        const queued = client.sendCommand(LOGIN.code, LOGIN.serialize({ username: 'queued', password: 'secret' })).catch((error: unknown) => error);
        const invalid = client.authenticate({ username: '', password: '' }).catch((error: unknown) => error);
        held!();
        assert.ok(await invalid instanceof Error);
        assert.ok(isSignedOut(await older));
        assert.ok(isSignedOut(await queued));
        await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)), isSignedOut);
        assert.equal(framesOf(server, Operation.Register).length, 1);
      } finally { held?.(); client.destroy(); await server.close(); }
    });
  }

  it('withdraws a queued explicit sign-in at its deadline before an older reply arrives', async () => {
    let held: (() => void) | undefined;
    const server = await startVsrServer((frame, socket) => {
      if (frame[REQUEST_OFFSET.operation] === Operation.Register && framesOf(server, Operation.Register).length === 1) {
        held = () => singleNodeHandler(server.port)(frame, socket);
        return;
      }
      singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      const older = client.sendCommand(LOGIN.code, LOGIN.serialize(caller)).catch((error: unknown) => error);
      await waitUntil(() => held !== undefined);
      let failure: unknown;
      const queued = client.sendCommand(LOGIN.code, LOGIN.serialize(caller), { deadline: Date.now() + 50 })
        .catch((error: unknown) => { failure = error; });
      await waitUntil(() => failure !== undefined);
      assert.ok(failure instanceof ResponseError && failure.errorCode === TRANSIENT_NOT_ACCEPTED);
      held!();
      await queued;
      assert.ok(isSignedOut(await older));
      await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)), isSignedOut);
      assert.equal(framesOf(server, Operation.Register).length, 1);
    } finally { held?.(); client.destroy(); await server.close(); }
  });

  for (const typeError of [false, true]) {
    it(`maps an automatic ${typeError ? 'configuration failure' : 'register eviction'} to 40 with its cause`, async (context) => {
      const configuration = new TypeError('invalid transport configuration');
      const server = await startVsrServer((_frame, socket) => socket.write(evictionFrame(EvictionReason.InvalidCredentials)));
      const client = new CommandResponseStream(vsrConfig(server.port));
      if (typeError) context.mock.method(IggyConnection.prototype, 'connect', () => Promise.reject(configuration));
      try {
        await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)), (error: unknown) =>
          isSignedOut(error) && (typeError ? (error as Error).cause === configuration : (error as Error).cause instanceof VsrEvictionError));
        await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)), isSignedOut);
        assert.equal(framesOf(server, Operation.Register).length, typeError ? 0 : 1);
      } finally { client.destroy(); await server.close(); }
    });
  }


  for (const failure of ['refusal', 'closed link', 'timeout', 'cancellation'] as const) {
    it(`discards an explicit session when its roster fails with ${failure}`, async () => {
      const abort = new AbortController();
      const cancellation = new Error('cancel after register');
      let failRoster = true;
      const server = await startVsrServer((frame, socket) => {
        if (failRoster && frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetClusterMetadata) {
          if (failure === 'refusal') socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), LIFECYCLE_BUSY));
          else if (failure === 'closed link') socket.destroy();
          else if (failure === 'cancellation') abort.abort(cancellation);
          return;
        }
        singleNodeHandler(server.port)(frame, socket);
      });
      const client = new CommandResponseStream(vsrConfig(server.port));
      try {
        const error = await client.sendCommand(LOGIN.code, LOGIN.serialize(caller),
          { deadline: Date.now() + 100, signal: abort.signal }).catch((error: unknown) => error);
        if (failure === 'refusal') assert.ok(error instanceof ResponseError && error.errorCode === LIFECYCLE_BUSY);
        else if (failure === 'closed link') assert.ok(error instanceof Error && error.message === CLOSED_LINK);
        else if (failure === 'timeout') assert.ok(error instanceof ResponseError &&
          error.errorCode === TRANSIENT_NOT_ACCEPTED);
        else assert.equal(error, cancellation);
        assert.equal(client.isAuthenticated, false);
        await waitUntil(() => server.sockets.size === 0);
        failRoster = false;
        await assert.rejects(client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0)), isSignedOut);
        assert.equal(framesOf(server, Operation.Register).length, 1);
        assert.equal(countCommand(server, COMMAND_CODE.BindSession), 0);
        await client.authenticate(caller);
        assert.equal(framesOf(server, Operation.Register).length, 2);
        assert.equal(client.isAuthenticated, true);
      } finally { client.destroy(); await server.close(); }
    });
  }

  it('exposes lineage without allowing callers to rewrite it', () => {
    const client = new CommandResponseStream(vsrConfig(1));
    try {
      const lineage = client.lineage;
      assert.equal(Reflect.set(client, 'lineage', lineage + 10), false);
      assert.equal(client.lineage, lineage);
    } finally { client.destroy(); }
  });
});

describe('replacement incarnation lane ownership', () => {
  it('orders an 88 restart against later writes in its replacement incarnation', async () => {
    let incarnation = 1n;
    let held: (() => void) | undefined;
    let refused: (() => void) | undefined;
    const server = await startVsrServer((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.GetPartitionRoute) {
        socket.write(replyFrame(frame, Operation.NonReplicated, routingBody(incarnation, incarnation, 0)));
      } else if (frame[REQUEST_OFFSET.operation] === Operation.SendMessages) {
        const count = framesOf(server, Operation.SendMessages).length;
        if (count === 1) {
          incarnation = 2n;
          refused = () => socket.write(replyFrame(frame, Operation.SendMessages, Buffer.alloc(0), LIFECYCLE_BUSY));
        } else if (count === 2) held = () => socket.write(provedReply(frame));
        else socket.write(provedReply(frame));
      } else singleNodeHandler(server.port)(frame, socket);
    });
    const client = new CommandResponseStream(vsrConfig(server.port));
    try {
      const first = client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      void first.catch(() => undefined);
      await waitUntil(() => refused !== undefined);
      await client.sendCommand(COMMAND_CODE.GetPartitionRoute, serializePartitionRoute(1, 2, 3));
      refused!();
      await waitUntil(() => held !== undefined);
      const later = client.sendCommand(COMMAND_CODE.SendMessages, explicitSend);
      void later.catch(() => undefined);
      // This read reaches the same link after the later write has entered admission.
      await client.sendCommand(COMMAND_CODE.GetStats, Buffer.alloc(0));
      assert.equal(framesOf(server, Operation.SendMessages).length, 2);
      held!();
      await Promise.all([first, later]);
      assert.deepEqual(framesOf(server, Operation.SendMessages).map((frame) => stampedContext(frame).incarnation), [1n, 2n, 2n]);
    } finally { refused?.(); held?.(); client.destroy(); await server.close(); }
  });
});

describe('consumer stream', () => {
  const POLLED_PARTITION = 3;
  const streamConfig = (port: number): ClientConfig => ({
    ...vsrConfig(port), heartbeatInterval: 20, reconnect: { enabled: true, interval: 10, maxRetries: 2 }
  });
  const pollRequest = {
    streamId: 1, topicId: 2, pollingStrategy: PollingStrategy.Next, count: 1, interval: 10, endOnLastOffset: true
  };

  const emptyPoll = (frame: Buffer): Buffer => {
    const body = Buffer.alloc(40);
    body.writeUInt32LE(POLLED_PARTITION, 0);
    return replyFrame(frame, Operation.NonReplicated, body);
  };

  /** Group 1, whose only member owns `POLLED_PARTITION`. */
  const groupHandler = (frame: Buffer, socket: Socket): boolean => {
    const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
    if (frame[REQUEST_OFFSET.operation] === Operation.JoinConsumerGroup) {
      socket.write(replyFrame(frame, Operation.JoinConsumerGroup, metadataResult(0)));
    } else if (code === COMMAND_CODE.GetGroup) {
      const group = Buffer.alloc(13);
      group.writeUInt32LE(1, 0);
      socket.write(replyFrame(frame, Operation.NonReplicated, group));
    } else if (code === COMMAND_CODE.SyncGroup) {
      const assignment = Buffer.alloc(16);
      assignment.writeBigUInt64LE(1n, 0);
      assignment.writeUInt32LE(1, 8);
      assignment.writeUInt32LE(POLLED_PARTITION, 12);
      socket.write(replyFrame(frame, Operation.NonReplicated, assignment));
    } else return false;
    return true;
  };

  /** Collects what a stream yields; the returned wait fails on the error that ended it. */
  const consume = (stream: Readable): () => Promise<unknown[]> => {
    const polled: unknown[] = [];
    let ended = false;
    let failed: unknown;
    stream.on('data', (response) => polled.push(response));
    stream.once('end', () => { ended = true; });
    stream.once('error', (error) => { failed = error; });
    return async () => {
      await waitUntil(() => ended || failed !== undefined);
      assert.ifError(failed);
      return polled;
    };
  };

  const closeAll = async (servers: VsrTestServer[], ...streams: (Readable | undefined)[]): Promise<void> => {
    for (const stream of streams) stream?.destroy();
    for (const server of servers)
      for (const socket of server.sockets) socket.destroy();
    await Promise.all(servers.map((server) => server.close()));
  };


  it('keeps polling after a repairable bootstrap bind loses its socket', async (context) => {
    const refusals = pollRefusals(context);
    let binds = 0;
    let polls = 0;
    let rosters = 0;
    const server = await startVsrServer((frame, socket) => {
      const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
      if (code === COMMAND_CODE.GetClusterMetadata) {
        socket.write(replyFrame(frame, Operation.NonReplicated, metadataBody([{ port: ++rosters === 1 ? 0 : server.port, replicaId: 0, role: 0 }])));
      } else if (code === COMMAND_CODE.BindSession && ++binds === 1) socket.destroy();
      else if (code === COMMAND_CODE.PollMessages) {
        polls += 1;
        socket.write(emptyPoll(frame));
      } else singleNodeHandler(server.port)(frame, socket);
    });
    let stream: Readable | undefined;
    try {
      stream = await singleConsumerStream({ ...streamConfig(server.port), reconnect: { enabled: true, interval: 1, maxRetries: 0 } })(
        { ...pollRequest, partitionId: POLLED_PARTITION });
      // Empty roster retains only bootstrap; the next poll must dial and bind it.
      for (const socket of server.sockets) socket.destroy();
      await waitUntil(() => server.sockets.size === 0);
      assert.equal((await consume(stream)()).length, 1);
      assert.equal(polls, 1);
      assert.equal(binds, 2);
      assert.ok(refusals.some((error) => error instanceof Error && error.message === CLOSED_LINK));
      assert.equal(framesOf(server, Operation.Register).length, 1);
    } finally { await closeAll([server], stream); }
  });

  // A budget far shorter than the heartbeat interval lets every link use it up
  // before a heartbeat restarts one.
  const outageConfig = (port: number): ClientConfig => ({
    ...streamConfig(port), reconnect: { enabled: true, interval: 1, maxRetries: 1 }
  });

  /** Records each error that a poll returns to the stream. */
  const pollRefusals = (context: TestContext): unknown[] => {
    const refusals: unknown[] = [];
    const sendCommand = CommandResponseStream.prototype.sendCommand;
    context.mock.method(CommandResponseStream.prototype, 'sendCommand',
      function (this: CommandResponseStream, ...args: Parameters<typeof sendCommand>) {
        const pending = sendCommand.apply(this, args);
        if (args[0] === COMMAND_CODE.PollMessages)
          void pending.catch((error: unknown) => { refusals.push(error); });
        return pending;
      });
    return refusals;
  };

  /** A cluster whose replicas all stop listening at the first route lookup, until `reopen`. */
  const startOutageCluster = async () => {
    let outage = false;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages) return false;
      socket.write(emptyPoll(frame));
      return true;
    }, (frame, socket) => {
      if (outage || frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetPartitionRoute)
        return groupHandler(frame, socket);
      // A poll without a route asks for any ready link, so it sees every link out of budget.
      outage = true;
      for (const server of [cluster.coordinator, cluster.primary]) {
        void server.close();
        for (const link of server.sockets) link.destroy();
      }
      return true;
    });
    return cluster;
  };

  for (const group of [false, true]) {
    for (const failure of ['a stale client refusal', 'a lost poll reply', 'a primary link lost past the poll deadline'] as const) {
      it(`keeps a ${group ? 'group' : 'single'} consumer polling on the same session after ${failure}`, async () => {
        const lostPrimary = failure === 'a primary link lost past the poll deadline';
        let primaryDown = lostPrimary;
        let polls = 0;
        const third = await startVsrServer((frame, socket) => singleNodeHandler(third.port)(frame, socket));
        const cluster = await startPollCluster((frame, socket) => {
          const code = frame.readUInt32LE(REQUEST_OFFSET.reserved);
          if (primaryDown && code === COMMAND_CODE.BindSession) {
            socket.destroy();
            return true;
          }
          if (code !== COMMAND_CODE.PollMessages) return false;
          polls += 1;
          if (polls > 1 || lostPrimary) socket.write(emptyPoll(frame));
          else if (failure === 'a lost poll reply') socket.destroy();
          else socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), STALE_CLIENT));
          return true;
        }, (frame, socket) => {
          if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetClusterMetadata)
            return groupHandler(frame, socket);
          socket.write(replyFrame(frame, Operation.NonReplicated,
            threeNodeMetadataBody(cluster.coordinator.port, cluster.primary.port, third.port)));
          return true;
        });
        const realNow = Date.now;
        let clockOffset = 0;
        Date.now = () => realNow() + clockOffset;
        let stream: Readable | undefined;
        try {
          const config = streamConfig(cluster.coordinator.port);
          stream = group
            ? await groupConsumerStream(config)({ ...pollRequest, groupName: 'group' })
            : await singleConsumerStream(config)({ ...pollRequest, partitionId: POLLED_PARTITION });
          const ended = consume(stream);
          if (primaryDown) {
            // The poll retries its route while the primary link is down, until its deadline passes.
            await waitUntil(() => countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute) >= 2);
            clockOffset = OPERATION_TIMEOUT_MS;
            primaryDown = false;
          }
          assert.equal((await ended()).length, 1);
          assert.equal(polls, lostPrimary ? 1 : 2);
          const registers = framesOf(cluster.coordinator, Operation.Register);
          const binds = cluster.primary.frames.filter((frame) =>
            frame.readUInt32LE(REQUEST_OFFSET.reserved) === COMMAND_CODE.BindSession);
          assert.equal(registers.length, 1);
          assert.ok(binds.length >= 2);
          for (const bind of binds) assertSharedBinding(registers[0], bind);
          await waitUntil(() => [cluster.coordinator, cluster.primary, third].every((server) => server.sockets.size === 0));
        } finally {
          Date.now = realNow;
          cluster.client.destroy();
          await closeAll([cluster.coordinator, cluster.primary, third], stream);
        }
      });
    }
  }

  it('keeps a single consumer polling after its session ends and the client signs in again', async () => {
    let polls = 0;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages) return false;
      polls += 1;
      socket.write(polls > 1 ? emptyPoll(frame) : evictionFrame(EvictionReason.SessionError));
      return true;
    });
    let stream: Readable | undefined;
    try {
      stream = await singleConsumerStream(streamConfig(cluster.coordinator.port))(
        { ...pollRequest, partitionId: POLLED_PARTITION });
      assert.equal((await consume(stream)()).length, 1);
      assert.equal(polls, 2);
      assert.equal(framesOf(cluster.coordinator, Operation.Register).length, 2);
    } finally {
      cluster.client.destroy();
      await closeAll([cluster.coordinator, cluster.primary], stream);
    }
  });

  for (const group of [false, true]) {
    it(`keeps a ${group ? 'group' : 'single'} consumer polling through an outage longer than every link budget`, async (context) => {
      const refusals = pollRefusals(context);
      const cluster = await startOutageCluster();
      let stream: Readable | undefined;
      try {
        const config = outageConfig(cluster.coordinator.port);
        stream = group
          ? await groupConsumerStream(config)({ ...pollRequest, groupName: 'group' })
          : await singleConsumerStream(config)({ ...pollRequest, partitionId: POLLED_PARTITION });
        const ended = consume(stream);
        await waitUntil(() => refusals.length > 0);
        await cluster.coordinator.reopen();
        // The coordinator comes back first, so that it answers the route lookup.
        await waitUntil(() => stream!.destroyed || countCommand(cluster.coordinator, COMMAND_CODE.GetPartitionRoute) >= 2);
        await cluster.primary.reopen();
        assert.equal((await ended()).length, 1);
        assert.equal(framesOf(cluster.coordinator, Operation.Register).length, 1);
        for (const refusal of refusals) assert.ok(refusal instanceof NotConnectedError, String(refusal));
      } finally {
        cluster.client.destroy();
        await closeAll([cluster.coordinator, cluster.primary], stream);
      }
    });
  }

  for (const [setting, overrides] of [
    ['reconnection is disabled', { reconnect: { enabled: false, interval: 1, maxRetries: 0 } }],
    ['heartbeats are disabled', { heartbeatInterval: 0 }]
  ] as const) {
    it(`ends a consumer stream after an outage when ${setting}`, async () => {
      const cluster = await startOutageCluster();
      let stream: Readable | undefined;
      try {
        stream = await singleConsumerStream({ ...outageConfig(cluster.coordinator.port), ...overrides })(
          { ...pollRequest, partitionId: POLLED_PARTITION });
        stream.on('error', () => undefined).resume();
        await waitUntil(() => stream!.destroyed);
        // The client never restarts these links, so the stream must not wait for them.
        assert.ok(stream.errored && !(stream.errored instanceof NotConnectedError), String(stream.errored));
      } finally {
        cluster.client.destroy();
        await closeAll([cluster.coordinator, cluster.primary], stream);
      }
    });
  }

  it('keeps a single consumer polling while its session ends and no replica accepts a dial', async (context) => {
    const refusals = pollRefusals(context);
    let polls = 0;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages) return false;
      polls += 1;
      if (polls > 1) {
        socket.write(emptyPoll(frame));
        return true;
      }
      // The client drops every link when its session ends, so the new sign-in has to dial.
      void cluster.coordinator.close();
      void cluster.primary.close();
      socket.write(evictionFrame(EvictionReason.SessionError));
      return true;
    });
    let stream: Readable | undefined;
    try {
      stream = await singleConsumerStream(outageConfig(cluster.coordinator.port))(
        { ...pollRequest, partitionId: POLLED_PARTITION });
      const ended = consume(stream);
      await waitUntil(() => refusals.length >= 2);
      await cluster.coordinator.reopen();
      // The coordinator comes back first, so that it answers the sign-in.
      await waitUntil(() => stream!.destroyed || framesOf(cluster.coordinator, Operation.Register).length === 2);
      await cluster.primary.reopen();
      assert.equal((await ended()).length, 1);
      assert.equal(polls, 2);
      assert.ok(refusals[0] instanceof ResponseError && refusals[0].errorCode === TRANSIENT_NOT_COMMITTED &&
        refusals[0].cause instanceof VsrEvictionError && refusals[0].cause.errorCode === UNAUTHENTICATED,
      String(refusals[0]));
      for (const refusal of refusals.slice(1)) assert.ok(refusal instanceof NotConnectedError, String(refusal));
    } finally {
      cluster.client.destroy();
      await closeAll([cluster.coordinator, cluster.primary], stream);
    }
  });

  for (const group of [false, true]) {
    const kind = group ? 'group' : 'single';
    const openStream = (config: ClientConfig): Promise<Readable> => group
      ? groupConsumerStream(config)({ ...pollRequest, groupName: 'group' })
      : singleConsumerStream(config)({ ...pollRequest, partitionId: POLLED_PARTITION });

    it(`rejects a ${kind} consumer stream at creation and releases its client when no replica accepts a dial`, async (context) => {
      const unreachable = await startVsrServer(() => undefined);
      await unreachable.close();
      const destroys = context.mock.method(CommandResponseStream.prototype, 'destroy');
      const dials = context.mock.method(IggyConnection.prototype, 'connect');
      await assert.rejects(openStream(outageConfig(unreachable.port)), NotConnectedError);
      assert.equal(destroys.mock.callCount(), 1);
      const settled = dials.mock.callCount();
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
      assert.equal(dials.mock.callCount(), settled);
    });

    it(`rejects a ${kind} consumer stream with the last dial error when its sign-in deadline ends the dials`, async (context) => {
      const unreachable = await startVsrServer(() => undefined);
      await unreachable.close();
      const dials = context.mock.method(IggyConnection.prototype, 'connect');
      context.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
      try {
        // The default retry budget outlasts the sign-in deadline.
        const outcome = openStream({ ...vsrConfig(unreachable.port), reconnect: undefined })
          .then(() => 'resolved', (error: unknown) => error);
        await waitUntilUnmocked(() => dials.mock.callCount() > 0);
        await dials.mock.calls[0].result?.catch(() => undefined);
        await new Promise<void>((resolve) => setImmediate(resolve));
        context.mock.timers.tick(OPERATION_TIMEOUT_MS);
        const error = await outcome;
        assert.ok(error instanceof NotConnectedError &&
          (error.cause as NodeJS.ErrnoException).code === 'ECONNREFUSED', String(error));
      } finally { context.mock.timers.reset(); }
    });

    it(`rejects a ${kind} consumer stream with its refused sign-in and releases its client`, async (context) => {
      const server = await startVsrServer((frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] === Operation.Register)
          socket.write(evictionFrame(EvictionReason.InvalidCredentials));
        else singleNodeHandler(server.port)(frame, socket);
      });
      const destroys = context.mock.method(CommandResponseStream.prototype, 'destroy');
      try {
        await assert.rejects(openStream(outageConfig(server.port)), (error: unknown) =>
          error instanceof VsrEvictionError && error.errorCode === INVALID_CREDENTIALS);
        assert.equal(destroys.mock.callCount(), 1);
        assert.equal(framesOf(server, Operation.Register).length, 1);
      } finally {
        await closeAll([server]);
      }
    });

    it(`ends a ${kind} consumer stream at its first retry when the server refuses its next sign-in with 40`, async () => {
      let registers = 0;
      const cluster = await startPollCluster((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages) return false;
        socket.write(evictionFrame(EvictionReason.SessionError));
        return true;
      }, (frame, socket) => {
        // The user became inactive after the stream signed in.
        if (frame[REQUEST_OFFSET.operation] !== Operation.Register || ++registers === 1)
          return groupHandler(frame, socket);
        socket.write(evictionFrame(EvictionReason.UserInactive));
        return true;
      });
      let stream: Readable | undefined;
      try {
        stream = await openStream(streamConfig(cluster.coordinator.port));
        stream.on('error', () => undefined).resume();
        await waitUntil(() => stream!.destroyed);
        assert.ok(stream.errored instanceof VsrEvictionError && stream.errored.errorCode === UNAUTHENTICATED &&
          stream.errored.reason === EvictionReason.UserInactive, String(stream.errored));
        // The first sign-in, the recovery, the sign-in of the retry and the one that the retry waits for.
        assert.ok(registers <= 4, `${registers} sign-ins`);
        await waitUntil(() => cluster.coordinator.sockets.size === 0 && cluster.primary.sockets.size === 0);
      } finally {
        cluster.client.destroy();
        await closeAll([cluster.coordinator, cluster.primary], stream);
      }
    });

    it(`releases the client of a ${kind} consumer stream destroyed while its retry waits for a sign-in`, async (context) => {
      const calls = context.mock.method(CommandResponseStream.prototype, 'sendCommand');
      let callsBeforeRetry = Number.POSITIVE_INFINITY;
      let registers = 0;
      const cluster = await startPollCluster((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages) return false;
        callsBeforeRetry = calls.mock.callCount();
        socket.write(evictionFrame(EvictionReason.SessionError));
        return true;
      }, (frame, socket) => {
        // The sign-in that recovers the session never ends.
        if (frame[REQUEST_OFFSET.operation] === Operation.Register && ++registers > 1) return true;
        return groupHandler(frame, socket);
      });
      let stream: Readable | undefined;
      try {
        stream = await openStream(streamConfig(cluster.coordinator.port));
        stream.on('error', () => undefined).resume();
        await waitUntil(() => calls.mock.callCount() > callsBeforeRetry);
        stream.destroy();
        await waitUntil(() => cluster.coordinator.sockets.size === 0 && cluster.primary.sockets.size === 0);
      } finally {
        cluster.client.destroy();
        await closeAll([cluster.coordinator, cluster.primary], stream);
      }
    });
  }

  it('rejects a group consumer stream with its refused join and releases its signed-in client', async () => {
    const cluster = await startPollCluster(undefined, (frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.GetGroup) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), UNAUTHORIZED));
      return true;
    });
    try {
      await assert.rejects(groupConsumerStream(streamConfig(cluster.coordinator.port))({ ...pollRequest, groupName: 'group' }),
        (error: unknown) => error instanceof ResponseError && error.errorCode === UNAUTHORIZED);
      await waitUntil(() => cluster.coordinator.sockets.size === 0 && cluster.primary.sockets.size === 0);
    } finally {
      cluster.client.destroy();
      await closeAll([cluster.coordinator, cluster.primary]);
    }
  });

  it('stops polling and releases its client when a retrying stream is destroyed', async () => {
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages) return false;
      socket.write(replyFrame(frame, Operation.NonReplicated, Buffer.alloc(0), STALE_CLIENT));
      return true;
    });
    const stream = await singleConsumerStream(streamConfig(cluster.coordinator.port))(
      { ...pollRequest, partitionId: POLLED_PARTITION });
    let failed: unknown;
    stream.once('error', (error) => { failed = error; });
    try {
      stream.resume();
      await waitUntil(() => failed !== undefined || countCommand(cluster.primary, COMMAND_CODE.PollMessages) >= 2);
      assert.ifError(failed);
      stream.destroy();
      await waitUntil(() => cluster.coordinator.sockets.size === 0 && cluster.primary.sockets.size === 0);
    } finally {
      cluster.client.destroy();
      await closeAll([cluster.coordinator, cluster.primary], stream);
    }
  });

  it('releases the client of a stream destroyed during a poll whose session then ends', async () => {
    let polled: Socket | undefined;
    let registers = 0;
    const cluster = await startPollCluster((frame, socket) => {
      if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages) return false;
      polled = socket;
      return true;
    }, (frame) => {
      // The sign-in that recovers the session never ends.
      return frame[REQUEST_OFFSET.operation] === Operation.Register && ++registers > 1;
    });
    let stream: Readable | undefined;
    try {
      // A poll without progress outlives the end of its session, and the stream then waits for a sign-in.
      stream = await singleConsumerStream(streamConfig(cluster.coordinator.port))({ ...pollRequest,
        partitionId: POLLED_PARTITION, pollingStrategy: PollingStrategy.First, autocommit: false });
      stream.on('error', () => undefined).resume();
      await waitUntil(() => polled !== undefined);
      stream.destroy();
      polled!.write(evictionFrame(EvictionReason.SessionError));
      await waitUntil(() => cluster.coordinator.sockets.size === 0 && cluster.primary.sockets.size === 0);
    } finally {
      cluster.client.destroy();
      await closeAll([cluster.coordinator, cluster.primary], stream);
    }
  });

  for (const [leave, status] of [['an accepted', 0], ['a refused', UNAUTHORIZED]] as const) {
    it(`leaves its group and releases its client when a group stream with ${leave} leave is destroyed`, async () => {
      const cluster = await startPollCluster((frame, socket) => {
        if (frame.readUInt32LE(REQUEST_OFFSET.reserved) !== COMMAND_CODE.PollMessages) return false;
        socket.write(emptyPoll(frame));
        return true;
      }, (frame, socket) => {
        if (frame[REQUEST_OFFSET.operation] !== Operation.LeaveConsumerGroup) return groupHandler(frame, socket);
        socket.write(replyFrame(frame, Operation.LeaveConsumerGroup, metadataResult(status)));
        return true;
      });
      let stream: Readable | undefined;
      try {
        stream = await groupConsumerStream(streamConfig(cluster.coordinator.port))(
          { ...pollRequest, groupName: 'group', endOnLastOffset: false });
        stream.resume();
        await waitUntil(() => countCommand(cluster.primary, COMMAND_CODE.PollMessages) > 0);
        stream.destroy();
        await waitUntil(() => cluster.coordinator.sockets.size === 0 && cluster.primary.sockets.size === 0);
        const leaves = framesOf(cluster.coordinator, Operation.LeaveConsumerGroup);
        assert.equal(leaves.length, 1);
        assert.deepEqual(leaves[0].subarray(HEADER_SIZE), LEAVE_GROUP.serialize(
          { streamId: pollRequest.streamId, topicId: pollRequest.topicId, groupId: 'group' }));
      } finally {
        cluster.client.destroy();
        await closeAll([cluster.coordinator, cluster.primary], stream);
      }
    });
  }
});
