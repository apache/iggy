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
import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';
import { describe, it } from 'node:test';
import { EMPTY_PARTITION_CONTEXT } from '../vsr/header.js';
import type {
  CommandResponse,
  RawClient,
  SendCommandOptions
} from '../../client/client.type.js';
import { COMMAND_CODE } from '../command.code.js';
import { ResponseError } from '../error.utils.js';
import { LaneBlockedError, OPERATION_TIMEOUT_MS, OperationUncertainError } from '../../client/client.operation.js';
import { Consumer } from '../offset/offset.utils.js';
import {
  PollingStrategy
} from './poll.utils.js';
import {
  NO_ASSIGNED_PARTITION,
  pollMessages,
  type PollMessages
} from './poll-messages.command.js';

const UNAUTHENTICATED = 40;

const response = (data: Buffer): CommandResponse => ({
  status: 0,
  length: data.length,
  data
});

const assignment = (
  generation: bigint,
  partitions: number[]
): CommandResponse => {
  const data = Buffer.alloc(12 + partitions.length * 4);
  data.writeBigUInt64LE(generation, 0);
  data.writeUInt32LE(partitions.length, 8);
  partitions.forEach((partition, index) => {
    data.writeUInt32LE(partition, 12 + index * 4);
  });
  return response(data);
};

const pollResponse = (
  partitionId: number,
  count = 0
): CommandResponse => {
  const data = Buffer.alloc(40);
  data.writeUInt32LE(partitionId, 0);
  data.writeBigUInt64LE(0n, 4);
  data.writeUInt32LE(count, 12);
  return response(data);
};

const groupRequest: PollMessages = {
  streamId: 1,
  topicId: 2,
  consumer: Consumer.Group(3),
  partitionId: null,
  pollingStrategy: PollingStrategy.Next,
  count: 10,
  autocommit: false
};

const stubClient = (
  responses: (CommandResponse | Error)[],
  onCommand?: (command: number, emitter: EventEmitter) => void,
): {
  client: RawClient,
  emitter: EventEmitter,
  commands: { command: number, payload: Buffer, options?: SendCommandOptions }[]
} => {
  const emitter = new EventEmitter();
  const commands: { command: number, payload: Buffer, options?: SendCommandOptions }[] = [];
  const client = {
    protocol: 'vsr',
    isAuthenticated: true,
    sendCommand: async (command: number, payload: Buffer, options?: SendCommandOptions) => {
      commands.push({ command, payload, options });
      onCommand?.(command, emitter);
      const next = responses.shift();
      if (!next)
        throw new Error('unexpected command');
      if (next instanceof Error)
        throw next;
      return next;
    },
    authenticate: async () => true,
    destroy: () => {},
    on: emitter.on.bind(emitter),
    once: emitter.once.bind(emitter),
    getReadStream: () => Readable.from([])
  } as RawClient;
  return { client, emitter, commands };
};

describe('VSR consumer-group polling', () => {
  it('rejects a missing assignment', async () => {
    const { client } = stubClient([
      response(Buffer.alloc(0)),
      response(Buffer.alloc(0)),
      response(Buffer.alloc(0))
    ]);
    await assert.rejects(
      () => pollMessages(async () => client)(groupRequest),
      (error: unknown) =>
        error instanceof ResponseError &&
        error.errorCode === 5006 &&
        error.message.includes('message: Consumer group member with client ID:')
    );
  });

  it('joins when the initial sync has no membership', async () => {
    const { client, commands } = stubClient([
      response(Buffer.alloc(0)),
      response(Buffer.alloc(0)),
      assignment(2n, [7]),
      pollResponse(7)
    ]);

    assert.equal(
      (await pollMessages(async () => client)(groupRequest)).partitionId,
      7
    );
    assert.deepEqual(
      commands.map(({ command }) => command),
      [
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.JoinGroup,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages
      ]
    );
  });

  it('rejoins when sync reports a missing member after session reset', async () => {
    const { client, emitter, commands } = stubClient([
      assignment(1n, [4]),
      pollResponse(4),
      new ResponseError(COMMAND_CODE.SyncGroup, 5006),
      response(Buffer.alloc(0)),
      assignment(2n, [7]),
      pollResponse(7)
    ]);
    const poll = pollMessages(async () => client);

    assert.equal((await poll(groupRequest)).partitionId, 4);
    emitter.emit('sessionReset');
    assert.equal((await poll(groupRequest)).partitionId, 7);
    assert.deepEqual(
      commands.map(({ command }) => command),
      [
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.JoinGroup,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages
      ]
    );
  });

  it('returns immediately for an empty assignment', async () => {
    const { client, commands } = stubClient([assignment(1n, [])]);
    assert.deepEqual(
      await pollMessages(async () => client)(groupRequest),
      {
        partitionId: NO_ASSIGNED_PARTITION,
        context: EMPTY_PARTITION_CONTEXT,
        currentOffset: 0n,
        count: 0,
        messages: []
      }
    );
    assert.deepEqual(
      commands.map(({ command }) => command),
      [COMMAND_CODE.SyncGroup]
    );
  });

  it('resynchronizes an empty assignment on the next poll', async () => {
    const { client, commands } = stubClient([
      assignment(1n, []),
      assignment(4n, [7]),
      pollResponse(7)
    ]);
    const poll = pollMessages(async () => client);
    assert.equal((await poll(groupRequest)).partitionId, NO_ASSIGNED_PARTITION);
    assert.equal((await poll(groupRequest)).partitionId, 7);
    assert.deepEqual(
      commands.map(({ command }) => command),
      [
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages
      ]
    );
  });

  it('caches a cursor and refreshes it after a heartbeat', async () => {
    const responses = [
      assignment(1n, [4, 5]),
      pollResponse(4),
      pollResponse(5),
      assignment(2n, [8]),
      pollResponse(8)
    ];
    const { client, emitter, commands } = stubClient(responses);
    const poll = pollMessages(async () => client);
    assert.equal((await poll(groupRequest)).partitionId, 4);
    assert.equal((await poll(groupRequest)).partitionId, 5);
    emitter.emit('heartbeat');
    assert.equal((await poll(groupRequest)).partitionId, 8);
    assert.deepEqual(
      commands
        .filter(({ command }) => command === COMMAND_CODE.PollMessages)
        .map(({ payload }) => payload.readUInt32LE(20)),
      [4, 5, 8]
    );
    assert.equal(
      commands.filter(
        ({ command }) => command === COMMAND_CODE.SyncGroup
      ).length,
      2
    );
  });

  it('keeps the round-robin position across a heartbeat refresh', async () => {
    const { client, emitter, commands } = stubClient([
      assignment(1n, [4, 5]),
      pollResponse(4),
      assignment(1n, [4, 5]),
      pollResponse(5)
    ]);
    const poll = pollMessages(async () => client);
    assert.equal((await poll(groupRequest)).partitionId, 4);
    emitter.emit('heartbeat');
    assert.equal((await poll(groupRequest)).partitionId, 5);
    assert.deepEqual(
      commands.map(({ command }) => command),
      [
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages
      ]
    );
  });

  it('retries a poll interrupted by a session reset', async () => {
    let resetPending = true;
    const { client, commands } = stubClient(
      [
        assignment(1n, [4]),
        new Error('connection closed while waiting for response'),
        response(Buffer.alloc(0)),
        response(Buffer.alloc(0)),
        assignment(2n, [7]),
        pollResponse(7)
      ],
      (command, emitter) => {
        if (command === COMMAND_CODE.PollMessages && resetPending) {
          resetPending = false;
          emitter.emit('sessionReset', true);
        }
      }
    );

    assert.equal(
      (await pollMessages(async () => client)(groupRequest)).partitionId,
      7
    );
    assert.deepEqual(
      commands.map(({ command }) => command),
      [
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.JoinGroup,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages
      ]
    );
  });

  it('returns the refused sign-in of a poll that starts signed out', async () => {
    let resetPending = true;
    const refusal = new ResponseError(COMMAND_CODE.SyncGroup, UNAUTHENTICATED);
    const { client, commands } = stubClient([refusal], (_command, emitter) => {
      // The sign-in that the first command waits for resets a session that was not signed in.
      if (resetPending) {
        resetPending = false;
        emitter.emit('sessionReset', false);
      }
    });
    client.isAuthenticated = false;

    await assert.rejects(pollMessages(async () => client)(groupRequest), (error: unknown) => error === refusal);
    assert.deepEqual(commands.map(({ command }) => command), [COMMAND_CODE.SyncGroup]);
  });

  it('retries a poll that starts signed out when the session it signed in to ends', async () => {
    let sessionEnded = false;
    const { client, commands } = stubClient(
      [
        assignment(1n, [4]),
        new Error('connection closed while waiting for response'),
        response(Buffer.alloc(0)),
        response(Buffer.alloc(0)),
        assignment(2n, [7]),
        pollResponse(7)
      ],
      (command, emitter) => {
        if (!client.isAuthenticated) {
          // The sign-in that the first command waits for resets a session that was not signed in.
          emitter.emit('sessionReset', false);
          client.isAuthenticated = true;
        } else if (command === COMMAND_CODE.PollMessages && !sessionEnded) {
          sessionEnded = true;
          emitter.emit('sessionReset', true);
        }
      }
    );
    client.isAuthenticated = false;

    assert.equal((await pollMessages(async () => client)(groupRequest)).partitionId, 7);
    assert.deepEqual(commands.map(({ command }) => command), [
      COMMAND_CODE.SyncGroup, COMMAND_CODE.PollMessages, COMMAND_CODE.SyncGroup,
      COMMAND_CODE.JoinGroup, COMMAND_CODE.SyncGroup, COMMAND_CODE.PollMessages
    ]);
  });

  it('bounds the retries after session ends by one deadline for the whole call', async (context) => {
    // Each attempt takes a third of the deadline, so the third one ends at it.
    const attempts = 3;
    const sessionEnds = attempts + 1;
    context.mock.timers.enable({ apis: ['Date'] });
    const deadline = Date.now() + OPERATION_TIMEOUT_MS;
    const refusal = () => new ResponseError(COMMAND_CODE.SyncGroup, UNAUTHENTICATED);
    const { client, commands } = stubClient(Array.from({ length: sessionEnds }, refusal), (_command, emitter) => {
      if (commands.length <= sessionEnds) emitter.emit('sessionReset', true);
      context.mock.timers.tick(OPERATION_TIMEOUT_MS / attempts);
    });

    await assert.rejects(pollMessages(async () => client)(groupRequest),
      (error: unknown) => error instanceof ResponseError && error.errorCode === UNAUTHENTICATED);
    assert.equal(commands.length, attempts);
    assert.ok(commands.every(({ options }) => options?.deadline === deadline));
  });

  it('does not replay an ambiguous auto-commit poll when its coordinator also resets', async () => {
    const { client, commands } = stubClient([
      assignment(1n, [4]),
      new ResponseError(COMMAND_CODE.PollMessages, 57)
    ], (command, emitter) => {
      if (command === COMMAND_CODE.PollMessages)
        emitter.emit('sessionReset', true);
    });
    await assert.rejects(pollMessages(async () => client)({ ...groupRequest, autocommit: true }),
      (error: unknown) => error instanceof ResponseError && error.errorCode === 57);
    assert.deepEqual(commands.map(({ command }) => command),
      [COMMAND_CODE.SyncGroup, COMMAND_CODE.PollMessages]);
  });

  it('resynchronizes twice before returning an empty result', async () => {
    const resyncPartition = 0xFFFF_FFFF;
    const { client, commands } = stubClient([
      assignment(1n, [1]),
      pollResponse(resyncPartition),
      assignment(2n, [2]),
      pollResponse(resyncPartition)
    ]);
    assert.deepEqual(
      await pollMessages(async () => client)(groupRequest),
      {
        partitionId: NO_ASSIGNED_PARTITION,
        context: EMPTY_PARTITION_CONTEXT,
        currentOffset: 0n,
        count: 0,
        messages: []
      }
    );
    assert.deepEqual(
      commands
        .filter(({ command }) => command === COMMAND_CODE.PollMessages)
        .map(({ payload }) => payload.readUInt32LE(20)),
      [1, 2]
    );
  });

  it('resolves its uncertain join before synchronizing the group again', async () => {
    const operationId = 17n;
    const { client, commands } = stubClient([
      response(Buffer.alloc(0)),
      new OperationUncertainError(operationId, new Error('lost join reply')),
      assignment(2n, [7]),
      pollResponse(7)
    ]);
    const resolutions: bigint[] = [];
    client.resolveOperation = async (id) => {
      resolutions.push(id);
      if (resolutions.length === 1)
        throw new OperationUncertainError(id, new Error('lost resolve reply'));
    };
    const poll = pollMessages(async () => client);
    await assert.rejects(poll(groupRequest), OperationUncertainError);
    await assert.rejects(poll(groupRequest), OperationUncertainError);
    assert.equal((await poll(groupRequest)).partitionId, 7);
    assert.deepEqual(resolutions, [operationId, operationId]);
    assert.deepEqual(commands.map(({ command }) => command), [
      COMMAND_CODE.SyncGroup, COMMAND_CODE.JoinGroup,
      COMMAND_CODE.SyncGroup, COMMAND_CODE.PollMessages
    ]);
  });

  it('leaves a join blocked by an unrelated operation for the caller to resolve', async () => {
    const { client } = stubClient([
      response(Buffer.alloc(0)), new LaneBlockedError(18n)
    ]);
    client.resolveOperation = async () => { assert.fail('must not resolve an unrelated operation'); };
    await assert.rejects(pollMessages(async () => client)(groupRequest), LaneBlockedError);
  });

  it('rejoins when a cached poll reports a missing member', async () => {
    const { client, commands } = stubClient([
      assignment(1n, [4]),
      new ResponseError(COMMAND_CODE.PollMessages, 5006),
      new ResponseError(COMMAND_CODE.SyncGroup, 5006),
      response(Buffer.alloc(0)),
      assignment(2n, [7]),
      pollResponse(7)
    ]);

    assert.equal(
      (await pollMessages(async () => client)(groupRequest)).partitionId,
      7
    );
    assert.deepEqual(
      commands.map(({ command }) => command),
      [
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.JoinGroup,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages
      ]
    );
  });

  it('resynchronizes when a cached poll reports stale ownership', async () => {
    const { client, commands } = stubClient([
      assignment(1n, [4]),
      new ResponseError(COMMAND_CODE.PollMessages, 5009),
      assignment(2n, [7]),
      pollResponse(7)
    ]);

    assert.equal(
      (await pollMessages(async () => client)(groupRequest)).partitionId,
      7
    );
    assert.deepEqual(
      commands.map(({ command }) => command),
      [
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages
      ]
    );
  });

  it('uses kind-tagged cursor keys', async () => {
    const { client, commands } = stubClient([
      assignment(1n, [7]),
      pollResponse(7),
      assignment(1n, [8]),
      pollResponse(8)
    ]);
    const poll = pollMessages(async () => client);
    assert.equal((await poll(groupRequest)).partitionId, 7);
    assert.equal(
      (await poll({
        ...groupRequest,
        streamId: '1',
        topicId: '2',
        consumer: Consumer.Group('3')
      })).partitionId,
      8
    );
    assert.equal(
      commands.filter(
        ({ command }) => command === COMMAND_CODE.SyncGroup
      ).length,
      2
    );
  });

  it('refreshes an expired assignment without a heartbeat', async () => {
    const realNow = Date.now;
    const { client, commands } = stubClient([
      assignment(1n, [7]),
      pollResponse(7),
      assignment(2n, [8]),
      pollResponse(8)
    ]);
    const poll = pollMessages(async () => client);
    try {
      Date.now = () => 0;
      assert.equal((await poll(groupRequest)).partitionId, 7);
      Date.now = () => 5_000;
      assert.equal((await poll(groupRequest)).partitionId, 8);
      assert.equal(
        commands.filter(
          ({ command }) => command === COMMAND_CODE.SyncGroup
        ).length,
        2
      );
    } finally {
      Date.now = realNow;
    }
  });

  it('holds the raw client for the complete multi-command poll', async () => {
    let held = false;
    const { client } = stubClient(
      [assignment(1n, [7]), pollResponse(7)],
      () => assert.equal(held, true)
    );
    client.hold = () => {
      held = true;
      return () => { held = false; };
    };

    assert.equal(
      (await pollMessages(async () => client)(groupRequest)).partitionId,
      7
    );
    assert.equal(held, false);
  });
});

describe('VSR poll context', () => {
  const context = { incarnation: 7n, ownerGeneration: 8n, metadataOp: 9n };

  it('passes the caller context with an explicit partition poll', async () => {
    const { client, commands } = stubClient([pollResponse(4)]);
    await pollMessages(async () => client)({ ...groupRequest, partitionId: 4, context });
    assert.deepEqual(commands.map(({ options }) => options?.context), [context]);
  });

  it('passes the caller context with the partition a group poll selects', async () => {
    const { client, commands } = stubClient([assignment(1n, [4]), pollResponse(4)]);
    await pollMessages(async () => client)({ ...groupRequest, context });
    assert.deepEqual(commands.map(({ options }) => options?.context), [undefined, context]);
  });

  it('surfaces stale ownership under a caller context and resynchronizes next time', async () => {
    const { client, commands } = stubClient([
      assignment(1n, [4]),
      new ResponseError(COMMAND_CODE.PollMessages, 5009),
      assignment(2n, [7]),
      pollResponse(7)
    ]);
    const poll = pollMessages(async () => client);
    await assert.rejects(poll({ ...groupRequest, context }), (error: unknown) =>
      error instanceof ResponseError && error.errorCode === 5009);
    assert.equal((await poll(groupRequest)).partitionId, 7);
    assert.deepEqual(
      commands.map(({ command }) => command),
      [
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages,
        COMMAND_CODE.SyncGroup,
        COMMAND_CODE.PollMessages
      ]
    );
  });
});
