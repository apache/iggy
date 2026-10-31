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
import { readFileSync } from 'node:fs';
import { Readable } from 'node:stream';
import { describe, it } from 'node:test';
import type { CommandResponse, RawClient, SendCommandOptions } from '../../client/client.type.js';
import { COMMAND_CODE } from '../command.code.js';
import type { ResponseError } from '../error.utils.js';
import { Consumer, ConsumerKind } from '../offset/offset.utils.js';
import { storePosition, deletePosition } from '../offset/position.command.js';
import { PollingStrategy, PollingStrategyKind } from './poll.utils.js';
import { DEFERRED_POLL_OFFSET, DEFERRED_POLL_OPTIONS_SIZE } from './deferred-poll.utils.js';
import { pollMessages } from './poll-messages.command.js';
import {
  CommitAfter, CommitEvery, CommitPolicy, DEFAULT_AUTO_COMMIT, DEFAULT_POLL_OPTIONS, pollOptions
} from './poll-options.js';
import { Checkpoint } from './checkpoint.js';

const INVALID_CONFIGURATION = 2;
const INVALID_COMMAND = 3;
const INVALID_MESSAGES_COUNT = 4009;
const SERVED = { incarnation: 11n, ownerGeneration: 4n, metadataOp: 42n };

const fixture = JSON.parse(readFileSync(new URL('../fixtures/deferred_polling.json', import.meta.url), 'utf8'));

/** A poll reply of the fixture section that holds offsets 41 to 43 of partition 1. */
const servedReply = (): Buffer => {
  const vector = fixture.fetch_replies.find((reply: { case: string }) => reply.case === 'ok_section_and_empty_start_section');
  const body = Buffer.from(vector.body_hex, 'hex');
  const records = body.subarray(4 + 2 * 56, 4 + 2 * 56 + vector.sections[0].records_len);
  return pollReply(1, 3, records);
};

const pollReply = (partitionId: number, count: number, records: Buffer = Buffer.alloc(0)): Buffer => {
  const head = Buffer.alloc(40);
  head.writeUInt32LE(partitionId, 0);
  head.writeBigUInt64LE(43n, 4);
  head.writeUInt32LE(count, 12);
  head.writeBigUInt64LE(SERVED.incarnation, 16);
  head.writeBigUInt64LE(SERVED.ownerGeneration, 24);
  head.writeBigUInt64LE(SERVED.metadataOp, 32);
  return Buffer.concat([head, records]);
};

const stubClient = (replies: Buffer[]) => {
  const emitter = new EventEmitter();
  const commands: { command: number, payload: Buffer, options?: SendCommandOptions }[] = [];
  const client = {
    protocol: 'vsr',
    isAuthenticated: true,
    sendCommand: async (command: number, payload: Buffer, options?: SendCommandOptions): Promise<CommandResponse> => {
      commands.push({ command, payload, options });
      const data = replies.shift() ?? Buffer.alloc(0);
      return { status: 0, length: data.length, data };
    },
    authenticate: async () => true,
    destroy: () => {},
    on: emitter.on.bind(emitter),
    once: emitter.once.bind(emitter),
    getReadStream: () => Readable.from([])
  } as RawClient;
  return { client, commands };
};

const consumer = { kind: ConsumerKind.Single, id: 4 };

describe('poll options', () => {
  it('answers at once and commits nothing by default', () => {
    assert.deepEqual(DEFAULT_POLL_OPTIONS,
      { count: 1000, maxWait: 0, minCount: 1, maxBytes: 1_048_576, commit: CommitPolicy.Manual });
    assert.deepEqual(pollOptions(), DEFAULT_POLL_OPTIONS);
  });

  it('stores every 10000 messages or 5 seconds after processing by default', () => {
    assert.deepEqual(DEFAULT_AUTO_COMMIT,
      { kind: 'auto', interval: 5_000, every: CommitEvery.Messages(10_000), after: CommitAfter.Processing });
    assert.deepEqual(CommitPolicy.Auto(), DEFAULT_AUTO_COMMIT);
  });

  it('refuses an automatic commit without an interval or an event, and an invalid count', () => {
    const refused = (options: Parameters<typeof pollOptions>[0]) => {
      try {
        pollOptions(options);
      } catch (error) {
        return (error as ResponseError).errorCode;
      }
      return 0;
    };

    assert.equal(refused({ commit: CommitPolicy.Auto({ after: CommitAfter.Delivery }) }), INVALID_CONFIGURATION);
    assert.equal(refused({ commit: CommitPolicy.Auto({ interval: 0, every: CommitEvery.Batch }) }),
      INVALID_CONFIGURATION);
    assert.equal(refused({ count: 0 }), INVALID_MESSAGES_COUNT);
    assert.equal(refused({ commit: CommitPolicy.Auto({ every: CommitEvery.Batch }) }), 0);
    assert.throws(() => CommitEvery.Messages(0), RangeError);
  });
});

describe('partition poll', () => {
  for (const onRead of [false, true]) {
    it(`sends one deferred poll with the limits of its options${onRead ? ' and commits on read' : ''}`, async () => {
      const { client, commands } = stubClient([pollReply(1, 0)]);
      const options = { count: 50, maxWait: 1_500, minCount: 2, maxBytes: 4_096,
        commit: onRead ? CommitPolicy.OnRead : CommitPolicy.Manual };

      await pollMessages(async () => client)({
        streamId: 1, topicId: 2, partitionId: 1, consumer, strategy: PollingStrategy.Next, options
      });

      const [{ command, payload }] = commands;
      assert.equal(command, COMMAND_CODE.PollMessagesDeferred);
      const tail = payload.subarray(payload.length - DEFERRED_POLL_OPTIONS_SIZE);
      assert.equal(payload[payload.length - DEFERRED_POLL_OPTIONS_SIZE - 1], onRead ? 1 : 0);
      assert.equal(payload.readUInt32LE(payload.length - DEFERRED_POLL_OPTIONS_SIZE - 5), 50);
      assert.equal(tail.readBigUInt64LE(DEFERRED_POLL_OFFSET.wait), 1_500_000n);
      assert.equal(tail.readUInt32LE(DEFERRED_POLL_OFFSET.minCount), 2);
      assert.equal(tail.readUInt32LE(DEFERRED_POLL_OFFSET.maxBytes), 4_096);
      assert.equal(tail.readBigUInt64LE(DEFERRED_POLL_OFFSET.requestTimeout), 11_500_000n);
    });
  }

  it('refuses an automatic commit before it sends anything', async () => {
    const { client, commands } = stubClient([]);

    await assert.rejects(pollMessages(async () => client)({
      streamId: 1, topicId: 2, partitionId: 1, consumer, strategy: PollingStrategy.Next,
      options: { commit: CommitPolicy.Auto() }
    }), (error: ResponseError) => error.errorCode === INVALID_CONFIGURATION);
    assert.equal(commands.length, 0);
  });

  it('names the position after each message and continues after the last one on its partition only', async () => {
    const { client, commands } = stubClient([servedReply(), pollReply(1, 0)]);
    const poll = pollMessages(async () => client);

    const polled = await poll({ streamId: 1, topicId: 2, partitionId: 1, consumer, strategy: PollingStrategy.Next });
    await poll({ streamId: 1, topicId: 2, partitionId: 1, consumer, strategy: polled.continuation() });

    assert.deepEqual(polled.checkpoint(), new Checkpoint(1, 43n, SERVED));
    assert.deepEqual(polled.checkpointAt(0), new Checkpoint(1, 41n, SERVED));
    assert.equal(polled.checkpointAt(3), undefined);
    const continued = commands[1];
    assert.deepEqual(continued.options?.context, SERVED);
    const strategyAt = continued.payload.length - DEFERRED_POLL_OPTIONS_SIZE - 14;
    assert.equal(continued.payload[strategyAt], PollingStrategyKind.Offset);
    assert.equal(continued.payload.readBigUInt64LE(strategyAt + 1), 44n);
    await assert.rejects(poll({ streamId: 1, topicId: 2, partitionId: 2, consumer, strategy: polled.continuation() }),
      (error: ResponseError) => error.errorCode === INVALID_COMMAND);
    assert.equal(commands.length, 2);
  });

  for (const [start, kind, value] of [
    [PollingStrategy.Next, PollingStrategyKind.Next, 0n],
    [PollingStrategy.First, PollingStrategyKind.Offset, 0n],
    [PollingStrategy.Offset(7n), PollingStrategyKind.Offset, 7n],
    [PollingStrategy.Last, PollingStrategyKind.Last, 0n]
  ] as const) {
    it(`keeps the start ${start.kind} of an empty batch in its continuation`, async () => {
      const { client } = stubClient([pollReply(1, 0)]);

      const polled = await pollMessages(async () => client)({
        streamId: 1, topicId: 2, partitionId: 1, consumer, strategy: start
      });

      assert.equal(polled.checkpoint(), undefined);
      const continuation = polled.continuation();
      assert.deepEqual([continuation.kind, continuation.value, continuation.context], [kind, value, SERVED]);
    });
  }

  it('stores and deletes a checkpoint under the history that delivered it', async () => {
    const { client, commands } = stubClient([Buffer.alloc(0), Buffer.alloc(0)]);
    const checkpoint = new Checkpoint(1, 43n, SERVED);

    await storePosition(async () => client)({ streamId: 1, topicId: 2, consumer: Consumer.Group(3), checkpoint });
    await deletePosition(async () => client)({ streamId: 1, topicId: 2, consumer: Consumer.Group(3), checkpoint });

    assert.deepEqual(commands.map(({ command, options }) => [command, options?.context]),
      [[COMMAND_CODE.StoreOffset, SERVED], [COMMAND_CODE.DeleteConsumerOffset, SERVED]]);
  });
});
