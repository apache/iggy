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
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import type { CommandResponse, RawClient, SendCommandOptions } from '../../client/client.type.js';
import { DEFAULT_MAX_RESPONSE_FRAME_SIZE } from '../../client/client.config.js';
import { FETCH_PROGRESS_UNCERTAIN, PARTITION_LANE_BLOCKED } from '../../client/client.operation.js';
import { COMMAND_CODE } from '../command.code.js';
import { DeserializeError, ResponseError } from '../error.utils.js';
import type { Id } from '../identifier.utils.js';
import { Consumer, type ConsumerKindValue } from '../offset/offset.utils.js';
import { HEADER_SIZE, deferredCancellation, encodeRequestHeader } from '../vsr/header.js';
import { Operation } from '../vsr/operation.js';
import { PollingStrategy, type PollingStrategyKindValue } from './poll.utils.js';
import { MAX_PARTITIONS_PER_REQUEST, MIN_DEFERRED_POLL_MAX_BYTES } from './deferred-poll.utils.js';
import { POLL_MESSAGES } from './poll-messages.command.js';
import {
  FETCH_MESSAGES, FETCH_SECTION_SIZE, decodeFetchSection, fetchMessages, type FetchMessages
} from './fetch-messages.command.js';

type WireId = { kind: 'numeric' | 'named', value: number | string };
type WireStrategy = { kind: number, value: number };
type WireRequest = {
  consumer: { kind: number, id: WireId },
  stream_id: WireId,
  topic_id: WireId,
  count: number,
  auto_commit: boolean,
  wait_us: number,
  min_count: number,
  max_bytes: number,
  request_timeout_us: number
};
type WirePoll = WireRequest & { partition_id: number, strategy: WireStrategy };
type WireFetch = WireRequest & {
  targets: { partition_id: number, strategy: WireStrategy, max_bytes: number, incarnation: number, owner_generation: number }[]
};
type WireSection = {
  partition_id: number, status: number, current_offset: number, messages_count: number,
  context: { incarnation: number, owner_generation: number, metadata_op: number },
  next_offset: number, records_len: number,
  messages: { offset: number, id: string, timestamp: number, origin_timestamp: number, payload: string }[]
};
type Vector<T = object> = T & { case: string, body_hex: string };

const fixture = JSON.parse(readFileSync(new URL('../fixtures/deferred_polling.json', import.meta.url), 'utf8'));

const idOf = ({ value }: WireId): Id => value;
const strategyOf = ({ kind, value }: WireStrategy): PollingStrategy =>
  ({ kind: kind as PollingStrategyKindValue, value: BigInt(value) }) as PollingStrategy;
const consumerOf = ({ consumer }: WireRequest): Consumer =>
  ({ kind: consumer.kind as ConsumerKindValue, id: idOf(consumer.id) });
const optionsOf = (request: WireRequest) => ({
  maxWait: request.wait_us / 1_000,
  requestTimeout: request.request_timeout_us / 1_000,
  minCount: request.min_count,
  maxBytes: request.max_bytes
});
const fetchOf = (request: WireFetch): FetchMessages => ({
  streamId: idOf(request.stream_id),
  topicId: idOf(request.topic_id),
  consumer: consumerOf(request),
  targets: request.targets.map(target => ({
    partitionId: target.partition_id,
    pollingStrategy: strategyOf(target.strategy),
    context: { incarnation: BigInt(target.incarnation), ownerGeneration: BigInt(target.owner_generation), metadataOp: 0n }
  })),
  count: request.count,
  autocommit: request.auto_commit,
  options: optionsOf(request)
});

/** The fields of a fetch body, read back so a refused vector can be offered to the encoder. */
const decodeFetch = (body: Buffer): { request: FetchMessages, targetBytes: number[] } => {
  let at = 0;
  const id = (): Id => {
    const kind = body.readUInt8(at);
    const length = body.readUInt8(at + 1);
    const value = kind === 1 ? body.readUInt32LE(at + 2) : body.toString('utf8', at + 2, at + 2 + length);
    at += 2 + length;
    return value;
  };
  const consumerKind = body.readUInt8(at++) as ConsumerKindValue;
  const consumer = { kind: consumerKind, id: id() };
  const streamId = id();
  const topicId = id();
  const maxWait = Number(body.readBigUInt64LE(at)) / 1_000;
  const requestTimeout = Number(body.readBigUInt64LE(at + 8)) / 1_000;
  const minCount = body.readUInt32LE(at + 16);
  const count = body.readUInt32LE(at + 20);
  const maxBytes = body.readUInt32LE(at + 24);
  const autocommit = body.readUInt8(at + 28) === 1;
  const targetsCount = body.readUInt32LE(at + 29);
  at += 33;
  const targets = Array.from({ length: targetsCount }, (_, index) => {
    const position = at + index * 33;
    return {
      partitionId: body.readUInt32LE(position),
      pollingStrategy: { kind: body.readUInt8(position + 4), value: body.readBigUInt64LE(position + 5) } as PollingStrategy,
      maxBytes: body.readUInt32LE(position + 13),
      context: { incarnation: body.readBigUInt64LE(position + 17), ownerGeneration: body.readBigUInt64LE(position + 25), metadataOp: 0n }
    };
  });
  return {
    request: {
      streamId, topicId, consumer, count, autocommit,
      targets: targets.map(({ partitionId, pollingStrategy, context }) => ({ partitionId, pollingStrategy, context })),
      options: { maxWait, requestTimeout, minCount, maxBytes }
    },
    targetBytes: targets.map(target => target.maxBytes)
  };
};

const fence = { incarnation: 3n, ownerGeneration: 9n, metadataOp: 0n };
const golden: FetchMessages = {
  streamId: 1,
  topicId: 'orders',
  consumer: Consumer.Group(7),
  targets: [
    { partitionId: 1, pollingStrategy: PollingStrategy.Offset(41n), context: fence },
    { partitionId: 2, pollingStrategy: PollingStrategy.Next, context: fence }
  ],
  count: 100,
  autocommit: true,
  options: { maxWait: 5_000, requestTimeout: 6_000, minCount: 1, maxBytes: 1_048_576 }
};

const reply = (hex: string): CommandResponse => {
  const data = Buffer.from(hex, 'hex');
  return { status: 0, length: data.length, data };
};

const refusedWith = (errorCode: number) => (error: unknown): boolean =>
  error instanceof ResponseError && error.commandCode === COMMAND_CODE.FetchMessages && error.errorCode === errorCode;

const targetsOf = (count: number) =>
  Array.from({ length: count }, (_, partitionId) => ({ partitionId, pollingStrategy: PollingStrategy.Next }));

describe('shared wire vectors', () => {
  it('hold the codes and limits of the client', () => {
    const { codes, errors, ...limits } = fixture.constants;
    assert.deepEqual(codes, {
      cancel_deferred_request: COMMAND_CODE.CancelDeferredRequest,
      fetch_messages: COMMAND_CODE.FetchMessages,
      poll_messages: COMMAND_CODE.PollMessages,
      poll_messages_deferred: COMMAND_CODE.PollMessagesDeferred
    });
    assert.deepEqual([errors.partition_lane_blocked, errors.fetch_progress_uncertain],
      [PARTITION_LANE_BLOCKED, FETCH_PROGRESS_UNCERTAIN]);
    assert.deepEqual(
      [limits.min_deferred_poll_max_bytes, limits.max_partitions_per_request, limits.request_header_size,
        limits.fetch_section_header_size],
      [MIN_DEFERRED_POLL_MAX_BYTES, MAX_PARTITIONS_PER_REQUEST, HEADER_SIZE, FETCH_SECTION_SIZE]);
    assert.ok(DEFAULT_MAX_RESPONSE_FRAME_SIZE >= limits.max_record_reply_size);
  });

  for (const vector of fixture.deferred_polls as Vector<{ request: WirePoll }>[])
    it(`encodes the deferred poll ${vector.case}`, () => {
      const { request } = vector;
      const body = POLL_MESSAGES.serialize({
        streamId: idOf(request.stream_id),
        topicId: idOf(request.topic_id),
        consumer: consumerOf(request),
        partitionId: request.partition_id,
        pollingStrategy: strategyOf(request.strategy),
        count: request.count,
        autocommit: request.auto_commit,
        deferred: optionsOf(request)
      });
      assert.equal(body.toString('hex'), vector.body_hex);
    });

  // The client gives every target the limit of the whole reply, since the
  // server serves a partition only the bytes it holds, so the vectors are
  // compared with that limit in each target.
  for (const vector of fixture.fetches as Vector<{ request: WireFetch }>[])
    it(`encodes the fetch ${vector.case} with the reply limit as each target limit`, () => {
      const expected = Buffer.from(vector.body_hex, 'hex');
      const { targetBytes } = decodeFetch(expected);
      const targetsAt = expected.length - targetBytes.length * 33;
      targetBytes.forEach((bytes, index) => {
        assert.equal(bytes, vector.request.targets[index].max_bytes);
        expected.writeUInt32LE(vector.request.max_bytes, targetsAt + index * 33 + 13);
      });

      assert.equal(FETCH_MESSAGES.serialize(fetchOf(vector.request)).toString('hex'), expected.toString('hex'));
    });

  for (const vector of fixture.cancellations as { case: string, cancels_code: number, client: number,
    request: number, session: number, header_hex: string }[])
    it(`encodes the cancellation ${vector.case}`, () => {
      const parked = encodeRequestHeader({
        size: HEADER_SIZE + 64,
        client: BigInt(vector.client),
        request: BigInt(vector.request),
        operation: Operation.NonReplicated,
        session: BigInt(vector.session),
        nonReplicatedCode: vector.cancels_code,
        context: fence
      });

      assert.equal(deferredCancellation(parked)?.toString('hex'), vector.header_hex);
    });

  for (const vector of fixture.fetch_replies as Vector<{ sections: WireSection[] }>[])
    it(`decodes the fetch reply ${vector.case}`, () => {
      const sections = FETCH_MESSAGES.deserialize(reply(vector.body_hex));

      assert.deepEqual(sections.map(section => ({ ...section, records: section.records.length })),
        vector.sections.map(section => ({
          partitionId: section.partition_id,
          status: section.status,
          currentOffset: BigInt(section.current_offset),
          messagesCount: section.messages_count,
          context: {
            incarnation: BigInt(section.context.incarnation),
            ownerGeneration: BigInt(section.context.owner_generation),
            metadataOp: BigInt(section.context.metadata_op)
          },
          nextOffset: BigInt(section.next_offset),
          records: section.records_len
        })));
      sections.forEach((section, index) => {
        const messages = section.status === 0 ? decodeFetchSection(section).messages : [];
        assert.deepEqual(messages.map(({ headers, payload }) => ({
          offset: headers.offset,
          id: BigInt(headers.id),
          timestamp: headers.timestamp.getTime(),
          originTimestamp: headers.originTimestamp.getTime(),
          payload: payload.toString()
        })), vector.sections[index].messages.map(message => ({
          offset: BigInt(message.offset),
          id: BigInt(message.id),
          // Message headers carry a Date, which holds milliseconds.
          timestamp: Math.floor(message.timestamp / 1_000),
          originTimestamp: Math.floor(message.origin_timestamp / 1_000),
          payload: message.payload
        })));
        if (section.status !== 0)
          assert.throws(() => decodeFetchSection(section), (error: unknown) =>
            refusedWith(section.status)(error) && error instanceof ResponseError && error.fromServer);
      });
    });

  for (const vector of fixture.refused.fetch_replies as Vector[])
    it(`refuses the fetch reply ${vector.case}`, () => {
      assert.throws(() => FETCH_MESSAGES.deserialize(reply(vector.body_hex)), DeserializeError);
    });

  // One byte limit goes to every target, so a target above the request limit
  // cannot be asked for.
  for (const vector of fixture.refused.fetches as Vector[])
    it(`refuses the fetch ${vector.case} before anything is sent`, () => {
      const { request, targetBytes } = decodeFetch(Buffer.from(vector.body_hex, 'hex'));
      if (targetBytes.some(bytes => bytes > (request.options?.maxBytes ?? 0)))
        return;

      assert.throws(() => FETCH_MESSAGES.serialize(request), ResponseError);
    });
});

describe('fetch encoding', () => {
  it('writes a target without a context with the route context left to the client', () => {
    const body = FETCH_MESSAGES.serialize({ ...golden, targets: [{ partitionId: 1, pollingStrategy: PollingStrategy.Next }] });
    assert.equal(body.subarray(body.length - 16).toString('hex'), '00'.repeat(16));
  });

  const refusals: [string, FetchMessages][] = [
    ['no target', { ...golden, targets: [] }],
    ['more than 1000 targets', { ...golden, targets: targetsOf(1_001) }],
    ['a target twice', { ...golden, targets: [golden.targets[0], golden.targets[0]] }],
    ['a First target', { ...golden, targets: [{ partitionId: 1, pollingStrategy: PollingStrategy.First }] }],
    ['a Last target', { ...golden, targets: [{ partitionId: 1, pollingStrategy: PollingStrategy.Last }] }],
    ['a Timestamp target', { ...golden, targets: [{ partitionId: 1, pollingStrategy: PollingStrategy.Timestamp(1n) }] }],
    ['a wait above 600 s', { ...golden, options: { maxWait: 600_001, requestTimeout: 630_000 } }],
    ['a request timeout above 630 s', { ...golden, options: { maxWait: 1_000, requestTimeout: 630_001 } }],
    ['a request timeout without 1 s after the wait', { ...golden, options: { maxWait: 5_000, requestTimeout: 5_999 } }],
    ['a wait of 0 without 1 s before the request timeout', { ...golden, options: { maxWait: 0, requestTimeout: 999 } }],
    ['a wait of 0 with exactly 1 s before the request timeout', { ...golden, options: { maxWait: 0, requestTimeout: 1_000 } }],
    ['a fractional millisecond timeout', { ...golden, options: { maxWait: 0, requestTimeout: 1_000.0001 } }]
  ];
  for (const [name, request] of refusals)
    it(`refuses ${name} with 3 before anything is sent`, () => {
      assert.throws(() => FETCH_MESSAGES.serialize(request), refusedWith(3));
    });

  for (const [name, request, errorCode] of [
    ['a min count of 0', { ...golden, options: { ...golden.options, minCount: 0 } }, 4009],
    ['a min count above the count', { ...golden, options: { ...golden.options, minCount: 101 } }, 4009],
    ['a byte limit below 344', { ...golden, options: { ...golden.options, maxBytes: 343 } }, 80]
  ] as const)
    it(`refuses ${name} with ${errorCode}`, () => {
      assert.throws(() => FETCH_MESSAGES.serialize(request), refusedWith(errorCode));
    });

  it('accepts 1000 targets and a wait of 0 with more than 1 s before the request timeout', () => {
    assert.ok(FETCH_MESSAGES.serialize({ ...golden, targets: targetsOf(1_000) }).length > 0);
    assert.ok(FETCH_MESSAGES.serialize({ ...golden, options: { maxWait: 0, requestTimeout: 1_001 } }).length > 0);
  });

  const head = (count: number) => Buffer.from([count, 0, 0, 0]);
  const section = (status: number, recordsLength: number) => {
    const header = Buffer.alloc(56);
    header.writeUInt32LE(1, 0);
    header.writeUInt32LE(status, 4);
    header.writeUInt32LE(recordsLength, 52);
    return header;
  };
  for (const [name, data] of [
    ['a truncated count', Buffer.from([1, 0])],
    ['more sections than fit its body', head(2)],
    ['more than 1000 sections', Buffer.from('e9030000', 'hex')],
    ['records in a refused section', Buffer.concat([head(1), section(89, 1), Buffer.alloc(1)])],
    ['records past its body', Buffer.concat([head(1), section(0, 2), Buffer.alloc(1)])],
    ['bytes after its last section', Buffer.concat([head(1), section(0, 0), Buffer.alloc(1)])]
  ] as const)
    it(`rejects a reply with ${name}`, () => {
      assert.throws(() => FETCH_MESSAGES.deserialize({ status: 0, length: data.length, data }), DeserializeError);
    });

  it('sends the fetch as command 109 with the signal of the caller', async () => {
    const signal = new AbortController().signal;
    const sent: { command: number, payload: Buffer, options?: SendCommandOptions }[] = [];
    const client = {
      sendCommand: async (command: number, payload: Buffer, options?: SendCommandOptions) => {
        sent.push({ command, payload, options });
        return reply('00000000');
      }
    } as RawClient;
    assert.deepEqual(await fetchMessages(async () => client)({ ...golden, signal }), []);
    assert.deepEqual(sent.map(({ command, payload, options }) => [command, payload, options]),
      [[COMMAND_CODE.FetchMessages, FETCH_MESSAGES.serialize(golden), { signal }]]);
  });
});
