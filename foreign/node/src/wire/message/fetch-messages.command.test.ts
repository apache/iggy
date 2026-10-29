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
import { describe, it } from 'node:test';
import type { CommandResponse, RawClient, SendCommandOptions } from '../../client/client.type.js';
import { COMMAND_CODE } from '../command.code.js';
import { DeserializeError, ResponseError } from '../error.utils.js';
import { Consumer } from '../offset/offset.utils.js';
import { PollingStrategy } from './poll.utils.js';
import {
  FETCH_MESSAGES, decodeFetchSection, fetchMessages, readinessGroups, type FetchMessages
} from './fetch-messages.command.js';

// The golden vectors of the Rust codec for group 7, stream 1 and topic
// "orders": a 5 s wait within a 6 s timeout, 100 messages of 1 MiB with
// auto-commit, partition 1 at offset 41 and partition 2 at Next, both fenced
// at incarnation 3 and owner generation 9.
const FETCH_BODY = '0201040700000001040100000002066f7264657273404b4c0000000000808d5b0000000000' +
  '0100000064000000000010000102000000010000000129000000000000000000080003000000000000000900000000000000' +
  '020000000500000000000000000000080003000000000000000900000000000000';
// Partition 1 serves "hello" at offset 41 in context (3, 9, 12) with next
// offset 42, and partition 2 is refused as not the primary.
const FETCH_REPLY = '020000000100000000000000290000000000000001000000030000000000000009000000000000000c000000000000002a00' +
  '0000000000003501000002000000590000000000000000000000000000000000000000000000000000000000000000000000' +
  '000000000000000000000000000000000100000000000000290000000000000064401e18240a060000401e18240a06003501' +
  '000000000000bd2d787d3cdb745c010000000000000000000000000000000000000000000000000000000000000000000000' +
  '0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' +
  '0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' +
  '0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' +
  '000000000000000000000000000000000000000000008e5ca59edbbdbfb90100000000000000000000000000000000000000' +
  '000000000000000005000000000000000000000068656c6c6f';
// Each golden target asks for 512 KiB. The client gives every target the
// limit of the whole reply instead, so the server never splits a batch.
const GOLDEN_TARGET_CAP = 524_288;
const TARGET_CAP_AT = [67, 100];

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

describe('fetch encoding', () => {
  it('encodes the golden fetch body with the reply limit as each target limit', () => {
    const expected = Buffer.from(FETCH_BODY, 'hex');
    for (const at of TARGET_CAP_AT) {
      assert.equal(expected.readUInt32LE(at), GOLDEN_TARGET_CAP);
      expected.writeUInt32LE(1_048_576, at);
    }
    assert.equal(FETCH_MESSAGES.serialize(golden).toString('hex'), expected.toString('hex'));
  });

  it('decodes the golden fetch reply into its sections', () => {
    const [served, refused, ...rest] = FETCH_MESSAGES.deserialize(reply(FETCH_REPLY));
    assert.deepEqual(rest, []);
    assert.deepEqual({ ...served, records: served.records.length }, {
      partitionId: 1,
      status: 0,
      currentOffset: 41n,
      messagesCount: 1,
      context: { incarnation: 3n, ownerGeneration: 9n, metadataOp: 12n },
      nextOffset: 42n,
      records: 309
    });
    assert.deepEqual({ ...refused, records: refused.records.length }, {
      partitionId: 2,
      status: 89,
      currentOffset: 0n,
      messagesCount: 0,
      context: { incarnation: 0n, ownerGeneration: 0n, metadataOp: 0n },
      nextOffset: 0n,
      records: 0
    });

    const polled = decodeFetchSection(served);
    assert.equal(polled.partitionId, 1);
    assert.equal(polled.count, 1);
    assert.deepEqual(polled.context, served.context);
    assert.equal(polled.messages[0].headers.offset, 41n);
    assert.equal(polled.messages[0].payload.toString(), 'hello');
    assert.throws(() => decodeFetchSection(refused), (error: unknown) =>
      refusedWith(89)(error) && error instanceof ResponseError && error.fromServer);
  });

  it('decodes a reply without sections', () => {
    assert.deepEqual(FETCH_MESSAGES.deserialize(reply('00000000')), []);
  });

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

  it('asks the client for the readiness groups, and fails with 5 on a client without them', async () => {
    const signal = new AbortController().signal;
    const asked: unknown[] = [];
    const client = {
      readinessGroups: async (...args: unknown[]) => {
        asked.push(args);
        return [[2], [1, 3]];
      }
    } as unknown as RawClient;
    const request = { streamId: 1, topicId: 'orders', partitions: [1, 2, 3], signal };
    assert.deepEqual(await readinessGroups(async () => client)(request), [[2], [1, 3]]);
    assert.deepEqual(asked, [[1, 'orders', [1, 2, 3], { signal }]]);
    await assert.rejects(readinessGroups(async () => ({}) as RawClient)(request),
      (error: unknown) => error instanceof ResponseError && error.errorCode === 5);
  });
});
