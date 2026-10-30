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
import { EventEmitter, once } from 'node:events';
import { Readable } from 'node:stream';
import { describe, it } from 'node:test';
import { FetchProgressUncertainError } from '../client/client.operation.js';
import type { CommandResponse, RawClient, SendCommandOptions } from '../client/client.type.js';
import { COMMAND_CODE } from '../wire/command.code.js';
import { responseError } from '../wire/error.utils.js';
import { ConsumerKind } from '../wire/offset/offset.utils.js';
import { encodeMessagesBatch } from '../wire/message/message.utils.js';
import { BATCH_HEADER_SIZE, FRAME_HEADER_SIZE } from '../wire/message/iggy-header.utils.js';
import { u128ToBuf } from '../wire/number.utils.js';
import { PollingStrategy, PollingStrategyKind } from '../wire/message/poll.utils.js';
import {
  FETCH_MESSAGES_FIELD, FETCH_MESSAGES_FIELDS_SIZE, FETCH_SECTION_FIELD, FETCH_TARGET_FIELD, FETCH_TARGET_SIZE,
  fetchReplyHeadSize
} from '../wire/message/fetch-messages.command.js';
import { DEFERRED_POLL_OFFSET, DEFERRED_POLL_OPTIONS_SIZE } from '../wire/message/deferred-poll.utils.js';
import { CommitAfter, CommitEvery, CommitPolicy } from '../wire/message/poll-options.js';
import { Checkpoint } from '../wire/message/checkpoint.js';
import { PartitionReader, ReadError } from './partition-reader.js';
import { IggyConsumer } from './iggy-consumer.js';

const SERVED = { incarnation: 11n, ownerGeneration: 4n, metadataOp: 42n };
const BATCH_BASE_OFFSET_AT = 8;
const HISTORY_UNAVAILABLE = 87;
const INVALID_COMMAND = 3;
/** A poll body ends with the strategy kind and value, the count and the auto-commit flag. */
const POLL_SUFFIX_SIZE = 1 + 8 + 4 + 1;

type Read = { code: number, partition: number, kind: number, value: bigint, autocommit: boolean };

/** User headers whose first key has length zero, so they do not decode. */
const UNPARSABLE_HEADERS = Buffer.from([1, 0, 0, 0, 0]);

/** Two messages from `offset` as batch records. */
const records = (offset: bigint, unparsableHeaders = new Set<bigint>()): Buffer => {
  const batch = encodeMessagesBatch([0, 1].map((index) => ({
    id: u128ToBuf(BigInt(index + 1)), payload: Buffer.from(`m${offset + BigInt(index)}`),
    userHeaders: unparsableHeaders.has(offset + BigInt(index)) ? UNPARSABLE_HEADERS : Buffer.alloc(0),
    originTimestamp: 0n
  })));
  batch.writeBigUInt64LE(offset, BATCH_BASE_OFFSET_AT);
  return batch;
};

/**
 * A client of one topic whose partitions hold `limit` messages each, two per
 * read, with primaries `partition % 2`. It stores and deletes offsets, and
 * answers a read that finds nothing with an empty reply.
 */
class FakeTopic {
  readonly reads: Read[] = [];
  readonly stored = new Map<number, bigint>();
  readonly undecodable = new Set<bigint>();
  /** The offsets whose user headers do not decode. */
  readonly unparsableHeaders = new Set<bigint>();
  readonly history = new Set<number>();
  readonly emitter = new EventEmitter();
  /** The partitions of the group member, and the assignment it moves to once a committing read is served. */
  assigned: number[] = [];
  assignedAfterRead?: number[];
  /** Fails every route lookup, as a client without a link does. */
  unrouted?: Error;
  routeLookups = 0;
  loseNextCommittingRead = false;
  onLostCommittingRead?: () => void;
  /** A fetch reads its targets in order until it holds its count, as the server does. */
  countLimited = false;
  /** A fetch replies without sections. */
  sectionless = false;
  constructor(readonly limit = 2n) {}

  client(): RawClient {
    return {
      protocol: 'vsr',
      isAuthenticated: true,
      sendCommand: async (code: number, payload: Buffer, options?: SendCommandOptions) => this.answer(code, payload, options),
      readinessGroups: async (_streamId: unknown, _topicId: unknown, partitions: number[]) => {
        this.routeLookups += 1;
        if (this.unrouted) throw this.unrouted;
        return [0, 1].map((primary) => partitions.filter((partition) => partition % 2 === primary))
          .filter((group) => group.length > 0);
      },
      authenticate: async () => true,
      destroy: () => {},
      on: this.emitter.on.bind(this.emitter),
      once: this.emitter.once.bind(this.emitter),
      getReadStream: () => Readable.from([])
    } as unknown as RawClient;
  }

  private start(partition: number, kind: number, value: bigint): bigint {
    return kind === PollingStrategyKind.Next ? (this.stored.has(partition) ? this.stored.get(partition)! + 1n : 0n)
      : kind === PollingStrategyKind.Offset ? value : 0n;
  }

  private async answer(code: number, payload: Buffer, options?: SendCommandOptions): Promise<CommandResponse> {
    const reply = (data: Buffer): CommandResponse => ({ status: 0, length: data.length, data });
    if (code === COMMAND_CODE.StoreOffset) {
      this.stored.set(payload.readUInt32LE(payload.length - 13), payload.readBigUInt64LE(payload.length - 9));
      return reply(Buffer.alloc(0));
    }
    if (code === COMMAND_CODE.DeleteConsumerOffset) {
      this.stored.delete(payload.readUInt32LE(payload.length - 5));
      return reply(Buffer.alloc(0));
    }
    if (code === COMMAND_CODE.JoinGroup || code === COMMAND_CODE.LeaveGroup) return reply(Buffer.alloc(0));
    if (code === COMMAND_CODE.SyncGroup) {
      const assignment = Buffer.alloc(12 + 4 * this.assigned.length);
      assignment.writeUInt32LE(this.assigned.length, 8);
      this.assigned.forEach((partition, index) => assignment.writeUInt32LE(partition, 12 + 4 * index));
      setImmediate(() => this.emitter.emit('synced'));
      return reply(assignment);
    }
    if (code === COMMAND_CODE.PollMessagesDeferred) {
      const body = payload.subarray(0, payload.length - DEFERRED_POLL_OPTIONS_SIZE);
      const at = body.length - POLL_SUFFIX_SIZE;
      const partition = body.readUInt32LE(at - 4);
      const read = { code, partition, kind: body[at], value: body.readBigUInt64LE(at + 1),
        autocommit: body[body.length - 1] !== 0 };
      this.reads.push(read);
      const start = this.start(partition, read.kind, read.value);
      if (start >= this.limit) return this.idle(options, reply(pollReply(partition, start, Buffer.alloc(0), 0)));
      if (read.autocommit) this.stored.set(partition, start + 1n);
      const batch = records(start, this.unparsableHeaders);
      return reply(pollReply(partition, start + 1n, this.undecodable.has(start) ? batch.subarray(0, batch.length - 1) : batch, 2));
    }
    if (code === COMMAND_CODE.FetchMessages) {
      let targets = 1;
      while (payload.readUInt32LE(payload.length - FETCH_MESSAGES_FIELDS_SIZE - targets * FETCH_TARGET_SIZE +
          FETCH_MESSAGES_FIELD.targetsCount) !== targets) targets += 1;
      const fieldsAt = payload.length - FETCH_MESSAGES_FIELDS_SIZE - targets * FETCH_TARGET_SIZE;
      const autocommit = payload[fieldsAt + FETCH_MESSAGES_FIELD.autoCommit] !== 0;
      const count = payload.readUInt32LE(fieldsAt + FETCH_MESSAGES_FIELD.count);
      const sections: { partition: number, status: number, start: bigint, records: Buffer }[] = [];
      let held = 0;
      for (let index = 0; index < targets; index += 1) {
        const at = fieldsAt + FETCH_MESSAGES_FIELDS_SIZE + index * FETCH_TARGET_SIZE;
        const partition = payload.readUInt32LE(at + FETCH_TARGET_FIELD.partitionId);
        const read = { code, partition, kind: payload[at + FETCH_TARGET_FIELD.kind],
          value: payload.readBigUInt64LE(at + FETCH_TARGET_FIELD.value), autocommit };
        this.reads.push(read);
        if (this.history.has(partition)) {
          sections.push({ partition, status: HISTORY_UNAVAILABLE, start: 0n, records: Buffer.alloc(0) });
          continue;
        }
        const start = this.start(partition, read.kind, read.value);
        if (start >= this.limit || (this.countLimited && held >= count)) {
          sections.push({ partition, status: 0, start, records: Buffer.alloc(0) });
          continue;
        }
        held += 2;
        if (autocommit) this.stored.set(partition, start + 1n);
        const batch = records(start, this.unparsableHeaders);
        sections.push({ partition, status: 0, start,
          records: this.undecodable.has(start) ? batch.subarray(0, batch.length - 1) : batch });
      }
      if (autocommit && this.loseNextCommittingRead) {
        this.loseNextCommittingRead = false;
        this.onLostCommittingRead?.();
        throw new FetchProgressUncertainError(1n, new Error('reply lost after storing'));
      }
      const body = fetchReply(this.sectionless ? [] : sections);
      if (autocommit && this.assignedAfterRead) {
        this.assigned = this.assignedAfterRead;
        this.assignedAfterRead = undefined;
      }
      return sections.every(({ records: held, status }) => held.length === 0 && status === 0)
        ? this.idle(options, reply(body)) : reply(body);
    }
    throw responseError(code, INVALID_COMMAND, 'not served by the fake topic');
  }

  /** A read that finds nothing waits until the reader stops it, as a parked read does. */
  private idle(options: SendCommandOptions | undefined, reply: CommandResponse): Promise<CommandResponse> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => resolve(reply), 50);
      options?.signal?.addEventListener('abort', () => {
        clearTimeout(timer);
        reject(options.signal!.reason);
      }, { once: true });
    });
  }
}

const pollReply = (partition: number, current: bigint, held: Buffer, count: number): Buffer => {
  const head = Buffer.alloc(40);
  head.writeUInt32LE(partition, 0);
  head.writeBigUInt64LE(current, 4);
  head.writeUInt32LE(count, 12);
  head.writeBigUInt64LE(SERVED.incarnation, 16);
  head.writeBigUInt64LE(SERVED.ownerGeneration, 24);
  head.writeBigUInt64LE(SERVED.metadataOp, 32);
  return Buffer.concat([head, held]);
};

const fetchReply = (sections: { partition: number, status: number, start: bigint, records: Buffer }[]): Buffer => {
  const head = Buffer.alloc(fetchReplyHeadSize(sections.length));
  head.writeUInt32LE(sections.length, 0);
  sections.forEach(({ partition, status, start, records: held }, index) => {
    const at = fetchReplyHeadSize(index);
    const count = held.length === 0 ? 0 : 2;
    head.writeUInt32LE(partition, at + FETCH_SECTION_FIELD.partitionId);
    head.writeUInt32LE(status, at + FETCH_SECTION_FIELD.status);
    head.writeUInt32LE(count, at + FETCH_SECTION_FIELD.messagesCount);
    if (status === 0) {
      head.writeBigUInt64LE(SERVED.incarnation, at + FETCH_SECTION_FIELD.context);
      head.writeBigUInt64LE(SERVED.ownerGeneration, at + FETCH_SECTION_FIELD.context + 8);
      head.writeBigUInt64LE(SERVED.metadataOp, at + FETCH_SECTION_FIELD.context + 16);
    }
    head.writeBigUInt64LE(start + BigInt(count), at + FETCH_SECTION_FIELD.nextOffset);
    head.writeUInt32LE(held.length, at + FETCH_SECTION_FIELD.recordsLength);
  });
  return Buffer.concat([head, ...sections.map(({ records: held }) => held)]);
};

const consumer = { kind: ConsumerKind.Single, id: 4 };

const open = (topic: FakeTopic, partitions: number[], options = {}) => {
  const client = topic.client();
  return PartitionReader.open(async () => client, {
    streamId: 1, topicId: 2, consumer, partitions, options: { maxWait: 100, ...options }, retryInterval: 20
  });
};

const offsets = (batches: { messages: { headers: { offset: bigint } }[] }[]): bigint[] =>
  batches.flatMap(({ messages }) => messages.map(({ headers }) => headers.offset));

describe('partition reader', () => {
  it('reads the partitions of each primary in one fetch and hands over every partition', async () => {
    const topic = new FakeTopic();
    const reader = await open(topic, [0, 1, 2, 3]);
    const partitions = [];
    for (let read = 0; read < 4; read += 1) partitions.push((await reader.next()).partitionId);
    await reader.close();

    assert.deepEqual(partitions.sort(), [0, 1, 2, 3]);
    const fetched = topic.reads.filter(({ code }) => code === COMMAND_CODE.FetchMessages);
    assert.ok(fetched.length >= 4);
  });

  it('stores a checkpoint of its own and refuses one of another reader', async () => {
    const topic = new FakeTopic();
    const reader = await open(topic, [1]);
    const batch = await reader.next();

    await reader.commit(batch.checkpoint()!);
    await assert.rejects(reader.commit(new Checkpoint(1, 1n, SERVED)), (error: { errorCode: number }) =>
      error.errorCode === INVALID_COMMAND);
    await reader.close();

    assert.equal(topic.stored.get(1), 1n);
  });

  it('keeps the configured limits for its initial probe and does not retry an oversized first record', async () => {
    const topic = new FakeTopic();
    const client = topic.client();
    const reads: { code: number, payload: Buffer }[] = [];
    const record = encodeMessagesBatch([{
      id: u128ToBuf(1n), payload: Buffer.alloc(2_000), userHeaders: Buffer.alloc(0), originTimestamp: 0n
    }]);
    record.writeBigUInt64LE(0n, BATCH_BASE_OFFSET_AT);
    client.sendCommand = async (code, payload) => {
      assert.ok(code === COMMAND_CODE.PollMessagesDeferred || code === COMMAND_CODE.FetchMessages);
      reads.push({ code, payload });
      const data = code === COMMAND_CODE.PollMessagesDeferred ? pollReply(1, 0n, record, 1)
        : fetchReply([{ partition: 1, status: 0, start: 0n, records: record }]);
      if (code === COMMAND_CODE.FetchMessages) {
        data.writeUInt32LE(1, fetchReplyHeadSize(0) + FETCH_SECTION_FIELD.messagesCount);
        data.writeBigUInt64LE(1n, fetchReplyHeadSize(0) + FETCH_SECTION_FIELD.nextOffset);
      }
      return { status: 0, length: data.length, data };
    };
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer, partitions: [1],
      options: { count: 8, minCount: 3, maxBytes: 512, maxWait: 100, commit: CommitPolicy.OnRead }
    });
    try {
      assert.equal((await reader.next()).messages[0].payload.length, 2_000);
      assert.equal(reads.length, 2);
      const probe = reads[0].payload;
      const probeOptions = probe.subarray(probe.length - DEFERRED_POLL_OPTIONS_SIZE);
      assert.equal(probe.readUInt32LE(probe.length - DEFERRED_POLL_OPTIONS_SIZE - 5), 1);
      assert.equal(probe[probe.length - DEFERRED_POLL_OPTIONS_SIZE - 1], 0);
      assert.equal(probeOptions.readBigUInt64LE(DEFERRED_POLL_OFFSET.wait), 100_000n);
      assert.equal(probeOptions.readUInt32LE(DEFERRED_POLL_OFFSET.maxBytes), 512);
      assert.equal(probeOptions.readUInt32LE(DEFERRED_POLL_OFFSET.minCount), 1);
      const fetch = reads[1].payload.subarray(reads[1].payload.length - FETCH_MESSAGES_FIELDS_SIZE - FETCH_TARGET_SIZE);
      assert.equal(fetch.readBigUInt64LE(FETCH_MESSAGES_FIELD.wait), 100_000n);
      assert.equal(fetch.readUInt32LE(FETCH_MESSAGES_FIELD.maxBytes), 512);
      assert.equal(fetch.readUInt32LE(FETCH_MESSAGES_FIELD.count), 8);
      assert.equal(fetch.readUInt32LE(FETCH_MESSAGES_FIELD.minCount), 3);
    } finally {
      await reader.close();
    }
  });

  for (const limit of [7n, 9n]) {
    it(`recovers a lost first committing reply before any successful delivery with tail ${limit}`, async () => {
      const topic = new FakeTopic(limit);
      topic.stored.set(1, 4n);
      topic.loseNextCommittingRead = true;
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
      try {
        const batch = await reader.next({ signal: AbortSignal.timeout(2_000) });
        assert.deepEqual(offsets([batch]), [5n, 6n]);
      } finally {
        await reader.close();
      }
    });
  }

  for (const start of [0n, 5n]) {
    it(`rewinds a lost first reply on close before any delivery from ${start}`, async () => {
      const topic = new FakeTopic(start + 4n);
      if (start > 0n) topic.stored.set(1, start - 1n);
      topic.loseNextCommittingRead = true;
      const cancellation = new AbortController();
      topic.onLostCommittingRead = () => cancellation.abort(new Error('stop before delivery'));
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
      await assert.rejects(reader.next({ signal: cancellation.signal }), /stop before delivery/);
      await reader.close();
      assert.equal(topic.stored.get(1), start === 0n ? undefined : start - 1n);
    });
  }

  it('reads again by offset without a commit the messages that a lost read stored', async () => {
    const topic = new FakeTopic(8n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    const batches = [await reader.next()];
    topic.stored.set(1, 3n);
    for (let read = 0; read < 3; read += 1) batches.push(await reader.next());
    await reader.close();

    assert.deepEqual(offsets(batches), [0n, 1n, 2n, 3n, 4n, 5n, 6n, 7n]);
    const catchUp = topic.reads.filter(({ code, kind }) =>
      code === COMMAND_CODE.PollMessagesDeferred && kind === PollingStrategyKind.Offset);
    assert.deepEqual(catchUp.map(({ kind, value, autocommit }) => [kind, value, autocommit]),
      [[PollingStrategyKind.Offset, 2n, false], [PollingStrategyKind.Offset, 4n, false]]);
    assert.equal(topic.stored.get(1), 7n);
  });

  it('reports the lost range and hands over the stored batch when reassignment interrupts gap recovery', async () => {
    const topic = new FakeTopic(6n);
    topic.assigned = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
    });
    try {
      assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
      topic.stored.set(1, 3n);
      topic.assignedAfterRead = [];
      await assert.rejects(reader.next({ signal: AbortSignal.timeout(2_000) }), (error: unknown) =>
        error instanceof ReadError && error.partitionId === 1 &&
        (error.error as { errorCode: number }).errorCode === 14010);
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [4n, 5n]);
    } finally {
      await reader.close();
    }
  });

  it('moves an on read offset back to the last batch taken when it closes', async () => {
    const topic = new FakeTopic(2n);
    const reader = await open(topic, [0, 1], { commit: CommitPolicy.OnRead });
    const taken = await reader.next();
    await new Promise((resolve) => setTimeout(resolve, 100));

    await reader.close();

    assert.equal(topic.stored.get(taken.partitionId), 1n);
    assert.equal(topic.stored.has(taken.partitionId === 0 ? 1 : 0), false);
  });

  it('reports a message that does not decode with the continuation that skips it', async () => {
    const topic = new FakeTopic(5n);
    topic.undecodable.add(2n);
    const reader = await open(topic, [1]);
    assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);

    const error = await reader.next().then(() => undefined, (failure: unknown) => failure);
    assert.ok(error instanceof ReadError);
    assert.deepEqual([error.partitionId, error.skip?.kind, error.skip?.value], [1, PollingStrategyKind.Offset, 3n],
      String(error.error));
    assert.equal(error.offset, undefined);
    assert.deepEqual(error.record, records(2n).subarray(0, -1));
    await reader.seek(1, error.skip!);
    const resumed = await reader.next();
    await reader.close();

    assert.deepEqual(offsets([resumed]), [3n, 4n]);
  });

  it('reports a message whose user headers do not decode with its offset and its frame as served', async () => {
    const topic = new FakeTopic(5n);
    topic.unparsableHeaders.add(2n);
    const reader = await open(topic, [1]);
    assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);

    const error = await reader.next().then(() => undefined, (failure: unknown) => failure);
    assert.ok(error instanceof ReadError);
    const frameEnd = BATCH_HEADER_SIZE + FRAME_HEADER_SIZE + 'm2'.length + UNPARSABLE_HEADERS.length;
    assert.deepEqual([error.partitionId, error.offset, error.skip?.kind, error.skip?.value],
      [1, 2n, PollingStrategyKind.Offset, 3n], String(error.error));
    assert.deepEqual(error.record, records(2n, topic.unparsableHeaders).subarray(BATCH_HEADER_SIZE, frameEnd));
    await reader.seek(1, error.skip!);
    const resumed = await reader.next();
    await reader.close();

    assert.deepEqual(offsets([resumed]), [3n, 4n]);
  });

  it('hands over an on read batch of a partition that left the assignment, since the server stored it', async () => {
    const topic = new FakeTopic();
    topic.assigned = [1, 3];
    topic.assignedAfterRead = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
    });
    let first, second;
    try {
      first = await reader.next();
      await once(topic.emitter, 'synced');
      await new Promise(setImmediate);

      second = await reader.next({ signal: AbortSignal.timeout(2_000) });
    } finally {
      await reader.close();
    }

    assert.deepEqual([first.partitionId, second.partitionId].sort(), [1, 3]);
    assert.deepEqual(offsets([second]), [0n, 1n]);
  });

  for (const seek of [false, true]) {
    it(`keeps a stored batch across reassignment unless a seek supersedes it (seek=${seek})`, async () => {
      const topic = new FakeTopic(4n);
      topic.assigned = [1, 3];
      topic.assignedAfterRead = [1];
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
        options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
      });
      try {
        assert.equal((await reader.next()).partitionId, 1);
        while (reader.partitions().includes(3)) await once(topic.emitter, 'synced');
        topic.assigned = [1, 3];
        while (!reader.partitions().includes(3)) await once(topic.emitter, 'synced');
        if (seek) await reader.seek(3, PollingStrategy.Offset(2n));
        let batch = await reader.next({ signal: AbortSignal.timeout(2_000) });
        while (batch.partitionId !== 3) batch = await reader.next({ signal: AbortSignal.timeout(2_000) });
        assert.deepEqual(offsets([batch]), seek ? [2n, 3n] : [0n, 1n]);
      } finally {
        await reader.close();
      }
    });
  }

  it('waits its retry interval after a failed route lookup while the caller asks again at once', async () => {
    const topic = new FakeTopic();
    topic.unrouted = new Error('not connected');
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    const until = Date.now() + 200;
    const errors: unknown[] = [];
    while (Date.now() < until)
      errors.push(await reader.next({ signal: AbortSignal.timeout(50) }).then(() => undefined, (error: unknown) => error));
    await reader.close();

    assert.ok(errors.some((error) => error instanceof ReadError && error.partitionId === undefined));
    assert.ok(topic.routeLookups <= 200 / 20 + 2, `${topic.routeLookups} route lookups in 200 ms`);
  });

  it('puts the partitions that a fetch served at the end of the next fetch, so each one gets its turn', async () => {
    const topic = new FakeTopic(100n);
    topic.countLimited = true;
    const reader = await open(topic, [1, 3, 5], { count: 2 });
    const partitions: number[] = [];
    try {
      for (let read = 0; read < 3; read += 1) partitions.push((await reader.next()).partitionId);
    } finally {
      await reader.close();
    }

    assert.deepEqual(partitions, [1, 3, 5]);
  });

  it('waits its retry interval before it fetches a target again that a reply left out, when reads do not wait', async () => {
    const topic = new FakeTopic();
    topic.sectionless = true;
    const reader = await open(topic, [1], { maxWait: 0 });
    await new Promise((resolve) => setTimeout(resolve, 200));
    await reader.close();

    const fetches = topic.reads.filter(({ code }) => code === COMMAND_CODE.FetchMessages).length;
    assert.ok(fetches > 0 && fetches <= 200 / 20 + 2, `${fetches} fetches in 200 ms`);
  });

  it('stops only the partition whose known history is gone, until a seek', async () => {
    const topic = new FakeTopic(1_000n);
    const reader = await open(topic, [1, 3]);
    await reader.next();
    await reader.next();
    topic.history.add(1);

    let error: unknown;
    while (!error) error = await reader.next().then(() => undefined, (failure: unknown) => failure);
    const others = [await reader.next(), await reader.next()];
    topic.history.clear();
    await reader.seek(1, PollingStrategy.Offset(0n));
    let resumed = await reader.next();
    while (resumed.partitionId !== 1) resumed = await reader.next();
    await reader.close();

    assert.ok(error instanceof ReadError);
    assert.equal(error.partitionId, 1);
    assert.ok(others.every(({ partitionId }) => partitionId === 3));
    assert.equal(resumed.messages[0].headers.offset, 0n);
  });
});

describe('consumer', () => {
  it('counts a message once the iterator is read again, and the last one on shutdown', async () => {
    const topic = new FakeTopic(4n);
    const reader = await open(topic, [1], {
      commit: CommitPolicy.Auto({ every: CommitEvery.Message, after: CommitAfter.Processing })
    });
    const consumer = new IggyConsumer(reader, CommitAfter.Processing);
    const messages = consumer[Symbol.asyncIterator]();

    await messages.next();
    await new Promise((resolve) => setTimeout(resolve, 100));
    const before = topic.stored.get(1);
    await messages.next();
    while (topic.stored.get(1) !== 0n) await new Promise((resolve) => setTimeout(resolve, 5));
    await consumer.shutdown();

    assert.equal(before, undefined);
    assert.equal(topic.stored.get(1), 1n);
  });

  it('ends the handler loop with the error of a handler and leaves its message uncounted', async () => {
    const topic = new FakeTopic(4n);
    const reader = await open(topic, [1], {
      commit: CommitPolicy.Auto({ interval: 3_600_000, after: CommitAfter.Processing })
    });
    const consumer = new IggyConsumer(reader, CommitAfter.Processing);
    const failure = new Error('the handler failed');

    await assert.rejects(consumer.consume(({ message }) => {
      if (message.headers.offset === 1n) throw failure;
    }), (error: unknown) => error === failure);
    await consumer.shutdown();

    assert.equal(topic.stored.get(1), 0n);
  });

  it('stops the handler loop between messages when a handler shuts the consumer down', async () => {
    const topic = new FakeTopic(4n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    const consumer = new IggyConsumer(reader);
    const received: bigint[] = [];
    await consumer.consume(async ({ message }) => {
      received.push(message.headers.offset);
      await consumer.shutdown();
    });

    assert.deepEqual(received, [0n]);
    assert.equal(topic.stored.get(1), 0n);
  });

  it('reports a read error that the reader recovers from and hands over the messages after it', async () => {
    const topic = new FakeTopic();
    topic.unrouted = new Error('not connected');
    const reader = await open(topic, [1]);
    const errors: ReadError[] = [];
    const consumer = new IggyConsumer(reader, undefined, (error) => {
      errors.push(error);
      topic.unrouted = undefined;
    });
    let first;
    try {
      first = await consumer[Symbol.asyncIterator]().next();
    } finally {
      await consumer.shutdown();
    }

    assert.equal(errors[0]?.partitionId, undefined);
    assert.equal(first.value?.message.headers.offset, 0n);
  });

  it('shuts down when its readable is destroyed while a read waits for messages', async () => {
    const topic = new FakeTopic(0n);
    const consumer = new IggyConsumer(await open(topic, [1]));
    const readable = consumer.toReadable();
    readable.resume();
    await new Promise((resolve) => setTimeout(resolve, 50));

    const closed = once(readable, 'close').then(() => 'closed');
    readable.destroy();
    const outcome = await Promise.race([closed, new Promise((resolve) => setTimeout(resolve, 1_000, 'still open'))]);
    if (outcome !== 'closed') await consumer.shutdown();

    assert.equal(outcome, 'closed');
  });
});
