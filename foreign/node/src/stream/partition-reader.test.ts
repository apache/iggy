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
const CONSUMER_GROUP_PARTITION_NOT_OWNED = 5009;
/** A poll body ends with the strategy kind and value, the count and the auto-commit flag. */
const POLL_SUFFIX_SIZE = 1 + 8 + 4 + 1;

type Read = { code: number, partition: number, kind: number, value: bigint, autocommit: boolean, count: number, at: number };

/** User headers whose first key has length zero, so they do not decode. */
const UNPARSABLE_HEADERS = Buffer.from([1, 0, 0, 0, 0]);

/** `count` messages from `offset` as batch records, two by default. */
const records = (offset: bigint, unparsableHeaders = new Set<bigint>(), count = 2): Buffer => {
  const batch = encodeMessagesBatch(Array.from({ length: count }, (_, index) => ({
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
  /** The operations that the reader abandoned. */
  readonly abandoned: bigint[] = [];
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
  /** A poll holds up to its count of the messages from its start, as the server does, instead of two. */
  pollsHoldCount = false;
  /** The owner generation of the member, which a fetch target with a fence must name. */
  ownerGeneration = SERVED.ownerGeneration;
  /** Partitions that the member does not own, whatever a fetch target names. */
  readonly notOwned = new Set<number>();
  /** A read by offset finds nothing, as after its messages were removed. */
  emptyOffsetReads = false;
  /** Every offset store fails with this error. */
  storeFailure?: Error;
  /**
   * Each fetch waits until the test releases it, in the order they arrived. An offset store of a partition that a
   * held committing fetch reads waits for that fetch, as the progress lane of the client makes it.
   */
  holdFetches = false;
  readonly heldFetches: (() => void)[] = [];
  private readonly lanes = new Map<number, Promise<void>>();
  /** The target partitions of each fetch. */
  readonly fetches: number[][] = [];
  /** A fetch section reports the last offset of the topic as the current offset of its partition. */
  reportsHead = false;
  constructor(public limit = 2n) {}

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
      abandonOperation: (operationId: bigint) => { this.abandoned.push(operationId); },
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
      if (this.storeFailure) throw this.storeFailure;
      const partition = payload.readUInt32LE(payload.length - 13);
      await this.lanes.get(partition);
      this.stored.set(partition, payload.readBigUInt64LE(payload.length - 9));
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
        autocommit: body[body.length - 1] !== 0, count: body.readUInt32LE(at + 9), at: Date.now() };
      this.reads.push(read);
      const start = this.start(partition, read.kind, read.value);
      if (start >= this.limit || (this.emptyOffsetReads && read.kind === PollingStrategyKind.Offset))
        return this.idle(options, reply(pollReply(partition, start, Buffer.alloc(0), 0, this.served())));
      const held = this.pollsHoldCount ? Math.min(read.count, Number(this.limit - start)) : 2;
      if (read.autocommit) this.stored.set(partition, start + BigInt(held) - 1n);
      if (read.autocommit && this.loseNextCommittingRead) {
        this.loseNextCommittingRead = false;
        this.onLostCommittingRead?.();
        throw new FetchProgressUncertainError(1n, new Error('reply lost after storing'));
      }
      const batch = records(start, this.unparsableHeaders, held);
      return reply(pollReply(partition, start + BigInt(held) - 1n,
        this.undecodable.has(start) ? batch.subarray(0, batch.length - 1) : batch, held, this.served()));
    }
    if (code === COMMAND_CODE.FetchMessages) {
      let targets = 1;
      while (payload.readUInt32LE(payload.length - FETCH_MESSAGES_FIELDS_SIZE - targets * FETCH_TARGET_SIZE +
          FETCH_MESSAGES_FIELD.targetsCount) !== targets) targets += 1;
      const fieldsAt = payload.length - FETCH_MESSAGES_FIELDS_SIZE - targets * FETCH_TARGET_SIZE;
      const partitions = Array.from({ length: targets }, (_, index) =>
        payload.readUInt32LE(fieldsAt + FETCH_MESSAGES_FIELDS_SIZE + index * FETCH_TARGET_SIZE));
      this.fetches.push(partitions);
      if (this.holdFetches) {
        const released = new Promise<void>((release) => this.heldFetches.push(release));
        if (payload[fieldsAt + FETCH_MESSAGES_FIELD.autoCommit] !== 0)
          for (const partition of partitions) this.lanes.set(partition, released);
        await released;
        for (const partition of partitions) if (this.lanes.get(partition) === released) this.lanes.delete(partition);
      }
      return this.fetch(code, payload, fieldsAt, targets, options);
    }
    throw responseError(code, INVALID_COMMAND, 'not served by the fake topic');
  }

  private async fetch(code: number, payload: Buffer, fieldsAt: number, targets: number,
    options?: SendCommandOptions): Promise<CommandResponse> {
    const reply = (data: Buffer): CommandResponse => ({ status: 0, length: data.length, data });
    const autocommit = payload[fieldsAt + FETCH_MESSAGES_FIELD.autoCommit] !== 0;
    const count = payload.readUInt32LE(fieldsAt + FETCH_MESSAGES_FIELD.count);
    const sections: { partition: number, status: number, start: bigint, records: Buffer }[] = [];
    let held = 0;
    for (let index = 0; index < targets; index += 1) {
      const at = fieldsAt + FETCH_MESSAGES_FIELDS_SIZE + index * FETCH_TARGET_SIZE;
      const partition = payload.readUInt32LE(at + FETCH_TARGET_FIELD.partitionId);
      const read = { code, partition, kind: payload[at + FETCH_TARGET_FIELD.kind],
        value: payload.readBigUInt64LE(at + FETCH_TARGET_FIELD.value), autocommit, count, at: Date.now() };
      this.reads.push(read);
      if (this.history.has(partition)) {
        sections.push({ partition, status: HISTORY_UNAVAILABLE, start: 0n, records: Buffer.alloc(0) });
        continue;
      }
      const fenced = payload.readBigUInt64LE(at + FETCH_TARGET_FIELD.incarnation) !== 0n;
      if (this.notOwned.has(partition) ||
          (fenced && payload.readBigUInt64LE(at + FETCH_TARGET_FIELD.ownerGeneration) !== this.ownerGeneration)) {
        sections.push({ partition, status: CONSUMER_GROUP_PARTITION_NOT_OWNED, start: 0n, records: Buffer.alloc(0) });
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
    const body = fetchReply(this.sectionless ? [] : sections, this.served(), this.reportsHead ? this.limit - 1n : 0n);
    if (autocommit && this.assignedAfterRead) {
      this.assigned = this.assignedAfterRead;
      this.assignedAfterRead = undefined;
    }
    return sections.every(({ records: held, status }) => held.length === 0 && status === 0)
      ? this.idle(options, reply(body)) : reply(body);
  }

  private served() {
    return { ...SERVED, ownerGeneration: this.ownerGeneration };
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

const pollReply = (partition: number, current: bigint, held: Buffer, count: number, context = SERVED): Buffer => {
  const head = Buffer.alloc(40);
  head.writeUInt32LE(partition, 0);
  head.writeBigUInt64LE(current, 4);
  head.writeUInt32LE(count, 12);
  head.writeBigUInt64LE(context.incarnation, 16);
  head.writeBigUInt64LE(context.ownerGeneration, 24);
  head.writeBigUInt64LE(context.metadataOp, 32);
  return Buffer.concat([head, held]);
};

const fetchReply = (sections: { partition: number, status: number, start: bigint, records: Buffer }[],
  context = SERVED, current = 0n): Buffer => {
  const head = Buffer.alloc(fetchReplyHeadSize(sections.length));
  head.writeUInt32LE(sections.length, 0);
  sections.forEach(({ partition, status, start, records: held }, index) => {
    const at = fetchReplyHeadSize(index);
    const count = held.length === 0 ? 0 : 2;
    head.writeUInt32LE(partition, at + FETCH_SECTION_FIELD.partitionId);
    head.writeUInt32LE(status, at + FETCH_SECTION_FIELD.status);
    head.writeUInt32LE(count, at + FETCH_SECTION_FIELD.messagesCount);
    head.writeBigUInt64LE(current, at + FETCH_SECTION_FIELD.currentOffset);
    if (status === 0) {
      head.writeBigUInt64LE(context.incarnation, at + FETCH_SECTION_FIELD.context);
      head.writeBigUInt64LE(context.ownerGeneration, at + FETCH_SECTION_FIELD.context + 8);
      head.writeBigUInt64LE(context.metadataOp, at + FETCH_SECTION_FIELD.context + 16);
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

const waitFor = async (condition: () => boolean, what: string): Promise<void> => {
  const deadline = Date.now() + 1_000;
  while (!condition()) {
    assert.ok(Date.now() < deadline, what);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};

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
    topic.loseNextCommittingRead = true;
    for (let read = 0; read < 3; read += 1) batches.push(await reader.next());
    await reader.close();

    assert.deepEqual(offsets(batches), [0n, 1n, 2n, 3n, 4n, 5n, 6n, 7n]);
    const catchUp = topic.reads.filter(({ code, kind }) =>
      code === COMMAND_CODE.PollMessagesDeferred && kind === PollingStrategyKind.Offset);
    assert.deepEqual(catchUp.map(({ kind, value, autocommit }) => [kind, value, autocommit]),
      [[PollingStrategyKind.Offset, 2n, false], [PollingStrategyKind.Offset, 4n, false]]);
    assert.equal(topic.stored.get(1), 7n);
  });

  it('reads a lost range again only up to the offset the server stored, so no message arrives twice', async () => {
    const topic = new FakeTopic(9n);
    topic.pollsHoldCount = true;
    topic.stored.set(1, 0n);
    topic.loseNextCommittingRead = true;
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    const batches = [];
    try {
      for (let read = 0; read < 3; read += 1) batches.push(await reader.next({ signal: AbortSignal.timeout(2_000) }));
    } finally {
      await reader.close();
    }

    assert.deepEqual(offsets(batches), [1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n]);
    const catchUp = topic.reads.filter(({ code, kind }) =>
      code === COMMAND_CODE.PollMessagesDeferred && kind === PollingStrategyKind.Offset);
    assert.deepEqual(catchUp.map(({ value, autocommit, count }) => [value, autocommit, count]), [[1n, false, 4]]);
  });

  it('delivers from where another session stored, without reading the skipped messages again', async () => {
    const topic = new FakeTopic(8n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    const batches = [await reader.next()];
    topic.stored.set(1, 3n);
    try {
      while (offsets(batches).at(-1)! < 7n) batches.push(await reader.next({ signal: AbortSignal.timeout(2_000) }));
    } finally {
      await reader.close();
    }

    assert.deepEqual(offsets(batches), [0n, 1n, 4n, 5n, 6n, 7n]);
    assert.ok(!topic.reads.some(({ kind }) => kind === PollingStrategyKind.Offset));
  });

  it('accepts a later jump once a read after the recovery of a lost read passed the gap check', async () => {
    const topic = new FakeTopic(12n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    const batches = [await reader.next()];
    topic.loseNextCommittingRead = true;
    try {
      for (let read = 0; read < 3; read += 1) batches.push(await reader.next({ signal: AbortSignal.timeout(2_000) }));
      topic.stored.set(1, 9n);
      while (offsets(batches).at(-1)! < 11n) batches.push(await reader.next({ signal: AbortSignal.timeout(2_000) }));
    } finally {
      await reader.close();
    }

    assert.deepEqual(offsets(batches), [0n, 1n, 2n, 3n, 4n, 5n, 6n, 7n, 10n, 11n]);
  });

  it('waits its retry interval after it abandons a lost committing read, before it reads that partition again', async () => {
    const topic = new FakeTopic(8n);
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer, partitions: [1], options: { maxWait: 100, commit: CommitPolicy.OnRead },
      retryInterval: 300
    });
    const first = await reader.next();
    topic.loseNextCommittingRead = true;
    let lostAt = 0;
    topic.onLostCommittingRead = () => { lostAt = Date.now(); };
    try {
      await reader.next({ signal: AbortSignal.timeout(2_000) });
    } finally {
      await reader.close();
    }

    assert.deepEqual([offsets([first]), topic.abandoned], [[0n, 1n], [1n]]);
    const after = topic.reads.find(({ at }) => at > lostAt)!;
    assert.ok(after.at - lostAt >= 250, `read again ${after.at - lostAt} ms after the lost read`);
  });

  it('abandons a lost committing poll of its partition as it abandons a lost fetch', async () => {
    const topic = new FakeTopic(4n);
    topic.loseNextCommittingRead = true;
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead }).then((opened) => opened);
    await reader.seek(1, PollingStrategy.Timestamp(0n));
    try {
      await reader.next({ signal: AbortSignal.timeout(2_000) });
    } finally {
      await reader.close();
    }

    const lost = topic.reads.find(({ autocommit }) => autocommit)!;
    assert.deepEqual([lost.code, lost.kind, topic.abandoned], [COMMAND_CODE.PollMessagesDeferred,
      PollingStrategyKind.Timestamp, [1n]]);
  });

  it('ends a catch-up whose read by offset finds nothing, reports the lost range and reads on', async () => {
    const topic = new FakeTopic(8n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    const first = await reader.next();
    topic.loseNextCommittingRead = true;
    topic.emptyOffsetReads = true;
    try {
      await assert.rejects(reader.next({ signal: AbortSignal.timeout(2_000) }), (error: unknown) =>
        error instanceof ReadError && error.partitionId === 1 &&
        (error.error as { errorCode: number }).errorCode === 14010);
      const held = await reader.next({ signal: AbortSignal.timeout(2_000) });
      const after = await reader.next({ signal: AbortSignal.timeout(2_000) });
      assert.deepEqual([offsets([first]), offsets([held]), offsets([after])], [[0n, 1n], [4n, 5n], [6n, 7n]]);
    } finally {
      await reader.close();
    }

    assert.equal(topic.reads.filter(({ kind }) => kind === PollingStrategyKind.Offset).length, 1);
  });

  it('keeps the read of a partition assigned again apart from a late read of its earlier assignment', async () => {
    const topic = new FakeTopic(2n);
    topic.assigned = [1];
    topic.holdFetches = true;
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 }, retryInterval: 20
    });
    const wakeUntil = async (condition: () => boolean) => {
      const deadline = Date.now() + 2_000;
      while (!condition()) {
        assert.ok(Date.now() < deadline, 'the reader did not get there in time');
        await reader.next({ signal: AbortSignal.timeout(10) }).then(() => assert.fail('a batch arrived'), () => undefined);
      }
    };
    try {
      await wakeUntil(() => topic.heldFetches.length === 1);
      topic.assigned = [];
      await wakeUntil(() => !reader.partitions().includes(1));
      topic.assigned = [1];
      await wakeUntil(() => topic.heldFetches.length === 2);
      topic.heldFetches[0]();
      await reader.next({ signal: AbortSignal.timeout(200) }).then(() => assert.fail('a batch arrived'), () => undefined);
      assert.equal(topic.heldFetches.length, 2, 'a second read of the new assignment started');
    } finally {
      topic.holdFetches = false;
      for (const release of topic.heldFetches) release();
      await reader.close();
    }
  });

  for (const closing of ['reader', 'consumer'] as const) {
    it(`makes a second ${closing} close wait for the first and fail as it does`, async () => {
      const topic = new FakeTopic(4n);
      const reader = await open(topic, [1], {
        commit: CommitPolicy.Auto({ interval: 3_600_000, after: CommitAfter.Delivery })
      });
      const consumer = closing === 'consumer' ? new IggyConsumer(reader, CommitAfter.Delivery) : undefined;
      if (consumer) await consumer[Symbol.asyncIterator]().next();
      else await reader.next();
      topic.storeFailure = responseError(COMMAND_CODE.StoreOffset, 41, 'unauthorized');
      const close = () => consumer ? consumer.shutdown() : reader.close();
      const first = close();
      const second = close();
      const isFailure = (error: unknown) => error === topic.storeFailure;
      await assert.rejects(second, isFailure);
      await assert.rejects(first, isFailure);
    });
  }

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
      topic.loseNextCommittingRead = true;
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

  it('stores the offset before an on read seek to an offset, so a restart resumes at the sought offset', async () => {
    const topic = new FakeTopic(10n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    try {
      assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
      await reader.seek(1, PollingStrategy.Offset(6n));
      assert.equal(topic.stored.get(1), 5n);
      assert.deepEqual(offsets([await reader.next()]), [6n, 7n]);
    } finally {
      await reader.close();
    }
    assert.equal(topic.stored.get(1), 7n);
  });

  for (const [name, start, commit] of [
    ['offset 0', PollingStrategy.Offset(0n), CommitPolicy.OnRead],
    ['first', PollingStrategy.First, CommitPolicy.OnRead],
    ['next', PollingStrategy.Next, CommitPolicy.OnRead],
    ['last', PollingStrategy.Last, CommitPolicy.OnRead],
    ['timestamp', PollingStrategy.Timestamp(0n), CommitPolicy.OnRead],
    ['an offset under a manual policy', PollingStrategy.Offset(6n), CommitPolicy.Manual]
  ] as const) {
    it(`stores nothing on a seek to ${name}`, async () => {
      const topic = new FakeTopic(10n);
      const reader = await open(topic, [1], { commit });
      try {
        assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
        await reader.seek(1, start);
        assert.equal(topic.stored.get(1), commit.kind === 'onRead' ? 1n : undefined);
      } finally {
        await reader.close();
      }
    });
  }

  it('fails an on read seek whose store is refused, and moves nothing', async () => {
    const topic = new FakeTopic(10n);
    const reader = await open(topic, [1, 3], { commit: CommitPolicy.OnRead });
    try {
      assert.deepEqual([(await reader.next()).partitionId, topic.stored.get(3)], [1, 1n]);
      topic.storeFailure = new Error('store refused');
      await assert.rejects(reader.seek(3, PollingStrategy.Offset(6n)), { message: 'store refused' });
      topic.storeFailure = undefined;
      assert.equal(topic.stored.get(3), 1n);
      const queued = await reader.next();
      assert.deepEqual([queued.partitionId, offsets([queued])], [3, [0n, 1n]]);
      let resumed = await reader.next();
      while (resumed.partitionId !== 3) resumed = await reader.next();
      assert.deepEqual(offsets([resumed]), [2n, 3n]);
    } finally {
      await reader.close();
    }
  });

  it('stores an on read seek after the committing read in flight, and drops that read', async () => {
    const topic = new FakeTopic(10n);
    topic.holdFetches = true;
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    try {
      const waiting = new AbortController();
      const pending = reader.next({ signal: waiting.signal }).then(() => undefined, (failure: unknown) => failure);
      await waitFor(() => topic.heldFetches.length === 1, 'the committing read did not start');
      let sought = false;
      const seek = reader.seek(1, PollingStrategy.Offset(6n)).then(() => { sought = true; });
      await new Promise((resolve) => setTimeout(resolve, 50));
      assert.deepEqual([topic.stored.has(1), sought], [false, false]);
      waiting.abort();
      assert.ok((await pending) instanceof Error);
      topic.holdFetches = false;
      topic.heldFetches.shift()!();
      await seek;
      assert.deepEqual(offsets([await reader.next()]), [6n, 7n]);
      const resumed = topic.reads.filter(({ code }) => code === COMMAND_CODE.FetchMessages).at(-1)!;
      assert.deepEqual([topic.fetches.length, resumed.kind, resumed.value], [2, PollingStrategyKind.Offset, 6n]);
    } finally {
      topic.holdFetches = false;
      for (const release of topic.heldFetches) release();
      await reader.close();
    }
    assert.equal(topic.stored.get(1), 7n);
  });

  it('applies an on read seek that waits for a read when the reader closes meanwhile, and the close keeps its offset',
    async () => {
      const topic = new FakeTopic(10n);
      topic.holdFetches = true;
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
      const pending = reader.next().then(() => undefined, (failure: unknown) => failure);
      await waitFor(() => topic.heldFetches.length === 1, 'the committing read did not start');
      const seek = reader.seek(1, PollingStrategy.Offset(6n));
      await new Promise((resolve) => setTimeout(resolve, 50));
      const closing = reader.close();
      await new Promise(setImmediate);
      topic.holdFetches = false;
      topic.heldFetches.shift()!();
      await seek;
      await closing;

      assert.equal(topic.stored.get(1), 5n);
      assert.ok((await pending) instanceof Error);
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

  for (const commit of [CommitPolicy.Manual, CommitPolicy.OnRead]) {
    it(`reads on from the stored offset under a new owner generation after a rejoin (${commit.kind})`, async () => {
      const topic = new FakeTopic(2n);
      topic.assigned = [1];
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
        options: { maxWait: 100, commit }, retryInterval: 20
      });
      try {
        const first = await reader.next({ signal: AbortSignal.timeout(2_000) });
        if (commit.kind === 'manual') await reader.commit(first.checkpoint()!);
        topic.ownerGeneration += 1n;
        topic.limit = 4n;
        const resumed = await reader.next({ signal: AbortSignal.timeout(2_000) });
        assert.deepEqual([offsets([first]), offsets([resumed])], [[0n, 1n], [2n, 3n]]);
        assert.equal(resumed.context.ownerGeneration, topic.ownerGeneration);
      } finally {
        await reader.close();
      }
    });
  }

  it('reports a partition that the server keeps refusing as not owned while the assignment still names it', async () => {
    const topic = new FakeTopic();
    topic.assigned = [1];
    topic.notOwned.add(1);
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 }, retryInterval: 5
    });
    try {
      await assert.rejects(reader.next({ signal: AbortSignal.timeout(2_000) }), (error: unknown) =>
        error instanceof ReadError && error.partitionId === 1 &&
        (error.error as { errorCode: number }).errorCode === CONSUMER_GROUP_PARTITION_NOT_OWNED);
    } finally {
      await reader.close();
    }
  });

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

  for (const [prefetchBytes, spread] of [
    [undefined, [[1], [3], [5], [7], [9]]],
    [3 * 1_048_576, [[1, 5, 9], [3, 7]]]
  ] as const) {
    it(`spreads the backlogged partitions of a primary over the free read slots (${spread.length} slots)`, async () => {
      const topic = new FakeTopic(100n);
      topic.reportsHead = true;
      topic.holdFetches = true;
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer, partitions: [1, 3, 5, 7, 9], options: { maxWait: 100 }, retryInterval: 20,
        ...(prefetchBytes === undefined ? {} : { prefetchBytes })
      });
      try {
        await waitFor(() => topic.fetches.length === 1, 'the first fetch did not start');
        topic.heldFetches[0]();
        // The fetches of one turn start together, so the first one of the next turn shows them all.
        await waitFor(() => topic.fetches.length > 1, 'no fetch followed the backlog');
        assert.deepEqual(topic.fetches, [[1, 3, 5, 7, 9], ...spread]);
      } finally {
        topic.holdFetches = false;
        for (const release of topic.heldFetches) release();
        await reader.close();
      }
    });
  }

  for (const [prefetchBytes, spread] of [
    [undefined, [[1], [3], [5], [7], [9]]],
    [3 * 1_048_576, [[1, 7], [3, 9], [5]]]
  ] as const) {
    it(`spreads the backlogged partitions of a primary over the free slots of a committing reader (${spread.length} slots)`,
      async () => {
        const topic = new FakeTopic(100n);
        topic.reportsHead = true;
        topic.holdFetches = true;
        const client = topic.client();
        const reader = await PartitionReader.open(async () => client, {
          streamId: 1, topicId: 2, consumer, partitions: [1, 3, 5, 7, 9], retryInterval: 20,
          options: { maxWait: 100, commit: CommitPolicy.OnRead }, ...(prefetchBytes === undefined ? {} : { prefetchBytes })
        });
        try {
          const first = reader.next();
          await waitFor(() => topic.heldFetches.length === 1, 'the first fetch did not start');
          topic.heldFetches.shift()!();
          const batches = [await first];
          for (let read = 1; read < 5; read += 1) batches.push(await reader.next());
          assert.deepEqual(batches.map(({ partitionId }) => partitionId), [1, 3, 5, 7, 9]);
          const sixth = reader.next();
          await waitFor(() => topic.fetches.length > 1, 'no fetch followed the backlog');
          await new Promise(setImmediate);
          assert.deepEqual(topic.fetches, [[1, 3, 5, 7, 9], ...spread]);
          assert.ok(topic.reads.filter(({ code }) => code === COMMAND_CODE.FetchMessages).every(({ autocommit }) => autocommit));
          topic.holdFetches = false;
          for (const release of topic.heldFetches.splice(0)) release();
          assert.deepEqual(offsets([await sixth]), [2n, 3n]);
        } finally {
          topic.holdFetches = false;
          for (const release of topic.heldFetches) release();
          await reader.close();
        }
      });
  }

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

  it('drops the rest of the batch of a partition that the consumer seeks', async () => {
    const topic = new FakeTopic(6n);
    const consumer = new IggyConsumer(await open(topic, [1]));
    const messages = consumer[Symbol.asyncIterator]();
    try {
      const first = await messages.next();
      await consumer.seek(1, PollingStrategy.Offset(4n));
      const second = await messages.next();
      assert.deepEqual([first.value?.message.headers.offset, second.value?.message.headers.offset], [0n, 4n]);
    } finally {
      await consumer.shutdown();
    }
  });

  it('does not count the message of a handler that still runs when the consumer shuts down', async () => {
    const topic = new FakeTopic(4n);
    const reader = await open(topic, [1], {
      commit: CommitPolicy.Auto({ interval: 3_600_000, after: CommitAfter.Processing })
    });
    const consumer = new IggyConsumer(reader, CommitAfter.Processing);
    let entered!: () => void;
    const handling = new Promise<void>((resolve) => { entered = resolve; });
    let release!: () => void;
    const released = new Promise<void>((resolve) => { release = resolve; });
    const failure = new Error('the handler failed');
    const consuming = consumer.consume(async ({ message }) => {
      if (message.headers.offset !== 1n) return;
      entered();
      await released;
      throw failure;
    });
    await handling;
    await consumer.shutdown();
    release();
    await assert.rejects(consuming, (error: unknown) => error === failure);

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
