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
import { createHook } from 'node:async_hooks';
import { EventEmitter, getEventListeners, once } from 'node:events';
import { Readable } from 'node:stream';
import { describe, it } from 'node:test';
import { debug } from '../client/client.debug.js';
import { FetchProgressUncertainError, LaneBlockedError, OperationUncertainError } from '../client/client.operation.js';
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
import { Checkpoint, continuation, type PolledBatch } from '../wire/message/checkpoint.js';
import { EMPTY_PARTITION_CONTEXT, type PartitionContext } from '../wire/vsr/header.js';
import { PartitionReader, ReadError, type PartitionReaderRequest } from './partition-reader.js';
import { IggyConsumer, openConsumerGroup } from './iggy-consumer.js';

const SERVED = { incarnation: 11n, ownerGeneration: 4n, metadataOp: 42n };
/** `SERVED` with owner generation 0, which the server binds to the owner that it installed. */
const INSTALLED_OWNER = { ...SERVED, ownerGeneration: 0n };
/** What a read serves after a rebalance by another member installed the owner again at the next metadata op. */
const REINSTALLED = { ...SERVED, ownerGeneration: SERVED.ownerGeneration + 1n, metadataOp: SERVED.metadataOp + 1n };
const BATCH_BASE_OFFSET_AT = 8;
const HISTORY_UNAVAILABLE = 87;
const FETCH_PROGRESS_UNCERTAIN = 14010;
const INVALID_COMMAND = 3;
const UNAUTHORIZED = 41;
const INVALID_SIZE_BYTES = 80;
const TRANSIENT_NOT_ACCEPTED = 58;
const NOT_PRIMARY = 89;
const DEFERRED_POLL_CAPACITY_EXCEEDED = 90;
const CONSUMER_OFFSET_NOT_FOUND = 3021;
const INVALID_OFFSET = 4100;
const TIERED_READ_FAILED = 96;
const CONSUMER_GROUP_MEMBER_NOT_FOUND = 5006;
const CONSUMER_GROUP_PARTITION_NOT_OWNED = 5009;
/** An offset reply holds the partition, its current offset and the stored offset. */
const OFFSET_REPLY_SIZE = 4 + 8 + 8;
const OFFSET_REPLY_STORED_AT = 12;
const PARKED_READ_WAIT_MS = 60_000;
/** A reader reads one message without a commit to find the installed owner of a partition. */
const PROBE_COUNT = 1;
/** After this many lasting refusals in a row a reader reports its partition, and stops it unless the refusal is 5009. */
const MAX_LASTING_REFUSALS = 6;
/** A poll reply cut after its partition id, before its context. */
const TRUNCATED_POLL_REPLY_SIZE = 4;
/** The operation of an offset store with an unknown outcome, apart from the lost reads, which are operation 1. */
const UNCERTAIN_WRITE_OPERATION = 2n;
/** The operation of a group create, join or leave with an unknown outcome. */
const UNCERTAIN_GROUP_OPERATION = 3n;
/** The most reads of one reader in flight, fetches and polls alike. */
const MAX_READS_IN_FLIGHT = 32;
/** The longest pause of a group reader between two syncs. */
const GROUP_REFRESH_MS = 1_000;
/** A group reader whose partitions stay the same syncs once per refresh interval from this time after its join. */
const SETTLED_AFTER_MS = 2_000;
/** The end of the window in which the tests count the syncs of a settled group reader, two refresh intervals on. */
const SETTLED_UNTIL_MS = SETTLED_AFTER_MS + 2 * GROUP_REFRESH_MS;
/** How soon a member reads a partition that the first sync after its join missed. */
const SETTLED_READ_MS = GROUP_REFRESH_MS / 2;
/** How much sooner than its pause a timer may seem to fire. */
const TIMER_SLACK_MS = 50;
/** The retry interval of a reader that gets an unserved reply, long enough to tell its pause from timer slack. */
const UNSERVED_RETRY_MS = 200;
/** The retry interval of a member whose rejoin loses its reply, so a write of the test comes before the next join. */
const REJOIN_RETRY_MS = 200;
/** A close reads for the installed owner of a refused write up to this long after it starts. */
const CLOSE_BUDGET_MS = 10_000;
/** How long `waitFor` waits for its condition by default. */
const WAIT_MS = 1_000;
/** The pause between two next() calls of a busy member, so the test does not spin the CPU. */
const NEXT_PAUSE_MS = 5;
/** How soon a trigger that syncs at once leads to the sync, a fraction of the refresh interval. */
const AT_ONCE_MS = 200;
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
 * read, with primary `partition % primaries`. It looks up, stores and deletes
 * offsets as the server does: the commit of a read never moves an offset back,
 * a store past the last message fails with 4100, and a delete of a missing
 * offset fails with 3021. The message at offset `n`
 * carries timestamp `n`. A Last poll starts at the last `count` messages, as
 * the server does. It answers a read that finds nothing with an empty
 * reply. It refuses a fetch with a target that starts anywhere but at an
 * offset or Next with 3, as the server does. A fetch goes to
 * the primary of its first target, which refuses each section of a partition
 * that another primary leads.
 */
class FakeTopic {
  primaries = 2;
  /** The primary of each partition that moved away from `partition % primaries`. */
  readonly movedPrimaries = new Map<number, number>();
  readonly reads: Read[] = [];
  readonly stored = new Map<number, bigint>();
  /** The partition of each offset lookup, store and delete. */
  readonly offsetLookups: number[] = [];
  readonly offsetStores: number[] = [];
  readonly offsetDeletes: number[] = [];
  readonly undecodable = new Set<bigint>();
  /** The offsets whose user headers do not decode. */
  readonly unparsableHeaders = new Set<bigint>();
  readonly history = new Set<number>();
  readonly emitter = new EventEmitter();
  /** The operations that the reader abandoned. */
  readonly abandoned: bigint[] = [];
  /**
   * The partitions of the group member, and the assignment it moves to once a committing read is served. A group
   * read of any other partition is refused with 5009, as the server fences group reads by ownership.
   */
  assigned: number[] = [];
  assignedAfterRead?: number[];
  /**
   * Set while the server has forgotten the member, until it joins again: the refusal of each group read, 5009 as the
   * server answers, or 5006. A sync gets an empty body meanwhile, as the server answers.
   */
  forgottenRefusal?: number;
  /** The server forgets the member again at each join. */
  forgetsJoins = false;
  /** When the member joined. */
  readonly joins: number[] = [];
  /** How many times the member left. */
  leaves = 0;
  /**
   * The next group write with this command code lands and then fails with an unknown outcome. The client keeps that
   * operation, so each later group write fails with 14007 until the reader abandons it.
   */
  uncertainGroupWrite?: number;
  /** The group write with an unknown outcome that blocks the metadata lane of the client. */
  private metadataBlocker?: bigint;
  /** Fails every route lookup, as a client without a link does. */
  unrouted?: Error;
  /** Each route lookup waits until the reader aborts it, as a lookup that its primary does not answer does. */
  routeLookupsHang = false;
  routeLookups = 0;
  loseNextCommittingRead = false;
  onLostCommittingRead?: () => void;
  /** Runs once, after the next poll that does not commit passed the checks of the server and before its reply. */
  beforeUncommittedPoll?: () => Promise<void> | void;
  /**
   * The next read of one message without a commit, as a reader sends to find the installed owner of a partition,
   * fails as a whole request with this status, as a server at its poll quota refuses a fetch of one partition.
   */
  probeRefusal?: number;
  /** Runs once, when the server refuses the next offset write as not owned. */
  onNotOwnedWrite?: () => void;
  /**
   * The context of each offset store and delete as the reader sent it. Undefined takes the context of the route, and
   * history 0 takes the history of the route, as the client fills it in.
   */
  readonly writeContexts: (PartitionContext | undefined)[] = [];
  /** The offsets whose poll reply ends before its context, so no part of it decodes. */
  readonly truncatedReplies = new Set<bigint>();
  /** A fetch reads its targets in order until it holds its count, as the server does. */
  countLimited = false;
  /** A fetch replies without sections. */
  sectionless = false;
  /** A poll holds up to its count of the messages from its start, as the server does, instead of two. */
  pollsHoldCount = false;
  /** The owner generation of the member, which a fetch target with a fence must name. */
  ownerGeneration = SERVED.ownerGeneration;
  /** The last metadata op that the client saw, which a request without a context names. */
  metadataOp = SERVED.metadataOp;
  /** The metadata op that installed the owner. A write with owner 0 binds to the owner only from that op on. */
  ownerInstalledAt = SERVED.metadataOp;
  /** The incarnation of the partition history that a read serves. */
  incarnation = SERVED.incarnation;
  /** Partitions that the member does not own while `assigned` still names them, whatever a read names. */
  readonly notOwned = new Set<number>();
  /** The status that refuses each fetch section of a partition. */
  readonly refused = new Map<number, number>();
  /** A read by offset finds nothing, as after its messages were removed. */
  emptyOffsetReads = false;
  /**
   * The first offset that the partition still holds, as after retention. A read from below it starts at it, as the
   * server starts a read below its first segment at that segment.
   */
  firstKept = 0n;
  /**
   * Each next poll gets an empty reply under the next of these contexts, before the server reads anything. The server
   * answers a poll that it could not serve with a zero context.
   */
  readonly unservedPolls: PartitionContext[] = [];
  readonly emptyPartitions = new Set<number>();
  idleWait = 50;
  /** Every offset store fails with this error. */
  storeFailure?: Error;
  /** Every offset lookup fails with this error. */
  lookupFailure?: Error;
  /** Every offset delete fails with this error. */
  deleteFailure?: Error;
  /**
   * The next offset store or delete lands or not, as set, and then fails with an unknown outcome. The client keeps
   * that operation, so each later offset write of its partition fails with 14007 until the reader abandons it.
   */
  uncertainWriteLands?: boolean;
  /** The operation with an unknown outcome that blocks the offset writes of each partition. */
  private readonly uncertainWrites = new Map<number, bigint>();
  /**
   * Each fetch waits until the test releases it, in the order they arrived. An offset lookup, store, delete or
   * committing read of a partition that a held committing fetch reads waits for that fetch, as the progress lane of
   * the client makes it.
   */
  holdFetches = false;
  readonly heldFetches: (() => void)[] = [];
  /**
   * Each read for the installed owner waits after the server answered it, until the test releases it from
   * `heldFetches`. An abort does not end the wait, as for a reply that the client took just before the abort.
   */
  holdProbes = false;
  /**
   * Each offset store and delete waits until the test releases it, in the order they arrived. A committing read or
   * an offset lookup of its partition waits for it, as the progress lane of the client makes it.
   */
  holdOffsetWrites = false;
  readonly heldOffsetWrites: (() => void)[] = [];
  private readonly lanes = new Map<number, Promise<void>>();
  /** The target partitions of each fetch. */
  readonly fetches: number[][] = [];
  /** A fetch section reports the last offset of the topic as the current offset of its partition. */
  reportsHead = false;
  /** The most commands that the fake answered at once. */
  mostInFlight = 0;
  private inFlight = 0;
  /** Each sync names a newer group generation, as hand-overs between other members make it. */
  private groupGeneration = 0n;
  constructor(public limit = 2n) {}

  /** A new session of the member. The server installs it as the owner again at the op of its Register. */
  changeSession(): void {
    this.ownerGeneration += 1n;
    this.metadataOp += 1n;
    this.ownerInstalledAt = this.metadataOp;
    this.emitter.emit('sessionReset');
  }

  client(): RawClient {
    return {
      protocol: 'vsr',
      isAuthenticated: true,
      sendCommand: async (code: number, payload: Buffer, options?: SendCommandOptions) => {
        // As in the client, a request whose signal aborted before it went out never reaches the server.
        options?.signal?.throwIfAborted();
        this.mostInFlight = Math.max(this.mostInFlight, ++this.inFlight);
        try {
          return await this.answer(code, payload, options);
        } finally {
          this.inFlight -= 1;
        }
      },
      readinessGroups: async (_streamId: unknown, _topicId: unknown, partitions: number[],
        options?: SendCommandOptions) => {
        this.routeLookups += 1;
        if (this.unrouted) throw this.unrouted;
        if (this.routeLookupsHang)
          await new Promise((_, reject) => options?.signal?.addEventListener('abort', () => reject(options.signal!.reason)));
        return Array.from({ length: this.primaries },
          (_, primary) => partitions.filter((partition) => this.primaryOf(partition) === primary))
          .filter((group) => group.length > 0);
      },
      authenticate: async () => true,
      abandonOperation: (operationId: bigint) => {
        this.abandoned.push(operationId);
        for (const [partition, blocking] of this.uncertainWrites)
          if (blocking === operationId) this.uncertainWrites.delete(partition);
        if (this.metadataBlocker === operationId) this.metadataBlocker = undefined;
      },
      destroy: () => {},
      on: this.emitter.on.bind(this.emitter),
      once: this.emitter.once.bind(this.emitter),
      off: this.emitter.off.bind(this.emitter),
      getReadStream: () => Readable.from([])
    } as unknown as RawClient;
  }

  private start(partition: number, kind: number, value: bigint, count: number): bigint {
    const start = kind === PollingStrategyKind.Next ? (this.stored.has(partition) ? this.stored.get(partition)! + 1n : 0n)
      : kind === PollingStrategyKind.Offset || kind === PollingStrategyKind.Timestamp ? value
        : kind === PollingStrategyKind.Last && this.limit > BigInt(count) ? this.limit - BigInt(count) : 0n;
    return start < this.firstKept ? this.firstKept : start;
  }

  private async answer(code: number, payload: Buffer, options?: SendCommandOptions): Promise<CommandResponse> {
    const reply = (data: Buffer): CommandResponse => ({ status: 0, length: data.length, data });
    if (code === COMMAND_CODE.StoreOffset) {
      const partition = payload.readUInt32LE(payload.length - 13);
      this.offsetStores.push(partition);
      this.writeContexts.push(options?.context);
      await this.offsetWrite(partition);
      if (this.storeFailure) throw this.storeFailure;
      this.fenceHistory(code, options);
      this.fenceOwner(code, payload, options);
      const offset = payload.readBigUInt64LE(payload.length - 9);
      if (offset >= this.limit) throw responseError(code, INVALID_OFFSET);
      this.write(partition, () => this.stored.set(partition, offset));
      return reply(Buffer.alloc(0));
    }
    if (code === COMMAND_CODE.GetOffset) {
      const partition = payload.readUInt32LE(payload.length - 4);
      this.offsetLookups.push(partition);
      if (this.lookupFailure) throw this.lookupFailure;
      this.fenceHistory(code, options);
      await this.lanes.get(partition);
      const stored = this.stored.get(partition);
      if (stored === undefined) return reply(Buffer.alloc(0));
      const offset = Buffer.alloc(OFFSET_REPLY_SIZE);
      offset.writeUInt32LE(partition, 0);
      offset.writeBigUInt64LE(stored, OFFSET_REPLY_STORED_AT);
      return reply(offset);
    }
    if (code === COMMAND_CODE.DeleteConsumerOffset) {
      const partition = payload.readUInt32LE(payload.length - 5);
      this.offsetDeletes.push(partition);
      this.writeContexts.push(options?.context);
      await this.offsetWrite(partition);
      if (this.deleteFailure) throw this.deleteFailure;
      this.fenceHistory(code, options);
      this.fenceOwner(code, payload, options);
      this.write(partition, () => {
        if (!this.stored.delete(partition)) throw responseError(code, CONSUMER_OFFSET_NOT_FOUND);
      });
      return reply(Buffer.alloc(0));
    }
    if (code === COMMAND_CODE.JoinGroup) {
      this.groupWrite(code, () => {
        this.joins.push(Date.now());
        if (!this.forgetsJoins) this.forgottenRefusal = undefined;
      });
      return reply(Buffer.alloc(0));
    }
    if (code === COMMAND_CODE.LeaveGroup || code === COMMAND_CODE.CreateGroup) {
      this.groupWrite(code, code === COMMAND_CODE.LeaveGroup ? () => { this.leaves += 1; } : undefined);
      return reply(Buffer.alloc(0));
    }
    if (code === COMMAND_CODE.GetGroup) return reply(Buffer.alloc(0));
    if (code === COMMAND_CODE.SyncGroup) {
      setImmediate(() => this.emitter.emit('synced'));
      if (this.forgottenRefusal !== undefined) return reply(Buffer.alloc(0));
      const assignment = Buffer.alloc(12 + 4 * this.assigned.length);
      assignment.writeBigUInt64LE(++this.groupGeneration, 0);
      assignment.writeUInt32LE(this.assigned.length, 8);
      this.assigned.forEach((partition, index) => assignment.writeUInt32LE(partition, 12 + 4 * index));
      return reply(assignment);
    }
    if (code === COMMAND_CODE.PollMessagesDeferred) {
      const body = payload.subarray(0, payload.length - DEFERRED_POLL_OPTIONS_SIZE);
      const at = body.length - POLL_SUFFIX_SIZE;
      const partition = body.readUInt32LE(at - 4);
      const read = { code, partition, kind: body[at], value: body.readBigUInt64LE(at + 1),
        autocommit: body[body.length - 1] !== 0, count: body.readUInt32LE(at + 9), at: Date.now() };
      this.reads.push(read);
      if (read.autocommit) await this.lanes.get(partition);
      const refusal = payload[0] === ConsumerKind.Group ? this.groupRefusal(partition) : undefined;
      if (refusal !== undefined) throw responseError(code, refusal);
      this.refuseProbe(code, read.autocommit, read.count);
      const unserved = this.unservedPolls.shift();
      if (unserved) return reply(pollReply(partition, 0n, Buffer.alloc(0), 0, unserved));
      if (!read.autocommit) {
        const change = this.beforeUncommittedPoll;
        this.beforeUncommittedPoll = undefined;
        await change?.();
      }
      const start = this.start(partition, read.kind, read.value, read.count);
      if (start >= this.limit || this.emptyPartitions.has(partition) ||
          (this.emptyOffsetReads && read.kind === PollingStrategyKind.Offset))
        return this.idle(options, reply(pollReply(partition, start, Buffer.alloc(0), 0, this.served())));
      const held = this.pollsHoldCount ? Math.min(read.count, Number(this.limit - start)) : 2;
      if (read.autocommit) this.autoCommit(partition, start + BigInt(held) - 1n);
      if (read.autocommit && this.loseNextCommittingRead) {
        this.loseNextCommittingRead = false;
        this.onLostCommittingRead?.();
        throw new FetchProgressUncertainError(1n, new Error('reply lost after storing'));
      }
      const batch = records(start, this.unparsableHeaders, held);
      const data = pollReply(partition, start + BigInt(held) - 1n,
        this.undecodable.has(start) ? batch.subarray(0, batch.length - 1) : batch, held, this.served());
      return reply(this.truncatedReplies.has(start) ? data.subarray(0, TRUNCATED_POLL_REPLY_SIZE) : data);
    }
    if (code === COMMAND_CODE.FetchMessages) {
      let targets = 1;
      while (payload.readUInt32LE(payload.length - FETCH_MESSAGES_FIELDS_SIZE - targets * FETCH_TARGET_SIZE +
          FETCH_MESSAGES_FIELD.targetsCount) !== targets) targets += 1;
      const fieldsAt = payload.length - FETCH_MESSAGES_FIELDS_SIZE - targets * FETCH_TARGET_SIZE;
      const partitions = Array.from({ length: targets }, (_, index) =>
        payload.readUInt32LE(fieldsAt + FETCH_MESSAGES_FIELDS_SIZE + index * FETCH_TARGET_SIZE));
      const kinds = Array.from({ length: targets }, (_, index) =>
        payload[fieldsAt + FETCH_MESSAGES_FIELDS_SIZE + index * FETCH_TARGET_SIZE + FETCH_TARGET_FIELD.kind]);
      if (kinds.some((kind) => kind !== PollingStrategyKind.Offset && kind !== PollingStrategyKind.Next))
        throw responseError(code, INVALID_COMMAND, 'a fetch target starts at an offset or Next');
      this.fetches.push(partitions);
      const autocommit = payload[fieldsAt + FETCH_MESSAGES_FIELD.autoCommit] !== 0;
      if (autocommit) await Promise.all(partitions.map((partition) => this.lanes.get(partition)));
      if (this.holdFetches) await this.hold(autocommit ? partitions : [], this.heldFetches);
      return this.fetch(code, payload, fieldsAt, targets, options);
    }
    throw responseError(code, INVALID_COMMAND, 'not served by the fake topic');
  }

  /**
   * An offset write runs after the held commands of its partition, fails while an earlier write of its partition has
   * an unknown outcome, and waits itself while `holdOffsetWrites` is set.
   */
  private async offsetWrite(partition: number): Promise<void> {
    await this.lanes.get(partition);
    const blocking = this.uncertainWrites.get(partition);
    if (blocking !== undefined) throw new LaneBlockedError(blocking, partition);
    if (this.holdOffsetWrites) await this.hold([partition], this.heldOffsetWrites);
  }

  /**
   * A group write fails while an earlier one has an unknown outcome, as the metadata lane of the client makes it. It
   * lands, and loses its reply when `uncertainGroupWrite` names its command.
   */
  private groupWrite(code: number, apply?: () => void): void {
    if (this.metadataBlocker !== undefined) throw new LaneBlockedError(this.metadataBlocker);
    apply?.();
    if (this.uncertainGroupWrite !== code) return;
    this.uncertainGroupWrite = undefined;
    this.metadataBlocker = UNCERTAIN_GROUP_OPERATION;
    throw new OperationUncertainError(UNCERTAIN_GROUP_OPERATION, new Error('reply lost after the group write'));
  }

  /**
   * An offset write or lookup whose context names another partition history than the current one fails with 87 before
   * the owner check, as in the server. No partition has history 0. A request without a context, or with history 0,
   * takes the history of its route, as the client fills it in.
   */
  private fenceHistory(code: number, options?: SendCommandOptions): void {
    const incarnation = options?.context?.incarnation ?? 0n;
    if (incarnation !== 0n && incarnation !== this.incarnation) throw responseError(code, HISTORY_UNAVAILABLE);
  }

  /**
   * A group offset write that names an owner other than the installed one fails with 5009, as the server fences it.
   * Owner 0 binds to the installed owner only from the op that installed it on. As the client sends it, a write under
   * owner 0 names at least the last metadata op that the client saw.
   */
  private fenceOwner(code: number, payload: Buffer, options?: SendCommandOptions): void {
    const owner = options?.context?.ownerGeneration ?? 0n;
    const named = options?.context?.metadataOp ?? 0n;
    const op = named > this.metadataOp ? named : this.metadataOp;
    if (payload[0] === ConsumerKind.Group &&
        (owner === 0n ? op < this.ownerInstalledAt : owner !== this.ownerGeneration)) {
      const refused = this.onNotOwnedWrite;
      this.onNotOwnedWrite = undefined;
      refused?.();
      throw responseError(code, CONSUMER_GROUP_PARTITION_NOT_OWNED);
    }
  }

  /** Applies an offset write unless `uncertainWriteLands` is false, and loses its reply while that is set. */
  private write(partition: number, apply: () => void): void {
    const lands = this.uncertainWriteLands;
    this.uncertainWriteLands = undefined;
    if (lands !== false) apply();
    if (lands !== undefined) {
      this.uncertainWrites.set(partition, UNCERTAIN_WRITE_OPERATION);
      throw new OperationUncertainError(UNCERTAIN_WRITE_OPERATION, new Error('reply lost after the write'));
    }
  }

  /** Waits until the test releases the command, while the commands of the `lane` partitions that come later wait too. */
  private async hold(lane: number[], held: (() => void)[]): Promise<void> {
    const released = new Promise<void>((release) => held.push(release));
    for (const partition of lane) this.lanes.set(partition, released);
    await released;
    for (const partition of lane) if (this.lanes.get(partition) === released) this.lanes.delete(partition);
  }

  private async fetch(code: number, payload: Buffer, fieldsAt: number, targets: number,
    options?: SendCommandOptions): Promise<CommandResponse> {
    const reply = (data: Buffer): CommandResponse => ({ status: 0, length: data.length, data });
    const autocommit = payload[fieldsAt + FETCH_MESSAGES_FIELD.autoCommit] !== 0;
    const group = payload[0] === ConsumerKind.Group;
    const count = payload.readUInt32LE(fieldsAt + FETCH_MESSAGES_FIELD.count);
    const sections: { partition: number, status: number, start: bigint, records: Buffer }[] = [];
    const primary = this.primaryOf(
      payload.readUInt32LE(fieldsAt + FETCH_MESSAGES_FIELDS_SIZE + FETCH_TARGET_FIELD.partitionId));
    let held = 0;
    for (let index = 0; index < targets; index += 1) {
      const at = fieldsAt + FETCH_MESSAGES_FIELDS_SIZE + index * FETCH_TARGET_SIZE;
      const partition = payload.readUInt32LE(at + FETCH_TARGET_FIELD.partitionId);
      const read = { code, partition, kind: payload[at + FETCH_TARGET_FIELD.kind],
        value: payload.readBigUInt64LE(at + FETCH_TARGET_FIELD.value), autocommit, count, at: Date.now() };
      this.reads.push(read);
      const refusal = this.refused.get(partition) ??
        (this.primaryOf(partition) === primary ? undefined : NOT_PRIMARY) ??
        (group ? this.groupRefusal(partition) : undefined);
      if (refusal !== undefined) {
        sections.push({ partition, status: refusal, start: 0n, records: Buffer.alloc(0) });
        continue;
      }
      if (this.history.has(partition)) {
        sections.push({ partition, status: HISTORY_UNAVAILABLE, start: 0n, records: Buffer.alloc(0) });
        continue;
      }
      const fenced = payload.readBigUInt64LE(at + FETCH_TARGET_FIELD.incarnation) !== 0n;
      // As in the server, a fence with owner 0 names the owner that the server installed.
      const owner = payload.readBigUInt64LE(at + FETCH_TARGET_FIELD.ownerGeneration);
      if (this.notOwned.has(partition) || (fenced && owner !== 0n && owner !== this.ownerGeneration)) {
        sections.push({ partition, status: CONSUMER_GROUP_PARTITION_NOT_OWNED, start: 0n, records: Buffer.alloc(0) });
        continue;
      }
      const start = this.start(partition, read.kind, read.value, count);
      if (start >= this.limit || this.emptyPartitions.has(partition) || (this.countLimited && held >= count)) {
        sections.push({ partition, status: 0, start, records: Buffer.alloc(0) });
        continue;
      }
      held += 2;
      if (autocommit) this.autoCommit(partition, start + 1n);
      const batch = records(start, this.unparsableHeaders);
      sections.push({ partition, status: 0, start,
        records: this.undecodable.has(start) ? batch.subarray(0, batch.length - 1) : batch });
    }
    this.refuseProbe(code, autocommit, count);
    if (this.holdProbes && !autocommit && count === PROBE_COUNT) await this.hold([], this.heldFetches);
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

  /** Refuses a read of one message without a commit once with `probeRefusal`. */
  private refuseProbe(code: number, autocommit: boolean, count: number): void {
    const status = this.probeRefusal;
    if (status === undefined || autocommit || count !== PROBE_COUNT) return;
    this.probeRefusal = undefined;
    throw responseError(code, status);
  }

  /** The context of a reply. As in the server, it names at least the metadata op that installed the owner. */
  private served() {
    const metadataOp = this.ownerInstalledAt > this.metadataOp ? this.ownerInstalledAt : this.metadataOp;
    return { ...SERVED, incarnation: this.incarnation, ownerGeneration: this.ownerGeneration, metadataOp };
  }

  /** The commit of a read, which the server applies as `max(stored, offset)`, unlike an explicit store. */
  private autoCommit(partition: number, offset: bigint): void {
    const stored = this.stored.get(partition);
    if (stored === undefined || stored < offset) this.stored.set(partition, offset);
  }

  private primaryOf(partition: number): number {
    return this.movedPrimaries.get(partition) ?? partition % this.primaries;
  }

  /** The refusal of a group read of `partition` by a forgotten member, or 5009 outside `assigned` or in `notOwned`. */
  private groupRefusal(partition: number): number | undefined {
    if (this.forgottenRefusal !== undefined) return this.forgottenRefusal;
    return this.assigned.includes(partition) && !this.notOwned.has(partition)
      ? undefined : CONSUMER_GROUP_PARTITION_NOT_OWNED;
  }

  /** A read that finds nothing waits until the reader stops it, as a parked read does. */
  private idle(options: SendCommandOptions | undefined, reply: CommandResponse): Promise<CommandResponse> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => resolve(reply), this.idleWait);
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

/** Opens a member of group 5 on `client`. */
const openGroup = (client: RawClient) => PartitionReader.open(async () => client, {
  streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 }, retryInterval: 20
});

const open = (topic: FakeTopic, partitions: number[], options = {}, start?: PollingStrategy) => {
  const client = topic.client();
  return PartitionReader.open(async () => client, {
    streamId: 1, topicId: 2, consumer, partitions, start, options: { maxWait: 100, ...options }, retryInterval: 20
  });
};

/**
 * An on read reader of partition 1 whose first command of `code`, the check of its start by default, reaches the
 * server on `release`.
 */
const openWithHeld = async (topic: FakeTopic, start?: PollingStrategy,
  code: number = COMMAND_CODE.PollMessagesDeferred, options = {}) => {
  const client = topic.client();
  const send = client.sendCommand;
  let release!: () => void;
  const released = new Promise<void>((resolve) => { release = resolve; });
  const held = { sent: 0, release };
  client.sendCommand = async (command, payload, options) => {
    if (command === code && ++held.sent === 1) await released;
    return send(command, payload, options);
  };
  const reader = await PartitionReader.open(async () => client, {
    streamId: 1, topicId: 2, consumer, partitions: [1], start,
    options: { maxWait: 100, commit: CommitPolicy.OnRead, ...options }, retryInterval: 20
  });
  return { reader, held };
};

/** An on read member of group 5 with partition 1, which a rebalance by another member installed again before a read. */
const openReinstalled = async (topic: FakeTopic) => {
  topic.assigned = [1];
  const client = topic.client();
  const reader = await PartitionReader.open(async () => client, {
    streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
    options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
  });
  while (!reader.partitions().includes(1)) await once(topic.emitter, 'synced');
  // The rebalance installed the owner at an op of the other member, which no reply to this client names.
  topic.ownerGeneration += 1n;
  topic.ownerInstalledAt = topic.metadataOp + 1n;
  return reader;
};

const offsets = (batches: { messages: { headers: { offset: bigint } }[] }[]): bigint[] =>
  batches.flatMap(({ messages }) => messages.map(({ headers }) => headers.offset));

const waitFor = async (condition: () => boolean, what: string, within = WAIT_MS): Promise<void> => {
  const deadline = Date.now() + within;
  while (!condition()) {
    assert.ok(Date.now() < deadline, what);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};

/** The reads of one message without a commit of a partition: the reads that find its installed owner. */
const probesOf = (topic: FakeTopic, partition: number) => topic.reads.filter((read) =>
  read.partition === partition && !read.autocommit && read.count === PROBE_COUNT);

/**
 * How many timers that `action` starts still run once it ends. It also counts unref'd timers, which
 * `process.getActiveResourcesInfo` leaves out.
 */
const timersLeftBy = async (action: () => Promise<void>): Promise<number> => {
  const running = new Set<number>();
  const hook = createHook({
    init: (id, type) => { if (type === 'Timeout') running.add(id); },
    destroy: (id) => { running.delete(id); }
  }).enable();
  try {
    await action();
    // A timer reports its end on a later turn of the event loop.
    await new Promise(setImmediate);
  } finally {
    hook.disable();
  }
  return running.size;
};

/** What the logger of the client writes while `action` runs. */
const logged = async (action: () => Promise<void>): Promise<string[]> => {
  const lines: string[] = [];
  const { enabled, log } = debug;
  debug.enabled = true;
  debug.log = (...args: unknown[]) => lines.push(args.map(String).join(' '));
  try {
    await action();
  } finally {
    debug.enabled = enabled;
    debug.log = log;
  }
  return lines;
};

/** The next batch of `partition`, past the batches of the other partitions and the errors of the whole reader. */
const nextOf = async (reader: PartitionReader, partition: number) => {
  // One deadline for the whole search, since errors of the whole reader can keep coming and renew a deadline per call.
  const signal = AbortSignal.timeout(1_000);
  for (;;) {
    const batch = await reader.next({ signal }).catch((error: unknown) => {
      if (error instanceof ReadError && error.partitionId === undefined) return undefined;
      throw error;
    });
    if (batch?.partitionId === partition) return batch;
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

  for (const [requestedBatches, expectedBatches] of [[1, 32], [40, 40]]) {
    it(`buffers ${expectedBatches} batches with a ${requestedBatches}-batch budget while two primaries stay empty`, async () => {
      const count = 2;
      const maxBytes = 512;
      const partitions = [0, 1, 2];
      const busy = partitions[2];
      const topic = new FakeTopic(BigInt((expectedBatches + 1) * count));
      topic.emptyPartitions.add(partitions[0]).add(partitions[1]);
      topic.idleWait = PARKED_READ_WAIT_MS;
      const client = topic.client();
      client.readinessGroups = async (_stream, _topic, targets) => targets.map((partition) => [partition]);
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer, partitions, start: PollingStrategy.Offset(0n),
        options: { count, maxBytes }, prefetchMessages: requestedBatches * count,
        prefetchBytes: requestedBatches * maxBytes
      });
      try {
        await waitFor(() => topic.reads.filter(({ partition }) => partition === busy).length === expectedBatches,
          'the busy primary did not fill the effective read-ahead budget');
        await new Promise(setImmediate);
        assert.equal(topic.reads.filter(({ partition }) => partition === busy).length, expectedBatches);
        for (const quiet of topic.emptyPartitions)
          assert.equal(topic.reads.filter(({ partition }) => partition === quiet).length, 1);
        assert.equal(topic.stored.size, 0);
      } finally {
        await reader.close();
      }
    });
  }

  it('refuses a read-ahead budget that is NaN, infinite or negative before it connects, and reads with a zero budget',
    async () => {
      const client = new FakeTopic().client();
      let connections = 0;
      const getClient = async () => {
        connections += 1;
        return client;
      };
      for (const budget of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY, -1]) {
        for (const name of ['prefetchBytes', 'prefetchMessages'] as const) {
          const request: PartitionReaderRequest = { streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 } };
          request[name] = budget;
          await assert.rejects(PartitionReader.open(getClient, request),
            (error: { errorCode: number }) => error.errorCode === INVALID_COMMAND, `${name} ${budget} opened a reader`);
        }
      }
      assert.equal(connections, 0, 'a refused budget reached the client');

      const reader = await PartitionReader.open(getClient, {
        streamId: 1, topicId: 2, consumer, partitions: [1], options: { maxWait: 100 }, prefetchBytes: 0, prefetchMessages: 0
      });
      try {
        assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [0n, 1n]);
      } finally {
        await reader.close();
      }
    });

  it('refuses a retry interval that is not a finite number above 0 before it connects', async () => {
    const client = new FakeTopic().client();
    let connections = 0;
    const getClient = async () => {
      connections += 1;
      return client;
    };
    for (const retryInterval of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      // A reader that opens anyway closes at once, so it does not read on without a pause.
      const failure = await PartitionReader.open(getClient, {
        streamId: 1, topicId: 2, consumer, partitions: [1], options: { maxWait: 100 }, retryInterval
      }).then((reader) => reader.close(), (error: unknown) => error);
      assert.equal((failure as { errorCode?: number } | undefined)?.errorCode, INVALID_COMMAND,
        `retryInterval ${retryInterval} opened a reader`);
    }
    assert.equal(connections, 0, 'a refused retry interval reached the client');
  });

  it('keeps on-read demand and drains a busy primary while two empty primaries wait', async () => {
    const count = 2;
    const maxBytes = 512;
    const topic = new FakeTopic(6n);
    topic.emptyPartitions.add(0).add(1);
    topic.idleWait = PARKED_READ_WAIT_MS;
    const client = topic.client();
    client.readinessGroups = async (_stream, _topic, targets) => targets.map((partition) => [partition]);
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer, partitions: [0, 1, 2], start: PollingStrategy.Offset(0n),
      options: { count, maxBytes, commit: CommitPolicy.OnRead }, prefetchMessages: count, prefetchBytes: maxBytes
    });
    try {
      await new Promise(setImmediate);
      assert.equal(topic.reads.length, 0, 'on-read waits for application demand');
      const batches = [];
      for (let batch = 0; batch < 3; batch += 1)
        batches.push(await reader.next({ signal: AbortSignal.timeout(1_000) }));
      assert.deepEqual(offsets(batches), [0n, 1n, 2n, 3n, 4n, 5n]);
      assert.ok(batches.every(({ partitionId }) => partitionId === 2));
      assert.ok(topic.reads.every(({ autocommit }) => autocommit));
      for (const quiet of topic.emptyPartitions)
        assert.equal(topic.reads.filter(({ partition }) => partition === quiet).length, 1);
      assert.deepEqual([...topic.stored], [[2, 5n]]);
    } finally {
      await reader.close();
    }
  });

  for (const commit of [CommitPolicy.Manual, CommitPolicy.OnRead]) {
    it(`waits on all three idle primaries together with a one-batch ${commit.kind} budget`, async () => {
      const count = 2;
      const maxBytes = 512;
      const topic = new FakeTopic(0n);
      topic.idleWait = PARKED_READ_WAIT_MS;
      const client = topic.client();
      client.readinessGroups = async (_stream, _topic, targets) => targets.map((partition) => [partition]);
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer, partitions: [0, 1, 2], start: PollingStrategy.Offset(0n),
        options: { count, maxBytes, commit }, prefetchMessages: count, prefetchBytes: maxBytes
      });
      let settled = false;
      const pending = reader.next().then(() => { settled = true; }, () => { settled = true; });
      try {
        await waitFor(() => topic.fetches.length === 3, 'an idle primary waited for another primary to finish');
        assert.deepEqual(topic.fetches, [[0], [1], [2]]);
        assert.equal(topic.stored.size, 0);
        assert.ok(topic.reads.every(({ autocommit }) => autocommit === (commit.kind === 'onRead')));
        assert.equal(settled, false, 'an idle request stays pending');
      } finally {
        await reader.close();
        await pending;
      }
    });
  }

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

  for (const lostFrom of [0n, 2n]) {
    it(`moves the offset back before the range from ${lostFrom} that a lost read stored, on close before its catch-up ` +
      'delivers', async () => {
      const topic = new FakeTopic(8n);
      if (lostFrom > 0n) topic.stored.set(1, lostFrom - 1n);
      topic.loseNextCommittingRead = true;
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
      // The catch-up reads by offset, and a read that finds nothing waits until the close stops it.
      topic.emptyOffsetReads = true;
      topic.idleWait = PARKED_READ_WAIT_MS;
      const pending = reader.next().then(() => undefined, (failure: unknown) => failure);
      await waitFor(() => topic.reads.some(({ kind, value }) => kind === PollingStrategyKind.Offset && value === lostFrom),
        'the catch-up did not start');
      await reader.close();

      assert.ok((await pending) instanceof Error);
      assert.deepEqual([topic.stored.get(1), topic.offsetDeletes], lostFrom === 0n ? [undefined, [1]] : [lostFrom - 1n, []]);
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
    // Without readiness groups each partition polls alone.
    const client: Partial<RawClient> = topic.client();
    delete client.readinessGroups;
    const reader = await PartitionReader.open(async () => client as RawClient, {
      streamId: 1, topicId: 2, consumer, partitions: [1], options: { maxWait: 100, commit: CommitPolicy.OnRead },
      retryInterval: 20
    });
    try {
      await reader.next({ signal: AbortSignal.timeout(2_000) });
    } finally {
      await reader.close();
    }

    const lost = topic.reads.find(({ autocommit }) => autocommit)!;
    assert.deepEqual([lost.code, lost.kind, topic.abandoned], [COMMAND_CODE.PollMessagesDeferred,
      PollingStrategyKind.Next, [1n]]);
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

  for (const [reply, context] of [['the fail-fast reply', EMPTY_PARTITION_CONTEXT],
    ['an empty reply of another history', { ...SERVED, incarnation: SERVED.incarnation + 1n }]] as const) {
    it(`keeps a catch-up whose read by offset gets ${reply}, reports nothing and reads that range again after its ` +
      'retry interval', async () => {
      const topic = new FakeTopic(8n);
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer, partitions: [1], options: { maxWait: 100, commit: CommitPolicy.OnRead },
        retryInterval: UNSERVED_RETRY_MS
      });
      const batches = [await reader.next()];
      topic.loseNextCommittingRead = true;
      topic.unservedPolls.push(context);
      try {
        for (let read = 0; read < 3; read += 1) batches.push(await reader.next({ signal: AbortSignal.timeout(2_000) }));
      } finally {
        await reader.close();
      }

      assert.deepEqual(offsets(batches), [0n, 1n, 2n, 3n, 4n, 5n, 6n, 7n]);
      const [unserved, again, ...rest] = topic.reads.filter(({ kind }) => kind === PollingStrategyKind.Offset);
      assert.deepEqual([unserved.value, again.value, rest.map(({ value }) => value)], [2n, 2n, [4n]]);
      assert.ok(again.at - unserved.at >= UNSERVED_RETRY_MS - TIMER_SLACK_MS,
        `read again ${again.at - unserved.at} ms after the unserved reply`);
    });
  }

  for (const [read, commit] of [['poll', CommitPolicy.Manual], ['check of its start', CommitPolicy.OnRead]] as const) {
    it(`reads on after the fail-fast reply to a ${read}, which does not end the partition`, async () => {
      const topic = new FakeTopic(2n);
      topic.unservedPolls.push(EMPTY_PARTITION_CONTEXT);
      // Without readiness groups each partition polls alone.
      const client: Partial<RawClient> = topic.client();
      delete client.readinessGroups;
      const reader = await PartitionReader.open(async () => client as RawClient, {
        streamId: 1, topicId: 2, consumer, partitions: [1], options: { maxWait: 100, commit },
        retryInterval: UNSERVED_RETRY_MS
      });
      const batches: PolledBatch[] = [];
      try {
        for (let batch = await reader.nextUntilEnd(); batch; batch = await reader.nextUntilEnd()) batches.push(batch);
      } finally {
        await reader.close();
      }

      assert.deepEqual(offsets(batches), [0n, 1n]);
      const [unserved, again] = topic.reads;
      assert.deepEqual([again.kind, again.value, again.count], [unserved.kind, unserved.value, unserved.count]);
      assert.ok(again.at - unserved.at >= UNSERVED_RETRY_MS - TIMER_SLACK_MS,
        `read again ${again.at - unserved.at} ms after the fail-fast reply`);
    });
  }

  it('ends a partition at an empty reply after a batch under a context that is all zero, as after one without a ' +
    'context', async () => {
    const topic = new FakeTopic(2n);
    const serveUnder = ({ incarnation, ownerGeneration, metadataOp }: PartitionContext) =>
      Object.assign(topic, { incarnation, ownerGeneration, metadataOp, ownerInstalledAt: metadataOp });
    serveUnder(EMPTY_PARTITION_CONTEXT);
    // Without readiness groups each partition polls alone.
    const client: Partial<RawClient> = topic.client();
    delete client.readinessGroups;
    const reader = await PartitionReader.open(async () => client as RawClient, {
      streamId: 1, topicId: 2, consumer, partitions: [1], options: { maxWait: 100, commit: CommitPolicy.Manual },
      retryInterval: 20
    });
    try {
      assert.deepEqual(offsets([(await reader.nextUntilEnd())!]), [0n, 1n]);
      serveUnder(SERVED);
      const ended = await Promise.race([reader.nextUntilEnd(),
        new Promise((resolve) => setTimeout(resolve, 1_000, 'still reading'))]);
      assert.equal(ended, undefined);
    } finally {
      await reader.close();
    }
  });

  for (const [firstKept, last, after] of [
    [3n, 2n, [[3n, 4n, 5n], [6n, 7n]]],
    [5n, 3n, [[4n], [5n], [6n, 7n]]],
    [8n, 3n, [[4n, 5n], [8n, 9n]]]
  ] as const) {
    const first = 2n;
    it(`reports offsets ${first} to ${last} once, in the text and the lost range of its error, and reads on when ` +
      `retention leaves a catch-up from ${first} only the offsets from ${firstKept}`, async () => {
      const topic = new FakeTopic(12n);
      topic.pollsHoldCount = true;
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
      const batches = [await reader.next()];
      // The lost read stores offsets 2 and 3 and the next read stores 4 and 5. Retention runs before the catch-up.
      topic.loseNextCommittingRead = true;
      topic.beforeUncommittedPoll = () => { topic.firstKept = firstKept; };
      try {
        await assert.rejects(reader.next({ signal: AbortSignal.timeout(2_000) }), (error: unknown) => {
          assert.ok(error instanceof ReadError, String(error));
          assert.deepEqual([error.partitionId, (error.error as { errorCode: number }).errorCode,
            String(error.error).includes(`offsets ${first} to ${last} of partition 1`), error.lost],
          [1, FETCH_PROGRESS_UNCERTAIN, true, { first, last }]);
          return true;
        });
        for (let read = 0; read < after.length; read += 1)
          batches.push(await reader.next({ signal: AbortSignal.timeout(2_000) }));
      } finally {
        await reader.close();
      }

      assert.deepEqual(batches.map((batch) => offsets([batch])), [[0n, 1n], ...after]);
    });
  }

  it('accepts a skip of retention right after a catch-up that read the range of a lost read again', async () => {
    const topic = new FakeTopic(12n);
    topic.pollsHoldCount = true;
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    const batches = [await reader.next()];
    // The lost read stores offsets 2 and 3 and the next read stores 4 and 5. The catch-up reads 2 to 5 again, and
    // retention removes offsets 6 and 7 before the committing read after it.
    topic.loseNextCommittingRead = true;
    topic.beforeUncommittedPoll = () => { topic.holdFetches = true; };
    try {
      batches.push(await reader.next({ signal: AbortSignal.timeout(2_000) }));
      // An on read reader reads only for a waiting next().
      const after = reader.next({ signal: AbortSignal.timeout(2_000) });
      await waitFor(() => topic.heldFetches.length === 1, 'the committing read after the catch-up did not start');
      topic.firstKept = 8n;
      topic.holdFetches = false;
      topic.heldFetches.shift()!();
      batches.push(await after);
    } finally {
      await reader.close();
    }

    assert.deepEqual(batches.map((batch) => offsets([batch])), [[0n, 1n], [2n, 3n, 4n, 5n], [8n, 9n]]);
  });

  it('reports the refusal that stops a catch-up after the range that it gives up, as no lost read is pending then',
    async () => {
      const topic = new FakeTopic(8n);
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
      const outcome = (read: Promise<PolledBatch>) => read.then((batch) => offsets([batch]), (error: unknown) =>
        error instanceof ReadError
          ? { code: (error.error as { errorCode: number }).errorCode, stops: error.stopsPartition }
          : String(error));
      const seen = [await outcome(reader.next())];
      // The lost read stores offsets 2 and 3 and the next read stores 4 and 5. The history changes before the catch-up.
      topic.loseNextCommittingRead = true;
      topic.beforeUncommittedPoll = () => {
        throw responseError(COMMAND_CODE.PollMessagesDeferred, HISTORY_UNAVAILABLE);
      };
      try {
        for (let read = 0; read < 3; read += 1)
          seen.push(await outcome(reader.next({ signal: AbortSignal.timeout(2_000) })));
      } finally {
        await reader.close();
      }

      assert.deepEqual(seen, [[0n, 1n], { code: FETCH_PROGRESS_UNCERTAIN, stops: false }, [4n, 5n],
        { code: HISTORY_UNAVAILABLE, stops: true }]);
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

  it('hands over no catch-up read of a partition that left the assignment, since the leave reported its range lost',
    async () => {
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
        // The server serves the catch-up read of the lost range before the leave, and its reply comes after it.
        topic.beforeUncommittedPoll = async () => {
          topic.assigned = [];
          await waitFor(() => !reader.partitions().includes(1), 'the partition did not leave the assignment');
        };
        await assert.rejects(reader.next({ signal: AbortSignal.timeout(2_000) }), (error: unknown) =>
          error instanceof ReadError && error.partitionId === 1 &&
          (error.error as { errorCode: number }).errorCode === 14010);
        assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [4n, 5n]);
        await reader.next({ signal: AbortSignal.timeout(200) }).then(() => assert.fail('a batch arrived'), () => undefined);
      } finally {
        await reader.close();
      }
    });

  for (const [name, start, firstOffset, partitions] of [
    ['next', PollingStrategy.Next, 0n, [0, 1]],
    ['next', PollingStrategy.Next, 0n, [1, 3]],
    ['offset 0', PollingStrategy.Offset(0n), 0n, [0, 1]],
    ['offset 4', PollingStrategy.Offset(4n), 4n, [0, 1]]
  ] as const) {
    it(`rewinds unread on read batches on close from ${name}, partitions ${partitions}`, async () => {
      const topic = new FakeTopic(firstOffset + 2n);
      const reader = await open(topic, [...partitions], { commit: CommitPolicy.OnRead }, start);
      let taken: number;
      try {
        taken = (await reader.next()).partitionId;
        await waitFor(() => partitions.every((partition) => topic.stored.get(partition) === firstOffset + 1n),
          'both partitions were not committed by their reads');
      } finally {
        await reader.close();
      }
      assert.equal(topic.stored.get(taken), firstOffset + 1n);
      const unread = partitions.find((partition) => partition !== taken)!;
      assert.equal(topic.stored.get(unread), firstOffset === 0n ? undefined : firstOffset - 1n);
      const resumed = await open(topic, [unread], { commit: CommitPolicy.OnRead });
      try {
        assert.deepEqual(offsets([await resumed.next()]), [firstOffset, firstOffset + 1n]);
      } finally {
        await resumed.close();
      }
    });
  }

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

  for (const [name, start] of [['offset 0', PollingStrategy.Offset(0n)], ['first', PollingStrategy.First]] as const) {
    it(`deletes the stored offset on an on read seek to ${name}, so the reads replay the partition`, async () => {
      const topic = new FakeTopic(6n);
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
      try {
        assert.deepEqual(offsets([await reader.next(), await reader.next()]), [0n, 1n, 2n, 3n]);
        assert.equal(topic.stored.get(1), 3n);
        await reader.seek(1, start);
        const storedAfterSeek = topic.stored.get(1);
        const replayed: PolledBatch[] = [];
        for (let batch = await reader.nextUntilEnd(); batch; batch = await reader.nextUntilEnd()) replayed.push(batch);
        assert.deepEqual(offsets(replayed), [0n, 1n, 2n, 3n, 4n, 5n]);
        assert.deepEqual([storedAfterSeek, topic.offsetLookups, topic.offsetDeletes], [undefined, [1], [1]]);
      } finally {
        await reader.close();
      }
    });
  }

  it('looks up the stored offset on an on read seek to offset 0 and deletes nothing when there is none', async () => {
    const topic = new FakeTopic(4n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    try {
      await reader.seek(1, PollingStrategy.Offset(0n));
      assert.deepEqual([topic.offsetLookups, topic.offsetDeletes], [[1], []]);
      assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
    } finally {
      await reader.close();
    }
  });

  for (const [name, start, commit] of [
    ['next', PollingStrategy.Next, CommitPolicy.OnRead],
    ['last', PollingStrategy.Last, CommitPolicy.OnRead],
    ['a timestamp after the last message', PollingStrategy.Timestamp(10n), CommitPolicy.OnRead],
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

  for (const [name, sought, failure] of [['store', 6n, 'storeFailure'], ['delete', 0n, 'deleteFailure']] as const) {
    it(`fails an on read seek whose ${name} is refused, and moves nothing`, async () => {
      const topic = new FakeTopic(10n);
      const reader = await open(topic, [1, 3], { commit: CommitPolicy.OnRead });
      try {
        assert.deepEqual([(await reader.next()).partitionId, topic.stored.get(3)], [1, 1n]);
        topic[failure] = new Error(`${name} refused`);
        await assert.rejects(reader.seek(3, PollingStrategy.Offset(sought)), { message: `${name} refused` });
        topic[failure] = undefined;
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
  }

  for (const lands of [true, false]) {
    it(`fails an on read seek whose store ${lands ? 'landed' : 'did not land'} with an unknown outcome, reads on from ` +
      'where it was and seeks again', async () => {
      const topic = new FakeTopic(10n);
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
      try {
        assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
        topic.uncertainWriteLands = lands;
        await assert.rejects(reader.seek(1, PollingStrategy.Offset(6n)), OperationUncertainError);
        assert.deepEqual(topic.abandoned, [UNCERTAIN_WRITE_OPERATION]);
        assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [2n, 3n]);
        await reader.seek(1, PollingStrategy.Offset(8n));
        assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [8n, 9n]);
      } finally {
        await reader.close();
      }
      assert.equal(topic.stored.get(1), 9n);
    });
  }

  for (const [position, limit, start, last, sought, expected] of [
    ['after it delivered up to 1, before a forward seek', 10n, undefined, 1n, PollingStrategy.Offset(6n), 1n],
    ['after it delivered up to 19, before a backward seek', 30n, undefined, 19n, PollingStrategy.Offset(10n), 19n],
    ['of an offset start past the end without a stored offset, before a backward seek', 20n,
      PollingStrategy.Offset(20n), undefined, PollingStrategy.Offset(10n), undefined]
  ] as const) {
    it('restores the offset from before an on read seek whose store landed with an unknown outcome on close, ' +
      `${position}`, async () => {
      const topic = new FakeTopic(limit);
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead }, start);
      let waiting: Promise<unknown> = Promise.resolve();
      if (last === undefined) {
        // A committing read starts on demand and finds nothing past the end, so the close goes by the start of that
        // read. The close ends the waiting next().
        waiting = reader.next().catch(() => undefined);
        await waitFor(() => topic.reads.length > 0, 'the reader did not read');
      }
      for (let delivered = -1n; last !== undefined && delivered < last;)
        delivered = offsets([await reader.next()]).at(-1)!;
      topic.uncertainWriteLands = true;
      await assert.rejects(reader.seek(1, sought), OperationUncertainError);
      await reader.close();
      await waiting;

      assert.equal(topic.stored.get(1), expected);
    });
  }

  /** Reads the message at 5 that does not decode, which the read stored with 6, and returns the skip of the error. */
  const skipOfUndecodable = async (topic: FakeTopic, reader: PartitionReader) => {
    topic.undecodable.add(5n);
    const error = await reader.next().then(() => undefined, (failure: unknown) => failure);
    assert.ok(error instanceof ReadError && error.skip !== undefined, String(error));
    return error.skip;
  };

  for (const [read, seek, start, stored, setUp] of [
    ['a lost read from a Next start', 'to 2', undefined, 4n,
      async (topic: FakeTopic, reader: PartitionReader) => {
        const cancellation = new AbortController();
        topic.loseNextCommittingRead = true;
        topic.onLostCommittingRead = () => cancellation.abort(new Error('stop before delivery'));
        await assert.rejects(reader.next({ signal: cancellation.signal }), /stop before delivery/);
        return PollingStrategy.Offset(2n);
      }],
    ['a read that does not decode', 'to its skip', PollingStrategy.Offset(5n), undefined, skipOfUndecodable],
    ['a read that does not decode', 'to First', PollingStrategy.Offset(5n), undefined,
      async (topic: FakeTopic, reader: PartitionReader) => {
        await skipOfUndecodable(topic, reader);
        return PollingStrategy.First;
      }]
  ] as const) {
    it(`restores the rewind point of ${read} on close after an on read seek ${seek} with an unknown outcome`,
      async () => {
        const topic = new FakeTopic(10n);
        if (stored !== undefined) topic.stored.set(1, stored);
        const client = topic.client();
        // No read starts within the retry interval after a lost read, so none can come before the seek.
        const reader = await PartitionReader.open(async () => client, {
          streamId: 1, topicId: 2, consumer, partitions: [1], start,
          options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: PARKED_READ_WAIT_MS
        });
        const sought = await setUp(topic, reader);
        topic.uncertainWriteLands = true;
        await assert.rejects(reader.seek(1, sought), OperationUncertainError);
        await reader.close();

        assert.equal(topic.stored.get(1), 4n);
      });
  }

  for (const [stored, checked] of [[50n, 'before'], [50n, 'after'], [undefined, 'before'], [undefined, 'after']] as const) {
    it(`restores ${stored === undefined ? 'no' : 'the'} stored offset on close after a seek to Last whose check ends ` +
      `${checked} an on read seek whose store landed with an unknown outcome`, async () => {
      const topic = new FakeTopic(100n);
      if (stored !== undefined) topic.stored.set(1, stored);
      // A refused committing read stores nothing, so no batch decides the close.
      topic.refused.set(1, TRANSIENT_NOT_ACCEPTED);
      const { reader, held } = await openWithHeld(topic, undefined, COMMAND_CODE.PollMessagesDeferred, { count: 10 });
      await reader.seek(1, PollingStrategy.Last);
      const waiting = reader.next().catch(() => undefined);
      await waitFor(() => held.sent === 1, 'the check did not start');
      // The check of the last 10 messages moves the position to offset 90.
      const readsFromCheck = () => topic.reads.some(({ autocommit, value }) => autocommit && value === 90n);
      if (checked === 'before') {
        held.release();
        await waitFor(readsFromCheck, 'the reader did not read from the offset of the check');
      }
      topic.uncertainWriteLands = true;
      await assert.rejects(reader.seek(1, PollingStrategy.Offset(20n)), OperationUncertainError);
      held.release();
      await waitFor(readsFromCheck, 'the reader did not read from the offset of the check');
      await reader.close();
      await waiting;

      assert.deepEqual([topic.stored.get(1), topic.offsetDeletes], [stored, stored === undefined ? [1] : []]);
    });
  }

  it('rewinds on close before a batch that arrived while an on read seek with an unknown outcome waited for it',
    async () => {
      const topic = new FakeTopic(10n);
      topic.holdFetches = true;
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead, count: 2 }, PollingStrategy.Last);
      const waiting = reader.next().catch(() => undefined);
      await waitFor(() => topic.heldFetches.length === 1, 'the committing read from offset 8 did not start');
      topic.uncertainWriteLands = true;
      const seek = assert.rejects(reader.seek(1, PollingStrategy.Offset(6n)), OperationUncertainError);
      // The close starts while the seek waits for the read.
      const closing = reader.close();
      topic.holdFetches = false;
      topic.heldFetches.shift()!();
      await seek;
      await closing;
      await waiting;

      assert.equal(topic.stored.get(1), 7n);
    });

  it('reads on by offset after an empty read that follows an on read seek to First whose delete landed with an ' +
    'unknown outcome', async () => {
    const topic = new FakeTopic(5n);
    topic.stored.set(1, 4n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    try {
      topic.uncertainWriteLands = true;
      await assert.rejects(reader.seek(1, PollingStrategy.First), OperationUncertainError);
      await assert.rejects(reader.next({ signal: AbortSignal.timeout(300) }));
      assert.ok(topic.reads.some(({ autocommit, kind, value }) =>
        autocommit && kind === PollingStrategyKind.Offset && value === 5n), 'no read from offset 5 found nothing');
      topic.limit = 7n;
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [5n, 6n]);
    } finally {
      await reader.close();
    }
  });

  it('keeps nothing of an on read seek write whose unknown outcome ends in a new session of a group reader',
    async () => {
      const topic = new FakeTopic(10n);
      topic.assigned = [1];
      topic.stored.set(1, 4n);
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
        options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
      });
      try {
        while (!reader.partitions().includes(1)) await once(topic.emitter, 'synced');
        topic.holdOffsetWrites = true;
        topic.uncertainWriteLands = false;
        const seek = reader.seek(1, PollingStrategy.Offset(2n));
        await waitFor(() => topic.heldOffsetWrites.length === 1, 'the store of the seek did not start');
        topic.emitter.emit('sessionReset');
        // Another member read on while the session changed.
        topic.stored.set(1, 7n);
        topic.holdOffsetWrites = false;
        topic.heldOffsetWrites.shift()!();
        await assert.rejects(seek, OperationUncertainError);
      } finally {
        topic.holdOffsetWrites = false;
        for (const release of topic.heldOffsetWrites) release();
        await reader.close();
      }

      assert.equal(topic.stored.get(1), 7n);
    });

  it('lets a queued batch of an earlier assignment of a partition that came back decide nothing on close', async () => {
    const topic = new FakeTopic(4n);
    topic.assigned = [1, 3];
    topic.assignedAfterRead = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
    });
    assert.equal((await reader.next()).partitionId, 1);
    while (reader.partitions().includes(3)) await once(topic.emitter, 'synced');
    // Another member owned the partition meanwhile and read on.
    topic.ownerGeneration += 1n;
    topic.stored.set(3, 3n);
    topic.assigned = [1, 3];
    while (!reader.partitions().includes(3)) await once(topic.emitter, 'synced');
    await reader.close();

    assert.deepEqual([topic.stored.get(3), topic.offsetDeletes], [3n, []]);
  });

  it('moves no offset on close of a partition that left the assignment, so its queued batch is lost for the group',
    async () => {
      const topic = new FakeTopic(4n);
      topic.assigned = [1, 3];
      topic.assignedAfterRead = [1];
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
        options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
      });
      assert.equal((await reader.next()).partitionId, 1);
      while (reader.partitions().includes(3)) await once(topic.emitter, 'synced');
      await reader.close();

      assert.deepEqual([topic.stored.get(3), topic.offsetDeletes], [1n, []]);
    });

  it('stores the seek of a group partition with owner generation 0 after a session change, before a read of it',
    async () => {
      const topic = new FakeTopic(10n);
      topic.assigned = [1];
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
        options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
      });
      try {
        assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
        topic.changeSession();
        await reader.seek(1, PollingStrategy.Offset(6n));
        assert.deepEqual([topic.stored.get(1), topic.writeContexts], [5n, [INSTALLED_OWNER]]);
      } finally {
        await reader.close();
      }
    });

  it('rewinds after a lost read of a group partition with owner generation 0 on close after a session change',
    async () => {
      const topic = new FakeTopic(10n);
      topic.assigned = [1];
      const client = topic.client();
      // No read starts within the retry interval after the lost read, so the close comes first.
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
        options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: PARKED_READ_WAIT_MS
      });
      assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
      topic.loseNextCommittingRead = true;
      const cancellation = new AbortController();
      topic.onLostCommittingRead = () => {
        // The session ends while the read is on the wire.
        topic.changeSession();
        cancellation.abort(new Error('stop before delivery'));
      };
      await assert.rejects(reader.next({ signal: cancellation.signal }), /stop before delivery/);
      await reader.close();

      assert.deepEqual([topic.stored.get(1), topic.writeContexts], [1n, [INSTALLED_OWNER]]);
    });

  for (const [position, start, stored, sought, expected] of [
    ['a Next start before the first read', undefined, 3n, PollingStrategy.Offset(8n), [4n, 5n, 6n, 7n, 8n, 9n]],
    ['an offset start before its lookup', PollingStrategy.Offset(2n), 5n, PollingStrategy.Offset(8n), [6n, 7n, 8n, 9n]],
    ['a timestamp start before its lookup, without a stored offset', PollingStrategy.Timestamp(2n), undefined,
      PollingStrategy.Offset(8n), [2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n]],
    ['a Next start before the first read', undefined, 3n, PollingStrategy.First, [4n, 5n, 6n, 7n, 8n, 9n]],
    ['an offset start before its lookup', PollingStrategy.Offset(2n), 5n, PollingStrategy.First, [6n, 7n, 8n, 9n]]
  ] as const) {
    const write = sought.kind === PollingStrategyKind.First ? 'to First whose delete' : 'whose store';
    it(`reads on from the stored offset of ${position} after an on read seek ${write} landed with an unknown ` +
      'outcome', async () => {
      const topic = new FakeTopic(10n);
      if (stored !== undefined) topic.stored.set(1, stored);
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead }, start);
      const delivered: bigint[] = [];
      try {
        topic.uncertainWriteLands = true;
        await assert.rejects(reader.seek(1, sought), OperationUncertainError);
        // A reader that skips messages delivers fewer than expected and then waits, so the deadline ends the loop.
        const signal = AbortSignal.timeout(2_000);
        while (delivered.length < expected.length) {
          const batch = await reader.next({ signal }).catch((error: unknown) => {
            if (signal.aborted) return undefined;
            throw error;
          });
          if (!batch) break;
          delivered.push(...offsets([batch]));
        }
      } finally {
        await reader.close();
      }
      assert.deepEqual(delivered, expected);
    });

    it(`restores the stored offset of ${position} on close after an on read seek ${write} landed with an unknown ` +
      'outcome', async () => {
      const topic = new FakeTopic(10n);
      if (stored !== undefined) topic.stored.set(1, stored);
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead }, start);
      topic.uncertainWriteLands = true;
      await assert.rejects(reader.seek(1, sought), OperationUncertainError);
      await reader.close();

      assert.equal(topic.stored.get(1), stored);
    });
  }

  it('drops a check that ends after an on read seek with an unknown outcome pinned its partition', async () => {
    const topic = new FakeTopic(10n);
    topic.stored.set(1, 3n);
    const { reader, held } = await openWithHeld(topic);
    try {
      const first = reader.next({ signal: AbortSignal.timeout(2_000) });
      await waitFor(() => held.sent === 1, 'the check did not start');
      topic.uncertainWriteLands = true;
      await assert.rejects(reader.seek(1, PollingStrategy.Offset(8n)), OperationUncertainError);
      // The server answers the check after the store landed, so it finds offset 8 past the pin at 4.
      held.release();
      assert.deepEqual(offsets([await first]), [4n, 5n]);
    } finally {
      await reader.close();
    }
  });

  for (const write of ['commit', 'close', 'leave'] as const) {
    it(`stores the offset of a group partition on ${write} with owner generation 0 after a session change`, async () => {
      const topic = new FakeTopic(10n);
      topic.assigned = [1];
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, retryInterval: 20,
        options: { maxWait: 100, commit: write === 'commit' ? CommitPolicy.Manual
          : CommitPolicy.Auto({ interval: PARKED_READ_WAIT_MS, after: CommitAfter.Delivery }) }
      });
      try {
        const checkpoint = (await reader.next({ signal: AbortSignal.timeout(2_000) })).checkpoint()!;
        topic.changeSession();
        if (write === 'commit') await reader.commit(checkpoint);
        if (write === 'leave') {
          topic.assigned = [];
          while (reader.partitions().includes(1)) await once(topic.emitter, 'synced');
        }
      } finally {
        await reader.close();
      }
      assert.deepEqual([topic.stored.get(1), topic.writeContexts], [1n, [INSTALLED_OWNER]]);
    });
  }

  it('reads the range of a lost read of a group partition again after a session change', async () => {
    const topic = new FakeTopic(10n);
    topic.assigned = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
    });
    try {
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [0n, 1n]);
      topic.loseNextCommittingRead = true;
      // The reply is lost with the connection.
      topic.onLostCommittingRead = () => topic.changeSession();
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [2n, 3n]);
    } finally {
      await reader.close();
    }
  });

  it('rewinds on close to an on read seek point of a group partition that a session change came before', async () => {
    const topic = new FakeTopic(10n);
    topic.assigned = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
    });
    try {
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [0n, 1n]);
      topic.changeSession();
      await reader.seek(1, PollingStrategy.Offset(6n));
      topic.holdFetches = true;
      const aborting = new AbortController();
      const aborted = reader.next({ signal: aborting.signal });
      await waitFor(() => topic.heldFetches.length === 1, 'the committing read from offset 6 did not start');
      aborting.abort();
      await assert.rejects(aborted);
      topic.holdFetches = false;
      topic.heldFetches.shift()!();
      // The read stores past a batch that nobody takes, so the close moves the offset back to the seek point.
      await waitFor(() => topic.stored.get(1) === 7n, 'the committing read did not store');
    } finally {
      await reader.close();
    }
    assert.deepEqual([topic.stored.get(1), topic.writeContexts], [5n, [INSTALLED_OWNER, INSTALLED_OWNER]]);
  });

  it('reads the range of a lost read of a group partition again after a read is refused as not owned', async () => {
    const topic = new FakeTopic(10n);
    topic.assigned = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
    });
    try {
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [0n, 1n]);
      topic.loseNextCommittingRead = true;
      // The server installs the owner again before the reader sees the session change, so the next read names the
      // earlier owner and is refused as not owned.
      topic.onLostCommittingRead = () => { topic.ownerGeneration += 1n; };
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [2n, 3n]);
    } finally {
      await reader.close();
    }
  });

  for (const [read, prepare, during] of [
    ['', () => undefined],
    [' when the message of that read does not decode', (topic: FakeTopic) => { topic.undecodable.add(0n); }],
    [' after the server refused that read once at its poll quota', (topic: FakeTopic) => {
      topic.probeRefusal = DEFERRED_POLL_CAPACITY_EXCEEDED;
    }],
    // The client gives a NotPrimary refusal of a fetch to its caller, so the read for the owner retries it.
    [' after a node that no longer leads the partition refused that read once', (topic: FakeTopic) => {
      topic.probeRefusal = NOT_PRIMARY;
    }],
    [' after the server refused the section of that read for a while', (topic: FakeTopic) => {
      topic.refused.set(1, TRANSIENT_NOT_ACCEPTED);
    }, async (topic: FakeTopic) => {
      await waitFor(() => probesOf(topic, 1).length > 1, 'the read for the owner did not go again');
      topic.refused.delete(1);
    }]
  ] as [string, (topic: FakeTopic) => void, ((topic: FakeTopic) => Promise<void>)?][]) {
    it('stores the seek of a group partition that a rebalance by another member installed, before a read of it, ' +
      `under the owner that a read from Next without a commit finds${read}`, async () => {
      const topic = new FakeTopic(10n);
      const reader = await openReinstalled(topic);
      try {
        prepare(topic);
        const sought = reader.seek(1, PollingStrategy.Offset(6n)).then(() => undefined, (error: unknown) => error);
        await during?.(topic);
        assert.equal(await sought, undefined);
        const [probe] = probesOf(topic, 1);
        assert.deepEqual([topic.stored.get(1), topic.writeContexts, [probe?.code, probe?.kind]],
          [5n, [undefined, REINSTALLED], [COMMAND_CODE.FetchMessages, PollingStrategyKind.Next]]);
      } finally {
        await reader.close();
      }
    });
  }

  it('ends the reads for the owner of a seek when the close starts, so the seek fails with its refusal', async () => {
    const topic = new FakeTopic(10n);
    const reader = await openReinstalled(topic);
    topic.refused.set(1, TRANSIENT_NOT_ACCEPTED);
    const sought = reader.seek(1, PollingStrategy.Offset(6n)).then(() => undefined, (error: unknown) => error);
    let closeFailure: unknown;
    try {
      await waitFor(() => probesOf(topic, 1).length > 1, 'the read for the owner did not go again');
    } finally {
      closeFailure = await reader.close().then(() => undefined, (error: unknown) => error);
    }
    const seekFailure = await sought as { errorCode?: number } | undefined;
    assert.deepEqual([seekFailure?.errorCode, closeFailure, topic.stored.get(1)],
      [CONSUMER_GROUP_PARTITION_NOT_OWNED, undefined, undefined]);
  });

  for (const [outcome, change, refusal, stored] of [
    ['lands under the owner that a read without a commit finds', () => undefined, undefined, 1n],
    ['lands under that owner after the server refused that read once at its poll quota', (topic: FakeTopic) => {
      topic.probeRefusal = DEFERRED_POLL_CAPACITY_EXCEEDED;
    }, undefined, 1n],
    ['fails when that read is refused as not owned', (topic: FakeTopic) => {
      topic.forgottenRefusal = CONSUMER_GROUP_PARTITION_NOT_OWNED;
    }, CONSUMER_GROUP_PARTITION_NOT_OWNED, 3n],
    ['fails with its own refusal when the server refuses that read for another reason', (topic: FakeTopic) => {
      topic.probeRefusal = UNAUTHORIZED;
    }, CONSUMER_GROUP_PARTITION_NOT_OWNED, 3n],
    // A race guard: the history of the partition changes between the refusal and the read for its owner.
    ['fails when the partition is recreated before that read, which then names another incarnation',
      (topic: FakeTopic) => { topic.onNotOwnedWrite = () => { topic.incarnation += 1n; }; },
      CONSUMER_GROUP_PARTITION_NOT_OWNED, 3n]
  ] as [string, (topic: FakeTopic) => void, number | undefined, bigint][]) {
    it(`rewinds on close a group partition that a rebalance by another member installed again: ${outcome}`,
      async () => {
        const topic = new FakeTopic(10n);
        topic.assigned = [1];
        const client = topic.client();
        const reader = await PartitionReader.open(async () => client, {
          streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
          options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
        });
        let failure: unknown;
        try {
          assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [0n, 1n]);
          topic.holdFetches = true;
          const aborting = new AbortController();
          const aborted = reader.next({ signal: aborting.signal });
          await waitFor(() => topic.heldFetches.length === 1, 'the committing read from offset 2 did not start');
          aborting.abort();
          await assert.rejects(aborted);
          topic.holdFetches = false;
          topic.heldFetches.shift()!();
          await waitFor(() => topic.stored.get(1) === 3n, 'the committing read did not store');
          // The rebalance installed the owner again at an op of the other member, which no reply to this client
          // names, so the rewind of the batch that nobody takes names an owner that the server replaced.
          topic.ownerGeneration += 1n;
          topic.ownerInstalledAt = topic.metadataOp + 1n;
          change(topic);
        } finally {
          failure = await reader.close().then(() => undefined, (error: unknown) => error);
        }
        assert.equal((failure as { errorCode?: number } | undefined)?.errorCode, refusal);
        assert.deepEqual([topic.stored.get(1), topic.writeContexts],
          [stored, refusal === undefined ? [SERVED, REINSTALLED] : [SERVED]]);
      });
  }

  it('keeps the refusal of the store of a group partition that left the assignment, without a read for its owner',
    async () => {
      const topic = new FakeTopic(10n);
      topic.assigned = [1];
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, retryInterval: 20,
        options: { maxWait: 100,
          commit: CommitPolicy.Auto({ interval: PARKED_READ_WAIT_MS, after: CommitAfter.Delivery }) }
      });
      try {
        assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [0n, 1n]);
        // A rebalance by another member gave the partition away.
        topic.ownerGeneration += 1n;
        topic.ownerInstalledAt = topic.metadataOp + 1n;
        topic.assigned = [];
        while (reader.partitions().includes(1)) await once(topic.emitter, 'synced');
      } finally {
        await reader.close();
      }
      assert.deepEqual([topic.stored.get(1), topic.writeContexts.length, probesOf(topic, 1).length], [undefined, 1, 0]);
    });

  it('keeps the refusal of a commit of a group partition that left the assignment during its store, without a read ' +
    'for its owner', async () => {
    const topic = new FakeTopic(10n);
    topic.assigned = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.Manual }, retryInterval: 20
    });
    try {
      const checkpoint = (await reader.next({ signal: AbortSignal.timeout(2_000) })).checkpoint()!;
      topic.holdOffsetWrites = true;
      const committed = reader.commit(checkpoint).then(() => undefined, (error: unknown) => error);
      await waitFor(() => topic.heldOffsetWrites.length === 1, 'the store of the commit did not start');
      topic.ownerGeneration += 1n;
      topic.ownerInstalledAt = topic.metadataOp + 1n;
      topic.assigned = [];
      while (reader.partitions().includes(1)) await once(topic.emitter, 'synced');
      topic.holdOffsetWrites = false;
      topic.heldOffsetWrites.shift()!();
      assert.deepEqual([(await committed as { errorCode?: number } | undefined)?.errorCode, probesOf(topic, 1).length],
        [CONSUMER_GROUP_PARTITION_NOT_OWNED, 0]);
    } finally {
      await reader.close();
    }
  });

  it('ends the read for the owner of an automatic store when the close starts and logs its refusal, so the close ' +
    'stores', async () => {
    const topic = new FakeTopic(10n);
    topic.assigned = [1];
    const client = topic.client();
    // A refused read for the owner goes again only after the test.
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, retryInterval: PARKED_READ_WAIT_MS,
      options: { maxWait: 100, commit: CommitPolicy.Auto({ every: CommitEvery.Message, after: CommitAfter.Marked }) }
    });
    let lines: string[] = [];
    try {
      const checkpoint = (await reader.next({ signal: AbortSignal.timeout(2_000) })).checkpoint()!;
      topic.ownerGeneration += 1n;
      topic.ownerInstalledAt = topic.metadataOp + 1n;
      topic.probeRefusal = DEFERRED_POLL_CAPACITY_EXCEEDED;
      reader.mark(checkpoint);
      await waitFor(() => probesOf(topic, 1).length === 1, 'the read for the owner did not start');
    } finally {
      lines = await logged(() => reader.close());
    }
    const refusals = lines.filter((line) => line.includes('automatic offset store failed') &&
      line.includes(`code: ${CONSUMER_GROUP_PARTITION_NOT_OWNED}`));
    assert.deepEqual([refusals.length, topic.stored.get(1)], [1, 1n]);
  });

  it('stores an on read seek to the skip of a reply that ends before its context under the history of its route',
    async () => {
      const topic = new FakeTopic(10n);
      topic.truncatedReplies.add(0n);
      // Without readiness groups the partition polls alone, so it reads its offset start without a context.
      const client: Partial<RawClient> = topic.client();
      delete client.readinessGroups;
      const reader = await PartitionReader.open(async () => client as RawClient, {
        streamId: 1, topicId: 2, consumer, partitions: [1], start: PollingStrategy.Offset(0n),
        options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
      });
      try {
        const error = await reader.next({ signal: AbortSignal.timeout(2_000) })
          .then(() => undefined, (failure: unknown) => failure);
        assert.ok(error instanceof ReadError && error.skip !== undefined, String(error));
        await reader.seek(1, error.skip);
        assert.deepEqual([topic.stored.get(1), topic.writeContexts], [0n, [undefined]]);
        assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [1n, 2n]);
      } finally {
        await reader.close();
      }
    });

  it('ends the interval timer of an automatic store when the close ends', async () => {
    const topic = new FakeTopic(4n);
    const timers = await timersLeftBy(async () => {
      const reader = await open(topic, [1],
        { commit: CommitPolicy.Auto({ interval: PARKED_READ_WAIT_MS, after: CommitAfter.Delivery }) });
      try {
        assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
      } finally {
        await reader.close();
      }
    });

    assert.deepEqual([topic.stored.get(1), timers], [1n, 0]);
  });

  it('keeps at most one pause timer of a group reader while reads arrive', async () => {
    const batches = 50;
    // The smallest read-ahead budget keeps the end of the topic out of reach, so no read waits on a timer of its own.
    const topic = new FakeTopic(BigInt(4 * batches));
    topic.assigned = [1];
    const client = topic.client();
    let reader: PartitionReader | undefined;
    const timers = await timersLeftBy(async () => {
      reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
        options: { maxWait: 100, count: 2 }, prefetchMessages: 0, retryInterval: 20
      });
      for (let batch = 0; batch < batches; batch += 1) await reader.next();
    });
    await reader?.close();

    assert.ok(timers <= 1, `${timers} timers of the reader run`);
  });

  it('ends the close and its second call when the signal of the first call aborts the reads for the owner of its ' +
    'store, which keep getting a transient refusal', async () => {
    const retryInterval = 20;
    const topic = new FakeTopic(10n);
    topic.assigned = [1];
    const client = topic.client();
    const failures: unknown[] = [];
    const timers = await timersLeftBy(async () => {
      // Only the close counts the batch and stores it, and no interval timer runs.
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
        options: { maxWait: 100, commit: CommitPolicy.Auto({ every: CommitEvery.Batch }) }, retryInterval
      });
      assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
      topic.ownerGeneration += 1n;
      topic.ownerInstalledAt = topic.metadataOp + 1n;
      topic.refused.set(1, TRANSIENT_NOT_ACCEPTED);
      const first = new AbortController();
      const second = new AbortController();
      const end = (closing: Promise<void>) => closing.then(() => undefined, (error: unknown) => error)
        .then((failure) => { failures.push(failure); });
      const closed = Promise.all([end(reader.close({ signal: first.signal })),
        end(reader.close({ signal: second.signal }))]);
      try {
        await waitFor(() => probesOf(topic, 1).length > 1, 'the read for the owner did not go again');
        second.abort();
        const seen = probesOf(topic, 1).length;
        await waitFor(() => probesOf(topic, 1).length > seen, 'the signal of the second call ended the close');
        assert.deepEqual(failures, []);
        first.abort();
        await waitFor(() => failures.length === 2, 'the close did not end');
      } finally {
        // A close that did not end reads for the owner until the refusal ends.
        topic.refused.delete(1);
        await closed;
      }
    });
    const probes = probesOf(topic, 1).length;
    await new Promise((resolve) => setTimeout(resolve, 2 * retryInterval));
    const codes = failures.map((failure) => (failure as { errorCode?: number } | undefined)?.errorCode);
    assert.deepEqual([codes, topic.leaves, probesOf(topic, 1).length, timers],
      [[CONSUMER_GROUP_PARTITION_NOT_OWNED, CONSUMER_GROUP_PARTITION_NOT_OWNED], 1, probes, 0]);
  });

  it('ends the close without a signal at its budget with the refusal of its store, whose reads for the owner keep ' +
    'getting a transient refusal, and still leaves the group', async () => {
    const retryInterval = 20;
    const topic = new FakeTopic(10n);
    topic.assigned = [1];
    const client = topic.client();
    // Only the close counts the batch and stores it.
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.Auto({ every: CommitEvery.Batch }) }, retryInterval
    });
    assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
    topic.ownerGeneration += 1n;
    topic.ownerInstalledAt = topic.metadataOp + 1n;
    topic.refused.set(1, TRANSIENT_NOT_ACCEPTED);
    const started = Date.now();
    let ended: number | undefined;
    let failure: unknown;
    const closed = reader.close().catch((error: unknown) => { failure = error; })
      .finally(() => { ended = Date.now(); });
    try {
      await waitFor(() => ended !== undefined, 'the close did not end at its budget', CLOSE_BUDGET_MS + WAIT_MS);
    } finally {
      // A close that did not end reads for the owner until the refusal ends.
      topic.refused.delete(1);
      await closed;
    }
    const probes = probesOf(topic, 1).length;
    await new Promise((resolve) => setTimeout(resolve, 2 * retryInterval));
    assert.ok(ended! - started >= CLOSE_BUDGET_MS - TIMER_SLACK_MS, `the close ended ${ended! - started} ms after it ` +
      'started, before its budget');
    assert.deepEqual([(failure as { errorCode?: number } | undefined)?.errorCode, topic.leaves, probes > 1,
      probesOf(topic, 1).length], [CONSUMER_GROUP_PARTITION_NOT_OWNED, 1, true, probes]);
  });

  it('ends the close at an abort during the reads for the owner of the refused store of one partition, and stores ' +
    'the next partition once without such a read', async () => {
    const retryInterval = 20;
    const topic = new FakeTopic(10n);
    topic.assigned = [1, 2];
    const client = topic.client();
    // Only the close stores the delivered batches.
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, retryInterval,
      options: { maxWait: 100, commit: CommitPolicy.Auto({ interval: PARKED_READ_WAIT_MS, after: CommitAfter.Delivery }) }
    });
    const delivered = new Set<number>();
    let closed: Promise<void> | undefined;
    let ended = false;
    let failure: unknown;
    try {
      while (delivered.size < 2) delivered.add((await reader.next({ signal: AbortSignal.timeout(2_000) })).partitionId);
      // A rebalance by another member installed the owner again, so the server refuses the stores of both partitions.
      topic.ownerGeneration += 1n;
      topic.ownerInstalledAt = topic.metadataOp + 1n;
      topic.refused.set(1, TRANSIENT_NOT_ACCEPTED);
      const aborting = new AbortController();
      closed = reader.close({ signal: aborting.signal })
        .catch((error: unknown) => { failure = error; }).finally(() => { ended = true; });
      await waitFor(() => probesOf(topic, 1).length > 1, 'the read for the owner of partition 1 did not go again');
      aborting.abort();
      await waitFor(() => ended, 'the close did not end');
    } finally {
      // A close that did not end reads for the owner until the refusal ends.
      topic.refused.delete(1);
      await (closed ?? reader.close());
    }
    const probes = probesOf(topic, 1).length;
    await new Promise((resolve) => setTimeout(resolve, 2 * retryInterval));
    assert.deepEqual([(failure as { errorCode?: number } | undefined)?.errorCode, probesOf(topic, 1).length,
      probesOf(topic, 2).length, topic.offsetStores, topic.leaves],
      [CONSUMER_GROUP_PARTITION_NOT_OWNED, probes, 0, [1, 2], 1]);
  });

  it('ends the close with the refusal of a store when its signal aborts while the read for the owner is in flight, ' +
    'so the reply that then names the owner writes nothing, and stores the next partition once', async () => {
    const topic = new FakeTopic(10n);
    topic.assigned = [1, 2];
    const client = topic.client();
    // Only the close stores the delivered batches.
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, retryInterval: 20,
      options: { maxWait: 100, commit: CommitPolicy.Auto({ interval: PARKED_READ_WAIT_MS, after: CommitAfter.Delivery }) }
    });
    const delivered = new Set<number>();
    let closed: Promise<void> | undefined;
    let failure: unknown;
    try {
      while (delivered.size < 2) delivered.add((await reader.next({ signal: AbortSignal.timeout(2_000) })).partitionId);
      // A rebalance by another member installed the owner again, so the server refuses the stores of both partitions.
      topic.ownerGeneration += 1n;
      topic.ownerInstalledAt = topic.metadataOp + 1n;
      topic.holdProbes = true;
      const aborting = new AbortController();
      closed = reader.close({ signal: aborting.signal }).catch((error: unknown) => { failure = error; });
      await waitFor(() => topic.heldFetches.length === 1, 'the read for the owner of partition 1 did not start');
      aborting.abort();
      topic.holdProbes = false;
      topic.heldFetches.shift()!();
      await closed;
    } finally {
      topic.holdProbes = false;
      for (const release of topic.heldFetches) release();
      await (closed ?? reader.close());
    }
    assert.deepEqual([(failure as { errorCode?: number } | undefined)?.errorCode, probesOf(topic, 1).length,
      probesOf(topic, 2).length, topic.offsetStores, topic.leaves],
      [CONSUMER_GROUP_PARTITION_NOT_OWNED, 1, 0, [1, 2], 1]);
  });

  for (const [ending, stop] of [
    ['its signal aborts', ({ aborting }) => aborting.abort()],
    ['the close starts', ({ reader }) => { reader.close().catch(() => undefined); }]
  ] as [string, (ends: { aborting: AbortController, reader: PartitionReader }) => void][]) {
    it(`ends the reads for the owner of a commit with a signal when ${ending}, so the commit fails with its refusal`,
      async () => {
        const retryInterval = 20;
        const topic = new FakeTopic(10n);
        topic.assigned = [1];
        const client = topic.client();
        const reader = await PartitionReader.open(async () => client, {
          streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
          options: { maxWait: 100, commit: CommitPolicy.Manual }, retryInterval
        });
        let ended = false;
        let failure: unknown;
        let committed = Promise.resolve();
        try {
          const checkpoint = (await reader.next({ signal: AbortSignal.timeout(2_000) })).checkpoint()!;
          topic.ownerGeneration += 1n;
          topic.ownerInstalledAt = topic.metadataOp + 1n;
          topic.refused.set(1, TRANSIENT_NOT_ACCEPTED);
          const aborting = new AbortController();
          committed = reader.commit(checkpoint, { signal: aborting.signal })
            .catch((error: unknown) => { failure = error; }).finally(() => { ended = true; });
          await waitFor(() => probesOf(topic, 1).length > 1, 'the read for the owner did not go again');
          stop({ aborting, reader });
          await waitFor(() => ended, 'the commit did not end');
          const probes = probesOf(topic, 1).length;
          await new Promise((resolve) => setTimeout(resolve, 2 * retryInterval));
          assert.deepEqual([(failure as { errorCode?: number } | undefined)?.errorCode, probesOf(topic, 1).length],
            [CONSUMER_GROUP_PARTITION_NOT_OWNED, probes]);
        } finally {
          // A commit that did not end reads for the owner until the refusal ends.
          topic.refused.delete(1);
          await committed;
          await reader.close();
        }
      });
  }

  it('fails a commit with the refusal of its store when its signal aborts while the read for the owner is in flight, ' +
    'so the reply that then names the owner writes nothing', async () => {
    const topic = new FakeTopic(10n);
    topic.assigned = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.Manual }, retryInterval: 20
    });
    let committed: Promise<void> | undefined;
    let failure: unknown;
    try {
      const checkpoint = (await reader.next({ signal: AbortSignal.timeout(2_000) })).checkpoint()!;
      topic.ownerGeneration += 1n;
      topic.ownerInstalledAt = topic.metadataOp + 1n;
      topic.holdProbes = true;
      const aborting = new AbortController();
      committed = reader.commit(checkpoint, { signal: aborting.signal }).catch((error: unknown) => { failure = error; });
      await waitFor(() => topic.heldFetches.length === 1, 'the read for the owner did not start');
      aborting.abort();
      topic.holdProbes = false;
      topic.heldFetches.shift()!();
      await committed;
    } finally {
      topic.holdProbes = false;
      for (const release of topic.heldFetches) release();
      await committed;
      await reader.close();
    }
    assert.deepEqual([(failure as { errorCode?: number } | undefined)?.errorCode, probesOf(topic, 1).length,
      topic.offsetStores], [CONSUMER_GROUP_PARTITION_NOT_OWNED, 1, [1]]);
  });

  for (const [sent, client] of [
    ['a position of an earlier owner', (topic: FakeTopic) => topic.client()],
    ['a position without a context', (topic: FakeTopic) => {
      // Without readiness groups the partition polls alone, so it reads its offset start without a context.
      const client: Partial<RawClient> = topic.client();
      delete client.readinessGroups;
      return client as RawClient;
    }]
  ] as const) {
    it(`stores an on read seek of a group partition to the skip of a message that does not decode, read from ${sent}`,
      async () => {
        const topic = new FakeTopic(10n);
        topic.assigned = [1];
        // A read of the partition from offset 0 does not decode either.
        topic.undecodable.add(0n);
        // A rebalance by another member installs the owner again at an op that no reply to this client names, while
        // the partition reads the message alone.
        topic.beforeUncommittedPoll = () => {
          topic.ownerGeneration += 1n;
          topic.ownerInstalledAt = topic.metadataOp + 1n;
        };
        const opened = client(topic);
        const reader = await PartitionReader.open(async () => opened, {
          streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, start: PollingStrategy.Offset(0n),
          options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
        });
        let closeFailure: unknown;
        try {
          const error = await reader.next({ signal: AbortSignal.timeout(2_000) })
            .then(() => undefined, (failure: unknown) => failure);
          assert.ok(error instanceof ReadError && error.skip !== undefined, String(error));
          await reader.seek(1, error.skip);
          // No read of this session served the partition yet, so the store names owner 0 and the op of the reply.
          assert.deepEqual([topic.stored.get(1), topic.writeContexts], [0n, [{ ...REINSTALLED, ownerGeneration: 0n }]]);
          assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [1n, 2n]);
        } finally {
          // A close that fails would replace the failure of the seek.
          closeFailure = await reader.close().then(() => undefined, (failure: unknown) => failure);
        }
        assert.equal(closeFailure, undefined);
      });
  }

  for (const commit of [CommitPolicy.Manual,
    CommitPolicy.Auto({ interval: PARKED_READ_WAIT_MS, after: CommitAfter.Delivery })]) {
    it(`writes no ${commit.kind} checkpoint from before a leave of a group partition that came back`, async () => {
      const topic = new FakeTopic(10n);
      topic.assigned = [1];
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
        options: { maxWait: 100, commit }, retryInterval: 20
      });
      try {
        const checkpoint = (await reader.next({ signal: AbortSignal.timeout(2_000) })).checkpoint()!;
        topic.assigned = [];
        while (reader.partitions().includes(1)) await once(topic.emitter, 'synced');
        topic.assigned = [1];
        while (!reader.partitions().includes(1)) await once(topic.emitter, 'synced');
        // Another member moved the offset while the partition was away.
        topic.stored.set(1, 7n);
        if (commit.kind === 'manual') await reader.commit(checkpoint);
      } finally {
        await reader.close();
      }
      assert.equal(topic.stored.get(1), 7n);
    });
  }

  it('stores no counted checkpoint of a group partition that left and came back while an automatic store of ' +
    'another partition waited', async () => {
    const topic = new FakeTopic(10n);
    topic.assigned = [1, 2];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, retryInterval: 20,
      options: { maxWait: 100, commit: CommitPolicy.Auto({ every: CommitEvery.Message, after: CommitAfter.Marked }) }
    });
    try {
      const checkpoints = [(await nextOf(reader, 1)).checkpoint()!, (await nextOf(reader, 2)).checkpoint()!];
      topic.holdOffsetWrites = true;
      // Both marks come before the store loop runs, so one pass of it holds both checkpoints.
      for (const checkpoint of checkpoints) reader.mark(checkpoint);
      await waitFor(() => topic.heldOffsetWrites.length === 1, 'the store of partition 1 did not start');
      topic.assigned = [1];
      while (reader.partitions().includes(2)) await once(topic.emitter, 'synced');
      // The worker waits for the store of the partition that left, so that store ends before the next sync.
      await waitFor(() => topic.heldOffsetWrites.length === 2, 'the store of the partition that left did not start');
      topic.heldOffsetWrites.pop()!();
      topic.assigned = [1, 2];
      while (!reader.partitions().includes(2)) await once(topic.emitter, 'synced');
      // Another member moved the offset while the partition was away.
      topic.stored.set(2, 7n);
      topic.holdOffsetWrites = false;
      topic.heldOffsetWrites.shift()!();
    } finally {
      await reader.close();
    }
    assert.deepEqual([topic.stored.get(2), topic.offsetStores], [7n, [1, 2]]);
  });

  it('reads no batch on read after the only waiting next() aborts, so the server stores none that nobody takes',
    async () => {
      const topic = new FakeTopic(10n);
      const { reader, held } = await openWithHeld(topic);
      try {
        const aborting = new AbortController();
        const aborted = reader.next({ signal: aborting.signal });
        await waitFor(() => held.sent === 1, 'the check did not start');
        aborting.abort();
        await assert.rejects(aborted);
        held.release();
        // The check and a read that it lets start run on promises only, so one turn of the event loop shows that read.
        await new Promise(setImmediate);
        assert.deepEqual([topic.reads.filter(({ autocommit }) => autocommit).length, topic.stored.get(1)], [0, undefined]);
      } finally {
        await reader.close();
      }
    });

  it('drops a check that ends after a session reset, and checks again', async () => {
    const topic = new FakeTopic(10n);
    topic.stored.set(1, 3n);
    const { reader, held } = await openWithHeld(topic);
    try {
      const first = reader.next({ signal: AbortSignal.timeout(2_000) });
      await waitFor(() => held.sent === 1, 'the check did not start');
      topic.emitter.emit('sessionReset');
      held.release();
      assert.deepEqual([offsets([await first]), held.sent], [[4n, 5n], 2]);
    } finally {
      await reader.close();
    }
  });

  it('drops a timestamp check that ends while an on read seek writes, and checks again after the write', async () => {
    const topic = new FakeTopic(10n);
    const { reader, held } = await openWithHeld(topic, PollingStrategy.Timestamp(4n));
    try {
      const first = reader.next({ signal: AbortSignal.timeout(2_000) });
      await waitFor(() => held.sent === 1, 'the check did not start');
      topic.holdOffsetWrites = true;
      topic.uncertainWriteLands = true;
      const seek = reader.seek(1, PollingStrategy.Offset(8n));
      await waitFor(() => topic.heldOffsetWrites.length === 1, 'the store of the seek did not start');
      // The client does not order a timestamp check after a store, so the check ends while the seek writes.
      held.release();
      await waitFor(() => topic.reads.length === 1, 'the check did not reach the server');
      await new Promise((resolve) => setTimeout(resolve, 50));
      topic.holdOffsetWrites = false;
      topic.heldOffsetWrites.shift()!();
      await assert.rejects(seek, OperationUncertainError);
      const delivered = offsets([await first]);
      const checks = topic.reads.filter(({ kind }) => kind === PollingStrategyKind.Timestamp).length;
      assert.deepEqual([delivered, checks], [[4n, 5n], 2]);
    } finally {
      topic.holdOffsetWrites = false;
      for (const release of topic.heldOffsetWrites) release();
      await reader.close();
    }
  });

  it('drops a start lookup that ends after an on read seek with an unknown outcome ended that lookup', async () => {
    const topic = new FakeTopic(10n);
    const { reader, held } = await openWithHeld(topic, PollingStrategy.Offset(5n), COMMAND_CODE.GetOffset);
    try {
      const first = reader.next({ signal: AbortSignal.timeout(2_000) });
      await waitFor(() => held.sent === 1, 'the start lookup did not start');
      topic.uncertainWriteLands = true;
      await assert.rejects(reader.seek(1, PollingStrategy.Offset(3n)), OperationUncertainError);
      // The server answers the lookup after the store landed, so it finds offset 2 before the start at 5.
      held.release();
      assert.deepEqual(offsets([await first]), [5n, 6n]);
    } finally {
      await reader.close();
    }
  });

  it('removes its listener from the signal of the caller when a wait of next() ends', async () => {
    const topic = new FakeTopic(10n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    const caller = new AbortController();
    try {
      // Under OnRead each call waits, since the reader reads only while next() waits.
      for (let call = 0; call < 3; call += 1) await reader.next({ signal: caller.signal });
      assert.deepEqual(getEventListeners(caller.signal, 'abort'), []);
    } finally {
      await reader.close();
    }
  });

  for (const fails of [false, true]) {
    it(`removes its session reset listener from the client when a close ${fails ? 'fails' : 'succeeds'}`, async () => {
      const topic = new FakeTopic(10n);
      const before = topic.emitter.listenerCount('sessionReset');
      const reader = await open(topic, [1],
        { commit: CommitPolicy.Auto({ interval: PARKED_READ_WAIT_MS, after: CommitAfter.Delivery }) });
      assert.equal(topic.emitter.listenerCount('sessionReset'), before + 1);
      assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
      if (fails) topic.storeFailure = new Error('store refused');
      await (fails ? assert.rejects(reader.close(), { message: 'store refused' }) : reader.close());
      assert.equal(topic.emitter.listenerCount('sessionReset'), before);
    });
  }

  it('releases a close store with an unknown outcome, so a later reader of the client writes the offset', async () => {
    const topic = new FakeTopic(8n);
    topic.stored.set(1, 4n);
    topic.loseNextCommittingRead = true;
    const cancellation = new AbortController();
    topic.onLostCommittingRead = () => {
      topic.uncertainWriteLands = true;
      cancellation.abort(new Error('stop before delivery'));
    };
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    await assert.rejects(reader.next({ signal: cancellation.signal }), /stop before delivery/);
    await assert.rejects(reader.close(), OperationUncertainError);
    assert.deepEqual([topic.stored.get(1), topic.abandoned], [4n, [1n, UNCERTAIN_WRITE_OPERATION]]);

    const later = await open(topic, [1], { commit: CommitPolicy.OnRead });
    await later.seek(1, PollingStrategy.Offset(2n));
    await later.close();
    assert.equal(topic.stored.get(1), 1n);
  });

  it('releases a commit with an unknown outcome, so a retry of the commit stores the checkpoint', async () => {
    const topic = new FakeTopic(10n);
    const reader = await open(topic, [1]);
    try {
      const checkpoint = (await reader.next()).checkpoint()!;
      topic.uncertainWriteLands = false;
      await assert.rejects(reader.commit(checkpoint), OperationUncertainError);
      assert.deepEqual([topic.abandoned, topic.stored.get(1)], [[UNCERTAIN_WRITE_OPERATION], undefined]);
      await reader.commit(checkpoint);
      assert.equal(topic.stored.get(1), 1n);
    } finally {
      await reader.close();
    }
  });

  it('releases an automatic store with an unknown outcome, so the next store of its partition lands', async () => {
    const topic = new FakeTopic(10n);
    const reader = await open(topic, [1],
      { commit: CommitPolicy.Auto({ every: CommitEvery.Batch, after: CommitAfter.Delivery }) });
    topic.uncertainWriteLands = false;
    try {
      assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
      await waitFor(() => topic.abandoned.length > 0, 'the store with an unknown outcome was not released');
      assert.deepEqual(offsets([await reader.next()]), [2n, 3n]);
    } finally {
      await reader.close();
    }
    assert.deepEqual([topic.abandoned, topic.stored.get(1)], [[UNCERTAIN_WRITE_OPERATION], 3n]);
  });

  it('releases the automatic store of a close with an unknown outcome, so a later reader of the client writes the ' +
    'offset', async () => {
    const topic = new FakeTopic(10n);
    const reader = await open(topic, [1],
      { commit: CommitPolicy.Auto({ interval: PARKED_READ_WAIT_MS, after: CommitAfter.Delivery }) });
    assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
    topic.uncertainWriteLands = true;
    await assert.rejects(reader.close(), OperationUncertainError);
    assert.deepEqual([topic.stored.get(1), topic.abandoned], [1n, [UNCERTAIN_WRITE_OPERATION]]);

    const later = await open(topic, [1], { commit: CommitPolicy.OnRead });
    await later.seek(1, PollingStrategy.Offset(4n));
    await later.close();
    assert.equal(topic.stored.get(1), 3n);
  });

  it('releases the automatic store of a partition that leaves the assignment with an unknown outcome', async () => {
    const topic = new FakeTopic(10n);
    topic.assigned = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, retryInterval: 20,
      options: { maxWait: 100, commit: CommitPolicy.Auto({ interval: PARKED_READ_WAIT_MS, after: CommitAfter.Delivery }) }
    });
    try {
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [0n, 1n]);
      topic.uncertainWriteLands = true;
      topic.assigned = [];
      while (reader.partitions().includes(1)) await once(topic.emitter, 'synced');
      await waitFor(() => topic.abandoned.length > 0, 'the store with an unknown outcome was not released');
      topic.assigned = [1];
      while (!reader.partitions().includes(1)) await once(topic.emitter, 'synced');
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [2n, 3n]);
    } finally {
      await reader.close();
    }
    assert.deepEqual([topic.abandoned, topic.stored.get(1)], [[UNCERTAIN_WRITE_OPERATION], 3n]);
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

  it('makes an on read seek wait for a committing read of an earlier assignment of its partition', async () => {
    const topic = new FakeTopic(10n);
    topic.assigned = [1];
    topic.holdFetches = true;
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
    });
    try {
      const waiting = new AbortController();
      const pending = reader.next({ signal: waiting.signal }).then(() => undefined, (failure: unknown) => failure);
      await waitFor(() => topic.heldFetches.length === 1, 'the committing read did not start');
      waiting.abort();
      assert.ok((await pending) instanceof Error);
      topic.assigned = [];
      while (reader.partitions().includes(1)) await once(topic.emitter, 'synced');
      // The new assignment waits for its group, so only the read of the earlier one is in flight.
      topic.routeLookupsHang = true;
      topic.assigned = [1];
      while (!reader.partitions().includes(1)) await once(topic.emitter, 'synced');
      const seek = reader.seek(1, PollingStrategy.Offset(0n));
      await new Promise((resolve) => setTimeout(resolve, 50));
      assert.deepEqual(topic.offsetLookups, [], 'the seek looked up the offset while a committing read was in flight');
      topic.holdFetches = false;
      topic.heldFetches.shift()!();
      await seek;
      assert.deepEqual([topic.offsetLookups, topic.offsetDeletes, topic.stored.has(1)], [[1], [1], false]);
    } finally {
      topic.holdFetches = false;
      for (const release of topic.heldFetches) release();
      await reader.close();
    }
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

  it('fails a seek that starts after the close, so no write lands beside the rewind', async () => {
    const topic = new FakeTopic(10n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
    const closing = reader.close();
    await assert.rejects(reader.seek(1, PollingStrategy.Offset(6n)), /closed/);
    await closing;

    assert.deepEqual([topic.stored.get(1), topic.offsetLookups, topic.offsetDeletes], [1n, [], []]);
  });

  it('fails a waiting next() at once when the close starts, while the close still waits for a read', async () => {
    const topic = new FakeTopic(10n);
    topic.holdFetches = true;
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    let ended: Error | undefined;
    const pending = reader.next().catch((error: Error) => { ended = error; });
    await waitFor(() => topic.heldFetches.length === 1, 'the committing read did not start');
    let closed = false;
    const closing = reader.close().then(() => { closed = true; });
    try {
      await waitFor(() => ended !== undefined, 'next() kept waiting while the close ran');
      assert.match(ended!.message, /closed/);
      assert.equal(closed, false, 'the close ended before the held read');
    } finally {
      topic.holdFetches = false;
      for (const release of topic.heldFetches) release();
      await Promise.all([pending, closing]);
    }
  });

  for (const [name, sought] of [['an offset', PollingStrategy.Offset(2n)], ['offset 0', PollingStrategy.Offset(0n)],
    ['a timestamp', PollingStrategy.Timestamp(2n)]] as const) {
    it(`starts no read of a partition while the write of an on read seek to ${name} waits, then reads from there`,
      async () => {
        const topic = new FakeTopic(10n);
        const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
        try {
          // A committing read by offset from 6 that crossed the write would store an offset past the sought one.
          await reader.seek(1, PollingStrategy.Offset(6n));
          topic.holdOffsetWrites = true;
          const seek = reader.seek(1, sought);
          const pending = reader.next({ signal: AbortSignal.timeout(2_000) });
          await waitFor(() => topic.heldOffsetWrites.length === 1, 'the write of the seek did not start');
          await new Promise((resolve) => setTimeout(resolve, 50));
          // Only the probe of a timestamp seek reads, before the write.
          const probes = sought.kind === PollingStrategyKind.Timestamp ? [[PollingStrategyKind.Timestamp, false]] : [];
          assert.deepEqual(topic.reads.map(({ kind, autocommit }) => [kind, autocommit]), probes,
            'a read started while the seek wrote');
          topic.holdOffsetWrites = false;
          topic.heldOffsetWrites.shift()!();
          await seek;
          assert.deepEqual(offsets([await pending]), [sought.value, sought.value + 1n]);
          const first = topic.reads.find(({ autocommit }) => autocommit)!;
          assert.deepEqual([first.kind, first.value, topic.stored.get(1)],
            [PollingStrategyKind.Offset, sought.value, sought.value + 1n]);
        } finally {
          topic.holdOffsetWrites = false;
          for (const release of topic.heldOffsetWrites) release();
          await reader.close();
        }
      });
  }

  for (const [name, sought, failure] of [['store', 6n, 'storeFailure'], ['delete', 0n, 'deleteFailure']] as const) {
    it(`starts no read while the ${name} of an on read seek waits, and reads on from where it was when it fails`,
      async () => {
        const topic = new FakeTopic(10n);
        const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
        try {
          assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
          topic.holdOffsetWrites = true;
          topic[failure] = new Error(`${name} refused`);
          const reads = topic.reads.length;
          const seek = reader.seek(1, PollingStrategy.Offset(sought));
          const pending = reader.next({ signal: AbortSignal.timeout(2_000) });
          await waitFor(() => topic.heldOffsetWrites.length === 1, `the ${name} of the seek did not start`);
          await new Promise((resolve) => setTimeout(resolve, 50));
          assert.equal(topic.reads.length, reads, 'a read started while the seek wrote');
          topic.holdOffsetWrites = false;
          topic.heldOffsetWrites.shift()!();
          await assert.rejects(seek, { message: `${name} refused` });
          topic[failure] = undefined;
          assert.deepEqual(offsets([await pending]), [2n, 3n]);
          assert.equal(topic.stored.get(1), 3n);
        } finally {
          topic.holdOffsetWrites = false;
          for (const release of topic.heldOffsetWrites) release();
          await reader.close();
        }
      });
  }

  it('makes a second on read seek of a partition wait until the write of the first one ends', async () => {
    const topic = new FakeTopic(10n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    try {
      assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
      topic.holdOffsetWrites = true;
      const first = reader.seek(1, PollingStrategy.Offset(6n));
      const second = reader.seek(1, PollingStrategy.Offset(0n));
      await waitFor(() => topic.heldOffsetWrites.length === 1, 'the store of the first seek did not start');
      await new Promise((resolve) => setTimeout(resolve, 50));
      assert.deepEqual(topic.offsetLookups, [], 'the second seek looked up the offset while the first one stored');
      topic.holdOffsetWrites = false;
      topic.heldOffsetWrites.shift()!();
      await Promise.all([first, second]);
      assert.deepEqual([topic.offsetLookups, topic.offsetDeletes, topic.stored.has(1)], [[1], [1], false]);
      assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
    } finally {
      topic.holdOffsetWrites = false;
      for (const release of topic.heldOffsetWrites) release();
      await reader.close();
    }
  });

  for (const [name, start] of [['offset 0', PollingStrategy.Offset(0n)], ['first', PollingStrategy.First],
    ['a timestamp', PollingStrategy.Timestamp(0n)], ['last', PollingStrategy.Last]] as const) {
    it(`resumes an on read start at ${name} after the stored offset, without a read from the start`, async () => {
      const topic = new FakeTopic(10n);
      topic.stored.set(1, 5n);
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead }, start);
      try {
        assert.deepEqual(offsets([await reader.next()]), [6n, 7n]);
      } finally {
        await reader.close();
      }
      assert.ok(topic.reads.every(({ kind }) => kind === PollingStrategyKind.Next), 'a read started from the start');
    });
  }

  it('reads an on read start at an offset from there when the server holds no offset of the partition', async () => {
    const topic = new FakeTopic(10n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead }, PollingStrategy.Offset(4n));
    try {
      assert.deepEqual(offsets([await reader.next()]), [4n, 5n]);
    } finally {
      await reader.close();
    }
    assert.deepEqual([topic.offsetLookups, topic.stored.get(1)], [[1], 5n]);
  });

  it('reads nothing while the lookup of the stored offset fails, and resumes after it once a lookup passes', async () => {
    const topic = new FakeTopic(10n);
    topic.stored.set(1, 5n);
    topic.lookupFailure = responseError(COMMAND_CODE.GetOffset, TRANSIENT_NOT_ACCEPTED);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead }, PollingStrategy.Offset(0n));
    try {
      await assert.rejects(reader.next({ signal: AbortSignal.timeout(200) }));
      assert.deepEqual(topic.reads, []);
      assert.ok(topic.offsetLookups.length > 1, 'the failed lookup was not retried');
      topic.lookupFailure = undefined;
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [6n, 7n]);
    } finally {
      await reader.close();
    }
  });

  it('resumes an on read group start at an offset after the stored offset at each assignment', async () => {
    const topic = new FakeTopic(12n);
    topic.assigned = [1];
    topic.stored.set(1, 3n);
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, start: PollingStrategy.Offset(0n),
      options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
    });
    try {
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [4n, 5n]);
      topic.assigned = [];
      while (reader.partitions().includes(1)) await once(topic.emitter, 'synced');
      // Another member read on while this one did not own the partition.
      topic.stored.set(1, 7n);
      topic.assigned = [1];
      while (!reader.partitions().includes(1)) await once(topic.emitter, 'synced');
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [8n, 9n]);
    } finally {
      await reader.close();
    }
    assert.ok(topic.reads.every(({ kind }) => kind === PollingStrategyKind.Next), 'a read started from the start');
  });

  for (const start of [0n, 4n]) {
    it(`moves the offset back before the undelivered batch of an on read start at offset ${start} on close`, async () => {
      const topic = new FakeTopic(8n);
      const reader = await open(topic, [1, 3], { commit: CommitPolicy.OnRead }, PollingStrategy.Offset(start));
      const taken = await reader.next();
      await reader.close();

      const undelivered = taken.partitionId === 1 ? 3 : 1;
      assert.deepEqual(topic.fetches[0], [1, 3]);
      assert.deepEqual([topic.stored.get(taken.partitionId), topic.stored.get(undelivered), topic.offsetDeletes],
        [start + 1n, start === 0n ? undefined : start - 1n, start === 0n ? [undelivered] : []]);
      const resumed = await open(topic, [undelivered], { commit: CommitPolicy.OnRead });
      try {
        assert.deepEqual(offsets([await resumed.next()]), [start, start + 1n]);
      } finally {
        await resumed.close();
      }
    });
  }

  it('sends no delete on close before offset 0 while the server holds no offset', async () => {
    const topic = new FakeTopic(4n);
    topic.refused.set(1, TRANSIENT_NOT_ACCEPTED);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    await assert.rejects(reader.next({ signal: AbortSignal.timeout(200) }));
    await reader.close();

    assert.deepEqual([topic.offsetLookups, topic.offsetDeletes], [[1], []]);
  });

  for (const [time, storedAfterSeek] of [[0n, undefined], [4n, 3n]] as const) {
    it(`reads on read from the first message at sought timestamp ${time} to the end past a stored offset ahead`,
      async () => {
        const topic = new FakeTopic(10n);
        topic.stored.set(1, 7n);
        const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
        try {
          await reader.seek(1, PollingStrategy.Timestamp(time));
          assert.equal(topic.stored.get(1), storedAfterSeek);
          const batches: PolledBatch[] = [];
          for (let batch = await reader.nextUntilEnd(); batch; batch = await reader.nextUntilEnd()) batches.push(batch);
          assert.deepEqual(offsets(batches),
            Array.from({ length: Number(topic.limit - time) }, (_, index) => time + BigInt(index)));
        } finally {
          await reader.close();
        }
      });
  }

  it('probes a sought timestamp again after the fail-fast reply, so an on read seek still stores the offset before ' +
    'its first message', async () => {
    const topic = new FakeTopic(10n);
    topic.stored.set(1, 7n);
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer, partitions: [1], options: { maxWait: 100, commit: CommitPolicy.OnRead },
      retryInterval: UNSERVED_RETRY_MS
    });
    topic.unservedPolls.push(EMPTY_PARTITION_CONTEXT);
    const batches: PolledBatch[] = [];
    let storedAfterSeek: bigint | undefined;
    try {
      await reader.seek(1, PollingStrategy.Timestamp(4n));
      storedAfterSeek = topic.stored.get(1);
      for (let batch = await reader.nextUntilEnd(); batch; batch = await reader.nextUntilEnd()) batches.push(batch);
    } finally {
      await reader.close();
    }

    assert.deepEqual([storedAfterSeek, offsets(batches)], [3n, [4n, 5n, 6n, 7n, 8n, 9n]]);
    const [unserved, again] = topic.reads.filter(({ kind }) => kind === PollingStrategyKind.Timestamp);
    assert.ok(again.at - unserved.at >= UNSERVED_RETRY_MS - TIMER_SLACK_MS,
      `probed again ${again.at - unserved.at} ms after the fail-fast reply`);
  });

  it('keeps the stored offset on close after an on read seek to Next whose read found nothing', async () => {
    const topic = new FakeTopic(2n);
    const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
    assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
    await reader.seek(1, PollingStrategy.Next);
    await assert.rejects(reader.next({ signal: AbortSignal.timeout(200) }));
    await reader.close();

    assert.deepEqual([topic.stored.get(1), topic.offsetDeletes], [1n, []]);
  });

  for (const [name, sought] of [['last', PollingStrategy.Last], ['a timestamp', PollingStrategy.Timestamp(4n)]] as const) {
    it(`sends no offset command on close after an on read seek to ${name} on an empty partition`, async () => {
      const topic = new FakeTopic(0n);
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead });
      await reader.seek(1, sought);
      await assert.rejects(reader.next({ signal: AbortSignal.timeout(300) }));
      await reader.close();

      assert.deepEqual([topic.offsetLookups, topic.offsetDeletes, [...topic.stored]], [[], [], []]);
    });
  }

  for (const [name, start, sought, rewound] of [
    ['an offset start at 0', PollingStrategy.Offset(0n), undefined, undefined],
    ['an offset start at 4', PollingStrategy.Offset(4n), undefined, 3n],
    ['a seek to offset 0', undefined, PollingStrategy.Offset(0n), undefined],
    ['a seek to offset 4', undefined, PollingStrategy.Offset(4n), 3n],
    ['a last start', PollingStrategy.Last, undefined, 3n],
    ['a timestamp start', PollingStrategy.Timestamp(4n), undefined, 3n]
  ] as const) {
    it(`rewinds a lost first on read reply after ${name} on close before any delivery`, async () => {
      const topic = new FakeTopic(8n);
      topic.loseNextCommittingRead = true;
      const cancellation = new AbortController();
      let storedByLostRead: bigint | undefined;
      topic.onLostCommittingRead = () => {
        storedByLostRead = topic.stored.get(1);
        cancellation.abort(new Error('stop before delivery'));
      };
      // With a count of 4, a Last start resolves to offset 4 of the 8 messages.
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead, count: 4 }, start);
      if (sought) await reader.seek(1, sought);
      await assert.rejects(reader.next({ signal: cancellation.signal }), /stop before delivery/);
      await reader.close();

      assert.ok(storedByLostRead !== undefined, 'the lost read stored nothing');
      assert.equal(topic.stored.get(1), rewound);
    });
  }

  for (const [name, sought, limitAtSeek] of [
    ['last', PollingStrategy.Last, 10n],
    ['a timestamp whose first message came after the seek', PollingStrategy.Timestamp(6n), 4n]
  ] as const) {
    it(`rewinds a lost first on read reply after a seek to ${name} on close, since a probe resolves the position`,
      async () => {
        const topic = new FakeTopic(limitAtSeek);
        const reader = await open(topic, [1], { commit: CommitPolicy.OnRead, count: 4 });
        assert.deepEqual(offsets([await reader.next()]), [0n, 1n]);
        await reader.seek(1, sought);
        topic.limit = 10n;
        topic.loseNextCommittingRead = true;
        const cancellation = new AbortController();
        topic.onLostCommittingRead = () => cancellation.abort(new Error('stop before delivery'));
        await assert.rejects(reader.next({ signal: cancellation.signal }), /stop before delivery/);
        await reader.close();

        // Both positions resolve to offset 6, and the lost read stored 6 and 7.
        const committedKinds = topic.reads.filter(({ autocommit }) => autocommit).map(({ kind }) => kind);
        assert.deepEqual([topic.stored.get(1), committedKinds.every((kind) => kind === PollingStrategyKind.Offset ||
          kind === PollingStrategyKind.Next)], [5n, true]);
      });
  }

  for (const [name, start, lookedUp] of [
    ['a timestamp start whose lookup found none', PollingStrategy.Timestamp(4n), true],
    ['an offset start whose lookup found none', PollingStrategy.Offset(5n), true],
    ['an offset start before its lookup, without a stored offset', PollingStrategy.Offset(5n), false]
  ] as const) {
    it(`restores no stored offset on close after ${name} and an on read seek back whose store landed with an ` +
      'unknown outcome', async () => {
      const topic = new FakeTopic(10n);
      // Without a group the partition reads nothing after its lookup.
      topic.routeLookupsHang = true;
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead }, start);
      if (lookedUp) {
        await assert.rejects(reader.next({ signal: AbortSignal.timeout(300) }));
        assert.deepEqual(topic.offsetLookups, [1]);
      }
      topic.uncertainWriteLands = true;
      await assert.rejects(reader.seek(1, PollingStrategy.Offset(3n)), OperationUncertainError);
      await reader.close();

      assert.deepEqual([topic.stored.has(1), topic.offsetDeletes, topic.reads], [false, [1], []]);
    });
  }

  for (const lost of [false, true]) {
    it(`leaves the offset on close after ${lost ? 'a lost reply that stored nothing' : 'an empty reply'} of an on read ` +
      'start at an offset past the end', async () => {
      const topic = new FakeTopic(4n);
      const cancellation = new AbortController();
      topic.loseNextCommittingRead = lost;
      topic.onLostCommittingRead = () => cancellation.abort(new Error('stop before delivery'));
      const reader = await open(topic, [1], { commit: CommitPolicy.OnRead }, PollingStrategy.Offset(2n * topic.limit));
      await assert.rejects(reader.next({ signal: lost ? cancellation.signal : AbortSignal.timeout(2 * topic.idleWait) }));
      await reader.close();

      assert.deepEqual([topic.reads.length > 0, [...topic.stored]], [true, []]);
    });
  }

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

  it('hands the sixth refusal in a row of a partition that the server refuses as not owned while the assignment still ' +
    'names it to onError, and reads the partition once the server serves it again', async () => {
    const topic = new FakeTopic();
    topic.assigned = [1];
    topic.notOwned.add(1);
    const client = topic.client();
    const reported: unknown[] = [];
    const consumer = new IggyConsumer(await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 }, retryInterval: 5
    }), undefined, (error) => {
      reported.push([error.partitionId, (error.error as { errorCode?: number }).errorCode, error.stopsPartition,
        topic.reads.filter(({ partition }) => partition === 1).length]);
      topic.notOwned.delete(1);
    });
    try {
      const first = await consumer[Symbol.asyncIterator]().next()
        .then(({ value }) => value?.message.headers.offset, (error: unknown) => String(error));
      assert.deepEqual([reported, first],
        [[[1, CONSUMER_GROUP_PARTITION_NOT_OWNED, false, MAX_LASTING_REFUSALS]], 0n]);
    } finally {
      await consumer.shutdown();
    }
  });

  it('starts the count of refusals as not owned again at a served check of an on read start, so six such refusals ' +
    'report no error and each waits one retry interval', async () => {
    // Long enough to tell one retry interval from two despite timer slack.
    const retryInterval = 200;
    const topic = new FakeTopic(0n);
    topic.assigned = [1];
    const client = topic.client();
    const send = client.sendCommand;
    client.sendCommand = async (code, payload, options) => {
      // The server refuses each other check as not owned and serves the one after it, so no two refusals are in a row.
      if (code === COMMAND_CODE.PollMessagesDeferred && !topic.notOwned.delete(1)) topic.notOwned.add(1);
      return send(code, payload, options);
    };
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval
    });
    const reads = () => topic.reads.filter(({ partition }) => partition === 1).map(({ at }) => at);
    const waiting = new AbortController();
    // An on read reader reads only while a next() waits.
    const ended = reader.next({ signal: waiting.signal }).then(() => undefined, (error: unknown) => error);
    try {
      await waitFor(() => reads().length >= 2 * MAX_LASTING_REFUSALS, 'the partition did not read after six refusals',
        MAX_LASTING_REFUSALS * retryInterval + WAIT_MS);
    } finally {
      waiting.abort();
      await reader.close();
    }
    const outcome = await ended;
    const checks = reads();
    const pauses = Array.from({ length: MAX_LASTING_REFUSALS }, (_, refusal) =>
      checks[2 * refusal + 1] - checks[2 * refusal]);
    assert.ok(!(outcome instanceof ReadError), `the reader reported a refusal: ${String(outcome)}`);
    assert.ok(pauses.every((pause) => pause >= retryInterval - TIMER_SLACK_MS &&
      pause < 2 * retryInterval - TIMER_SLACK_MS), `read again ${pauses.join(', ')} ms after each refusal`);
  });

  it('reads a partition that the server refused as not owned at once after a session reset, and waits one retry ' +
    'interval after its next refusal', async () => {
    // Long enough to tell one retry interval from two despite timer slack.
    const retryInterval = 200;
    const topic = new FakeTopic();
    topic.assigned = [1];
    topic.notOwned.add(1);
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 }, retryInterval
    });
    const reads = () => topic.reads.filter(({ partition }) => partition === 1).map(({ at }) => at);
    let reset = 0;
    try {
      // After its second refusal in a row the partition waits two retry intervals.
      await waitFor(() => reads().length === 2, 'the partition was not refused twice');
      reset = Date.now();
      topic.changeSession();
      // Counts kept from the old session would read again after two and then four retry intervals.
      await waitFor(() => reads().length >= 4, 'the partition did not read twice after the session reset',
        6 * retryInterval + WAIT_MS);
    } finally {
      await reader.close();
    }
    const [, , afterReset, next] = reads();
    const pause = next - afterReset;
    assert.ok(afterReset - reset < retryInterval, `read ${afterReset - reset} ms after the session reset`);
    assert.ok(pause >= retryInterval - TIMER_SLACK_MS && pause < 2 * retryInterval - TIMER_SLACK_MS,
      `read again ${pause} ms after the first refusal of the new session`);
  });

  it('reads on a group partition that the server refuses as not owned during the catch-up of a lost read, and ' +
    'delivers the lost range once the server serves the partition again', async () => {
    const topic = new FakeTopic(8n);
    topic.assigned = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
      options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 5
    });
    const catchUpReads = () => topic.reads.filter(({ kind, autocommit }) =>
      kind === PollingStrategyKind.Offset && !autocommit).length;
    const outcome = (read: Promise<PolledBatch>) => read.then((batch) => offsets([batch]), (error: unknown) =>
      error instanceof ReadError ? { code: (error.error as { errorCode?: number }).errorCode,
        stops: error.stopsPartition, reads: catchUpReads() } : String(error));
    const refused: unknown[] = [];
    const served: unknown[] = [];
    try {
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [0n, 1n]);
      // The next read stores 2 and 3 and loses its reply. The read after it serves 4 and 5, which shows the gap.
      topic.loseNextCommittingRead = true;
      // The server refuses the first read of the catch-up as not owned, and every later read until the test ends that.
      topic.beforeUncommittedPoll = () => {
        topic.notOwned.add(1);
        throw responseError(COMMAND_CODE.PollMessagesDeferred, CONSUMER_GROUP_PARTITION_NOT_OWNED);
      };
      while (refused.filter((seen) => !Array.isArray(seen)).length < 2)
        refused.push(await outcome(reader.next({ signal: AbortSignal.timeout(2_000) })));
      topic.notOwned.delete(1);
      for (let last: unknown; last !== 7n;) {
        const seen = await outcome(reader.next({ signal: AbortSignal.timeout(2_000) }));
        served.push(seen);
        if (!Array.isArray(seen)) break;
        last = seen.at(-1);
      }
    } finally {
      await reader.close();
    }

    const notOwned = { code: CONSUMER_GROUP_PARTITION_NOT_OWNED, stops: false };
    assert.deepEqual([refused, served], [
      [{ ...notOwned, reads: MAX_LASTING_REFUSALS }, { ...notOwned, reads: 2 * MAX_LASTING_REFUSALS }],
      [[2n, 3n], [4n, 5n], [6n, 7n]]
    ]);
  });

  it('reads a partition that the first sync after the join missed without waiting for the refresh interval', async () => {
    const topic = new FakeTopic();
    // The first sync comes before the partition log persists the new owner, so it lists nothing.
    void once(topic.emitter, 'synced').then(() => { topic.assigned = [1]; });
    const client = topic.client();
    const started = Date.now();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 }, retryInterval: 20
    });
    try {
      await waitFor(() => topic.reads.some(({ partition }) => partition === 1), 'the member did not read its partition');
      const elapsed = Date.now() - started;
      assert.ok(elapsed < SETTLED_READ_MS, `the member read its partition ${elapsed} ms after the start`);
    } finally {
      await reader.close();
    }
  });

  it('syncs once per refresh interval while its partitions stay the same and the group generation moves on', async () => {
    const topic = new FakeTopic(0n);
    topic.assigned = [1];
    topic.idleWait = PARKED_READ_WAIT_MS;
    const syncs: number[] = [];
    topic.emitter.on('synced', () => syncs.push(Date.now()));
    const client = topic.client();
    const started = Date.now();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 }, retryInterval: 20
    });
    try {
      await new Promise((resolve) => setTimeout(resolve, SETTLED_UNTIL_MS));
    } finally {
      await reader.close();
    }

    const since = syncs.map((at) => at - started);
    const settled = since.filter((at) => at >= SETTLED_AFTER_MS);
    const gaps = settled.slice(1).map((at, index) => at - settled[index]);
    assert.ok(settled.length >= 2 && gaps.every((gap) => gap >= GROUP_REFRESH_MS - TIMER_SLACK_MS),
      `syncs at ${since.join(', ')} ms after the start`);
  });

  it('syncs only when due while its reads keep finishing and next() keeps waking it', async () => {
    const topic = new FakeTopic(1_000_000n);
    topic.assigned = [1];
    const syncs: number[] = [];
    topic.emitter.on('synced', () => syncs.push(Date.now()));
    const client = topic.client();
    const started = Date.now();
    // The smallest read ahead, so the reader keeps reading while the member takes its batches.
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100, count: 2 },
      prefetchMessages: 0, prefetchBytes: 0, retryInterval: 20
    });
    try {
      while (Date.now() - started < SETTLED_UNTIL_MS) {
        await reader.next({ signal: AbortSignal.timeout(2_000) });
        await new Promise((resolve) => setTimeout(resolve, NEXT_PAUSE_MS));
      }
    } finally {
      await reader.close();
    }

    const settled = syncs.map((at) => at - started).filter((at) => at >= SETTLED_AFTER_MS && at < SETTLED_UNTIL_MS);
    assert.ok(settled.length >= 1 && settled.length <= 2,
      `${settled.length} of ${syncs.length} syncs came ${SETTLED_AFTER_MS} to ${SETTLED_UNTIL_MS} ms after the start`);
  });

  it('joins again and syncs at once when a read finds the member gone, without waiting for the pause', async () => {
    const topic = new FakeTopic(0n);
    topic.assigned = [1];
    const syncs: number[] = [];
    topic.emitter.on('synced', () => syncs.push(Date.now()));
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 }, retryInterval: 20
    });
    try {
      await new Promise((resolve) => setTimeout(resolve, SETTLED_AFTER_MS));
      // A sync just came, so the next one is due a refresh interval later.
      await once(topic.emitter, 'synced');
      const synced = syncs.length;
      topic.forgottenRefusal = CONSUMER_GROUP_MEMBER_NOT_FOUND;
      const lost = Date.now();
      await waitFor(() => topic.joins.length > 1 && syncs.length > synced, 'the member did not join and sync again');
      const [joined, resynced] = [topic.joins[1], syncs[synced]];
      assert.ok(resynced >= joined && resynced - lost < AT_ONCE_MS,
        `joined ${joined - lost} ms and synced ${resynced - lost} ms after the member was lost`);
    } finally {
      await reader.close();
    }
  });

  it('joins again and reads its partition again when the server forgot the member and its sync comes back empty',
    async () => {
      const topic = new FakeTopic(0n);
      topic.assigned = [1];
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 }, retryInterval: 20
      });
      try {
        await waitFor(() => topic.reads.length > 0, 'the member did not read its partition');
        topic.forgottenRefusal = CONSUMER_GROUP_PARTITION_NOT_OWNED;
        topic.limit = 2n;
        const batch = await nextOf(reader, 1)
          .catch((error: unknown) => assert.fail(`the member stayed idle: ${String(error)}`));
        assert.deepEqual([offsets([batch]), topic.joins.length], [[0n, 1n], 2]);
      } finally {
        await reader.close();
      }
    });

  for (const outcome of ['fails', 'never completes'] as const) {
    it(`keeps reading its partition while each later sync of its assignment ${outcome}`, async () => {
      const topic = new FakeTopic();
      topic.assigned = [1];
      const client = topic.client();
      const send = client.sendCommand;
      let syncs = 0;
      let reads = 0;
      let laterSyncStarted!: () => void;
      const laterSync = new Promise<void>((resolve) => { laterSyncStarted = resolve; });
      let endSync = () => {};
      client.sendCommand = async (command, payload, options) => {
        if (command === COMMAND_CODE.SyncGroup && ++syncs > 1) {
          laterSyncStarted();
          if (outcome === 'never completes') await new Promise<void>((resolve) => { endSync = resolve; });
          throw responseError(command, TRANSIENT_NOT_ACCEPTED);
        }
        // The first read ends after a later sync started, so a second read can start only while that sync fails or
        // waits for its answer.
        const read = command === COMMAND_CODE.FetchMessages || command === COMMAND_CODE.PollMessagesDeferred;
        if (read && ++reads === 1) await laterSync;
        return send(command, payload, options);
      };
      const reader = await openGroup(client);
      try {
        assert.deepEqual(offsets([await nextOf(reader, 1)]), [0n, 1n]);
        await waitFor(() => topic.reads.filter(({ partition }) => partition === 1).length > 1,
          'the member did not read its partition again');
      } finally {
        endSync();
        await reader.close();
      }
    });
  }

  it('releases a rejoin with an unknown outcome at once, so it blocks no metadata write of the client, and joins ' +
    'again after its retry interval', async () => {
    const topic = new FakeTopic(0n);
    topic.assigned = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 },
      retryInterval: REJOIN_RETRY_MS
    });
    try {
      topic.uncertainGroupWrite = COMMAND_CODE.JoinGroup;
      topic.emitter.emit('sessionReset');
      await waitFor(() => topic.joins.length === 2, 'the member did not join after the session reset');
      await client.sendCommand(COMMAND_CODE.CreateGroup, Buffer.alloc(0))
        .catch((error: unknown) => assert.fail(`the lost join blocked a metadata write: ${String(error)}`));
      topic.limit = 2n;
      // The reads go on under the last assignment during the retry interval, so the batch can come before the join.
      const batch = await reader.next({ signal: AbortSignal.timeout(2_000) });
      await waitFor(() => topic.joins.length === 3, 'the member did not join again after the lost join');
      assert.deepEqual([offsets([batch]), topic.abandoned], [[0n, 1n], [UNCERTAIN_GROUP_OPERATION]]);
    } finally {
      await reader.close();
    }
  });

  for (const [write, first] of [
    ['the leave of the close with an unknown outcome', async (topic: FakeTopic, client: RawClient) => {
      const reader = await openGroup(client);
      topic.uncertainGroupWrite = COMMAND_CODE.LeaveGroup;
      await reader.close();
    }],
    ['the join of an open with an unknown outcome', async (topic: FakeTopic, client: RawClient) => {
      topic.uncertainGroupWrite = COMMAND_CODE.JoinGroup;
      await assert.rejects(openGroup(client), OperationUncertainError);
    }],
    ['the create of a consumer group open with an unknown outcome', async (topic: FakeTopic, client: RawClient) => {
      topic.uncertainGroupWrite = COMMAND_CODE.CreateGroup;
      await assert.rejects(openConsumerGroup(async () => client)({ name: 5, streamId: 1, topicId: 2 }),
        OperationUncertainError);
    }]
  ] as const) {
    it(`releases ${write}, so a later member on the same client joins`, async () => {
      const topic = new FakeTopic(0n);
      topic.assigned = [1];
      const client = topic.client();
      await first(topic, client);
      const later = await openGroup(client);
      await later.close();

      assert.deepEqual(topic.abandoned, [UNCERTAIN_GROUP_OPERATION]);
    });
  }

  it('waits its retry interval between joins when the server forgets the member again at each join', async () => {
    const retryInterval = 20;
    const watched = 10 * retryInterval;
    const topic = new FakeTopic(0n);
    topic.assigned = [1];
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 }, options: { maxWait: 100 }, retryInterval
    });
    try {
      topic.forgottenRefusal = CONSUMER_GROUP_PARTITION_NOT_OWNED;
      topic.forgetsJoins = true;
      const error = await reader.next({ signal: AbortSignal.timeout(1_000) })
        .then(() => undefined, (failure: unknown) => failure);
      assert.ok(error instanceof ReadError && error.partitionId === undefined &&
        (error.error as { errorCode: number }).errorCode === CONSUMER_GROUP_MEMBER_NOT_FOUND, String(error));
      const joins = topic.joins.length;
      await new Promise((resolve) => setTimeout(resolve, watched));
      assert.ok(topic.joins.length - joins <= watched / retryInterval + 2,
        `${topic.joins.length - joins} joins in ${watched} ms`);
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

  it('spreads a committing backlog over 32 slots even when the requested budget holds one batch', async () => {
    const slots = 32;
    const count = 2;
    const maxBytes = 512;
    const partitions = Array.from({ length: slots }, (_, index) => index * 2 + 1);
    const topic = new FakeTopic(100n);
    topic.countLimited = true;
    topic.reportsHead = true;
    topic.holdFetches = true;
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer, partitions, start: PollingStrategy.Offset(0n),
      options: { count, maxBytes, commit: CommitPolicy.OnRead }, prefetchMessages: count, prefetchBytes: maxBytes
    });
    let next: Promise<unknown> | undefined;
    try {
      const first = reader.next();
      await waitFor(() => topic.heldFetches.length === 1, 'the initial grouped fetch did not start');
      topic.heldFetches.shift()!();
      assert.deepEqual(offsets([await first]), [0n, 1n]);
      next = reader.next();
      await waitFor(() => topic.fetches.length === slots + 1, 'the backlog did not use all 32 slots');
      assert.deepEqual(topic.fetches.slice(1).map(([partition]) => partition).sort((left, right) => left - right), partitions);
      assert.ok(topic.fetches.slice(1).every((targets) => targets.length === 1));
    } finally {
      topic.holdFetches = false;
      for (const release of topic.heldFetches.splice(0)) release();
      await next;
      await reader.close();
    }
  });

  for (const [prefetchBytes, spread] of [
    [undefined, [[1], [3], [5], [7], [9]]],
    [3 * 1_048_576, [[1], [3], [5], [7], [9]]]
  ] as const) {
    it(`spreads backlogged partitions over free read slots with ${prefetchBytes ?? 'default'} requested bytes`, async () => {
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
    [3 * 1_048_576, [[1], [3], [5], [7], [9]]]
  ] as const) {
    it(`spreads backlogged partitions over committing read slots with ${prefetchBytes ?? 'default'} requested bytes`,
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

  for (const primaries of [undefined, 3]) {
    const polls = MAX_READS_IN_FLIGHT - (primaries ?? 0);
    it(`polls at most ${polls} partitions alone at once ${primaries ? `beside ${primaries} readiness groups`
      : 'without readiness groups'}, each poll from the partition after the last one`, async () => {
      const partitions = Array.from({ length: MAX_READS_IN_FLIGHT + 8 }, (_, index) => index);
      const topic = new FakeTopic(0n);
      const client = topic.client();
      if (primaries === undefined) client.readinessGroups = undefined;
      else topic.primaries = primaries;
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer, partitions, start: PollingStrategy.Last, options: { maxWait: 100 }, retryInterval: 20
      });
      try {
        await waitFor(() => topic.reads.length > partitions.length, 'the polls that ended empty did not pass their turn on');
        assert.equal(topic.mostInFlight, polls);
        assert.deepEqual(topic.reads.slice(0, partitions.length + 1).map(({ partition }) => partition), [...partitions, 0]);
      } finally {
        await reader.close();
      }
    });
  }

  it('fetches at most 32 groups at once, and the groups left out fetch first after a fetch ends', async () => {
    const partitions = Array.from({ length: MAX_READS_IN_FLIGHT + 8 }, (_, index) => index);
    const topic = new FakeTopic();
    topic.holdFetches = true;
    const client = topic.client();
    client.readinessGroups = async (_stream, _topic, targets) =>
      [...targets].sort((left, right) => left - right).map((partition) => [partition]);
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer, partitions, options: { maxWait: 100 }, retryInterval: 20
    });
    try {
      await waitFor(() => topic.fetches.length >= MAX_READS_IN_FLIGHT, 'the first groups did not fetch');
      await new Promise(setImmediate);
      assert.equal(topic.fetches.length, MAX_READS_IN_FLIGHT);
      topic.heldFetches.shift()!();
      await waitFor(() => topic.fetches.length > MAX_READS_IN_FLIGHT, 'no group fetched after a fetch ended');
      await new Promise(setImmediate);
      assert.deepEqual(topic.fetches, partitions.slice(0, MAX_READS_IN_FLIGHT + 1).map((partition) => [partition]));
      assert.equal(topic.mostInFlight, MAX_READS_IN_FLIGHT);
    } finally {
      topic.holdFetches = false;
      for (const release of topic.heldFetches.splice(0)) release();
      await reader.close();
    }
  });

  it('leaves a request for each group while partitions poll alone', async () => {
    const busy = [0, 1];
    const partitions = Array.from({ length: MAX_READS_IN_FLIGHT + 10 }, (_, index) => index);
    const topic = new FakeTopic();
    for (const partition of partitions.slice(busy.length)) topic.emptyPartitions.add(partition);
    topic.idleWait = PARKED_READ_WAIT_MS;
    const client = topic.client();
    const reader = await PartitionReader.open(async () => client, {
      streamId: 1, topicId: 2, consumer, partitions, start: PollingStrategy.Last, options: { maxWait: 100 }, retryInterval: 20
    });
    try {
      await waitFor(() => topic.fetches.length === busy.length, 'a group with messages did not fetch beside the polls alone');
      await new Promise(setImmediate);
      const polled = topic.reads.filter(({ code }) => code === COMMAND_CODE.PollMessagesDeferred);
      assert.deepEqual(polled.map(({ partition }) => partition), partitions.slice(0, MAX_READS_IN_FLIGHT));
      assert.deepEqual(topic.fetches.flat().sort((left, right) => left - right), busy);
      assert.equal(topic.mostInFlight, MAX_READS_IN_FLIGHT);
    } finally {
      await reader.close();
    }
  });

  it('fetches a group at once when its retry is due while more partitions poll alone than the reader has requests',
    async () => {
      const [busy, ...quiet] = Array.from({ length: MAX_READS_IN_FLIGHT + 8 }, (_, index) => index);
      const topic = new FakeTopic(100n);
      topic.primaries = 3;
      for (const partition of quiet) topic.emptyPartitions.add(partition);
      topic.idleWait = PARKED_READ_WAIT_MS;
      topic.refused.set(busy, TRANSIENT_NOT_ACCEPTED);
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer, partitions: [busy, ...quiet], start: PollingStrategy.Last,
        options: { maxWait: 100 }, retryInterval: 20
      });
      try {
        assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(1_000) })]), [0n, 1n]);
        await waitFor(() => topic.fetches.length > 0, 'the busy group did not fetch');
        topic.refused.delete(busy);
        assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(1_000) })]), [2n, 3n]);
      } finally {
        await reader.close();
      }
    });

  it('fetches a group at once when the application takes its reply while more partitions poll alone than requests',
    async () => {
      const [busy, ...quiet] = Array.from({ length: MAX_READS_IN_FLIGHT + 8 }, (_, index) => index);
      const topic = new FakeTopic(100n);
      topic.primaries = 3;
      for (const partition of quiet) topic.emptyPartitions.add(partition);
      topic.idleWait = PARKED_READ_WAIT_MS;
      const client = topic.client();
      const reader = await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer, partitions: [busy, ...quiet],
        options: { maxWait: 100, commit: CommitPolicy.OnRead }, retryInterval: 20
      });
      try {
        assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(1_000) })]), [0n, 1n]);
        assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(1_000) })]), [2n, 3n]);
      } finally {
        await reader.close();
      }
    });

  it('groups its partitions again when a moved primary refuses a fetch, and reads the partition in its new group',
    async () => {
      const moved = 2;
      const topic = new FakeTopic(4n);
      const reader = await open(topic, [0, 1, 2, 3], { commit: CommitPolicy.OnRead });
      try {
        assert.deepEqual(offsets([await nextOf(reader, moved)]), [0n, 1n]);
        topic.movedPrimaries.set(moved, 1);
        assert.deepEqual(offsets([await nextOf(reader, moved)]), [2n, 3n]);
      } finally {
        await reader.close();
      }

      assert.ok(topic.fetches.some((targets) => targets.includes(moved) && [1, 3].some((partition) => targets.includes(partition))),
        `the moved partition never fetched with its new group: ${JSON.stringify(topic.fetches)}`);
    });

  for (const lookup of ['fails', 'hangs'] as const) {
    it(`keeps reading a partition of a known group while the route lookup of another partition ${lookup}`, async () => {
      const [known, unroutable] = [0, 1];
      const topic = new FakeTopic(100n);
      const reader = await open(topic, [known, unroutable], { commit: CommitPolicy.OnRead });
      try {
        assert.deepEqual(offsets([await nextOf(reader, known)]), [0n, 1n]);
        const lookups = topic.routeLookups;
        // One partition without a route fails or holds the whole lookup, as in the client, and its reads are refused.
        if (lookup === 'fails') topic.unrouted = new Error(`no route to partition ${unroutable}`);
        else topic.routeLookupsHang = true;
        topic.refused.set(unroutable, TRANSIENT_NOT_ACCEPTED);
        topic.emitter.emit('sessionReset');
        await waitFor(() => topic.routeLookups > lookups, 'the session reset did not group the partitions again');
        assert.deepEqual(offsets([await nextOf(reader, known), await nextOf(reader, known)]), [2n, 3n, 4n, 5n]);
      } finally {
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

  it('sends a read once that fails with a tiered read failure, and stops its partition until a seek', async () => {
    const topic = new FakeTopic(1_000n);
    topic.refused.set(1, TIERED_READ_FAILED);
    const reader = await open(topic, [1, 3]);
    const readsOf = (partition: number) => topic.reads.filter((read) => read.partition === partition).length;

    let stopped: unknown;
    while (!stopped) stopped = await reader.next().then(() => undefined, (failure: unknown) => failure);
    const others = [await reader.next(), await reader.next()];
    // Several retry intervals, in which a refusal that retries reads again.
    await new Promise((resolve) => setTimeout(resolve, 100));
    const refusedReads = readsOf(1);
    topic.refused.delete(1);
    await reader.seek(1, PollingStrategy.Offset(0n));
    let resumed = await reader.next();
    while (resumed.partitionId !== 1) resumed = await reader.next();
    await reader.close();

    assert.ok(stopped instanceof ReadError);
    assert.deepEqual([stopped.partitionId, (stopped.error as { errorCode: number }).errorCode,
      stopped.stopsPartition, stopped.resumable], [1, TIERED_READ_FAILED, true, true]);
    assert.equal(refusedReads, 1, 'the refused read went out again');
    assert.ok(others.every(({ partitionId }) => partitionId === 3));
    assert.equal(resumed.messages[0].headers.offset, 0n);
  });

  it('reads again after a history refusal of a read from a seek to a continuation whose context is all zero, as ' +
    'from one without a context', async () => {
    const topic = new FakeTopic(10n);
    topic.history.add(1);
    const reader = await open(topic, [1]);
    try {
      await reader.seek(1, continuation(1, PollingStrategyKind.Offset, 4n, EMPTY_PARTITION_CONTEXT));
      await waitFor(() => topic.reads.filter(({ partition, kind, value }) =>
        partition === 1 && kind === PollingStrategyKind.Offset && value === 4n).length > 1,
      'the read from offset 4 did not go again');
      topic.history.clear();
      assert.deepEqual(offsets([await reader.next({ signal: AbortSignal.timeout(2_000) })]), [4n, 5n]);
    } finally {
      await reader.close();
    }
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

  for (const [stop, status] of [['six refusals in a row', UNAUTHORIZED], ['a lost history', HISTORY_UNAVAILABLE]] as const) {
    it(`ends the iteration with the stop after ${stop}, and a seek to Next reads the partition again`, async () => {
      const topic = new FakeTopic(4n);
      topic.stored.set(1, 1n);
      const client = topic.client();
      const errors: ReadError[] = [];
      const consumer = new IggyConsumer(await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Single, id: 4 }, partitions: [1],
        options: { maxWait: 100 }, retryInterval: 5
      }), undefined, (error) => { errors.push(error); });
      try {
        const messages = consumer[Symbol.asyncIterator]();
        const before = [await messages.next(), await messages.next()];
        topic.refused.set(1, status);
        const stopped = await messages.next().then(() => undefined, (error: unknown) => error);
        topic.refused.delete(1);
        await consumer.seek(1, PollingStrategy.Next);
        const resumed = await consumer[Symbol.asyncIterator]().next();

        assert.deepEqual(before.map((read) => read.value?.message.headers.offset), [2n, 3n]);
        assert.ok(stopped instanceof ReadError, `the iteration did not end with the stop: ${String(stopped)}`);
        assert.deepEqual([stopped.partitionId, (stopped.error as { errorCode: number }).errorCode,
          stopped.stopsPartition, stopped.resumable], [1, status, true, true]);
        assert.equal(resumed.value?.message.headers.offset, 2n, 'a seek to Next reads after the stored offset');
        assert.deepEqual(errors, [], 'a stop is thrown, not handed to onError');
      } finally {
        await consumer.shutdown();
      }
    });
  }

  it('reports a pending lost read instead of the stop that ends the iteration', async () => {
    const topic = new FakeTopic(4n);
    topic.onLostCommittingRead = () => topic.history.add(1);
    const consumer = new IggyConsumer(await open(topic, [1], { commit: CommitPolicy.OnRead }));
    const messages = consumer[Symbol.asyncIterator]();
    try {
      const first = [await messages.next(), await messages.next()];
      topic.loseNextCommittingRead = true;
      const stopped = await messages.next().then(() => undefined, (error: unknown) => error);

      assert.deepEqual(first.map((read) => read.value?.message.headers.offset), [0n, 1n]);
      assert.ok(stopped instanceof ReadError, `the iteration did not end with the stop: ${String(stopped)}`);
      assert.deepEqual([stopped.partitionId, (stopped.error as { errorCode: number }).errorCode,
        stopped.stopsPartition, stopped.resumable], [1, FETCH_PROGRESS_UNCERTAIN, true, true],
        'a seek to Next would skip what the lost read stored, so the stop reports that read');
    } finally {
      await consumer.shutdown();
    }
  });

  it('ends the iteration with the error of a lost range, and a new iteration reads on after that range', async () => {
    const topic = new FakeTopic(8n);
    const errors: ReadError[] = [];
    const consumer = new IggyConsumer(await open(topic, [1], { commit: CommitPolicy.OnRead }), undefined,
      (error) => { errors.push(error); });
    try {
      const messages = consumer[Symbol.asyncIterator]();
      const first = [await messages.next(), await messages.next()];
      // The next read stores 2 and 3 and loses its reply, and the catch-up by offset finds them gone.
      topic.loseNextCommittingRead = true;
      topic.emptyOffsetReads = true;
      const ended = await messages.next().then(() => undefined, (error: unknown) => error);
      const resumed = await consumer[Symbol.asyncIterator]().next();

      assert.deepEqual(first.map((read) => read.value?.message.headers.offset), [0n, 1n]);
      assert.ok(ended instanceof ReadError, `the iteration did not end with the lost range: ${String(ended)}`);
      assert.deepEqual([ended.partitionId, (ended.error as { errorCode: number }).errorCode, ended.lost,
        resumed.value?.message.headers.offset, errors], [1, FETCH_PROGRESS_UNCERTAIN, { first: 2n, last: 3n }, 4n, []]);
    } finally {
      await consumer.shutdown();
    }
  });

  for (const read of ['iterator', 'consume', 'readable'] as const) {
    it(`ends the ${read} with the error of a stop that the consumer does not resume`, async () => {
      const topic = new FakeTopic(4n);
      topic.refused.set(1, INVALID_SIZE_BYTES);
      const errors: ReadError[] = [];
      const consumer = new IggyConsumer(await open(topic, [1]), undefined, (error) => { errors.push(error); });
      try {
        const ended = read === 'iterator' ? consumer[Symbol.asyncIterator]().next()
          : read === 'consume' ? consumer.consume(() => undefined)
          : once(consumer.toReadable().resume(), 'error').then(([error]) => Promise.reject(error));
        const outcome = await Promise.race([ended.then(() => 'ended', (error: unknown) => error),
          new Promise((resolve) => setTimeout(resolve, 2_000, 'stalled'))]);

        assert.ok(outcome instanceof ReadError, `the ${read} did not end with a read error: ${String(outcome)}`);
        assert.deepEqual([outcome.partitionId, (outcome.error as { errorCode: number }).errorCode,
          outcome.stopsPartition, outcome.resumable, errors.length], [1, INVALID_SIZE_BYTES, true, false, 0]);
      } finally {
        await consumer.shutdown();
      }
    });
  }

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

  for (const [action, end, leaves] of [
    ['commit', (consumer, checkpoint, signal) => consumer.commit(checkpoint, { signal }), 0],
    ['shutdown', (consumer, _, signal) => consumer.shutdown({ signal }), 1]
  ] as [string, (consumer: IggyConsumer, checkpoint: Checkpoint, signal: AbortSignal) => Promise<void>, number][]) {
    it(`ends a ${action} when its signal aborts the reads for the owner of its store, which keep getting a transient ` +
      'refusal', async () => {
      const retryInterval = 20;
      const topic = new FakeTopic(10n);
      topic.assigned = [1];
      const client = topic.client();
      // Only the shutdown counts the message, and no interval timer runs.
      const consumer = new IggyConsumer(await PartitionReader.open(async () => client, {
        streamId: 1, topicId: 2, consumer: { kind: ConsumerKind.Group, id: 5 },
        options: { maxWait: 100, commit: CommitPolicy.Auto({ every: CommitEvery.Batch }) }, retryInterval
      }), CommitAfter.Processing);
      let ended = false;
      let failure: unknown;
      let ending = Promise.resolve();
      try {
        const { value } = await consumer[Symbol.asyncIterator]().next();
        topic.ownerGeneration += 1n;
        topic.ownerInstalledAt = topic.metadataOp + 1n;
        topic.refused.set(1, TRANSIENT_NOT_ACCEPTED);
        const aborting = new AbortController();
        ending = end(consumer, value!.checkpoint, aborting.signal)
          .catch((error: unknown) => { failure = error; }).finally(() => { ended = true; });
        await waitFor(() => probesOf(topic, 1).length > 1, 'the read for the owner did not go again');
        aborting.abort();
        await waitFor(() => ended, `the ${action} did not end`);
        const probes = probesOf(topic, 1).length;
        await new Promise((resolve) => setTimeout(resolve, 2 * retryInterval));
        assert.deepEqual([(failure as { errorCode?: number } | undefined)?.errorCode, topic.leaves,
          probesOf(topic, 1).length], [CONSUMER_GROUP_PARTITION_NOT_OWNED, leaves, probes]);
      } finally {
        // A commit or a shutdown that did not end reads for the owner until the refusal ends.
        topic.refused.delete(1);
        await ending;
        // A shutdown that the abort ended rejects again with its refusal.
        await consumer.shutdown().catch(() => undefined);
      }
    });
  }
});
