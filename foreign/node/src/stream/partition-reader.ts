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

import { setTimeout as sleep } from 'node:timers/promises';
import type { ClientProvider, RawClient } from '../client/client.type.js';
import { FetchProgressUncertainError, OperationUncertainError } from '../client/client.operation.js';
import { COMMAND_CODE } from '../wire/command.code.js';
import { DeserializeError, ResponseError, responseError } from '../wire/error.utils.js';
import type { Id } from '../wire/identifier.utils.js';
import { ConsumerKind, type Consumer } from '../wire/offset/offset.utils.js';
import { storeOffset } from '../wire/offset/store-offset.command.js';
import { deleteOffset } from '../wire/offset/delete-offset.command.js';
import { storePosition } from '../wire/offset/position.command.js';
import { joinGroup } from '../wire/consumer-group/join-group.command.js';
import { leaveGroup } from '../wire/consumer-group/leave-group.command.js';
import { syncGroup } from '../wire/consumer-group/sync-group.command.js';
import { decodeFetchSection, fetchMessages, fetchSectionError } from '../wire/message/fetch-messages.command.js';
import { pollMessages } from '../wire/message/poll-messages.command.js';
import { PollingStrategy, PollingStrategyKind, type PollMessagesResponse } from '../wire/message/poll.utils.js';
import { CommitPolicy, pollOptions, wireOptions, type PollOptions } from '../wire/message/poll-options.js';
import {
  continuation, polledBatch, validatePartition, type Checkpoint, type Continuation, type PolledBatch
} from '../wire/message/checkpoint.js';
import type { PartitionContext } from '../wire/vsr/header.js';

const INVALID_COMMAND = 3;
const UNAUTHORIZED = 41;
const TRANSIENT_NOT_COMMITTED = 57;
const TRANSIENT_NOT_ACCEPTED = 58;
const INVALID_SIZE_BYTES = 80;
const HISTORY_UNAVAILABLE = 87;
const LIFECYCLE_BUSY = 88;
const NOT_PRIMARY = 89;
const DEFERRED_POLL_CAPACITY_EXCEEDED = 90;
const PARTITION_NOT_FOUND = 3007;
const CONSUMER_OFFSET_NOT_FOUND = 3021;
const CONSUMER_GROUP_MEMBER_NOT_FOUND = 5006;
const CONSUMER_GROUP_PARTITION_NOT_OWNED = 5009;
const FETCH_PROGRESS_UNCERTAIN = 14010;
const LASTING_REFUSALS = new Set([UNAUTHORIZED, 5, INVALID_COMMAND, PARTITION_NOT_FOUND, 1009, 1010, 2010, 2011]);
const TRANSIENT_REFUSALS = new Set([TRANSIENT_NOT_COMMITTED, TRANSIENT_NOT_ACCEPTED, NOT_PRIMARY,
  DEFERRED_POLL_CAPACITY_EXCEEDED, LIFECYCLE_BUSY]);
const MAX_LASTING_REFUSALS = 6;
const GROUP_REFRESH_MS = 1_000;
/** The wait of a reader read when the caller sets none. */
export const DEFAULT_READER_MAX_WAIT_MS = 5_000;
export const DEFAULT_PREFETCH_BYTES = 16 * 1024 * 1024;
export const DEFAULT_PREFETCH_MESSAGES = 16_000;

/** Parameters of `message.reader`. */
export type PartitionReaderRequest = {
  streamId: Id,
  topicId: Id,
  /** A standalone consumer reads `partitions`. A consumer group reads the assignment of its member. */
  consumer: Consumer,
  /** The partitions of a standalone consumer. A consumer group refuses them. */
  partitions?: number[],
  /** Where a partition without a position starts, Next by default. */
  start?: PollingStrategy,
  /** The limits of each read and the commit policy. A read waits up to 5 s by default. */
  options?: Partial<PollOptions>,
  /** The most bytes the reader holds read ahead of the application, 16 MiB by default. */
  prefetchBytes?: number,
  /** The most messages the reader holds read ahead of the application, 16,000 by default. */
  prefetchMessages?: number,
  /** The wait in milliseconds before a refused partition reads again, 1 s by default. */
  retryInterval?: number
};

/**
 * An error of a read of a `PartitionReader`. The reader stays usable after it
 * and retries by itself.
 */
export class ReadError extends Error {
  constructor(
    /** The partition of the error, or undefined for an error of the whole reader. */
    readonly partitionId: number | undefined,
    /** The error itself. */
    readonly error: unknown,
    /**
     * For a message that does not decode: the continuation after it. The
     * reader reads the message again until a `seek` to this continuation
     * skips it. The messages before it arrive in a batch before this error.
     */
    readonly skip?: Continuation
  ) {
    super(partitionId === undefined ? `reading failed: ${String(error)}`
      : `reading partition ${partitionId} failed: ${String(error)}`, { cause: error });
    this.name = 'ReadError';
    if (error instanceof DeserializeError) {
      this.offset = error.offset;
      this.record = error.record;
    }
  }

  /**
   * For a message that does not decode: its offset, known when its frame
   * decoded and a later step failed, such as its user headers. Undefined when
   * the framing of the reply is malformed, so no message can be told apart.
   */
  readonly offset?: bigint;

  /**
   * For a message that does not decode: the record as the reply carried it,
   * its 48-byte frame header, payload and user headers. For malformed
   * framing, the remaining bytes of the reply records from where decoding
   * stopped. Undefined for an error that is not a decode failure.
   */
  readonly record?: Buffer;
}

type Target = { partition: number, sent: PollingStrategy, generation: number };

type Entry = {
  partitionId?: number,
  generation: number,
  batch?: PolledBatch,
  error?: ReadError,
  messages: number,
  bytes: number
};

type PartitionState = {
  position: PollingStrategy,
  generation: number,
  inFlight: boolean,
  idle?: Promise<void>,
  settleIdle?: () => void,
  stopped?: ReadError,
  retryAt: number,
  lasting: number,
  catchUpUntil?: bigint,
  catchUpBatch?: PollMessagesResponse,
  pendingStart?: Continuation,
  aloneUntil?: bigint,
  delivered?: { offset: bigint, context: PartitionContext },
  handed?: { offset: bigint, context: PartitionContext },
  firstHanded?: bigint,
  counted?: Checkpoint,
  countedSinceStore: number,
  stored?: Checkpoint,
  /** The last read of the partition found no message. */
  atEnd?: boolean,
  /** When a read last served messages of the partition, so served partitions go last in the next fetch. */
  servedAt: number
};

let readers = 0;

const sameHistory = (sent: PartitionContext, served: PartitionContext): boolean =>
  sent.incarnation === served.incarnation && sent.ownerGeneration === served.ownerGeneration;

const errorCode = (error: unknown): number | undefined =>
  error instanceof FetchProgressUncertainError ? FETCH_PROGRESS_UNCERTAIN
    : error instanceof ResponseError ? error.errorCode : undefined;

const isAbort = (error: unknown): boolean => error instanceof Error && error.name === 'AbortError';

/**
 * Reads the partitions of a topic in batches, with routing, read ahead and the
 * position of each partition inside. Open one with `client.message.reader`.
 *
 * - The partitions of one primary share one request, and a moved primary is
 *   found again by itself.
 * - `next()` returns one batch of one partition, never empty, in offset order
 *   within one generation of the partition: from its first read, each `seek`
 *   and each new assignment to this member. One caller at a time calls it.
 * - With `CommitPolicy.Manual`, `commit` stores a checkpoint. With
 *   `CommitPolicy.OnRead` the server stores the offset of each read, the reader
 *   reads only while `next()` waits, and `close()` moves the offset back to
 *   the last batch the application took. A read whose reply was lost is
 *   repaired: the next read finds the messages that it stored, and the reader
 *   reads them again by offset without a commit. With `CommitPolicy.Auto` the
 *   reader counts each batch as its `after` says, or each `mark`, and stores
 *   the latest counted position at its interval or event and in `close()`.
 * - A `ReadError` leaves the reader usable. A history refusal (87) of a
 *   position from an earlier read, an oversized `Last` start (80) and a refusal
 *   that six retries did not change stop their partition until a `seek`.
 */
export class PartitionReader implements AsyncIterable<PolledBatch> {
  private readonly id = ++readers;
  private readonly states = new Map<number, PartitionState>();
  private readonly sought = new Map<number, number>();
  private readonly queue: Entry[] = [];
  private readonly reads = new Set<Promise<void>>();
  private readonly lifetime = new AbortController();
  private readonly closing = new Promise<void>((resolve) =>
    this.lifetime.signal.addEventListener('abort', () => resolve(), { once: true }));
  private readonly options: PollOptions;
  private readonly start: PollingStrategy;
  private readonly group: boolean;
  private readonly prefetchBytes: number;
  private readonly prefetchMessages: number;
  private readonly retryInterval: number;
  private arrival = resolvers();
  private wakeUp = resolvers();
  private storeDue = resolvers();
  private demand = false;
  private closed = false;
  private resync = false;
  private queuedMessages = 0;
  private queuedBytes = 0;
  private generations = 0;
  private serves = 0;
  private uncounted?: Checkpoint;
  private uncountedMessages = 0;
  private sessionResets = 0;
  private joinedResets = 0;
  private readonly worker: Promise<void>;
  private readonly committer: Promise<void>;
  /** @internal Set by a consumer, which reports and counts each delivered message itself. */
  countsMessages = false;

  private constructor(
    private readonly getClient: ClientProvider,
    private readonly request: PartitionReaderRequest
  ) {
    this.options = pollOptions({ maxWait: DEFAULT_READER_MAX_WAIT_MS, ...request.options });
    const start = request.start ?? PollingStrategy.Next;
    this.start = start.kind === PollingStrategyKind.First ? PollingStrategy.Offset(0n) : start;
    this.group = request.consumer.kind === ConsumerKind.Group;
    this.prefetchBytes = request.prefetchBytes ?? DEFAULT_PREFETCH_BYTES;
    this.prefetchMessages = request.prefetchMessages ?? DEFAULT_PREFETCH_MESSAGES;
    this.retryInterval = request.retryInterval ?? 1_000;
    for (const partition of request.partitions ?? [])
      this.states.set(partition, this.newState());
    this.worker = this.run();
    this.committer = this.options.commit.kind === 'auto' ? this.commitLoop() : Promise.resolve();
  }

  /**
   * Checks the request, joins the group of a group reader and starts reading.
   * A standalone consumer without partitions, a group with partitions and a
   * continuation as the start fail with 3.
   */
  static async open(getClient: ClientProvider, request: PartitionReaderRequest): Promise<PartitionReader> {
    pollOptions({ maxWait: DEFAULT_READER_MAX_WAIT_MS, ...request.options });
    const group = request.consumer.kind === ConsumerKind.Group;
    const partitions = request.partitions ?? [];
    if (group ? partitions.length > 0 : partitions.length === 0)
      throw responseError(0, INVALID_COMMAND, group
        ? 'a consumer group reads the partitions of its assignment' : 'a standalone reader needs partitions');
    if (request.start && (request.start as Partial<Continuation>).partitionId !== undefined)
      throw responseError(0, INVALID_COMMAND, 'a continuation belongs to one partition and cannot start a reader');
    const client = await getClient();
    if (group) {
      await joinGroup(getClient)({ streamId: request.streamId, topicId: request.topicId, groupId: request.consumer.id });
    }
    const reader = new PartitionReader(getClient, { ...request, partitions: [...new Set(partitions)] });
    client.on('sessionReset', () => { reader.sessionResets += 1; reader.wake(); });
    return reader;
  }

  /**
   * The partitions that the reader reads: those of a standalone consumer, or
   * the current assignment of a group member.
   */
  partitions(): number[] {
    return [...this.states.keys()].sort((left, right) => left - right);
  }

  /**
   * The next batch of one partition, never empty. Rejects with a `ReadError`,
   * after which the reader stays usable. An aborted call leaves the batches in
   * the reader.
   */
  async next({ signal }: { signal?: AbortSignal } = {}): Promise<PolledBatch> {
    return (await this.take(signal, false))!;
  }

  /** @internal As `next`, but undefined once the last read of every partition found no message. */
  nextUntilEnd(): Promise<PolledBatch | undefined> {
    return this.take(undefined, true);
  }

  private async take(signal: AbortSignal | undefined, untilEnd: boolean): Promise<PolledBatch | undefined> {
    this.countUncounted();
    while (true) {
      if (this.closed) throw new Error('the partition reader is closed');
      let entry: Entry | undefined;
      while ((entry = this.queue.shift()) !== undefined) {
        this.release(entry);
        if (!this.isCurrent(entry)) continue;
        this.demand = false;
        if (entry.error) throw entry.error;
        return this.deliver(entry.batch!);
      }
      if (untilEnd && this.states.size > 0 && [...this.states.values()].every((state) => state.atEnd)) {
        this.demand = false;
        return undefined;
      }
      this.demand = true;
      const arrival = this.arrival.promise;
      this.wake();
      await (signal ? Promise.race([arrival, abortOf(signal)]) : arrival);
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<PolledBatch> {
    while (!this.closed) yield await this.next();
  }

  /**
   * Counts a checkpoint that this reader delivered under `CommitPolicy.Auto`.
   * It stores nothing now. Fails with 3 without an automatic policy and for a
   * checkpoint of another reader.
   */
  mark(checkpoint: Checkpoint): void {
    if (this.options.commit.kind !== 'auto')
      throw responseError(0, INVALID_COMMAND, 'only an automatic commit policy counts marks');
    this.ensureOwn(checkpoint);
    this.count(checkpoint, 1, false);
  }

  /**
   * Stores the offset of a checkpoint that this reader delivered. A checkpoint
   * of an older generation of its partition, or at or below the last commit,
   * stores nothing. Fails with 3 with `CommitPolicy.OnRead` and for a
   * checkpoint of another reader.
   */
  async commit(checkpoint: Checkpoint): Promise<void> {
    if (this.options.commit.kind === 'onRead')
      throw responseError(0, INVALID_COMMAND, 'the server stores the offsets of an on read policy');
    this.ensureOwn(checkpoint);
    const state = this.states.get(checkpoint.partitionId);
    if (!state || state.generation !== checkpoint.stamp!.generation ||
        (state.stored && state.stored.offset >= checkpoint.offset))
      return;
    await storePosition(this.getClient)({ ...this.target(), checkpoint });
    this.recordStored(checkpoint);
  }

  /**
   * Sets the next read position of a partition and starts a new generation of
   * it. It stores no offset, drops the queued batches of the partition, ignores
   * its late replies and resumes a stopped partition. With
   * `CommitPolicy.OnRead` it waits for the read of the partition in flight.
   * Fails with 3 for a partition the reader does not read and for a
   * continuation of another partition.
   */
  async seek(partitionId: number, start: PollingStrategy): Promise<void> {
    validatePartition(start, partitionId, 0);
    const state = this.stateOf(partitionId);
    if (this.options.commit.kind === 'onRead' && state.inFlight) await state.idle;
    const position = start.kind === PollingStrategyKind.First ? PollingStrategy.Offset(0n) : start;
    Object.assign(state, {
      position: continuation(partitionId, position.kind, position.value,
        (start as Partial<Continuation>).context ?? (state.position as Partial<Continuation>).context!),
      generation: ++this.generations,
      stopped: undefined, lasting: 0, retryAt: 0, catchUpUntil: undefined, catchUpBatch: undefined, pendingStart: undefined, aloneUntil: undefined,
      counted: undefined, countedSinceStore: 0, stored: undefined,
      delivered: undefined, handed: undefined, firstHanded: undefined
    });
    this.sought.set(partitionId, state.generation);
    if ((start as Partial<Continuation>).context === undefined && (state.position as Partial<Continuation>).context === undefined)
      state.position = position;
    for (let index = this.queue.length - 1; index >= 0; index -= 1) {
      if (this.queue[index].partitionId === partitionId) {
        this.release(this.queue[index]);
        this.queue.splice(index, 1);
      }
    }
    this.wake();
  }

  /**
   * Stops the reader and waits for its reads in flight. With
   * `CommitPolicy.Auto` it counts the last batch under `after: processing`
   * and stores the latest counted position of each partition. With
   * `CommitPolicy.OnRead` it moves the offset of each partition with
   * undelivered messages back to its last delivered message, or before the
   * first undelivered one when it delivered nothing and started at Next. A
   * group reader then leaves its group. Rejects with the first failed store.
   */
  async close(): Promise<void> {
    if (this.closed) return;
    this.countUncounted();
    this.closed = true;
    this.lifetime.abort();
    this.wake();
    this.storeDue.resolve();
    await this.worker;
    await Promise.allSettled([...this.reads]);
    await this.committer;
    let failure: unknown;
    if (this.options.commit.kind === 'onRead') failure = await this.rewind();
    else if (this.options.commit.kind === 'auto') failure = await this.flushCounted();
    if (this.group) {
      try {
        await leaveGroup(this.getClient)({ streamId: this.request.streamId, topicId: this.request.topicId,
          groupId: this.request.consumer.id });
      } catch {
        // The session or the member may be gone already, which ends the membership too.
      }
    }
    this.queue.length = 0;
    this.arrival.resolve();
    if (failure !== undefined) throw failure;
  }

  /** @internal Records the last message that a consumer handed over. */
  delivered(checkpoint: Checkpoint): void {
    const state = this.states.get(checkpoint.partitionId);
    if (state && state.generation === checkpoint.stamp?.generation)
      state.delivered = { offset: checkpoint.offset, context: checkpoint.context };
  }

  /**
   * @internal Takes back a batch that `next` returned and nobody read, so a
   * close under `CommitPolicy.OnRead` moves the offset back before it.
   */
  returned(batch: PolledBatch): void {
    const generation = batch.checkpoint()?.stamp?.generation;
    const state = this.states.get(batch.partitionId);
    if (!state || generation === undefined || state.generation !== generation) return;
    const first = batch.messages[0].headers.offset;
    if (state.firstHanded !== undefined && first > state.firstHanded) {
      state.handed = { offset: first - 1n, context: batch.context };
      state.delivered = state.handed;
    } else {
      state.handed = state.delivered = state.firstHanded = undefined;
    }
    this.queue.unshift({ partitionId: batch.partitionId, generation, batch, messages: batch.messages.length,
      bytes: batch.messages.reduce((sum, message) => sum + message.payload.length, 0) });
    this.queuedMessages += batch.messages.length;
  }

  /** @internal Counts messages that a consumer delivered or processed. */
  countDelivered(checkpoint: Checkpoint, messages: number, endsBatch: boolean): void {
    this.count(checkpoint, messages, endsBatch);
  }

  private newState(): PartitionState {
    return {
      position: this.start, generation: ++this.generations, inFlight: false, retryAt: 0, lasting: 0, countedSinceStore: 0,
      servedAt: 0
    };
  }

  private target() {
    return { streamId: this.request.streamId, topicId: this.request.topicId, consumer: this.request.consumer };
  }

  private stateOf(partitionId: number): PartitionState {
    const state = this.states.get(partitionId);
    if (!state) throw responseError(0, INVALID_COMMAND, `the reader does not read partition ${partitionId}`);
    return state;
  }

  private ensureOwn(checkpoint: Checkpoint): void {
    if (checkpoint.stamp?.reader !== this.id)
      throw responseError(0, INVALID_COMMAND, 'the checkpoint was not delivered by this reader');
  }

  private deliver(batch: PolledBatch): PolledBatch {
    const checkpoint = batch.checkpoint()!;
    const state = this.states.get(batch.partitionId);
    if (state && state.generation === checkpoint.stamp?.generation) {
      state.firstHanded ??= batch.messages[0].headers.offset;
      state.handed = { offset: checkpoint.offset, context: batch.context };
      if (!this.countsMessages) state.delivered = state.handed;
    }
    const commit = this.options.commit;
    if (this.countsMessages || commit.kind !== 'auto') return batch;
    if (commit.after === 'delivery') this.count(checkpoint, batch.messages.length, true);
    else if (commit.after === 'processing') {
      this.uncounted = checkpoint;
      this.uncountedMessages = batch.messages.length;
    }
    return batch;
  }

  private countUncounted(): void {
    const uncounted = this.uncounted;
    this.uncounted = undefined;
    if (uncounted) this.count(uncounted, this.uncountedMessages, true);
  }

  private count(checkpoint: Checkpoint, messages: number, endsBatch: boolean): void {
    const commit = this.options.commit;
    const state = this.states.get(checkpoint.partitionId);
    if (commit.kind !== 'auto' || !state || state.generation !== checkpoint.stamp?.generation) return;
    if (!state.counted || state.counted.offset < checkpoint.offset) state.counted = checkpoint;
    state.countedSinceStore += messages;
    const every = commit.every;
    const due = every?.kind === 'message' ||
      (every?.kind === 'messages' && state.countedSinceStore >= every.count) ||
      (every?.kind === 'batch' && endsBatch);
    if (due) this.storeDue.resolve();
  }

  private recordStored(checkpoint: Checkpoint): void {
    const state = this.states.get(checkpoint.partitionId);
    if (state && state.generation === checkpoint.stamp?.generation &&
        (!state.stored || state.stored.offset < checkpoint.offset)) {
      state.stored = checkpoint;
      if (!state.counted || state.counted.offset <= checkpoint.offset) state.countedSinceStore = 0;
    }
  }

  private async commitLoop(): Promise<void> {
    const commit = this.options.commit;
    const interval = commit.kind === 'auto' ? commit.interval : undefined;
    while (!this.closed) {
      await Promise.race([this.storeDue.promise, ...(interval === undefined ? [] : [delay(interval)])]);
      this.storeDue = resolvers();
      if (this.closed) return;
      await this.flushCounted();
    }
  }

  /** Stores the latest counted position of each partition ahead of its last store, one at a time. */
  private async flushCounted(): Promise<unknown> {
    const due = [...this.states.values()]
      .filter((state) => state.counted && state.counted.stamp?.generation === state.generation &&
        (!state.stored || state.stored.offset < state.counted.offset))
      .map((state) => state.counted!);
    let failure: unknown;
    for (const checkpoint of due) {
      try {
        await storePosition(this.getClient)({ ...this.target(), checkpoint });
        this.recordStored(checkpoint);
      } catch (error) {
        failure ??= error;
      }
    }
    return failure;
  }

  /**
   * With `CommitPolicy.OnRead` the server stored the offsets of messages that
   * the application never took, so each such partition moves its offset back.
   */
  private async rewind(): Promise<unknown> {
    const queued = new Map<number, PolledBatch>();
    for (const entry of this.queue)
      if (entry.batch && this.isCurrent(entry) && !queued.has(entry.batch.partitionId))
        queued.set(entry.batch.partitionId, entry.batch);
    let failure: unknown;
    for (const [partitionId, state] of this.states) {
      const handedOver = state.handed && (!state.delivered || state.delivered.offset < state.handed.offset);
      const first = queued.get(partitionId) ?? state.catchUpBatch;
      if (!handedOver && !first && !state.pendingStart) continue;
      let offset: bigint | null | undefined;
      let context: PartitionContext | undefined;
      if (state.delivered) {
        offset = state.delivered.offset;
        context = state.delivered.context;
      } else if (this.start.kind === PollingStrategyKind.Next) {
        const firstOffset = state.firstHanded ?? first?.messages[0].headers.offset ?? state.pendingStart!.value;
        offset = firstOffset === 0n ? null : firstOffset - 1n;
        context = state.handed?.context ?? first?.context ?? state.pendingStart!.context;
      }
      if (offset === undefined) continue;
      try {
        if (offset === null) {
          await deleteOffset(this.getClient)({ ...this.target(), partitionId, context });
        } else {
          await storeOffset(this.getClient)({ ...this.target(), partitionId, offset, context });
        }
      } catch (error) {
        if (!(offset === null && errorCode(error) === CONSUMER_OFFSET_NOT_FOUND)) failure ??= error;
      }
    }
    return failure;
  }

  private wake(): void {
    this.wakeUp.resolve();
  }

  private async run(): Promise<void> {
    while (!this.closed) {
      let wait: number | undefined;
      let failed = false;
      try {
        if (this.group) await this.refreshAssignment();
        wait = await this.startReads();
      } catch (error) {
        if (this.closed) return;
        const code = errorCode(error);
        if (!(code !== undefined && TRANSIENT_REFUSALS.has(code)) && !(error instanceof OperationUncertainError))
          this.report(undefined, 0, error);
        wait = this.retryInterval;
        failed = true;
      }
      // A caller that asks again after a failure does not cut its retry interval short. The pause
      // keeps the process alive like a read in flight, until the reader closes.
      const woken = failed ? this.closing : this.wakeUp.promise;
      await Promise.race([woken, ...(wait === undefined ? []
        : [sleep(wait, undefined, { signal: this.lifetime.signal }).catch(() => undefined)])]);
      this.wakeUp = resolvers();
    }
  }

  private async refreshAssignment(): Promise<void> {
    const { streamId, topicId, consumer } = this.request;
    if (this.joinedResets !== this.sessionResets) {
      const resets = this.sessionResets;
      await joinGroup(this.getClient)({ streamId, topicId, groupId: consumer.id });
      this.joinedResets = resets;
      this.resync = true;
    }
    let assignment;
    try {
      assignment = await syncGroup(this.getClient)({ streamId, topicId, groupId: consumer.id });
    } catch (error) {
      if (errorCode(error) === CONSUMER_GROUP_MEMBER_NOT_FOUND) {
        await joinGroup(this.getClient)({ streamId, topicId, groupId: consumer.id });
        assignment = await syncGroup(this.getClient)({ streamId, topicId, groupId: consumer.id });
      } else {
        throw error;
      }
    }
    this.resync = false;
    const owned = new Set(assignment?.partitions ?? []);
    const leaving: Checkpoint[] = [];
    for (const [partition, state] of this.states) {
      if (owned.has(partition)) continue;
      if (state.counted && (!state.stored || state.stored.offset < state.counted.offset)) leaving.push(state.counted);
      this.giveUpGap(partition, state);
      this.states.delete(partition);
      state.settleIdle?.();
    }
    for (const partition of owned)
      if (!this.states.has(partition)) this.states.set(partition, this.newState());
    for (const checkpoint of leaving) {
      try {
        await storePosition(this.getClient)({ ...this.target(), checkpoint });
      } catch {
        // The partition moved to another member, which may refuse this store.
      }
    }
  }

  /** Starts a read for each partition that may read now, and returns how long the worker may sleep. */
  private async startReads(): Promise<number | undefined> {
    const now = Date.now();
    let nextRetry = Number.POSITIVE_INFINITY;
    const grouped: number[] = [];
    const alone: number[] = [];
    const reading = this.options.commit.kind === 'onRead'
      ? this.demand && this.queue.length === 0
      : this.queuedMessages < this.prefetchMessages && this.queuedBytes < this.prefetchBytes;
    if (reading) {
      for (const [partition, state] of this.states) {
        if (state.inFlight || state.stopped) continue;
        if (state.retryAt > now) {
          nextRetry = Math.min(nextRetry, state.retryAt);
          continue;
        }
        const fetchable = (state.position.kind === PollingStrategyKind.Offset ||
          state.position.kind === PollingStrategyKind.Next) && state.catchUpUntil === undefined &&
          state.aloneUntil === undefined && !this.needsStart(state.position);
        (fetchable ? grouped : alone).push(partition);
      }
    }
    for (const partition of alone) this.startPoll(partition);
    // A fetch reads its targets in order until it holds its count, so the partitions served last go last.
    grouped.sort((left, right) => this.states.get(left)!.servedAt - this.states.get(right)!.servedAt || left - right);
    if (grouped.length > 0) {
      const client: RawClient = await this.getClient();
      const groups = client.readinessGroups
        ? await client.readinessGroups(this.request.streamId, this.request.topicId, grouped,
          { signal: this.lifetime.signal })
        : grouped.map((partition) => [partition]);
      for (const group of groups) this.startFetch(group);
    }
    let wait = Number.isFinite(nextRetry) ? Math.max(1, nextRetry - Date.now()) : undefined;
    if (this.group && (wait === undefined || wait > GROUP_REFRESH_MS)) wait = GROUP_REFRESH_MS;
    return wait;
  }

  private claim(partition: number): Target | undefined {
    const state = this.states.get(partition);
    if (!state || state.inFlight || state.stopped) return undefined;
    state.inFlight = true;
    const idle = resolvers();
    state.idle = idle.promise;
    state.settleIdle = idle.resolve;
    return { partition, sent: state.position, generation: state.generation };
  }

  private track(read: Promise<void>): void {
    this.reads.add(read);
    void read.finally(() => {
      this.reads.delete(read);
      this.wake();
    });
  }

  private startPoll(partition: number): void {
    const target = this.claim(partition);
    if (!target) return;
    const state = this.states.get(partition)!;
    const single = state.aloneUntil !== undefined;
    const check = this.needsStart(target.sent);
    const commits = this.options.commit.kind === 'onRead' && state.catchUpUntil === undefined && !single && !check;
    if (commits && (target.sent as Partial<Continuation>).context)
      state.pendingStart ??= target.sent as Continuation;
    this.track(this.poll(target, commits, single, check));
  }

  private startFetch(group: number[]): void {
    const targets = group.map((partition) => this.claim(partition)).filter((target) => target !== undefined);
    if (this.options.commit.kind === 'onRead') {
      for (const target of targets)
        if ((target.sent as Partial<Continuation>).context)
          this.states.get(target.partition)!.pendingStart ??= target.sent as Continuation;
    }
    if (targets.length > 0) this.track(this.fetch(targets));
  }

  private async fetch(targets: Target[]): Promise<void> {
    let sections;
    try {
      sections = await fetchMessages(this.getClient)({
        ...this.target(),
        targets: targets.map(({ partition, sent }) => ({
          partitionId: partition, pollingStrategy: { kind: sent.kind, value: sent.value } as PollingStrategy,
          context: (sent as Partial<Continuation>).context
        })),
        count: this.options.count,
        autocommit: this.options.commit.kind === 'onRead',
        options: wireOptions(this.options, COMMAND_CODE.FetchMessages),
        signal: this.lifetime.signal
      });
    } catch (error) {
      await this.abandonLost(error);
      for (const target of targets) this.refuse(target, error);
      return;
    }
    for (const target of targets) {
      const section = sections.find(({ partitionId }) => partitionId === target.partition);
      if (!section) {
        // A read that does not wait would ask the server again at once.
        this.retry(target, this.options.maxWait === 0 ? this.retryInterval : 0);
        continue;
      }
      if (section.status !== 0) {
        const refusal = fetchSectionError(section);
        await this.abandonLost(refusal);
        this.refuse(target, refusal);
        continue;
      }
      let polled: PollMessagesResponse;
      try {
        polled = decodeFetchSection(section);
      } catch (error) {
        this.readAlone(target, section.nextOffset - BigInt(section.messagesCount), section.nextOffset,
          section.context, error);
        continue;
      }
      this.complete(target, polled, section.nextOffset);
    }
  }

  private needsStart(position: PollingStrategy): boolean {
    return this.options.commit.kind === 'onRead' && position.kind === PollingStrategyKind.Next &&
      (position as Partial<Continuation>).context === undefined;
  }

  private async poll(target: Target, commits: boolean, single: boolean, check = false): Promise<void> {
    const options = { ...this.options, commit: commits ? CommitPolicy.OnRead : CommitPolicy.Manual,
      ...(single || check ? { count: 1, minCount: 1 } : {}) };
    let polled: PolledBatch;
    try {
      polled = await pollMessages(this.getClient)({
        ...this.target(), partitionId: target.partition, strategy: target.sent, options,
        signal: this.lifetime.signal
      });
    } catch (error) {
      // A reply whose records end early fails to decode with a range error.
      const decodeFailed = error instanceof RangeError ||
        (error instanceof Error && error.name === 'DeserializeError');
      if (decodeFailed && target.sent.kind === PollingStrategyKind.Offset) {
        if (single) this.undecodable(target, error);
        else this.readAlone(target, target.sent.value, target.sent.value + BigInt(this.options.count),
          (target.sent as Partial<Continuation>).context, error);
        return;
      }
      this.refuse(target, error);
      return;
    }
    if (check) {
      const state = this.settle(target);
      if (state) {
        state.atEnd = polled.messages.length === 0;
        this.notify();
        if (polled.messages.length > 0)
          state.position = continuation(target.partition, PollingStrategyKind.Next,
            polled.messages[0].headers.offset, polled.context);
        else if (this.options.maxWait === 0) state.retryAt = Date.now() + this.retryInterval;
      }
      return;
    }
    this.complete(target, polled);
  }

  /** Abandons the operation of a lost committing read of this reader, so the next read finds what it stored. */
  private async abandonLost(error: unknown): Promise<void> {
    if (!(error instanceof FetchProgressUncertainError)) return;
    try {
      const client = await this.getClient();
      if (error.operationId !== undefined) client.abandonOperation?.(error.operationId);
    } catch {
      // A later read reports what keeps the partition blocked.
    }
  }

  /** Ends the read of a target. Undefined when the generation of its partition moved on. */
  private settle(target: Target): PartitionState | undefined {
    const state = this.states.get(target.partition);
    if (!state) return undefined;
    state.inFlight = false;
    state.settleIdle?.();
    return state.generation === target.generation ? state : undefined;
  }

  private complete(target: Target, polled: PollMessagesResponse, nextOffset?: bigint): void {
    const state = this.settle(target);
    const onRead = this.options.commit.kind === 'onRead';
    if (!state) {
      // The server stored this read, so a partition that left the assignment still hands it over.
      if (onRead && polled.messages.length > 0 && this.keeps(target.partition, target.generation))
        this.enqueueBatch(target, polledBatch(polled, target.sent, { reader: this.id, generation: target.generation }));
      return;
    }
    if (polled.messages.length > 0) state.pendingStart = undefined;
    state.lasting = 0;
    state.atEnd = polled.messages.length === 0;
    if (polled.messages.length === 0) {
      const context = (target.sent as Partial<Continuation>).context;
      const next = nextOffset ?? polled.currentOffset + 1n;
      if (onRead && target.sent.kind === PollingStrategyKind.Next && context &&
          sameHistory(context, polled.context) && next > target.sent.value) {
        state.position = continuation(target.partition, PollingStrategyKind.Offset, target.sent.value, polled.context);
        state.catchUpUntil = next - 1n;
        state.atEnd = false;
        return;
      }
      this.notify();
      if (target.sent.kind !== PollingStrategyKind.Next || !onRead)
        state.position = polledBatch(polled, target.sent).continuation();
      if (this.options.maxWait === 0) state.retryAt = Date.now() + this.retryInterval;
      return;
    }
    const first = polled.messages[0].headers.offset;
    const last = polled.messages.at(-1)!.headers.offset;
    const sentContext = (target.sent as Partial<Continuation>).context;
    if (onRead && target.sent.kind === PollingStrategyKind.Next && sentContext &&
        first > target.sent.value && sameHistory(sentContext, polled.context)) {
      // A lost read stored these offsets without handing them over.
      state.position = continuation(target.partition, PollingStrategyKind.Offset, target.sent.value, polled.context);
      state.catchUpUntil = last;
      state.catchUpBatch = polled;
      return;
    }
    const next = last + 1n;
    const byOffset = (state.aloneUntil !== undefined && next < state.aloneUntil) ||
      (state.catchUpUntil !== undefined && next <= state.catchUpUntil);
    if (state.aloneUntil !== undefined && next >= state.aloneUntil) state.aloneUntil = undefined;
    if (state.catchUpUntil !== undefined && next > state.catchUpUntil) {
      state.catchUpUntil = undefined;
      state.catchUpBatch = undefined;
    }
    state.position = continuation(target.partition,
      onRead && !byOffset ? PollingStrategyKind.Next : PollingStrategyKind.Offset, next, polled.context);
    state.servedAt = ++this.serves;
    this.enqueueBatch(target, polledBatch(polled, target.sent, { reader: this.id, generation: target.generation }));
  }

  private enqueueBatch(target: Target, batch: PolledBatch): void {
    this.enqueue({
      partitionId: target.partition, generation: target.generation, batch,
      messages: batch.messages.length,
      bytes: batch.messages.reduce((sum, message) => sum + message.payload.length, 0)
    });
  }

  /** A section or a poll that did not decode: its partition reads that range one message at a time. */
  private readAlone(target: Target, from: bigint, until: bigint, context: PartitionContext | undefined,
    error: unknown): void {
    void error;
    const state = this.settle(target);
    if (!state) return;
    state.position = context ? continuation(target.partition, PollingStrategyKind.Offset, from, context)
      : PollingStrategy.Offset(from);
    state.aloneUntil = until;
  }

  /** The message at the sent offset does not decode. The partition reads it again until a seek past it. */
  private undecodable(target: Target, error: unknown): void {
    const state = this.settle(target);
    if (!state) return;
    state.aloneUntil ??= target.sent.value + 1n;
    state.retryAt = Date.now() + this.retryInterval;
    const context = (target.sent as Partial<Continuation>).context;
    const skip = continuation(target.partition, PollingStrategyKind.Offset, target.sent.value + 1n,
      context ?? { incarnation: 0n, ownerGeneration: 0n, metadataOp: 0n });
    this.enqueue({ partitionId: target.partition, generation: target.generation,
      error: new ReadError(target.partition, error, skip), messages: 0, bytes: 0 });
  }

  /**
   * A read that did not return its partition. A refusal before the read
   * retries, and only a lasting or a history refusal stops the partition.
   */
  private refuse(target: Target, error: unknown): void {
    const state = this.settle(target);
    if (!state || this.closed) return;
    const now = Date.now();
    const code = errorCode(error);
    const report = (stop: boolean) => {
      const readError = new ReadError(target.partition, error);
      if (stop) state.stopped = readError;
      this.enqueue({ partitionId: target.partition, generation: target.generation, error: readError,
        messages: 0, bytes: 0 });
    };
    if (isAbort(error)) return;
    if (code === FETCH_PROGRESS_UNCERTAIN || error instanceof OperationUncertainError || code === NOT_PRIMARY) return;
    if (code === CONSUMER_GROUP_PARTITION_NOT_OWNED) {
      this.resync = true;
      state.retryAt = now + this.retryInterval;
      this.giveUpGap(target.partition, state);
      return;
    }
    if (code === HISTORY_UNAVAILABLE && (target.sent as Partial<Continuation>).context === undefined) {
      state.retryAt = now + this.retryInterval;
      return;
    }
    if (code === HISTORY_UNAVAILABLE || code === INVALID_SIZE_BYTES) {
      report(true);
      return;
    }
    if (code !== undefined && LASTING_REFUSALS.has(code)) {
      state.lasting += 1;
      if (state.lasting >= MAX_LASTING_REFUSALS) {
        report(true);
        return;
      }
      state.retryAt = now + this.retryInterval * 2 ** (state.lasting - 1);
      return;
    }
    state.retryAt = now + this.retryInterval;
    if (code !== undefined && TRANSIENT_REFUSALS.has(code)) return;
    if (code === undefined) {
      this.report(undefined, 0, error);
      return;
    }
    report(false);
  }

  private giveUpGap(partitionId: number, state: PartitionState): void {
    const held = state.catchUpBatch;
    if (state.catchUpUntil === undefined) return;
    const target = { partition: partitionId, generation: state.generation, sent: state.position };
    const first = held?.messages[0].headers.offset ?? state.catchUpUntil + 1n;
    if (state.position.value < first) {
      const lost = responseError(COMMAND_CODE.PollMessagesDeferred, FETCH_PROGRESS_UNCERTAIN,
        `a lost read stored offsets ${state.position.value} to ${first - 1n} of partition ${partitionId}, ` +
        'and the partition moved before they were read again');
      this.enqueue({ partitionId, generation: state.generation,
        error: new ReadError(partitionId, lost), messages: 0, bytes: 0 });
    }
    const messages = held?.messages.filter(({ headers }) => headers.offset >= state.position.value) ?? [];
    const next = held ? held.messages.at(-1)!.headers.offset + 1n : state.catchUpUntil + 1n;
    const context = held?.context ?? (state.position as Continuation).context;
    state.catchUpUntil = undefined;
    state.catchUpBatch = undefined;
    state.position = continuation(partitionId, PollingStrategyKind.Next, next, context);
    if (held && messages.length > 0)
      this.enqueueBatch(target, polledBatch({ ...held, messages }, target.sent,
        { reader: this.id, generation: target.generation }));
  }

  private retry(target: Target, after: number): void {
    const state = this.settle(target);
    if (state) state.retryAt = after === 0 ? 0 : Date.now() + after;
  }

  private report(partitionId: number | undefined, generation: number, error: unknown): void {
    // One error of the whole reader at a time, so a lasting outage does not fill the queue.
    if (partitionId === undefined && this.queue.some((entry) => entry.error && entry.partitionId === undefined)) return;
    this.enqueue({ partitionId, generation, error: new ReadError(partitionId, error), messages: 0, bytes: 0 });
  }

  private enqueue(entry: Entry): void {
    this.queue.push(entry);
    this.queuedMessages += entry.messages;
    this.queuedBytes += entry.bytes;
    this.notify();
  }

  /** Wakes a caller that waits in `next`. */
  private notify(): void {
    const arrival = this.arrival;
    this.arrival = resolvers();
    arrival.resolve();
  }

  private release(entry: Entry): void {
    this.queuedMessages -= entry.messages;
    this.queuedBytes -= entry.bytes;
  }

  private isCurrent(entry: Entry): boolean {
    return entry.partitionId === undefined || this.keeps(entry.partitionId, entry.generation);
  }

  /**
   * Whether a read of `partition` under `generation` still counts. Only a seek
   * drops stored reads under `CommitPolicy.OnRead`, including across reassignment.
   */
  private keeps(partition: number, generation: number): boolean {
    const state = this.states.get(partition);
    return state?.generation === generation || (this.options.commit.kind === 'onRead' &&
      generation >= (this.sought.get(partition) ?? 0));
  }
}

/** Opens a `PartitionReader` on the client. */
export const openReader = (getClient: ClientProvider) =>
  (request: PartitionReaderRequest): Promise<PartitionReader> => PartitionReader.open(getClient, request);

type Resolvers = { promise: Promise<void>, resolve: () => void };

const resolvers = (): Resolvers => {
  let resolve!: () => void;
  const promise = new Promise<void>((settle) => { resolve = settle; });
  return { promise, resolve };
};

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds).unref());

const abortOf = (signal: AbortSignal): Promise<never> => new Promise((_, reject) => {
  if (signal.aborted) reject(signal.reason);
  else signal.addEventListener('abort', () => reject(signal.reason), { once: true });
});

