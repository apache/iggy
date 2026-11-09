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
import { debug } from '../client/client.debug.js';
import type { ClientProvider, RawClient } from '../client/client.type.js';
import {
  FetchProgressUncertainError, OperationNotRetainedError, OperationUncertainError
} from '../client/client.operation.js';
import { COMMAND_CODE } from '../wire/command.code.js';
import { DeserializeError, ResponseError, responseError } from '../wire/error.utils.js';
import type { Id } from '../wire/identifier.utils.js';
import { ConsumerKind, type Consumer, type OffsetResponse } from '../wire/offset/offset.utils.js';
import { storeOffset } from '../wire/offset/store-offset.command.js';
import { getOffset } from '../wire/offset/get-offset.command.js';
import { deleteOffset } from '../wire/offset/delete-offset.command.js';
import { joinGroup } from '../wire/consumer-group/join-group.command.js';
import { leaveGroup } from '../wire/consumer-group/leave-group.command.js';
import { syncGroup } from '../wire/consumer-group/sync-group.command.js';
import {
  decodeFetchSection, fetchMessages, fetchSectionError, type FetchMessagesSection
} from '../wire/message/fetch-messages.command.js';
import { pollMessages } from '../wire/message/poll-messages.command.js';
import { PollingStrategy, PollingStrategyKind, type PollMessagesResponse } from '../wire/message/poll.utils.js';
import { CommitPolicy, pollOptions, wireOptions, type PollOptions } from '../wire/message/poll-options.js';
import {
  continuation, polledBatch, validatePartition, withoutEmptyContext, type Checkpoint, type Continuation,
  type PolledBatch
} from '../wire/message/checkpoint.js';
import { EMPTY_PARTITION_CONTEXT, sameContext, type PartitionContext } from '../wire/vsr/header.js';

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
const LASTING_REFUSALS = new Set([UNAUTHORIZED, 5, INVALID_COMMAND, PARTITION_NOT_FOUND, 1009, 1010, 2010, 2011,
  CONSUMER_GROUP_PARTITION_NOT_OWNED]);
const TRANSIENT_REFUSALS = new Set([TRANSIENT_NOT_COMMITTED, TRANSIENT_NOT_ACCEPTED, NOT_PRIMARY,
  DEFERRED_POLL_CAPACITY_EXCEEDED, LIFECYCLE_BUSY]);
const MAX_LASTING_REFUSALS = 6;
/** A read for the installed owner needs only the context of its reply, and a read asks for one message at least. */
const OWNER_PROBE_COUNT = 1;
/** The most reads of one reader in flight, fetches and polls alike, the deferred request limit of a Rust SDK consumer. */
const MAX_READS_IN_FLIGHT = 32;
const MIN_PREFETCH_BATCHES = 32;
const GROUP_REFRESH_MS = 1_000;
/**
 * The pause before the next sync after a join or a changed assignment. A join moves
 * ownership only after the partition logs persist the new owner.
 */
const ASSIGNMENT_SETTLE_PAUSE_MS = 10;
/** A close reads for the installed owner of a refused write up to this long after it starts. */
const CLOSE_BUDGET_MS = 10_000;
const READER_CLOSED = 'the partition reader is closed';
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
  /**
   * Where a partition without a position starts, Next by default. With
   * `CommitPolicy.OnRead` the start applies only to a partition without a
   * stored offset. A partition with one resumes after it. A Last start reads
   * the last `count` messages. With `CommitPolicy.OnRead` a poll that stores
   * nothing finds the offset of a timestamp or Last position first, so
   * `close()` can move the offset back before it.
   */
  start?: PollingStrategy,
  /** The limits of each read and the commit policy. A read waits up to 5 s by default. */
  options?: Partial<PollOptions>,
  /**
   * Requested read-ahead bytes, 16 MiB by default, raised to hold at least 32
   * full batches. With `CommitPolicy.OnRead` the server stores the offset of
   * each read before the application takes its batch. A crash, or a reader
   * that stops without `close()`, loses the batches that the reader held: up
   * to the effective budget, which holds at least 32 batches.
   * `CommitPolicy.Auto` or `CommitPolicy.Manual` stores offsets after delivery
   * instead.
   */
  prefetchBytes?: number,
  /**
   * Requested read-ahead messages, 16,000 by default, raised to hold at least
   * 32 full batches. With `CommitPolicy.OnRead` a crash can lose the batches
   * that the reader held, as `prefetchBytes` explains.
   */
  prefetchMessages?: number,
  /** The wait in milliseconds before a refused partition reads again, 1 s by default. A finite number above 0. */
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
    readonly skip?: Continuation,
    /** The reader stopped the partition, which it reads again only after a `seek`. */
    readonly stopsPartition = false,
    /**
     * The reader stopped the partition after refusals in a row or a lost
     * history. A `seek` to Next reads it again after its stored offset.
     */
    readonly resumable = false,
    /**
     * For messages that a lost read stored and that the reader cannot read
     * again: their first and last offset. Undefined for every other error.
     */
    readonly lost?: { readonly first: bigint, readonly last: bigint }
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

type Target = {
  partition: number, sent: PollingStrategy, generation: number, state?: PartitionState, session?: number,
  /** The read stores the offset of what it returns. */
  commits?: boolean
};

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
  /** The position is the configured start, which applies only while the server holds no offset of the partition. */
  lookupDue?: boolean,
  /** The start lookup found no stored offset, and no committing read of the partition started since. */
  emptyAtStart?: boolean,
  generation: number,
  inFlight: boolean,
  stopped?: ReadError,
  retryAt: number,
  lasting: number,
  catchUpUntil?: bigint,
  catchUpBatch?: PollMessagesResponse,
  /**
   * A committing read of the partition lost its reply, so a later read that skips offsets skipped what it stored. The
   * end of the catch-up of that range ends it, and a skip after that comes from another store or from retention.
   */
  lost?: boolean,
  /**
   * The start of a committing read whose store nobody took yet, so the close moves the offset back before it. With
   * `restores`, a seek write with an unknown outcome came after it, and the close moves the offset back before this
   * point without a check: the earlier of that start and the offset after the one that the server held before the
   * write, or 0 when the server held none.
   */
  pendingStart?: PollingStrategy & { context?: PartitionContext, restores?: boolean },
  aloneUntil?: bigint,
  delivered?: { offset: bigint, context?: PartitionContext },
  handed?: { offset: bigint, context?: PartitionContext },
  firstHanded?: bigint,
  counted?: Checkpoint,
  countedSinceStore: number,
  stored?: Checkpoint,
  /** The last read of the partition found no message. */
  atEnd?: boolean,
  /** Its last fetch section ended before the head of the partition, so its next read returns at once. */
  backlogged?: boolean,
  /** When a read last served messages of the partition, so served partitions go last in the next fetch. */
  servedAt: number
};

let readers = 0;

/** Owner 0 names the owner that the server installed, so it matches the owner of each reply. */
const sameHistory = (sent: PartitionContext, served: PartitionContext): boolean =>
  sent.incarnation === served.incarnation &&
  (sent.ownerGeneration === 0n || sent.ownerGeneration === served.ownerGeneration);

/**
 * Whether a reply comes from the history that its read names, or from any history for a read that names none, as with
 * a context that is all zero. The fail-fast reply of a server that could not serve a read has a zero context and comes
 * from no history. So when it is empty, it shows neither the end of the partition nor a lost range.
 */
const servedInHistory = (sent: PartitionContext | undefined, served: PartitionContext): boolean =>
  !sameContext(served, EMPTY_PARTITION_CONTEXT) &&
  (sent === undefined || sameContext(sent, EMPTY_PARTITION_CONTEXT) || sameHistory(sent, served));

/** An empty partition reports current offset 0, so only a section with messages or a later head shows a backlog. */
const endsBeforeHead = ({ nextOffset, currentOffset, messagesCount }: FetchMessagesSection): boolean =>
  nextOffset <= currentOffset && (messagesCount > 0 || currentOffset > 0n);

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
 *   reads them again by offset without a commit. A partition that leaves the
 *   assignment still hands over the batches that the server stored for it. If
 *   a lost read of that partition stored a range that the reader did not read
 *   again, `next()` first rejects with a `ReadError` of error 14010 that names
 *   that range. `close()` does not move the offset of a partition that left,
 *   so its batches that `next()` did not return are lost for the group. With
 *   `CommitPolicy.Auto` the reader counts each batch as its `after` says, or
 *   each `mark`, and stores the latest counted position at its interval or
 *   event and in `close()`.
 * - A `ReadError` leaves the reader usable. A history refusal (87) of a
 *   position from an earlier read, an oversized `Last` start (80) and a refusal
 *   other than 5009 that six retries did not change stop their partition until
 *   a `seek`. Each sixth 5009 in a row gives one `ReadError` and stops nothing.
 */
export class PartitionReader implements AsyncIterable<PolledBatch> {
  private readonly id = ++readers;
  private readonly states = new Map<number, PartitionState>();
  private readonly sought = new Map<number, number>();
  /**
   * The lowest owner generation that a read of the current session served for each partition, with that session. A new
   * session installs the owner again with a higher generation, so a context below it comes from an earlier session.
   */
  private readonly owners = new Map<number, { session: number, generation: bigint }>();
  private readonly queue: Entry[] = [];
  /**
   * The reads in flight, each with the partitions whose offsets it commits,
   * until the client released its request. A read of a partition that left
   * the assignment stays here.
   */
  private readonly reads = new Map<Promise<void>, number[]>();
  /**
   * The end of the last seek of each partition in progress. No read of the
   * partition starts before it, and the next seek of the partition waits for
   * it, so no read and no other seek crosses the offset writes of a seek.
   */
  private readonly seeking = new Map<number, Promise<void>>();
  private readonly lifetime = new AbortController();
  private readonly closing = new Promise<void>((resolve) =>
    this.lifetime.signal.addEventListener('abort', () => resolve(), { once: true }));
  private readonly options: PollOptions;
  private readonly start: PollingStrategy;
  /** Under `CommitPolicy.OnRead` a start other than Next applies only to a partition without a stored offset. */
  private readonly startNeedsLookup: boolean;
  private readonly group: boolean;
  private readonly prefetchBytes: number;
  private readonly prefetchMessages: number;
  private readonly retryInterval: number;
  private arrival = resolvers();
  private wakeUp = resolvers();
  private storeDue = resolvers();
  /** The `next()` calls that wait for a batch. An on read reader reads only while one waits. */
  private waiting = 0;
  private closed = false;
  private closure?: Promise<void>;
  /** A join came after the last sync. `open` joins before the first one. */
  private resync = true;
  /** A refusal 5009 showed that the assignment may have moved, so the next pass syncs at once. */
  private syncRequested = false;
  /** A read found the member gone, so the next pass joins again before it syncs. */
  private memberLost = false;
  /**
   * A join with an unknown outcome. It holds the metadata lane of the client, so the next join resolves it first,
   * and the close abandons it while its outcome stays unknown.
   */
  private uncertainJoin?: OperationUncertainError;
  /** The pause after the last sync before the next one is due. */
  private syncPause = ASSIGNMENT_SETTLE_PAUSE_MS;
  /** When the last sync was answered, on the monotonic clock, so a wall clock step cannot hold the syncs back. */
  private syncedAt = 0;
  private queuedMessages = 0;
  private queuedBytes = 0;
  private generations = 0;
  private serves = 0;
  /**
   * The readiness groups of all partitions of the reader from the last good
   * grouping, so each group keeps a request whatever its partitions do. Empty
   * while the client has none.
   */
  private groups: number[][] = [];
  /** The assignment changes, NotPrimary refusals and session resets, each of which groups the partitions again. */
  private routeChanges = 0;
  /** The route changes that `groups` follow, undefined before the first grouping. */
  private groupedChanges?: number;
  /**
   * The grouping in flight, with the retry pause after a failed one. The reads
   * never wait for it, so a slow or failed grouping keeps the last good groups.
   */
  private regrouping?: Promise<void>;
  /** A grouping failed since the last good one, so the partitions in no group poll alone. */
  private regroupFailed = false;
  /** The index of the group that fetches first in the next turn. */
  private groupCursor = 0;
  /** The partition of the last poll alone, so the next one starts after it. */
  private pollCursor?: number;
  private pollsInFlight = 0;
  private uncounted?: Checkpoint;
  private uncountedMessages = 0;
  private sessionResets = 0;
  private joinedResets = 0;
  private readonly worker: Promise<void>;
  private readonly committer: Promise<void>;
  /** Removes the session reset listener, so a long-lived client keeps no closed reader and wakes none. */
  private detachFromClient?: () => void;
  /** @internal Set by a consumer, which reports and counts each delivered message itself. */
  countsMessages = false;

  private constructor(
    private readonly getClient: ClientProvider,
    private readonly request: PartitionReaderRequest
  ) {
    this.options = pollOptions({ maxWait: DEFAULT_READER_MAX_WAIT_MS, ...request.options });
    const onRead = this.options.commit.kind === 'onRead';
    const start = request.start ?? PollingStrategy.Next;
    // Without a stored offset the server answers Next from offset 0, so under OnRead First resumes as Next does.
    this.start = start.kind !== PollingStrategyKind.First ? start
      : onRead ? PollingStrategy.Next : PollingStrategy.Offset(0n);
    this.startNeedsLookup = onRead && this.start.kind !== PollingStrategyKind.Next;
    this.group = request.consumer.kind === ConsumerKind.Group;
    this.prefetchBytes = Math.max(request.prefetchBytes ?? DEFAULT_PREFETCH_BYTES,
      MIN_PREFETCH_BATCHES * this.options.maxBytes);
    this.prefetchMessages = Math.max(request.prefetchMessages ?? DEFAULT_PREFETCH_MESSAGES,
      MIN_PREFETCH_BATCHES * this.options.count);
    this.retryInterval = request.retryInterval ?? 1_000;
    for (const partition of request.partitions ?? [])
      this.states.set(partition, this.newState());
    this.worker = this.run();
    this.committer = this.options.commit.kind === 'auto' ? this.commitLoop() : Promise.resolve();
  }

  /**
   * Checks the request, joins the group of a group reader and starts reading.
   * A standalone consumer without partitions, a group with partitions and a
   * continuation as the start fail with 3. So do a read-ahead budget that is
   * NaN, infinite or negative and a `retryInterval` that is not a finite
   * number above 0. A join with an unknown outcome fails the open, which
   * releases the join in the client, so a later open can join. A join that
   * landed anyway keeps its member in the group until the session ends.
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
    for (const name of ['prefetchBytes', 'prefetchMessages'] as const) {
      const budget = request[name];
      if (budget != null && !(Number.isFinite(budget) && budget >= 0))
        throw responseError(0, INVALID_COMMAND, `${name} must be a finite number of at least 0, not ${budget}`);
    }
    const { retryInterval } = request;
    if (retryInterval != null && !(Number.isFinite(retryInterval) && retryInterval > 0))
      throw responseError(0, INVALID_COMMAND, `retryInterval must be a finite number above 0, not ${retryInterval}`);
    const client = await getClient();
    if (group) {
      try {
        await joinGroup(getClient)({
          streamId: request.streamId, topicId: request.topicId, groupId: request.consumer.id
        });
      } catch (error) {
        // No reader takes this join on, so nobody resolves it later.
        await PartitionReader.abandonUnknown(getClient, error);
        throw error;
      }
    }
    const reader = new PartitionReader(getClient, { ...request, partitions: [...new Set(partitions)] });
    const onSessionReset = () => {
      reader.sessionResets += 1;
      // A reset drops every cached route, and the primaries may have moved meanwhile.
      reader.routeChanges += 1;
      // The new session can serve what the old one refused, so each partition reads at once and counts its refusals
      // again, as at a new epoch of the Rust reader.
      for (const state of reader.states.values()) {
        state.lasting = 0;
        state.retryAt = 0;
      }
      reader.wake();
    };
    client.on('sessionReset', onSessionReset);
    reader.detachFromClient = () => client.off?.('sessionReset', onSessionReset);
    return reader;
  }

  /**
   * @internal Releases the lane that a request with an unknown outcome, the `error`, holds in the client. Otherwise each
   * later request of that lane fails as blocked until a session reset. Any other error leaves the client as it is.
   */
  static async abandonUnknown(getClient: ClientProvider, error: unknown): Promise<void> {
    if (!(error instanceof OperationUncertainError) || error.operationId === undefined) return;
    try {
      (await getClient()).abandonOperation?.(error.operationId);
    } catch {
      // A later request reports what keeps the lane blocked.
    }
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
      if (this.closed) throw new Error(READER_CLOSED);
      let entry: Entry | undefined;
      while ((entry = this.queue.shift()) !== undefined) {
        this.release(entry);
        if (!this.isCurrent(entry)) continue;
        if (entry.error) throw entry.error;
        return this.deliver(entry.batch!);
      }
      if (untilEnd && this.states.size > 0 && [...this.states.values()].every((state) => state.atEnd))
        return undefined;
      const arrival = this.arrival.promise;
      // An on read reader reads only while a call waits, so an aborted call leaves no batch that the server stored and
      // nobody takes.
      this.waiting += 1;
      this.wake();
      try {
        await (signal ? unlessAborted(arrival, signal) : arrival);
      } finally {
        this.waiting -= 1;
      }
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
   * checkpoint of another reader. A store with an unknown outcome fails too,
   * and the reader releases it in the client, so a retry can store. After a
   * session change the store names no owner of the old session, as for a seek.
   * When the server refuses the store as not owned, a read finds the
   * installed owner for it, as for the close. That read ends when `signal`
   * aborts or the close starts, and the commit then fails with the refusal.
   */
  async commit(checkpoint: Checkpoint, { signal }: { signal?: AbortSignal } = {}): Promise<void> {
    if (this.options.commit.kind === 'onRead')
      throw responseError(0, INVALID_COMMAND, 'the server stores the offsets of an on read policy');
    this.ensureOwn(checkpoint);
    const state = this.states.get(checkpoint.partitionId);
    if (!state || state.generation !== checkpoint.stamp!.generation ||
        (state.stored && state.stored.offset >= checkpoint.offset))
      return;
    try {
      await this.storeCheckpoint(checkpoint,
        signal === undefined ? this.lifetime.signal : AbortSignal.any([signal, this.lifetime.signal]));
    } catch (error) {
      await this.abandonLost(error);
      throw error;
    }
    this.recordStored(checkpoint);
  }

  /**
   * Sets the next read position of a partition and starts a new generation of
   * it: it drops the queued batches of the partition, ignores its late replies
   * and resumes a stopped partition. No read of the partition starts until the
   * seek ends, and a seek waits for the seek of the partition before it. With
   * `CommitPolicy.OnRead` the seek first waits for each committing read of the
   * partition in flight, also one from before a new assignment. Then a seek to
   * an offset above 0 stores the offset before it on the server, so a restart
   * resumes at the sought offset. If the server holds an offset, a seek to
   * offset 0 or First deletes it instead, because the commit of a read never
   * moves an offset back. A seek to a timestamp first reads the offset of the
   * first message at that time without a commit, and then seeks to that
   * offset. Without such a message it stores nothing. A refused read, store,
   * lookup or delete fails the seek and moves nothing: the partition keeps its
   * queued batches and reads on from where it was. A store or delete whose
   * outcome is unknown fails the seek too, and the reader handles it as a lost
   * read: it reads skipped messages again, and `close()` moves the offset
   * back. Before its write, a seek from a position without an offset, such as
   * a Next start before the first read, a timestamp or Last, looks up the
   * stored offset. So does a seek from a start that applies because the server
   * held no offset, and a seek of a partition that delivered no message. After
   * a write with an unknown outcome, a Next position reads on from that offset,
   * so it skips no message. Until a later read
   * returns messages, `close()` restores that offset, or deletes the stored
   * offset when the server held none. If an earlier read stored messages that
   * nobody took, `close()` moves the offset back before that read instead.
   * A group reader whose session changed during the write leaves the
   * partition to the new owner and keeps nothing of that write. After a
   * session change, the write of a group reader names no owner of the old
   * session, so the server binds it to the installed owner.
   * Every other seek stores and deletes nothing. A continuation whose context
   * is all zero counts as a start without a context.
   * Fails with 3 for a
   * partition the reader does not read and for a continuation of another
   * partition, and fails after `close()`.
   */
  seek(partitionId: number, start: PollingStrategy): Promise<void> {
    // The close rewinds after the seeks that it found, so a later seek would write beside the rewind.
    if (this.closed) return Promise.reject(new Error(READER_CLOSED));
    const previous = this.seeking.get(partitionId);
    const seek = previous ? previous.then(() => this.seekOnce(partitionId, start)) : this.seekOnce(partitionId, start);
    const ended: Promise<void> = seek.catch(() => undefined).then(() => {
      if (this.seeking.get(partitionId) === ended) this.seeking.delete(partitionId);
      this.wake();
    });
    this.seeking.set(partitionId, ended);
    return seek;
  }

  private async seekOnce(partitionId: number, start: PollingStrategy): Promise<void> {
    validatePartition(start, partitionId, 0);
    start = withoutEmptyContext(start);
    const state = this.stateOf(partitionId);
    const onRead = this.options.commit.kind === 'onRead';
    // The commit of a read in flight could land after the writes of the seek, past the sought offset. A read of an
    // earlier assignment of the partition commits too.
    if (onRead) {
      await Promise.allSettled([...this.reads].filter(([, committed]) => committed.includes(partitionId))
        .map(([read]) => read));
    }
    // The commit of a read from the timestamp would not move a stored offset back, so the seek goes by offset.
    if (onRead && start.kind === PollingStrategyKind.Timestamp) start = await this.firstAt(partitionId, start);
    const position = start.kind === PollingStrategyKind.First ? PollingStrategy.Offset(0n) : start;
    let stored: { offset: bigint, context?: PartitionContext } | undefined;
    if (onRead && position.kind === PollingStrategyKind.Offset) {
      // The seek point keeps the context as served, so each later write names its owner again.
      const context = (start as Partial<Continuation>).context ?? state.handed?.context;
      // A position that names no offset, such as one that follows the stored offset, leaves only the stored offset from
      // before a write with an unknown outcome to tell where the reads and the close go. So does a start that applies
      // because the server held no offset. Without a delivered message, the close would check the stored offset to see
      // whether a read stored messages, and this write may have moved that offset back.
      const lookupDue = state.lookupDue === true;
      const pins = lookupDue || state.emptyAtStart === true || this.needsStart(state.position) || !state.delivered;
      const session = this.sessionResets;
      // The server refuses a delete of a missing offset, so a seek to 0 looks the offset up as well.
      const before = pins || position.value === 0n
        ? await getOffset(this.getClient)({ ...this.target(), partitionId }) : null;
      try {
        // The close waits for the seek, and a seek takes no signal, so the close ends its reads for the owner.
        if (position.value > 0n) {
          stored = { offset: position.value - 1n, context };
          await this.writeOffset(partitionId, stored.offset, context, this.lifetime.signal);
        } else if (before !== null) {
          await this.writeOffset(partitionId, null, context, this.lifetime.signal);
        }
      } catch (error) {
        await this.abandonLost(error);
        // The write may have moved the offset, so the partition recovers as from a lost read of its position. A new
        // session gives a group partition a new owner, which reads on from the progress of the server instead.
        if (error instanceof OperationUncertainError && (!this.group || session === this.sessionResets)) {
          if (pins) this.pin(state, before, lookupDue);
          state.lost = true;
          // A position that its probe resolves names no offset that the close could move back before.
          if (!this.needsStart(state.position)) state.pendingStart ??= state.position;
        }
        throw error;
      }
    }
    Object.assign(state, {
      position: continuation(partitionId, position.kind, position.value,
        (start as Partial<Continuation>).context ?? (state.position as Partial<Continuation>).context!),
      generation: ++this.generations, lookupDue: undefined, emptyAtStart: undefined,
      stopped: undefined, lasting: 0, retryAt: 0, catchUpUntil: undefined, catchUpBatch: undefined, lost: undefined,
      pendingStart: undefined, aloneUntil: undefined, backlogged: undefined,
      counted: undefined, countedSinceStore: 0, stored: undefined,
      delivered: stored, handed: stored, firstHanded: undefined
    });
    this.sought.set(partitionId, state.generation);
    // Under OnRead a seek to Next drops the old context, so the check poll of the start finds the stored offset.
    if ((start as Partial<Continuation>).context === undefined && ((onRead && position.kind === PollingStrategyKind.Next) ||
        (state.position as Partial<Continuation>).context === undefined))
      state.position = position;
    for (let index = this.queue.length - 1; index >= 0; index -= 1) {
      if (this.queue[index].partitionId === partitionId) {
        this.release(this.queue[index]);
        this.queue.splice(index, 1);
      }
    }
  }

  /**
   * Gives a position that follows the stored offset the offset that it followed before a seek write with an unknown
   * outcome. A start whose lookup was due ends that lookup with the offset from before the write. Every other position
   * stays, and the close restores the offset from before the write: without one, its lookup-then-delete leaves none,
   * so a restart applies the start again. The first such write sets the point to restore. The start of a committing
   * read whose messages nobody took stays that point when it comes first, because a lost read stores past messages
   * that nobody delivered. The close then skips its check of the stored offset, which the write may have moved back.
   */
  private pin(state: PartitionState, before: OffsetResponse | null, lookupDue: boolean): void {
    const resume = PollingStrategy.Offset(before === null ? 0n : before.storedOffset + 1n);
    if (lookupDue) this.endLookup(state, before, resume);
    if (state.position.kind === PollingStrategyKind.Next && this.needsStart(state.position)) state.position = resume;
    const pending = state.pendingStart;
    if (!pending?.restores)
      state.pendingStart = { ...(pending && pending.value < resume.value ? pending : resume), restores: true };
  }

  /**
   * An offset continuation at the first message at or after a timestamp, read
   * without a commit, or the timestamp while no message is that late. Only a
   * served empty reply shows that no message is that late, so after any other
   * empty reply the probe reads again after the retry interval.
   */
  private async firstAt(partitionId: number, timestamp: PollingStrategy): Promise<PollingStrategy> {
    const sent = (timestamp as Partial<Continuation>).context;
    for (;;) {
      // Without a message at the timestamp every later one lies after the stored offset, so the probe does not wait.
      const probe = await pollMessages(this.getClient)({
        ...this.target(), partitionId, strategy: timestamp,
        options: { ...this.options, commit: CommitPolicy.Manual, count: 1, minCount: 1, maxWait: 0 },
        signal: this.lifetime.signal
      });
      const first = probe.messages[0];
      if (first !== undefined)
        return continuation(partitionId, PollingStrategyKind.Offset, first.headers.offset, probe.context);
      if (servedInHistory(sent, probe.context)) return timestamp;
      await sleep(this.retryInterval, undefined, { signal: this.lifetime.signal });
    }
  }

  /** Stores the offset of a checkpoint as `writeOffset` does. */
  private storeCheckpoint(checkpoint: Checkpoint, cancel?: AbortSignal): Promise<void> {
    return this.writeOffset(checkpoint.partitionId, checkpoint.offset, checkpoint.context, cancel);
  }

  /** Deletes the stored offset of a partition if the server holds one, since it refuses a delete of a missing offset. */
  private async deleteStored(
    partitionId: number, context: PartitionContext | undefined, cancel?: AbortSignal
  ): Promise<void> {
    if (await getOffset(this.getClient)({ ...this.target(), partitionId }) !== null)
      await this.writeOffset(partitionId, null, context, cancel);
  }

  /**
   * Stores `offset` for a partition, or deletes its stored offset when `offset` is null, under the context that
   * `owned` gives. No partition has history 0, so for a context of history 0 the client names the history of its
   * route. A group refuses the write as not owned when it installed the owner after the metadata op of
   * the write, as in a rebalance that another member starts, and no reply to this client names that op. So the
   * refusal alone is not a leave either. A partition that left its assignment since the write started keeps the
   * refusal. Otherwise its owner counts as lost, the reader asks for a refresh of the assignment, and
   * `installedOwner` reads the context of the installed owner. If the server serves that read in the incarnation of
   * the write, or the write names no history, the write goes once more under the owner and op of the read. Otherwise
   * the write fails with its refusal. `cancel` ends the reads of `installedOwner`. After it aborts, the write fails
   * with its refusal, also when a read that was in flight then serves the owner.
   */
  private async writeOffset(
    partitionId: number, offset: bigint | null, context: PartitionContext | undefined, cancel?: AbortSignal
  ): Promise<void> {
    const assignment = this.states.get(partitionId);
    const history = context?.incarnation ?? 0n;
    const write = (fence: PartitionContext | undefined) => offset === null
      ? deleteOffset(this.getClient)({ ...this.target(), partitionId, context: fence })
      : storeOffset(this.getClient)({ ...this.target(), partitionId, offset, context: fence });
    try {
      await write(this.owned(partitionId, context));
    } catch (error) {
      if (!this.group || errorCode(error) !== CONSUMER_GROUP_PARTITION_NOT_OWNED || !assignment ||
          this.states.get(partitionId) !== assignment)
        throw error;
      this.owners.delete(partitionId);
      this.syncRequested = true;
      this.wake();
      const served = await this.installedOwner(partitionId, cancel);
      // A write that names no history takes the history that the read serves. A reply that the client took just
      // before an abort can get here after it.
      if (!served || cancel?.aborted || (history !== 0n && served.incarnation !== history)) throw error;
      await write(served);
    }
  }

  /**
   * The context of the owner that the server installed for this member: a fetch of one message from Next without a
   * commit and an owner, which waits for no message. Only the context of its section counts, so a message that does
   * not decode cannot fail it. After a transient refusal of the section, or of the whole request as at the poll quota
   * of the server, it reads again after the retry interval until `cancel` aborts. Undefined for any other failure.
   */
  private async installedOwner(partitionId: number, cancel?: AbortSignal): Promise<PartitionContext | undefined> {
    const options = wireOptions({ ...this.options, count: OWNER_PROBE_COUNT, minCount: OWNER_PROBE_COUNT, maxWait: 0 },
      COMMAND_CODE.FetchMessages);
    for (;;) {
      let refusal: unknown;
      try {
        const sections = await fetchMessages(this.getClient)({
          ...this.target(), targets: [{ partitionId, pollingStrategy: PollingStrategy.Next }],
          count: OWNER_PROBE_COUNT, autocommit: false, options, signal: cancel
        });
        const section = sections.find((candidate) => candidate.partitionId === partitionId);
        if (section === undefined) return undefined;
        if (section.status === 0) return section.context;
        refusal = fetchSectionError(section);
      } catch (error) {
        refusal = error;
      }
      const code = errorCode(refusal);
      if (code === undefined || !TRANSIENT_REFUSALS.has(code)) return undefined;
      try {
        await sleep(this.retryInterval, undefined, { signal: cancel });
      } catch {
        return undefined;
      }
    }
  }

  /** Whether the stored offset of a partition lies past `offset`, which shows that a read after it stored messages. */
  private async storedPast(partitionId: number, offset: bigint): Promise<boolean> {
    const stored = await getOffset(this.getClient)({ ...this.target(), partitionId });
    return stored !== null && stored.storedOffset > offset;
  }

  /**
   * Stops the reader and waits for its reads and seeks in progress. A `next()`
   * that waits fails at once because the reader is closed. With
   * `CommitPolicy.Auto` it counts the last batch under `after: processing`
   * and stores the latest counted position of each partition. With
   * `CommitPolicy.OnRead` it moves the offset of each partition with
   * undelivered messages back to its last delivered message, or before the
   * first undelivered one when it delivered nothing. A batch of an earlier
   * assignment of the partition moves nothing. Nor does a partition that left
   * the assignment, so its batches that `next()` did not return are lost for
   * the group. After a session change its writes name no owner of the old
   * session, as for a seek. Before offset 0 it deletes the stored offset, if
   * the server holds one. Without a known batch it moves only a stored offset
   * at or past the start of the read, since a read past the end stores
   * nothing. It restores the offset from before a seek write with an unknown
   * outcome, as `seek` describes. A group reader then leaves its group. It
   * releases a join that is still unknown and a leave with an unknown outcome
   * in the client, so neither blocks a later metadata write of the client.
   * Rejects with the first failed store, lookup or delete.
   * A store or delete with an unknown outcome still rejects, but the close
   * releases it in the client, so it does not block a later reader. When the
   * server refuses a store or delete of the close as not owned, a read finds
   * the installed owner for it. That read goes again after each transient
   * refusal until 10 s after the close started, or until `signal` of the
   * first call aborts before that. Then that write fails with its refusal,
   * and each later write of the close goes once without such a read. The
   * close still leaves the group, so no read for the owner outlives it. The
   * same read of a seek, a commit or an automatic store ends when the close
   * starts. A second call waits for the first one and ends as it does. Its
   * signal does not end the close.
   */
  close({ signal }: { signal?: AbortSignal } = {}): Promise<void> {
    this.closure ??= this.closeOnce(signal).finally(() => this.detachFromClient?.());
    return this.closure;
  }

  private async closeOnce(signal?: AbortSignal): Promise<void> {
    this.countUncounted();
    this.closed = true;
    // The close can wait long for reads, seeks and writes, so a waiting next() ends now with the error of a closed
    // reader. A later call fails before it waits.
    this.arrival.resolve();
    this.lifetime.abort();
    this.wake();
    this.storeDue.resolve();
    // A refusal that stays transient repeats the reads for the owner without end, so the budget ends them. The writes
    // and the leave do not take it, so each still goes once.
    const budget = new AbortController();
    const timer = setTimeout(() => budget.abort(), CLOSE_BUDGET_MS);
    let failure: unknown;
    try {
      await this.worker;
      await this.regrouping;
      await Promise.allSettled([...this.reads.keys()]);
      // A seek whose write is in progress still applies, so the rewind sees its position and not the dropped batch.
      await Promise.all([...this.seeking.values()]);
      await this.committer;
      const cancel = signal === undefined ? budget.signal : AbortSignal.any([signal, budget.signal]);
      if (this.options.commit.kind === 'onRead') failure = await this.rewind(cancel);
      else if (this.options.commit.kind === 'auto') failure = await this.flushCounted(cancel);
    } finally {
      clearTimeout(timer);
    }
    if (this.group) {
      // A join with an unknown outcome would block the leave and every later metadata write of the client.
      await this.abandonLost(this.uncertainJoin);
      try {
        await leaveGroup(this.getClient)({ streamId: this.request.streamId, topicId: this.request.topicId,
          groupId: this.request.consumer.id });
      } catch (error) {
        // The session or the member may be gone already, which ends the membership too.
        await this.abandonLost(error);
      }
    }
    this.queue.length = 0;
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
      position: this.start, lookupDue: this.startNeedsLookup, generation: ++this.generations, inFlight: false, retryAt: 0,
      lasting: 0, countedSinceStore: 0, servedAt: 0
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
      let timer: NodeJS.Timeout | undefined;
      await Promise.race([this.storeDue.promise, ...(interval === undefined ? []
        : [new Promise<void>((resolve) => { timer = setTimeout(resolve, interval).unref(); })])]);
      // A store on an event and the close end the timer of the pass, so timers do not pile up or outlive the close.
      clearTimeout(timer);
      this.storeDue = resolvers();
      if (this.closed) return;
      const failure = await this.flushCounted(this.lifetime.signal);
      // The reader is the caller of this store, so nobody else hears of its failure.
      if (failure !== undefined) debug('automatic offset store failed', failure);
    }
  }

  /**
   * Stores the latest counted position of each partition ahead of its last store, one at a time. `cancel` ends the
   * reads for the owner of a store refused as not owned.
   */
  private async flushCounted(cancel?: AbortSignal): Promise<unknown> {
    const due = [...this.states.values()]
      .filter((state) => state.counted && state.counted.stamp?.generation === state.generation &&
        (!state.stored || state.stored.offset < state.counted.offset))
      .map((state) => state.counted!);
    let failure: unknown;
    for (const checkpoint of due) {
      try {
        await this.storeCheckpoint(checkpoint, cancel);
        this.recordStored(checkpoint);
      } catch (error) {
        await this.abandonLost(error);
        failure ??= error;
      }
    }
    return failure;
  }

  /**
   * With `CommitPolicy.OnRead` the server stored the offsets of messages that
   * the application never took, so each such partition moves its offset back.
   * `cancel` ends the reads for the owner of a write refused as not owned.
   */
  private async rewind(cancel?: AbortSignal): Promise<unknown> {
    const queued = new Map<number, PolledBatch>();
    // A batch of an earlier assignment names an owner that the server replaced, and another member can have moved the
    // offset since, so it decides nothing.
    for (const entry of this.queue)
      if (entry.batch && entry.generation === this.states.get(entry.batch.partitionId)?.generation &&
          !queued.has(entry.batch.partitionId))
        queued.set(entry.batch.partitionId, entry.batch);
    let failure: unknown;
    // A partition that left the assignment is not here. The server refuses its store after the handoff, and during the
    // handoff a store below the last served offset holds the handoff back until the rebalance times out.
    for (const [partitionId, state] of this.states) {
      const handedOver = state.handed && (!state.delivered || state.delivered.offset < state.handed.offset);
      const first = queued.get(partitionId);
      // A lost read stored the range that its catch-up reads again, so the catch-up position comes before the batch
      // that showed the loss.
      const catchUp: PartitionState['pendingStart'] = state.catchUpUntil === undefined ? undefined : state.position;
      if (!handedOver && !first && !catchUp && !state.pendingStart) continue;
      let offset: bigint | null;
      let context: PartitionContext | undefined;
      // Without a known batch a read from an offset may have stored nothing, as from a start past the end, where
      // the server refuses a store before that offset.
      let unconfirmed = false;
      if (state.delivered) {
        offset = state.delivered.offset;
        context = state.delivered.context;
      } else {
        const known = state.firstHanded ?? first?.messages[0].headers.offset ?? catchUp?.value;
        unconfirmed = known === undefined && state.pendingStart!.kind === PollingStrategyKind.Offset &&
          !state.pendingStart!.restores;
        const firstOffset = known ?? state.pendingStart!.value;
        offset = firstOffset === 0n ? null : firstOffset - 1n;
        context = state.handed?.context ?? first?.context ?? catchUp?.context ?? state.pendingStart!.context;
      }
      try {
        if (offset === null) {
          await this.deleteStored(partitionId, context, cancel);
        } else if (!unconfirmed || await this.storedPast(partitionId, offset)) {
          await this.writeOffset(partitionId, offset, context, cancel);
        }
      } catch (error) {
        await this.abandonLost(error);
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
        this.startRegroup();
        wait = this.startReads();
      } catch (error) {
        if (this.closed) return;
        this.reportFailure(error);
        wait = this.retryInterval;
        failed = true;
      }
      // A caller that asks again after a failure does not cut its retry interval short. The pause
      // keeps the process alive like a read in flight, until the reader closes.
      const woken = failed ? this.closing : this.wakeUp.promise;
      let timer: NodeJS.Timeout | undefined;
      await Promise.race([woken, ...(wait === undefined ? []
        : [new Promise<void>((resolve) => { timer = setTimeout(resolve, wait); })])]);
      // A pause that a wake or the close ends early clears its timer, so a pass per read does not pile up timers.
      clearTimeout(timer);
      this.wakeUp = resolvers();
    }
  }

  private async refreshAssignment(): Promise<void> {
    const { streamId, topicId, consumer } = this.request;
    if (this.memberLost || this.joinedResets !== this.sessionResets) {
      const resets = this.sessionResets;
      await this.join();
      this.joinedResets = resets;
      this.memberLost = false;
      this.resync = true;
    }
    // Each sync is a request to the server, so a pass that a read, a next() or a seek woke keeps the last assignment.
    if (!this.resync && !this.syncRequested && performance.now() < this.syncedAt + this.syncPause) return;
    // Cleared before the request, so a refusal 5009 that arrives while it is in flight asks again.
    this.syncRequested = false;
    let assignment;
    try {
      assignment = await syncGroup(this.getClient)({ streamId, topicId, groupId: consumer.id });
    } catch (error) {
      if (errorCode(error) !== CONSUMER_GROUP_MEMBER_NOT_FOUND) throw error;
    }
    // The server answers a sync of a member that it does not know with an empty body, so it joins again.
    if (!assignment) {
      await this.join();
      this.resync = true;
      assignment = await syncGroup(this.getClient)({ streamId, topicId, groupId: consumer.id });
      // A failed sync waits for the retry interval, so a member that the server forgets again does not loop on joins.
      if (!assignment)
        throw responseError(COMMAND_CODE.SyncGroup, CONSUMER_GROUP_MEMBER_NOT_FOUND,
          'the group forgot the member as it joined');
    }
    this.syncedAt = performance.now();
    const owned = new Set(assignment.partitions);
    // Only the partitions of this member count: the group generation also moves when other members hand over.
    const moved = owned.size !== this.states.size || [...owned].some((partition) => !this.states.has(partition));
    if (moved) this.routeChanges += 1;
    this.syncPause = this.resync || moved ? ASSIGNMENT_SETTLE_PAUSE_MS : Math.min(this.syncPause * 2, GROUP_REFRESH_MS);
    this.resync = false;
    const leaving: Checkpoint[] = [];
    for (const [partition, state] of this.states) {
      if (owned.has(partition)) continue;
      if (state.counted && (!state.stored || state.stored.offset < state.counted.offset)) leaving.push(state.counted);
      this.giveUpGap(partition, state);
      this.states.delete(partition);
    }
    for (const partition of owned)
      if (!this.states.has(partition)) this.states.set(partition, this.newState());
    for (const checkpoint of leaving) {
      try {
        await this.storeCheckpoint(checkpoint);
      } catch (error) {
        // The partition moved to another member, which may refuse this store.
        await this.abandonLost(error);
      }
    }
  }

  /**
   * Joins the group, as `core/sdk/src/clients/reader/membership.rs` does. A join with an unknown outcome blocks the
   * metadata lane of the client, so this join resolves it first. The server takes a second join from the same session
   * as a no-op, so this join runs also when the earlier one landed.
   */
  private async join(): Promise<void> {
    const { streamId, topicId, consumer } = this.request;
    const operationId = this.uncertainJoin?.operationId;
    if (operationId !== undefined) {
      const client = await this.getClient();
      try {
        if (client.resolveOperation) await client.resolveOperation(operationId);
        else client.abandonOperation?.(operationId);
        this.uncertainJoin = undefined;
      } catch (error) {
        if (error instanceof OperationUncertainError) throw error;
        // The replay proved a refusal, or a session reset or an abandon came first. Either way the lane is free.
        this.uncertainJoin = undefined;
        if (!(error instanceof OperationNotRetainedError)) throw error;
      }
    }
    try {
      await joinGroup(this.getClient)({ streamId, topicId, groupId: consumer.id });
    } catch (error) {
      if (error instanceof OperationUncertainError) {
        this.uncertainJoin = error;
        // The member may have joined or not, so the next pass joins again.
        this.memberLost = true;
      }
      throw error;
    }
  }

  /**
   * Groups all partitions of the reader by primary apart from the reads, in
   * the first pass and after each route change. After a failed grouping the
   * next one waits for the retry interval.
   */
  private startRegroup(): void {
    const changes = this.routeChanges;
    if (this.regrouping || changes === this.groupedChanges) return;
    this.regrouping = this.regroup(changes).finally(() => {
      this.regrouping = undefined;
      this.wake();
    });
  }

  private async regroup(changes: number): Promise<void> {
    try {
      const client: RawClient = await this.getClient();
      const partitions = this.partitions();
      this.groups = client.readinessGroups && partitions.length > 0
        ? await client.readinessGroups(this.request.streamId, this.request.topicId, partitions,
          { signal: this.lifetime.signal })
        : [];
      this.groupedChanges = changes;
      this.regroupFailed = false;
    } catch (error) {
      if (this.closed) return;
      this.regroupFailed = true;
      this.reportFailure(error);
      // The partitions in no group poll alone from now on, not only after the pause.
      this.wake();
      await sleep(this.retryInterval, undefined, { signal: this.lifetime.signal }).catch(() => undefined);
    }
  }

  /**
   * Starts the reads that may start now, and returns how long the worker may
   * sleep. The lookups of stored offsets go first, then the group fetches. The
   * polls alone leave a request for each readiness group, and at most 32 reads
   * are in flight in all.
   */
  private startReads(): number | undefined {
    const now = Date.now();
    let nextRetry = Number.POSITIVE_INFINITY;
    const lookups: number[] = [];
    const idle = new Set<number>();
    const alone: number[] = [];
    const reading = this.options.commit.kind === 'onRead'
      ? this.waiting > 0 && this.queue.length === 0
      : this.queuedMessages < this.prefetchMessages && this.queuedBytes < this.prefetchBytes;
    if (reading) {
      // A partition in no group polls alone. While a grouping is in flight it waits for its group instead, so its
      // polls do not take the requests of the groups, but after a failed grouping it polls alone until one succeeds.
      const grouped = new Set(this.groups.flat());
      const awaitingGroups = this.regrouping !== undefined && !this.regroupFailed;
      for (const [partition, state] of this.states) {
        if (state.inFlight || state.stopped || this.seeking.has(partition)) continue;
        if (state.retryAt > now) {
          nextRetry = Math.min(nextRetry, state.retryAt);
          continue;
        }
        if (state.lookupDue) {
          lookups.push(partition);
          continue;
        }
        if (awaitingGroups && !grouped.has(partition)) continue;
        const fetchable = grouped.has(partition) && (state.position.kind === PollingStrategyKind.Offset ||
          state.position.kind === PollingStrategyKind.Next) && state.catchUpUntil === undefined &&
          state.aloneUntil === undefined && !this.needsStart(state.position);
        if (fetchable) idle.add(partition);
        else alone.push(partition);
      }
    }
    for (const partition of lookups.slice(0, Math.max(this.requestRoom(), 0))) this.startLookup(partition);
    // Each group keeps a request, also while its partitions poll alone, wait for a retry or for next().
    const aloneLimit = Math.max(MAX_READS_IN_FLIGHT - this.groups.length, 1);
    const aloneReserve = Math.min(alone.length, Math.max(aloneLimit - this.pollsInFlight, 0));
    for (const targets of this.spread(idle, aloneReserve)) this.startFetch(targets);
    for (const partition of this.pollOrder(alone)) {
      if (this.pollsInFlight >= aloneLimit || this.requestRoom() <= 0) break;
      this.startPoll(partition);
    }
    let wait = Number.isFinite(nextRetry) ? Math.max(1, nextRetry - Date.now()) : undefined;
    // A timer truncates its delay, so the wait rounds up to wake after the sync is due, not just before.
    const untilSync = Math.max(1, Math.ceil(this.syncedAt + this.syncPause - performance.now()));
    if (this.group && (wait === undefined || wait > untilSync)) wait = untilSync;
    return wait;
  }

  /**
   * The fetches of a turn. Each group with idle partitions reads them in one
   * fetch while requests remain, from the group after the last one that
   * fetched, so the groups that a full turn left out go first in the next.
   * The requests and the read ahead room left after those fetches and the
   * polls alone then spread the backlogged partitions of the groups over more
   * fetches, each with the full limits. Their reads return at once, so each
   * slot keeps a full read in flight, as a poll of each partition would, and
   * no more reads park. Under `CommitPolicy.OnRead` a turn starts only while
   * `next()` waits, and a partition is in one committing read at a time, so
   * the fetches of a turn commit disjoint partitions.
   */
  private spread(idle: Set<number>, aloneReserve: number): number[][] {
    const groups = this.groups;
    // A fetch reads its targets in order until it holds its count, so the partitions served last go last.
    const byServedAt = (left: number, right: number) =>
      this.states.get(left)!.servedAt - this.states.get(right)!.servedAt || left - right;
    const plans = groups
      .map((_, index) => (this.groupCursor + index) % groups.length)
      .map((group) => ({ group, targets: groups[group].filter((partition) => idle.has(partition)).sort(byServedAt) }))
      .filter(({ targets }) => targets.length > 0)
      .slice(0, Math.max(this.requestRoom(), 0))
      .map(({ group, targets }) => {
        const backlogged = targets.filter((partition) => this.states.get(partition)?.backlogged);
        const quiet = targets.filter((partition) => !this.states.get(partition)?.backlogged);
        return { group, quiet, backlogged, extra: 0, room: backlogged.length - (quiet.length === 0 ? 1 : 0) };
      });
    if (plans.length > 0) this.groupCursor = (plans.at(-1)!.group + 1) % groups.length;
    let extra = this.freeSlots() - plans.length - aloneReserve;
    while (extra > 0 && plans.some(({ extra: taken, room }) => taken < room)) {
      for (const plan of plans) {
        if (extra === 0) break;
        if (plan.extra < plan.room) {
          plan.extra += 1;
          extra -= 1;
        }
      }
    }
    return plans.flatMap(({ quiet, backlogged, extra: taken }) => {
      const chunks = taken + (quiet.length === 0 ? 1 : 0);
      const spread: number[][] = Array.from({ length: chunks }, () => []);
      const waiting = [...quiet];
      backlogged.forEach((partition, index) => (chunks > 0 ? spread[index % chunks] : waiting).push(partition));
      return waiting.length > 0 ? [waiting, ...spread] : spread;
    });
  }

  /** The full reads that the read ahead budget has room for besides those in flight. */
  private freeSlots(): number {
    const room = Math.min(Math.floor((this.prefetchMessages - this.queuedMessages) / this.options.count),
      Math.floor((this.prefetchBytes - this.queuedBytes) / this.options.maxBytes), MAX_READS_IN_FLIGHT);
    return room - this.reads.size;
  }

  /** The reads that may start now. Each fetch and each poll holds one. */
  private requestRoom(): number {
    return MAX_READS_IN_FLIGHT - this.reads.size;
  }

  /** `partitions` from the one after the last poll alone, so a poll that ends empty does not take the turn of the others. */
  private pollOrder(partitions: number[]): number[] {
    partitions.sort((left, right) => left - right);
    const next = partitions.findIndex((partition) => this.pollCursor === undefined || partition > this.pollCursor);
    return next > 0 ? [...partitions.slice(next), ...partitions.slice(0, next)] : partitions;
  }

  private claim(partition: number): Target | undefined {
    const state = this.states.get(partition);
    if (!state || state.inFlight || state.stopped) return undefined;
    state.inFlight = true;
    return { partition, sent: state.position, generation: state.generation, state, session: this.sessionResets };
  }

  private track(read: Promise<void>, committed: number[] = []): void {
    this.reads.set(read, committed);
    void read.finally(() => {
      this.reads.delete(read);
      this.wake();
    });
  }

  private startPoll(partition: number): void {
    const target = this.claim(partition);
    if (!target) return;
    this.pollCursor = partition;
    const state = this.states.get(partition)!;
    const single = state.aloneUntil !== undefined;
    const check = this.needsStart(target.sent);
    const commits = this.options.commit.kind === 'onRead' && state.catchUpUntil === undefined && !single && !check;
    if (commits) this.startsCommit(target);
    this.pollsInFlight += 1;
    this.track(this.poll(target, commits, single, check).finally(() => {
      this.pollsInFlight -= 1;
    }), commits ? [partition] : []);
  }

  private startLookup(partition: number): void {
    const target = this.claim(partition);
    if (target) this.track(this.lookUp(target));
  }

  /** A partition with a stored offset reads on after it as with a Next start, and one without takes the start. */
  private async lookUp(target: Target): Promise<void> {
    let stored;
    try {
      stored = await getOffset(this.getClient)({ ...this.target(), partitionId: target.partition });
    } catch (error) {
      this.refuse(target, error);
      return;
    }
    const state = this.settle(target);
    // After a seek write with an unknown outcome, the pin ended the lookup with the offset from before the write.
    if (state?.lookupDue && this.stillApplies(target, state)) this.endLookup(state, stored, PollingStrategy.Next);
  }

  /** Ends the start lookup of a partition: with a stored offset it resumes at `resume`, without one it takes the start. */
  private endLookup(state: PartitionState, stored: OffsetResponse | null, resume: PollingStrategy): void {
    state.lookupDue = undefined;
    if (stored === null) state.emptyAtStart = true;
    else state.position = resume;
  }

  private startFetch(group: number[]): void {
    const targets = group.map((partition) => this.claim(partition)).filter((target) => target !== undefined);
    const onRead = this.options.commit.kind === 'onRead';
    // A fetch reads only Offset and Next positions, so the close can move the offset back before each start.
    if (onRead) for (const target of targets) this.startsCommit(target);
    if (targets.length > 0) this.track(this.fetch(targets), onRead ? targets.map(({ partition }) => partition) : []);
  }

  /** Records the start of a committing read of a claimed target, so the close can move the offset back before it. */
  private startsCommit(target: Target): void {
    target.commits = true;
    target.state!.pendingStart ??= target.sent;
    // The read may store an offset, so the empty offset that the start lookup found may be gone.
    target.state!.emptyAtStart = undefined;
  }

  private async fetch(targets: Target[]): Promise<void> {
    let sections;
    try {
      sections = await fetchMessages(this.getClient)({
        ...this.target(),
        targets: targets.map(({ partition, sent }) => ({
          partitionId: partition, pollingStrategy: { kind: sent.kind, value: sent.value } as PollingStrategy,
          context: this.owned(partition, (sent as Partial<Continuation>).context)
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
      } catch {
        this.readAlone(target, section.nextOffset - BigInt(section.messagesCount), section.nextOffset,
          section.context);
        continue;
      }
      this.complete(target, polled, section.nextOffset, endsBeforeHead(section));
    }
  }

  /**
   * Under `CommitPolicy.OnRead` a committing read from a position that names no offset gives the close no offset to
   * move back before, so a poll that neither commits nor delivers resolves the position first.
   */
  private needsStart(position: PollingStrategy): boolean {
    return this.options.commit.kind === 'onRead' && (position.kind === PollingStrategyKind.Timestamp ||
      position.kind === PollingStrategyKind.Last ||
      (position.kind === PollingStrategyKind.Next && (position as Partial<Continuation>).context === undefined));
  }

  private async poll(target: Target, commits: boolean, single: boolean, check = false): Promise<void> {
    const until = this.states.get(target.partition)?.catchUpUntil;
    const catchUp = !commits && !single && !check && until !== undefined &&
      target.sent.kind === PollingStrategyKind.Offset && target.sent.value <= until;
    const options = { ...this.options, commit: commits ? CommitPolicy.OnRead : CommitPolicy.Manual,
      // A Last position starts at the first of the last `count` messages. Every other probe needs one message.
      ...(single || check
        ? { count: check && target.sent.kind === PollingStrategyKind.Last ? this.options.count : 1, minCount: 1 }
        : {}),
      // The server stored these messages already, so the read takes them and no more.
      ...(catchUp ? { count: Math.min(Number(until - target.sent.value + 1n), this.options.count), minCount: 1,
        maxWait: 0 } : {}) };
    const fence = this.owned(target.partition, (target.sent as Partial<Continuation>).context);
    const strategy = fence === undefined ? target.sent : { ...(target.sent as Continuation), context: fence };
    let polled: PolledBatch;
    try {
      polled = await pollMessages(this.getClient)({
        ...this.target(), partitionId: target.partition, strategy, options,
        signal: this.lifetime.signal
      });
    } catch (error) {
      // A reply whose records end early fails to decode with a range error.
      const decodeFailed = error instanceof RangeError ||
        (error instanceof Error && error.name === 'DeserializeError');
      if (decodeFailed && target.sent.kind === PollingStrategyKind.Offset) {
        if (single) this.undecodable(target, error);
        else this.readAlone(target, target.sent.value, target.sent.value + BigInt(this.options.count),
          (target.sent as Partial<Continuation>).context);
        return;
      }
      await this.abandonLost(error);
      this.refuse(target, error);
      return;
    }
    if (check) {
      this.serve(target, polled.context);
      const state = this.settle(target);
      if (state && this.stillApplies(target, state)) {
        if (polled.messages.length === 0 && !servedInHistory(fence, polled.context)) {
          state.retryAt = Date.now() + this.retryInterval;
          return;
        }
        state.atEnd = polled.messages.length === 0;
        this.notify();
        // Next reads on after the stored offset. A timestamp or Last position reads from its first message by offset.
        if (polled.messages.length > 0)
          state.position = continuation(target.partition,
            target.sent.kind === PollingStrategyKind.Next ? PollingStrategyKind.Next : PollingStrategyKind.Offset,
            polled.messages[0].headers.offset, polled.context);
        else if (this.options.maxWait === 0) state.retryAt = Date.now() + this.retryInterval;
      }
      return;
    }
    this.complete(target, polled);
  }

  /**
   * Abandons an operation of this reader with an unknown outcome, a committing
   * read or an offset write of a seek, of a commit or of the close, so the
   * client takes the next read and write of its partition, also of a later
   * reader. The close abandons a join or a leave the same way.
   */
  private abandonLost(error: unknown): Promise<void> {
    return PartitionReader.abandonUnknown(this.getClient, error);
  }

  /** Records the owner generation that a read of the current session served for its partition. */
  private serve(target: Target, context: PartitionContext): void {
    const owner = this.owners.get(target.partition);
    if (target.session === this.sessionResets && context.ownerGeneration > 0n &&
        (owner?.session !== this.sessionResets || context.ownerGeneration < owner.generation))
      this.owners.set(target.partition, { session: this.sessionResets, generation: context.ownerGeneration });
  }

  /**
   * The context of a read or an offset write of a group reader. A context from an earlier session, and every context
   * before a read of the current session served the partition, goes out with owner generation 0, which the server
   * binds to the installed owner. Its client and session check still refuses a member that does not own the
   * partition. After a refusal as not owned, this holds again until a read serves the partition.
   */
  private owned(partitionId: number, context: PartitionContext | undefined): PartitionContext | undefined {
    const owner = this.owners.get(partitionId);
    if (!this.group || context === undefined ||
        (owner?.session === this.sessionResets && context.ownerGeneration >= owner.generation))
      return context;
    return { ...context, ownerGeneration: 0n };
  }

  /**
   * Whether the result of a check or a lookup still moves its partition. A seek owns the position until it ends, since
   * the server may answer after a seek write that lands with an unknown outcome. That seek may have pinned the
   * position since, and a new session may hold another stored offset.
   */
  private stillApplies(target: Target, state: PartitionState): boolean {
    return target.session === this.sessionResets && state.position === target.sent &&
      !this.seeking.has(target.partition);
  }

  /** Ends the read of a target. Undefined when the generation of its partition moved on. */
  private settle(target: Target): PartitionState | undefined {
    const state = this.states.get(target.partition);
    // A partition assigned again has a new state, whose read this is not.
    if (!state || state !== target.state) return undefined;
    state.inFlight = false;
    return state.generation === target.generation ? state : undefined;
  }

  private complete(target: Target, polled: PollMessagesResponse, nextOffset?: bigint, backlogged?: boolean): void {
    this.serve(target, polled.context);
    const state = this.settle(target);
    const onRead = this.options.commit.kind === 'onRead';
    if (!state) {
      // The server stored this read, so a partition that left the assignment still hands it over. A read without a
      // commit, such as one that reads a lost range again, hands over nothing: the leave reported that range as lost.
      if (target.commits && polled.messages.length > 0 && this.keeps(target.partition, target.generation))
        this.enqueueBatch(target, polledBatch(polled, target.sent, { reader: this.id, generation: target.generation }));
      return;
    }
    if (backlogged !== undefined) state.backlogged = backlogged;
    if (polled.messages.length > 0) state.pendingStart = undefined;
    const sentContext = this.owned(target.partition, (target.sent as Partial<Continuation>).context);
    // An empty reply from outside the history of its read decides nothing. The partition reads the same range again
    // after one retry interval, as after a transient refusal.
    if (polled.messages.length === 0 && !servedInHistory(sentContext, polled.context)) {
      state.retryAt = Date.now() + this.retryInterval;
      return;
    }
    state.lasting = 0;
    state.atEnd = polled.messages.length === 0;
    if (polled.messages.length === 0) {
      const next = nextOffset ?? polled.currentOffset + 1n;
      if (onRead && state.lost && target.sent.kind === PollingStrategyKind.Next && sentContext &&
          sameHistory(sentContext, polled.context) && next > target.sent.value) {
        state.position = continuation(target.partition, PollingStrategyKind.Offset, target.sent.value, polled.context);
        state.catchUpUntil = next - 1n;
        state.atEnd = false;
        return;
      }
      // A catch-up that finds nothing by offset cannot read the lost range again.
      if (state.catchUpUntil !== undefined && target.sent.kind === PollingStrategyKind.Offset) {
        this.giveUpGap(target.partition, state);
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
    if (onRead && state.lost && target.sent.kind === PollingStrategyKind.Next && sentContext &&
        first > target.sent.value && sameHistory(sentContext, polled.context)) {
      // A lost read stored these offsets without handing them over.
      state.position = continuation(target.partition, PollingStrategyKind.Offset, target.sent.value, polled.context);
      state.catchUpUntil = last;
      state.catchUpBatch = polled;
      return;
    }
    // This store fences out any older lost read, so a later jump is a store of the application or another session.
    if (onRead && target.sent.kind === PollingStrategyKind.Next) state.lost = undefined;
    const until = state.catchUpUntil;
    if (until !== undefined && target.sent.kind === PollingStrategyKind.Offset && first > target.sent.value &&
        servedInHistory(sentContext, polled.context)) {
      // The history no longer holds the offsets of the catch-up before the first message, as after retention: the
      // server starts a read below its first segment at that segment. A message past the catch-up comes again with the
      // next committing read.
      this.giveUpGap(target.partition, state, first);
      if (first > until) return;
      polled = { ...polled, messages: polled.messages.filter(({ headers }) => headers.offset <= until) };
    }
    const next = polled.messages.at(-1)!.headers.offset + 1n;
    const byOffset = (state.aloneUntil !== undefined && next < state.aloneUntil) ||
      (state.catchUpUntil !== undefined && next <= state.catchUpUntil);
    if (state.aloneUntil !== undefined && next >= state.aloneUntil) state.aloneUntil = undefined;
    if (state.catchUpUntil !== undefined && next > state.catchUpUntil) {
      state.catchUpUntil = undefined;
      state.catchUpBatch = undefined;
      state.lost = undefined;
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
  private readAlone(target: Target, from: bigint, until: bigint, context: PartitionContext | undefined): void {
    const state = this.settle(target);
    if (!state) return;
    state.position = context ? continuation(target.partition, PollingStrategyKind.Offset, from, context)
      : PollingStrategy.Offset(from);
    state.aloneUntil = until;
  }

  /**
   * The message at the sent offset does not decode. The partition reads it again until a seek past it. The skip takes
   * the context of the reply that returned the message, so the store of an on read seek to it names the history and
   * the owner that served it. The sent position can name an earlier owner or no history, and the server can refuse a
   * store under either.
   */
  private undecodable(target: Target, error: unknown): void {
    const state = this.settle(target);
    if (!state) return;
    state.aloneUntil ??= target.sent.value + 1n;
    state.retryAt = Date.now() + this.retryInterval;
    const served = error instanceof DeserializeError ? error.context : undefined;
    const skip = continuation(target.partition, PollingStrategyKind.Offset, target.sent.value + 1n,
      served ?? (target.sent as Partial<Continuation>).context ?? EMPTY_PARTITION_CONTEXT);
    this.enqueue({ partitionId: target.partition, generation: target.generation,
      error: new ReadError(target.partition, error, skip), messages: 0, bytes: 0 });
  }

  /**
   * A read that did not return its partition. A refusal before the read
   * retries, and only a lasting or a history refusal stops the partition. A
   * lasting refusal waits twice as long before each retry. A 5009 is lasting
   * but never stops: each sixth one in a row reports one error and starts the
   * count again.
   */
  private refuse(target: Target, error: unknown): void {
    const code = errorCode(error);
    if (code === NOT_PRIMARY) {
      // The client marked the route stale. The partition leaves its group until the next grouping, so it does not
      // go to the old primary again meanwhile.
      this.groups = this.groups.map((group) => group.filter((partition) => partition !== target.partition))
        .filter((group) => group.length > 0);
      this.routeChanges += 1;
    }
    const state = this.settle(target);
    if (!state || this.closed) return;
    state.backlogged = undefined;
    const now = Date.now();
    const report = (stop: boolean, resumable = false) => {
      if (stop) this.giveUpGap(target.partition, state);
      // A committing read that lost its reply stays pending until the next read shows what it stored, and a seek
      // to Next after the stop would skip that range without a word, so the stop reports the lost read instead.
      const reported = stop && state.lost
        ? responseError(COMMAND_CODE.PollMessagesDeferred, FETCH_PROGRESS_UNCERTAIN,
          `a lost read may have stored offsets of partition ${target.partition} from ${state.position.value}, ` +
          `and the partition stopped on: ${String(error)}`)
        : error;
      const readError = new ReadError(target.partition, reported, undefined, stop, resumable);
      if (stop) state.stopped = readError;
      this.enqueue({ partitionId: target.partition, generation: target.generation, error: readError,
        messages: 0, bytes: 0 });
    };
    if (isAbort(error) || code === NOT_PRIMARY) return;
    if (code === FETCH_PROGRESS_UNCERTAIN || error instanceof OperationUncertainError) {
      state.lost = true;
      state.retryAt = now + this.retryInterval;
      return;
    }
    if (code === CONSUMER_GROUP_PARTITION_NOT_OWNED) {
      // A refusal alone is not a leave: the server may have installed the owner again for a session change that the
      // reader did not see yet, and a move away and back looks the same. So the partition keeps its position and its
      // close state, reads on under owner 0 and asks for a refresh, which drops the partition only when it left.
      this.owners.delete(target.partition);
      this.syncRequested = true;
    }
    // The member is gone, so the next pass joins before it syncs and saves the sync that would come back empty.
    if (code === CONSUMER_GROUP_MEMBER_NOT_FOUND) this.memberLost = true;
    if (code === HISTORY_UNAVAILABLE && (target.sent as Partial<Continuation>).context === undefined) {
      state.retryAt = now + this.retryInterval;
      return;
    }
    if (code === HISTORY_UNAVAILABLE || code === INVALID_SIZE_BYTES) {
      report(true, code === HISTORY_UNAVAILABLE);
      return;
    }
    if (code !== undefined && LASTING_REFUSALS.has(code)) {
      state.lasting += 1;
      state.retryAt = now + this.retryInterval * 2 ** (state.lasting - 1);
      if (state.lasting < MAX_LASTING_REFUSALS) return;
      if (code !== CONSUMER_GROUP_PARTITION_NOT_OWNED) {
        report(true, true);
        return;
      }
      // Only a refresh that shows a leave drops a partition refused as not owned, so its count and wait start again.
      state.lasting = 0;
      report(false);
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

  /**
   * Gives up the offsets of a catch-up below `end`, all of them by default. It reports those that no held batch has and
   * hands over the held messages among them. Once no offset of the catch-up is left, no lost read is pending, and the
   * partition reads Next after it.
   */
  private giveUpGap(partitionId: number, state: PartitionState, end?: bigint): void {
    const held = state.catchUpBatch;
    const until = state.catchUpUntil;
    if (until === undefined) return;
    const target = { partition: partitionId, generation: state.generation, sent: state.position };
    const whole = end === undefined || end > until;
    const below = whole ? until + 1n : end;
    const heldFirst = held?.messages[0].headers.offset ?? below;
    const lostBelow = heldFirst < below ? heldFirst : below;
    if (state.position.value < lostBelow) {
      const lost = { first: state.position.value, last: lostBelow - 1n };
      const error = responseError(COMMAND_CODE.PollMessagesDeferred, FETCH_PROGRESS_UNCERTAIN,
        `a lost read stored offsets ${lost.first} to ${lost.last} of partition ${partitionId}, ` +
        'which the reader could not read again');
      this.enqueue({ partitionId, generation: state.generation,
        error: new ReadError(partitionId, error, undefined, false, false, lost), messages: 0, bytes: 0 });
    }
    const messages = held?.messages.filter(({ headers }) =>
      headers.offset >= state.position.value && headers.offset < below) ?? [];
    const context = held?.context ?? (state.position as Continuation).context;
    if (whole) {
      state.catchUpUntil = undefined;
      state.catchUpBatch = undefined;
      state.lost = undefined;
    }
    // A held batch ends where the catch-up ends, so after the whole catch-up the next read starts after both.
    state.position = continuation(partitionId, whole ? PollingStrategyKind.Next : PollingStrategyKind.Offset, below,
      context);
    if (held && messages.length > 0)
      this.enqueueBatch(target, polledBatch({ ...held, messages }, target.sent,
        { reader: this.id, generation: target.generation }));
  }

  private retry(target: Target, after: number): void {
    const state = this.settle(target);
    if (state) state.retryAt = after === 0 ? 0 : Date.now() + after;
  }

  /** Reports a failure of the whole reader, unless it is a refusal that passes or a request with an unknown outcome. */
  private reportFailure(error: unknown): void {
    const code = errorCode(error);
    if (!(code !== undefined && TRANSIENT_REFUSALS.has(code)) && !(error instanceof OperationUncertainError))
      this.report(undefined, 0, error);
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

/** Waits for `promise` unless `signal` aborts first. The signal is the caller's, so the wait removes its listener. */
const unlessAborted = async (promise: Promise<void>, signal: AbortSignal): Promise<void> => {
  signal.throwIfAborted();
  let abort!: () => void;
  const aborted = new Promise<never>((_, reject) => { abort = () => reject(signal.reason); });
  signal.addEventListener('abort', abort, { once: true });
  try {
    await Promise.race([promise, aborted]);
  } finally {
    signal.removeEventListener('abort', abort);
  }
};

