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

import { Readable } from "node:stream";
import type { ClientConfigOrString, RawClient } from "../client/client.type.js";
import type { Id } from '../wire/identifier.utils.js';
import { SimpleClient } from "../client/client.js";
import { normalizeClientConfig } from "../client/client.config.js";
import { debug } from "../client/client.debug.js";
import { getRawClient } from "../client/client.socket.js";
import {
  ChangeSignal, FetchProgressUncertainError, LaneBlockedError, NotConnectedError, OPERATION_TIMEOUT_MS,
  OperationUncertainError, VsrResponseTimeoutError, pause
} from "../client/client.operation.js";
import { ConnectionClosedError } from '../client/client.connection.js';
import { ResponseError } from "../wire/error.utils.js";
import { LEAVE_GROUP } from "../wire/consumer-group/leave-group.command.js";
import { ConsumerKind, type Consumer as StreamConsumer } from "../wire/offset/offset.utils.js";
import type { PartitionContext } from "../wire/vsr/header.js";
import {
  GROUP_ASSIGNMENT_REFRESH_MS, groupAssignment, type PollMessages
} from "../wire/message/poll-messages.command.js";
import { deferredPollOptions, type DeferredPollOptions } from "../wire/message/deferred-poll.utils.js";
import {
  decodeFetchSection, fetchOptions, fetchSectionError, type FetchMessagesSection
} from "../wire/message/fetch-messages.command.js";
import {
  PollingStrategy,
  PollingStrategyKind,
  type PollMessagesResponse,
  type CommandAPI,
  Consumer
} from "../wire/index.js";

const STALE_CLIENT = 30;
const UNAUTHENTICATED = 40;
const TRANSIENT_NOT_COMMITTED = 57;
const TRANSIENT_NOT_ACCEPTED = 58;
const NOT_PRIMARY = 89;
const DEFERRED_POLL_CAPACITY_EXCEEDED = 90;
const INVALID_SIZE_BYTES = 80;
const HISTORY_UNAVAILABLE = 87;
const GROUP_MEMBER_NOT_FOUND = 5006;
const GROUP_PARTITION_NOT_OWNED = 5009;
const STATUS_OK = 0;
const RECOVERED_ERROR_CODES = new Set([
  STALE_CLIENT, UNAUTHENTICATED, TRANSIENT_NOT_COMMITTED, TRANSIENT_NOT_ACCEPTED, NOT_PRIMARY,
  DEFERRED_POLL_CAPACITY_EXCEEDED
]);
const GROUP_FENCE_CODES = new Set([GROUP_MEMBER_NOT_FOUND, GROUP_PARTITION_NOT_OWNED]);
/** Refusals of a read in an old history, or for an owner or a member that ended. */
const FENCE_CODES = new Set([HISTORY_UNAVAILABLE, ...GROUP_FENCE_CODES]);
const DEFAULT_POLLING_RETRY_INTERVAL_MS = 1_000;
const MAX_POLL_BYTES = 0xFFFF_FFFF;
/** A partition that joins while the fetch of its group waits goes into a second fetch, and no more. */
const MAX_GROUP_FETCHES = 2;
// A crash loses the replies that the server committed and the reader did not
// handle, so a stream that commits keeps one fetch per group.
const MAX_COMMITTING_GROUP_FETCHES = 1;
// A group stream takes back the reply in its buffer that nobody read: see
// `rewind`. With room for one reply, that reply is the last one handed out.
const BUFFERED_REPLIES = 1;
// Readable.from must not advance a committing group generator while the
// reader handles its last batch, since that would commit another one.
const DEMAND_DRIVEN_REPLIES = 0;
// A closed group stream keeps its client until the leave ends. A leave that
// misses this deadline leaves the membership to its lease.
const LEAVE_GROUP_TIMEOUT_MS = 5_000;

const wait = (interval: number, cb?: () => void): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(() => resolve(cb ? cb() : undefined), interval)
  });

// The client binds the session again, signs in again, reconnects or routes
// again on its own after these errors, so they must not end a stream that
// nothing restarts. A server at its deferred poll capacity takes the poll
// later, so the stream polls again, as the Rust consumer does.
const pollsAgainAfter = (error: unknown): boolean =>
  !needsExplicitRecovery(error) && (error instanceof OperationUncertainError || error instanceof VsrResponseTimeoutError ||
  error instanceof NotConnectedError || error instanceof ConnectionClosedError ||
  (error instanceof ResponseError && RECOVERED_ERROR_CODES.has(error.errorCode)));

const needsExplicitRecovery = (error: unknown): error is FetchProgressUncertainError | LaneBlockedError =>
  error instanceof FetchProgressUncertainError || error instanceof LaneBlockedError && error.requiresExplicitRecovery;

const isUnauthenticated = (error: unknown): boolean =>
  error instanceof ResponseError && error.errorCode === UNAUTHENTICATED;

const isOversized = (error: unknown): error is ResponseError =>
  error instanceof ResponseError && error.fromServer && error.errorCode === INVALID_SIZE_BYTES;

/** The poll of the next message alone, without a byte limit, as a plain poll reads it. */
const readAlone = (poll: PollMessages): PollMessages => ({
  ...poll, count: 1, deferred: { ...poll.deferred, minCount: 1, maxBytes: MAX_POLL_BYTES }
});

// A 40 also comes from a sign-in that the server refuses, as for an inactive
// user, and every later sign-in gets the same refusal. So after a 40 the
// stream waits for the next sign-in, and a refused one ends it.
const signInAgain = async (error: unknown, signIn: () => Promise<unknown>): Promise<void> => {
  if (isUnauthenticated(error))
    await signIn().catch((failure: unknown) => {
      if (isUnauthenticated(failure) || !pollsAgainAfter(failure)) throw failure;
      debug('consumer stream sign-in failed, polling again', failure);
    });
};

// Each poll waits on the server for messages, so an empty reply needs no
// pause before the next poll.
async function* genEagerUntilPoll(
  c: CommandAPI,
  poll: PollMessages,
  signIn: () => Promise<unknown>,
  stopped: () => boolean,
  pollingRetryInterval: number,
  endOnLastOffset: boolean
) {
  const state: Map<string, number> = new Map();
  // As in the Rust consumer, a message larger than the byte limit would stall
  // its partition, so the next poll reads it alone. Only a failure of that
  // poll is reported.
  let oversized = false;

  while (!stopped()) {
    const retry = oversized;
    oversized = false;
    let r: PollMessagesResponse;
    try {
      r = await c.message.poll(retry ? readAlone(poll) : poll);
    } catch (error) {
      if (stopped()) return;
      if (!retry && isOversized(error)) {
        oversized = true;
        continue;
      }
      if (!pollsAgainAfter(error)) throw error;
      debug('consumer stream poll failed, polling again', error);
      await signInAgain(error, signIn);
      await wait(pollingRetryInterval);
      continue;
    }
    yield r;
    state.set(`${r.partitionId}`, r.count);

    if (endOnLastOffset && Array.from(state).every(([, last]) => last === 0))
      return;
  }
};

/** A fetch reads from these start points. A partition with another one is first polled alone. */
const isFetchStrategy = ({ kind }: PollingStrategy): boolean =>
  kind === PollingStrategyKind.Offset || kind === PollingStrategyKind.Next;

/** Offsets compare only within one partition incarnation and owner. */
const sameHistory = (left: PartitionContext, right: PartitionContext): boolean =>
  left.incarnation === right.incarnation && left.ownerGeneration === right.ownerGeneration;

/** Where the next read of an assigned partition starts. */
type Position = {
  strategy: PollingStrategy,
  /** Context of the reply that the offset came from, which fences the next read. */
  context?: PartitionContext,
  /** The rank of the last fetch of the partition: see `startFetches`. */
  rank?: number,
  /** Its next message is larger than the byte limit, so a poll reads it alone. */
  oversized: boolean,
  /** After a refusal, the partition waits until this time. */
  pausedUntil: number
};

/** A fetch of partitions of one readiness group, or a poll of one partition. */
type GroupRead = {
  partitions: number[],
  /** The rank of a fetch. A poll has none. */
  rank?: number,
  /** Cancels a read that commits nothing. A read that commits is never cancelled. */
  cancel?: AbortController,
  outcome: Promise<GroupReadOutcome>
};

type GroupFetchRead = GroupRead & { rank: number };

const isFetch = (read: GroupRead): read is GroupFetchRead => read.rank !== undefined;

type GroupReadOutcome = { read: GroupRead } &
  ({ sections: FetchMessagesSection[] } | { polled: PollMessagesResponse } | { error: unknown });

const isServed = ({ status, messagesCount }: FetchMessagesSection): boolean =>
  status === STATUS_OK && messagesCount > 0;

/**
 * A reply with messages that waits for its hand-out: a poll, or a section
 * that the stream decodes only when it hands it out.
 */
type Pending = PollMessagesResponse | FetchMessagesSection;

const isSection = (pending: Pending): pending is FetchMessagesSection => 'records' in pending;

/** The offset of the first message of a reply. A section has it in its table entry, without its records. */
const firstOffset = (pending: Pending): bigint | undefined =>
  isSection(pending) ? pending.nextOffset - BigInt(pending.messagesCount) : pending.messages.at(0)?.headers.offset;

/** The replies with messages of a read that ended. */
const pendingOf = (outcome: GroupReadOutcome): Pending[] =>
  'polled' in outcome ? [outcome.polled] : 'sections' in outcome ? outcome.sections.filter(isServed) : [];

/** The last message that a group stream handed out of a partition. */
type HandedOut = { offset: bigint, context: PartitionContext };

/** What a group stream reads, and how. */
type GroupFetch = {
  streamId: Id,
  topicId: Id,
  consumer: StreamConsumer,
  pollingStrategy: PollingStrategy,
  count: number,
  autocommit: boolean,
  options: DeferredPollOptions
};

/**
 * Reads the assignment of a group member with one fetch (109) per readiness
 * group, which ends as soon as any of its partitions holds messages. The
 * stream keeps the position of each partition: a section moves it to its
 * next offset, and the partitions that a read served go to the end of the
 * next fetch, so each one gets its turn. The stream hands out only replies
 * with messages, and decodes a section only when it hands it out.
 *
 * A stream that commits nothing sends the next reads before it hands out the
 * reply, and a partition that joins while the fetch of its group waits goes
 * into a second fetch. Its reads are cancelled when the member loses one of
 * their partitions, and at stop. A stream that commits sends the next reads
 * only when the reader took the replies before them, and a partition that
 * joins waits for the next fetch of its group. Such a read is never
 * cancelled, so a stopped stream ends only after its replies, at most
 * `maxWait` later, and then takes back what nobody read: see `rewind`.
 */
async function* genFetchUntilStopped(
  c: CommandAPI,
  raw: RawClient,
  group: GroupFetch,
  signIn: () => Promise<unknown>,
  stop: AbortSignal,
  unread: () => boolean,
  pollingRetryInterval: number,
  endOnLastOffset: boolean,
  reportRecovery: (error: Error) => void
) {
  const { streamId, topicId, consumer, pollingStrategy, count, autocommit, options } = group;
  const maxGroupFetches = autocommit ? MAX_COMMITTING_GROUP_FETCHES : MAX_GROUP_FETCHES;
  /** The assigned partitions, in the order of the next fetch. */
  const positions = new Map<number, Position>();
  const running = new Set<GroupRead>();
  /** Reads that ended, in the order that they ended. */
  const ended: GroupReadOutcome[] = [];
  const state = new Map<number, number>();
  const handedOut = new Map<number, HandedOut>();
  /** Replies that wait for their turn to be handed out. */
  const queue: Pending[] = [];
  /** The reply that the stream hands out. While `unread` holds, it waits in the stream buffer. */
  let handing: PollMessagesResponse | undefined;
  let lastRank = 0;
  let resync = false;
  let resumeAt = 0;
  const blocked = new Set<number>();
  let progressFailure: FetchProgressUncertainError | LaneBlockedError | undefined;
  const recordRecovery = (error: FetchProgressUncertainError | LaneBlockedError, partitions: readonly number[]): void => {
    if (error instanceof LaneBlockedError && error.partitionId !== undefined) blocked.add(error.partitionId);
    else for (const partition of partitions) blocked.add(partition);
    progressFailure ??= error;
    reportRecovery(progressFailure);
  };
  // Wakes `next` when a read ends, at stop and at each heartbeat. A heartbeat
  // expires the assignment, so the stream syncs it again, also while its
  // fetches wait, and a partition that joins does not wait for them.
  const woken = new ChangeSignal();
  stop.addEventListener('abort', () => woken.pulse(), { once: true });
  raw.on('heartbeat', () => woken.pulse());

  const hold = (partitionIds: number[]): void => {
    const until = Date.now() + pollingRetryInterval;
    for (const partitionId of partitionIds) {
      const position = positions.get(partitionId);
      if (position) position.pausedUntil = until;
    }
  };

  /**
   * After a refusal for an old history or owner, the partitions read again
   * from the start strategy, as a member that just got them. One for the
   * membership also syncs the assignment again.
   */
  const fence = (partitionIds: number[], code: number): boolean => {
    if (!FENCE_CODES.has(code)) return false;
    for (const partitionId of partitionIds) {
      const position = positions.get(partitionId);
      if (position) Object.assign(position, { strategy: pollingStrategy, context: undefined, oversized: false });
    }
    resync ||= GROUP_FENCE_CODES.has(code);
    return true;
  };

  /** Ends the stream unless `error` is a fence of `partitionIds` or clears with time. */
  const recover = async (error: unknown, partitionIds: number[]): Promise<void> => {
    if (error instanceof ResponseError && error.fromServer && fence(partitionIds, error.errorCode)) return;
    if (!pollsAgainAfter(error)) throw error;
    debug('consumer stream read failed, reading again', error);
    await signInAgain(error, signIn);
  };

  const rotate = (partitionId: number): void => {
    const position = positions.get(partitionId);
    if (!position) return;
    positions.delete(partitionId);
    positions.set(partitionId, position);
  };

  const start = (partitions: number[], rank: number | undefined,
    send: (signal?: AbortSignal) => Promise<FetchMessagesSection[] | PollMessagesResponse>): void => {
    const cancel = autocommit ? undefined : new AbortController();
    const read: GroupRead = {
      partitions, rank, cancel,
      outcome: send(cancel?.signal).then(
        (reply) => Array.isArray(reply) ? { read, sections: reply } : { read, polled: reply },
        (error: unknown) => ({ read, error }))
    };
    running.add(read);
    void read.outcome.then((outcome) => {
      ended.push(outcome);
      woken.pulse();
    });
  };

  const pollAlone = (partitionId: number, { strategy, context, oversized }: Position): void => {
    const poll: PollMessages = {
      streamId, topicId, consumer, partitionId, pollingStrategy: strategy, count, autocommit, context,
      deferred: options
    };
    start([partitionId], undefined, (signal) => c.message.poll({ ...(oversized ? readAlone(poll) : poll), signal }));
  };

  /**
   * Starts one read per readiness group of the `idle` partitions. A partition
   * that a fetch of a higher rank read waits for the fetch of the lowest rank
   * of its group in flight and then joins it, so the fetches of a group merge
   * again. A partition never fetched has no rank.
   */
  const startFetches = async (idle: Map<number, Position>): Promise<void> => {
    const reads = autocommit ? [...running] : [...running].filter(isFetch);
    const busy = new Set(reads.flatMap(({ partitions }) => partitions));
    const groups = await c.message.readinessGroups({
      streamId, topicId, signal: stop,
      partitions: [...positions.keys()].filter((partitionId) => idle.has(partitionId) || busy.has(partitionId))
    });
    for (const members of groups) {
      const inFlight = reads.filter(({ partitions }) => partitions.some((partitionId) => members.includes(partitionId)));
      if (inFlight.length >= maxGroupFetches) continue;
      const oldest = Math.min(...inFlight.flatMap(({ rank }) => rank ?? []));
      let targets: [number, Position][] = [];
      for (const partitionId of members) {
        const position = idle.get(partitionId);
        if (position && (position.rank === undefined || position.rank < oldest)) targets.push([partitionId, position]);
      }
      if (targets.length === 0) continue;
      const [first, position] = targets[0];
      if (position.oversized || !isFetchStrategy(position.strategy)) {
        pollAlone(first, position);
        continue;
      }
      targets = targets.filter(([, position]) => !position.oversized && isFetchStrategy(position.strategy));
      const ranks = targets.flatMap(([, { rank }]) => rank ?? []);
      const rank = ranks.length > 0 ? Math.min(...ranks) : ++lastRank;
      for (const [, position] of targets) position.rank = rank;
      start(targets.map(([partitionId]) => partitionId), rank, (signal) => c.message.fetch({
        streamId, topicId, consumer, count, autocommit, options, signal,
        targets: targets.map(([partitionId, { strategy, context }]) => ({ partitionId, pollingStrategy: strategy, context }))
      }));
    }
  };

  /** Syncs the assignment, cancels the reads of lost partitions that commit nothing, and reads the idle partitions. */
  const schedule = async (): Promise<void> => {
    const assigned = new Set(await groupAssignment(raw, { streamId, topicId, consumer },
      { deadline: Date.now() + OPERATION_TIMEOUT_MS, lineage: raw.lineage, signal: stop }, resync));
    resync = false;
    for (const partitionId of positions.keys())
      if (!assigned.has(partitionId)) {
        positions.delete(partitionId);
        state.delete(partitionId);
      }
    for (const partitionId of assigned)
      if (!positions.has(partitionId)) positions.set(partitionId, { strategy: pollingStrategy, oversized: false, pausedUntil: 0 });
    for (const read of running)
      if (read.partitions.some((partitionId) => !assigned.has(partitionId))) read.cancel?.abort();
    const now = Date.now();
    const busy = new Set([...running].flatMap(({ partitions }) => partitions));
    const fetchable = new Map<number, Position>();
    for (const [partitionId, position] of positions) {
      if (busy.has(partitionId) || position.pausedUntil > now) continue;
      if (!autocommit && (position.oversized || !isFetchStrategy(position.strategy))) pollAlone(partitionId, position);
      else fetchable.set(partitionId, position);
    }
    if (fetchable.size > 0) await startFetches(fetchable);
  };

  /** Schedules, unless a failed schedule paused the stream. */
  const step = async (): Promise<void> => {
    if (stop.aborted || Date.now() < resumeAt) return;
    try {
      await schedule();
    } catch (error) {
      if (stop.aborted) return;
      await recover(error, []);
      resumeAt = Date.now() + pollingRetryInterval;
    }
  };

  /**
   * The next read that ends, or nothing at stop and when the stream must
   * schedule again: when a pause ends, and when the assignment can have
   * changed. Without heartbeats, the assignment expires after
   * GROUP_ASSIGNMENT_REFRESH_MS. A member that owns no partition syncs it
   * again after `pollingRetryInterval`.
   */
  const next = async (): Promise<GroupReadOutcome | undefined> => {
    if (ended.length === 0 && !stop.aborted) {
      const now = Date.now();
      const wakeAt = Math.min(
        now + (positions.size === 0 ? pollingRetryInterval : GROUP_ASSIGNMENT_REFRESH_MS),
        ...[resumeAt, ...[...positions.values()].map(({ pausedUntil }) => pausedUntil)].filter((at) => at > now)
      );
      const timer = new AbortController();
      await Promise.race([woken.promise, pause(wakeAt - now, timer.signal)]).finally(() => timer.abort());
    }
    return ended.shift();
  };

  /** Moves the partition of a poll to the offset after its messages, and returns the poll if it has some. */
  const settlePoll = (polled: PollMessagesResponse): PollMessagesResponse[] => {
    const position = positions.get(polled.partitionId);
    const last = polled.messages.at(-1);
    if (position) {
      position.oversized = false;
      if (last) Object.assign(position, { strategy: PollingStrategy.Offset(last.headers.offset + 1n), context: polled.context });
    }
    state.set(polled.partitionId, polled.count);
    rotate(polled.partitionId);
    if (!last) return [];
    return [polled];
  };

  /**
   * Moves each partition of a reply to its next offset, and returns the
   * sections with messages. A reply without messages comes at the end of the
   * wait, so every partition of the fetch then read to its end.
   */
  const settleFetch = (read: GroupRead, sections: FetchMessagesSection[]): FetchMessagesSection[] => {
    const served: FetchMessagesSection[] = [];
    for (const section of sections) {
      const { partitionId, status } = section;
      const position = positions.get(partitionId);
      if (status === STATUS_OK) {
        if (position) Object.assign(position, { strategy: PollingStrategy.Offset(section.nextOffset), context: section.context });
        state.set(partitionId, section.messagesCount);
        if (isServed(section)) served.push(section);
      } else if (status === INVALID_SIZE_BYTES) {
        if (position) position.oversized = true;
      } else if (section.progressError) {
        recordRecovery(section.progressError, [partitionId]);
      } else {
        if (!fence([partitionId], status)) debug('consumer stream partition refused, reading it again', fetchSectionError(section));
        hold([partitionId]);
      }
    }
    if (served.length === 0 && sections.every(({ status }) => status === STATUS_OK)) {
      const waitingPoll = autocommit && [...positions.values()].some((position) =>
        position.oversized || !isFetchStrategy(position.strategy));
      for (const partitionId of read.partitions) {
        state.set(partitionId, 0);
        if (waitingPoll) rotate(partitionId);
      }
    }
    for (const { partitionId } of served) rotate(partitionId);
    return served;
  };

  const handOut = ({ partitionId, context, messages }: PollMessagesResponse): void => {
    const last = messages.at(-1);
    if (last) handedOut.set(partitionId, { offset: last.headers.offset, context });
  };

  /** Every assigned partition read to its end. */
  const readToEnd = (): boolean =>
    positions.size > 0 && [...positions.keys()].every((partitionId) => state.get(partitionId) === 0);

  /**
   * The server committed these replies, but nobody read them. As in the Rust
   * consumer, the offset then goes back to the last message that the stream
   * handed out in the same partition history. Without one, it stores the
   * offset before the first unread message, or deletes it before offset 0.
   * Each write carries the context of its
   * history, so none lands in a newer one.
   */
  const rewind = async (replies: Pending[]): Promise<void> => {
    const rewound = new Set<string>();
    const writes: Promise<unknown>[] = [];
    for (const reply of replies) {
      const { partitionId, context } = reply;
      const first = firstOffset(reply);
      const history = `${partitionId}:${context.incarnation}:${context.ownerGeneration}`;
      if (first === undefined || blocked.has(partitionId) || !positions.has(partitionId) || rewound.has(history)) continue;
      rewound.add(history);
      const last = handedOut.get(partitionId);
      let write: Promise<unknown>;
      if (last && sameHistory(last.context, context))
        write = c.offset.store({ streamId, topicId, consumer, partitionId, offset: last.offset, context: last.context });
      else if (first > 0n)
        write = c.offset.store({ streamId, topicId, consumer, partitionId, offset: first - 1n, context });
      else
        write = c.offset.delete({ streamId, topicId, consumer, partitionId, context });
      writes.push(write.catch((error: unknown) => {
        if (needsExplicitRecovery(error)) recordRecovery(error, [partitionId]);
        else debug('consumer stream rewind failed', partitionId, error);
      }));
    }
    await Promise.all(writes);
  };

  try {
    while (!stop.aborted) {
      await step();
      const outcome = await next();
      if (!outcome) continue;
      const { read } = outcome;
      running.delete(read);
      if ('error' in outcome) {
        if (needsExplicitRecovery(outcome.error)) {
          recordRecovery(outcome.error, read.partitions);
          throw outcome.error;
        }
        if (stop.aborted || read.cancel?.signal.aborted) continue;
        await recover(outcome.error, read.partitions);
        hold(read.partitions);
        continue;
      }
      const polled = 'polled' in outcome ? settlePoll(outcome.polled) : [];
      const served = 'sections' in outcome ? settleFetch(read, outcome.sections) : [];
      // A stream that read every partition to its end sends no more reads.
      const ending = endOnLastOffset && readToEnd();
      if (!ending && !autocommit) await step();
      queue.push(...polled, ...served);
      while (!stop.aborted) {
        const pending = queue.at(0);
        if (!pending) break;
        // A section that does not decode ends the stream and stays queued, so
        // the rewind takes it back and none of its messages is skipped.
        const reply = isSection(pending) ? decodeFetchSection(pending) : pending;
        queue.shift();
        handing = reply;
        yield reply;
        handing = undefined;
        handOut(reply);
      }
      if (progressFailure) throw progressFailure;
      if (ending) return;
    }
  } finally {
    for (const read of running) read.cancel?.abort();
    // A stream destroyed while it hands out a reply stops the generator at
    // that reply, which can still wait in the stream buffer.
    if (handing && unread()) queue.unshift(handing);
    const outcomes = await Promise.all([...running].map(({ outcome }) => outcome));
    for (const outcome of outcomes) {
      if ('error' in outcome && needsExplicitRecovery(outcome.error)) {
        recordRecovery(outcome.error, outcome.read.partitions);
      } else if ('sections' in outcome) {
        for (const section of outcome.sections)
          if (section.progressError) {
            recordRecovery(section.progressError, [section.partitionId]);
          }
      }
    }
    if (autocommit)
      await rewind([...queue, ...outcomes.flatMap(pendingOf)])
        .catch((error: unknown) => { debug('consumer stream rewind failed', error); });
    if (progressFailure) throw progressFailure;
  }
};

/** The client that a stream owns, and the sign-in that the stream waits for. */
type StreamClient = { client: SimpleClient, raw: RawClient, signIn: () => Promise<boolean> };

// A running stream polls again after NotConnectedError, so the factory signs
// in first: a wrong address or broken TLS then rejects the factory call. The
// stream owns this client, so a failed sign-in or setup destroys it.
const openClient = async (
  config: ClientConfigOrString,
  setup?: (c: SimpleClient) => Promise<unknown>
): Promise<StreamClient> => {
  const options = normalizeClientConfig(config);
  const raw = getRawClient(options);
  const signIn = () => raw.authenticate(options.credentials);
  try {
    await signIn();
    const client = new SimpleClient(raw);
    await setup?.(client);
    return { client, raw, signIn };
  } catch (error) {
    raw.destroy();
    throw error;
  }
};

/** The consumer of a stream and what it reads. */
type StreamMember = { streamId: Id, topicId: Id, consumer: StreamConsumer };

// A client without a session would have to sign in again to leave, and that
// new session is not the member.
const leaveConsumerGroup = async (raw: RawClient, { streamId, topicId, consumer }: StreamMember): Promise<void> => {
  if (consumer.kind !== ConsumerKind.Group || !raw.isAuthenticated) return;
  await raw.sendCommand(LEAVE_GROUP.code, LEAVE_GROUP.serialize({ streamId, topicId, groupId: consumer.id }),
    { deadline: Date.now() + LEAVE_GROUP_TIMEOUT_MS });
};

// The stream owns its client, so a closed stream must not keep the links,
// heartbeat and group membership of that client alive.
const pollStream = (
  { client, raw, signIn }: StreamClient,
  member: StreamMember,
  read: (client: CommandAPI, signIn: () => Promise<unknown>, stop: AbortSignal, unread: () => boolean,
    reportRecovery: (error: Error) => void) =>
    AsyncGenerator<PollMessagesResponse>,
  highWaterMark = BUFFERED_REPLIES
) => {
  const stop = new AbortController();
  let unread = false;
  let recoveryError: Error | undefined;
  const stream: Readable = Readable.from(read(client, signIn, stop.signal, () => unread,
    (error) => { recoveryError ??= error; }),
    { objectMode: true, highWaterMark });
  // A destroyed stream closes only after its running reads end, so it
  // cancels them, also on the server where they wait for messages. A group
  // stream lets the reads that commit end instead. A client without a session
  // has no membership to leave, so it goes at once, also when it loses its
  // session later.
  const closeGenerator = stream._destroy.bind(stream);
  stream._destroy = (error, callback) => {
    unread = stream.readableLength > 0;
    stop.abort();
    const releaseSignedOut = () => { if (!raw.isAuthenticated) raw.destroy(); };
    releaseSignedOut();
    raw.on('sessionReset', releaseSignedOut);
    // Readable.from can suppress an iterator's cleanup error during destroy.
    closeGenerator(error, (failure) => callback(failure ?? recoveryError ?? error));
  };
  stream.once('close', () => {
    void leaveConsumerGroup(raw, member)
      .catch((error: unknown) => { debug('consumer stream group leave failed', error); })
      .finally(() => raw.destroy());
  });
  return stream;
};


export type ConsumerStreamRequest = {
  streamId: Id,
  topicId: Id,
  pollingStrategy: PollingStrategy,
  count: number,
  /**
   * How each poll waits on the server for messages, over
   * `DEFAULT_DEFERRED_POLL_OPTIONS`: up to one second for one message, with
   * replies up to 1 MiB. A message larger than `maxBytes` is read alone,
   * without that limit. A `maxWait` of 0 polls again at once. A group stream
   * fetches with them, so its `requestTimeout` must be at least `maxWait`
   * plus 1 s, and greater than 1 s at a `maxWait` of 0.
   */
  pollOptions?: Partial<DeferredPollOptions>,
  /**
   * The pause in milliseconds after a failed poll, and while a group member
   * owns no partition. Defaults to 1000.
   */
  pollingRetryInterval?: number,
  autocommit?: boolean
  endOnLastOffset?: boolean
}

export type SingleConsumerStreamRequest = ConsumerStreamRequest & {
  partitionId: number,
};

export type GroupConsumerStreamRequest = ConsumerStreamRequest & { groupName: string };

export const singleConsumerStream = (config: ClientConfigOrString) => async (
  {
    streamId,
    topicId,
    partitionId,
    pollingStrategy,
    count,
    pollOptions = {},
    pollingRetryInterval = DEFAULT_POLLING_RETRY_INTERVAL_MS,
    autocommit = true,
    endOnLastOffset = false
  }: SingleConsumerStreamRequest
): Promise<Readable> => {
  // Invalid options fail here, before the stream owns a client.
  const deferred = deferredPollOptions(pollOptions, count);
  const s = await openClient(config);

  const poll = {
    streamId,
    topicId,
    partitionId,
    consumer: Consumer.Single,
    pollingStrategy,
    count,
    autocommit,
    deferred
  };

  return pollStream(s, poll, (client, signIn, stop) =>
    genEagerUntilPoll(client, { ...poll, signal: stop }, signIn, () => stop.aborted, pollingRetryInterval,
      endOnLastOffset));
};


export const groupConsumerStream = (config: ClientConfigOrString) =>
  async function groupConsumerStream({
    groupName,
    streamId,
    topicId,
    pollingStrategy,
    count,
    pollOptions = {},
    pollingRetryInterval = DEFAULT_POLLING_RETRY_INTERVAL_MS,
    autocommit = true,
    endOnLastOffset = false
  }: GroupConsumerStreamRequest): Promise<Readable> {
    // Invalid options fail here, before the stream owns a client.
    const options = fetchOptions(pollOptions, count);
    const s = await openClient(config, (client) => client.group.ensureAndJoin(streamId, topicId, groupName));

    const group = {
      streamId,
      topicId,
      consumer: Consumer.Group(groupName),
      pollingStrategy,
      count,
      autocommit,
      options
    };
    return pollStream(s, group, (client, signIn, stop, unread, reportRecovery) =>
      genFetchUntilStopped(client, s.raw, group, signIn, stop, unread, pollingRetryInterval, endOnLastOffset, reportRecovery),
    autocommit ? DEMAND_DRIVEN_REPLIES : BUFFERED_REPLIES);
  };
