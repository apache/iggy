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

import {
  LaneBlockedError, OPERATION_TIMEOUT_MS, OperationNotRetainedError, OperationUncertainError
} from '../../client/client.operation.js';
import { idKey, type Id } from '../identifier.utils.js';
import type {
  CommandResponse,
  ClientProvider,
  RawClient,
  SendCommandOptions,
} from '../../client/client.type.js';
import {
  ConsumerKind,
  type Consumer,
} from '../offset/offset.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { ResponseError, responseError } from '../error.utils.js';
import { EMPTY_PARTITION_CONTEXT, type PartitionContext } from '../vsr/header.js';
import {
  SYNC_GROUP,
  type ConsumerGroupAssignment,
} from '../consumer-group/sync-group.command.js';
import { JOIN_GROUP } from '../consumer-group/join-group.command.js';
import {
  serializePollMessages, deserializePollMessages,
  type PollingStrategy, type PollMessagesResponse
} from './poll.utils.js';
import {
  deferredPollOptions, serializeDeferredPollOptions, type DeferredPollOptions
} from './deferred-poll.utils.js';
import { pollOptions, wireOptions, type PollOptions } from './poll-options.js';
import { polledBatch, validatePartition, type Continuation, type PolledBatch } from './checkpoint.js';

const RESYNC_REQUIRED_PARTITION = 0xFFFF_FFFF;
/** Internal result used when a group member currently owns no partitions. */
export const NO_ASSIGNED_PARTITION = 0xFFFF_FFFE;
const GROUP_POLL_MAX_ATTEMPTS = 2;
/** How long an assignment that a member synced stays current. */
export const GROUP_ASSIGNMENT_REFRESH_MS = 5_000;
const GROUP_MEMBER_NOT_FOUND = 5006;
const GROUP_PARTITION_NOT_OWNED = 5009;
const INVALID_CONFIGURATION = 2;
const INVALID_COMMAND = 3;
const TRANSIENT_NOT_COMMITTED = 57;

type GroupCursor = {
  generation: bigint,
  partitions: number[],
  position: number,
  synchronizedAt: number,
};

type GroupState = {
  cursors: Map<string, GroupCursor>,
  sessionGeneration: number,
  /** Session resets that ended a signed-in session. */
  endedSessions: number,
  pendingJoin?: bigint,
};

const groupStates = new WeakMap<RawClient, GroupState>();

const getGroupState = (client: RawClient): GroupState => {
  let state = groupStates.get(client);
  if (state)
    return state;

  state = {
    cursors: new Map(),
    sessionGeneration: 0,
    endedSessions: 0,
  };
  groupStates.set(client, state);
  client.on('sessionReset', (signedIn) => {
    state.cursors.clear();
    state.pendingJoin = undefined;
    state.sessionGeneration += 1;
    if (signedIn) state.endedSessions += 1;
  });
  client.on('heartbeat', () => {
    // Expire instead of delete so an unchanged assignment keeps its
    // round-robin position across the refresh.
    for (const cursor of state.cursors.values())
      cursor.synchronizedAt = 0;
  });
  return state;
};

/**
 * Parameters for the poll messages command.
 */
export type PollMessages = {
  /** Stream identifier */
  streamId: Id,
  /** Topic identifier */
  topicId: Id,
  /** Consumer configuration */
  consumer: Consumer,
  /** Partition ID (null for all partitions) */
  partitionId: number | null,
  /** Strategy for selecting messages */
  pollingStrategy: PollingStrategy,
  /** Maximum number of messages to poll */
  count: number,
  /** Whether to auto-commit offset after polling */
  autocommit: boolean,
  /**
   * Context of the reply an offset came from (`PollMessagesResponse.context`),
   * so a continuation poll fails instead of reading that offset in a partition
   * deleted and recreated since. Without one the client takes the context its
   * cached route reports: after another client deletes and recreates the
   * partition, one poll can fail with 87 (5009 for a group consumer). The
   * failed route is dropped and the next call routes again.
   */
  context?: PartitionContext,
  /**
   * Makes the server wait for messages before it replies (command 106), with
   * these options over `DEFAULT_DEFERRED_POLL_OPTIONS`. The call gives up at
   * `requestTimeout`. A consumer-group poll without a partition does not
   * wait: with these options it fails with 3 before anything is sent. To wait
   * on many partitions, read them with `message.reader`.
   */
  deferred?: Partial<DeferredPollOptions>,
  /** Cancels the poll. A poll that waits on the server is cancelled there too. */
  signal?: AbortSignal
};

/**
 * Parameters of the poll of one partition: one batch read with
 * `PollOptions`, as the Rust `poll_messages` reads it.
 */
export type PollPartition = {
  /** Stream identifier */
  streamId: Id,
  /** Topic identifier */
  topicId: Id,
  /** The partition to read */
  partitionId: number,
  /** The consumer whose position Next means */
  consumer: Consumer,
  /** Where the read starts, such as the continuation of the previous batch */
  strategy: PollingStrategy | Continuation,
  /**
   * The limits of the read and the commit policy, over `DEFAULT_POLL_OPTIONS`.
   * A one-shot poll refuses `CommitPolicy.Auto` with 2 (InvalidConfiguration).
   */
  options?: Partial<PollOptions>,
  /**
   * Cancels the poll. A waiting poll is cancelled on the server too. A poll
   * that commits keeps its reply then: the server answers it once, refused
   * when the cancel won and nothing was stored, so the call rejects with the
   * abort reason, or with the messages it stored, which the call returns.
   */
  signal?: AbortSignal
};

/** `message.poll`: the poll of one partition with `PollOptions`, or the plain poll of `PollMessages`. */
export type PollMessagesCall = {
  (request: PollPartition): Promise<PolledBatch>,
  (request: PollMessages): Promise<PollMessagesResponse>
};

/**
 * Poll messages command definition.
 * Retrieves messages from a topic partition.
 */
export const POLL_MESSAGES = {
  code: COMMAND_CODE.PollMessages,

  serialize: ({
    streamId, topicId, consumer, partitionId, pollingStrategy, count, autocommit, deferred
  }: PollMessages) => {
    const poll = serializePollMessages(
      streamId, topicId, consumer, partitionId, pollingStrategy, count, autocommit
    );
    return deferred
      ? Buffer.concat([poll, serializeDeferredPollOptions(deferredPollOptions(deferred, count))])
      : poll;
  },

  deserialize: (r: CommandResponse) => {
    return deserializePollMessages(r.data);
  }
};

/** A deferred poll goes out as command 106, with the body of a plain poll and its options. */
const pollCode = ({ deferred }: PollMessages): number =>
  deferred ? COMMAND_CODE.PollMessagesDeferred : POLL_MESSAGES.code;

/** The consumer-group member that a call names. */
type GroupMember = Pick<PollMessages, 'streamId' | 'topicId' | 'consumer'>;

const groupKey = ({ streamId, topicId, consumer }: GroupMember): string =>
  `${idKey(streamId)}\0${idKey(topicId)}\0` +
  `${consumer.kind}:${idKey(consumer.id)}`;

const syncAssignment = async (
  client: RawClient,
  request: GroupMember,
  state: GroupState,
  call: SendCommandOptions,
): Promise<GroupCursor> => {
  const generation = state.sessionGeneration;
  const pendingJoin = state.pendingJoin;
  if (pendingJoin !== undefined && client.resolveOperation) {
    try {
      await client.resolveOperation(pendingJoin, call);
      if (state.pendingJoin === pendingJoin) state.pendingJoin = undefined;
    } catch (error) {
      if (state.pendingJoin === pendingJoin && (error instanceof OperationNotRetainedError ||
          (error instanceof ResponseError && error.committed))) state.pendingJoin = undefined;
      throw error;
    }
  }
  const target = {
    streamId: request.streamId,
    topicId: request.topicId,
    groupId: request.consumer.id,
  };
  let assignment: ConsumerGroupAssignment | null;
  try {
    const response = await client.sendCommand(
      SYNC_GROUP.code,
      SYNC_GROUP.serialize(target),
      call,
    );
    assignment = SYNC_GROUP.deserialize(response);
  } catch (error) {
    if (!(error instanceof ResponseError) ||
        error.errorCode !== GROUP_MEMBER_NOT_FOUND)
      throw error;
    assignment = null;
  }
  if (assignment === null) {
    try {
      await client.sendCommand(
        JOIN_GROUP.code,
        JOIN_GROUP.serialize(target),
        call,
      );
    } catch (error) {
      if (error instanceof OperationUncertainError && error.operationId !== undefined &&
          state.sessionGeneration === generation)
        state.pendingJoin = error.operationId;
      if (error instanceof LaneBlockedError && error.operationId === state.pendingJoin)
        throw new OperationUncertainError(error.operationId, error);
      throw error;
    }
    const response = await client.sendCommand(
      SYNC_GROUP.code,
      SYNC_GROUP.serialize(target),
      call,
    );
    assignment = SYNC_GROUP.deserialize(response);
  }
  if (assignment === null)
    throw responseError(SYNC_GROUP.code, GROUP_MEMBER_NOT_FOUND);

  const key = groupKey(request);
  const current = state.cursors.get(key);
  if (current && current.generation === assignment.generation) {
    current.partitions = assignment.partitions;
    current.synchronizedAt = Date.now();
    if (current.position >= current.partitions.length)
      current.position = 0;
    return current;
  }
  const cursor = {
    ...assignment,
    position: 0,
    synchronizedAt: Date.now()
  };
  state.cursors.set(key, cursor);
  return cursor;
};

const currentCursor = async (
  client: RawClient,
  member: GroupMember,
  state: GroupState,
  call: SendCommandOptions,
): Promise<GroupCursor> => {
  const cached = state.cursors.get(groupKey(member));
  const cacheAge = cached ? Date.now() - cached.synchronizedAt : 0;
  // As in the Rust SDK, an empty assignment is synced again on every poll:
  // a member gets its partitions only as each one installs the new owner.
  return cached &&
    cached.partitions.length > 0 &&
    cacheAge >= 0 &&
    cacheAge < GROUP_ASSIGNMENT_REFRESH_MS
    ? cached
    : await syncAssignment(client, member, state, call);
};

/**
 * The partitions that a consumer-group member owns, as a poll without a
 * partition picks from them. The assignment is synced again after
 * GROUP_ASSIGNMENT_REFRESH_MS, at each heartbeat, while it is empty, and with
 * `resync`, as after a fence. A member that the group lost joins it again.
 */
export const groupAssignment = async (
  client: RawClient,
  member: GroupMember,
  call: SendCommandOptions,
  resync = false,
): Promise<number[]> => {
  const state = getGroupState(client);
  if (resync) state.cursors.delete(groupKey(member));
  return (await currentCursor(client, member, state, call)).partitions;
};

const pollConsumerGroup = async (
  client: RawClient,
  request: PollMessages,
  state: GroupState,
  call: SendCommandOptions,
): Promise<PollMessagesResponse> => {
  const key = groupKey(request);
  for (let attempt = 0; attempt < GROUP_POLL_MAX_ATTEMPTS; attempt += 1) {
    const cursor = await currentCursor(client, request, state, call);
    if (cursor.partitions.length === 0)
      return {
        partitionId: NO_ASSIGNED_PARTITION,
        context: EMPTY_PARTITION_CONTEXT,
        currentOffset: 0n,
        count: 0,
        messages: []
      };

    const partitionId = cursor.partitions[cursor.position];
    cursor.position = (cursor.position + 1) % cursor.partitions.length;
    let response: CommandResponse;
    try {
      response = await client.sendCommand(
        POLL_MESSAGES.code,
        POLL_MESSAGES.serialize({ ...request, partitionId }),
        { ...call, context: request.context },
      );
    } catch (error) {
      if (!isGroupFence(error))
        throw error;
      state.cursors.delete(key);
      // A caller context names one owner, so a retry under it would be
      // refused again. The caller decides, as in the Rust SDK.
      if (request.context && error.errorCode === GROUP_PARTITION_NOT_OWNED)
        throw error;
      continue;
    }
    const polled = POLL_MESSAGES.deserialize(response);
    if (polled.count === 0 &&
        polled.partitionId === RESYNC_REQUIRED_PARTITION) {
      state.cursors.delete(key);
      continue;
    }
    return polled;
  }
  return {
    partitionId: NO_ASSIGNED_PARTITION,
    context: EMPTY_PARTITION_CONTEXT,
    currentOffset: 0n,
    count: 0,
    messages: []
  };
};

/** A refusal of a consumer-group request for a membership or an assignment that ended. */
export const isGroupFence = (error: unknown): error is ResponseError =>
  error instanceof ResponseError &&
  (error.errorCode === GROUP_MEMBER_NOT_FOUND || error.errorCode === GROUP_PARTITION_NOT_OWNED);

/**
 * Executable poll messages command function.
 */
export const pollMessages = (getClient: ClientProvider): PollMessagesCall =>
  (async (request: PollPartition | PollMessages) => 'strategy' in request
    ? pollPartition(getClient, request)
    : pollPlain(getClient, request)) as PollMessagesCall;

const pollPartition = async (getClient: ClientProvider, request: PollPartition): Promise<PolledBatch> => {
  const command = COMMAND_CODE.PollMessagesDeferred;
  const options = pollOptions(request.options, command);
  if (options.commit.kind === 'auto')
    throw responseError(command, INVALID_CONFIGURATION,
      'a one-shot poll has nothing to count, so it cannot commit automatically: use a partition reader');
  validatePartition(request.strategy, request.partitionId, command);
  const polled = await pollPlain(getClient, {
    streamId: request.streamId,
    topicId: request.topicId,
    consumer: request.consumer,
    partitionId: request.partitionId,
    pollingStrategy: request.strategy,
    count: options.count,
    autocommit: options.commit.kind === 'onRead',
    deferred: wireOptions(options, command),
    context: (request.strategy as Partial<Continuation>).context,
    signal: request.signal
  });
  return polledBatch(polled, request.strategy);
};

const pollPlain = async (getClient: ClientProvider, request: PollMessages): Promise<PollMessagesResponse> => {
    const client = await getClient();
    const release = client.hold?.();
    try {
      if (request.consumer.kind === ConsumerKind.Group &&
          request.partitionId === null) {
        if (request.deferred)
          throw responseError(COMMAND_CODE.PollMessagesDeferred, INVALID_COMMAND,
            'a deferred group poll needs a partition: read many partitions with message.reader');
        const state = getGroupState(client);
        // As in the Rust SDK, one deadline bounds the whole call, also the
        // attempts after a session end, and no command of the call runs
        // under a session that the caller opened after it started.
        const deadline = Date.now() + OPERATION_TIMEOUT_MS;
        const call = { deadline, lineage: client.lineage, signal: request.signal };
        while (true) {
          const endedSessions = state.endedSessions;
          try {
            return await pollConsumerGroup(client, request, state, call);
          } catch (error) {
            // Retry only when the command stream confirms that a signed-in VSR
            // session ended. Other command, authorization, and decode errors
            // remain terminal for the caller. A sign-in after a refused one
            // resets the session too, but it ends no signed-in session, so a
            // refused sign-in is not repeated without end.
            if (error instanceof OperationUncertainError || state.endedSessions === endedSessions ||
                Date.now() >= deadline ||
                (error instanceof ResponseError &&
                  error.errorCode === TRANSIENT_NOT_COMMITTED))
              throw error;
          }
        }
      }
      return POLL_MESSAGES.deserialize(
        await client.sendCommand(
          pollCode(request),
          POLL_MESSAGES.serialize(request),
          { context: request.context, signal: request.signal }
        )
      );
    } finally {
      release?.();
    }
};
