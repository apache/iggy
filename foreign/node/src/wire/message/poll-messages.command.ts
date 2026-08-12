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
//

import type { Id } from '../identifier.utils.js';
import type {
  CommandResponse,
  ClientProvider,
  RawClient,
} from '../../client/client.type.js';
import {
  ConsumerKind,
  type Consumer,
} from '../offset/offset.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { ResponseError, responseError } from '../error.utils.js';
import {
  SYNC_GROUP,
  type ConsumerGroupAssignment,
} from '../consumer-group/sync-group.command.js';
import { JOIN_GROUP } from '../consumer-group/join-group.command.js';
import {
  serializePollMessages, deserializePollMessages,
  type PollingStrategy, type PollMessagesResponse
} from './poll.utils.js';

const RESYNC_REQUIRED_PARTITION = 0xFFFF_FFFF;
/** Internal result used when a group member currently owns no partitions. */
export const NO_ASSIGNED_PARTITION = 0xFFFF_FFFE;
const GROUP_POLL_MAX_ATTEMPTS = 2;
const GROUP_ASSIGNMENT_REFRESH_MS = 5_000;
const GROUP_MEMBER_NOT_FOUND = 5006;
const GROUP_PARTITION_NOT_OWNED = 5009;

type GroupCursor = {
  generation: bigint,
  partitions: number[],
  position: number,
  synchronizedAt: number,
};

type GroupState = {
  cursors: Map<string, GroupCursor>,
  sessionGeneration: number,
};

const groupStates = new WeakMap<RawClient, GroupState>();

const getGroupState = (client: RawClient): GroupState => {
  let state = groupStates.get(client);
  if (state)
    return state;

  state = {
    cursors: new Map(),
    sessionGeneration: 0,
  };
  groupStates.set(client, state);
  client.on('sessionReset', () => {
    state.cursors.clear();
    state.sessionGeneration += 1;
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
  autocommit: boolean
};

/**
 * Poll messages command definition.
 * Retrieves messages from a topic partition.
 */
export const POLL_MESSAGES = {
  code: COMMAND_CODE.PollMessages,

  serialize: ({
    streamId, topicId, consumer, partitionId, pollingStrategy, count, autocommit
  }: PollMessages) => {
    return serializePollMessages(
      streamId, topicId, consumer, partitionId, pollingStrategy, count, autocommit
    );
  },

  deserialize: (r: CommandResponse) => {
    return deserializePollMessages(r.data);
  }
};

const idKey = (id: Id): string =>
  typeof id === 'number' ? `number:${id}` : `string:${id}`;

const groupKey = ({ streamId, topicId, consumer }: PollMessages): string =>
  `${idKey(streamId)}\0${idKey(topicId)}\0` +
  `${consumer.kind}:${idKey(consumer.id)}`;

const syncAssignment = async (
  client: RawClient,
  request: PollMessages,
  state: GroupState,
): Promise<GroupCursor> => {
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
    );
    assignment = SYNC_GROUP.deserialize(response);
  } catch (error) {
    if (!(error instanceof ResponseError) ||
        error.errorCode !== GROUP_MEMBER_NOT_FOUND)
      throw error;
    assignment = null;
  }
  if (assignment === null) {
    await client.sendCommand(
      JOIN_GROUP.code,
      JOIN_GROUP.serialize(target),
    );
    const response = await client.sendCommand(
      SYNC_GROUP.code,
      SYNC_GROUP.serialize(target),
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

const pollConsumerGroup = async (
  client: RawClient,
  request: PollMessages,
  state: GroupState,
): Promise<PollMessagesResponse> => {
  const key = groupKey(request);
  for (let attempt = 0; attempt < GROUP_POLL_MAX_ATTEMPTS; attempt += 1) {
    const cached = state.cursors.get(key);
    const cacheAge = cached ? Date.now() - cached.synchronizedAt : 0;
    const cursor = cached &&
      cacheAge >= 0 &&
      cacheAge < GROUP_ASSIGNMENT_REFRESH_MS
      ? cached
      : await syncAssignment(client, request, state);
    if (cursor.partitions.length === 0)
      return {
        partitionId: NO_ASSIGNED_PARTITION,
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
      );
    } catch (error) {
      if (!(error instanceof ResponseError) ||
          (error.errorCode !== GROUP_MEMBER_NOT_FOUND &&
           error.errorCode !== GROUP_PARTITION_NOT_OWNED))
        throw error;
      state.cursors.delete(key);
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
    currentOffset: 0n,
    count: 0,
    messages: []
  };
};

/**
 * Executable poll messages command function.
 */
export const pollMessages = (getClient: ClientProvider) =>
  async (request: PollMessages): Promise<PollMessagesResponse> => {
    const client = await getClient();
    const release = client.hold?.();
    try {
      if (client.protocol === 'vsr' &&
          request.consumer.kind === ConsumerKind.Group &&
          request.partitionId === null) {
        const state = getGroupState(client);
        while (true) {
          const sessionGeneration = state.sessionGeneration;
          try {
            return await pollConsumerGroup(client, request, state);
          } catch (error) {
            // Retry only when the command stream confirms that the VSR session
            // was reset. Other command, authorization, and decode errors remain
            // terminal for the caller.
            if (state.sessionGeneration === sessionGeneration)
              throw error;
          }
        }
      }
      return POLL_MESSAGES.deserialize(
        await client.sendCommand(
          POLL_MESSAGES.code,
          POLL_MESSAGES.serialize(request)
        )
      );
    } finally {
      release?.();
    }
  };
