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
import { responseError } from '../error.utils.js';
import { SYNC_GROUP } from '../consumer-group/sync-group.command.js';
import {
  serializePollMessages, deserializePollMessages,
  type PollingStrategy, type PollMessagesResponse
} from './poll.utils.js';

const RESYNC_REQUIRED_PARTITION = 0xFFFF_FFFF;
const GROUP_POLL_MAX_ATTEMPTS = 2;

type GroupCursor = {
  generation: bigint,
  partitions: number[],
  position: number,
};

const groupCursors = new WeakMap<RawClient, Map<string, GroupCursor>>();

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

const groupKey = ({ streamId, topicId, consumer }: PollMessages): string =>
  `${String(streamId)}\0${String(topicId)}\0${String(consumer.id)}`;

const syncAssignment = async (
  client: RawClient,
  request: PollMessages,
): Promise<GroupCursor> => {
  const response = await client.sendCommand(
    SYNC_GROUP.code,
    SYNC_GROUP.serialize({
      streamId: request.streamId,
      topicId: request.topicId,
      groupId: request.consumer.id,
    }),
  );
  const assignment = SYNC_GROUP.deserialize(response);
  if (assignment === null)
    throw responseError(SYNC_GROUP.code, 5006);

  let cursors = groupCursors.get(client);
  if (!cursors) {
    cursors = new Map();
    groupCursors.set(client, cursors);
    client.once('sessionReset', () => groupCursors.delete(client));
  }
  const key = groupKey(request);
  const current = cursors.get(key);
  if (current && current.generation === assignment.generation) {
    current.partitions = assignment.partitions;
    if (current.position >= current.partitions.length)
      current.position = 0;
    return current;
  }
  const cursor = { ...assignment, position: 0 };
  cursors.set(key, cursor);
  return cursor;
};

const pollConsumerGroup = async (
  client: RawClient,
  request: PollMessages,
): Promise<PollMessagesResponse> => {
  for (let attempt = 0; attempt < GROUP_POLL_MAX_ATTEMPTS; attempt += 1) {
    const cursor = await syncAssignment(client, request);
    if (cursor.partitions.length === 0)
      return { partitionId: 0, currentOffset: 0n, count: 0, messages: [] };

    const partitionId = cursor.partitions[cursor.position];
    cursor.position = (cursor.position + 1) % cursor.partitions.length;
    const response = await client.sendCommand(
      POLL_MESSAGES.code,
      POLL_MESSAGES.serialize({ ...request, partitionId }),
    );
    const polled = POLL_MESSAGES.deserialize(response);
    if (polled.count === 0 &&
        polled.partitionId === RESYNC_REQUIRED_PARTITION)
      continue;
    return polled;
  }
  return { partitionId: 0, currentOffset: 0n, count: 0, messages: [] };
};

/**
 * Executable poll messages command function.
 */
export const pollMessages = (getClient: ClientProvider) =>
  async (request: PollMessages): Promise<PollMessagesResponse> => {
    const client = await getClient();
    if (client.protocol === 'vsr' &&
        request.consumer.kind === ConsumerKind.Group &&
        request.partitionId === null)
      return pollConsumerGroup(client, request);
    return POLL_MESSAGES.deserialize(
      await client.sendCommand(POLL_MESSAGES.code, POLL_MESSAGES.serialize(request))
    );
  };
