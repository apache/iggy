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

import { xxh32 } from '@node-rs/xxhash';
import { idKey, type Id } from '../identifier.utils.js';
import { serializeSendMessages, type CreateMessage } from './message.utils.js';
import { Partitioning, PartitionKind, serializeMessageKey } from './partitioning.utils.js';
import type { ClientProvider, CommandResponse, RawClient } from '../../client/client.type.js';
import { DeserializeError, ResponseError, responseError } from '../error.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { GET_TOPIC } from '../topic/get-topic.command.js';

/** Size of the confirmation count prefixing the list. */
const CONFIRMATIONS_COUNT_SIZE = 4;

/**
 * Size of one confirmation entry:
 * `stream_id(4) + topic_id(4) + partition_id(4) + base_offset(8)`.
 */
const CONFIRMATION_SIZE = 20;

/**
 * Parameters for the send messages command.
 */
export type SendMessages = {
  /** Stream identifier */
  streamId: Id,
  /** Topic identifier */
  topicId: Id,
  /** Array of messages to send */
  messages: CreateMessage[],
  /** Optional partitioning strategy */
  partition?: Partitioning,
};

/** Commit confirmation for one partition written by a send. */
export type SendMessagesConfirmation = {
  /** Numeric id of the stream the batch was written to */
  streamId: number,
  /** Numeric id of the topic the batch was written to */
  topicId: number,
  /** Partition the batch was written to */
  partitionId: number,
  /**
   * Offset assigned to the first message of the batch in that partition.
   *
   * Delivery is at-least-once, so an earlier retry of the same batch may
   * already have committed at a lower offset: this never identifies a batch
   * uniquely. Confirmation follows VSR quorum commit. Persisted message
   * durability also requires recoverable stable-storage copies on the quorum.
   */
  baseOffset: bigint,
};

/**
 * Outcome of a successful send, one confirmation per written partition. The
 * legacy server returns an empty list: it commits without reporting offsets.
 */
export type SendMessagesResponse = {
  /** Commit confirmations, one per partition the batch was written to */
  confirmations: SendMessagesConfirmation[],
};

/**
 * Decodes the reply body of a send: `[confirmations_count:4]` then that many
 * `[stream_id:4 topic_id:4 partition_id:4 base_offset:8]` entries.
 *
 * The legacy server reports a commit by sending no body at all, so absence
 * decodes to no confirmations instead of surfacing as a decode failure.
 */
const deserializeSendMessages = (data: Buffer): SendMessagesResponse => {
  if (data.length === 0) return { confirmations: [] };
  if (data.length < CONFIRMATIONS_COUNT_SIZE)
    throw new DeserializeError('send messages confirmation count is truncated');

  const count = data.readUInt32LE(0);
  const expected = CONFIRMATIONS_COUNT_SIZE + count * CONFIRMATION_SIZE;
  if (expected > data.length)
    throw new DeserializeError('send messages confirmation list is truncated');
  if (expected !== data.length)
    throw new DeserializeError('send messages confirmations have trailing bytes');

  const confirmations = new Array<SendMessagesConfirmation>(count);
  for (let index = 0; index < count; index += 1) {
    const at = CONFIRMATIONS_COUNT_SIZE + index * CONFIRMATION_SIZE;
    confirmations[index] = {
      streamId: data.readUInt32LE(at),
      topicId: data.readUInt32LE(at + 4),
      partitionId: data.readUInt32LE(at + 8),
      baseOffset: data.readBigUInt64LE(at + 12)
    };
  }
  return { confirmations };
};

/**
 * Send messages command definition.
 * Publishes messages to a topic.
 */
export const SEND_MESSAGES = {
  code: COMMAND_CODE.SendMessages,

  serialize: ({ streamId, topicId, messages, partition }: SendMessages) => {
    return serializeSendMessages(streamId, topicId, messages, partition);
  },

  deserialize: (r: CommandResponse) => deserializeSendMessages(r.data)
};

/** Rust `calculate_32` hashes message keys with XXH32 under this seed. */
const MESSAGE_KEY_SEED = 0;
const TOPIC_ID_NOT_FOUND = 2010;

type TopicPartitions = { count?: number, cursor: number };

const topicPartitions = new WeakMap<RawClient, Map<string, TopicPartitions>>();

const getTopicPartitions = (client: RawClient, streamId: Id, topicId: Id): TopicPartitions => {
  let topics = topicPartitions.get(client);
  if (!topics) {
    const created = new Map<string, TopicPartitions>();
    topicPartitions.set(client, created);
    // The client's own stream, topic or partition change can alter any
    // count, as it drops the Rust SDK's topic discovery.
    client.on('topicDiscoveryReset', () => {
      for (const topic of created.values())
        topic.count = undefined;
    });
    topics = created;
  }
  const key = `${idKey(streamId)}\0${idKey(topicId)}`;
  let topic = topics.get(key);
  if (!topic) {
    topic = { cursor: 0 };
    topics.set(key, topic);
  }
  return topic;
};

/**
 * Resolves Balanced and MessageKey to an explicit partition id as the Rust SDK
 * does, so the send carries that partition's incarnation.
 */
const resolvePartitioning = async (
  client: RawClient,
  { streamId, topicId, partition }: SendMessages
): Promise<Partitioning> => {
  if (partition?.kind === PartitionKind.PartitionId)
    return partition;
  const topic = getTopicPartitions(client, streamId, topicId);
  topic.count ||= GET_TOPIC.deserialize(await client.sendCommand(
    GET_TOPIC.code, GET_TOPIC.serialize({ streamId, topicId })
  ))?.partitionsCount;
  if (!topic.count)
    throw responseError(COMMAND_CODE.SendMessages, TOPIC_ID_NOT_FOUND);
  if (partition?.kind === PartitionKind.MessageKey)
    return Partitioning.PartitionId(
      xxh32(serializeMessageKey(partition.value), MESSAGE_KEY_SEED) % topic.count);
  const index = topic.cursor % topic.count;
  topic.cursor += 1;
  return Partitioning.PartitionId(index);
};

/**
 * Executable send messages command function. Resolves to the commit
 * confirmations of the written partitions, empty against the legacy server.
 */
export const sendMessages = (getClient: ClientProvider) =>
  async (request: SendMessages): Promise<SendMessagesResponse> => {
    const client = await getClient();
    const release = client.hold?.();
    try {
      const partition = await resolvePartitioning(client, request);
      return SEND_MESSAGES.deserialize(await client.sendCommand(
        SEND_MESSAGES.code, SEND_MESSAGES.serialize({ ...request, partition })));
    } catch (error) {
      // A refusal can mean the topic changed shape since its count was read.
      if (error instanceof ResponseError)
        getTopicPartitions(client, request.streamId, request.topicId).count = undefined;
      throw error;
    } finally {
      release?.();
    }
  };
