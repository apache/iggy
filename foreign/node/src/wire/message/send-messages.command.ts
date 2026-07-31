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

import { type Id } from '../identifier.utils.js';
import { serializeSendMessages, type CreateMessage } from './message.utils.js';
import type { Partitioning } from './partitioning.utils.js';
import type { CommandResponse } from '../../client/client.type.js';
import { DeserializeError } from '../error.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';

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
   * uniquely. A batch is confirmed once it is committed in memory, not once it
   * is fsynced, so a crash-restart can stamp a later batch with an offset a
   * client has already recorded.
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

/**
 * Executable send messages command function. Resolves to the commit
 * confirmations of the written partitions, empty against the legacy server.
 */
export const sendMessages =
  wrapCommand<SendMessages, SendMessagesResponse>(SEND_MESSAGES);
