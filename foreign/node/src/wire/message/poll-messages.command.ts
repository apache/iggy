/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import type { Id } from '../identifier.utils.js';
import type { CommandResponse } from '../../client/client.type.js';
import type { Consumer } from '../offset/offset.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import {
  serializePollMessages, deserializePollMessages,
  type PollingStrategy, type PollMessagesResponse
} from './poll.utils.js';


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

/**
 * Executable poll messages command function.
 */
export const pollMessages = wrapCommand<PollMessages, PollMessagesResponse>(POLL_MESSAGES);
