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


export type PollMessages = {
  streamId: Id,
  topicId: Id,
  consumer: Consumer,
  partitionId: number | null,
  pollingStrategy: PollingStrategy,
  count: number,
  autocommit: boolean
};

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

export const pollMessages = wrapCommand<PollMessages, PollMessagesResponse>(POLL_MESSAGES);
