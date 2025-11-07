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


import { type Id } from '../identifier.utils.js';
import { serializeSendMessages, type CreateMessage } from './message.utils.js';
import type { Partitioning } from './partitioning.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';

export type SendMessages = {
  streamId: Id,
  topicId: Id,
  messages: CreateMessage[],
  partition?: Partitioning,
};

export const SEND_MESSAGES = {
  code: COMMAND_CODE.SendMessages,

  serialize: ({ streamId, topicId, messages, partition }: SendMessages) => {
    return serializeSendMessages(streamId, topicId, messages, partition);
  },

  deserialize: deserializeVoidResponse
};

export const sendMessages = wrapCommand<SendMessages, boolean>(SEND_MESSAGES);
