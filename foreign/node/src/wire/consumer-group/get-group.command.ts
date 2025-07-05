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


import type { CommandResponse } from '../../client/client.type.js';
import { type Id } from '../identifier.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import {
  serializeTargetGroup, deserializeConsumerGroup,
  type ConsumerGroup
} from './group.utils.js';

export type GetGroup = {
  streamId: Id,
  topicId: Id,
  groupId: Id
};

export const GET_GROUP = {
  code: COMMAND_CODE.GetGroup,

  serialize: ({streamId, topicId, groupId}: GetGroup) => {
    return serializeTargetGroup(streamId, topicId, groupId);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeConsumerGroup(r.data).data;
  }
};


export const getGroup = wrapCommand<GetGroup, ConsumerGroup>(GET_GROUP);
