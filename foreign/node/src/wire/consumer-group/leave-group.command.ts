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
import { serializeTargetGroup } from './group.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';

export type LeaveGroup = {
  streamId: Id,
  topicId: Id,
  groupId: Id
};

export const LEAVE_GROUP = {
  code: COMMAND_CODE.LeaveGroup,

  serialize: ({streamId, topicId, groupId}: LeaveGroup) => {
    return serializeTargetGroup(streamId, topicId, groupId);
  },

  deserialize: deserializeVoidResponse
};

export const leaveGroup = wrapCommand<LeaveGroup, boolean>(LEAVE_GROUP);
