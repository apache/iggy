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
import { ClientProvider } from '../../client/index.js';
import { createGroup } from './create-group.command.js';
import { getGroup } from './get-group.command.js';
import { joinGroup } from './join-group.command.js';


export const ensureConsumerGroup = (c: ClientProvider) =>
  async function ensureConsumerGroup(
    streamId: Id,
    topicId: Id,
    groupName: string
  ) {
    const group = await getGroup(c)({ streamId, topicId, groupId: groupName });
    if (!group)
      return await createGroup(c)({ streamId, topicId, name: groupName });
    return group;
  }

export const ensureConsumerGroupAndJoin = (c: ClientProvider) =>
  async function ensureConsumerGroupAndJoin(
    streamId: Id,
    topicId: Id,
    groupName: string
  ) {
    const group = await ensureConsumerGroup(c)(streamId, topicId, groupName);
    await joinGroup(c)({ streamId, topicId, groupId: group.id });
    return group;
  };

