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

import type { ClientProvider, CommandResponse } from '../../client/client.type.js';
import type { ConsumerGroup } from './group.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializeConsumerGroup } from './group.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import {
  deserializePage, readWholeList, serializeNamedPageQuery, type NamedPageQuery, type Page
} from '../page.utils.js';

/**
 * Parameters for the get consumer groups command.
 */
export type GetGroups = {
  /** Stream identifier (ID or name) */
  streamId: Id,
  /** Topic identifier (ID or name) */
  topicId: Id
};

/**
 * Parameters for the get consumer groups page command: the topic and one page
 * request.
 */
export type GetGroupsPage = GetGroups & NamedPageQuery;

/**
 * Get consumer groups command definition.
 * Retrieves one page of the consumer groups within a topic.
 */
export const GET_GROUPS = {
  code: COMMAND_CODE.GetGroups,

  serialize: ({ streamId, topicId, ...query }: GetGroupsPage) => {
    return Buffer.concat([
      serializeIdentifier(streamId),
      serializeIdentifier(topicId),
      serializeNamedPageQuery(COMMAND_CODE.GetGroups, query)
    ]);
  },

  deserialize: (r: CommandResponse) => deserializePage(r.data, deserializeConsumerGroup)
};


/**
 * Executable get consumer groups page command function.
 */
export const getGroupsPage = wrapCommand<GetGroupsPage, Page<ConsumerGroup>>(GET_GROUPS);

/**
 * Executable get consumer groups command function. Reads every consumer group
 * of the topic page by page.
 */
export const getGroups = (c: ClientProvider) =>
  ({ streamId, topicId }: GetGroups): Promise<ConsumerGroup[]> =>
    readWholeList(c, GET_GROUPS, (query) => ({ streamId, topicId, ...query }));
