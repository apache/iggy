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

import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializePage, serializePageQuery, type Page, type PageQuery } from '../page.utils.js';
import { deserializePartition, type Partition } from '../topic/topic.utils.js';

/**
 * Parameters for the get partitions page command: the topic and one page
 * request. A partition has no name, so the request is a `PageQuery`. The
 * client refuses a request with a name before it sends anything.
 */
export type GetPartitionsPage = {
  /** Stream identifier (ID or name) */
  streamId: Id,
  /** Topic identifier (ID or name) */
  topicId: Id
} & PageQuery;

/**
 * Get partitions command definition.
 * Retrieves one page of the partitions of a topic.
 */
export const GET_PARTITIONS = {
  code: COMMAND_CODE.GetPartitions,

  serialize: ({ streamId, topicId, ...query }: GetPartitionsPage) => Buffer.concat([
    serializeIdentifier(streamId),
    serializeIdentifier(topicId),
    serializePageQuery(COMMAND_CODE.GetPartitions, query)
  ]),

  deserialize: (r: CommandResponse) => deserializePage(r.data, deserializePartition)
};

/**
 * Executable get partitions page command function.
 */
export const getPartitionsPage = wrapCommand<GetPartitionsPage, Page<Partition>>(GET_PARTITIONS);
