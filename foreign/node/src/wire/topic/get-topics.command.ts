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
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import {
  deserializePage, readWholeList, serializeNamedPageQuery, type NamedPageQuery, type Page
} from '../page.utils.js';
import { deserializeBaseTopic, type Topic } from './topic.utils.js';

/**
 * Parameters for the get topics command.
 */
export type GetTopics = {
  /** Stream identifier (ID or name) */
  streamId: Id
};

/**
 * Parameters for the get topics page command: the stream and one page request.
 */
export type GetTopicsPage = GetTopics & NamedPageQuery;

/**
 * Get topics command definition.
 * Retrieves one page of the topics within a stream. A topic of a page carries
 * no partition details.
 */
export const GET_TOPICS = {
  code: COMMAND_CODE.GetTopics,

  serialize: ({ streamId, ...query }: GetTopicsPage) => Buffer.concat([
    serializeIdentifier(streamId),
    serializeNamedPageQuery(COMMAND_CODE.GetTopics, query)
  ]),

  deserialize: (r: CommandResponse) => deserializePage(r.data, deserializeTopicHeader)
};

/**
 * Executable get topics page command function.
 */
export const getTopicsPage = wrapCommand<GetTopicsPage, Page<Topic>>(GET_TOPICS);

/**
 * Executable get topics command function. Reads every topic of the stream page
 * by page.
 */
export const getTopics = (c: ClientProvider) =>
  ({ streamId }: GetTopics): Promise<Topic[]> =>
    readWholeList(c, GET_TOPICS, (query) => ({ streamId, ...query }));

const deserializeTopicHeader = (p: Buffer, pos: number): { bytesRead: number, data: Topic } => {
  const { bytesRead, data } = deserializeBaseTopic(p, pos);
  return { bytesRead, data: { ...data, partitions: [] } };
};
