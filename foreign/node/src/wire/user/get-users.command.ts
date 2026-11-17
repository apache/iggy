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
import {
  deserializePage, readWholeList, serializeNamedPageQuery, type NamedPageQuery, type Page
} from '../page.utils.js';
import { deserializeBaseUser, type BaseUser } from './user.utils.js';


/**
 * Get users command definition.
 * Retrieves one page of the users (without permissions).
 */
export const GET_USERS = {
  code: COMMAND_CODE.GetUsers,

  serialize: (query: NamedPageQuery) => serializeNamedPageQuery(COMMAND_CODE.GetUsers, query),

  deserialize: (r: CommandResponse) => deserializePage(r.data, deserializeBaseUser)
};


/**
 * Executable get users page command function.
 */
export const getUsersPage = wrapCommand<NamedPageQuery, Page<BaseUser>>(GET_USERS);

/**
 * Executable get users command function. Reads every user page by page.
 */
export const getUsers = (c: ClientProvider) =>
  (): Promise<BaseUser[]> => readWholeList(c, GET_USERS, (query) => query);
