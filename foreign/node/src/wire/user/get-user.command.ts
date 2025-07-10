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
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializeUser, type User } from './user.utils.js';


export type GetUser = {
  userId: Id
};

export const GET_USER = {
  code: COMMAND_CODE.GetUser,

  serialize: ({userId}: GetUser) => {
    return serializeIdentifier(userId);
  },

  deserialize: (r: CommandResponse) => deserializeUser(r.data)
};


export const getUser = wrapCommand<GetUser, User>(GET_USER);
