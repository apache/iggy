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
import { deserializeUser, type User, type UserStatus } from './user.utils.js';
import { uint8ToBuf, uint32ToBuf, boolToBuf } from '../number.utils.js';
import { serializePermissions, type UserPermissions } from './permissions.utils.js';


export type CreateUser = {
  username: string,
  password: string,
  status: UserStatus
  permissions?: UserPermissions
};

export const CREATE_USER = {
  code: COMMAND_CODE.CreateUser,

  serialize: ({ username, password, status, permissions }: CreateUser) => {
    const bUsername = Buffer.from(username);
    const bPassword = Buffer.from(password);

    if (bUsername.length < 1 || bUsername.length > 255)
      throw new Error('User username should be between 1 and 255 bytes');

    if (bPassword.length < 1 || bPassword.length > 255)
      throw new Error('User password should be between 1 and 255 bytes');

    const bPermissions = serializePermissions(permissions);

    return Buffer.concat([
      uint8ToBuf(bUsername.length),
      bUsername,
      uint8ToBuf(bPassword.length),
      bPassword,
      uint8ToBuf(status),
      boolToBuf(!!permissions),
      uint32ToBuf(bPermissions.length),
      bPermissions
    ]);
  },

  deserialize: (r: CommandResponse) => deserializeUser(r.data)
};


export const createUser = wrapCommand<CreateUser, User>(CREATE_USER);
