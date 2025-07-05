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


import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import type { UserStatus } from './user.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { uint8ToBuf } from '../number.utils.js';


export type UpdateUser = {
  userId: Id,
  username?: string,
  status?: UserStatus
};

export const UPDATE_USER = {
  code: COMMAND_CODE.UpdateUser,

  serialize: ({ userId, username, status }: UpdateUser) => {

    const bId = serializeIdentifier(userId);
    let bUsername, bStatus;

    if (username) {
      const bn = Buffer.from(username);

      if (bn.length < 1 || bn.length > 255)
        throw new Error('User username should be between 1 and 255 bytes');

      bUsername = Buffer.concat([
        uint8ToBuf(1),
        uint8ToBuf(bn.length),
        bn
      ]);
    } else
      bUsername = uint8ToBuf(0);

    if (status)
      bStatus = Buffer.concat([uint8ToBuf(1), uint8ToBuf(status)]);
    else
      bStatus = uint8ToBuf(0);

    return Buffer.concat([
      bId,
      bUsername,
      bStatus,
    ]);
  },

  deserialize: deserializeVoidResponse
};


export const updateUser = wrapCommand<UpdateUser, boolean>(UPDATE_USER);
