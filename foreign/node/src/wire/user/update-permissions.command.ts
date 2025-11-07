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
import { uint32ToBuf, boolToBuf } from '../number.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { serializePermissions, type UserPermissions } from './permissions.utils.js';


export type UpdatePermissions = {
  userId: Id,
  permissions: UserPermissions
};

export const UPDATE_PERMISSIONS = {
  code: COMMAND_CODE.UpdatePermissions,

  serialize: ({ userId, permissions }: UpdatePermissions) => {

    const bPermissions = serializePermissions(permissions);

    return Buffer.concat([
      serializeIdentifier(userId),
      boolToBuf(!!permissions),
      uint32ToBuf(bPermissions.length),
      bPermissions
    ]);
  },

  deserialize: deserializeVoidResponse
};


export const updatePermissions = wrapCommand<UpdatePermissions, boolean>(UPDATE_PERMISSIONS);
