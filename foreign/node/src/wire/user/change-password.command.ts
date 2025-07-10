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


import { uint8ToBuf } from '../number.utils.js';
import { serializeIdentifier } from '../identifier.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';


export type ChangePassword = {
  userId: number,
  currentPassword: string,
  newPassword: string
};


export const CHANGE_PASSWORD = {
  code: COMMAND_CODE.ChangePassword,

  serialize: ({ userId, currentPassword, newPassword }: ChangePassword) => {

    const bId = serializeIdentifier(userId);
    const bCur = Buffer.from(currentPassword);
    const bNew = Buffer.from(newPassword);

    if (bCur.length < 1 || bCur.length > 255)
      throw new Error('User password should be between 1 and 255 bytes (current)');

    if (bNew.length < 1 || bNew.length > 255)
      throw new Error('User password should be between 1 and 255 bytes (new)');

    return Buffer.concat([
      bId,
      uint8ToBuf(bCur.length),
      bCur,
      uint8ToBuf(bNew.length),
      bNew
    ]);
  },

  deserialize: deserializeVoidResponse
};


export const changePassword = wrapCommand<ChangePassword, boolean>(CHANGE_PASSWORD);
