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
import type { LoginResponse } from './login.type.js';
import { wrapCommand } from '../command.utils.js';

export type LoginWithTokenParam = {
  token: string
};


export const LOGIN_WITH_TOKEN = {
  code: 44,

  serialize: ({token}: LoginWithTokenParam) => {
    const bToken = Buffer.from(token);
    if (bToken.length < 1 || bToken.length > 255)
      throw new Error('Token length should be between 1 and 255 bytes');
    const b = Buffer.alloc(1);
    b.writeUInt8(bToken.length);
    return Buffer.concat([
      b,
      bToken
    ]);
  },

  deserialize: (r: CommandResponse) => ({
    userId: r.data.readUInt32LE(0)
  })
};

export const loginWithToken =
  wrapCommand<LoginWithTokenParam, LoginResponse>(LOGIN_WITH_TOKEN);
