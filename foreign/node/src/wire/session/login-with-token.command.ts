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
import { serializeLoginWithToken, type LoginResponse } from './login.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';

export type LoginWithTokenParam = {
  token: string
};

export const LOGIN_WITH_TOKEN = {
  code: COMMAND_CODE.LoginWithAccessToken,

  serialize: ({token}: LoginWithTokenParam) => serializeLoginWithToken(token),

  deserialize: (r: CommandResponse) => ({
    userId: r.data.readUInt32LE(0)
  })
};


export const loginWithToken = wrapCommand<LoginWithTokenParam, LoginResponse>(LOGIN_WITH_TOKEN);
