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
import { deserializeCreateToken, type CreateTokenResponse } from './token.utils.js';
import { wrapCommand } from '../command.utils.js';

export type CreateToken = {
  name: string,
  expiry?: bigint
}


export const CREATE_TOKEN = {
  code: 42,

  serialize: ({name, expiry = 600n}: CreateToken): Buffer => {
    const bName = Buffer.from(name);
    if (bName.length < 1 || bName.length > 255)
      throw new Error('Token name should be between 1 and 255 bytes');
    const b1 = Buffer.alloc(1);
    b1.writeUInt8(bName.length);
    const b2 = Buffer.alloc(8);
    b2.writeBigUInt64LE(expiry);
    return Buffer.concat([
      b1,
      bName,
      b2
    ]);
  },

  deserialize: (r: CommandResponse) => deserializeCreateToken(r.data).data
};

export const createToken = wrapCommand<CreateToken, CreateTokenResponse>(CREATE_TOKEN);
