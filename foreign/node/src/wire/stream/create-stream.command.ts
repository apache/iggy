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
import { deserializeToStream, type Stream } from './stream.utils.js';

export type CreateStream = {
  streamId?: number, // Optional - auto-assigned by server if not provided
  name: string
};

export const CREATE_STREAM = {
  code: COMMAND_CODE.CreateStream,

  serialize: ({name}: CreateStream) => {
    // Stream ID is now auto-assigned by the server, not sent in the protocol
    const bName = Buffer.from(name);

    if (bName.length < 1 || bName.length > 255)
      throw new Error('Stream name should be between 1 and 255 bytes');

    const b = Buffer.allocUnsafe(1);
    b.writeUInt8(bName.length, 0);

    return Buffer.concat([
      b,
      bName
    ]);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeToStream(r.data, 0).data
  }
};


export const createStream = wrapCommand<CreateStream, Stream>(CREATE_STREAM);
