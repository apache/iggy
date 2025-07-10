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

import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { uint8ToBuf } from '../number.utils.js';

export type UpdateStream = {
  streamId: Id,
  name: string
}

export const UPDATE_STREAM = {
  code: COMMAND_CODE.UpdateStream,

  serialize: ({streamId, name}: UpdateStream) => {
    const bId = serializeIdentifier(streamId);
    const bName = Buffer.from(name);

    if (bName.length < 1 || bName.length > 255)
      throw new Error('Stream name should be between 1 and 255 bytes');

    return Buffer.concat([
      bId,
      uint8ToBuf(bName.length),
      bName
    ]);
  },

  deserialize: deserializeVoidResponse
};


export const updateStream = wrapCommand<UpdateStream, boolean>(UPDATE_STREAM);
