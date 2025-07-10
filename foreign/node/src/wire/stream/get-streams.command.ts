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

export const GET_STREAMS = {
  code: COMMAND_CODE.GetStreams,
  
  serialize: () => Buffer.alloc(0),
  
  deserialize: (r: CommandResponse) => {
    const payloadSize = r.data.length;
    const streams = [];
    let pos = 0;
    while (pos < payloadSize) {
      const { bytesRead, data } = deserializeToStream(r.data, pos)
      streams.push(data);
      pos += bytesRead;
    }
    return streams;
  }
};


export const getStreams = wrapCommand<void, Stream[]>(GET_STREAMS);
