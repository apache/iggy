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
import type { Id } from '../identifier.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { serializeGetOffset, type Consumer, type OffsetResponse } from './offset.utils.js';

export type GetOffset = {
  streamId: Id,
  topicId: Id,
  consumer: Consumer,
  partitionId: number | null
};


export const GET_OFFSET = {
  code: COMMAND_CODE.GetOffset,

  serialize: ({ streamId, topicId, consumer, partitionId = 0 }: GetOffset) => {
    return serializeGetOffset(streamId, topicId, consumer, partitionId);
  },

  deserialize: (r: CommandResponse) => {
    if (r.status === 0 && r.length === 0)
      return null;

    const partitionId = r.data.readUInt32LE(0);
    const currentOffset = r.data.readBigUInt64LE(4);
    const storedOffset = r.data.readBigUInt64LE(12);

    return {
      partitionId,
      currentOffset,
      storedOffset
    }
  }
};


export const getOffset = wrapCommand<GetOffset, OffsetResponse | null>(GET_OFFSET);
