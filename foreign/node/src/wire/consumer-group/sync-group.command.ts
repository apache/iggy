// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import {
  serializeTargetGroup,
  type TargetGroup,
} from './group.utils.js';

/** Current assignment for a consumer-group member. */
export type ConsumerGroupAssignment = {
  /** Monotonic group generation */
  generation: bigint,
  /** Partition IDs currently owned by this member */
  partitions: number[],
};

export const SYNC_GROUP = {
  code: COMMAND_CODE.SyncGroup,

  serialize: ({ streamId, topicId, groupId }: TargetGroup) =>
    serializeTargetGroup(streamId, topicId, groupId),

  deserialize: (response: CommandResponse): ConsumerGroupAssignment | null => {
    if (response.data.length === 0)
      return null;
    const generation = response.data.readBigUInt64LE(0);
    const partitionsCount = response.data.readUInt32LE(8);
    const partitions = new Array<number>(partitionsCount);
    for (let index = 0; index < partitionsCount; index += 1)
      partitions[index] = response.data.readUInt32LE(12 + index * 4);
    return { generation, partitions };
  },
};

/** Fetches this connection's current consumer-group assignment. */
export const syncGroup =
  wrapCommand<TargetGroup, ConsumerGroupAssignment | null>(SYNC_GROUP);
