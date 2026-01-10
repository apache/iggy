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
import type { Id } from '../identifier.utils.js';
import { serializePartitionParams } from './partition.utils.js';

/**
 * Parameters for the create partition command.
 */
export type CreatePartition = {
  /** Stream identifier (ID or name) */
  streamId: Id,
  /** Topic identifier (ID or name) */
  topicId: Id,
  /** Number of partitions to create (1-1000, default: 1) */
  partitionCount?: number
};

/**
 * Create partition command definition.
 * Adds new partitions to a topic.
 */
export const CREATE_PARTITION = {
  code: COMMAND_CODE.CreatePartitions,

  serialize: ({ streamId, topicId, partitionCount = 1 }: CreatePartition) => {
    return serializePartitionParams(streamId, topicId, partitionCount);
  },

  deserialize: deserializeVoidResponse
};


/**
 * Executable create partition command function.
 */
export const createPartition = wrapCommand<CreatePartition, boolean>(CREATE_PARTITION);
