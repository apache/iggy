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
import type { Id } from '../identifier.utils.js';
import { serializePartitionParams } from './partition.utils.js';
import { COMMAND_CODE } from '../command.code.js';

export type DeletePartition = {
  streamId: Id,
  topicId: Id,
  partitionCount: number
};
    
export const DELETE_PARTITIONS = {
  code: COMMAND_CODE.DeletePartitions,

  serialize: ({ streamId, topicId, partitionCount }: DeletePartition) => {
    return serializePartitionParams(streamId, topicId, partitionCount);
  },
  
  deserialize: deserializeVoidResponse
};


export const deletePartition = wrapCommand<DeletePartition, boolean>(DELETE_PARTITIONS);
