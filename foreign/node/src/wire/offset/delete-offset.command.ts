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

import { deserializeVoidResponse } from '../../client/client.utils.js';
import type { PartitionContext } from '../vsr/header.js';
import type { GetOffset } from './get-offset.command.js';
import { wrapCommand } from '../command.utils.js';
import { serializeDeleteOffset } from './offset.utils.js';
import { COMMAND_CODE } from '../command.code.js';


/**
 * Parameters for the delete offset command: the GetOffset parameters and an
 * optional partition context. Without a context the client takes the one its
 * cached route reports: after another client deletes and recreates the
 * partition, one delete can fail with 87 (5009 for a group consumer). The
 * failed route is dropped and the next call routes again.
 */
export type DeleteOffset = GetOffset & {
  /** Context of the poll the delete belongs to (`PollMessagesResponse.context`) */
  context?: PartitionContext
};

/**
 * Delete offset command definition.
 * Removes a stored consumer offset.
 */
export const DELETE_OFFSET = {
  code: COMMAND_CODE.DeleteConsumerOffset,

  serialize: ({ streamId, topicId, consumer, partitionId = 0 }: DeleteOffset) => {
    return serializeDeleteOffset(streamId, topicId, consumer, partitionId);
  },

  deserialize: deserializeVoidResponse
};


/**
 * Executable delete offset command function.
 */
export const deleteOffset = wrapCommand<DeleteOffset, boolean>(DELETE_OFFSET, ({ context }) => ({ context }));
