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
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { type Id } from '../identifier.utils.js';
import type { PartitionContext } from '../vsr/header.js';
import { serializeStoreOffset, type Consumer } from './offset.utils.js';

/**
 * Parameters for the store offset command.
 */
export type StoreOffset = {
  /** Stream identifier (ID or name) */
  streamId: Id,
  /** Topic identifier (ID or name) */
  topicId: Id,
  /** Consumer identifier (single or group) */
  consumer: Consumer,
  /** Partition ID. VSR requires the explicit partition returned by polling. */
  partitionId: number | null,
  /** Offset value to store */
  offset: bigint,
  /**
   * Context of the poll the offset came from (`PollMessagesResponse.context`).
   * The store then fails instead of committing the offset in a partition
   * deleted and recreated since that poll. Without one the client takes the
   * context its cached route reports: after another client deletes and
   * recreates the partition, one store can fail with 87 (5009 for a group
   * consumer). The failed route is dropped and the next call routes again.
   */
  context?: PartitionContext
};

/**
 * Store offset command definition.
 * Persists a consumer's offset for a partition.
 */
export const STORE_OFFSET = {
  code: COMMAND_CODE.StoreOffset,

  serialize: ({streamId, topicId, consumer, partitionId, offset}: StoreOffset) =>
    serializeStoreOffset(streamId, topicId, consumer, partitionId, offset),

  deserialize: deserializeVoidResponse
};


/**
 * Executable store offset command function.
 */
export const storeOffset = wrapCommand<StoreOffset, boolean>(STORE_OFFSET, ({ context }) => ({ context }));
