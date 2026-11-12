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

import type { ClientProvider } from '../../client/client.type.js';
import type { Id } from '../identifier.utils.js';
import type { Checkpoint } from '../message/checkpoint.js';
import type { Consumer } from './offset.utils.js';
import { storeOffset } from './store-offset.command.js';
import { deleteOffset } from './delete-offset.command.js';

/** Parameters of a store or delete of the position of a checkpoint. */
export type ConsumerPosition = {
  /** Stream identifier (ID or name) */
  streamId: Id,
  /** Topic identifier (ID or name) */
  topicId: Id,
  /** Consumer identifier (single or group) */
  consumer: Consumer,
  /** A checkpoint of a read, such as `PolledBatch.checkpoint()` */
  checkpoint: Checkpoint
};

/**
 * Commits a checkpoint: stores its offset under the partition history that
 * delivered its message. The server refuses it with 87 when the partition was
 * deleted and recreated since, and with 5009 when the partition moved to
 * another member of the consumer group.
 */
export const storePosition = (getClient: ClientProvider) =>
  ({ streamId, topicId, consumer, checkpoint }: ConsumerPosition): Promise<boolean> =>
    storeOffset(getClient)({
      streamId, topicId, consumer, partitionId: checkpoint.partitionId, offset: checkpoint.offset,
      context: checkpoint.context
    });

/** Deletes the offset of the partition of a checkpoint under the history that delivered it. */
export const deletePosition = (getClient: ClientProvider) =>
  ({ streamId, topicId, consumer, checkpoint }: ConsumerPosition): Promise<boolean> =>
    deleteOffset(getClient)({ streamId, topicId, consumer, partitionId: checkpoint.partitionId,
      context: checkpoint.context });
