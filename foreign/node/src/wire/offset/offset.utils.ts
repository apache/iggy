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


import type { ValueOf } from '../../type.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { uint8ToBuf } from '../number.utils.js';

/**
 * Consumer kind options for offset operations.
 */
export const ConsumerKind = {
  /** Single consumer (not part of a group) */
  Single: 1,
  /** Consumer group member */
  Group: 2
} as const;

/** Type alias for the ConsumerKind object */
export type ConsumerKind = typeof ConsumerKind;
/** String literal type of consumer kind names */
export type ConsumerKindId = keyof ConsumerKind;
/** Numeric values of consumer kinds */
export type ConsumerKindValue = ValueOf<ConsumerKind>


/**
 * Consumer identifier for offset operations.
 */
export type Consumer = {
  /** Consumer kind (Single or Group) */
  kind: ConsumerKindValue,
  /** Consumer or group identifier */
  id: Id
}

/**
 * Default single consumer instance.
 */
export const ConsumerSingle = {
  kind: ConsumerKind.Single,
  id: 0
};

/** Type for single consumer */
export type ConsumerSingle = typeof ConsumerSingle;

/**
 * Consumer factory for creating consumer identifiers.
 */
export const Consumer = {
  /** Single consumer instance */
  Single: ConsumerSingle,
  /** Creates a group consumer identifier */
  Group: (groupId: Id) => ({
    kind: ConsumerKind.Group,
    id: groupId
  })
}

/**
 * Response from offset operations.
 */
export type OffsetResponse = {
  /** Partition ID */
  partitionId: number,
  /** Current offset in the partition */
  currentOffset: bigint,
  /** Stored consumer offset */
  storedOffset: bigint
};

/**
 * Serializes parameters for get/delete offset operations.
 *
 * @param streamId - Stream identifier (ID or name)
 * @param topicId - Topic identifier (ID or name)
 * @param consumer - Consumer identifier (single or group)
 * @param partitionId - Partition ID (required for single consumer, optional for group)
 * @returns Buffer containing serialized offset request
 * @throws Error if partitionId is null for single consumer kind
 */
export const serializeGetOffset = (
  streamId: Id,
  topicId: Id,
  consumer: Consumer,
  partitionId: number | null
) => {

  if (consumer.kind === ConsumerKind.Single && partitionId === null)
    throw new Error('getOffset error: partitionId must be provided for single consumer kind');

  const streamIdentifier = serializeIdentifier(streamId);
  const topicIdentifier = serializeIdentifier(topicId);
  const consumerIdentifier = serializeIdentifier(consumer.id);
  const b1 = uint8ToBuf(consumer.kind);

  // Encode partition_id with a flag byte: 1 = Some, 0 = None
  const b2 = Buffer.allocUnsafe(5);
  if (partitionId !== null) {
    b2.writeUInt8(1, 0); // Flag byte: partition_id is Some
    b2.writeUInt32LE(partitionId, 1);
  } else {
    b2.writeUInt8(0, 0); // Flag byte: partition_id is None
    b2.writeUInt32LE(0, 1); // Padding
  }

  return Buffer.concat([
    b1,
    consumerIdentifier,
    streamIdentifier,
    topicIdentifier,
    b2
  ]);
};

/**
 * Serializes parameters for store offset operation.
 *
 * @param streamId - Stream identifier (ID or name)
 * @param topicId - Topic identifier (ID or name)
 * @param consumer - Consumer identifier (single or group)
 * @param partitionId - Partition ID (required for single consumer, optional for group)
 * @param offset - Offset value to store
 * @returns Buffer containing serialized store offset request
 */
export const serializeStoreOffset = (
  streamId: Id,
  topicId: Id,
  consumer: Consumer,
  partitionId: number | null,
  offset: bigint
) => {
  const b = Buffer.allocUnsafe(8);
  b.writeBigUInt64LE(offset, 0);

  return Buffer.concat([
    serializeGetOffset(streamId, topicId, consumer, partitionId),
    b
  ]);
}
