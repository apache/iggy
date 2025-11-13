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

export const ConsumerKind = {
  Single: 1,
  Group: 2
} as const;

export type ConsumerKind = typeof ConsumerKind;
export type ConsumerKindId = keyof ConsumerKind;
export type ConsumerKindValue = ValueOf<ConsumerKind>


export type Consumer = {
  kind: ConsumerKindValue,
  id: Id
}

export const ConsumerSingle = {
  kind: ConsumerKind.Single,
  id: 0
};

export type ConsumerSingle = typeof ConsumerSingle;

export const Consumer = {
  Single: ConsumerSingle,
  Group: (groupId: Id) => ({
    kind: ConsumerKind.Group,
    id: groupId
  })
}

export type OffsetResponse = {
  partitionId: number,
  currentOffset: bigint,
  storedOffset: bigint
};

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
