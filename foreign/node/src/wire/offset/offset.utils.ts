
import type { ValueOf } from '../../type.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { uint32ToBuf, uint8ToBuf } from '../number.utils.js';

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

export type OffsetResponse = {
  partitionId: number,
  currentOffset: bigint,
  storedOffset: bigint
};

export const serializeGetOffset = (
  streamId: Id,
  topicId: Id,
  consumer: Consumer,
  partitionId?: number
) => {

  if (consumer.kind === ConsumerKind.Single && (!partitionId || partitionId < 1))
    throw new Error('getOffset error: partitionId must be > 0 for single consumer kind');

  const streamIdentifier = serializeIdentifier(streamId);
  const topicIdentifier = serializeIdentifier(topicId);
  const consumerIdentifier = serializeIdentifier(consumer.id);

  const b1 = uint8ToBuf(consumer.kind);
  const b2 = uint32ToBuf(partitionId || 0);

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
  partitionId: number,
  offset: bigint
) => {
  const b = Buffer.allocUnsafe(8);
  b.writeBigUInt64LE(offset, 0);

  return Buffer.concat([
    serializeGetOffset(streamId, topicId, consumer, partitionId),
    b
  ]);
}
