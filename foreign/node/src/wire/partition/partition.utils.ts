
import { serializeIdentifier, type Id } from '../identifier.utils.js';

export const serializePartitionParams = (
  streamId: Id, topicId: Id, partitionCount = 1,
) => {

  if (partitionCount < 1 || partitionCount > 1000)
    throw new Error('Topic partition_count must be between 1 and 1000');

  const streamIdentifier = serializeIdentifier(streamId);
  const topicIdentifier = serializeIdentifier(topicId);
  const b = Buffer.alloc(4);
  b.writeUInt32LE(partitionCount, 0);

  return Buffer.concat([
    streamIdentifier,
    topicIdentifier,
    b,
  ])
};
