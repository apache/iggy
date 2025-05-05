
import type { CommandResponse } from '../../client/client.type.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { wrapCommand } from '../command.utils.js';
import {
  isValidCompressionAlgorithm, CompressionAlgorithmKind,
  deserializeTopic,
  type Topic,
  type CompressionAlgorithm
} from './topic.utils.js';

export type CreateTopic = {
  streamId: Id,
  topicId: number,
  name: string,
  partitionCount: number,
  compressionAlgorithm: CompressionAlgorithm,
  messageExpiry?: bigint,
  maxTopicSize?: bigint,
  replicationFactor?: number
};

export const CREATE_TOPIC = {
  code: 302,
  serialize: ({
    streamId,
    topicId,
    name,
    partitionCount,
    compressionAlgorithm = CompressionAlgorithmKind.None,
    messageExpiry = 0n,
    maxTopicSize = 0n,
    replicationFactor = 1
  }: CreateTopic
  ) => {
    const streamIdentifier = serializeIdentifier(streamId);
    const bName = Buffer.from(name)
  
    if (replicationFactor < 1 || replicationFactor > 255)
      throw new Error('Topic replication factor should be between 1 and 255');
    if (bName.length < 1 || bName.length > 255)
      throw new Error('Topic name should be between 1 and 255 bytes');
    if(!isValidCompressionAlgorithm(compressionAlgorithm))
      throw new Error(`createTopic: invalid compressionAlgorithm (${compressionAlgorithm})`);
    
    const b = Buffer.allocUnsafe(4 + 4 + 1 + 8 + 8 + 1 + 1);
    b.writeUInt32LE(topicId, 0);
    b.writeUInt32LE(partitionCount, 4);
    b.writeUInt8(compressionAlgorithm, 8);
    b.writeBigUInt64LE(messageExpiry, 9); // 0 is unlimited
    b.writeBigUInt64LE(maxTopicSize, 17); // optional, 0 is null
    b.writeUInt8(replicationFactor, 25); // must be > 0
    b.writeUInt8(bName.length, 26);
  
    return Buffer.concat([
      streamIdentifier,
      b,
      bName,
    ]);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeTopic(r.data).data;
  }
};

export const createTopic = wrapCommand<CreateTopic, Topic>(CREATE_TOPIC);
