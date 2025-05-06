
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import {
  type CompressionAlgorithm, CompressionAlgorithmKind, isValidCompressionAlgorithm
} from './topic.utils.js';


export type UpdateTopic = {
  streamId: Id,
  topicId: Id,
  name: string,
  compressionAlgorithm?: CompressionAlgorithm,
  messageExpiry?: bigint,
  maxTopicSize?: bigint,
  replicationFactor?: number,
};

export const UPDATE_TOPIC = {
  code: 304,

  serialize: ({
    streamId,
    topicId,
    name,
    compressionAlgorithm = CompressionAlgorithmKind.None,
    messageExpiry = 0n,
    maxTopicSize = 0n,
    replicationFactor = 1,
  }: UpdateTopic) => {
    const streamIdentifier = serializeIdentifier(streamId);
    const topicIdentifier = serializeIdentifier(topicId);
    const bName = Buffer.from(name)

    if (bName.length < 1 || bName.length > 255)
      throw new Error('Topic name should be between 1 and 255 bytes');
    if(!isValidCompressionAlgorithm(compressionAlgorithm))
      throw new Error(`createTopic: invalid compressionAlgorithm (${compressionAlgorithm})`);

    const b = Buffer.allocUnsafe(8 + 8 + 1 + 1 + 1);
    b.writeUInt8(compressionAlgorithm, 0);
    b.writeBigUInt64LE(messageExpiry, 1); // 0 is unlimited ???
    b.writeBigUInt64LE(maxTopicSize, 9); // optional, 0 is null
    b.writeUInt8(replicationFactor, 17); // must be > 0
    b.writeUInt8(bName.length, 18);

    return Buffer.concat([
      streamIdentifier,
      topicIdentifier,
      b,
      bName,
    ]);
  },

  deserialize: deserializeVoidResponse
};

export const updateTopic = wrapCommand<UpdateTopic, boolean>(UPDATE_TOPIC);
