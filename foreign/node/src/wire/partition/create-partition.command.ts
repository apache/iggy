
import { wrapCommand } from '../command.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import type { Id } from '../identifier.utils.js';
import { serializePartitionParams } from './partition.utils.js';

export type CreatePartition = {
  streamId: Id,
  topicId: Id,
  partitionCount?: number
};

export const CREATE_PARTITION = {
  code: 402,
  serialize: ({ streamId, topicId, partitionCount = 1 }: CreatePartition) => {
    return serializePartitionParams(streamId, topicId, partitionCount);
  },
  deserialize: deserializeVoidResponse
};

export const createPartition = wrapCommand<CreatePartition, boolean>(CREATE_PARTITION);
