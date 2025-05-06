
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import type { Id } from '../identifier.utils.js';
import { serializePartitionParams } from './partition.utils.js';

export type DeletePartition = {
  streamId: Id,
  topicId: Id,
  partitionCount: number
};
    
export const DELETE_PARTITION = {
  code: 403,

  serialize: ({ streamId, topicId, partitionCount }: DeletePartition) => {
    return serializePartitionParams(streamId, topicId, partitionCount);
  },
  
  deserialize: deserializeVoidResponse
};

export const deletePartition = wrapCommand<DeletePartition, boolean>(DELETE_PARTITION);
