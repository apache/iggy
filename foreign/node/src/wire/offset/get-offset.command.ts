
import type { CommandResponse } from '../../client/client.type.js';
import type { Id } from '../identifier.utils.js';
import { wrapCommand } from '../command.utils.js';
import { serializeGetOffset, type Consumer, type OffsetResponse } from './offset.utils.js';

export type GetOffset = {
  streamId: Id,
  topicId: Id,
  consumer: Consumer,
  partitionId?: number
};


export const GET_OFFSET = {
  code: 120,

  serialize: ({streamId, topicId, consumer, partitionId = 1}: GetOffset) => {
    return serializeGetOffset(streamId, topicId, consumer, partitionId);
  },

  deserialize: (r: CommandResponse): OffsetResponse => {
    const partitionId = r.data.readUInt32LE(0);
    const currentOffset = r.data.readBigUInt64LE(4);
    const storedOffset = r.data.readBigUInt64LE(12);

    return {
      partitionId,
      currentOffset,
      storedOffset
    }
  }
};

export const getOffset = wrapCommand<GetOffset, OffsetResponse>(GET_OFFSET);
