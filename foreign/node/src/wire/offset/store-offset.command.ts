
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { type Id } from '../identifier.utils.js';
import { serializeStoreOffset, type Consumer } from './offset.utils.js';

export type StoreOffset = {
  streamId: Id,
  topicId: Id,
  consumer: Consumer,
  partitionId: number,
  offset: bigint
};

export const STORE_OFFSET = {
  code: 121,

  serialize: ({streamId, topicId, consumer, partitionId, offset}: StoreOffset) =>
    serializeStoreOffset(streamId, topicId, consumer, partitionId, offset),

  deserialize: deserializeVoidResponse
};

export const storeOffset = wrapCommand<StoreOffset, boolean>(STORE_OFFSET);
