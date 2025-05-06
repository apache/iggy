
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { uint32ToBuf } from '../number.utils.js';

type DeleteTopic = {
  streamId: Id,
  topicId: Id,
  partitionsCount: number
}

export const DELETE_TOPIC = {
  code: 303,

  serialize: ({streamId, topicId, partitionsCount}: DeleteTopic) => {
    return Buffer.concat([
      serializeIdentifier(streamId),
      serializeIdentifier(topicId),
      uint32ToBuf(partitionsCount)
    ]);
  },

  deserialize: deserializeVoidResponse
};

export const deleteTopic = wrapCommand<DeleteTopic, boolean>(DELETE_TOPIC);
