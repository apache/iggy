
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';

export type PurgeTopic = {
  streamId: Id,
  topicId: Id
};

export const PURGE_TOPIC = {
  code: 305,

  serialize: ({ streamId, topicId }: PurgeTopic) => {
    return Buffer.concat([
      serializeIdentifier(streamId),
      serializeIdentifier(topicId),
    ]);
  },

  deserialize: deserializeVoidResponse
};

export const purgeTopic = wrapCommand<PurgeTopic, boolean>(PURGE_TOPIC);
