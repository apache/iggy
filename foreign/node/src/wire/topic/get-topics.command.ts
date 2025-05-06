
import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializeTopics, type Topic } from './topic.utils.js';

export type GetTopics = {
  streamId: Id
};

export const GET_TOPICS = {
  code: 301,

  serialize: ({streamId}: GetTopics) => {
    return serializeIdentifier(streamId);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeTopics(r.data);
  }
};

export const getTopics = wrapCommand<GetTopics, Topic[]>(GET_TOPICS);
