
import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializeTopic, type Topic } from './topic.utils.js';

type GetTopic = {
  streamId: Id,
  topicId: Id
}

export const GET_TOPIC = {
  code: 300,

  serialize: ({streamId, topicId}: GetTopic) => {
    return Buffer.concat([
      serializeIdentifier(streamId),
      serializeIdentifier(topicId)
    ]);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeTopic(r.data).data;
  }
};

export const getTopic = wrapCommand<GetTopic, Topic>(GET_TOPIC);
