
import type { CommandResponse } from '../../client/client.type.js';
import type { ConsumerGroup } from './group.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializeConsumerGroups } from './group.utils.js';
import { wrapCommand } from '../command.utils.js';

export type GetGroups = {
  streamId: Id,
  topicId: Id
};

export const GET_GROUPS = {
  code: 601,

  serialize: ({ streamId, topicId }: GetGroups) => {
    return Buffer.concat([
      serializeIdentifier(streamId),
      serializeIdentifier(topicId),
    ]);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeConsumerGroups(r.data);
  }
};

export const getGroups = wrapCommand<GetGroups, ConsumerGroup[]>(GET_GROUPS);
