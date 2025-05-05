
import type { CommandResponse } from '../../client/client.type.js';
import { type Id } from '../identifier.utils.js';
import { wrapCommand } from '../command.utils.js';
import {
  serializeTargetGroup, deserializeConsumerGroup,
  type ConsumerGroup
} from './group.utils.js';

export type GetGroup = {
  streamId: Id,
  topicId: Id,
  groupId: Id
};

export const GET_GROUP = {
  code: 600,

  serialize: ({streamId, topicId, groupId}: GetGroup) => {
    return serializeTargetGroup(streamId, topicId, groupId);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeConsumerGroup(r.data).data;
  }
};

export const getGroup = wrapCommand<GetGroup, ConsumerGroup>(GET_GROUP);
