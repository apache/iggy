
import { type Id } from '../identifier.utils.js';
import { serializeTargetGroup } from './group.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';

export type JoinGroup = {
  streamId: Id,
  topicId: Id,
  groupId: Id
};

export const JOIN_GROUP = {
  code: 604,

  serialize: ({ streamId, topicId, groupId }: JoinGroup) => {
    return serializeTargetGroup(streamId, topicId, groupId);
  },

  deserialize: deserializeVoidResponse
};

export const joinGroup = wrapCommand<JoinGroup, boolean>(JOIN_GROUP);
