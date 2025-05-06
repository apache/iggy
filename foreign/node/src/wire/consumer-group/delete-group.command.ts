
import { type Id } from '../identifier.utils.js';
import { serializeTargetGroup } from './group.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';

export type DeleteGroup = {
  streamId: Id,
  topicId: Id,
  groupId: Id
};

export const DELETE_GROUP = {
  code: 603,

  serialize: ({streamId, topicId, groupId}: DeleteGroup) => {
    return serializeTargetGroup(streamId, topicId, groupId);
  },

  deserialize: deserializeVoidResponse
};

export const deleteGroup = wrapCommand<DeleteGroup, boolean>(DELETE_GROUP);
