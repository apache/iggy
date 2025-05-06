
import { type Id } from '../identifier.utils.js';
import { serializeTargetGroup } from './group.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';

export type LeaveGroup = {
  streamId: Id,
  topicId: Id,
  groupId: Id
};

export const LEAVE_GROUP = {
  code: 605,

  serialize: ({streamId, topicId, groupId}: LeaveGroup) => {
    return serializeTargetGroup(streamId, topicId, groupId);
  },

  deserialize: deserializeVoidResponse
};

export const leaveGroup = wrapCommand<LeaveGroup, boolean>(LEAVE_GROUP);
