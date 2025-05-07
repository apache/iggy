
import type { CommandResponse } from '../../client/client.type.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { wrapCommand } from '../command.utils.js';
import { deserializeConsumerGroup, type ConsumerGroup } from './group.utils.js';

export type CreateGroup = {
  streamId: Id,
  topicId: Id,
  groupId: number,
  name: string,
};

export const CREATE_GROUP = {
  code: 602,

  serialize: ({ streamId, topicId, groupId, name }: CreateGroup) => {
    const bName = Buffer.from(name);

    if (bName.length < 1 || bName.length > 255)
      throw new Error('Consumer group name should be between 1 and 255 bytes');

    const b = Buffer.allocUnsafe(5);
    b.writeUInt32LE(groupId);
    b.writeUInt8(bName.length, 4);

    return Buffer.concat([
      serializeIdentifier(streamId),
      serializeIdentifier(topicId),
      b,
      bName
    ]);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeConsumerGroup(r.data).data;
  }
};

export const createGroup = wrapCommand<CreateGroup, ConsumerGroup>(CREATE_GROUP);
