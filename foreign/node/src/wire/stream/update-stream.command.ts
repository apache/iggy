import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { uint8ToBuf } from '../number.utils.js';

export type UpdateStream = {
  streamId: Id,
  name: string
}

export const UPDATE_STREAM = {
  code: 204,

  serialize: ({streamId, name}: UpdateStream) => {
    const bId = serializeIdentifier(streamId);
    const bName = Buffer.from(name);

    if (bName.length < 1 || bName.length > 255)
      throw new Error('Stream name should be between 1 and 255 bytes');

    return Buffer.concat([
      bId,
      uint8ToBuf(bName.length),
      bName
    ]);
  },

  deserialize: deserializeVoidResponse
};

export const updateStream = wrapCommand<UpdateStream, boolean>(UPDATE_STREAM);
