
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';

export type DeleteToken = {
  name: string
};

export const DELETE_TOKEN = {
  code: 43,

  serialize: ({name}: DeleteToken): Buffer => {
    const bName = Buffer.from(name);
    if (bName.length < 1 || bName.length > 255)
      throw new Error('Token name should be between 1 and 255 bytes');
    const b = Buffer.alloc(1);
    b.writeUInt8(bName.length);
    return Buffer.concat([
      b,
      bName
    ]);
  },

  deserialize: deserializeVoidResponse
};

export const deleteToken = wrapCommand<DeleteToken, boolean>(DELETE_TOKEN);
