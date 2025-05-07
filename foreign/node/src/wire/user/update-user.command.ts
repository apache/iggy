
import { wrapCommand } from '../command.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import type { UserStatus } from './user.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { uint8ToBuf } from '../number.utils.js';

export type UpdateUser = {
  userId: Id,
  username?: string,
  status?: UserStatus
};


export const UPDATE_USER = {
  code: 35,

  serialize: ({userId, username, status}: UpdateUser) => {

    const bId = serializeIdentifier(userId);
    let bUsername, bStatus;

    if (username) {
      const bn = Buffer.from(username);

      if (bn.length < 1 || bn.length > 255)
        throw new Error('User username should be between 1 and 255 bytes');

      bUsername = Buffer.concat([
        uint8ToBuf(1),
        uint8ToBuf(bn.length),
        bn
      ]);
    } else
      bUsername = uint8ToBuf(0);

    if (status)
      bStatus = Buffer.concat([uint8ToBuf(1), uint8ToBuf(status)]);
    else
      bStatus = uint8ToBuf(0);


    return Buffer.concat([
      bId,
      bUsername,
      bStatus,
    ]);
  },

  deserialize: deserializeVoidResponse
};

export const updateUser = wrapCommand<UpdateUser, boolean>(UPDATE_USER);
