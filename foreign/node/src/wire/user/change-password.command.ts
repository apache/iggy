
import { uint8ToBuf } from '../number.utils.js';
import { serializeIdentifier } from '../identifier.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';


export type ChangePassword = {
  userId: number,
  currentPassword: string,
  newPassword: string
};


export const CHANGE_PASSWORD = {
  code: 37,

  serialize: ({ userId, currentPassword, newPassword }: ChangePassword) => {

    const bId = serializeIdentifier(userId);
    const bCur = Buffer.from(currentPassword);
    const bNew = Buffer.from(newPassword);

    if (bCur.length < 1 || bCur.length > 255)
      throw new Error('User password should be between 1 and 255 bytes (current)');

    if (bNew.length < 1 || bNew.length > 255)
      throw new Error('User password should be between 1 and 255 bytes (new)');

    return Buffer.concat([
      bId,
      uint8ToBuf(bCur.length),
      bCur,
      uint8ToBuf(bNew.length),
      bNew
    ]);
  },

  deserialize: deserializeVoidResponse
};

export const changePassword = wrapCommand<ChangePassword, boolean>(CHANGE_PASSWORD);
