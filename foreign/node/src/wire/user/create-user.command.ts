
import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { deserializeUser, type User, type UserStatus } from './user.utils.js';
import { uint8ToBuf, uint32ToBuf, boolToBuf } from '../number.utils.js';
import { serializePermissions, type UserPermissions } from './permissions.utils.js';

export type CreateUser = {
  username: string,
  password: string,
  status: UserStatus
  permissions?: UserPermissions
};

export const CREATE_USER = {
  code: 33,

  serialize: ({ username, password, status, permissions }: CreateUser) => {
    const bUsername = Buffer.from(username);
    const bPassword = Buffer.from(password);

    if (bUsername.length < 1 || bUsername.length > 255)
      throw new Error('User username should be between 1 and 255 bytes');

    if (bPassword.length < 1 || bPassword.length > 255)
      throw new Error('User password should be between 1 and 255 bytes');

    const bPermissions = serializePermissions(permissions);

    return Buffer.concat([
      uint8ToBuf(bUsername.length),
      bUsername,
      uint8ToBuf(bPassword.length),
      bPassword,
      uint8ToBuf(status),
      boolToBuf(!!permissions),
      uint32ToBuf(bPermissions.length),
      bPermissions
    ]);
  },

  deserialize: (r: CommandResponse) => deserializeUser(r.data)
};

export const createUser = wrapCommand<CreateUser, User>(CREATE_USER);
