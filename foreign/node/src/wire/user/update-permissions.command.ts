
import { wrapCommand } from '../command.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { uint32ToBuf, boolToBuf } from '../number.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { serializePermissions, type UserPermissions } from './permissions.utils.js';

export type UpdatePermissions = {
  userId: Id,
  permissions: UserPermissions
};

export const UPDATE_PERMISSIONS = {
  code: 36,

  serialize: ({ userId, permissions}: UpdatePermissions  ) => {

    const bPermissions = serializePermissions(permissions);

    return Buffer.concat([
      serializeIdentifier(userId),
      boolToBuf(!!permissions),
      uint32ToBuf(bPermissions.length),
      bPermissions
    ]);
  },

  deserialize: deserializeVoidResponse
};

export const updatePermissions = wrapCommand<UpdatePermissions, boolean>(UPDATE_PERMISSIONS);
