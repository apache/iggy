
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';

export type DeleteUser = {
  userId: Id
}

export const DELETE_USER = {
  code: 34,

  serialize: ({userId}: DeleteUser) => {
    return serializeIdentifier(userId);
  },

  deserialize: deserializeVoidResponse
};

export const deleteUser = wrapCommand<DeleteUser, boolean>(DELETE_USER);
