
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';

// LOGOUT
export const LOGOUT = {
  code: 39,
  serialize: () => {
    return Buffer.alloc(0);
  },

  deserialize: deserializeVoidResponse
};

export const logout = wrapCommand<void, boolean>(LOGOUT);
