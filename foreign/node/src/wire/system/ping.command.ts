
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';

// PING
export const PING = {
  code: 1,
  serialize: () => {
    return Buffer.alloc(0);
  },

  deserialize: deserializeVoidResponse

};

export const ping = wrapCommand<void, boolean>(PING);
