
import type { CommandResponse } from '../../client/client.type.js';
import { deserializeClient, type Client } from './client.utils.js';
import { wrapCommand } from '../command.utils.js';

// GET ME
export const GET_ME = {
  code: 20,

  serialize: () => Buffer.alloc(0),

  deserialize: (r: CommandResponse) => deserializeClient(r.data).data
};


export const getMe = wrapCommand<void, Client>(GET_ME);
