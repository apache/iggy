
import type { CommandResponse } from '../../client/client.type.js';
import { deserializeClient, type Client } from './client.utils.js';
import { wrapCommand } from '../command.utils.js';

export type GetClient = {
  clientId: number
};

// GET CLIENT by id
export const GET_CLIENT = {
  code: 21,
  serialize: ({ clientId }: GetClient): Buffer => {
    const b = Buffer.alloc(4);
    b.writeUInt32LE(clientId);
    return b;
  },
  deserialize: (r: CommandResponse) => deserializeClient(r.data).data
};

export const getClient = wrapCommand<GetClient, Client>(GET_CLIENT);
