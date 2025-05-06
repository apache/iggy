
import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { deserializeUsers, type BaseUser } from './user.utils.js';


// GET USERS
export const GET_USERS = {
  code: 32,

  serialize: () =>  Buffer.alloc(0),
  
  deserialize: (r: CommandResponse) => deserializeUsers(r.data)
};

export const getUsers = wrapCommand<void, BaseUser[]>(GET_USERS);
