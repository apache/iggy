
import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializeUser, type User } from './user.utils.js';


export type GetUser = {
  userId: Id
};


// GET USER by id
export const GET_USER = {
  code: 31,
  serialize: ({userId}: GetUser) => {
    return serializeIdentifier(userId);
  },
  deserialize: (r: CommandResponse) => deserializeUser(r.data)

};

export const getUser = wrapCommand<GetUser, User>(GET_USER);
