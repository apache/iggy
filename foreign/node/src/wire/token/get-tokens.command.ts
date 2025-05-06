
import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { deserializeTokens, type Token } from './token.utils.js';


export const GET_TOKENS = {
  code: 41,

  serialize: (): Buffer => Buffer.alloc(0),

  deserialize: (r: CommandResponse) => deserializeTokens(r.data)
};

export const getTokens = wrapCommand<void, Token[]>(GET_TOKENS);
