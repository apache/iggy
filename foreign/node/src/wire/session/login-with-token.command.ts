
import type { CommandResponse } from '../../client/client.type.js';
import type { LoginResponse } from './login.type.js';
import { wrapCommand } from '../command.utils.js';

export type LoginWithTokenParam = {
  token: string
};


export const LOGIN_WITH_TOKEN = {
  code: 44,

  serialize: ({token}: LoginWithTokenParam) => {
    const bToken = Buffer.from(token);
    if (bToken.length < 1 || bToken.length > 255)
      throw new Error('Token length should be between 1 and 255 bytes');
    const b = Buffer.alloc(1);
    b.writeUInt8(bToken.length);
    return Buffer.concat([
      b,
      bToken
    ]);
  },

  deserialize: (r: CommandResponse) => ({
    userId: r.data.readUInt32LE(0)
  })
};

export const loginWithToken =
  wrapCommand<LoginWithTokenParam, LoginResponse>(LOGIN_WITH_TOKEN);
