
import type { CommandResponse } from '../../client/client.type.js';
import { deserializeCreateToken, type CreateTokenResponse } from './token.utils.js';
import { wrapCommand } from '../command.utils.js';

export type CreateToken = {
  name: string,
  expiry?: bigint
}


export const CREATE_TOKEN = {
  code: 42,

  serialize: ({name, expiry = 600n}: CreateToken): Buffer => {
    const bName = Buffer.from(name);
    if (bName.length < 1 || bName.length > 255)
      throw new Error('Token name should be between 1 and 255 bytes');
    const b1 = Buffer.alloc(1);
    b1.writeUInt8(bName.length);
    const b2 = Buffer.alloc(8);
    b2.writeBigUInt64LE(expiry);
    return Buffer.concat([
      b1,
      bName,
      b2
    ]);
  },

  deserialize: (r: CommandResponse) => deserializeCreateToken(r.data).data
};

export const createToken = wrapCommand<CreateToken, CreateTokenResponse>(CREATE_TOKEN);
