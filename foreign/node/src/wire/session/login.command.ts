
import type { CommandResponse } from '../../client/client.type.js';
import type { LoginResponse } from './login.type.js';
import { wrapCommand } from '../command.utils.js';


export type LoginCredentials = {
  username: string,
  password: string,
  version?: string,
  context?: string
}

// LOGIN
export const LOGIN = {
  code: 38,

  serialize: ({
    username,
    password,
    version,
    context
  }: LoginCredentials) => {
    const bUsername = Buffer.from(username);
    const bPassword = Buffer.from(password);

    if (bUsername.length < 1 || bUsername.length > 255)
      throw new Error('Username should be between 1 and 255 bytes');
    if (bPassword.length < 1 || bPassword.length > 255)
      throw new Error('Password should be between 1 and 255 bytes');

    const l1 = Buffer.allocUnsafe(1);
    const l2 = Buffer.allocUnsafe(1);
    l1.writeUInt8(bUsername.length);
    l2.writeUInt8(bPassword.length);

    const binVersion: Buffer[] = [];
    const l3 = Buffer.allocUnsafe(4);
    
    if(version && version.length > 0) {
      const bVersion = Buffer.from(version);
      l3.writeUInt32LE(bVersion.length);
      binVersion.push(l3, bVersion);
    } else {
      l3.writeUInt32LE(0);
      binVersion.push(l3);
    }

    const binContext: Buffer[] = [];
    const l4 = Buffer.allocUnsafe(4);
    
    if(context && context.length > 0) {
      const bContext = Buffer.from(context);
      l4.writeUInt32LE(bContext.length);
      binContext.push(l4, bContext);
    } else {
      l4.writeUInt32LE(0);
      binContext.push(l4);
    }
    
    return Buffer.concat([
      l1,
      bUsername,
      l2,
      bPassword,
      ...binVersion,
      ...binContext
    ])
  },

  deserialize: (r: CommandResponse) => ({
    userId: r.data.readUInt32LE(0)
  })

};

export const login = wrapCommand<LoginCredentials, LoginResponse>(LOGIN);
