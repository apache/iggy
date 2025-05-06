
import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { deserializeToStream, type Stream } from './stream.utils.js';

export type CreateStream = {
  streamId: number,
  name: string
};

export const CREATE_STREAM = {
  code: 202,

  serialize: ({streamId, name}: CreateStream) => {
    const bName = Buffer.from(name);

    if (bName.length < 1 || bName.length > 255)
      throw new Error('Stream name should be between 1 and 255 bytes');

    const b = Buffer.allocUnsafe(4 + 1);
    b.writeUInt32LE(streamId, 0);
    b.writeUInt8(bName.length, 4);

    return Buffer.concat([
      b,
      bName
    ]);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeToStream(r.data, 0).data
  }
};

export const createStream = wrapCommand<CreateStream, Stream>(CREATE_STREAM);
