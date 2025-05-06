
import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializeToStream, type Stream } from './stream.utils.js';

export type GetStream = {
  streamId: Id
};

export const GET_STREAM = {
  code: 200,

  serialize: ({ streamId }: GetStream) => {
    return serializeIdentifier(streamId);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeToStream(r.data, 0).data
  }
}

export const getStream = wrapCommand<GetStream, Stream>(GET_STREAM);
