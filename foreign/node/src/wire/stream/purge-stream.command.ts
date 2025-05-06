
import { wrapCommand } from '../command.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';

export type PurgeStream = {
  streamId: Id
};

export const PURGE_STREAM = {
  code: 205,

  serialize: ({streamId}: PurgeStream) => {
    return serializeIdentifier(streamId);
  },

  deserialize: deserializeVoidResponse
};

export const purgeStream = wrapCommand<PurgeStream, boolean>(PURGE_STREAM);
