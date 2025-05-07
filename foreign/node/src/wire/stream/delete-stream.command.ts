
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';

export type DeleteStream = {
  streamId: Id
};

export const DELETE_STREAM = {
  code: 203,

  serialize: ({streamId}: DeleteStream) => {
    return serializeIdentifier(streamId);
  },

  deserialize: deserializeVoidResponse
};

export const deleteStream = wrapCommand<DeleteStream, boolean>(DELETE_STREAM);
