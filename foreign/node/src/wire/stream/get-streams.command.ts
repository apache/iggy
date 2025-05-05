
import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { deserializeToStream, type Stream } from './stream.utils.js';

export const GET_STREAMS = {
  code: 201,
  serialize: () => Buffer.alloc(0),
  deserialize: (r: CommandResponse) => {
    const payloadSize = r.data.length;
    const streams = [];
    let pos = 0;
    while (pos < payloadSize) {
      const { bytesRead, data } = deserializeToStream(r.data, pos)
      streams.push(data);
      pos += bytesRead;
    }
    return streams;
  }
};

export const getStreams = wrapCommand<void, Stream[]>(GET_STREAMS);
