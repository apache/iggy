
import { toDate } from '../serialize.utils.js';

export type Stream = {
  id: number,
  name: string,
  topicsCount: number,
  sizeBytes: bigint,
  messagesCount: bigint,
  createdAt: Date
}

type StreamDeserialized = {
  bytesRead: number,
  data: Stream
};

export const deserializeToStream = (r: Buffer, pos = 0): StreamDeserialized => {
  const id = r.readUInt32LE(pos);
  const createdAt = toDate(r.readBigUint64LE(pos + 4));
  const topicsCount = r.readUInt32LE(pos + 12);
  const sizeBytes = r.readBigUint64LE(pos + 16);
  const messagesCount = r.readBigUint64LE(pos + 24);
  const nameLength = r.readUInt8(pos + 32);
  const name = r.subarray(pos + 33, pos + 33 + nameLength).toString();

  return {
    bytesRead: 33 + nameLength,
    data: {
      id, name, topicsCount, messagesCount, sizeBytes, createdAt
    }
  };
};
