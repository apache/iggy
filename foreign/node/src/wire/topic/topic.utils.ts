
import { toDate } from '../serialize.utils.js';
import type { ValueOf } from '../../type.utils.js';

export type BaseTopic = {
  id: number
  name: string,
  createdAt: Date,
  partitionsCount: number,
  compressionAlgorithm: number,
  messageExpiry: bigint,
  maxTopicSize: bigint,
  replicationFactor: number
  sizeBytes: bigint,
  messagesCount: bigint,
};

export type Partition = {
  id: number,
  createdAt: Date,
  segmentsCount: number,
  currentOffset: bigint,
  sizeBytes: bigint,
  messagesCount: bigint
};

export type Topic = BaseTopic & { partitions: Partition[] };

type Serialized = { bytesRead: number };

type PartitionSerialized = { data: Partition } & Serialized;

type BaseTopicSerialized = { data: BaseTopic } & Serialized;

type TopicSerialized = { data: Topic } & Serialized;


export const CompressionAlgorithmKind = {
  None: 1,
  Gzip: 2
};

export type CompressionAlgorithmKind = typeof CompressionAlgorithmKind;
export type CompressionAlgorithmKindId = keyof CompressionAlgorithmKind;
export type CompressionAlgorithmKindValue = ValueOf<CompressionAlgorithmKind>;

export type CompressionAlgorithmNone = CompressionAlgorithmKind['None'];
export type CompressionAlgorithmGzip = CompressionAlgorithmKind['Gzip'];
export type CompressionAlgorithm = CompressionAlgorithmNone | CompressionAlgorithmGzip;

export const isValidCompressionAlgorithm = (ca: number): ca is CompressionAlgorithm =>
  Object.values(CompressionAlgorithmKind).includes(ca);


export const deserializeBaseTopic = (p: Buffer, pos = 0): BaseTopicSerialized => {
  const id = p.readUInt32LE(pos);
  const createdAt = toDate(p.readBigUint64LE(pos + 4));
  const partitionsCount = p.readUInt32LE(pos + 12);
  const compressionAlgorithm = p.readUInt8(pos + 16);
  const messageExpiry = p.readBigUInt64LE(pos + 17);
  const maxTopicSize = p.readBigUInt64LE(pos + 25);
  const replicationFactor = p.readUInt8(pos + 33);
  const sizeBytes = p.readBigUInt64LE(pos + 34);
  const messagesCount = p.readBigUInt64LE(pos + 42);

  const nameLength = p.readUInt8(pos + 50);
  const name = p.subarray(pos + 51, pos + 51 + nameLength).toString();

  return {
    bytesRead: 4 + 8 + 4 + 1 + 8 + 8 + 1 + 8 + 8 + 1 + nameLength,
    data: {
      id,
      name,
      createdAt,
      partitionsCount,
      compressionAlgorithm,
      maxTopicSize,
      replicationFactor,
      messageExpiry,
      messagesCount,
      sizeBytes,
    }
  }
};


export const deserializePartition = (p: Buffer, pos = 0): PartitionSerialized => {
  return {
    bytesRead: 4 + 8 + 4 + 8 + 8 + 8,
    data: {
      id: p.readUInt32LE(pos),
      createdAt: toDate(p.readBigUint64LE(pos + 4)),
      segmentsCount: p.readUInt32LE(pos + 12),
      currentOffset: p.readBigUint64LE(pos + 16),
      sizeBytes: p.readBigUint64LE(pos + 24),
      messagesCount: p.readBigUint64LE(pos + 24),
    }
  }
};


export const deserializeTopic = (p: Buffer, pos = 0): TopicSerialized => {
  const start = pos;
  const { bytesRead, data } = deserializeBaseTopic(p, pos);
  pos += bytesRead;
  const partitions = [];
  const end = p.length;
  while (pos < end) {
    const { bytesRead, data } = deserializePartition(p, pos);
    partitions.push(data);
    pos += bytesRead;
  }
  return { bytesRead: pos - start, data: { ...data, partitions } };
};


export const deserializeTopics = (p: Buffer, pos = 0): Topic[] => {
  const topics = [];
  const len = p.length;
  while (pos < len) {
    const { bytesRead, data } = deserializeTopic(p, pos);
    topics.push(data);
    pos += bytesRead;
  }
  return topics;
};
