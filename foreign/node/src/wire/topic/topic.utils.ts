// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

import { toDate } from '../serialize.utils.js';
import type { ValueOf } from '../../type.utils.js';
import { deserializePrefixedOptions, type ParsedOptions } from '../options.utils.js';

/**
 * Basic topic information without partition details.
 */
export type BaseTopic = {
  /** Topic ID */
  id: number
  /** Topic name */
  name: string,
  /** Topic creation timestamp */
  createdAt: Date,
  /** Number of partitions */
  partitionsCount: number,
  /** Compression algorithm used */
  compressionAlgorithm: number,
  /** Message expiry time in microseconds (0 = unlimited) */
  messageExpiry: bigint,
  /** Maximum topic size in bytes (0 = unlimited) */
  maxTopicSize: bigint,
  /** Replication factor (no longer on the wire, always 1) */
  replicationFactor: number
  /** Total size of the topic in bytes */
  sizeBytes: bigint,
  /** Total number of messages in the topic */
  messagesCount: bigint,
  /** Options the client explicitly sent at create */
  options: ParsedOptions,
  /** Options resolved from server defaults at create */
  derivedOptions: ParsedOptions,
};

/**
 * Partition information within a topic.
 */
export type Partition = {
  /** Partition ID */
  id: number,
  /** Partition creation timestamp */
  createdAt: Date,
  /** Number of segments in the partition */
  segmentsCount: number,
  /** Current offset in the partition */
  currentOffset: bigint,
  /** Total size of the partition in bytes */
  sizeBytes: bigint,
  /** Total number of messages in the partition */
  messagesCount: bigint
};

/** Topic with partition details */
export type Topic = BaseTopic & { partitions: Partition[] };

/** Base serialization result */
type Serialized = { bytesRead: number };

/** Result of deserializing a partition */
type PartitionSerialized = { data: Partition } & Serialized;

/** Result of deserializing a base topic */
type BaseTopicSerialized = { data: BaseTopic } & Serialized;

/** Result of deserializing a topic */
type TopicSerialized = { data: Topic } & Serialized;

/**
 * Compression algorithm options.
 */
export const CompressionAlgorithm = {
  /** No compression */
  None: 1,
  /** Gzip compression */
  Gzip: 2
};

/** Type alias for the CompressionAlgorithm object */
export type CompressionAlgorithmKind = typeof CompressionAlgorithm;
/** String literal type of compression algorithm names */
export type CompressionAlgorithmKindId = keyof CompressionAlgorithm;
/** Numeric values of compression algorithms */
export type CompressionAlgorithmKindValue = ValueOf<CompressionAlgorithm>;

/** No compression type */
export type CompressionAlgorithmNone = CompressionAlgorithmKind['None'];
/** Gzip compression type */
export type CompressionAlgorithmGzip = CompressionAlgorithmKind['Gzip'];
/** Union of compression algorithm types */
export type CompressionAlgorithm = CompressionAlgorithmNone | CompressionAlgorithmGzip;


/**
 * Type guard for valid compression algorithms.
 *
 * @param ca - Compression algorithm value to check
 * @returns True if the value is a valid compression algorithm
 */
export const isValidCompressionAlgorithm = (ca: number): ca is CompressionAlgorithm =>
  Object.values(CompressionAlgorithm).includes(ca);

/**
 * Maps a compression algorithm code to its wire option name.
 *
 * @param ca - Compression algorithm code
 * @returns Option value string ('none' or 'gzip')
 */
export const compressionAlgorithmName = (ca: CompressionAlgorithm): string =>
  ca === CompressionAlgorithm.Gzip ? 'gzip' : 'none';

/**
 * Deserializes a topic header from a buffer.
 * Layout: 50 fixed bytes, name, then two length-prefixed options blocks
 * (client-explicit and server-derived).
 *
 * @param p - Buffer containing serialized topic data
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read and deserialized topic data
 */
export const deserializeBaseTopic = (p: Buffer, pos = 0): BaseTopicSerialized => {
  const start = pos;
  const id = p.readUInt32LE(pos);
  const createdAt = toDate(p.readBigUint64LE(pos + 4));
  const partitionsCount = p.readUInt32LE(pos + 12);
  const messageExpiry = p.readBigUInt64LE(pos + 16);
  const compressionAlgorithm = p.readUInt8(pos + 24);
  const maxTopicSize = p.readBigUInt64LE(pos + 25);
  const sizeBytes = p.readBigUInt64LE(pos + 33);
  const messagesCount = p.readBigUInt64LE(pos + 41);

  const nameLength = p.readUInt8(pos + 49);
  const name = p.subarray(pos + 50, pos + 50 + nameLength).toString();
  pos += 50 + nameLength;

  const explicit = deserializePrefixedOptions(p, pos);
  pos += explicit.bytesRead;
  const derived = deserializePrefixedOptions(p, pos);
  pos += derived.bytesRead;

  return {
    bytesRead: pos - start,
    data: {
      id,
      name,
      createdAt,
      partitionsCount,
      compressionAlgorithm,
      maxTopicSize,
      replicationFactor: 1,
      messageExpiry,
      messagesCount,
      sizeBytes,
      options: explicit.options,
      derivedOptions: derived.options,
    }
  }
};


/**
 * Deserializes a partition from a buffer.
 *
 * @param p - Buffer containing serialized partition data
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read and deserialized partition data
 */
export const deserializePartition = (p: Buffer, pos = 0): PartitionSerialized => {
  return {
    bytesRead: 4 + 8 + 4 + 8 + 8 + 8,
    data: {
      id: p.readUInt32LE(pos),
      createdAt: toDate(p.readBigUint64LE(pos + 4)),
      segmentsCount: p.readUInt32LE(pos + 12),
      currentOffset: p.readBigUint64LE(pos + 16),
      sizeBytes: p.readBigUint64LE(pos + 24),
      messagesCount: p.readBigUint64LE(pos + 32),
    }
  }
};


/**
 * Deserializes a topic with partitions from a buffer.
 * Partition elements are count-driven from the topic's partitionsCount:
 * the header's variable-length options blocks make greedy consumption
 * ambiguous.
 *
 * @param p - Buffer containing serialized topic data
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read and deserialized topic with partitions
 * @throws Error if the buffer is empty (topic does not exist)
 */
export const deserializeTopic = (p: Buffer, pos = 0): TopicSerialized => {
  if (p.length === 0)
    throw new Error('Topic does not exist');

  const start = pos;
  const { bytesRead, data } = deserializeBaseTopic(p, pos);
  pos += bytesRead;
  const partitions = [];
  for (let i = 0; i < data.partitionsCount; i++) {
    const { bytesRead, data } = deserializePartition(p, pos);
    partitions.push(data);
    pos += bytesRead;
  }
  return { bytesRead: pos - start, data: { ...data, partitions } };
};


/**
 * Deserializes a GetTopics response: `[topics_count:u32_le]` prefix followed
 * by count-driven topic headers (no partition details).
 *
 * @param p - Buffer containing serialized topics data
 * @param pos - Starting position in the buffer
 * @returns Array of deserialized topics
 */
export const deserializeTopics = (p: Buffer, pos = 0): Topic[] => {
  const topicsCount = p.readUInt32LE(pos);
  pos += 4;
  const topics = [];
  for (let i = 0; i < topicsCount; i++) {
    const { bytesRead, data } = deserializeBaseTopic(p, pos);
    topics.push({ ...data, partitions: [] });
    pos += bytesRead;
  }
  return topics;
};
