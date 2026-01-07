/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import { serializeIdentifier, type Id } from '../identifier.utils.js';

/**
 * Consumer group information.
 */
export type ConsumerGroup = {
  /** Consumer group ID */
  id: number,
  /** Consumer group name */
  name: string,
  /** Number of members in the group */
  membersCount: number,
  /** Number of partitions assigned to the group */
  partitionsCount: number,
};

/**
 * Result of deserializing a consumer group.
 */
type ConsumerGroupDeserialized = {
  /** Number of bytes consumed */
  bytesRead: number,
  /** Deserialized consumer group data */
  data: ConsumerGroup
};

/**
 * Serializes stream, topic, and group identifiers for targeting a consumer group.
 *
 * @param streamId - Stream identifier (ID or name)
 * @param topicId - Topic identifier (ID or name)
 * @param groupId - Consumer group identifier (ID or name)
 * @returns Buffer containing serialized identifiers
 */
export const serializeTargetGroup = (streamId: Id, topicId: Id, groupId: Id) => {
  return Buffer.concat([
    serializeIdentifier(streamId),
    serializeIdentifier(topicId),
    serializeIdentifier(groupId)
  ]);
};


/**
 * Deserializes a consumer group from a buffer.
 *
 * @param r - Buffer containing serialized consumer group data
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read and deserialized consumer group data
 */
export const deserializeConsumerGroup = (r: Buffer, pos = 0): ConsumerGroupDeserialized => {
  const id = r.readUInt32LE(pos);
  const partitionsCount = r.readUInt32LE(pos + 4);
  const membersCount = r.readUInt32LE(pos + 8);
  const nameLength = r.readUInt8(pos + 12);
  const name = r.subarray(pos + 13, pos + 13 + nameLength).toString();

  return {
    bytesRead: 4 + 4 + 4 + 1 + nameLength,
    data: {
      id,
      name,
      partitionsCount,
      membersCount,
    }
  }
};

/**
 * Deserializes multiple consumer groups from a buffer.
 *
 * @param r - Buffer containing serialized consumer groups data
 * @param pos - Starting position in the buffer
 * @returns Array of deserialized consumer groups
 */
export const deserializeConsumerGroups = (r: Buffer, pos = 0) => {
  const end = r.length;
  const cgroups = [];
  while (pos < end) {
    const { bytesRead, data } = deserializeConsumerGroup(r, pos);
    cgroups.push(data);
    pos += bytesRead;
  }
  return cgroups;
};
