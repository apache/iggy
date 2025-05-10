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

export type ConsumerGroup = {
  id: number,
  name: string,
  membersCount: number,
  partitionsCount: number,
};

type ConsumerGroupDeserialized = {
  bytesRead: number,
  data: ConsumerGroup
};

export const serializeTargetGroup = (streamId: Id, topicId: Id, groupId: Id) => {
  return Buffer.concat([
    serializeIdentifier(streamId),
    serializeIdentifier(topicId),
    serializeIdentifier(groupId)
  ]);
};


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
