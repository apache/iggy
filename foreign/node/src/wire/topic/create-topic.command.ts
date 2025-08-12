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


import type { CommandResponse } from '../../client/client.type.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import {
  isValidCompressionAlgorithm, CompressionAlgorithm,
  deserializeTopic,
  type Topic,
  type CompressionAlgorithm as CompressionAlgorithmT
} from './topic.utils.js';


export type CreateTopic = {
  streamId: Id,
  topicId: number,
  name: string,
  partitionCount: number,
  compressionAlgorithm: CompressionAlgorithmT,
  messageExpiry?: bigint,
  maxTopicSize?: bigint,
  replicationFactor?: number
};

export const CREATE_TOPIC = {
  code: COMMAND_CODE.CreateTopic,

  serialize: ({
    streamId,
    topicId,
    name,
    partitionCount,
    compressionAlgorithm = CompressionAlgorithm.None,
    messageExpiry = 0n,
    maxTopicSize = 0n,
    replicationFactor = 1
  }: CreateTopic
  ) => {
    const streamIdentifier = serializeIdentifier(streamId);
    const bName = Buffer.from(name)
  
    if (replicationFactor < 1 || replicationFactor > 255)
      throw new Error('Topic replication factor should be between 1 and 255');
    if (bName.length < 1 || bName.length > 255)
      throw new Error('Topic name should be between 1 and 255 bytes');
    if(!isValidCompressionAlgorithm(compressionAlgorithm))
      throw new Error(`createTopic: invalid compressionAlgorithm (${compressionAlgorithm})`);
    
    const b = Buffer.allocUnsafe(4 + 4 + 1 + 8 + 8 + 1 + 1);
    b.writeUInt32LE(topicId, 0);
    b.writeUInt32LE(partitionCount, 4);
    b.writeUInt8(compressionAlgorithm, 8);
    b.writeBigUInt64LE(messageExpiry, 9); // 0 is unlimited
    b.writeBigUInt64LE(maxTopicSize, 17); // optional, 0 is null
    b.writeUInt8(replicationFactor, 25); // must be > 0
    b.writeUInt8(bName.length, 26);
  
    return Buffer.concat([
      streamIdentifier,
      b,
      bName,
    ]);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeTopic(r.data).data;
  }
};


export const createTopic = wrapCommand<CreateTopic, Topic>(CREATE_TOPIC);
