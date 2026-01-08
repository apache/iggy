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
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import {
  type CompressionAlgorithm as CompressionAlgorithmT,
  CompressionAlgorithm,
  isValidCompressionAlgorithm
} from './topic.utils.js';


/**
 * Parameters for the update topic command.
 */
export type UpdateTopic = {
  /** Stream identifier (ID or name) */
  streamId: Id,
  /** Topic identifier (ID or name) */
  topicId: Id,
  /** New topic name (1-255 bytes) */
  name: string,
  /** Compression algorithm (None or Gzip) */
  compressionAlgorithm?: CompressionAlgorithmT,
  /** Message expiry time in microseconds (0 = unlimited) */
  messageExpiry?: bigint,
  /** Maximum topic size in bytes (0 = unlimited) */
  maxTopicSize?: bigint,
  /** Replication factor (1-255) */
  replicationFactor?: number,
};

/**
 * Update topic command definition.
 * Updates a topic's configuration.
 */
export const UPDATE_TOPIC = {
  code: COMMAND_CODE.UpdateTopic,

  serialize: ({
    streamId,
    topicId,
    name,
    compressionAlgorithm = CompressionAlgorithm.None,
    messageExpiry = 0n,
    maxTopicSize = 0n,
    replicationFactor = 1,
  }: UpdateTopic) => {
    const streamIdentifier = serializeIdentifier(streamId);
    const topicIdentifier = serializeIdentifier(topicId);
    const bName = Buffer.from(name)

    if (bName.length < 1 || bName.length > 255)
      throw new Error('Topic name should be between 1 and 255 bytes');
    if(!isValidCompressionAlgorithm(compressionAlgorithm))
      throw new Error(`createTopic: invalid compressionAlgorithm (${compressionAlgorithm})`);

    const b = Buffer.allocUnsafe(8 + 8 + 1 + 1 + 1);
    b.writeUInt8(compressionAlgorithm, 0);
    b.writeBigUInt64LE(messageExpiry, 1); // 0 is unlimited ???
    b.writeBigUInt64LE(maxTopicSize, 9); // optional, 0 is null
    b.writeUInt8(replicationFactor, 17); // must be > 0
    b.writeUInt8(bName.length, 18);

    return Buffer.concat([
      streamIdentifier,
      topicIdentifier,
      b,
      bName,
    ]);
  },

  deserialize: deserializeVoidResponse
};


/**
 * Executable update topic command function.
 */
export const updateTopic = wrapCommand<UpdateTopic, boolean>(UPDATE_TOPIC);
