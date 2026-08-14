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

import type { CommandResponse } from '../../client/client.type.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { serializeOptions, type OptionEntry } from '../options.utils.js';
import { HeaderValue } from '../message/header.utils.js';
import {
  isValidCompressionAlgorithm, CompressionAlgorithm,
  compressionAlgorithmName,
  deserializeTopic,
  type Topic,
  type CompressionAlgorithm as CompressionAlgorithmT
} from './topic.utils.js';


/**
 * Parameters for the create topic command.
 */
export type CreateTopic = {
  /** Stream identifier (ID or name) */
  streamId: Id,
  /** Topic name (1-255 bytes) */
  name: string,
  /** Number of partitions to create */
  partitionCount: number,
  /** Compression algorithm (None or Gzip) */
  compressionAlgorithm: CompressionAlgorithmT,
  /** Message expiry time in microseconds (0 = unlimited) */
  messageExpiry?: bigint,
  /** Maximum topic size in bytes (0 = unlimited) */
  maxTopicSize?: bigint,
  /** Segment size in bytes: 512-byte multiple between 1 MiB and 1 GiB */
  segmentSize?: bigint,
  /** Fsync every write instead of leaving it to the page cache */
  enforceFsync?: boolean,
  /** Message count that triggers a save (must be non-zero) */
  messagesRequiredToSave?: number,
  /** Accumulated message bytes that trigger a save */
  sizeOfMessagesRequiredToSave?: bigint,
  /** Preallocate segment files when the topic is created */
  preallocateSegments?: boolean
};

/**
 * Create topic command definition.
 * Creates a new topic within a stream.
 * Layout: `[stream_id][partitions_count:u32_le][name_len:u8][name]`
 * followed by the options TLV block running to the end of the payload.
 */
export const CREATE_TOPIC = {
  code: COMMAND_CODE.CreateTopic,

  serialize: ({
    streamId,
    name,
    partitionCount,
    compressionAlgorithm = CompressionAlgorithm.None,
    messageExpiry = 0n,
    maxTopicSize = 0n,
    segmentSize,
    enforceFsync,
    messagesRequiredToSave,
    sizeOfMessagesRequiredToSave,
    preallocateSegments
  }: CreateTopic
  ) => {
    // Topic ID is now auto-assigned by the server, not sent in the protocol
    const streamIdentifier = serializeIdentifier(streamId);
    const bName = Buffer.from(name)

    if (bName.length < 1 || bName.length > 255)
      throw new Error('Topic name should be between 1 and 255 bytes');
    if(!isValidCompressionAlgorithm(compressionAlgorithm))
      throw new Error(`createTopic: invalid compressionAlgorithm (${compressionAlgorithm})`);

    // partitions_count rides the command's own fixed field, never the
    // options block: the server rejects it as an unsupported option key.
    // Server-default sentinels (expiry 0, size 0, compression none) and
    // unset optionals are omitted so the server resolves them from its own
    // config and reports them back as derived options.
    const options: OptionEntry[] = [];
    if (compressionAlgorithm !== CompressionAlgorithm.None)
      options.push({
        key: 'compression_algorithm',
        value: HeaderValue.String(compressionAlgorithmName(compressionAlgorithm))
      });
    if (messageExpiry !== 0n)
      options.push({
        key: 'message_expiry', value: HeaderValue.Uint64(messageExpiry)
      });
    if (maxTopicSize !== 0n)
      options.push({
        key: 'max_topic_size', value: HeaderValue.Uint64(maxTopicSize)
      });
    if (segmentSize !== undefined)
      options.push({
        key: 'segment_size', value: HeaderValue.Uint64(segmentSize)
      });
    if (enforceFsync !== undefined)
      options.push({
        key: 'enforce_fsync', value: HeaderValue.Bool(enforceFsync)
      });
    if (messagesRequiredToSave !== undefined)
      options.push({
        key: 'messages_required_to_save',
        value: HeaderValue.Uint32(messagesRequiredToSave)
      });
    if (sizeOfMessagesRequiredToSave !== undefined)
      options.push({
        key: 'size_of_messages_required_to_save',
        value: HeaderValue.Uint64(sizeOfMessagesRequiredToSave)
      });
    if (preallocateSegments !== undefined)
      options.push({
        key: 'preallocate_segments',
        value: HeaderValue.Bool(preallocateSegments)
      });
    const b = Buffer.allocUnsafe(4 + 1);
    b.writeUInt32LE(partitionCount, 0);
    b.writeUInt8(bName.length, 4);

    return Buffer.concat([
      streamIdentifier,
      b,
      bName,
      serializeOptions(options),
    ]);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeTopic(r.data).data;
  }
};


/**
 * Executable create topic command function.
 */
export const createTopic = wrapCommand<CreateTopic, Topic>(CREATE_TOPIC);
