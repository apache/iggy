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

import type { CommandResponse } from '../../client/client.type.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { responseError } from '../error.utils.js';
import { HeaderValue } from '../message/header.utils.js';
import { StreamOptionKey, dedupeOptions, serializeOptions, sortOptions } from '../options.utils.js';
import { deserializeToStream, type Stream } from './stream.utils.js';
import { StreamEncryptionKey, type StreamOptions } from './stream-options.js';

const INVALID_ENCRYPTION_KEY = 70;
const INVALID_COMMAND = 3;

/**
 * Parameters for the create stream command.
 */
export type CreateStream = {
  /** Optional stream ID (auto-assigned by server if not provided) */
  streamId?: number,
  /** Stream name (1-255 bytes) */
  name: string
} & StreamOptions;

/**
 * Create stream command definition.
 * Creates a new stream with the specified name.
 * Layout: `[name_len:u8][name]` followed by the options block running to the
 * end of the payload.
 */
export const CREATE_STREAM = {
  code: COMMAND_CODE.CreateStream,

  serialize: ({ name, encryptionKey, options: extraOptions = [] }: CreateStream) => {
    // Stream ID is now auto-assigned by the server, not sent in the protocol
    const bName = Buffer.from(name);

    if (bName.length < 1 || bName.length > 255)
      throw new Error('Stream name should be between 1 and 255 bytes');

    // A plain entry prints its value wherever the caller logs the arguments.
    if (extraOptions.some(({ key }) => key === StreamOptionKey.EncryptionKey))
      throw responseError(
        COMMAND_CODE.CreateStream, INVALID_COMMAND,
        'stream key must be passed as encryptionKey, not as an options entry'
      );

    const options = [...extraOptions];
    if (encryptionKey !== undefined) {
      if (!(encryptionKey instanceof StreamEncryptionKey))
        throw responseError(
          COMMAND_CODE.CreateStream, INVALID_ENCRYPTION_KEY,
          'stream key must be a StreamEncryptionKey'
        );
      options.push({ key: StreamOptionKey.EncryptionKey, value: HeaderValue.Raw(encryptionKey.asBytes()) });
    }

    const b = Buffer.allocUnsafe(1);
    b.writeUInt8(bName.length, 0);

    return Buffer.concat([
      b,
      bName,
      serializeOptions(sortOptions(dedupeOptions(options)))
    ]);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeToStream(r.data, 0).data
  }
};


/**
 * Executable create stream command function.
 */
export const createStream = wrapCommand<CreateStream, Stream>(CREATE_STREAM);
