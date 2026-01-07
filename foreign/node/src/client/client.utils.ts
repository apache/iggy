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


import { Transform, type TransformCallback } from 'node:stream';
import type { CommandResponse } from './client.type.js';
import { translateCommandCode } from '../wire/command.code.js';
import { debug } from './client.debug.js';


/**
 * Parses a raw response buffer into a structured CommandResponse.
 * Extracts status code, length, and payload data from the buffer.
 *
 * @param r - Raw response buffer from the server
 * @returns Parsed command response with status, length, and data
 */
export const handleResponse = (r: Buffer) => {
  const status = r.readUint32LE(0);
  const length = r.readUint32LE(4);
  debug('<== handleResponse', { status, length });
  return {
    status, length, data: r.subarray(8)
  }
};

/**
 * Creates a Transform stream that parses response buffers.
 * Transforms raw server responses into just the data payload.
 *
 * @returns Transform stream for processing server responses
 */
export const handleResponseTransform = () => new Transform({
  transform(chunk: Buffer, encoding: BufferEncoding, cb: TransformCallback) {
    try {
      const r = handleResponse(chunk);
      debug('response::', r)
      return cb(null, r.data);
    } catch (err: unknown) {
      return cb(new Error('handleResponseTransform error', { cause: err }), null);
    }
  }
});

/**
 * Deserializes a void response from the server.
 * Returns true if the command succeeded with no data.
 *
 * @param r - Command response to check
 * @returns True if the response indicates success with no data
 */
export const deserializeVoidResponse =
  (r: CommandResponse) => r.status === 0 && r.data.length === 0;

/** Length of the command code in bytes */
const COMMAND_LENGTH = 4;

/**
 * Serializes a command and its payload into a buffer for sending to the server.
 * Creates the wire format: [payload_size (4 bytes)][command (4 bytes)][payload]
 *
 * @param command - Command code to send
 * @param payload - Command payload buffer
 * @returns Buffer ready to be sent to the server
 */
export const serializeCommand = (command: number, payload: Buffer) => {
  const payloadSize = payload.length + COMMAND_LENGTH;
  const data = Buffer.allocUnsafe(8 + payload.length);

  data.writeUint32LE(payloadSize, 0);
  data.writeUint32LE(command, 4);
  data.fill(payload, 8);

  debug(
    '==> CMD', command,
    translateCommandCode(command),
    'LENGTH', payloadSize
  );

  debug('FullMessage#Base64', data.toString('base64'));

  return data;
}
