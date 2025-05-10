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


export const handleResponse = (r: Buffer) => {
  const status = r.readUint32LE(0);
  const length = r.readUint32LE(4);
  debug('<== handleResponse', { status, length });
  return {
    status, length, data: r.subarray(8)
  }
};

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

export const deserializeVoidResponse =
  (r: CommandResponse) => r.status === 0 && r.data.length === 0;

const COMMAND_LENGTH = 4;

export const serializeCommand = (command: number, payload: Buffer) => {
  const payloadSize = payload.length + COMMAND_LENGTH;
  const head = Buffer.allocUnsafe(8);

  head.writeUint32LE(payloadSize, 0);
  head.writeUint32LE(command, 4);

  debug(
    '==> CMD', command,
    translateCommandCode(command),
    head.subarray(4, 8).toString('hex'),
    'LENGTH', payloadSize,
    head.subarray(0, 4).toString('hex')
  );

  debug('message#HEAD', head.toString('hex'));
  debug('message#PAYLOAD', payload.toString('hex'));

  const pl = Buffer.concat([head, payload])
  
  debug('FullMessage#Base64', pl.toString('base64'));

  return pl;
}


