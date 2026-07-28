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

import type { Protocol } from './client.type.js';
import {
  HEADER_SIZE as VSR_HEADER_SIZE,
  readSize as readVsrSize
} from '../wire/vsr/header.js';

const CLASSIC_HEADER_SIZE = 8;

export class ProtocolFrameError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ProtocolFrameError';
  }
}

export type ExtractedFrames = {
  frames: Buffer[],
  remainder: Buffer
};

export const extractResponseFrames = (
  protocol: Protocol,
  buffer: Buffer,
  maximumFrameSize: number
): ExtractedFrames => {
  const headerSize =
    protocol === 'vsr' ? VSR_HEADER_SIZE : CLASSIC_HEADER_SIZE;
  const frames: Buffer[] = [];
  let offset = 0;

  while (buffer.length - offset >= headerSize) {
    const available = buffer.length - offset;
    const declaredSize = protocol === 'vsr'
      ? readVsrSize(buffer.subarray(offset, offset + headerSize))
      : CLASSIC_HEADER_SIZE + buffer.readUInt32LE(offset + 4);

    if (declaredSize < headerSize)
      throw new ProtocolFrameError(
        `declared ${protocol} frame size ${declaredSize} is below header size`
      );
    if (!Number.isSafeInteger(declaredSize) ||
        declaredSize > maximumFrameSize)
      throw new ProtocolFrameError(
        `declared ${protocol} frame size ${declaredSize} exceeds ` +
        `the ${maximumFrameSize} byte limit`
      );
    if (available < declaredSize)
      break;

    frames.push(buffer.subarray(offset, offset + declaredSize));
    offset += declaredSize;
  }

  return {
    frames,
    remainder: offset === buffer.length
      ? Buffer.alloc(0)
      : Buffer.from(buffer.subarray(offset))
  };
};
