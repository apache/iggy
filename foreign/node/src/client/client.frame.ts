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

const headerSizeFor = (protocol: Protocol): number =>
  protocol === 'vsr' ? VSR_HEADER_SIZE : CLASSIC_HEADER_SIZE;

const declaredFrameSize = (
  protocol: Protocol,
  header: Buffer,
  maximumFrameSize: number
): number => {
  const headerSize = headerSizeFor(protocol);
  const declaredSize = protocol === 'vsr'
    ? readVsrSize(header)
    : CLASSIC_HEADER_SIZE + header.readUInt32LE(4);

  if (declaredSize < headerSize)
    throw new ProtocolFrameError(
      `declared ${protocol} frame size ${declaredSize} is below header size`
    );
  if (declaredSize > maximumFrameSize)
    throw new ProtocolFrameError(
      `declared ${protocol} frame size ${declaredSize} exceeds ` +
      `the ${maximumFrameSize} byte limit`
    );
  return declaredSize;
};

export const extractResponseFrames = (
  protocol: Protocol,
  buffer: Buffer,
  maximumFrameSize: number
): ExtractedFrames => {
  const headerSize = headerSizeFor(protocol);
  const frames: Buffer[] = [];
  let offset = 0;

  while (buffer.length - offset >= headerSize) {
    const available = buffer.length - offset;
    const declaredSize = declaredFrameSize(
      protocol,
      buffer.subarray(offset, offset + headerSize),
      maximumFrameSize
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
      : buffer.subarray(offset)
  };
};

/**
 * Incrementally extracts response frames without repeatedly copying an
 * incomplete frame as new socket chunks arrive.
 */
export class ResponseFrameDecoder {
  private readonly protocol: Protocol;
  private readonly maximumFrameSize: number;
  private readonly headerSize: number;
  private chunks: Buffer[];
  private chunkIndex: number;
  private chunkOffset: number;
  private bufferedLength: number;
  private expectedFrameSize?: number;

  constructor(protocol: Protocol, maximumFrameSize: number) {
    this.protocol = protocol;
    this.maximumFrameSize = maximumFrameSize;
    this.headerSize = headerSizeFor(protocol);
    this.chunks = [];
    this.chunkIndex = 0;
    this.chunkOffset = 0;
    this.bufferedLength = 0;
    this.expectedFrameSize = undefined;
  }

  get hasBufferedData(): boolean {
    return this.bufferedLength > 0;
  }

  clear(): void {
    this.chunks = [];
    this.chunkIndex = 0;
    this.chunkOffset = 0;
    this.bufferedLength = 0;
    this.expectedFrameSize = undefined;
  }

  push(data: Buffer): Buffer[] {
    if (data.length > 0) {
      this.chunks.push(data);
      this.bufferedLength += data.length;
    }

    const frames: Buffer[] = [];
    while (true) {
      if (this.expectedFrameSize === undefined) {
        if (this.bufferedLength < this.headerSize)
          break;
        this.expectedFrameSize = declaredFrameSize(
          this.protocol,
          this.peek(this.headerSize),
          this.maximumFrameSize
        );
      }
      if (this.bufferedLength < this.expectedFrameSize)
        break;

      frames.push(this.consume(this.expectedFrameSize));
      this.expectedFrameSize = undefined;
    }
    this.compact();
    return frames;
  }

  private peek(length: number): Buffer {
    const first = this.chunks[this.chunkIndex];
    const firstAvailable = first.length - this.chunkOffset;
    if (firstAvailable >= length)
      return first.subarray(this.chunkOffset, this.chunkOffset + length);

    const result = Buffer.allocUnsafe(length);
    let resultOffset = 0;
    for (let index = this.chunkIndex; resultOffset < length; index += 1) {
      const chunk = this.chunks[index];
      const offset = index === this.chunkIndex ? this.chunkOffset : 0;
      const copyLength = Math.min(chunk.length - offset, length - resultOffset);
      chunk.copy(result, resultOffset, offset, offset + copyLength);
      resultOffset += copyLength;
    }
    return result;
  }

  private consume(length: number): Buffer {
    const first = this.chunks[this.chunkIndex];
    const firstAvailable = first.length - this.chunkOffset;
    if (firstAvailable >= length) {
      const frame = first.subarray(
        this.chunkOffset,
        this.chunkOffset + length
      );
      this.chunkOffset += length;
      this.bufferedLength -= length;
      if (this.chunkOffset === first.length) {
        this.chunkIndex += 1;
        this.chunkOffset = 0;
      }
      return frame;
    }

    const frame = Buffer.allocUnsafe(length);
    let frameOffset = 0;
    while (frameOffset < length) {
      const chunk = this.chunks[this.chunkIndex];
      const available = chunk.length - this.chunkOffset;
      const copyLength = Math.min(available, length - frameOffset);
      chunk.copy(
        frame,
        frameOffset,
        this.chunkOffset,
        this.chunkOffset + copyLength
      );
      frameOffset += copyLength;
      this.chunkOffset += copyLength;
      if (this.chunkOffset === chunk.length) {
        this.chunkIndex += 1;
        this.chunkOffset = 0;
      }
    }
    this.bufferedLength -= length;
    return frame;
  }

  private compact(): void {
    if (this.chunkIndex === 0)
      return;
    if (this.chunkIndex === this.chunks.length) {
      this.chunks = [];
      this.chunkIndex = 0;
      return;
    }
    this.chunks = this.chunks.slice(this.chunkIndex);
    this.chunkIndex = 0;
  }
}
