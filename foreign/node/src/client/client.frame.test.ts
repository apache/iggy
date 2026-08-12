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

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  extractResponseFrames,
  ProtocolFrameError,
  ResponseFrameDecoder
} from './client.frame.js';
import {
  Command2,
  HEADER_SIZE,
  REPLY_OFFSET
} from '../wire/vsr/header.js';

const LIMIT = 1024;

const vsrFrame = (body: Buffer): Buffer => {
  const frame = Buffer.alloc(HEADER_SIZE + body.length);
  frame.writeUInt32LE(frame.length, REPLY_OFFSET.size);
  frame.writeUInt8(Command2.Reply, REPLY_OFFSET.command);
  body.copy(frame, HEADER_SIZE);
  return frame;
};

describe('extractResponseFrames', () => {
  it('buffers headers split at every boundary', () => {
    const frame = vsrFrame(Buffer.from('payload'));

    for (let split = 0; split < HEADER_SIZE; split += 1) {
      const first = extractResponseFrames(frame.subarray(0, split), LIMIT);
      assert.equal(first.frames.length, 0);
      const second = extractResponseFrames(
        Buffer.concat([first.remainder, frame.subarray(split)]),
        LIMIT
      );
      assert.deepEqual(second.frames, [frame]);
      assert.equal(second.remainder.length, 0);
    }
  });

  it('buffers a fragmented body', () => {
    const frame = vsrFrame(Buffer.from('payload'));
    const split = frame.length - 2;
    const first = extractResponseFrames(frame.subarray(0, split), LIMIT);
    assert.equal(first.frames.length, 0);
    const second = extractResponseFrames(
      Buffer.concat([first.remainder, frame.subarray(split)]),
      LIMIT
    );
    assert.deepEqual(second.frames, [frame]);
  });

  it('extracts coalesced frames and a partial tail', () => {
    const first = vsrFrame(Buffer.from('one'));
    const second = vsrFrame(Buffer.from('two'));
    const third = vsrFrame(Buffer.from('three'));
    const input = Buffer.concat([first, second, third.subarray(0, 3)]);
    const extracted = extractResponseFrames(input, LIMIT);

    assert.deepEqual(extracted.frames, [first, second]);
    assert.deepEqual(extracted.remainder, third.subarray(0, 3));
    assert.equal(extracted.remainder.buffer, input.buffer);
  });

  it('rejects a size below the header', () => {
    const frame = vsrFrame(Buffer.alloc(0));
    frame.writeUInt32LE(0, REPLY_OFFSET.size);
    assert.throws(
      () => extractResponseFrames(frame, LIMIT),
      ProtocolFrameError
    );
  });

  it('rejects an oversized frame before buffering its body', () => {
    const header = vsrFrame(Buffer.alloc(0));
    header.writeUInt32LE(LIMIT + 1, REPLY_OFFSET.size);
    assert.throws(
      () => extractResponseFrames(header, LIMIT),
      ProtocolFrameError
    );
  });
});

describe('ResponseFrameDecoder', () => {
  it('decodes bytewise input without losing coalesced frames', () => {
    const decoder = new ResponseFrameDecoder(LIMIT);
    const first = vsrFrame(Buffer.from('first'));
    const second = vsrFrame(Buffer.from('second'));
    const input = Buffer.concat([first, second]);
    const frames: Buffer[] = [];

    for (const byte of input)
      frames.push(...decoder.push(Buffer.from([byte])));

    assert.deepEqual(frames, [first, second]);
    assert.equal(decoder.hasBufferedData, false);
  });

  it('clears a partial frame', () => {
    const decoder = new ResponseFrameDecoder(LIMIT);
    decoder.push(vsrFrame(Buffer.from('body')).subarray(0, HEADER_SIZE - 2));
    assert.equal(decoder.hasBufferedData, true);
    decoder.clear();
    assert.equal(decoder.hasBufferedData, false);
  });

  it('rejects an oversized frame as soon as its header is complete', () => {
    const decoder = new ResponseFrameDecoder(LIMIT);
    const header = vsrFrame(Buffer.alloc(0));
    header.writeUInt32LE(LIMIT + 1, REPLY_OFFSET.size);
    assert.throws(() => decoder.push(header), ProtocolFrameError);
  });
});
