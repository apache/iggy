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
import { ResponseError } from '../error.utils.js';
import {
  Command2,
  EVICTION_OFFSET,
  EvictionReason,
  HEADER_SIZE,
  REPLY_OFFSET
} from './header.js';
import { Operation } from './operation.js';
import {
  decodeResponse,
  splitMetadataResult,
  VsrEvictionError
} from './reply.js';

const reply = (
  operation: number,
  body = Buffer.alloc(0),
  status = 0
): Buffer => {
  const frame = Buffer.alloc(HEADER_SIZE + body.length);
  frame.writeUInt32LE(frame.length, REPLY_OFFSET.size);
  frame.writeUInt8(Command2.Reply, REPLY_OFFSET.command);
  frame.writeUInt8(operation, REPLY_OFFSET.operation);
  frame.writeUInt32LE(status, REPLY_OFFSET.status);
  body.copy(frame, HEADER_SIZE);
  return frame;
};

const eviction = (reason: number): Buffer => {
  const frame = Buffer.alloc(HEADER_SIZE);
  frame.writeUInt32LE(HEADER_SIZE, REPLY_OFFSET.size);
  frame.writeUInt8(Command2.Eviction, REPLY_OFFSET.command);
  frame.writeUInt8(reason, EVICTION_OFFSET.reason);
  return frame;
};

describe('VSR reply decoding', () => {
  it('rejects truncated, undersized, and unsupported frames', () => {
    assert.throws(() => decodeResponse(Buffer.alloc(255)), ResponseError);

    const undersized = reply(Operation.NonReplicated);
    undersized.writeUInt32LE(1, REPLY_OFFSET.size);
    assert.throws(() => decodeResponse(undersized), ResponseError);

    const unsupported = reply(Operation.NonReplicated);
    unsupported.writeUInt8(Command2.Request, REPLY_OFFSET.command);
    assert.throws(() => decodeResponse(unsupported), ResponseError);
  });

  it('rejects a declared size beyond the received bytes', () => {
    const frame = reply(Operation.NonReplicated);
    frame.writeUInt32LE(HEADER_SIZE + 1, REPLY_OFFSET.size);
    assert.throws(() => decodeResponse(frame), ResponseError);
  });

  it('rejects an undeclared reply operation discriminant', () => {
    for (const operation of [69, 150, 163, 255])
      assert.throws(
        () => decodeResponse(reply(operation)),
        (error: unknown) =>
          error instanceof ResponseError && error.errorCode === 3
      );
  });

  it('passes an empty register body through as a terminal failure', () => {
    assert.deepEqual(
      decodeResponse(reply(Operation.Register)),
      Buffer.alloc(0)
    );
    const committed = Buffer.concat([Buffer.alloc(4), Buffer.from('ok')]);
    assert.deepEqual(
      decodeResponse(reply(Operation.Register, committed)),
      Buffer.from('ok')
    );
  });

  it('returns only the declared body and ignores a following frame', () => {
    const first = reply(Operation.NonReplicated, Buffer.from('one'));
    const second = reply(Operation.NonReplicated, Buffer.from('two'));
    assert.deepEqual(
      decodeResponse(Buffer.concat([first, second])),
      Buffer.from('one')
    );
  });

  it('surfaces pre-commit status before decoding the body', () => {
    assert.throws(
      () => decodeResponse(reply(Operation.CreateStream, Buffer.alloc(0), 57)),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 57
    );
  });

  it('strips a successful result section', () => {
    const body = Buffer.concat([Buffer.alloc(4), Buffer.from('stream')]);
    assert.deepEqual(
      decodeResponse(reply(Operation.CreateStream, body)),
      Buffer.from('stream')
    );
  });

  it('surfaces the first committed result error', () => {
    const body = Buffer.alloc(20);
    body.writeUInt32LE(2, 0);
    body.writeUInt32LE(7, 4);
    body.writeUInt32LE(1009, 8);
    body.writeUInt32LE(8, 12);
    body.writeUInt32LE(1010, 16);
    assert.throws(
      () => splitMetadataResult(Operation.CreateStream, body),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 1009
    );
  });

  it('rejects truncated result sections', () => {
    assert.throws(
      () => splitMetadataResult(Operation.CreateStream, Buffer.alloc(3)),
      ResponseError
    );
    const body = Buffer.alloc(4);
    body.writeUInt32LE(1, 0);
    assert.throws(
      () => splitMetadataResult(Operation.CreateStream, body),
      ResponseError
    );
  });

  it('maps every eviction reason to a terminal eviction error', () => {
    for (const reason of Object.values(EvictionReason)) {
      const frame = eviction(reason);
      if (reason === EvictionReason.IncompatibleProtocol) {
        frame.writeUInt32LE((0 << 20) | (10 << 10) | 3, 144);
        frame.writeUInt32LE(10 << 10, 148);
      }
      assert.throws(
        () => decodeResponse(frame),
        VsrEvictionError
      );
    }
    assert.throws(
      () => decodeResponse(eviction(255)),
      VsrEvictionError
    );
  });

  it('rejects malformed incompatible-protocol windows safely', () => {
    const frame = eviction(EvictionReason.IncompatibleProtocol);
    frame.writeUInt32LE(1, 144);
    frame.writeUInt32LE(2, 148);
    assert.throws(
      () => decodeResponse(frame),
      (error: unknown) =>
        error instanceof VsrEvictionError && error.errorCode === 40
    );
  });
});
