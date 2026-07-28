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

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BINARY_REQUEST_KIND } from '../command-set.js';
import { ResponseError } from '../error.utils.js';
import { HEADER_SIZE, REQUEST_OFFSET } from './header.js';
import { prepareVsrCommand, VsrSession } from './index.js';
import { Operation } from './operation.js';
import { COMMAND_CODE } from '../command.code.js';

describe('VSR custom request framing', () => {
  it('encodes a custom non-replicated code and opaque payload', () => {
    const session = new VsrSession();
    const payload = Buffer.from([0xAA, 0xBB, 0xCC]);
    const frame = session.encode(
      60_000,
      payload,
      BINARY_REQUEST_KIND.NonReplicated
    );

    assert.equal(frame.readUInt8(REQUEST_OFFSET.operation), Operation.NonReplicated);
    assert.equal(frame.readUInt32LE(REQUEST_OFFSET.reserved), 60_000);
    assert.deepEqual(frame.subarray(256), payload);
  });

  it('rejects a custom replicated code without a server registry', () => {
    const session = new VsrSession();
    assert.throws(
      () => session.encode(
        60_000,
        Buffer.alloc(0),
        BINARY_REQUEST_KIND.Replicated
      ),
      /Feature is unavailable/
    );
  });

  it('does not consume a request ID when local routing fails', () => {
    const session = new VsrSession(7n);
    session.bind(42n);
    assert.throws(
      () => session.encode(
        COMMAND_CODE.SendMessages,
        Buffer.alloc(0)
      )
    );

    const frame = session.encode(
      COMMAND_CODE.CreateStream,
      Buffer.alloc(0)
    );
    assert.equal(frame.readBigUInt64LE(REQUEST_OFFSET.request), 1n);
  });

  it('rejects an unbound replicated request with a typed error', () => {
    const session = new VsrSession();
    assert.throws(
      () => session.encode(COMMAND_CODE.CreateStream, Buffer.alloc(0)),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 40
    );
  });

  it('rejects a truncated legacy login payload locally', () => {
    assert.throws(
      () => prepareVsrCommand(COMMAND_CODE.LoginUser, Buffer.alloc(0))
    );
    assert.throws(
      () => prepareVsrCommand(
        COMMAND_CODE.LoginUser,
        Buffer.from([4, 0x69, 0x67])
      )
    );
  });

  it('does not advance for non-replicated or partition operations', () => {
    const session = new VsrSession(7n);
    session.bind(42n);
    const custom = session.encode(
      60_001,
      Buffer.alloc(0),
      BINARY_REQUEST_KIND.NonReplicated
    );
    assert.equal(custom.readBigUInt64LE(REQUEST_OFFSET.request), 1n);

    const metadata = session.encode(
      COMMAND_CODE.CreateStream,
      Buffer.alloc(0)
    );
    assert.equal(metadata.readBigUInt64LE(REQUEST_OFFSET.request), 1n);
    assert.equal(metadata.length, HEADER_SIZE);
  });
});
