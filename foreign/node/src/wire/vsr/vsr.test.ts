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

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ResponseError } from '../error.utils.js';
import { Command, COMMITTED_RESULT_MARKER, HEADER_SIZE, REPLY_OFFSET, REQUEST_OFFSET } from './header.js';
import { prepareVsrCommand, VsrSession } from './index.js';
import { Operation } from './operation.js';
import { validateReplyForRequest } from './reply.js';
import { COMMAND_CODE } from '../command.code.js';
import { serializeSendMessages } from '../message/message.utils.js';
import { Partitioning } from '../message/partitioning.utils.js';

const BIND_SECRET = Buffer.alloc(32, 0x5a);

describe('VSR custom request framing', () => {
  it('stamps distinct random identifiers for metadata and session requests', () => {
    const payloads = [Buffer.alloc(0), Buffer.from([1, 2, 3]), Buffer.from('abc')];
    for (const command of [COMMAND_CODE.LoginRegister, COMMAND_CODE.LoginRegisterWithAccessToken,
      COMMAND_CODE.LogoutUser, COMMAND_CODE.CreateStream, COMMAND_CODE.DeleteSegments]) {
      const session = new VsrSession(7n);
      session.bind(42n);
      for (const payload of payloads) {
        const frame = session.encode(command, payload);
        const another = session.encode(command, payload);
        const identifier = frame.subarray(REQUEST_OFFSET.requestChecksum, REQUEST_OFFSET.requestChecksum + 16);
        assert.notDeepEqual(identifier, Buffer.alloc(16));
        const repeated = another.subarray(REQUEST_OFFSET.requestChecksum, REQUEST_OFFSET.requestChecksum + 16);
        if (command === COMMAND_CODE.LoginRegister || command === COMMAND_CODE.LoginRegisterWithAccessToken)
          assert.deepEqual(identifier, repeated);
        else
          assert.notDeepEqual(identifier, repeated);
        assert.deepEqual(frame.subarray(HEADER_SIZE), payload);
      }
    }
  });

  it('leaves partition and non-replicated request checksums unstamped', () => {
    const session = new VsrSession(7n);
    session.bind(42n);
    for (const command of [COMMAND_CODE.SendMessages, COMMAND_CODE.StoreOffset,
      COMMAND_CODE.DeleteConsumerOffset, COMMAND_CODE.Ping, COMMAND_CODE.GetPartitionRoute,
      COMMAND_CODE.BindSession, 60_000]) {
      const frame = session.encode(command, Buffer.from([1, 2, 3]));
      assert.deepEqual(frame.subarray(REQUEST_OFFSET.requestChecksum, REQUEST_OFFSET.requestChecksum + 16),
        Buffer.alloc(16), `command ${command}`);
    }
  });

  it('checks marked metadata receipts against their original payload stamp', () => {
    const session = new VsrSession(7n);
    session.bind(42n);
    const request = session.encode(COMMAND_CODE.CreateStream, Buffer.from([1, 2, 3]));
    const differentPayload = session.encode(COMMAND_CODE.CreateStream, Buffer.from('abc'));
    const receipt = Buffer.alloc(HEADER_SIZE);
    receipt.writeUInt32LE(HEADER_SIZE, REPLY_OFFSET.size);
    receipt[REPLY_OFFSET.command] = Command.Reply;
    receipt[REPLY_OFFSET.operation] = Operation.CreateStream;
    receipt[REPLY_OFFSET.reserved] = COMMITTED_RESULT_MARKER;
    receipt.writeBigUInt64LE(1n, REPLY_OFFSET.op);
    receipt.writeBigUInt64LE(1n, REPLY_OFFSET.commit);
    receipt.writeBigUInt64LE(request.readBigUInt64LE(REQUEST_OFFSET.request), REPLY_OFFSET.request);
    request.copy(receipt, REPLY_OFFSET.client, REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16);
    request.copy(receipt, REPLY_OFFSET.requestChecksum, REQUEST_OFFSET.requestChecksum, REQUEST_OFFSET.requestChecksum + 16);
    assert.doesNotThrow(() => validateReplyForRequest(receipt, request, COMMAND_CODE.CreateStream));
    differentPayload.copy(receipt, REPLY_OFFSET.requestChecksum, REQUEST_OFFSET.requestChecksum, REQUEST_OFFSET.requestChecksum + 16);
    assert.throws(() => validateReplyForRequest(receipt, request, COMMAND_CODE.CreateStream),
      (error: unknown) => error instanceof ResponseError && error.commandCode === COMMAND_CODE.CreateStream && error.errorCode === 3);
  });

  it('encodes a custom non-replicated code and opaque payload', () => {
    const session = new VsrSession();
    const payload = Buffer.from([0xAA, 0xBB, 0xCC]);
    const frame = session.encode(60_000, payload);

    assert.equal(frame.readUInt8(REQUEST_OFFSET.operation), Operation.NonReplicated);
    assert.equal(frame.readUInt32LE(REQUEST_OFFSET.reserved), 60_000);
    assert.deepEqual(frame.subarray(256), payload);
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
      () => prepareVsrCommand(COMMAND_CODE.LoginUser, Buffer.alloc(0), BIND_SECRET)
    );
    assert.throws(
      () => prepareVsrCommand(
        COMMAND_CODE.LoginUser,
        Buffer.from([4, 0x69, 0x67]),
        BIND_SECRET
      )
    );
  });

  it('converts legacy token login to VSR registration', () => {
    const token = Buffer.from('secret');
    const prepared = prepareVsrCommand(
      COMMAND_CODE.LoginWithAccessToken,
      Buffer.concat([Buffer.from([token.length]), token]),
      BIND_SECRET
    );
    assert.equal(
      prepared.command,
      COMMAND_CODE.LoginRegisterWithAccessToken
    );
    assert.ok(prepared.payload.includes(token));

    const frame = new VsrSession(7n).encode(
      prepared.command,
      prepared.payload
    );
    assert.equal(
      frame.readUInt8(REQUEST_OFFSET.operation),
      Operation.Register
    );
  });

  it('assigns distinct request ids to bound reads and writes', () => {
    const session = new VsrSession(7n);
    session.bind(42n);
    const custom = session.encode(
      60_001,
      Buffer.alloc(0)
    );
    assert.equal(custom.readBigUInt64LE(REQUEST_OFFSET.request), 1n);

    const metadata = session.encode(
      COMMAND_CODE.CreateStream,
      Buffer.alloc(0)
    );
    assert.equal(metadata.readBigUInt64LE(REQUEST_OFFSET.request), 2n);
    assert.equal(metadata.length, HEADER_SIZE);
  });

  it('partition operations consume a distinct id per send', () => {
    // Dedup identity requires each send to carry a distinct number, so
    // partition ops advance the counter exactly like metadata ops and the
    // two planes interleave on one sequence.
    const session = new VsrSession(7n);
    session.bind(42n);
    const sendMessages = () =>
      session.encode(
        COMMAND_CODE.SendMessages,
        serializeSendMessages(
          1,
          2,
          [{ payload: 'x' }],
          Partitioning.PartitionId(3)
        )
      );

    const first = sendMessages();
    assert.equal(
      first.readUInt8(REQUEST_OFFSET.operation),
      Operation.SendMessages
    );
    assert.equal(first.readBigUInt64LE(REQUEST_OFFSET.request), 1n);
    assert.equal(sendMessages().readBigUInt64LE(REQUEST_OFFSET.request), 2n);

    const metadata = session.encode(
      COMMAND_CODE.CreateStream,
      Buffer.alloc(0)
    );
    assert.equal(metadata.readBigUInt64LE(REQUEST_OFFSET.request), 3n);
  });
});
