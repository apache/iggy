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
import { serializeIdentifier } from '../identifier.utils.js';
import { serializeSendMessages } from '../message/message.utils.js';
import { Partitioning } from '../message/partitioning.utils.js';
import { Consumer, serializeStoreOffset } from '../offset/offset.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { ResponseError } from '../error.utils.js';
import {
  METADATA_CONSENSUS_NAMESPACE,
  namespaceForRequest,
  packNamespace
} from './namespace.js';
import { Operation } from './operation.js';

describe('VSR namespace routing', () => {
  it('routes register and logout to metadata consensus', () => {
    for (const operation of [Operation.Register, Operation.Logout])
      assert.equal(
        namespaceForRequest(0, Buffer.alloc(0), operation),
        METADATA_CONSENSUS_NAMESPACE
      );
  });

  it('routes metadata and non-replicated requests to zero', () => {
    assert.equal(
      namespaceForRequest(
        COMMAND_CODE.CreateStream,
        Buffer.alloc(0),
        Operation.CreateStream
      ),
      0n
    );
    assert.equal(
      namespaceForRequest(
        COMMAND_CODE.GetStats,
        Buffer.alloc(0),
        Operation.NonReplicated
      ),
      0n
    );
  });

  it('packs explicit send-message partition identifiers', () => {
    const payload = serializeSendMessages(
      7,
      11,
      [],
      Partitioning.PartitionId(13)
    );
    assert.equal(
      namespaceForRequest(
        COMMAND_CODE.SendMessages,
        payload,
        Operation.SendMessages
      ),
      packNamespace(7, 11, 13)
    );
  });

  it('defers named stream and topic routing to the server', () => {
    const payload = serializeSendMessages(
      'stream',
      'topic',
      [],
      Partitioning.PartitionId(1)
    );
    assert.equal(
      namespaceForRequest(
        COMMAND_CODE.SendMessages,
        payload,
        Operation.SendMessages
      ),
      0n
    );
  });

  it('rejects server-selected message partitioning', () => {
    const payload = serializeSendMessages(
      1,
      2,
      [],
      Partitioning.Balanced
    );
    assert.throws(
      () => namespaceForRequest(
        COMMAND_CODE.SendMessages,
        payload,
        Operation.SendMessages
      ),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 5
    );
  });

  it('routes consumer offsets from their explicit partition', () => {
    const payload = serializeStoreOffset(
      1,
      2,
      Consumer.Single,
      3,
      99n
    );
    assert.equal(
      namespaceForRequest(
        COMMAND_CODE.StoreOffset,
        payload,
        Operation.StoreConsumerOffset
      ),
      packNamespace(1, 2, 3)
    );
  });

  it('routes delete-segments payloads', () => {
    const payload = Buffer.concat([
      serializeIdentifier(1),
      serializeIdentifier(2),
      Buffer.from([3, 0, 0, 0])
    ]);
    assert.equal(
      namespaceForRequest(
        COMMAND_CODE.DeleteSegments,
        payload,
        Operation.DeleteSegments
      ),
      packNamespace(1, 2, 3)
    );
  });

  it('accepts the maximum packable identifiers', () => {
    assert.equal(
      packNamespace(4095, 4095, 999_999),
      (4095n << 32n) | (4095n << 20n) | 999_999n
    );
  });

  it('rejects peeks past the declared send-messages metadata region', () => {
    const payload = serializeSendMessages(
      1,
      2,
      [],
      Partitioning.PartitionId(3)
    );
    const underDeclared = Buffer.from(payload);
    // Shrink the declared metadata region so the partitioning bytes sit
    // outside it; the peek must fail instead of reading them.
    underDeclared.writeUInt32LE(payload.readUInt32LE(0) - 6, 0);
    assert.throws(
      () => namespaceForRequest(
        COMMAND_CODE.SendMessages,
        underDeclared,
        Operation.SendMessages
      ),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 3
    );
  });

  it('rejects unknown codes in partition routing', () => {
    assert.throws(
      () => namespaceForRequest(
        60_001,
        Buffer.alloc(0),
        Operation.SendMessages
      ),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 5
    );
  });

  it('requires an explicit consumer-offset partition', () => {
    const payload = serializeStoreOffset(
      1,
      2,
      Consumer.Single,
      3,
      99n
    );
    // [kind u8][consumer 6][stream 6][topic 6] puts the partition flag at 19.
    const withoutPartition = Buffer.from(payload);
    withoutPartition.writeUInt8(0, 19);
    assert.throws(
      () => namespaceForRequest(
        COMMAND_CODE.StoreOffset,
        withoutPartition,
        Operation.StoreConsumerOffset
      ),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 6
    );
  });

  it('rejects namespace fields before masking', () => {
    for (const [streamId, topicId, partitionId] of [
      [4096, 0, 0],
      [0, 4096, 0],
      [0, 0, 1_000_000]
    ] as const)
      assert.throws(
        () => packNamespace(streamId, topicId, partitionId),
        (error: unknown) =>
          error instanceof ResponseError && error.errorCode === 6
      );
  });

  it('rejects malformed identifiers at every prefix boundary', () => {
    const payload = serializeSendMessages(
      1,
      2,
      [],
      Partitioning.PartitionId(3)
    );
    for (let length = 0; length < payload.length; length += 1)
      assert.throws(
        () => namespaceForRequest(
          COMMAND_CODE.SendMessages,
          payload.subarray(0, length),
          Operation.SendMessages
        ),
        ResponseError
      );

    const invalidKind = Buffer.from(payload);
    invalidKind.writeUInt8(99, 4);
    assert.throws(
      () => namespaceForRequest(
        COMMAND_CODE.SendMessages,
        invalidKind,
        Operation.SendMessages
      ),
      ResponseError
    );
  });
});
