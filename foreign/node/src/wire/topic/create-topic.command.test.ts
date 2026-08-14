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
import { CREATE_TOPIC } from './create-topic.command.js';

describe('CreateTopic', () => {

  describe('serialize', () => {

    const t1 = {
      streamId: 1,
      name: 'test-topic',
      partitionCount: 1,
      compressionAlgorithm: 1, // 1 = None, 2 = Gzip
      messageExpiry: 0n,
      maxTopicSize: 0n
    };

    // TLV field: [kind:u8][len:u32_le][bytes]
    const tlvSize = (bytes: number) => 1 + 4 + bytes;
    const identifierSize = 1 + 1 + 4; // numeric stream id
    const fixedSize = identifierSize + 4 + 1; // + partitions_count + name_len

    it('serialize name and default options into buffer', () => {
      // Server-default sentinels are omitted, leaving an empty options block.
      assert.deepEqual(
        CREATE_TOPIC.serialize(t1).length,
        fixedSize + t1.name.length
      );
    });

    it('serialize partitionCount as a fixed u32 before the name', () => {
      const t = { ...t1, partitionCount: 7 };
      const b = CREATE_TOPIC.serialize(t);
      assert.equal(b.readUInt32LE(identifierSize), 7);
      assert.equal(b.readUInt8(identifierSize + 4), t.name.length);
      assert.equal(b.subarray(fixedSize).toString(), t.name);
    });

    it('serialize non-default options into buffer', () => {
      const t = {
        ...t1,
        compressionAlgorithm: 2,
        messageExpiry: 42n,
        maxTopicSize: 1024n
      };
      assert.deepEqual(
        CREATE_TOPIC.serialize(t).length,
        fixedSize + t1.name.length
        + tlvSize('compression_algorithm'.length) + tlvSize('gzip'.length)
        + tlvSize('message_expiry'.length) + tlvSize(8)
        + tlvSize('max_topic_size'.length) + tlvSize(8)
      );
    });

    it('serialize segment and save-trigger options into buffer', () => {
      const t = {
        ...t1,
        segmentSize: 1048576n,
        enforceFsync: true,
        messagesRequiredToSave: 1000,
        sizeOfMessagesRequiredToSave: 4096n,
        preallocateSegments: false
      };
      assert.deepEqual(
        CREATE_TOPIC.serialize(t).length,
        fixedSize + t1.name.length
        + tlvSize('segment_size'.length) + tlvSize(8)
        + tlvSize('enforce_fsync'.length) + tlvSize(1)
        + tlvSize('messages_required_to_save'.length) + tlvSize(4)
        + tlvSize('size_of_messages_required_to_save'.length) + tlvSize(8)
        + tlvSize('preallocate_segments'.length) + tlvSize(1)
      );
    });

    it('serialize replicationFactor as an option', () => {
      // 1 is an explicit value, not a sentinel: only an unset field lets the
      // server resolve its own default.
      const t = { ...t1, replicationFactor: 1 };
      assert.deepEqual(
        CREATE_TOPIC.serialize(t).length,
        fixedSize + t1.name.length
        + tlvSize('replication_factor'.length) + tlvSize(1)
      );
    });

    it('throw on name < 1', () => {
      const t = { ...t1, name: '' };
      assert.throws(
        () => CREATE_TOPIC.serialize(t)
      );
    });

    it("throw on name > 255 bytes", () => {
      const t = { ...t1, name: "YoLo".repeat(65)};
      assert.throws(
        () => CREATE_TOPIC.serialize(t)
      );
    });

    it("throw on name > 255 bytes - utf8 version", () => {
      const t = { ...t1, name: "¥Ø£Ø".repeat(33) };
      assert.throws(
        () => CREATE_TOPIC.serialize(t)
      );
    });

    it('accept compressionAlgorithm = 2 (gzip)', () => {
      const t = { ...t1, compressionAlgorithm: 2 };
      assert.doesNotThrow(
        () => CREATE_TOPIC.serialize(t),
      );
    });

    it('throw on invalid compressionAlgorithm', () => {
      const t = { ...t1, compressionAlgorithm: 42 };
      assert.throws(
        () => CREATE_TOPIC.serialize(t),
      );
    });

  });
});
