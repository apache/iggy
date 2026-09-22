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
import { Durability } from './topic.utils.js';
import { CREATE_TOPIC } from './create-topic.command.js';
import { deserializeOptions } from '../options.utils.js';
import { HeaderValue } from '../message/header.utils.js';

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
    const defaultPolicySize = tlvSize('durability'.length) + tlvSize('replicated'.length)
      + tlvSize('consumer_offset_durability'.length) + tlvSize('replicated'.length);
    const fixedSize = identifierSize + 4 + 1; // + partitions_count + name_len

    it('serialize name and default options into buffer', () => {
      // Durability defaults are explicit, while the other sentinels are omitted.
      assert.deepEqual(
        CREATE_TOPIC.serialize(t1).length,
        fixedSize + t1.name.length + defaultPolicySize
      );
    });

    it('serialize partitionCount as a fixed u32 before the name', () => {
      const t = { ...t1, partitionCount: 7 };
      const b = CREATE_TOPIC.serialize(t);
      assert.equal(b.readUInt32LE(identifierSize), 7);
      assert.equal(b.readUInt8(identifierSize + 4), t.name.length);
      assert.equal(b.subarray(fixedSize, fixedSize + t.name.length).toString(), t.name);
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
        fixedSize + t1.name.length + defaultPolicySize
        + tlvSize('compression_algorithm'.length) + tlvSize('gzip'.length)
        + tlvSize('message_expiry'.length) + tlvSize(8)
        + tlvSize('max_topic_size'.length) + tlvSize(8)
      );
    });

    it('serialize segment and save-trigger options into buffer', () => {
      const t = {
        ...t1,
        segmentSize: 1048576n,
        durability: Durability.Persisted,
        messagesRequiredToSave: 1000,
        sizeOfMessagesRequiredToSave: 4096n,
        preallocateSegments: false
      };
      assert.deepEqual(
        CREATE_TOPIC.serialize(t).length,
        fixedSize + t1.name.length + defaultPolicySize
        + tlvSize('segment_size'.length) + tlvSize(8)
        + 'persisted'.length - 'replicated'.length
        + tlvSize('messages_required_to_save'.length) + tlvSize(4)
        + tlvSize('size_of_messages_required_to_save'.length) + tlvSize(8)
        + tlvSize('preallocate_segments'.length) + tlvSize(1)
      );
    });

    it('serialize caller-supplied option keys, typed fields winning', () => {
      const t = {
        ...t1,
        maxTopicSize: 4096n,
        options: [
          { key: 'preallocate_segments', value: HeaderValue.Bool(true) },
          // The typed field covers this key, so the caller's entry is dropped:
          // a duplicate key makes the server refuse the whole block.
          { key: 'max_topic_size', value: HeaderValue.String('1 GiB') }
        ]
      };

      const b = CREATE_TOPIC.serialize(t);
      // The create payload runs its options block to the end, unprefixed.
      const options = deserializeOptions(b, fixedSize + t.name.length);

      assert.deepEqual(Object.keys(options).sort(), ['consumer_offset_durability', 'durability', 'max_topic_size', 'preallocate_segments']);
      assert.equal(options.preallocate_segments, true);
      assert.equal(options.max_topic_size, 4096n);
    });

    it('keeps each omitted durability policy replicated', () => {
      for (const selected of [
        { durability: Durability.Persisted },
        { consumerOffsetDurability: Durability.Persisted }
      ]) {
        const input = { ...t1, ...selected };
        const encoded = CREATE_TOPIC.serialize(input);
        const options = deserializeOptions(encoded, fixedSize + input.name.length);
        assert.equal(options.durability, selected.durability ?? 'replicated');
        assert.equal(options.consumer_offset_durability, selected.consumerOffsetDurability ?? 'replicated');
      }
    });

    it('rejects a conflicting raw durability instead of weakening it', () => {
      assert.throws(() => CREATE_TOPIC.serialize({ ...t1, options: [
        { key: 'durability', value: HeaderValue.String('persisted') }
      ] }));
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
