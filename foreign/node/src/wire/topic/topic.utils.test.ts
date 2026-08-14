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
import { deserializeBaseTopic } from './topic.utils.js';
import { serializeOptions } from '../options.utils.js';
import { HeaderValue } from '../message/header.utils.js';

describe('deserializeBaseTopic', () => {
  it('uses the 50-byte fixed layout with two options blocks', () => {
    const fixed = Buffer.alloc(51);
    fixed.writeUInt32LE(7, 0);
    fixed.writeBigUInt64LE(1_710_000_000_000n, 4);
    fixed.writeUInt32LE(3, 12);
    fixed.writeBigUInt64LE(86_400_000_000n, 16);
    fixed.writeUInt8(2, 24);
    fixed.writeBigUInt64LE(1_000_000n, 25);
    fixed.writeBigUInt64LE(4096n, 33);
    fixed.writeBigUInt64LE(12n, 41);
    fixed.writeUInt8(1, 49);
    fixed.write('t', 50);

    const explicit = serializeOptions([
      { key: 'message_expiry', value: HeaderValue.Uint64(86_400_000_000n) }
    ]);
    const derived = serializeOptions([
      { key: 'compression_algorithm', value: HeaderValue.String('gzip') },
      { key: 'max_topic_size', value: HeaderValue.Uint64(1_000_000n) }
    ]);
    const prefix = (block: Buffer) => {
      const len = Buffer.alloc(4);
      len.writeUInt32LE(block.length, 0);
      return Buffer.concat([len, block]);
    };
    const data = Buffer.concat([fixed, prefix(explicit), prefix(derived)]);

    const { bytesRead, data: topic } = deserializeBaseTopic(data);

    assert.equal(bytesRead, data.length);
    assert.equal(topic.id, 7);
    assert.equal(topic.partitionsCount, 3);
    assert.equal(topic.messageExpiry, 86_400_000_000n);
    assert.equal(topic.compressionAlgorithm, 2);
    assert.equal(topic.maxTopicSize, 1_000_000n);
    assert.equal(topic.sizeBytes, 4096n);
    assert.equal(topic.messagesCount, 12n);
    assert.equal(topic.name, 't');
    assert.deepEqual(topic.options, { message_expiry: 86_400_000_000n });
    assert.deepEqual(topic.derivedOptions, {
      compression_algorithm: 'gzip',
      max_topic_size: 1_000_000n
    });
  });

  it('skips empty options blocks', () => {
    const fixed = Buffer.alloc(51);
    fixed.writeUInt32LE(1, 0);
    fixed.writeUInt32LE(2, 12);
    fixed.writeUInt8(1, 49);
    fixed.write('t', 50);
    const data = Buffer.concat([fixed, Buffer.alloc(8)]);

    const { bytesRead, data: topic } = deserializeBaseTopic(data);

    assert.equal(bytesRead, data.length);
    assert.equal(topic.partitionsCount, 2);
    assert.deepEqual(topic.options, {});
    assert.deepEqual(topic.derivedOptions, {});
  });
});
