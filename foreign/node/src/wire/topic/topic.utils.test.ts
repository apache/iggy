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

describe('deserializeBaseTopic', () => {
  it('uses the current message-expiry then compression wire layout', () => {
    const data = Buffer.alloc(52);
    data.writeUInt32LE(7, 0);
    data.writeBigUInt64LE(1_710_000_000_000n, 4);
    data.writeUInt32LE(3, 12);
    data.writeBigUInt64LE(86_400_000_000n, 16);
    data.writeUInt8(2, 24);
    data.writeBigUInt64LE(1_000_000n, 25);
    data.writeUInt8(3, 33);
    data.writeBigUInt64LE(4096n, 34);
    data.writeBigUInt64LE(12n, 42);
    data.writeUInt8(1, 50);
    data.write('t', 51);

    const { bytesRead, data: topic } = deserializeBaseTopic(data);

    assert.equal(bytesRead, data.length);
    assert.equal(topic.messageExpiry, 86_400_000_000n);
    assert.equal(topic.compressionAlgorithm, 2);
    assert.equal(topic.maxTopicSize, 1_000_000n);
  });
});
