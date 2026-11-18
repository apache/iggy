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
import { DeserializeError } from '../error.utils.js';
import { deserializeConsumerGroup } from './group.utils.js';

/** Builds a well-formed consumer group buffer: [id:u32][partitions:u32][members:u32][nameLen:u8][name]. */
const encodeGroup = (id: number, partitionsCount: number, membersCount: number, name: string) => {
  const nameBytes = Buffer.from(name);
  const buf = Buffer.alloc(13 + nameBytes.length);
  buf.writeUInt32LE(id, 0);
  buf.writeUInt32LE(partitionsCount, 4);
  buf.writeUInt32LE(membersCount, 8);
  buf.writeUInt8(nameBytes.length, 12);
  nameBytes.copy(buf, 13);
  return buf;
};

describe('deserializeConsumerGroup', () => {
  it('decodes a valid group', () => {
    assert.deepEqual(deserializeConsumerGroup(encodeGroup(7, 3, 2, 'group-a')), {
      bytesRead: 20,
      data: { id: 7, name: 'group-a', partitionsCount: 3, membersCount: 2 }
    });
  });

  it('decodes a group at a non-zero offset', () => {
    const buf = Buffer.concat([Buffer.alloc(5, 0xff), encodeGroup(7, 3, 2, 'group-a')]);
    const { bytesRead, data } = deserializeConsumerGroup(buf, 5);
    assert.equal(bytesRead, 20);
    assert.deepEqual(data, { id: 7, name: 'group-a', partitionsCount: 3, membersCount: 2 });
  });

  it('rejects an 8-byte buffer with DeserializeError, not RangeError', () => {
    try {
      deserializeConsumerGroup(Buffer.alloc(8));
      assert.fail('expected DeserializeError');
    } catch (error) {
      assert.ok(error instanceof DeserializeError, String(error));
      assert.ok(!(error instanceof RangeError), String(error));
    }
  });

  it('rejects a truncated name with DeserializeError instead of truncating it', () => {
    const full = encodeGroup(7, 3, 2, 'group-a');
    const truncated = full.subarray(0, 16);
    assert.throws(() => deserializeConsumerGroup(truncated), DeserializeError);
  });

  it('rejects a truncated name at a non-zero offset', () => {
    const full = Buffer.concat([Buffer.alloc(5), encodeGroup(7, 3, 2, 'group-a')]);
    const truncated = full.subarray(0, 5 + 16);
    assert.throws(() => deserializeConsumerGroup(truncated, 5), DeserializeError);
  });
});
