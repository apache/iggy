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
import { SYNC_GROUP } from './sync-group.command.js';

describe('SyncGroup', () => {
  it('serializes group identifiers', () => {
    assert.equal(
      SYNC_GROUP.serialize({ streamId: 5, topicId: 2, groupId: 1 }).length,
      18,
    );
  });

  it('deserializes an assignment', () => {
    const data = Buffer.alloc(20);
    data.writeBigUInt64LE(7n, 0);
    data.writeUInt32LE(2, 8);
    data.writeUInt32LE(1, 12);
    data.writeUInt32LE(4, 16);
    assert.deepEqual(
      SYNC_GROUP.deserialize({ status: 0, length: data.length, data }),
      { generation: 7n, partitions: [1, 4] },
    );
  });

  it('maps an empty response to no assignment', () => {
    assert.equal(
      SYNC_GROUP.deserialize({ status: 0, length: 0, data: Buffer.alloc(0) }),
      null,
    );
  });

  it('rejects a truncated fixed header', () => {
    assert.throws(
      () => SYNC_GROUP.deserialize({
        status: 0,
        length: 11,
        data: Buffer.alloc(11),
      }),
      /assignment is truncated/,
    );
  });

  it('rejects a truncated partition list', () => {
    const data = Buffer.alloc(16);
    data.writeUInt32LE(2, 8);
    assert.throws(
      () => SYNC_GROUP.deserialize({
        status: 0,
        length: data.length,
        data,
      }),
      /partition list is truncated/,
    );
  });

  it('rejects an impossible partition count', () => {
    const data = Buffer.alloc(16);
    data.writeUInt32LE(0xFFFF_FFFF, 8);
    assert.throws(
      () => SYNC_GROUP.deserialize({
        status: 0,
        length: data.length,
        data,
      }),
      /partition list is truncated/,
    );
  });

  it('rejects trailing bytes', () => {
    const data = Buffer.alloc(17);
    data.writeUInt32LE(1, 8);
    assert.throws(
      () => SYNC_GROUP.deserialize({
        status: 0,
        length: data.length,
        data,
      }),
      /trailing bytes/,
    );
  });
});
