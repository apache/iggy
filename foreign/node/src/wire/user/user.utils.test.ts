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
import { deserializeBaseUser, deserializeUser } from './user.utils.js';
import { serializePermissions, type UserPermissions } from './permissions.utils.js';

describe('deserializeBaseUser', () => {
  for (const [statusCode, status] of [
    [1, 'Active'], [2, 'Inactive'], [9, 'unknown_status_9']
  ] as const) {
    it(`reads status ${statusCode} as ${status} and the bytes consumed`, () => {
      const record = buildUserRecord(statusCode);
      const { data, bytesRead } = deserializeBaseUser(record);

      assert.deepEqual(data, {
        id: 42,
        createdAt: new Date('2025-01-01T00:00:00.000Z'),
        status,
        userName: 'test-user',
        options: {},
        derivedOptions: {}
      });
      assert.equal(bytesRead, record.length);
    });
  }

  it('throws for an empty buffer', () => {
    assert.throws(() => deserializeBaseUser(Buffer.alloc(0)), {
      message: 'User does not exist'
    });
  });
});

describe('deserializeUser', () => {
  it('returns null permissions when the flag is zero', () => {
    const record = Buffer.concat([buildUserRecord(1), Buffer.alloc(4)]);
    assert.deepEqual(deserializeUser(record), {
      id: 42,
      createdAt: new Date('2025-01-01T00:00:00.000Z'),
      status: 'Active',
      userName: 'test-user',
      options: {},
      derivedOptions: {},
      permissions: null
    });
  });

  it('reads permissions when the flag is one', () => {
    const permissions: UserPermissions = {
      global: {
        ManageServers: false,
        ReadServers: true,
        ManageUsers: false,
        ReadUsers: true,
        ManageStreams: false,
        ReadStreams: true,
        ManageTopics: false,
        ReadTopics: true,
        PollMessages: true,
        SendMessages: false
      },
      streams: []
    };
    const block = serializePermissions(permissions);
    const prefix = Buffer.alloc(5);
    prefix.writeUInt8(1, 0);
    prefix.writeUInt32LE(block.length, 1);
    const record = Buffer.concat([buildUserRecord(1), prefix, block]);

    assert.deepEqual(deserializeUser(record), {
      id: 42,
      createdAt: new Date('2025-01-01T00:00:00.000Z'),
      status: 'Active',
      userName: 'test-user',
      options: {},
      derivedOptions: {},
      permissions
    });
  });

  it('throws for an empty buffer', () => {
    assert.throws(() => deserializeUser(Buffer.alloc(0)), {
      message: 'User does not exist'
    });
  });
});

const buildUserRecord = (status: number): Buffer => {
  const userName = Buffer.from('test-user');
  // Each empty options block still carries a four-byte length prefix.
  const record = Buffer.alloc(14 + userName.length + 4 + 4);
  record.writeUInt32LE(42, 0);
  record.writeBigUInt64LE(1735689600000000n, 4);
  record.writeUInt8(status, 12);
  record.writeUInt8(userName.length, 13);
  userName.copy(record, 14);
  return record;
};
