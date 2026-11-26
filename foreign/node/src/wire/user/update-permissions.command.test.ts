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
import { COMMAND_CODE } from '../command.code.js';
import { UPDATE_PERMISSIONS } from './update-permissions.command.js';
import { serializePermissions, type UserPermissions } from './permissions.utils.js';

describe('UpdatePermissions', () => {

  const permissions = {
    global: {
      ManageServers: false,
      ReadServers: false,
      ManageUsers: false,
      ReadUsers: false,
      ManageStreams: false,
      ReadStreams: false,
      ManageTopics: false,
      ReadTopics: false,
      PollMessages: false,
      SendMessages: false
    },
    streams: []
  } satisfies UserPermissions;

  const serializedPermissions = serializePermissions(permissions);

  describe('serialize', () => {

    it('serializes a numeric user identifier and permissions', () => {
      const serialized = UPDATE_PERMISSIONS.serialize({ userId: 12345, permissions });
      const expected = Buffer.from([
        1, 4, 0x39, 0x30, 0, 0,
        1,
        11, 0, 0, 0,
        ...serializedPermissions
      ]);

      assert.deepEqual(serialized, expected);
      assert.equal(serialized.readUInt8(6), 1);
      assert.equal(serialized.readUInt32LE(7), serializedPermissions.length);
      assert.deepEqual(serialized.subarray(11), serializedPermissions);
    });

    it('serializes a string user identifier and permissions', () => {
      const serialized = UPDATE_PERMISSIONS.serialize({ userId: 'bob1', permissions });
      const expected = Buffer.from([
        2, 4, 0x62, 0x6f, 0x62, 0x31,
        1,
        11, 0, 0, 0,
        ...serializedPermissions
      ]);

      assert.deepEqual(serialized, expected);
      assert.equal(serialized.readUInt8(6), 1);
      assert.equal(serialized.readUInt32LE(7), serializedPermissions.length);
      assert.deepEqual(serialized.subarray(11), serializedPermissions);
    });

  });

  it('uses the update permissions command code', () => {
    assert.equal(UPDATE_PERMISSIONS.code, COMMAND_CODE.UpdatePermissions);
  });

});