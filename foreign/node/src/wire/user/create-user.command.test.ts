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
import { CREATE_USER } from './create-user.command.js';
import type { UserPermissions } from './permissions.utils.js';

describe('CreateUser', () => {

  describe('serialize', () => {

    const u1 = {
      username: 'test-user',
      password: 'test-pwd',
      status: 1, // Active,
    };

    const baseLength = 1 + Buffer.byteLength(u1.username)
      + 1 + Buffer.byteLength(u1.password) + 1;

    it('serialize without permissions', () => {
      const serialized = CREATE_USER.serialize(u1);
      assert.equal(serialized.length, baseLength + 1);
      assert.equal(serialized.readUInt8(baseLength), 0);
    });

    it('serialize with permissions', () => {
      const permissions: UserPermissions = {
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
      };
      const serialized = CREATE_USER.serialize({ ...u1, permissions });

      assert.equal(serialized.length, baseLength + 1 + 4 + 11);
      assert.equal(serialized.readUInt8(baseLength), 1);
      assert.equal(serialized.readUInt32LE(baseLength + 1), 11);
      assert.deepEqual(serialized.subarray(baseLength + 5), Buffer.alloc(11));
    });

    it('throw on username < 1', () => {
      const u2 = { ...u1, username: '' };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

    it('throw on username > 255 bytes', () => {
      const u2 = { ...u1, username: "YoLo".repeat(65) };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

    it('throw on username > 255 bytes - utf8 version', () => {
      const u2 = { ...u1, username: "¥Ø£Ø".repeat(33) };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

    it('throw on password < 1', () => {
      const u2 = { ...u1, password: '' };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

    it('throw on password > 255 bytes', () => {
      const u2 = { ...u1, password: "yolo".repeat(65) };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

    it('throw on password > 255 bytes - utf8 version', () => {
      const u2 = { ...u1, password: "¥Ø£Ø".repeat(33) };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

  });
});
