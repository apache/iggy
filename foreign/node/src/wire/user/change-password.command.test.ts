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
import { CHANGE_PASSWORD } from './change-password.command.js';

describe('ChangePassword', () => {

  describe('serialize', () => {

    const input = {
      userId: 42,
      currentPassword: 'old',
      newPassword: 'new-pwd',
    };

    const currentPasswordError = /between 1 and 255 bytes \(current\)/;
    const newPasswordError = /between 1 and 255 bytes \(new\)/;

    it('serialize numeric user id and both passwords', () => {
      const expected = Buffer.from([
        1, 4, 42, 0, 0, 0,
        3, 0x6f, 0x6c, 0x64,
        7, 0x6e, 0x65, 0x77, 0x2d, 0x70, 0x77, 0x64,
      ]);
      assert.deepEqual(CHANGE_PASSWORD.serialize(input), expected);
    });

    it('throw on empty current password', () => {
      assert.throws(
        () => CHANGE_PASSWORD.serialize({ ...input, currentPassword: '' }),
        currentPasswordError
      );
    });

    it('throw on empty new password', () => {
      assert.throws(
        () => CHANGE_PASSWORD.serialize({ ...input, newPassword: '' }),
        newPasswordError
      );
    });

    it('accept 255 byte password', () => {
      const password = 'a'.repeat(255);
      const expected = Buffer.concat([
        Buffer.from([1, 4, 42, 0, 0, 0]),
        Buffer.from([255]), Buffer.alloc(255, 0x61),
        Buffer.from([255]), Buffer.alloc(255, 0x61),
      ]);
      const serialized = CHANGE_PASSWORD.serialize({
        ...input, currentPassword: password, newPassword: password
      });
      assert.deepEqual(serialized, expected);
    });

    it('throw on current password > 255 bytes', () => {
      assert.throws(
        () => CHANGE_PASSWORD.serialize({ ...input, currentPassword: 'a'.repeat(256) }),
        currentPasswordError
      );
    });

    it('throw on new password > 255 bytes', () => {
      assert.throws(
        () => CHANGE_PASSWORD.serialize({ ...input, newPassword: 'a'.repeat(256) }),
        newPasswordError
      );
    });

    it('throw on new password > 255 bytes - utf8 version', () => {
      const password = '¥Ø£Ø'.repeat(33);
      assert.throws(
        () => CHANGE_PASSWORD.serialize({ ...input, newPassword: password }),
        newPasswordError
      );
    });

  });
});
