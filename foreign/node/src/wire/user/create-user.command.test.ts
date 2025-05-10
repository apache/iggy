/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CREATE_USER } from './create-user.command.js';

describe('CreateUser', () => {

  describe('serialize', () => {

    const u1 = {
      id: 1,
      username: 'test-user',
      password: 'test-pwd',
      status: 1, // Active,
      // perms: undefined // @TODO
    };

    it('serialize username, password, status, permissions into buffer', () => {
      assert.deepEqual(
        CREATE_USER.serialize(u1).length,
        1 + u1.username.length + 1 + u1.password.length + 1 + 1 + 4 + 1
      );
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

