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
import { CREATE_TOKEN } from './create-token.command.js';

describe('CreateToken', () => {

  describe('serialize', () => {

    const t1 = {
      name: 'test-token',
      expiry: 1234n
    };

    it('serialize 1 name & 1 uint32 into buffer', () => {

      assert.deepEqual(
        CREATE_TOKEN.serialize(t1).length,
        8 + 1 + t1.name.length
      );
    });

    it('throw on name < 1', () => {
      const t2 = { ...t1, name: ''};
      assert.throws(
        () => CREATE_TOKEN.serialize(t2)
      );
    });

    it("throw on name > 255 bytes", () => {
      const t2 = { ...t1, name: "YoLo".repeat(65)};
      assert.throws(
        () => CREATE_TOKEN.serialize(t2)
      );
    });

    it("throw on name > 255 bytes - utf8 version", () => {
      const t2 = { ...t1, name: "¥Ø£Ø".repeat(33)};
      assert.throws(
        () => CREATE_TOKEN.serialize(t2)
      );
    });

    it('writes the expiry as microseconds in a u64 right after the name', () => {
      assert.deepEqual(
        CREATE_TOKEN.serialize({ name: 'token', expiry: 0x0102030405060708n }),
        Buffer.from([5, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 8, 7, 6, 5, 4, 3, 2, 1])
      );
    });

    it('defaults the expiry to ten minutes in microseconds', () => {
      assert.equal(
        CREATE_TOKEN.serialize({ name: 'token' }).readBigUInt64LE(6),
        600_000_000n
      );
    });

  });
});
