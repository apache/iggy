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
import { CREATE_GROUP } from './create-group.command.js';

describe('CreateGroup', () => {

  describe('serialize', () => {

    const s1 = {
      streamId: 5,
      topicId: 2,
      name: 'test-group'
    };

    it('serialize group id & name into buffer', () => {
      assert.deepEqual(
        CREATE_GROUP.serialize(s1).length,
        6 + 6 + 1 + s1.name.length
      );
    });

    it('throw on name < 1', () => {
      const s2 = {...s1, name: ''};
      assert.throws(
        () => CREATE_GROUP.serialize(s2)
      );
    });

    it("throw on name > 255 bytes", () => {
      const s2 = { ...s1, name: "YoLo".repeat(65)};
      assert.throws(
        () => CREATE_GROUP.serialize(s2)
      );
    });

    it("throw on name > 255 bytes - utf8 version", () => {
      const s2 = { ...s1, name: "¥Ø£Ø".repeat(33)};
      assert.throws(
        () => CREATE_GROUP.serialize(s2)
      );
    });

  });
});
