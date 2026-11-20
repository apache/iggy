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
import { DELETE_STREAM } from './delete-stream.command.js';

describe('DeleteStream', () => {

  it('uses the delete stream command code', () => {
    assert.equal(DELETE_STREAM.code, COMMAND_CODE.DeleteStream);
  });

  describe('serialize', () => {

    it('serializes a numeric stream id into exact bytes', () => {
      const serialized = DELETE_STREAM.serialize({
        streamId: 0x04030201
      });

      const expected = Buffer.from([
        // numeric (kind 1, length 4, value 0x04030201 LE)
        1, 4, 0x01, 0x02, 0x03, 0x04
      ]);

      assert.deepEqual(serialized, expected);
    });

    it('serializes a string stream id into exact bytes', () => {
      const serialized = DELETE_STREAM.serialize({
        streamId: 'test-stream'
      });

      // "test-stream" = 11 bytes
      const expected = Buffer.from([
        // string (kind 2, length 11, bytes of "test-stream")
        2, 11,
        0x74, 0x65, 0x73, 0x74, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d
      ]);

      assert.deepEqual(serialized, expected);
    });

  });

});
