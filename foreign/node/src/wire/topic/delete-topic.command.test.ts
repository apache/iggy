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
import { DELETE_TOPIC } from './delete-topic.command.js';

describe('DeleteTopic', () => {

  describe('serialize', () => {

    it('serializes numeric stream and topic identifiers into exact bytes', () => {
      const serialized = DELETE_TOPIC.serialize({
        streamId: 5,
        topicId: 10,
        partitionsCount: 3
      });

      const expected = Buffer.from([
        // streamId: numeric (kind 1, length 4, value 5 LE)
        1, 4, 5, 0, 0, 0,
        // topicId: numeric (kind 1, length 4, value 10 LE)
        1, 4, 10, 0, 0, 0,
        // partitionsCount: uint32 LE (3)
        3, 0, 0, 0
      ]);

      assert.deepEqual(serialized, expected);
    });

    it('serializes string stream and topic identifiers into exact bytes', () => {
      const serialized = DELETE_TOPIC.serialize({
        streamId: 'test-stream',
        topicId: 'test-topic',
        partitionsCount: 7
      });

      // "test-stream" = 11 bytes, "test-topic" = 10 bytes
      const expected = Buffer.from([
        // streamId: string (kind 2, length 11, bytes of "test-stream")
        2, 11,
        0x74, 0x65, 0x73, 0x74, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d,
        // topicId: string (kind 2, length 10, bytes of "test-topic")
        2, 10,
        0x74, 0x65, 0x73, 0x74, 0x2d, 0x74, 0x6f, 0x70, 0x69, 0x63,
        // partitionsCount: uint32 LE (7)
        7, 0, 0, 0
      ]);

      assert.deepEqual(serialized, expected);
    });

    it('serializes partitionsCount of 0', () => {
      const serialized = DELETE_TOPIC.serialize({
        streamId: 1,
        topicId: 2,
        partitionsCount: 0
      });

      const expected = Buffer.from([
        // streamId: numeric (kind 1, length 4, value 1 LE)
        1, 4, 1, 0, 0, 0,
        // topicId: numeric (kind 1, length 4, value 2 LE)
        1, 4, 2, 0, 0, 0,
        // partitionsCount: uint32 LE (0)
        0, 0, 0, 0
      ]);

      assert.deepEqual(serialized, expected);
    });

    it('serializes a large partitionsCount (max uint32)', () => {
      const serialized = DELETE_TOPIC.serialize({
        streamId: 1,
        topicId: 1,
        partitionsCount: 0xffffffff
      });

      const expected = Buffer.from([
        // streamId: numeric (kind 1, length 4, value 1 LE)
        1, 4, 1, 0, 0, 0,
        // topicId: numeric (kind 1, length 4, value 1 LE)
        1, 4, 1, 0, 0, 0,
        // partitionsCount: uint32 LE (4294967295)
        0xff, 0xff, 0xff, 0xff
      ]);

      assert.deepEqual(serialized, expected);
    });

  });

});
