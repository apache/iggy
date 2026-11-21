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
import { DELETE_SEGMENTS } from './delete-segments.command.js';

describe('DeleteSegments', () => {

  describe('serialize', () => {

    it('serializes numeric stream and topic identifiers', () => {
      const serialized = DELETE_SEGMENTS.serialize({
        streamId: 1,
        topicId: 300,
        partitionId: 2,
        segmentsCount: 5
      });

      const expected = Buffer.concat([
        Buffer.from([1, 4, 1, 0, 0, 0]),       // stream id: numeric kind, 4 bytes, 1 (u32 LE)
        Buffer.from([1, 4, 0x2c, 0x01, 0, 0]), // topic id: numeric kind, 4 bytes, 300 (u32 LE)
        Buffer.from([2, 0, 0, 0]),             // partition id (u32 LE)
        Buffer.from([5, 0, 0, 0])              // segments count (u32 LE)
      ]);

      assert.deepEqual(serialized, expected);
    });

    it('serializes string stream and topic identifiers', () => {
      const serialized = DELETE_SEGMENTS.serialize({
        streamId: 'orders',
        topicId: 'events',
        partitionId: 1,
        segmentsCount: 10
      });

      const expected = Buffer.concat([
        Buffer.from([2, 6]),           // stream id: string kind, 6 bytes
        Buffer.from('orders'),
        Buffer.from([2, 6]),           // topic id: string kind, 6 bytes
        Buffer.from('events'),
        Buffer.from([1, 0, 0, 0]),     // partition id (u32 LE)
        Buffer.from([10, 0, 0, 0])     // segments count (u32 LE)
      ]);

      assert.deepEqual(serialized, expected);
    });

    const params = { streamId: 1, topicId: 2, partitionId: 3 };

    it('accepts a segments count of 1', () => {
      const serialized = DELETE_SEGMENTS.serialize({ ...params, segmentsCount: 1 });
      assert.deepEqual(serialized.subarray(-4), Buffer.from([1, 0, 0, 0]));
    });

    it('accepts a segments count of 1000', () => {
      const serialized = DELETE_SEGMENTS.serialize({ ...params, segmentsCount: 1000 });
      assert.deepEqual(serialized.subarray(-4), Buffer.from([0xe8, 0x03, 0, 0]));
    });

    it('throws on segments count < 1', () => {
      assert.throws(
        () => DELETE_SEGMENTS.serialize({ ...params, segmentsCount: 0 })
      );
    });

    it('throws on segments count > 1000', () => {
      assert.throws(
        () => DELETE_SEGMENTS.serialize({ ...params, segmentsCount: 1001 })
      );
    });

  });
});
