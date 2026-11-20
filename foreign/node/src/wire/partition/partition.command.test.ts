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
import { CREATE_PARTITION } from './create-partition.command.js';
import { DELETE_PARTITIONS } from './delete-partition.command.js';

describe('CreatePartition', () => {
  it('uses the CreatePartitions command code', () => {
    assert.equal(CREATE_PARTITION.code, 402);
  });

  describe('serialize', () => {
    it('serializes numeric identifiers with the default count of 1', () => {
      assert.deepEqual(
        CREATE_PARTITION.serialize({ streamId: 5, topicId: 10 }),
        Buffer.from([
          // streamId
          1, 4, 5, 0, 0, 0,
          // topicId
          1, 4, 10, 0, 0, 0,
          // partitionCount
          1, 0, 0, 0
        ])
      );
    });

    it('serializes string identifiers and an explicit count into exact bytes', () => {
      assert.deepEqual(
        CREATE_PARTITION.serialize({
          streamId: 'test-stream',
          topicId: 'test-topic',
          partitionCount: 3
        }),
        Buffer.from([
          // streamId
          2, 11,
          0x74, 0x65, 0x73, 0x74, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d,
          // topicId
          2, 10,
          0x74, 0x65, 0x73, 0x74, 0x2d, 0x74, 0x6f, 0x70, 0x69, 0x63,
          // partitionCount
          3, 0, 0, 0
        ])
      );
    });

    for (const partitionCount of [0, 1001]) {
      it(`rejects a partition count of ${partitionCount}`, () => {
        assert.throws(
          () => CREATE_PARTITION.serialize({ streamId: 5, topicId: 10, partitionCount }),
          /partition_count must be between 1 and 1000/
        );
      });
    }

    it(`accepts a partition count of 1000`, () => {
      assert.deepEqual(
        CREATE_PARTITION.serialize({ streamId: 5, topicId: 10, partitionCount: 1000 }).subarray(-4),
        Buffer.from([0xe8, 0x03, 0, 0])
      );
    });
  });
});

describe('DeletePartitions', () => {
  it('uses the DeletePartitions command code', () => {
    assert.equal(DELETE_PARTITIONS.code, 403);
  });

  describe('serialize', () => {
    it('serializes numeric identifiers and the count into exact bytes', () => {
      assert.deepEqual(
        DELETE_PARTITIONS.serialize({ streamId: 5, topicId: 10, partitionCount: 3 }),
        Buffer.from([
          // streamId
          1, 4, 5, 0, 0, 0,
          // topicId
          1, 4, 10, 0, 0, 0,
          // partitionCount
          3, 0, 0, 0
        ])
      );
    });

    it('serializes string identifiers and the count into exact bytes', () => {
      assert.deepEqual(
        DELETE_PARTITIONS.serialize({
          streamId: 'test-stream',
          topicId: 'test-topic',
          partitionCount: 3
        }),
        Buffer.from([
          // streamId
          2, 11,
          0x74, 0x65, 0x73, 0x74, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d,
          // topicId
          2, 10,
          0x74, 0x65, 0x73, 0x74, 0x2d, 0x74, 0x6f, 0x70, 0x69, 0x63,
          // partitionCount
          3, 0, 0, 0
        ])
      );
    });

    for (const partitionCount of [0, 1001]) {
      it(`rejects a partition count of ${partitionCount}`, () => {
        assert.throws(
          () => DELETE_PARTITIONS.serialize({ streamId: 5, topicId: 10, partitionCount }),
          /partition_count must be between 1 and 1000/
        );
      });
    }

    it(`accepts a partition count of 1000`, () => {
      assert.deepEqual(
        DELETE_PARTITIONS.serialize({ streamId: 5, topicId: 10, partitionCount: 1000 }).subarray(-4),
        Buffer.from([0xe8, 0x03, 0, 0])
      );
    });
  });
});
