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

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  Consumer,
  serializeDeleteOffset,
  serializeGetOffset,
  serializeStoreOffset
} from './offset.utils.js';

describe('offset serializers', () => {
  const consumers = [
    {
      name: 'single consumer',
      consumer: Consumer.Single,
      bytes: [0x01, 0x01, 0x04, 0x00, 0x00, 0x00, 0x00]
    },
    {
      name: 'group consumer',
      consumer: Consumer.Group('workers'),
      bytes: [0x02, 0x02, 0x07, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x73]
    }
  ];
  const streamId = 0x12345678;
  const topicId = 'events';
  const partitionId = 0x90abcdef;

  for (const { name, consumer, bytes } of consumers) {
    describe(name, () => {
      // Keep expected wire bytes independent of identifier and offset serializers.
      const prefix = [
        ...bytes,
        0x01, 0x04, 0x78, 0x56, 0x34, 0x12,
        0x02, 0x06, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x73
      ];

      it('serializes get with a partition in little-endian order', () => {
        const expected = Buffer.from([...prefix, 0x01, 0xef, 0xcd, 0xab, 0x90]);
        assert.deepEqual(serializeGetOffset(streamId, topicId, consumer, partitionId), expected);
      });

      it('serializes get with partition zero as present', () => {
        const expected = Buffer.from([...prefix, 0x01, 0x00, 0x00, 0x00, 0x00]);
        assert.deepEqual(serializeGetOffset(streamId, topicId, consumer, 0), expected);
      });

      it('serializes get with a null partition and zero padding', () => {
        const expected = Buffer.from([...prefix, 0x00, 0x00, 0x00, 0x00, 0x00]);
        assert.deepEqual(serializeGetOffset(streamId, topicId, consumer, null), expected);
      });

      it('serializes store with an offset above Number.MAX_SAFE_INTEGER and quorum acknowledgement', () => {
        const expected = Buffer.from([
          ...prefix, 0x01, 0xef, 0xcd, 0xab, 0x90,
          0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
          0x01
        ]);
        assert.deepEqual(
          serializeStoreOffset(streamId, topicId, consumer, partitionId, 0x0123456789abcdefn),
          expected
        );
      });

      it('serializes delete with quorum acknowledgement', () => {
        const expected = Buffer.from([...prefix, 0x01, 0xef, 0xcd, 0xab, 0x90, 0x01]);
        assert.deepEqual(serializeDeleteOffset(streamId, topicId, consumer, partitionId), expected);
      });

      it('rejects store with a null partition', () => {
        assert.throws(
          () => serializeStoreOffset(streamId, topicId, consumer, null, 0n),
          { name: 'Error', message: 'storeOffset error: partitionId must be provided' }
        );
      });

      it('rejects delete with a null partition', () => {
        assert.throws(
          () => serializeDeleteOffset(streamId, topicId, consumer, null),
          { name: 'Error', message: 'deleteOffset error: partitionId must be provided' }
        );
      });
    });
  }
});
