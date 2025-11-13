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
import { CREATE_TOPIC } from './create-topic.command.js';

describe('CreateTopic', () => {

  describe('serialize', () => {

    const t1 = {
      streamId: 1,
      name: 'test-topic',
      partitionCount: 1,
      compressionAlgorithm: 1, // 1 = None, 2 = Gzip
      messageExpiry: 0n,
      maxTopicSize: 0n,
      replicationFactor: 1
    };

    it('serialize 1 name into buffer', () => {
      assert.deepEqual(
        CREATE_TOPIC.serialize(t1).length,
        6  + 4 + 1 + 8 + 8 + 1 + 1 + t1.name.length
      );
    });

    it('throw on name < 1', () => {
      const t = { ...t1, name: '' };
      assert.throws(
        () => CREATE_TOPIC.serialize(t)
      );
    });

    it("throw on name > 255 bytes", () => {
      const t = { ...t1, name: "YoLo".repeat(65)};
      assert.throws(
        () => CREATE_TOPIC.serialize(t)
      );
    });

    it("throw on name > 255 bytes - utf8 version", () => {
      const t = { ...t1, name: "¥Ø£Ø".repeat(33) };
      assert.throws(
        () => CREATE_TOPIC.serialize(t)
      );
    });

    it('throw on replication_factor < 1', () => {
      const t = { ...t1, replicationFactor: 0 };
      assert.throws(
        () => CREATE_TOPIC.serialize(t),
      );
    });

    it('throw on replication_factor > 255', () => {
      const t = { ...t1, replicationFactor: 257 };
      assert.throws(
        () => CREATE_TOPIC.serialize(t),
      );
    });

    it('accept compressionAlgorithm = 2 (gzip)', () => {
      const t = { ...t1, compressionAlgorithm: 2 };
      assert.doesNotThrow(
        () => CREATE_TOPIC.serialize(t),
      );
    });

    it('throw on invalid compressionAlgorithm', () => {
      const t = { ...t1, compressionAlgorithm: 42 };
      assert.throws(
        () => CREATE_TOPIC.serialize(t),
      );
    });

  });
});
