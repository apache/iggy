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
import {
  Partitioning,
  serializeMessageKey,
  serializePartitioning,
  serializePartitioningValue,
} from './partitioning.utils.js';

describe('partitioning utils', () => {

  describe('serializeMessageKey', () => {

    it('writes a string key as its UTF-8 bytes', () => {
      assert.deepEqual(serializeMessageKey('abc'), Buffer.from([0x61, 0x62, 0x63]));
    });

    it('writes a number key as a little-endian uint32', () => {
      assert.deepEqual(serializeMessageKey(5), Buffer.from([5, 0, 0, 0]));
    });

    it('writes a bigint key as a little-endian uint64', () => {
      assert.deepEqual(
        serializeMessageKey(5n),
        Buffer.from([5, 0, 0, 0, 0, 0, 0, 0])
      );
    });

    it('passes a Buffer key through unchanged', () => {
      const key = Buffer.from([1, 2, 3]);
      assert.equal(serializeMessageKey(key), key);
    });

    it('throws for a key type the wire cannot express', () => {
      assert.throws(
        () => serializeMessageKey(true as unknown as string),
        /cannot serialize messageKey/
      );
    });

  });

  describe('serializePartitioningValue', () => {

    it('writes no value for balanced partitioning', () => {
      assert.deepEqual(serializePartitioningValue(Partitioning.Balanced), Buffer.alloc(0));
    });

    it('writes a partition id as a little-endian uint32', () => {
      assert.deepEqual(
        serializePartitioningValue(Partitioning.PartitionId(7)),
        Buffer.from([7, 0, 0, 0])
      );
    });

    it('writes a message key through serializeMessageKey', () => {
      assert.deepEqual(
        serializePartitioningValue(Partitioning.MessageKey('hi')),
        Buffer.from('hi')
      );
    });

  });

  describe('serializePartitioning', () => {

    it('defaults to balanced partitioning', () => {
      assert.deepEqual(serializePartitioning(), Buffer.from([1, 0]));
    });

    it('writes the kind, the value length and the value', () => {
      assert.deepEqual(
        serializePartitioning(Partitioning.PartitionId(5)),
        Buffer.from([2, 4, 5, 0, 0, 0])
      );
      assert.deepEqual(
        serializePartitioning(Partitioning.MessageKey('hi')),
        Buffer.from([3, 2, 0x68, 0x69])
      );
    });

  });

});
