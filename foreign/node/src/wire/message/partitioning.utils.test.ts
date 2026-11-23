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
  type MessageKeyValue
} from './partitioning.utils.js';

describe('Partitioning', () => {
  describe('serializePartitioning', () => {
    it('defaults to balanced partitioning with an empty value', () => {
      assert.deepEqual(serializePartitioning(), Buffer.from([1, 0]));
    });

    it('serializes explicitly balanced partitioning', () => {
      assert.deepEqual(serializePartitioning(Partitioning.Balanced), Buffer.from([1, 0]));
    });

    it('serializes a partition ID as a little-endian uint32', () => {
      assert.deepEqual(serializePartitioning(Partitioning.PartitionId(7)), Buffer.from([
        2, 4, 7, 0, 0, 0
      ]));
    });

    it('serializes a string message key', () => {
      assert.deepEqual(serializePartitioning(Partitioning.MessageKey('ab')), Buffer.from([
        3, 2, 97, 98
      ]));
    });

    it('encodes non-ASCII message keys as UTF-8 bytes', () => {
      assert.deepEqual(serializePartitioning(Partitioning.MessageKey('é')), Buffer.from([
        3, 2, 0xc3, 0xa9
      ]));
    });

    it('serializes a numeric message key as a little-endian uint32', () => {
      assert.deepEqual(serializePartitioning(Partitioning.MessageKey(0x12345678)), Buffer.from([
        3, 4, 0x78, 0x56, 0x34, 0x12
      ]));
    });

    it('serializes a bigint message key as a little-endian uint64', () => {
      assert.deepEqual(serializePartitioning(Partitioning.MessageKey(0x0102030405060708n)), Buffer.from([
        3, 8, 8, 7, 6, 5, 4, 3, 2, 1
      ]));
    });

    it('serializes a Buffer message key without changing its bytes', () => {
      assert.deepEqual(serializePartitioning(Partitioning.MessageKey(Buffer.from([0, 255, 128]))), Buffer.from([
        3, 3, 0, 255, 128
      ]));
    });
  });

  describe('serializePartitioningValue', () => {
    it('returns an empty Buffer for balanced partitioning', () => {
      assert.deepEqual(serializePartitioningValue(Partitioning.Balanced), Buffer.alloc(0));
    });

    it('returns the four partition ID bytes without a header', () => {
      assert.deepEqual(serializePartitioningValue(Partitioning.PartitionId(0x12345678)), Buffer.from([
        0x78, 0x56, 0x34, 0x12
      ]));
    });

    it('returns message key bytes without a header', () => {
      assert.deepEqual(serializePartitioningValue(Partitioning.MessageKey('ab')), Buffer.from([
        97, 98
      ]));
    });
  });

  describe('serializeMessageKey', () => {
    it('serializes a number into four little-endian bytes', () => {
      assert.deepEqual(serializeMessageKey(0x12345678), Buffer.from([
        0x78, 0x56, 0x34, 0x12
      ]));
    });

    it('serializes a bigint into eight little-endian bytes', () => {
      assert.deepEqual(serializeMessageKey(0x0102030405060708n), Buffer.from([
        8, 7, 6, 5, 4, 3, 2, 1
      ]));
    });

    it('returns Buffer keys unchanged', () => {
      const key = Buffer.from([0, 255, 128]);
      assert.strictEqual(serializeMessageKey(key), key);
    });

    it('rejects unsupported key types', () => {
      assert.throws(
        () => serializeMessageKey({} as MessageKeyValue),
        /cannot serialize messageKey/
      );
    });
  });
});
