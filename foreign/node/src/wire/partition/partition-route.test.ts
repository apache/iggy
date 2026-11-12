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
import { deserializePartitionRoute, serializePartitionRoute } from './partition-route.js';
import { DeserializeError } from '../error.utils.js';
import { serializeIdentifier } from '../identifier.utils.js';

describe('partition route codec', () => {
  it('decodes the route fields without interpreting view and count as metadata', () => {
    const route = Buffer.from(
      '08070605040302011817161514131211242322213433323107', 'hex');
    assert.deepEqual(deserializePartitionRoute(route), {
      context: { incarnation: 0x0102030405060708n, ownerGeneration: 0n,
        metadataOp: 0x1112131415161718n },
      view: 0x21222324, partitionsCount: 0x31323334, primaryReplica: 7
    });
  });

  for (const length of [0, 24, 26]) {
    it(`rejects a ${length}-byte route`, () => {
      assert.throws(() => deserializePartitionRoute(Buffer.alloc(length)), DeserializeError);
    });
  }

  it('serializes both identifier forms and the concrete partition', () => {
    assert.deepEqual(serializePartitionRoute('stream', 9, 7), Buffer.concat([
      serializeIdentifier('stream'), serializeIdentifier(9), Buffer.from([7, 0, 0, 0])
    ]));
  });
});
