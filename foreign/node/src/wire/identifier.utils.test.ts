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
import { serializeIdentifier } from './identifier.utils.js';

describe('serializeIdentifier', () => {

  it('serialize numeric id into buffer', () => {
    assert.deepEqual(
      serializeIdentifier(123).length,
      1 + 1 + 4
    );
  });

  it('serialize numeric string into buffer', () => {
    const strId = 'Groot';
    assert.deepEqual(
      serializeIdentifier(strId).length,
      1 + 1 + strId.length
    );
  });

  it('throw on empty string id', () => {
    assert.throws(
      () => serializeIdentifier(''),
    );
  });

  it('throw on string id > 255 bytes', () => {
    assert.throws(
      () => serializeIdentifier('YoLo'.repeat(65)),
    );
  });

  it('throw on string id > 255 bytes - utf8 version', () => {
    assert.throws(
      () => serializeIdentifier('¥Ø£Ø'.repeat(33)),
    );
  });

});
