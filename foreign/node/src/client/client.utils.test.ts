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
import { deserializeVoidResponse } from './client.utils.js';

describe('deserializeVoidResponse', () => {

  it('returns true only for a success status with an empty payload', () => {
    const success = { status: 0, length: 0, data: Buffer.alloc(0) };
    assert.equal(deserializeVoidResponse(success), true);

    const failed = { status: 1, length: 0, data: Buffer.alloc(0) };
    assert.equal(deserializeVoidResponse(failed), false);

    const withData = { status: 0, length: 4, data: Buffer.alloc(4) };
    assert.equal(deserializeVoidResponse(withData), false);
  });

});
