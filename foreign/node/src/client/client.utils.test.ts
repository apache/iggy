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
import { handleResponse, deserializeVoidResponse } from './client.utils.js';

describe('handleResponse', () => {

  it('bounds data to the length field, not the full buffer', () => {
    // Server says: status=0, length=0, no payload.
    // But the raw buffer has 4 trailing bytes (e.g. start of next response).
    const buf = Buffer.alloc(12);
    buf.writeUInt32LE(0, 0);  // status = 0
    buf.writeUInt32LE(0, 4);  // length = 0  (void response)
    buf.writeUInt32LE(42, 8); // trailing bytes — NOT part of this response

    const r = handleResponse(buf);
    assert.equal(r.data.length, 0);
  });

  it('deserializeVoidResponse returns true for a valid void response with trailing buffer bytes', () => {
    const buf = Buffer.alloc(12);
    buf.writeUInt32LE(0, 0);  // status = 0
    buf.writeUInt32LE(0, 4);  // length = 0
    buf.writeUInt32LE(42, 8); // trailing bytes

    const r = handleResponse(buf);
    assert.equal(deserializeVoidResponse(r), true);
  });

});
