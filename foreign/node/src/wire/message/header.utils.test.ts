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
import { uuidv7, uuidv4 } from 'uuidv7'
import { serializeHeaders, deserializeHeaders, HeaderValue } from './header.utils.js';

describe('Headers', () => {

  const headers = {
    'p': HeaderValue.Bool(true),
    'x': HeaderValue.Uint32(123),
    'y': HeaderValue.Uint64(42n),
    'z': HeaderValue.Float(42.20000076293945),
    'a': HeaderValue.Double(1/3),
    'ID': HeaderValue.String(uuidv7()),
    'val': HeaderValue.Raw(Buffer.from(uuidv4()))
  };

  it('serialize/deserialize', () => {
    const s = serializeHeaders(headers);
    const d = deserializeHeaders(s);
    assert.deepEqual(headers, d);
  });

});
