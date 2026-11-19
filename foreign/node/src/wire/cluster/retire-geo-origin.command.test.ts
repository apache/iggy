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
import { COMMAND_CODE } from '../command.code.js';
import { RETIRE_GEO_ORIGIN } from './retire-geo-origin.command.js';

describe('RetireGeoOrigin', () => {
  it('sends code 18 with the origin as a u8-length-prefixed name', () => {
    assert.equal(RETIRE_GEO_ORIGIN.code, COMMAND_CODE.RetireGeoOrigin);
    assert.equal(RETIRE_GEO_ORIGIN.code, 18);
    assert.equal(RETIRE_GEO_ORIGIN.serialize({ origin: 'dc-a' }).toString('hex'), '0464632d61');
  });

  it('counts the origin length in bytes, not characters', () => {
    const encoded = RETIRE_GEO_ORIGIN.serialize({ origin: 'dc-ü' });
    assert.equal(encoded.readUInt8(0), 5);
    assert.equal(encoded.length, 6);
  });

  it('rejects an empty origin', () => {
    assert.throws(() => RETIRE_GEO_ORIGIN.serialize({ origin: '' }));
  });

  it('rejects an origin over 255 bytes', () => {
    assert.throws(() => RETIRE_GEO_ORIGIN.serialize({ origin: 'ü'.repeat(128) }));
  });

  it('reads an empty reply as success', () => {
    assert.equal(RETIRE_GEO_ORIGIN.deserialize({ status: 0, length: 0, data: Buffer.alloc(0) }), true);
  });
});
