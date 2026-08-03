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
  deserializeLoginRegister,
  IGGY_PROTOCOL_VERSION,
  serializeLoginRegister,
  serializeLoginRegisterWithPat
} from './register.js';

describe('VSR register payloads', () => {
  it('encodes protocol, SDK, credentials, and empty context', () => {
    const body = serializeLoginRegister('iggy', 'secret', '0.8.1-edge.3');
    assert.equal(body.readUInt32LE(0), IGGY_PROTOCOL_VERSION);
    assert.match(body.toString('utf8'), /node-sdk/);
    assert.match(body.toString('utf8'), /0\.8\.1-edge\.3/);
    assert.match(body.toString('utf8'), /iggy/);
    assert.match(body.toString('utf8'), /secret/);
    assert.equal(body.readUInt32LE(body.length - 4), 0);
  });

  it('uses UTF-8 byte lengths and enforces one-byte bounds', () => {
    const body = serializeLoginRegister('żółw', 'hasło', '版本');
    assert.equal(body.readUInt8(4), Buffer.byteLength('node-sdk'));
    assert.match(body.toString('utf8'), /żółw/);
    assert.throws(
      () => serializeLoginRegister('', 'secret', '1.0.0'),
      /1\.\.255 bytes/
    );
    assert.throws(
      () => serializeLoginRegister('x'.repeat(256), 'secret', '1.0.0'),
      /1\.\.255 bytes/
    );
    const maximum = serializeLoginRegister(
      'x'.repeat(255),
      'secret',
      '1.0.0'
    );
    assert.match(maximum.toString('utf8'), /x{255}/);
  });

  it('encodes PAT registration and rejects oversized tokens', () => {
    const body = serializeLoginRegisterWithPat('token', '0.8.1-edge.3');
    assert.match(body.toString('utf8'), /token/);
    assert.throws(
      () => serializeLoginRegisterWithPat('x'.repeat(256), '0.8.1-edge.3'),
      /token exceeds/
    );
  });

  it('decodes a complete login response', () => {
    const version = Buffer.from('0.10.3');
    const body = Buffer.alloc(17 + version.length);
    body.writeUInt32LE(7, 0);
    body.writeBigUInt64LE(42n, 4);
    body.writeUInt32LE(IGGY_PROTOCOL_VERSION, 12);
    body.writeUInt8(version.length, 16);
    version.copy(body, 17);
    assert.deepEqual(deserializeLoginRegister(body), {
      userId: 7,
      session: 42n,
      serverProtocolVersion: IGGY_PROTOCOL_VERSION,
      serverVersion: '0.10.3'
    });
  });

  it('rejects every truncated fixed response and variable tail', () => {
    for (let length = 0; length < 17; length += 1)
      assert.throws(
        () => deserializeLoginRegister(Buffer.alloc(length)),
        /too short/
      );
    const body = Buffer.alloc(17);
    body.writeUInt8(1, 16);
    assert.throws(() => deserializeLoginRegister(body), /truncated/);
  });
});
