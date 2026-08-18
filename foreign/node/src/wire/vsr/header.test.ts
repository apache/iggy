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
  Command,
  encodeRequestHeader,
  HEADER_SIZE,
  REQUEST_OFFSET
} from './header.js';

describe('VSR request header', () => {
  it('encodes every field at its canonical offset', () => {
    const client = 0x1122334455667788_99AABBCCDDEEFF00n;
    const header = encodeRequestHeader({
      size: HEADER_SIZE + 3,
      client,
      request: 0x0102030405060708n,
      operation: 2,
      session: 0x1020304050607080n,
      nonReplicatedCode: 60_001
    });

    assert.equal(header.length, HEADER_SIZE);
    assert.equal(header.readUInt32LE(REQUEST_OFFSET.size), HEADER_SIZE + 3);
    assert.equal(header.readUInt8(REQUEST_OFFSET.command), Command.Request);
    assert.equal(
      header.readBigUInt64LE(REQUEST_OFFSET.client),
      0x99AABBCCDDEEFF00n
    );
    assert.equal(
      header.readBigUInt64LE(REQUEST_OFFSET.client + 8),
      0x1122334455667788n
    );
    assert.equal(
      header.readBigUInt64LE(REQUEST_OFFSET.request),
      0x0102030405060708n
    );
    assert.equal(header.readUInt8(REQUEST_OFFSET.operation), 2);
    assert.equal(
      header.readBigUInt64LE(REQUEST_OFFSET.session),
      0x1020304050607080n
    );
    assert.equal(header.readUInt32LE(REQUEST_OFFSET.reserved), 60_001);
    assert.equal(header.readBigUInt64LE(REQUEST_OFFSET.timestamp), 0n);
  });

  it('leaves all unspecified and reserved bytes zero', () => {
    const header = encodeRequestHeader({
      size: HEADER_SIZE,
      client: 1n,
      request: 0n,
      operation: 1,
      session: 0n
    });
    const expected = Buffer.alloc(HEADER_SIZE);
    expected.writeUInt32LE(HEADER_SIZE, REQUEST_OFFSET.size);
    expected.writeUInt8(Command.Request, REQUEST_OFFSET.command);
    expected.writeBigUInt64LE(1n, REQUEST_OFFSET.client);
    expected.writeUInt8(1, REQUEST_OFFSET.operation);
    assert.deepEqual(header, expected);
  });

  it('supports maximum-width unsigned request fields', () => {
    const maximum = 0xFFFF_FFFF_FFFF_FFFFn;
    const header = encodeRequestHeader({
      size: HEADER_SIZE,
      client: maximum << 64n | maximum,
      request: maximum,
      operation: 160,
      session: maximum
    });
    assert.equal(header.readBigUInt64LE(REQUEST_OFFSET.request), maximum);
    assert.equal(header.readBigUInt64LE(REQUEST_OFFSET.session), maximum);
  });
});
