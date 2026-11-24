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
import { deserializeClient } from './client.utils.js';

const u32 = (n: number) => {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(n);
  return b;
};

const buildClient = (
  {
    clientId = 1,
    userId = 2,
    transport = 1,
    address = '127.0.0.1:8090',
    consumerGroupCount = 3,
  }: {
    clientId?: number,
    userId?: number,
    transport?: number,
    address?: string,
    consumerGroupCount?: number,
  } = {}
): Buffer => {
  const addressBuf = Buffer.from(address);
  return Buffer.concat([
    u32(clientId),
    u32(userId),
    Buffer.from([transport]),
    u32(addressBuf.length),
    addressBuf,
    u32(consumerGroupCount),
  ]);
};

describe('client utils', () => {

  describe('deserializeClient', () => {

    it('decodes every field of a hand-built client', () => {
      const buffer = buildClient();

      const { bytesRead, data } = deserializeClient(buffer);

      assert.equal(bytesRead, buffer.length);
      assert.deepEqual(data, {
        clientId: 1,
        userId: 2,
        transport: 'tcp',
        address: '127.0.0.1:8090',
        consumerGroupCount: 3,
      });
    });

    it('decodes from a given offset', () => {
      const prefix = Buffer.from([0xff, 0xff]);
      const buffer = Buffer.concat([prefix, buildClient({ clientId: 9 })]);

      const { data } = deserializeClient(buffer, prefix.length);

      assert.equal(data.clientId, 9);
      assert.equal(data.address, '127.0.0.1:8090');
    });

    it('names an unknown transport byte', () => {
      const { data } = deserializeClient(buildClient({ transport: 9 }));
      assert.equal(data.transport, 'unknown_transport_9');
    });

    it('throws when fewer than 17 bytes remain from the offset', () => {
      const buffer = Buffer.concat([Buffer.from([0xff]), buildClient().subarray(0, 10)]);
      assert.throws(() => deserializeClient(buffer, 1), /Client does not exist/);
    });

    it('throws when the address length runs past the end', () => {
      const buffer = buildClient();
      buffer.writeUInt32LE(1000, 9);
      assert.throws(() => deserializeClient(buffer), /Client does not exist/);
    });

  });

});
