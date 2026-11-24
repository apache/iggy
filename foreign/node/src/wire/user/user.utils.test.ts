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
  serializePermissions,
  type GlobalPermissions,
  type UserPermissions,
} from './permissions.utils.js';
import {
  deserializeBaseUser,
  deserializeUser,
} from './user.utils.js';

const u32 = (n: number) => {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(n);
  return b;
};

const u64 = (n: bigint) => {
  const b = Buffer.alloc(8);
  b.writeBigUInt64LE(n);
  return b;
};

const emptyGlobal = (): GlobalPermissions => ({
  ManageServers: false,
  ReadServers: false,
  ManageUsers: false,
  ReadUsers: false,
  ManageStreams: false,
  ReadStreams: false,
  ManageTopics: false,
  ReadTopics: false,
  PollMessages: false,
  SendMessages: false,
});

const emptyPermissions = (): UserPermissions => ({
  global: emptyGlobal(),
  streams: [],
});

const buildBaseUser = (
  {
    id = 7,
    createdAtUs = 1_700_000_000_000_000n,
    status = 1,
    userName = 'alice',
  }: { id?: number, createdAtUs?: bigint, status?: number, userName?: string } = {}
): Buffer => {
  const name = Buffer.from(userName);
  return Buffer.concat([
    u32(id),
    u64(createdAtUs),
    Buffer.from([status, name.length]),
    name,
    u32(0),
    u32(0),
  ]);
};

describe('user utils', () => {

  describe('deserializeBaseUser', () => {

    it('decodes an active user', () => {
      const buffer = buildBaseUser();

      const { bytesRead, data } = deserializeBaseUser(buffer);

      assert.equal(bytesRead, buffer.length);
      assert.equal(data.id, 7);
      assert.equal(data.userName, 'alice');
      assert.equal(data.status, 'Active');
      assert.equal(data.createdAt.toISOString(), '2023-11-14T22:13:20.000Z');
      assert.deepEqual(data.options, {});
      assert.deepEqual(data.derivedOptions, {});
    });

    it('maps the inactive status byte', () => {
      const { data } = deserializeBaseUser(buildBaseUser({ status: 2 }));
      assert.equal(data.status, 'Inactive');
    });

    it('names an unknown status byte', () => {
      const { data } = deserializeBaseUser(buildBaseUser({ status: 9 }));
      assert.equal(data.status, 'unknown_status_9');
    });

    it('throws for an empty buffer', () => {
      assert.throws(() => deserializeBaseUser(Buffer.alloc(0)), /User does not exist/);
    });

  });

  describe('deserializeUser', () => {

    it('answers null permissions when the flag is zero', () => {
      const user = deserializeUser(Buffer.concat([buildBaseUser(), Buffer.from([0])]));
      assert.equal(user.permissions, null);
    });

    it('decodes the permissions block when the flag is one', () => {
      const permissions = emptyPermissions();
      const block = serializePermissions(permissions);
      const user = deserializeUser(Buffer.concat([
        buildBaseUser(),
        Buffer.from([1]),
        u32(block.length),
        block,
      ]));

      assert.deepEqual(user.permissions, permissions);
    });

  });

});
