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
//

import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { getTestClient } from './test-client.utils.js';
import { COMMAND_CODE } from '../wire/command.code.js';

describe('e2e -> raw', async () => {

  const c = getTestClient();

  it('e2e -> raw::ping', async () => {
    const response = await c.sendBinaryRequest(COMMAND_CODE.Ping, Buffer.alloc(0));
    assert.deepEqual(response, Buffer.alloc(0));
  });

  it('e2e -> raw::getStats', async () => {
    const response = await c.sendBinaryRequest(COMMAND_CODE.GetStats, Buffer.alloc(0));
    assert.ok(response.length > 0);
  });

  it('e2e -> raw::sessionControlCodeRejectedClientSide', async () => {
    await assert.rejects(
      () => c.sendBinaryRequest(COMMAND_CODE.LoginUser, Buffer.alloc(0))
    );
  });

  it('e2e -> raw::unknownCodeRejectedByServer', async () => {
    await assert.rejects(
      () => c.sendBinaryRequest(60000, Buffer.alloc(0))
    );
  });

  after(() => {
    c.destroy();
  });
});
