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


import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { getTestClient } from './test-client.utils.js';

describe('e2e -> client', async () => {

  const c = getTestClient();

  it('e2e -> client::getMe', async () => {
    const cli = await c.client.getMe();
    assert.ok(cli);
  });

  it('e2e -> client::get#id', async () => {
    const cli = await c.client.getMe();
    const cli2 = await c.client.get({ clientId: cli.clientId })
    assert.ok(cli);
    assert.deepEqual(cli, cli2);
  });

  it('e2e -> client::list', async () => {
    const clis = await c.client.list();
    assert.ok(clis.length > 0);
  });

  it('e2e -> client::cleanup', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
