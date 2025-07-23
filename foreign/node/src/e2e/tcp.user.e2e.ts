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

describe('e2e -> user', async () => {

  const c = getTestClient();

  const userId = 42;
  const username = 'test-user';
  const password = 'test-pwd123$!';
  const status = 1; // Active;
  const permissions = {
    global: {
      ManageServers: false,
      ReadServers: false,
      ManageUsers: true,
      ReadUsers: true,
      ManageStreams: true,
      ReadStreams: true,
      ManageTopics: true,
      ReadTopics: true,
      PollMessages: true,
      SendMessages: true
    },
    streams: []
  };
  
  const cUser = { userId, username, password, status, permissions };

  it('e2e -> user::create', async () => {
    const user = await c.user.create(cUser);
    assert.ok(user);
  });

  it('e2e -> user::list', async () => {
    const users = await c.user.list();
    assert.ok(users.length > 0);
  });

  it('e2e -> user::get#name', async () => {
    const user = await c.user.get({ userId: username });
    assert.ok(user);
  });

  it('e2e -> user::get#id', async () => {
    const u1 = await c.user.get({ userId: username });
    const u2 = await c.user.get({ userId: u1.id });
    assert.deepEqual(u1, u2);
  });

  it('e2e -> user::update', async () => {
    const user = await c.user.get({ userId: username });
    const u2 = await c.user.update({
      userId: user.id,
      status: 2
    });
    assert.ok(u2);
  });

  it('e2e -> user::changePassword', async () => {
    const user = await c.user.get({ userId: username });
    assert.ok(await c.user.changePassword({
      userId: user.id, currentPassword: password, newPassword: 'h4x0r42'
    }));
  });

  it('e2e -> user::updatePermissions', async () => {
    const user = await c.user.get({ userId: username });
    const perms2 = { ...permissions };
    perms2.global.ReadServers = true;
    const u2 = await c.user.updatePermissions({
      userId: user.id, permissions: perms2
    });
    assert.ok(u2);
  });
  
  it('e2e -> user::delete', async () => {
    const user = await c.user.get({ userId: username });
    assert.ok(await c.user.delete({ userId: user.id }));
  });

  it('e2e -> user::logout', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
