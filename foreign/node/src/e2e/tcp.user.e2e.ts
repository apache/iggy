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

import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { getTestClient } from './test-client.utils.js';
import { ResponseError } from '../wire/error.utils.js';

describe('e2e -> user', async () => {

  const c = getTestClient();

  // The server refuses a grant on a stream or topic that does not exist.
  const stream = await c.stream.create({ name: 'e2e-tcp-user-stream' });
  const topic = await c.topic.create({
    streamId: stream.id,
    name: 'e2e-tcp-user-topic',
    partitionCount: 1,
    compressionAlgorithm: 1
  });

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
    streams: [
      {
        streamId: stream.id,
        permissions: {
          manageStream: true,
          readStream: true,
          manageTopics: true,
          readTopics: true,
          pollMessages: true,
          sendMessages: true
        },
        topics: [
          {
            topicId: topic.id,
            permissions: {
              manage: true,
              read: true,
              pollMessages: true,
              sendMessages: true
            }
          }
        ]
      }
    ]
  };

  const cUser = { userId, username, password, status, permissions };

  it('e2e -> user::create', async () => {
    const user = await c.user.create(cUser);
    assert.ok(user);
    assert.deepEqual(user.permissions, permissions);
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
    assert.ok(u1);
    const u2 = await c.user.get({ userId: u1.id });
    assert.deepEqual(u1, u2);
  });

  it('e2e -> user::update', async () => {
    const user = await c.user.get({ userId: username });
    assert.ok(user);
    const u2 = await c.user.update({
      userId: user.id,
      status: 2
    });
    assert.ok(u2);
  });

  it('e2e -> user::changePassword', async () => {
    const user = await c.user.get({ userId: username });
    assert.ok(user);
    assert.ok(await c.user.changePassword({
      userId: user.id, currentPassword: password, newPassword: 'h4x0r42'
    }));
  });

  it('e2e -> user::updatePermissions', async () => {
    const user = await c.user.get({ userId: username });
    assert.ok(user);
    const perms2 = {
      ...permissions,
      global: {
        ...permissions.global,
        ReadServers: true
      }
    };
    const u2 = await c.user.updatePermissions({
      userId: user.id, permissions: perms2
    });
    assert.ok(u2);

    const updatedUser = await c.user.get({ userId: user.id });
    assert.deepEqual(updatedUser?.permissions, perms2);
  });

  it('e2e -> user::delete', async () => {
    const user = await c.user.get({ userId: username });
    assert.ok(user);
    assert.ok(await c.user.delete({ userId: user.id }));
  });

  it('e2e -> user::rejectDanglingGrantsAndPruneDeletedResources', async () => {
    const MISSING_RESOURCE_ID = 0xffff_ffff;
    const STREAM_NOT_FOUND = 1009;
    const TOPIC_NOT_FOUND = 2010;
    const grantUser = await c.user.create({ ...cUser, username: 'grant-user' });
    for (const [streamId, topicId, code] of [
      [MISSING_RESOURCE_ID, topic.id, STREAM_NOT_FOUND],
      [stream.id, MISSING_RESOURCE_ID, TOPIC_NOT_FOUND]
    ]) {
      const dangling = {
        ...permissions,
        streams: [{
          ...permissions.streams[0],
          streamId,
          topics: [{ ...permissions.streams[0].topics[0], topicId }]
        }]
      };
      const refused = (error: unknown) => error instanceof ResponseError && error.errorCode === code;
      await assert.rejects(c.user.create({ ...cUser, username: 'dangling-grant', permissions: dangling }), refused);
      await assert.rejects(c.user.updatePermissions({ userId: grantUser.id, permissions: dangling }), refused);
      assert.deepEqual((await c.user.get({ userId: grantUser.id }))?.permissions, permissions);
    }
    await c.topic.delete({ streamId: stream.id, topicId: topic.id, partitionsCount: 1 });
    const afterTopic = await c.user.get({ userId: grantUser.id });
    assert.ok(afterTopic?.permissions);
    assert.deepEqual(afterTopic.permissions.streams[0].topics, []);
    assert.deepEqual(afterTopic.permissions.streams[0].permissions, permissions.streams[0].permissions);
    await c.stream.delete({ streamId: stream.id });
    const afterStream = await c.user.get({ userId: grantUser.id });
    assert.ok(afterStream?.permissions);
    assert.deepEqual(afterStream.permissions.streams, []);
    assert.deepEqual(afterStream.permissions.global, permissions.global);
    await c.user.delete({ userId: grantUser.id });
  });

  it('e2e -> user::cleanup', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
