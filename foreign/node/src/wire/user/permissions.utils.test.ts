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

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  deserializePermissions,
  serializePermissions,
  type UserPermissions
} from './permissions.utils.js';

describe('Permissions', () => {

  const globalPermissions = {
    ManageServers: true,
    ReadServers: true,
    ManageUsers: true,
    ReadUsers: true,
    ManageStreams: true,
    ReadStreams: true,
    ManageTopics: true,
    ReadTopics: true,
    PollMessages: true,
    SendMessages: true
  };

  const globalOnlyPermissions = {
    global: globalPermissions,
    streams: []
  } satisfies UserPermissions;

  const scopedPermissions = {
    global: globalPermissions,
    streams: [
      {
        streamId: 1,
        permissions: {
          manageStream: true,
          readStream: false,
          manageTopics: true,
          readTopics: false,
          pollMessages: true,
          sendMessages: false
        },
        topics: [
          {
            topicId: 10,
            permissions: {
              manage: true,
              read: false,
              pollMessages: true,
              sendMessages: false
            }
          },
          {
            topicId: 20,
            permissions: {
              manage: false,
              read: true,
              pollMessages: false,
              sendMessages: true
            }
          }
        ]
      },
      {
        streamId: 2,
        permissions: {
          manageStream: false,
          readStream: true,
          manageTopics: false,
          readTopics: true,
          pollMessages: false,
          sendMessages: true
        },
        topics: []
      }
    ]
  } satisfies UserPermissions;

  const serializedScopedPermissions = Buffer.from([
    // Global permissions and has_streams.
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    // Stream 1, its permissions, and has_topics.
    1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1,
    // Topic 10 and has_next_topic.
    10, 0, 0, 0, 1, 0, 1, 0, 1,
    // Topic 20 and has_next_topic.
    20, 0, 0, 0, 0, 1, 0, 1, 0,
    // has_next_stream.
    1,
    // Stream 2, its permissions, has_topics, and has_next_stream.
    2, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0
  ]);

  it('round-trips global permissions', () => {
    const serialized = serializePermissions(globalOnlyPermissions);
    const deserialized = deserializePermissions(serialized);
    assert.deepEqual(deserialized, globalOnlyPermissions);
  });

  it('serializes stream and topic continuation markers', () => {
    const serialized = serializePermissions(scopedPermissions);
    assert.deepEqual(serialized, serializedScopedPermissions);
  });

  it('round-trips multiple streams and topics', () => {
    const deserialized = deserializePermissions(serializedScopedPermissions);
    assert.deepEqual(deserialized, scopedPermissions);
  });

});
