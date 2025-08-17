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


import type { ClientProvider } from '../client/client.type.js';

import { login } from './session/login.command.js';
import { logout } from './session/logout.command.js';
import { loginWithToken } from './session/login-with-token.command.js';

import { getMe } from './client/get-me.command.js';
import { getClients } from './client/get-clients.command.js';
import { getClient } from './client/get-client.command.js';

import { createGroup } from './consumer-group/create-group.command.js';
import { joinGroup } from './consumer-group/join-group.command.js';
import { getGroup } from './consumer-group/get-group.command.js';
import { getGroups } from './consumer-group/get-groups.command.js';
import { leaveGroup } from './consumer-group/leave-group.command.js';
import { deleteGroup } from './consumer-group/delete-group.command.js';
import {
  ensureConsumerGroup, ensureConsumerGroupAndJoin
} from './consumer-group/ensure-group.virtual.command.js';

import { purgeTopic } from './topic/purge-topic.command.js';
import { createTopic } from './topic/create-topic.command.js';
import { updateTopic } from './topic/update-topic.command.js';
import { getTopic } from './topic/get-topic.command.js';
import { getTopics } from './topic/get-topics.command.js';
import { deleteTopic } from './topic/delete-topic.command.js';
import { ensureTopic } from './topic/ensure-topic.virtual.command.js';

import { getOffset } from './offset/get-offset.command.js';
import { storeOffset } from './offset/store-offset.command.js';
import { deleteOffset } from './offset/delete-offset.command.js';

import { sendMessages } from './message/send-messages.command.js';
import { pollMessages } from './message/poll-messages.command.js';
import { flushUnsavedBuffers } from './message/flush-unsaved-buffers.command.js';

import { createStream } from './stream/create-stream.command.js';
import { updateStream } from './stream/update-stream.command.js';
import { getStream } from './stream/get-stream.command.js';
import { getStreams } from './stream/get-streams.command.js';
import { deleteStream } from './stream/delete-stream.command.js';
import { purgeStream } from './stream/purge-stream.command.js';
import { ensureStream } from './stream/ensure-stream.virtual.command.js';

import { createPartition } from './partition/create-partition.command.js';
import { deletePartition } from './partition/delete-partition.command.js';

import { deleteSegments } from './segment/delete-segments.command.js';

import { getStats } from './system/get-stats.command.js';
import { ping } from './system/ping.command.js';

import { getTokens } from './token/get-tokens.command.js';
import { createToken } from './token/create-token.command.js';
import { deleteToken } from './token/delete-token.command.js';

import { getUser } from './user/get-user.command.js';
import { createUser } from './user/create-user.command.js';
import { changePassword } from './user/change-password.command.js';
import { updateUser } from './user/update-user.command.js';
import { updatePermissions } from './user/update-permissions.command.js';
import { deleteUser } from './user/delete-user.command.js';
import { getUsers } from './user/get-users.command.js';


const userAPI = (c: ClientProvider) => ({
  get: getUser(c),
  list: getUsers(c),
  create: createUser(c),
  update: updateUser(c),
  updatePermissions: updatePermissions(c),
  changePassword: changePassword(c),
  delete: deleteUser(c),
});

type UserAPI = ReturnType<typeof userAPI>;

const sessionAPI = (c: ClientProvider) => ({
  login: login(c),
  loginWithToken: loginWithToken(c),
  logout: logout(c)
});

type SessionAPI = ReturnType<typeof sessionAPI>;

const clientAPI = (c: ClientProvider) => ({
  get: getClient(c),
  getMe: getMe(c),
  list: getClients(c)
});

type ClientAPI = ReturnType<typeof clientAPI>;

const tokenAPI = (c: ClientProvider) => ({
  list: getTokens(c),
  create: createToken(c),
  delete: deleteToken(c),
});

type TokenAPI = ReturnType<typeof tokenAPI>;

const streamAPI = (c: ClientProvider) => ({
  get: getStream(c),
  list: getStreams(c),
  create: createStream(c),
  update: updateStream(c),
  delete: deleteStream(c),
  purge: purgeStream(c),
  ensure: ensureStream(c)
});

type StreamAPI = ReturnType<typeof streamAPI>;

const topicAPI = (c: ClientProvider) => ({
  get: getTopic(c),
  list: getTopics(c),
  create: createTopic(c),
  update: updateTopic(c),
  delete: deleteTopic(c),
  purge: purgeTopic(c),
  ensure: ensureTopic(c)
});

type TopicAPI = ReturnType<typeof topicAPI>;

const partitionAPI = (c: ClientProvider) => ({
  create: createPartition(c),
  delete: deletePartition(c),
});

type PartitionAPI = ReturnType<typeof partitionAPI>;

const segmentAPI = (c: ClientProvider) => ({
  delete: deleteSegments(c),
});

type SegmentAPI = ReturnType<typeof segmentAPI>;

const groupAPI = (c: ClientProvider) => ({
  get: getGroup(c),
  list: getGroups(c),
  create: createGroup(c),
  join: joinGroup(c),
  leave: leaveGroup(c),
  delete: deleteGroup(c),
  ensure: ensureConsumerGroup(c),
  ensureAndJoin: ensureConsumerGroupAndJoin(c)
});

type GroupAPI = ReturnType<typeof groupAPI>;

const offsetAPI = (c: ClientProvider) => ({
  get: getOffset(c),
  store: storeOffset(c),
  delete: deleteOffset(c)
});

type OffsetAPI = ReturnType<typeof offsetAPI>;

const messageAPI = (c: ClientProvider) => ({
  poll: pollMessages(c),
  send: sendMessages(c),
  flushUnsavedBuffers: flushUnsavedBuffers(c)
});

type MessageAPI = ReturnType<typeof messageAPI>;

const systemAPI = (c: ClientProvider) => ({
  ping: ping(c),
  getStats: getStats(c)
});

type SystemAPI = ReturnType<typeof systemAPI>;

export abstract class AbstractAPI {
  clientProvider: ClientProvider;

  constructor(getClient: ClientProvider) {
    this.clientProvider = getClient;
  }
}

export abstract class CommandAPI extends AbstractAPI {
  user: UserAPI = userAPI(this.clientProvider);
  session: SessionAPI = sessionAPI(this.clientProvider);
  client: ClientAPI = clientAPI(this.clientProvider);
  token: TokenAPI = tokenAPI(this.clientProvider);
  stream: StreamAPI = streamAPI(this.clientProvider);
  topic: TopicAPI = topicAPI(this.clientProvider);
  partition: PartitionAPI = partitionAPI(this.clientProvider);
  segment: SegmentAPI = segmentAPI(this.clientProvider);
  group: GroupAPI = groupAPI(this.clientProvider);
  offset: OffsetAPI = offsetAPI(this.clientProvider);
  message: MessageAPI = messageAPI(this.clientProvider);
  system: SystemAPI = systemAPI(this.clientProvider);

  constructor(c: ClientProvider) {
    super(c);
  }
};
