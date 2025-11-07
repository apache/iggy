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


import assert from 'node:assert/strict';
import { getRawClient } from './client/index.js';

import {
  login, logout,
  updateStream, getStream, getStreams,
  updateTopic, getTopic, getTopics,
  createPartition, deletePartition,
  createTopic, deleteTopic,
  createStream, deleteStream,
} from './wire/index.js';


try {
  // create socket
  const cli = getRawClient({
    transport: 'TCP' as const,
    options: {
      host: '127.0.0.1',
      port: 8090
    },
    credentials: { username: 'iggy', password: 'iggy' }
  });
  const s = () => Promise.resolve(cli);

  // LOGIN
  const r = await login(s)({ username: 'iggy', password: 'iggy' });
  console.log('RESPONSE_login', r);

  const stream = {
    name: 'test-stream',
    streamId: 1
  };

  // CREATE_STREAM
  const r_createStream = await createStream(s)(stream);
  console.log('RESPONSE_createStream', r_createStream);

  // GET_STREAM #ID
  const r7 = await getStream(s)({ streamId: stream.streamId });
  console.log('RESPONSE7', r7);

  // GET_STREAM #NAME
  const r8 = await getStream(s)({ streamId: stream.name });
  console.log('RESPONSE8', r8);

  // UPDATE_STREAM
  const r_updateStream = await updateStream(s)({
    streamId: stream.streamId, name: 'updatedStreamName'
  });
  console.log('RESPONSE_updateStream', r_updateStream);

  // GET_STREAMS
  const r9 = await getStreams(s)();
  console.log('RESPONSE9', r9);

  const topic1 = {
    streamId: stream.streamId,
    topicId: 44,
    name: 'topic-name-44',
    partitionCount: 3,
    compressionAlgorithm: 1, // 1 = None, 2 = Gzip
  };

  // CREATE_TOPIC
  const r_createTopic = await createTopic(s)(topic1);
  console.log('RESPONSE_createTopic', r_createTopic);

  // GET_TOPIC
  const t2 = await getTopic(s)({ streamId: topic1.streamId, topicId: topic1.name });
  console.log('RESPONSE_getTopic', t2);
  assert.ok(t2);

  // UPDATE_TOPIC
  const r_updateTopic = await updateTopic(s)({
    streamId: topic1.streamId, topicId: topic1.topicId, name: topic1.name, messageExpiry: 42n
  });
  console.log('RESPONSE_updateTopic', r_updateTopic);

  // CREATE_PARTITION
  const r_createPartition = await createPartition(s)({
    streamId: topic1.streamId, topicId: t2.id, partitionCount: 22
  });
  console.log('RESPONSE_createPartition', r_createPartition);

  // DELETE_PARTITION
  const r_deletePartition = await deletePartition(s)({
    streamId: topic1.streamId, topicId: t2.id, partitionCount: 19
  });
  console.log('RESPONSE_deletePartition', r_deletePartition);

  // GET_TOPIC AGAIN
  const r_getTopic2 = await getTopic(s)({ streamId: topic1.streamId, topicId: topic1.name });
  console.log('RESPONSE_getTopic2', r_getTopic2);

  // GET_TOPICS
  const r_getTopics = await getTopics(s)({ streamId: topic1.streamId });
  console.log('RESPONSE_getTopics', r_getTopics);

  // DELETE TOPIC
  const r_deleteTopic = await deleteTopic(s)({
    streamId: topic1.streamId, topicId: t2.id, partitionsCount: 3
  });
  console.log('RESPONSE_deleteTopic', r_deleteTopic);

  // DELETE STREAM
  const rDelS = await deleteStream(s)({ streamId: stream.streamId });
  console.log('RESPONSEDelS', rDelS);

  // LOGOUT
  const rOut = await logout(s)();
  console.log('RESPONSE LOGOUT', rOut);


} catch (err) {
  console.error('FAILED!', err);
}

process.exit(0);
