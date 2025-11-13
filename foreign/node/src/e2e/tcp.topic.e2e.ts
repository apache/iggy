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


describe('e2e -> topic', async () => {

  const c = getTestClient();
  const streamName = 'e2e-tcp-topic-stream';

  const topicName = 'e2e-tcp-topic-topic-123';
  await c.stream.create({ name: streamName });

  it('e2e -> topic::create', async () => {
    const TOPIC = await c.topic.create({
      streamId: streamName,
      name: topicName,
      partitionCount: 0,
      compressionAlgorithm: 1,
      messageExpiry: 0n,
      replicationFactor: 1
    });
    assert.ok(TOPIC);
  });

  it('e2e -> topic::list', async () => {
    const topics = await c.topic.list({ streamId: streamName });
    assert.ok(topics.length > 0);
  });

  it('e2e -> topic::get', async () => {
    const topic = await c.topic.get({ streamId: streamName, topicId: topicName });
    assert.ok(topic);
  });

  it('e2e -> topic::createPartition', async () => {
    const cp = await c.partition.create({
      streamId: streamName,
      topicId: topicName,
      partitionCount: 22
    });
    assert.ok(cp);
  });

  it('e2e -> topic::deletePartition', async () => {
    const dp = await c.partition.delete({
      streamId: streamName,
      topicId: topicName,
      partitionCount: 19
    });
    assert.ok(dp);
  });

  it('e2e -> topic::purge', async () => {
    assert.ok(await c.topic.purge({ streamId: streamName, topicId: topicName }));
  });

  it('e2e -> topic::delete', async () => {
    assert.ok(await c.topic.delete({
      streamId: streamName, topicId: topicName, partitionsCount: 0
    }));
  });

  it('e2e -> topic::cleanup', async () => {
    assert.ok(await c.stream.delete({ streamId: streamName }));
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
