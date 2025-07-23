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
  
  const streamId = 111;
  const topicId = 123;

  await c.stream.create({ streamId, name: 'e2e-tcp-topic-stream' });

  it('e2e -> topic::create', async () => {
    const topic = await c.topic.create({
      streamId, topicId,
      name: 'e2e-tcp-topic',
      partitionCount: 0,
      compressionAlgorithm: 1,
      messageExpiry: 0n,
      replicationFactor: 1
    });
    assert.ok(topic);
  });

  it('e2e -> topic::list', async () => {
    const topics = await c.topic.list({ streamId });
    assert.ok(topics.length > 0);
  });

  it('e2e -> topic::get', async () => {
    const topic = await c.topic.get({ streamId, topicId });
    assert.ok(topic);
  });

  it('e2e -> topic::createPartition', async () => {
    const cp = await c.partition.create({
      streamId, topicId,
      partitionCount : 22
    });
    assert.ok(cp);
  });

  it('e2e -> topic::deletePartition', async () => {
    const dp = await c.partition.delete({
      streamId, topicId,
      partitionCount : 19
    });
    assert.ok(dp);
  });

  it('e2e -> topic::update', async () => {
    const topic = await c.topic.get({ streamId, topicId });
    const u2 = await c.topic.update({
      streamId, topicId,
      name: topic.name,
      messageExpiry: 42n
    });
    assert.ok(u2);
  });

  it('e2e -> topic::purge', async () => {
    assert.ok(await c.topic.purge({ streamId, topicId }));
  });

  it('e2e -> topic::delete', async () => {
    assert.ok(await c.topic.delete({ streamId, topicId, partitionsCount: 0 }));
  });

  it('e2e -> topic::cleanup', async () => {
    assert.ok(await c.stream.delete({ streamId }));
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
