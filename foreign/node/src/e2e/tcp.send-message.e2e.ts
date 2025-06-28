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
import { ConsumerKind, PollingStrategy, Partitioning } from '../wire/index.js';
import { generateMessages } from '../tcp.sm.utils.js';
import { getTestClient } from './test-client.utils.js';


describe('e2e -> message', async () => {

  const c = getTestClient();
  
  const streamId = 934;
  const topicId = 832;
  const partitionId = 1;

  const stream = {
    streamId,
    name: 'e2e-send-message-stream'
  };

  const topic = {
    streamId,
    topicId,
    name: 'e2e-send-message-topic',
    partitionCount: 1,
    compressionAlgorithm: 1
  };

  const msg = {
    streamId,
    topicId,
    messages: generateMessages(6),
    partition: Partitioning.PartitionId(partitionId)
  };

  await c.stream.create(stream);
  await c.topic.create(topic);

  it('e2e -> message::send', async () => {
    assert.ok(await c.message.send(msg));
  });

  it('e2e -> message::poll/last', async () => {
    const pollReq = {
      streamId,
      topicId,
      consumer: { kind: ConsumerKind.Single, id: 12 },
      partitionId,
      pollingStrategy: PollingStrategy.Last,
      count: 10,
      autocommit: false
    };
    const { messages, ...resp } = await c.message.poll(pollReq);
    assert.equal(messages.length, resp.count);
    assert.equal(messages.length, msg.messages.length)
  });

  it('e2e -> message::poll/first', async () => {
    const pollReq = {
      streamId,
      topicId,
      consumer: { kind: ConsumerKind.Single, id: 12 },
      partitionId,
      pollingStrategy: PollingStrategy.First,
      count: 10,
      autocommit: false
    };
    const { messages, ...resp } = await c.message.poll(pollReq);
    assert.equal(messages.length, resp.count);
    assert.equal(messages.length, msg.messages.length)
  });

  it('e2e -> message::poll/next', async () => {
    const pollReq = {
      streamId,
      topicId,
      consumer: { kind: ConsumerKind.Single, id: 12 },
      partitionId,
      pollingStrategy: PollingStrategy.Next,
      count: 10,
      autocommit: false
    };
    const { messages, ...resp } = await c.message.poll(pollReq);
    assert.equal(messages.length, resp.count);
    assert.equal(messages.length, msg.messages.length)
  });

  it('e2e -> message::poll/next+commit', async () => {
    const pollReq = {
      streamId,
      topicId,
      consumer: { kind: ConsumerKind.Single, id: 12 },
      partitionId,
      pollingStrategy: PollingStrategy.Next,
      count: 10,
      autocommit: true
    };
    const { messages, ...resp } = await c.message.poll(pollReq);
    assert.equal(messages.length, resp.count);
    assert.equal(messages.length, msg.messages.length)

    const r2 = await c.message.poll(pollReq);
    assert.equal(r2.count, 0);
    assert.equal(r2.messages.length, 0)
  });

  it('e2e -> message::cleanup', async () => {
    assert.ok(await c.stream.delete(stream));
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
