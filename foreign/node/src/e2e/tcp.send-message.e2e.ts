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
import { Consumer, PollingStrategy, Partitioning } from '../wire/index.js';
import { generateMessages } from '../tcp.sm.utils.js';
import { getTestClient } from './test-client.utils.js';


describe('e2e -> message', async () => {

  const c = getTestClient();

  const streamName = 'e2e-stream-934';
  const topicName = 'e2e-topic-832';
  const partitionId = 0;

  await c.stream.create({ name: streamName });
  const topic = {
    streamId: streamName,
    name: topicName,
    partitionCount: 1,
    compressionAlgorithm: 1
  };
  await c.topic.create(topic);

  const msg = {
    streamId: streamName,
    topicId: topicName,
    messages: generateMessages(6),
    partition: Partitioning.PartitionId(partitionId)
  };

  it('e2e -> message::send', async () => {
    assert.ok(await c.message.send(msg));
  });

  it('e2e -> message::poll/last', async () => {
    const pollReq = {
      streamId: streamName,
      topicId: topicName,
      consumer: Consumer.Single,
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
      streamId: streamName,
      topicId: topicName,
      consumer: Consumer.Single,
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
      streamId: streamName,
      topicId: topicName,
      consumer: Consumer.Single,
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
      streamId: streamName,
      topicId: topicName,
      consumer: Consumer.Single,
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

  it('e2e -> message::getOffset', async () => {
    const offset = await c.offset.get({
      streamId: streamName,
      topicId: topicName,
      consumer: Consumer.Single,
      partitionId
    });
    assert.deepEqual(offset, { partitionId: 0, currentOffset: 5n, storedOffset: 5n });
  });

  it('e2e -> message::deleteOffset', async () => {
    const r = await c.offset.delete({
      streamId: streamName,
      topicId: topicName,
      consumer: Consumer.Single,
      partitionId
    });
    assert.ok(r);

    const offset = await c.offset.get({
      streamId: streamName,
      topicId: topicName,
      consumer: Consumer.Single,
      partitionId
    });
    assert.equal(offset, null);
  });

  it('e2e -> message::storeOffset', async () => {
    const r = await c.offset.store({
      streamId: streamName,
      topicId: topicName,
      consumer: Consumer.Single,
      partitionId,
      offset: 2n
    });
    assert.ok(r);

    const offset = await c.offset.get({
      streamId: streamName,
      topicId: topicName,
      consumer: Consumer.Single,
      partitionId
    });
    assert.deepEqual(offset, { partitionId: 0, currentOffset: 5n, storedOffset: 2n });
  });

  it('e2e -> message::cleanup', async () => {
    assert.ok(await c.stream.delete({ streamId: streamName }));
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });

});
