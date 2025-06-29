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
import { SingleClient } from '../client/client.js';
import { ConsumerKind, PollingStrategy, Partitioning } from '../wire/index.js';
import { generateMessages } from '../tcp.sm.utils.js';
import { getIggyAddress } from '../tcp.sm.utils.js';

describe('e2e -> consumer-group', async () => {

  const [host, port] = getIggyAddress();

  const c = new SingleClient({
    transport: 'TCP',
    options: { host, port },
    credentials: { username: 'iggy', password: 'iggy' }
  });
  
  const streamId = 555;
  const topicId = 666;

  const stream = {
    streamId,
    name: 'e2e-consumer-group-stream'
  };

  const topic = {
    streamId,
    topicId,
    name: 'e2e-consumer-group-topic',
    partitionCount: 3,
    compressionAlgorithm: 1
  };

  let payloadLength = 0;

  await c.stream.create(stream);
  await c.topic.create(topic);

  const groupId = 333;
  const group = { streamId, topicId, groupId, name: 'e2e-cg-1' };

  it('e2e -> consumer-group::create', async () => {
    const r = await c.group.create(group);
    assert.deepEqual(
      r, { id: groupId, name: 'e2e-cg-1', partitionsCount: 3, membersCount: 0 }
    );
  });

  it('e2e -> consumer-group::get', async () => {
    const gg = await c.group.get({ streamId, topicId, groupId });
    assert.deepEqual(
      gg, { id: groupId, name: 'e2e-cg-1', partitionsCount: 3, membersCount: 0 }
    );
  });

  it('e2e -> consumer-group::list', async () => {
    const lg = await c.group.list({ streamId, topicId });
    assert.deepEqual(
      lg,
      [{ id: groupId, name: 'e2e-cg-1', partitionsCount: 3, membersCount: 0 }]
    );
  });

  it('e2e -> consumer-stream::send-messages', async () => {
    const ct = 1000;
    const mn = 200;
    for (let i = 0; i <= ct; i += mn) {
      assert.ok(await c.message.send({
        streamId, topicId,
        messages: generateMessages(mn),
        partition: Partitioning.MessageKey(`key-${ i % 300 }`)
      }));
    }
    payloadLength = ct;
  });

  it('e2e -> consumer-group::join', async () => {
    assert.ok(await c.group.join({ streamId, topicId, groupId }));
  });

  it('e2e -> consumer-group::poll', async () => {
    const pollReq = {
      streamId,
      topicId,
      consumer: { kind: ConsumerKind.Group, id: groupId },
      partitionId: 0,
      pollingStrategy: PollingStrategy.Next,
      count: 100,
      autocommit: true
    };
    let ct = 0;
    while (ct < payloadLength) {
      const { messages, ...resp } = await c.message.poll(pollReq);
      // console.log('POLL', messages.length, 'R/C', resp.count, messages, resp, ct);
      assert.equal(messages.length, resp.count);
      ct += messages.length;
    }
    assert.equal(ct, payloadLength);

    const { count } = await c.message.poll(pollReq);
    assert.equal(count, 0);
  });

  it('e2e -> consumer-group::leave', async () => {
    assert.ok(await c.group.leave({ streamId, topicId, groupId }));
  });

  it('e2e -> consumer-group::delete', async () => {
    assert.ok(await c.group.delete({ streamId, topicId, groupId }));
  });

  after(async () => {
    // await c.group.leave({ streamId, topicId, groupId });
    // await c.group.delete({ streamId, topicId, groupId });
    assert.ok(await c.stream.delete(stream));
    assert.ok(await c.session.logout());
    c.destroy();
  });
});
