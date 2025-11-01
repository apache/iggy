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
import { Consumer, PollingStrategy, Partitioning } from '../wire/index.js';
import { generateMessages } from '../tcp.sm.utils.js';
import { getIggyAddress } from '../tcp.sm.utils.js';

describe('e2e -> consumer-group', async () => {

  const [host, port] = getIggyAddress();

  const c = new SingleClient({
    transport: 'TCP',
    options: { host, port },
    credentials: { username: 'iggy', password: 'iggy' }
  });

  const stream = {
    name: 'e2e-consumer-group-stream'
  };

  let payloadLength = 0;

  const STREAM = await c.stream.create(stream);
  
  const topic = {
    streamId: STREAM.id,
    name: 'e2e-consumer-group-topic',
    partitionCount: 3,
    compressionAlgorithm: 1
  };

  const TOPIC = await c.topic.create(topic);
  
  const group = { streamId: STREAM.id, topicId: TOPIC.id, name: 'e2e-cg-1' };

    const GROUP = await c.group.create(group);
    assert.deepEqual(
      GROUP, { id: 0, name: 'e2e-cg-1', partitionsCount: 3, membersCount: 0 }
    );

  it('e2e -> consumer-group::get', async () => {
    const gg = await c.group.get({
      streamId: STREAM.id, topicId: TOPIC.id, groupId: GROUP.id
    });
    assert.deepEqual(
      gg, { id: GROUP.id, name: 'e2e-cg-1', partitionsCount: 3, membersCount: 0 }
    );
  });

  it('e2e -> consumer-group::list', async () => {
    const lg = await c.group.list({ streamId: STREAM.id, topicId: TOPIC.id });
    assert.deepEqual(
      lg,
      [{ id: GROUP.id, name: 'e2e-cg-1', partitionsCount: 3, membersCount: 0 }]
    );
  });

  it('e2e -> consumer-stream::send-messages', async () => {
    const ct = 1000;
    const mn = 200;
    for (let i = 0; i <= ct; i += mn) {
      assert.ok(await c.message.send({
        streamId: STREAM.id,
        topicId: TOPIC.id,
        messages: generateMessages(mn),
        partition: Partitioning.MessageKey(`key-${ i % 300 }`)
      }));
    }
    payloadLength = ct;
  });

  it('e2e -> consumer-group::join', async () => {
    assert.ok(await c.group.join({
      streamId: STREAM.id, topicId: TOPIC.id, groupId: GROUP.id
    }));
  });

  // it('e2e -> consumer-group::poll', async () => {
  //   const pollReq = {
  //     streamId: STREAM.id,
  //     topicId: TOPIC.id,
  //     consumer: Consumer.Group(GROUP.id),
  //     partitionId: 0,
  //     pollingStrategy: PollingStrategy.Next,
  //     count: 100,
  //     autocommit: true
  //   };
  //   let ct = 0;
  //   while (ct < payloadLength) {
  //     const { messages, ...resp } = await c.message.poll(pollReq);
  //     // console.log('POLL', messages.length, 'R/C', resp.count, messages, resp, ct);
  //     assert.equal(messages.length, resp.count);
  //     ct += messages.length;
  //   }
  //   assert.equal(ct, payloadLength);

  //   const { count } = await c.message.poll(pollReq);
  //   assert.equal(count, 0);
  // });

  it('e2e -> consumer-group::leave', async () => {
    assert.ok(await c.group.leave({
      streamId: STREAM.id, topicId: TOPIC.id, groupId: GROUP.id
    }));
  });

  it('e2e -> consumer-group::delete', async () => {
    assert.ok(await c.group.delete({
      streamId: STREAM.id, topicId: TOPIC.id, groupId: GROUP.id
    }));
  });

  after(async () => {
    assert.ok(await c.stream.delete({ streamId: STREAM.id }));
    assert.ok(await c.session.logout());
    c.destroy();
  });
});
