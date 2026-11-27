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
import { once } from 'node:events';
import { Client } from '../client/client.js';
import { groupConsumerStream } from '../stream/consumer-stream.js';
import {
  PollingStrategy, Partitioning,
  type PollMessagesResponse
} from '../wire/index.js';
import { sendSomeMessages, getIggyAddress } from '../tcp.sm.utils.js';


describe('e2e -> consumer-stream', async () => {

  const [host, port] = getIggyAddress();
  const credentials = { username: 'iggy', password: 'iggy' };

  const opt = {
    transport: 'TCP' as const,
    options: { host, port },
    credentials
  };

  const c = new Client(opt);

  const groupName = 'cgtest-12';
  const streamName = `e2e-stream-consumer-stream`;
  const topicName = 'test-topic-stream-cg';

  await c.stream.create({ name: streamName });
  await c.topic.create({
    streamId: streamName,
    name: topicName,
    partitionCount: 3,
    compressionAlgorithm: 1
  });

  const ct = 1000;

  after(async () => {
    assert.ok(await c.stream.delete({ streamId: streamName }));
    assert.ok(await c.session.logout());
    c.destroy();
  });

  it('e2e -> consumer-stream::send-messages', async () => {
    for (let i = 0; i < ct; i += 100) {
      await sendSomeMessages(c.clientProvider)(
        streamName,
        topicName,
        Partitioning.PartitionId((i / 100) % 3)
      );
    }
  });

  it('e2e -> consumer-stream::poll-stream', async () => {
    // POLL MESSAGE
    const pollReq = {
      groupName,
      streamId: streamName,
      topicId: topicName,
      pollingStrategy: PollingStrategy.Next,
      count: 100
    };

    const pollStream = groupConsumerStream(opt);
    const s = await pollStream(pollReq);
    const s2 = await pollStream(pollReq);
    const received = new Set<string>();
    try {
      await new Promise<void>((resolve, reject) => {
        for (const stream of [s, s2]) {
          stream.once('error', reject);
          stream.on('data', ({ partitionId, messages, count }: PollMessagesResponse) => {
            try {
              assert.equal(messages.length, count);
              for (const { headers: { offset } } of messages) {
                const key = `${partitionId}:${offset}`;
                assert.ok(!received.has(key), `message ${key} was delivered twice`);
                received.add(key);
              }
              if (received.size === ct) resolve();
            } catch (error) { reject(error); }
          });
        }
      });
      assert.equal(received.size, ct);
    } finally {
      await Promise.all([s, s2].map(async (stream) => {
        const closed = once(stream, 'close');
        stream.destroy();
        await closed;
      }));
    }
  });
});
