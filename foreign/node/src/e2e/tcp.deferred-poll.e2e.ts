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
import { setTimeout as sleep } from 'node:timers/promises';
import { SingleClient } from '../client/client.js';
import { singleConsumerStream } from '../stream/consumer-stream.js';
import {
  Consumer, PollingStrategy, Partitioning, syncGroup, type PollMessagesResponse
} from '../wire/index.js';
import { generateMessages, getIggyAddress } from '../tcp.sm.utils.js';

describe('e2e -> deferred-poll', async () => {
  const [host, port] = getIggyAddress();
  const config = {
    transport: 'TCP' as const,
    options: { host, port },
    credentials: { username: 'iggy', password: 'iggy' }
  };
  const PARTITIONS = 3;
  const LONG_WAIT_MS = 10_000;
  const LONG_WAIT = { maxWait: LONG_WAIT_MS, requestTimeout: LONG_WAIT_MS + 1_000 };
  const PROMPT_MS = 2_000;
  const ASSIGNMENT_TIMEOUT_MS = 10_000;
  /** The server parks at most 64 deferred requests of one session on one shard. */
  const MAX_PARKED = 64;

  const streamId = 'e2e-deferred-poll-stream';
  const topicId = 'e2e-deferred-poll-topic';
  const backlogTopicId = 'e2e-deferred-poll-backlog-topic';
  const idleTopicId = 'e2e-deferred-poll-idle-topic';
  const laneTopicId = 'e2e-deferred-poll-lane-topic';
  const groupId = 'e2e-deferred-poll-group';

  const producer = new SingleClient(config);
  const consumer = new SingleClient(config);
  // One fetch reads the partitions of one primary, and one partition more
  // than nodes puts two of them on the same primary.
  const backlogPartitions = (await producer.cluster.getClusterMetadata()).nodes.length + 1;

  await producer.stream.create({ name: streamId });
  await producer.topic.create({ streamId, name: topicId, partitionCount: PARTITIONS, compressionAlgorithm: 1 });
  await producer.topic.create({ streamId, name: idleTopicId, partitionCount: 1, compressionAlgorithm: 1 });
  await producer.topic.create({ streamId, name: laneTopicId, partitionCount: 1, compressionAlgorithm: 1 });
  await producer.topic.create({
    streamId, name: backlogTopicId, partitionCount: backlogPartitions, compressionAlgorithm: 1
  });
  for (const topic of [topicId, backlogTopicId]) {
    await producer.group.create({ streamId, topicId: topic, name: groupId });
    await consumer.group.join({ streamId, topicId: topic, groupId });
  }

  after(async () => {
    await producer.stream.delete({ streamId });
    await producer.session.logout();
    await consumer.session.logout();
    producer.destroy();
    consumer.destroy();
  });

  const send = (topic: string, partition: number, count = 1) => producer.message.send({
    streamId, topicId: topic, messages: generateMessages(count), partition: Partitioning.PartitionId(partition)
  });

  // A member gets its partitions one by one after it joins.
  const waitForAssignment = async (topic: string, partitions: number) => {
    const deadline = Date.now() + ASSIGNMENT_TIMEOUT_MS;
    while ((await syncGroup(consumer.clientProvider)({ streamId, topicId: topic, groupId }))?.partitions.length !==
        partitions) {
      assert.ok(Date.now() < deadline, 'the member did not get every partition');
      await sleep(50);
    }
  };

  it('e2e -> deferred-poll::a group reader reads the partition that holds messages', async () => {
    await waitForAssignment(topicId, PARTITIONS);
    const reader = await consumer.message.reader({
      streamId, topicId, consumer: Consumer.Group(groupId), options: { count: 10, maxWait: LONG_WAIT_MS }
    });
    try {
      while (reader.partitions().length < PARTITIONS) await sleep(50);
      // The reads of every primary wait at once, so none of these waits 10 s on an idle partition.
      for (let partition = PARTITIONS - 1; partition >= 0; partition -= 1) {
        await send(topicId, partition);
        const started = Date.now();
        const batch = await reader.next();
        const elapsed = Date.now() - started;
        assert.equal(batch.partitionId, partition);
        assert.equal(batch.messages.length, 1);
        assert.ok(elapsed < PROMPT_MS, `the read of partition ${partition} took ${elapsed} ms`);
      }
    } finally {
      await reader.close();
    }
  });

  it('e2e -> deferred-poll::a backlog in one partition does not hide a message in another', async () => {
    const BACKLOG = 1_000;
    await waitForAssignment(backlogTopicId, backlogPartitions);
    const assigned = Array.from({ length: backlogPartitions }, (_, partitionId) => partitionId);
    const raw = await consumer.clientProvider();
    const groups = await raw.readinessGroups!(streamId, backlogTopicId, assigned);
    const shared = groups.find((group) => group.length > 1);
    assert.ok(shared, `no primary leads two partitions in ${JSON.stringify(groups)}`);
    const [backlogPartition, latePartition] = shared;
    await send(backlogTopicId, backlogPartition, BACKLOG);
    await send(backlogTopicId, latePartition);
    const reader = await consumer.message.reader({
      streamId, topicId: backlogTopicId, consumer: Consumer.Group(groupId), start: PollingStrategy.Offset(0n),
      options: { count: 10, maxWait: 1_000 }
    });
    try {
      while (reader.partitions().length < backlogPartitions) await sleep(50);
      // The server shares the count of one read among the partitions that hold messages.
      const first = await reader.next();
      const second = await reader.next();
      assert.ok([first.partitionId, second.partitionId].includes(latePartition),
        `read ${first.partitionId} and ${second.partitionId} before ${latePartition}`);
    } finally {
      await reader.close();
    }
  });

  it('e2e -> deferred-poll::a client sends at once to the partition where its progress poll waits', async () => {
    const PARK_MS = 200;
    const polling = consumer.message.poll({
      streamId, topicId: laneTopicId, consumer: Consumer.Single, partitionId: 0,
      pollingStrategy: PollingStrategy.Next, count: 1, autocommit: false, deferred: LONG_WAIT
    });
    await sleep(PARK_MS);
    const started = Date.now();
    await consumer.message.send({
      streamId, topicId: laneTopicId, messages: generateMessages(1), partition: Partitioning.PartitionId(0)
    });
    // One lane for polls and sends would hold the send until the poll ends after 10 s.
    const sent = Date.now() - started;
    assert.ok(sent < PROMPT_MS, `the send took ${sent} ms`);
    assert.equal((await polling).count, 1);
    assert.ok(Date.now() - started < PROMPT_MS);
  });

  it('e2e -> deferred-poll::aborted deferred polls end at once and free their server slots', async () => {
    const poll = (deferred: { maxWait: number, requestTimeout: number }, signal?: AbortSignal) => consumer.message.poll({
      streamId, topicId: idleTopicId, consumer: Consumer.Single, partitionId: 0,
      pollingStrategy: PollingStrategy.Offset(1_000n), count: 1, autocommit: false, deferred, signal
    });
    const controller = new AbortController();
    const parked = Array.from({ length: MAX_PARKED }, () => poll(LONG_WAIT, controller.signal));
    await sleep(500);
    const started = Date.now();
    controller.abort();
    const aborted = await Promise.allSettled(parked);
    assert.ok(aborted.every((result) => result.status === 'rejected' && result.reason?.name === 'AbortError'));
    assert.ok(Date.now() - started < PROMPT_MS);
    // Without the cancellations the server would still park the aborted polls
    // and refuse these with 90 until their deadline.
    const SHORT_WAIT = { maxWait: 500, requestTimeout: 1_500 };
    const next = await Promise.all(Array.from({ length: MAX_PARKED }, () => poll(SHORT_WAIT)));
    assert.ok(next.every(({ count }) => count === 0));
  });

  it('e2e -> deferred-poll::a consumer stream delivers a message at once after an idle period', async () => {
    const IDLE_MS = 1_500;
    const stream = await singleConsumerStream(config)({
      streamId, topicId: idleTopicId, partitionId: 0, pollingStrategy: PollingStrategy.Next,
      count: 10, pollOptions: LONG_WAIT
    });
    try {
      const received: { response: PollMessagesResponse, at: number }[] = [];
      stream.on('data', (response: PollMessagesResponse) => received.push({ response, at: Date.now() }));
      await sleep(IDLE_MS);
      // The poll waits on the server, so an idle stream sends no poll after poll.
      assert.equal(received.length, 0);
      await send(idleTopicId, 0);
      const sentAt = Date.now();
      while (received.length === 0) {
        assert.ok(Date.now() - sentAt < PROMPT_MS, 'the stream did not deliver the message');
        await sleep(10);
      }
      assert.equal(received[0].response.count, 1);
      assert.ok(received[0].at - sentAt < PROMPT_MS / 2, `delivered after ${received[0].at - sentAt} ms`);
    } finally {
      stream.destroy();
    }
  });
});
