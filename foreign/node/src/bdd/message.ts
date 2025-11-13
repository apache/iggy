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
import { When, Then } from "@cucumber/cucumber";
import { Consumer,  PollingStrategy, Partitioning } from '../wire/index.js';
import type { TestWorld } from './world.js';

const generateTestMessages = (count = 1) => {
  return [...Array(count)].map((_, i) => ({
    id: i + 1,
    payload: Buffer.from(`Test message ${i}`)
  }));
}

When(
  'I send {int} messages to stream {int}, topic {int}, partition {int}',
  async function (
    this: TestWorld,
    msgCount: number,
    streamId: number,
    topicId: number,
    partitionId: number
  ) {
    this.sendMessages = generateTestMessages(msgCount);
    assert.ok(
      await this.client.message.send({
        streamId,
        topicId,
        messages: this.sendMessages,
        partition: Partitioning.PartitionId(partitionId)
      })
    );
  }
);


Then(
  'all messages should be sent successfully',
  () => true
);

When(
  'I poll messages from stream {int}, topic {int}, partition {int} starting from offset {int}',
  async function (
    this: TestWorld,
    streamId: number,
    topicId: number,
    partitionId: number,
    offset: number
  ) {
    const pollReq = {
      streamId,
      topicId,
      consumer: Consumer.Single,
      partitionId,
      pollingStrategy: PollingStrategy.Offset(BigInt(offset)),
      count: 100,
      autocommit: true
    };
    const { messages } = await this.client.message.poll(pollReq);
    this.polledMessages = messages;
    assert.equal(this.polledMessages.length, this.sendMessages.length);
  }
);

Then(
  'I should receive {int} messages',
  function (this: TestWorld, msgCount: number) {
    assert.equal(this.polledMessages.length, msgCount);
  }
);

Then(
  'the messages should have sequential offsets from {int} to {int}',
  function (this: TestWorld, from: number, to: number) {
    for (let i = from; i < to; i++) {
      assert.equal(BigInt(i), this.polledMessages[i].headers.offset)
    }
  }
);

Then(
  'each message should have the expected payload content',
  function (this: TestWorld) {
    this.sendMessages.forEach((msg, i) => {
      assert.deepEqual(msg.payload.toString(), this.polledMessages[i].payload.toString());
      assert.equal(BigInt(msg.id || 0), this.polledMessages[i].headers.id)
    })
  }
);

Then(
  'the last polled message should match the last sent message',
  function (this: TestWorld) {
    const lastSent = this.sendMessages[this.sendMessages.length - 1];
    const lastPoll = this.polledMessages[this.polledMessages.length - 1];
    assert.deepEqual(lastSent.payload.toString(), lastPoll.payload.toString());
    assert.deepEqual(lastSent.headers || {}, lastPoll.userHeaders);
  }
);
