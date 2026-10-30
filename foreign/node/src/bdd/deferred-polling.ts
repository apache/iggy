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

import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { After, Given, When, Then } from '@cucumber/cucumber';
import { VsrResponseTimeoutError, waitWithin } from '../client/client.operation.js';
import { ConsumerKind } from '../wire/offset/offset.utils.js';
import {
  CommitAfter, CommitPolicy, Consumer, Partitioning, PollingStrategy,
  type PollMessagesResponse, type PollOptions, type PollPartition
} from '../wire/index.js';
import type { BackgroundCall, Outcome, TestWorld } from './world.js';

const MS_PER_SECOND = 1_000;
const POLL_COUNT = 10;
/** A cancelled poll that commits ends once the server answers its cancel. */
const CANCELLED_END_MS = 2_000;
const ABORT_ERROR = 'AbortError';
const COMMIT_INTERVAL_MS = 5_000;

const waitOptions = (maxWaitSeconds: number): Partial<PollOptions> =>
  ({ count: POLL_COUNT, maxWait: maxWaitSeconds * MS_PER_SECOND });

const namedConsumer = (name: string): Consumer => ({ kind: ConsumerKind.Single, id: name });

const poll = (world: TestWorld, request: Omit<PollPartition, 'streamId' | 'topicId'>) =>
  world.client.message.poll({ streamId: world.stream.id, topicId: world.topic.id, ...request });

const startInBackground = <T>(call: (signal: AbortSignal) => Promise<T>): BackgroundCall<T> => {
  const cancel = new AbortController();
  const started = Date.now();
  const outcome = call(cancel.signal).then(
    (value): Outcome<T> => ({ ended: Date.now(), ok: true, value }),
    (error: unknown): Outcome<T> => ({ ended: Date.now(), ok: false, error })
  );
  return { started, cancel, outcome };
};

const requireStarted = <T>(call: BackgroundCall<T> | undefined, what: string): BackgroundCall<T> => {
  assert.ok(call, `no ${what} was started`);
  return call;
};

/** The outcome of `call` by `deadline`. A call still running then is cancelled and fails the step. */
const endBy = async <T>(call: BackgroundCall<T>, deadline: number, what: string): Promise<Outcome<T>> => {
  try {
    return await waitWithin(call.outcome, deadline);
  } catch (error) {
    if (!(error instanceof VsrResponseTimeoutError)) throw error;
    call.cancel.abort();
    assert.fail(`${what} did not end in time`);
  }
};

const within = async <T>(promise: Promise<T>, seconds: number, what: string): Promise<T> => {
  try {
    return await waitWithin(promise, Date.now() + seconds * MS_PER_SECOND);
  } catch (error) {
    if (error instanceof VsrResponseTimeoutError) assert.fail(`${what} did not return in time`);
    throw error;
  }
};

const valueOrThrow = <T>(outcome: Outcome<T>): T => {
  if (!outcome.ok) throw outcome.error;
  return outcome.value;
};

const assertOnlyPayload = (response: PollMessagesResponse, payload: string) =>
  assert.deepEqual(response.messages.map((message) => message.payload.toString()), [payload]);

const closeConsumer = async (world: TestWorld): Promise<void> => {
  const consumer = world.consumer;
  world.consumer = undefined;
  world.consumerMessages = undefined;
  await consumer?.shutdown();
};

const openGroupConsumer = async (world: TestWorld, name: string, options: Partial<PollOptions>): Promise<void> => {
  world.consumer = await world.client.consumerGroup({
    name, streamId: world.stream.id, topicId: world.topic.id, start: PollingStrategy.Next, options
  });
  world.consumerMessages = world.consumer[Symbol.asyncIterator]();
};

After(async function (this: TestWorld) {
  await closeConsumer(this);
  await this.reader?.close();
  this.reader = undefined;
});

Given(
  'a stream named {string} with a topic named {string} with {int} partitions exists',
  async function (this: TestWorld, streamName: string, topicName: string, partitionCount: number) {
    const existing = await this.client.stream.get({ streamId: streamName });
    if (existing) assert.ok(await this.client.stream.delete({ streamId: existing.id }));
    this.stream = await this.client.stream.create({ name: streamName });
    this.topic = await this.client.topic.create({ streamId: this.stream.id, name: topicName, partitionCount });
  }
);

When(
  'I start a deferred poll of partition {int} from offset {int} with a max wait of {int} second(s)',
  function (this: TestWorld, partitionId: number, offset: number, maxWait: number) {
    this.deferredPoll = startInBackground((signal) => poll(this, {
      consumer: Consumer.Single, partitionId, strategy: PollingStrategy.Offset(BigInt(offset)),
      options: waitOptions(maxWait), signal
    }));
  }
);

When(
  'I send a message with payload {string} to partition {int}',
  async function (this: TestWorld, payload: string, partitionId: number) {
    assert.ok(await this.client.message.send({
      streamId: this.stream.id, topicId: this.topic.id,
      partition: Partitioning.PartitionId(partitionId), messages: [{ payload }]
    }));
    this.lastSentPartition = partitionId;
  }
);

Then(
  'the deferred poll should return the payload {string} within {int} second(s)',
  async function (this: TestWorld, payload: string, seconds: number) {
    const call = requireStarted(this.deferredPoll, 'deferred poll');
    const outcome = await endBy(call, Date.now() + seconds * MS_PER_SECOND, 'the deferred poll');
    assertOnlyPayload(valueOrThrow(outcome), payload);
  }
);

Then(
  'the deferred poll should return no messages between {int} and {int} seconds after it started',
  async function (this: TestWorld, from: number, to: number) {
    const call = requireStarted(this.deferredPoll, 'deferred poll');
    const outcome = await endBy(call, call.started + to * MS_PER_SECOND, 'the deferred poll');
    assert.equal(valueOrThrow(outcome).messages.length, 0);
    const elapsed = outcome.ended - call.started;
    assert.ok(elapsed >= from * MS_PER_SECOND, `the poll ended after ${elapsed} ms`);
  }
);

Then(
  'a poll of partition {int} for consumer {string} with the next strategy and the on read commit policy should return the payload {string}',
  async function (this: TestWorld, partitionId: number, consumer: string, payload: string) {
    const polled = await poll(this, {
      consumer: namedConsumer(consumer), partitionId, strategy: PollingStrategy.Next,
      options: { count: 1, commit: CommitPolicy.OnRead }
    });
    assertOnlyPayload(polled, payload);
  }
);

When(
  'I start a deferred poll of partition {int} for consumer {string} with the on read commit policy, a min count of {int} and a max wait of {int} second(s)',
  function (this: TestWorld, partitionId: number, consumer: string, minCount: number, maxWait: number) {
    this.deferredPoll = startInBackground((signal) => poll(this, {
      consumer: namedConsumer(consumer), partitionId, strategy: PollingStrategy.Next,
      options: { ...waitOptions(maxWait), minCount, commit: CommitPolicy.OnRead }, signal
    }));
  }
);

// The server answers a cancelled poll that commits once, refused when the cancel won, so the poll ends as aborted.
When('I cancel the deferred poll after {int} milliseconds', async function (this: TestWorld, delay: number) {
  const call = requireStarted(this.deferredPoll, 'deferred poll');
  await sleep(delay);
  call.cancel.abort();
  const outcome = await endBy(call, Date.now() + CANCELLED_END_MS, 'the cancelled poll');
  assert.ok(!outcome.ok, 'the cancelled poll returned a result');
  assert.ok(outcome.error instanceof Error && outcome.error.name === ABORT_ERROR,
    `the cancelled poll failed with ${String(outcome.error)}`);
});

When('I wait {int} second(s)', (seconds: number) => sleep(seconds * MS_PER_SECOND));

Then(
  'a poll of partition {int} for consumer {string} with the next strategy should return the payload {string}',
  async function (this: TestWorld, partitionId: number, consumer: string, payload: string) {
    const { messages } = await poll(this, {
      consumer: namedConsumer(consumer), partitionId, strategy: PollingStrategy.Next
    });
    assert.ok(messages.length > 0, 'the poll returned no message');
    assert.equal(messages[0].payload.toString(), payload);
  }
);

When(
  'I open a partition reader on partitions {int}, {int} and {int} from offset {int} with a max wait of {int} second(s)',
  async function (this: TestWorld, first: number, second: number, third: number, offset: number, maxWait: number) {
    this.reader = await this.client.message.reader({
      streamId: this.stream.id, topicId: this.topic.id, consumer: Consumer.Single,
      partitions: [first, second, third], start: PollingStrategy.Offset(BigInt(offset)), options: waitOptions(maxWait)
    });
  }
);

Then(
  'the partition reader should return the payload {string} from partition {int} within {int} second(s)',
  async function (this: TestWorld, payload: string, partitionId: number, seconds: number) {
    assert.ok(this.reader, 'no partition reader was opened');
    const batch = await within(this.reader.next(), seconds, 'the partition reader');
    assert.equal(batch.partitionId, partitionId);
    assertOnlyPayload(batch, payload);
  }
);

When(
  'I open a consumer in group {string} on the topic with a max wait of {int} second(s)',
  function (this: TestWorld, name: string, maxWait: number) {
    return openGroupConsumer(this, name, waitOptions(maxWait));
  }
);

When(
  'I open a consumer in group {string} on the topic that commits after processing',
  function (this: TestWorld, name: string) {
    return openGroupConsumer(this, name, {
      maxWait: 5 * MS_PER_SECOND,
      commit: CommitPolicy.Auto({ interval: COMMIT_INTERVAL_MS, after: CommitAfter.Processing })
    });
  }
);

Then(
  'the consumer should return the payload {string} within {int} second(s)',
  async function (this: TestWorld, payload: string, seconds: number) {
    assert.ok(this.consumerMessages, 'no consumer was opened');
    const next = await within(this.consumerMessages.next(), seconds, 'the consumer');
    assert.ok(!next.done, 'the consumer ended');
    assert.equal(next.value.message.payload.toString(), payload);
  }
);

When('I close the consumer', function (this: TestWorld) {
  return closeConsumer(this);
});
