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
import { Given, When, Then } from '@cucumber/cucumber';
import { VsrResponseTimeoutError, waitWithin } from '../client/client.operation.js';
import { ResponseError } from '../wire/error.utils.js';
import { ConsumerKind } from '../wire/offset/offset.utils.js';
import {
  Consumer, DEFAULT_DEFERRED_POLL_OPTIONS, MIN_DEFERRED_POLL_REPLY_MARGIN_US, Partitioning,
  PollingStrategy, US_PER_MS, decodeFetchSection, syncGroup,
  type PollMessages, type PollMessagesResponse
} from '../wire/index.js';
import type { BackgroundCall, Outcome, TestWorld } from './world.js';

const MS_PER_SECOND = 1_000;
const POLL_COUNT = 10;
const REPLY_MARGIN_MS = Number(MIN_DEFERRED_POLL_REPLY_MARGIN_US / US_PER_MS);
const ASSIGNMENT_TIMEOUT_MS = 10_000;
const ASSIGNMENT_RETRY_MS = 50;
/** An abort ends the call at once, so this only bounds a broken cancellation. */
const CANCELLED_END_MS = 1_000;
const TRANSIENT_NOT_COMMITTED = 57;
const ABORT_ERROR = 'AbortError';

/** The wait, and a request timeout that leaves the SDK reply margin after it. */
const waitOptions = (maxWaitSeconds: number) => {
  const maxWait = maxWaitSeconds * MS_PER_SECOND;
  return { maxWait, requestTimeout: Math.max(DEFAULT_DEFERRED_POLL_OPTIONS.requestTimeout, maxWait + REPLY_MARGIN_MS) };
};

const namedConsumer = (name: string): Consumer => ({ kind: ConsumerKind.Single, id: name });

const poll = (world: TestWorld, request: Omit<PollMessages, 'streamId' | 'topicId' | 'count'>) =>
  world.client.message.poll({ streamId: world.stream.id, topicId: world.topic.id, count: POLL_COUNT, ...request });

const fetchReady = async (world: TestWorld, consumer: Consumer, partitions: number[], pollingStrategy: PollingStrategy,
  maxWait: number, signal: AbortSignal): Promise<PollMessagesResponse[]> => {
  const topic = { streamId: world.stream.id, topicId: world.topic.id };
  const cancel = new AbortController();
  const stopped = AbortSignal.any([signal, cancel.signal]);
  try {
    const groups = await world.client.message.readinessGroups({ ...topic, partitions, signal: stopped });
    return await Promise.any(groups.map(async (group) => {
      const sections = await world.client.message.fetch({
        ...topic, consumer, count: POLL_COUNT, autocommit: false, options: waitOptions(maxWait), signal: stopped,
        targets: group.map((partitionId) => ({ partitionId, pollingStrategy }))
      });
      const replies = sections.map(decodeFetchSection).filter(({ count }) => count > 0);
      assert.ok(replies.length > 0, 'the fetch returned no messages');
      return replies;
    }));
  } finally {
    cancel.abort();
  }
};

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

const valueOrThrow = <T>(outcome: Outcome<T>): T => {
  if (!outcome.ok) throw outcome.error;
  return outcome.value;
};

const assertOnlyPayload = (response: PollMessagesResponse, payload: string) =>
  assert.deepEqual(response.messages.map((message) => message.payload.toString()), [payload]);

/**
 * An auto-commit poll that reached the server ends with 57 and the abort as
 * its cause, because the client cannot know whether the server committed its offset.
 */
const isCancellation = (error: unknown): boolean =>
  error instanceof Error && (error.name === ABORT_ERROR ||
    (error instanceof ResponseError && error.errorCode === TRANSIENT_NOT_COMMITTED &&
      error.cause instanceof Error && error.cause.name === ABORT_ERROR));

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
      consumer: Consumer.Single, partitionId, pollingStrategy: PollingStrategy.Offset(BigInt(offset)),
      autocommit: false, deferred: waitOptions(maxWait), signal
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

When(
  'I start a fetch on partitions {int}, {int} and {int} from offset {int} with a max wait of {int} second(s)',
  function (this: TestWorld, first: number, second: number, third: number, offset: number, maxWait: number) {
    this.deferredFetch = startInBackground((signal) => fetchReady(this, Consumer.Single, [first, second, third],
      PollingStrategy.Offset(BigInt(offset)), maxWait, signal));
  }
);

Then(
  'the fetch should return only partition {int} within {int} second(s)',
  async function (this: TestWorld, partitionId: number, seconds: number) {
    const call = requireStarted(this.deferredFetch, 'fetch');
    const outcome = await endBy(call, Date.now() + seconds * MS_PER_SECOND, 'the fetch');
    const replies = valueOrThrow(outcome);
    assert.deepEqual(replies.map((reply) => reply.partitionId), [partitionId]);
    assert.equal(replies[0].messages.length, 1);
  }
);

// The assignment can take longer than the default step timeout of cucumber.
Given(
  'I am the only member of consumer group {string} on the topic',
  { timeout: 2 * ASSIGNMENT_TIMEOUT_MS },
  async function (this: TestWorld, name: string) {
    const topic = { streamId: this.stream.id, topicId: this.topic.id };
    await this.client.group.create({ ...topic, name });
    assert.ok(await this.client.group.join({ ...topic, groupId: name }));
    // A member gets its partitions one by one after it joins.
    const deadline = Date.now() + ASSIGNMENT_TIMEOUT_MS;
    while ((await syncGroup(this.client.clientProvider)({ ...topic, groupId: name }))?.partitions.length !==
        this.topic.partitionsCount) {
      assert.ok(Date.now() < deadline, 'the member did not get every partition');
      await sleep(ASSIGNMENT_RETRY_MS);
    }
    this.groupName = name;
  }
);

Then(
  'a group fetch with a max wait of {int} second(s) should return the payload {string} within {int} second(s)',
  async function (this: TestWorld, maxWait: number, payload: string, seconds: number) {
    const group = this.groupName;
    assert.ok(group, 'the client joined no consumer group');
    const topic = { streamId: this.stream.id, topicId: this.topic.id };
    const assigned = await syncGroup(this.client.clientProvider)({ ...topic, groupId: group });
    assert.ok(assigned, 'the client has no group assignment');
    const consumer = Consumer.Group(group);
    const call = startInBackground((signal) => fetchReady(this, consumer, assigned.partitions,
      PollingStrategy.Next, maxWait, signal));
    const outcome = await endBy(call, call.started + seconds * MS_PER_SECOND, 'the group fetch');
    const replies = valueOrThrow(outcome);
    assert.deepEqual(replies.map(({ partitionId }) => partitionId), [this.lastSentPartition]);
    assertOnlyPayload(replies[0], payload);
    for (const { partitionId, context, messages } of replies)
      await this.client.offset.store({ ...topic, consumer, partitionId, context, offset: messages.at(-1)!.headers.offset });
  }
);

When(
  'I start a deferred poll of partition {int} for consumer {string} with auto commit, a min count of {int} and a max wait of {int} second(s)',
  function (this: TestWorld, partitionId: number, consumer: string, minCount: number, maxWait: number) {
    this.deferredPoll = startInBackground((signal) => poll(this, {
      consumer: namedConsumer(consumer), partitionId, pollingStrategy: PollingStrategy.Next,
      autocommit: true, deferred: { ...waitOptions(maxWait), minCount }, signal
    }));
  }
);

When('I cancel the deferred poll after {int} milliseconds', async function (this: TestWorld, delay: number) {
  const call = requireStarted(this.deferredPoll, 'deferred poll');
  await sleep(delay);
  call.cancel.abort();
  const outcome = await endBy(call, Date.now() + CANCELLED_END_MS, 'the cancelled poll');
  assert.ok(!outcome.ok, 'the cancelled poll returned a result');
  assert.ok(isCancellation(outcome.error), `the cancelled poll failed with ${String(outcome.error)}`);
});

When('I wait {int} second(s)', (seconds: number) => sleep(seconds * MS_PER_SECOND));

Then(
  'a poll of partition {int} for consumer {string} with the next strategy should return the payload {string}',
  async function (this: TestWorld, partitionId: number, consumer: string, payload: string) {
    const { messages } = await poll(this, {
      consumer: namedConsumer(consumer), partitionId, pollingStrategy: PollingStrategy.Next, autocommit: false
    });
    assert.ok(messages.length > 0, 'the poll returned no message');
    assert.equal(messages[0].payload.toString(), payload);
  }
);
