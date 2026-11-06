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
import { Given, When, Then } from '@cucumber/cucumber';
import {
  Consumer, HeaderKeyFactory, HeaderValue, OptionsScope, Partitioning, PollingStrategy,
  StreamEncryptionKey, encryptionKeyFingerprint,
  type OptionSpec, type StreamOptionKey
} from '../wire/index.js';
import type { Headers } from '../wire/message/header.utils.js';
import type { TestWorld } from './world.js';

const NEVER_RETURNED = ['encryption_key', 'encryption_key_wrapped', 'critical_keys'];

const createStream = async (world: TestWorld, name: string, key?: Buffer) => {
  world.stream = await world.client.stream.create({
    name, encryptionKey: key === undefined ? undefined : StreamEncryptionKey.from(key)
  });
  world.streamKey = key;
};

const fetchStream = async (world: TestWorld) => {
  const stream = await world.client.stream.get({ streamId: world.stream.id });
  assert.ok(stream, 'the stream exists');
  return stream;
};

const optionSpec = (world: TestWorld, key: string): OptionSpec => {
  const spec = world.optionSpecs?.find((candidate) => candidate.key === key);
  assert.ok(spec, `the server describes ${key}`);
  return spec;
};

const sendMessage = async (world: TestWorld, payload: string, headers: Headers = []) => {
  assert.ok(await world.client.message.send({
    streamId: world.stream.id,
    topicId: world.topic.id,
    partition: Partitioning.PartitionId(0),
    messages: [{ payload, headers }]
  }));
};

const pollFirst = async (world: TestWorld) => {
  const { messages } = await world.client.message.poll({
    streamId: world.stream.id,
    topicId: world.topic.id,
    consumer: Consumer.Single,
    partitionId: 0,
    pollingStrategy: PollingStrategy.First,
    count: 1,
    autocommit: false
  });
  assert.equal(messages.length, 1);
  return messages[0];
};

When(
  'I create a stream named {string} with an encryption key of 32 bytes valued {int}',
  async function (this: TestWorld, name: string, byte: number) {
    await createStream(this, name, Buffer.alloc(32, byte));
  }
);

Given(
  'a stream named {string} with an encryption key of 32 bytes valued {int} exists',
  async function (this: TestWorld, name: string, byte: number) {
    await createStream(this, name, Buffer.alloc(32, byte));
  }
);

When(
  'I create a stream named {string} without an encryption key',
  async function (this: TestWorld, name: string) {
    await createStream(this, name);
  }
);

Then(
  'the stream should report encryption key source {string}',
  async function (this: TestWorld, source: string) {
    assert.equal((await fetchStream(this)).encryptionKeySource, source);
  }
);

Then(
  'the stream encryption key fingerprint should match the key',
  async function (this: TestWorld) {
    assert.ok(this.streamKey, 'the stream was created with a key');
    assert.equal(
      (await fetchStream(this)).encryptionKeyFingerprint,
      encryptionKeyFingerprint(this.streamKey)
    );
  }
);

Then('the stream should report an encryption key fingerprint', async function (this: TestWorld) {
  assert.match((await fetchStream(this)).encryptionKeyFingerprint ?? '', /^[0-9a-f]{16}$/);
});

Then('the stream should report at least one KEK fingerprint', async function (this: TestWorld) {
  assert.ok((await fetchStream(this)).encryptionKekFingerprints.length >= 1);
});

Then('the stream should expose no encryption key material', async function (this: TestWorld) {
  const stream = await fetchStream(this);
  for (const block of [stream.options, stream.derivedOptions]) {
    for (const key of NEVER_RETURNED)
      assert.equal(key in block, false, `${key} must never be returned`);
    for (const value of Object.values(block))
      assert.ok(!(Buffer.isBuffer(value) && this.streamKey?.equals(value)), 'a value holds the key');
  }
});

When(
  'I create a topic named {string} with {int} partition(s) in the stream',
  async function (this: TestWorld, name: string, partitionCount: number) {
    this.topic = await this.client.topic.create({
      streamId: this.stream.id, name, partitionCount
    });
  }
);

When(
  'I send a message with payload {string} to the topic',
  async function (this: TestWorld, payload: string) {
    await sendMessage(this, payload);
  }
);

When(
  'I send a message with payload {string} and header {string} set to {string} to the topic',
  async function (this: TestWorld, payload: string, header: string, value: string) {
    await sendMessage(this, payload, [
      { key: HeaderKeyFactory.String(header), value: HeaderValue.String(value) }
    ]);
  }
);

Then(
  'polling the topic should return the payload {string}',
  async function (this: TestWorld, payload: string) {
    assert.equal((await pollFirst(this)).payload.toString(), payload);
  }
);

Then(
  'polling the topic should return the payload {string} and header {string} set to {string}',
  async function (this: TestWorld, payload: string, header: string, value: string) {
    const message = await pollFirst(this);
    assert.equal(message.payload.toString(), payload);
    const entry = message.userHeaders.find((candidate) => candidate.key.value === header);
    assert.ok(entry, `header ${header} is present`);
    assert.equal(entry.value.value, value);
  }
);

When('I rename the stream to {string}', async function (this: TestWorld, name: string) {
  assert.ok(await this.client.stream.update({ streamId: this.stream.id, name }));
});

Then('the stream name should be {string}', async function (this: TestWorld, name: string) {
  assert.equal((await fetchStream(this)).name, name);
});

When(
  'I request a reset of the stream option {string}',
  async function (this: TestWorld, key: string) {
    try {
      await this.client.stream.update({
        streamId: this.stream.id, reset: [key as StreamOptionKey]
      });
      this.updateError = undefined;
    } catch (error) {
      this.updateError = error;
    }
  }
);

Then('the stream update should be rejected', function (this: TestWorld) {
  assert.ok(this.updateError, 'the update must fail');
});

When('I describe the stream options', async function (this: TestWorld) {
  this.optionSpecs = await this.client.system.describeOptions({ scope: OptionsScope.Stream });
});

Then(
  'the stream option {string} should be updatable and not secret',
  function (this: TestWorld, key: string) {
    const spec = optionSpec(this, key);
    assert.ok(spec.updatable && !spec.secret && !spec.readOnly);
  }
);

Then(
  'the stream option {string} should be create only and secret',
  function (this: TestWorld, key: string) {
    const spec = optionSpec(this, key);
    assert.ok(!spec.updatable && !spec.resettable && spec.secret && !spec.readOnly);
  }
);

Then('the stream option {string} should be read only', function (this: TestWorld, key: string) {
  const spec = optionSpec(this, key);
  assert.ok(spec.readOnly && !spec.updatable && !spec.secret);
});
