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

import { randomBytes } from 'node:crypto';
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import type { Client } from '../client/client.js';
import { HeaderValue } from '../wire/message/header.utils.js';
import { ResponseError } from '../wire/error.utils.js';
import {
  Consumer, EncryptionKeySource, OptionsScope, PartitionResizePolicy, Partitioning,
  PollingStrategy, StreamEncryptionKey, UserStatus, encryptionKeyFingerprint, type ParsedOptions
} from '../wire/index.js';
import type { Stream } from '../wire/stream/stream.utils.js';

const NEVER_RETURNED = ['encryption_key', 'encryption_key_wrapped', 'critical_keys'];
const FINGERPRINT = /^[0-9a-f]{16}$/;
const UNLIMITED = 18446744073709551615n;
const UNSUPPORTED_OPTION_KEY = 4041;
const OPTION_NOT_UPDATABLE = 4103;

type Credentials = { username: string, password: string };

const assertProjection = (
  stream: Stream, source: EncryptionKeySource, fingerprint?: string
) => {
  assert.equal(stream.encryptionKeySource, source);
  if (source === EncryptionKeySource.None) {
    assert.equal(stream.encryptionKeyFingerprint, null);
    assert.deepEqual(stream.encryptionKekFingerprints, []);
    return;
  }
  assert.match(stream.encryptionKeyFingerprint ?? '', FINGERPRINT);
  if (fingerprint !== undefined)
    assert.equal(stream.encryptionKeyFingerprint, fingerprint);
  assert.ok(stream.encryptionKekFingerprints.length >= 1);
  for (const kek of stream.encryptionKekFingerprints)
    assert.match(kek, FINGERPRINT);
};

const assertNoKeyMaterial = (stream: Stream, key?: Buffer) => {
  for (const block of [stream.options, stream.derivedOptions] as ParsedOptions[]) {
    for (const name of NEVER_RETURNED)
      assert.equal(name in block, false, `${name} was returned`);
    if (key)
      for (const value of Object.values(block))
        assert.ok(
          !(Buffer.isBuffer(value) && value.equals(key)) && value !== key.toString('base64'),
          'a returned option holds the key'
        );
  }
};

/**
 * Registers the resource options cases for one transport. `connect` builds a
 * client of that transport signed in with the given credentials.
 * `keylessSource` is what a stream created without a key reports: `server`
 * when the server runs with encryption enabled, `none` otherwise.
 */
export const describeResourceOptions = (
  label: string, prefix: string, connect: (credentials: Credentials) => Client,
  keylessSource: EncryptionKeySource, skip: string | false = false
) => describe(label, { skip }, async () => {

  const c = connect({ username: 'iggy', password: 'iggy' });

  it('given stream key when creating should read matching projection', async () => {
    const key = randomBytes(32);
    const name = `${prefix}-keyed`;
    const created = await c.stream.create({ name, encryptionKey: StreamEncryptionKey.from(key) });
    try {
      assertProjection(created, EncryptionKeySource.Stream, encryptionKeyFingerprint(key));
      assertNoKeyMaterial(created, key);

      const fetched = await c.stream.get({ streamId: name });
      assert.ok(fetched);
      assertProjection(fetched, EncryptionKeySource.Stream, encryptionKeyFingerprint(key));
      assert.deepEqual(fetched.encryptionKekFingerprints, created.encryptionKekFingerprints);
      assertNoKeyMaterial(fetched, key);

      const listed = (await c.stream.list()).find((stream) => stream.id === created.id);
      assert.ok(listed);
      assertProjection(listed, EncryptionKeySource.Stream, encryptionKeyFingerprint(key));
      assertNoKeyMaterial(listed, key);

      const base64 = `${prefix}-keyed-base64`;
      const fromText = await c.stream.create({
        name: base64, encryptionKey: StreamEncryptionKey.from(key.toString('base64'))
      });
      assert.equal(fromText.encryptionKeyFingerprint, encryptionKeyFingerprint(key));
      await c.stream.delete({ streamId: fromText.id });
    } finally {
      await c.stream.delete({ streamId: created.id });
    }
  });

  it(`given keyless stream when reading should report key source ${keylessSource}`, async () => {
    const created = await c.stream.create({ name: `${prefix}-keyless` });
    try {
      assertProjection(created, keylessSource);
      const fetched = await c.stream.get({ streamId: created.id });
      assert.ok(fetched);
      assertProjection(fetched, keylessSource, created.encryptionKeyFingerprint ?? undefined);
      assertNoKeyMaterial(fetched);
    } finally {
      await c.stream.delete({ streamId: created.id });
    }
  });

  it('given stream rename when updating should preserve encryption projection', async () => {
    const key = randomBytes(32);
    const created = await c.stream.create({
      name: `${prefix}-rename`, encryptionKey: StreamEncryptionKey.from(key)
    });
    try {
      await c.topic.create({ streamId: created.id, name: 'events', partitionCount: 1 });
      await c.message.send({
        streamId: created.id, topicId: 'events', partition: Partitioning.PartitionId(0),
        messages: [{ payload: 'before-rename', headers: [] }]
      });

      assert.ok(await c.stream.update({ streamId: created.id, name: `${prefix}-renamed` }));
      await assert.rejects(
        c.stream.update({ streamId: created.id, reset: ['encryption_key'] }),
        (error: unknown) => error instanceof ResponseError &&
          error.errorCode === OPTION_NOT_UPDATABLE && !error.fromServer
      );

      const renamed = await c.stream.get({ streamId: created.id });
      assert.ok(renamed);
      assert.equal(renamed.id, created.id);
      assert.equal(renamed.name, `${prefix}-renamed`);
      assertProjection(renamed, EncryptionKeySource.Stream, encryptionKeyFingerprint(key));
      assert.deepEqual(renamed.encryptionKekFingerprints, created.encryptionKekFingerprints);

      const polled = await c.message.poll({
        streamId: created.id, topicId: 'events', consumer: Consumer.Single, partitionId: 0,
        pollingStrategy: PollingStrategy.First, count: 10, autocommit: false
      });
      assert.deepEqual(polled.messages.map((message) => message.payload.toString()), ['before-rename']);
    } finally {
      await c.stream.delete({ streamId: created.id });
    }
  });

  it('given topic reset when updating should preserve unmentioned values', async () => {
    const stream = await c.stream.create({ name: `${prefix}-topic-reset` });
    try {
      const topic = await c.topic.create({
        streamId: stream.id, name: 'settings', partitionCount: 1,
        compressionAlgorithm: 2, messageExpiry: 3_600_000_000n, maxTopicSize: 1_073_741_824n
      });
      assert.ok(await c.topic.update({
        streamId: stream.id, topicId: topic.id,
        messageExpiry: 7_200_000_000n, reset: ['max_topic_size']
      }));

      const updated = await c.topic.get({ streamId: stream.id, topicId: topic.id });
      assert.ok(updated);
      assert.equal(updated.name, 'settings');
      assert.equal(updated.options.compression_algorithm, 'gzip');
      assert.equal(updated.compressionAlgorithm, 2);
      assert.equal(updated.options.message_expiry, 7_200_000_000n);
      assert.equal(updated.messageExpiry, 7_200_000_000n);
      assert.equal('max_topic_size' in updated.options, false);
      assert.equal(updated.derivedOptions.max_topic_size, UNLIMITED);
      assert.equal(updated.maxTopicSize, UNLIMITED);
      assert.deepEqual(updated.derivedOptions, {
        ...topic.derivedOptions, max_topic_size: UNLIMITED
      });
    } finally {
      await c.stream.delete({ streamId: stream.id });
    }
  });

  it('given user options when creating and updating should preserve user behavior', async () => {
    const password = 'e2e-ro-password';
    const first = await c.user.create({
      username: `${prefix}-user`, password, status: UserStatus.Active, options: []
    });
    const second = await c.user.create({
      username: `${prefix}-user-taken`, password, status: UserStatus.Active
    });
    try {
      assert.deepEqual(first.options, {});
      assert.deepEqual(first.derivedOptions, {});

      assert.ok(await c.user.update({
        userId: first.id, username: `${prefix}-user-renamed`, status: UserStatus.Inactive
      }));
      const updated = await c.user.get({ userId: first.id });
      assert.ok(updated);
      assert.equal(updated.userName, `${prefix}-user-renamed`);
      assert.equal(updated.status, 'Inactive');
      assert.deepEqual(updated.options, {});

      await assert.rejects(
        c.user.update({ userId: first.id, username: `${prefix}-user-taken`, status: UserStatus.Active }),
        (error: unknown) => error instanceof ResponseError && error.fromServer
      );
      const unchanged = await c.user.get({ userId: first.id });
      assert.ok(unchanged);
      assert.equal(unchanged.userName, `${prefix}-user-renamed`);
      assert.equal(unchanged.status, 'Inactive');
    } finally {
      await c.user.delete({ userId: first.id });
      await c.user.delete({ userId: second.id });
    }
  });

  it('given logged in user rename when reconnecting should keep new username', async () => {
    const password = 'e2e-ro-self-password';
    const user = await c.user.create({
      username: `${prefix}-self`, password, status: UserStatus.Active,
      permissions: {
        global: {
          ManageServers: false, ReadServers: false, ManageUsers: true, ReadUsers: true,
          ManageStreams: true, ReadStreams: true, ManageTopics: true, ReadTopics: true,
          PollMessages: false, SendMessages: false
        },
        streams: []
      }
    });
    const taken = await c.user.create({
      username: `${prefix}-self-taken`, password, status: UserStatus.Active
    });
    const self = connect({ username: `${prefix}-self`, password });
    const signedInAs = async (username: string) => {
      await self.session.logout();
      await self.session.login({ username, password });
      const me = await self.client.getMe();
      return (await c.user.get({ userId: me.userId }))?.userName;
    };
    try {
      assert.equal((await self.client.getMe()).userId, user.id);

      await assert.rejects(
        self.user.update({ userId: user.id, username: `${prefix}-self-taken` }),
        (error: unknown) => error instanceof ResponseError && error.fromServer
      );
      assert.equal(await signedInAs(`${prefix}-self`), `${prefix}-self`);

      const stream = await self.stream.create({ name: `${prefix}-self-stream` });
      await self.stream.update({ streamId: stream.id, name: `${prefix}-self` });
      const topic = await self.topic.create({ streamId: stream.id, name: 'events', partitionCount: 1 });
      await self.topic.update({ streamId: stream.id, topicId: topic.id, name: `${prefix}-self-topic` });
      await self.stream.delete({ streamId: stream.id });
      assert.equal(await signedInAs(`${prefix}-self`), `${prefix}-self`);

      assert.ok(await self.user.update({ userId: user.id, username: `${prefix}-self-renamed` }));
      assert.equal(await signedInAs(`${prefix}-self-renamed`), `${prefix}-self-renamed`);
    } finally {
      await self.destroy();
      await c.user.delete({ userId: user.id });
      await c.user.delete({ userId: taken.id });
    }
  });

  it('given raw create-only key or unknown raw reset when updating should let the server refuse', async () => {
    const stream = await c.stream.create({ name: `${prefix}-raw` });
    try {
      const topic = await c.topic.create({
        streamId: stream.id, name: 'fixed', partitionCount: 1,
        partitionResizePolicy: PartitionResizePolicy.Fixed
      });
      assert.equal(topic.options.partition_resize_policy, PartitionResizePolicy.Fixed);
      const fromServer = (code: number) => (error: unknown) =>
        error instanceof ResponseError && error.errorCode === code && error.fromServer;
      await assert.rejects(
        c.topic.update({
          streamId: stream.id, topicId: topic.id,
          options: [{ key: 'compression_algorithm', value: HeaderValue.String('gzip') }]
        }),
        fromServer(OPTION_NOT_UPDATABLE)
      );
      await assert.rejects(
        c.topic.update({ streamId: stream.id, topicId: topic.id, rawReset: ['future_option'] }),
        fromServer(UNSUPPORTED_OPTION_KEY)
      );
      const unchanged = await c.topic.get({ streamId: stream.id, topicId: topic.id });
      assert.equal(unchanged?.compressionAlgorithm, topic.compressionAlgorithm);
    } finally {
      await c.stream.delete({ streamId: stream.id });
    }
  });

  it('given stream scope when describing options should report sensitivity and mutability', async () => {
    const specs = await c.system.describeOptions({ scope: OptionsScope.Stream });
    const byKey = new Map(specs.map((spec) => [spec.key, spec]));
    assert.deepEqual([...byKey.keys()], [
      'name', 'encryption_key', 'encryption_key_source',
      'encryption_key_fingerprint', 'encryption_kek_fingerprint'
    ]);
    assert.equal(byKey.get('name')?.updatable, true);
    assert.equal(byKey.get('name')?.secret, false);
    assert.equal(byKey.get('encryption_key')?.updatable, false);
    assert.equal(byKey.get('encryption_key')?.secret, true);
    assert.equal(byKey.get('encryption_key')?.critical, true);
    assert.equal(byKey.get('encryption_key_fingerprint')?.readOnly, true);
  });

  it(`${label}::cleanup`, async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
