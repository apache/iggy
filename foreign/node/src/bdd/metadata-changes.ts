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
import { ResponseError } from '../wire/error.utils.js';
import {
  MAX_METADATA_CHANGES_LIMIT, MetadataChangeStart, MetadataChangeStatus, UserStatus,
  encryptionKeyFingerprint,
  type MetadataChangeGroup, type MetadataChangeRecord, type MetadataCurrentPageToken,
  type MetadataStreamChange, type ParsedOptions
} from '../wire/index.js';
import type { TestWorld } from './world.js';

const ROOT_USER_ID = 0;
const UNAUTHORIZED = 41;
// A two phase delete commits its record after the call returns.
const FOLLOW_ATTEMPTS = 100;
const FOLLOW_INTERVAL_MS = 100;
const HIDDEN_OPTIONS = ['encryption_key', 'encryption_key_wrapped', 'critical_keys'];

const readChanges = (world: TestWorld, start: MetadataChangeStart) =>
  world.client.system.getMetadataChanges({
    start, groups: world.changeGroups ?? [], limit: MAX_METADATA_CHANGES_LIMIT
  });

const groupList = (groups: string) =>
  groups.split(',').map((group) => group.trim() as MetadataChangeGroup);

/** The stream a stream, topic or partitions record names, null for any other record. */
const changeStreamId = (record: MetadataChangeRecord): number | null => {
  switch (record.kind) {
    case 'stream_created':
    case 'stream_updated':
    case 'stream_deleted':
    case 'topic_created':
    case 'topic_updated':
    case 'topic_deleted':
    case 'partitions_changed':
      return record.body.streamId;
    default:
      return null;
  }
};

const followed = (world: TestWorld, kind: string): MetadataChangeRecord => {
  const record = world.followedChanges?.find((candidate) => candidate.kind === kind);
  assert.ok(record, `a followed ${kind} change`);
  return record;
};

const followedStreamOptions = (world: TestWorld, kind: string) => {
  const record = followed(world, kind);
  assert.ok(record.kind === 'stream_created' || record.kind === 'stream_updated', `${kind} is a stream change`);
  const { options } = record.body as MetadataStreamChange;
  assert.ok(!options.unreadable, 'stream options stay readable');
  return options;
};

Given(
  'I hold the latest metadata change cursor for the groups {string}',
  async function (this: TestWorld, groups: string) {
    this.changeGroups = groupList(groups);
    const changes = await readChanges(this, MetadataChangeStart.Latest);
    assert.equal(changes.status, MetadataChangeStatus.Ok);
    assert.ok(changes.next, 'latest returns a cursor');
    this.changeCursor = changes.next;
  }
);

When('I delete the stream', async function (this: TestWorld) {
  assert.ok(await this.client.stream.delete({ streamId: this.stream.id }));
});

When(
  'I create and delete {int} streams named {string}',
  async function (this: TestWorld, count: number, prefix: string) {
    for (let index = 0; index < count; index++) {
      const stream = await this.client.stream.create({ name: `${prefix}-${index}` });
      assert.ok(await this.client.stream.delete({ streamId: stream.id }));
    }
  }
);

Then(
  'following the metadata changes should return the kinds {string} for the stream',
  async function (this: TestWorld, kinds: string) {
    const expected = kinds.split(',').map((kind) => kind.trim());
    const records: MetadataChangeRecord[] = [];
    let cursor = this.changeCursor;
    assert.ok(cursor, 'a held cursor');
    let mine: MetadataChangeRecord[] = [];
    for (let attempt = 0; attempt < FOLLOW_ATTEMPTS; attempt++) {
      const changes = await readChanges(this, MetadataChangeStart.After(cursor));
      assert.equal(changes.status, MetadataChangeStatus.Ok);
      assert.ok(changes.next, 'a continuation');
      cursor = changes.next;
      records.push(...changes.records);
      if (changes.hasMore)
        continue;
      mine = records.filter((record) => changeStreamId(record) === this.stream.id);
      if (mine.length >= expected.length)
        break;
      await sleep(FOLLOW_INTERVAL_MS);
    }
    for (const record of records)
      assert.ok(this.changeGroups?.includes(record.group as MetadataChangeGroup), `${record.group} was requested`);
    assert.deepEqual(mine.map((record) => record.kind), expected);
    this.followedChanges = mine;
  }
);

Then('each followed change should name the root user as its actor', function (this: TestWorld) {
  for (const record of this.followedChanges ?? [])
    assert.equal(record.actorUserId, ROOT_USER_ID, `op ${record.op}`);
});

Then(
  'the followed {string} change should carry a requested op before its own op',
  function (this: TestWorld, kind: string) {
    const record = followed(this, kind);
    assert.ok(record.kind === 'stream_deleted', `${kind} is a stream deletion`);
    assert.ok(record.body.requestedOp !== null, 'a requested op');
    assert.ok(record.body.requestedOp < record.op, `requested op ${record.body.requestedOp} before ${record.op}`);
  }
);

Then(
  'the followed {string} change should report encryption key source {string}',
  function (this: TestWorld, kind: string, source: string) {
    const options = followedStreamOptions(this, kind);
    assert.equal(options.derivedOptions.encryption_key_source ?? options.options.encryption_key_source, source);
  }
);

Then(
  'the followed {string} change should carry the stream key fingerprint',
  function (this: TestWorld, kind: string) {
    const options = followedStreamOptions(this, kind);
    assert.ok(this.streamKey, 'the stream was created with a key');
    assert.equal(
      options.derivedOptions.encryption_key_fingerprint ?? options.options.encryption_key_fingerprint,
      encryptionKeyFingerprint(this.streamKey)
    );
  }
);

Then('the followed changes should expose no encryption key material', function (this: TestWorld) {
  for (const record of this.followedChanges ?? []) {
    if (record.kind !== 'stream_created' && record.kind !== 'stream_updated')
      continue;
    const { options } = record.body;
    assert.ok(!options.unreadable, 'stream options stay readable');
    for (const block of [options.options, options.derivedOptions] as ParsedOptions[]) {
      for (const hidden of HIDDEN_OPTIONS)
        assert.equal(hidden in block, false, `${hidden} must never be returned`);
      for (const value of Object.values(block))
        assert.ok(!(Buffer.isBuffer(value) && this.streamKey?.equals(value)), 'a value holds the key');
    }
  }
});

Then(
  'following the metadata changes should answer {string}',
  async function (this: TestWorld, status: string) {
    assert.ok(this.changeCursor, 'a held cursor');
    const changes = await readChanges(this, MetadataChangeStart.After(this.changeCursor));
    assert.equal(changes.status, status);
    if (changes.status !== MetadataChangeStatus.Ok) {
      assert.deepEqual(changes.records, []);
      assert.equal(changes.next, null);
    }
  }
);

When(
  'I read the current state for the groups {string}',
  async function (this: TestWorld, groups: string) {
    this.changeGroups = groupList(groups);
    this.currentItems = [];
    let pageToken: MetadataCurrentPageToken | undefined;
    for (;;) {
      const changes = await readChanges(this, MetadataChangeStart.Current(pageToken));
      assert.equal(changes.status, MetadataChangeStatus.Ok);
      this.currentItems.push(...changes.items);
      if (changes.currentPageToken === null) {
        assert.ok(changes.next, 'the last page returns the anchor');
        this.changeCursor = changes.next;
        return;
      }
      pageToken = changes.currentPageToken;
    }
  }
);

Then('the current state should list the same streams as the stream list', async function (this: TestWorld) {
  const listed = new Map((await this.client.stream.list()).map((stream) => [stream.id, stream.name]));
  const projected = new Map<number, string>();
  for (const item of this.currentItems ?? [])
    if (item.group === 'streams')
      projected.set(item.body.streamId, item.body.name);
  assert.deepEqual(projected, listed);
});

Given(
  'a user named {string} with password {string} and no server permissions exists',
  async function (this: TestWorld, username: string, password: string) {
    const user = await this.client.user.create({
      username, password, status: UserStatus.Active,
      permissions: {
        global: {
          ManageServers: false, ReadServers: true, ManageUsers: false, ReadUsers: false,
          ManageStreams: false, ReadStreams: false, ManageTopics: false, ReadTopics: false,
          PollMessages: false, SendMessages: false
        },
        streams: []
      }
    });
    this.userId = user.id;
  }
);

When(
  'I authenticate as {string} with password {string}',
  async function (this: TestWorld, username: string, password: string) {
    assert.ok(await this.client.session.logout());
    await this.client.session.login({ username, password });
  }
);

Then('reading the metadata changes should be refused as unauthorized', async function (this: TestWorld) {
  this.changeGroups = [];
  await assert.rejects(
    readChanges(this, MetadataChangeStart.Earliest),
    (error: unknown) => error instanceof ResponseError && error.errorCode === UNAUTHORIZED && error.fromServer
  );
});
