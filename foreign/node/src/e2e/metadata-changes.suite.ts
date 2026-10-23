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
import { setTimeout as sleep } from 'node:timers/promises';
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import type { Client } from '../client/client.js';
import { ResponseError } from '../wire/error.utils.js';
import {
  EncryptionKeySource, MAX_METADATA_CHANGES_LIMIT, MetadataChangeCursor, MetadataChangeKind,
  MetadataChangeStart, MetadataChangeStatus, StreamEncryptionKey, UserStatus,
  encryptionKeyFingerprint,
  type GetMetadataChanges, type MetadataChangeGroup, type MetadataChangeRecord,
  type MetadataCurrentPageToken, type ParsedOptions
} from '../wire/index.js';

const NEVER_RETURNED = ['encryption_key', 'encryption_key_wrapped', 'critical_keys'];
const FINGERPRINT = /^[0-9a-f]{16}$/;
const INVALID_COMMAND = 3;
const UNAUTHORIZED = 41;
// A two phase delete commits its record after the call returns.
const FOLLOW_BUDGET_MS = 15_000;
const FOLLOW_INTERVAL_MS = 100;

type Credentials = { username: string, password: string };
type StreamIdentity = { streamId: number, streamCreatedOp: bigint };

const latestCursor = async (c: Client, groups: MetadataChangeGroup[]) => {
  const changes = await c.system.getMetadataChanges({ start: MetadataChangeStart.Latest, groups });
  assert.equal(changes.status, MetadataChangeStatus.Ok);
  assert.deepEqual(changes.records, []);
  assert.ok(changes.next);
  assert.equal(changes.next.logId, changes.logId);
  return changes.next;
};

/** The stream a stream, topic or partitions record names, null for any other record. */
const recordStream = (record: MetadataChangeRecord): StreamIdentity | null => {
  switch (record.kind) {
    case 'stream_created':
    case 'stream_updated':
    case 'stream_deleted':
    case 'topic_created':
    case 'topic_updated':
    case 'topic_deleted':
    case 'partitions_changed':
      return { streamId: record.body.streamId, streamCreatedOp: record.body.streamCreatedOp };
    default:
      return null;
  }
};

/**
 * Follows the change log after `cursor` until `want` records name the stream
 * the first record `introduces` accepts. Other e2e files run concurrently and
 * stream ids are reused, so the stream key identifies the stream, not its id.
 */
const followStream = async (
  c: Client, cursor: MetadataChangeCursor, groups: MetadataChangeGroup[],
  introduces: (record: MetadataChangeRecord) => boolean, want: number
) => {
  const records: MetadataChangeRecord[] = [];
  let identity: StreamIdentity | undefined;
  const deadline = Date.now() + FOLLOW_BUDGET_MS;
  for (;;) {
    const changes = await c.system.getMetadataChanges({
      start: MetadataChangeStart.After(cursor), groups, limit: MAX_METADATA_CHANGES_LIMIT
    });
    assert.equal(changes.status, MetadataChangeStatus.Ok);
    assert.equal(changes.phase, 'changes');
    assert.deepEqual(changes.items, []);
    assert.equal(changes.currentPageToken, null);
    assert.ok(changes.next);
    cursor = changes.next;
    for (const record of changes.records) {
      assert.ok(groups.includes(record.group as MetadataChangeGroup), `${record.group} was not requested`);
      const stream = recordStream(record);
      if (stream === null)
        continue;
      if (identity === undefined && introduces(record))
        identity = stream;
      if (identity !== undefined && stream.streamId === identity.streamId &&
        stream.streamCreatedOp === identity.streamCreatedOp)
        records.push(record);
    }
    if (changes.hasMore)
      continue;
    if (records.length >= want || Date.now() > deadline)
      break;
    await sleep(FOLLOW_INTERVAL_MS);
  }
  assert.equal(records.length, want);
  return records;
};

/** Accepts the stream_created record of a stream by its id and name. */
const createdStream = (streamId: number, name: string) => (record: MetadataChangeRecord) =>
  record.kind === 'stream_created' && record.body.streamId === streamId && record.body.name === name;

/** Pages the streams state one item at a time, returning the names by id. */
const currentStreams = async (c: Client) => {
  const streams = new Map<number, string>();
  let pageToken: MetadataCurrentPageToken | undefined;
  for (;;) {
    const changes = await c.system.getMetadataChanges({
      start: MetadataChangeStart.Current(pageToken), groups: ['streams'], limit: 1
    });
    assert.equal(changes.status, MetadataChangeStatus.Ok);
    assert.equal(changes.phase, 'current');
    assert.deepEqual(changes.records, []);
    assert.ok(changes.items.length <= 1);
    for (const item of changes.items) {
      assert.equal(item.group, 'streams');
      if (item.group === 'streams')
        streams.set(item.body.streamId, item.body.name);
    }
    if (changes.currentPageToken === null) {
      assert.ok(changes.next, 'the last page returns the anchor');
      return streams;
    }
    assert.equal(changes.hasMore, true);
    pageToken = changes.currentPageToken;
  }
};

const isRefusedLocally = (code: number) => (error: unknown) =>
  error instanceof ResponseError && error.errorCode === code && !error.fromServer;

/**
 * Registers the metadata changes cases for one transport. `connect` builds a
 * client of that transport signed in with the given credentials.
 */
export const describeMetadataChanges = (
  label: string, prefix: string, connect: (credentials: Credentials) => Client,
  skip: string | false = false
) => describe(label, { skip }, async () => {

  const c = connect({ username: 'iggy', password: 'iggy' });

  it('given latest cursor when creating renaming and deleting a stream should follow its records', async () => {
    const cursor = await latestCursor(c, ['streams']);
    const created = await c.stream.create({ name: `${prefix}-follow` });
    assert.ok(await c.stream.update({ streamId: created.id, name: `${prefix}-follow-renamed` }));
    assert.ok(await c.stream.delete({ streamId: created.id }));

    const records = await followStream(c, cursor, ['streams'], createdStream(created.id, created.name), 3);
    assert.deepEqual(records.map((record) => record.kind), [
      MetadataChangeKind.StreamCreated, MetadataChangeKind.StreamUpdated, MetadataChangeKind.StreamDeleted
    ]);
    for (const record of records) {
      assert.equal(record.actorUserId, 0);
      assert.equal(record.group, 'streams');
      assert.ok(record.checksum > 0n);
      assert.ok(record.timestamp > 0n);
    }
    const [createdRecord, updatedRecord, deletedRecord] = records;
    assert.ok(createdRecord.kind === 'stream_created' && updatedRecord.kind === 'stream_updated' &&
      deletedRecord.kind === 'stream_deleted');
    assert.equal(createdRecord.body.name, `${prefix}-follow`);
    assert.equal(updatedRecord.body.name, `${prefix}-follow-renamed`);
    assert.equal(deletedRecord.body.name, `${prefix}-follow-renamed`);
    assert.equal(createdRecord.body.streamCreatedOp, createdRecord.op);
    assert.equal(createdRecord.body.options.unreadable, false);
    assert.ok(createdRecord.op < updatedRecord.op && updatedRecord.op < deletedRecord.op);
  });

  it('given topics group when creating a stream and topic should return only the topic record', async () => {
    const cursor = await latestCursor(c, ['topics']);
    const stream = await c.stream.create({ name: `${prefix}-filter` });
    try {
      const topic = await c.topic.create({ streamId: stream.id, name: 'events', partitionCount: 2 });
      const introduces = (record: MetadataChangeRecord) =>
        record.kind === 'topic_created' && record.body.streamId === stream.id && record.body.topicId === topic.id;
      const records = await followStream(c, cursor, ['topics'], introduces, 1);
      const [record] = records;
      assert.equal(record.kind, MetadataChangeKind.TopicCreated);
      assert.ok(record.kind === 'topic_created');
      assert.equal(record.body.topicId, topic.id);
      assert.equal(record.body.name, 'events');
      assert.equal(record.body.partitionsCount, 2);
      assert.equal(record.body.topicCreatedOp, record.op);
    } finally {
      await c.stream.delete({ streamId: stream.id });
    }
  });

  it('given limit of one when following three streams should page one record at a time', async () => {
    let cursor = await latestCursor(c, ['streams']);
    const created = new Map<number, string>();
    try {
      for (const index of [1, 2, 3]) {
        const stream = await c.stream.create({ name: `${prefix}-page-${index}` });
        created.set(stream.id, stream.name);
      }
      const seen = new Map<number, string>();
      const deadline = Date.now() + FOLLOW_BUDGET_MS;
      while (seen.size < created.size) {
        assert.ok(Date.now() < deadline, `paged ${[...seen.values()]} of ${[...created.values()]}`);
        const changes = await c.system.getMetadataChanges({
          start: MetadataChangeStart.After(cursor), groups: ['streams'], limit: 1
        });
        assert.equal(changes.status, MetadataChangeStatus.Ok);
        assert.ok(changes.records.length <= 1);
        assert.ok(changes.next);
        for (const record of changes.records) {
          if (record.kind === 'stream_created' && created.get(record.body.streamId) === record.body.name)
            seen.set(record.body.streamId, record.body.name);
          assert.equal(record.op, changes.next.op);
          assert.equal(record.ordinal, changes.next.ordinal);
        }
        assert.ok(changes.next.op >= cursor.op);
        cursor = changes.next;
        if (!changes.hasMore && seen.size < created.size)
          await sleep(FOLLOW_INTERVAL_MS);
      }
      assert.deepEqual(seen, created);
    } finally {
      for (const streamId of created.keys())
        await c.stream.delete({ streamId });
    }
  });

  it('given streams when paging current state one item at a time should list the streams the list returns', async () => {
    const created = [
      await c.stream.create({ name: `${prefix}-current-1` }),
      await c.stream.create({ name: `${prefix}-current-2` })
    ];
    try {
      const before = await c.stream.list();
      const projected = await currentStreams(c);
      const afterwards = await c.stream.list();
      for (const stream of created)
        assert.equal(projected.get(stream.id), stream.name);
      // Other e2e files change streams meanwhile: a stream listed unchanged
      // before and after the scan must be in it.
      for (const stream of before)
        if (afterwards.some((later) => later.id === stream.id && later.name === stream.name))
          assert.equal(projected.get(stream.id), stream.name, `stream ${stream.id}`);
    } finally {
      for (const stream of created)
        await c.stream.delete({ streamId: stream.id });
    }
  });

  it('given queries breaking the request rules when reading should refuse before sending', async () => {
    const created = [
      await c.stream.create({ name: `${prefix}-token-1` }),
      await c.stream.create({ name: `${prefix}-token-2` })
    ];
    try {
      const first = await c.system.getMetadataChanges({
        start: MetadataChangeStart.Current(), groups: ['streams'], limit: 1
      });
      assert.ok(first.currentPageToken);
      const refused: GetMetadataChanges[] = [
        { limit: 0 },
        { limit: MAX_METADATA_CHANGES_LIMIT + 1 },
        { since: 2n, until: 1n },
        { start: MetadataChangeStart.Latest, since: 1n },
        { start: MetadataChangeStart.Current(), until: 1n },
        { start: MetadataChangeStart.Current(), groups: ['topics'] },
        { start: MetadataChangeStart.Current(first.currentPageToken), groups: ['streams', 'users'] }
      ];
      for (const query of refused)
        await assert.rejects(c.system.getMetadataChanges(query), isRefusedLocally(INVALID_COMMAND));
      assert.throws(() => MetadataChangeCursor.parse('mc1_x'), isRefusedLocally(INVALID_COMMAND));

      const latest = await c.system.getMetadataChanges({ start: MetadataChangeStart.Latest });
      assert.equal(latest.status, MetadataChangeStatus.Ok, 'the connection stays usable');
    } finally {
      for (const stream of created)
        await c.stream.delete({ streamId: stream.id });
    }
  });

  it('given user without manage servers when reading changes should be refused as unauthorized', async () => {
    const password = 'changes-secret';
    const user = await c.user.create({
      username: `${prefix}-reader`, password, status: UserStatus.Active,
      permissions: {
        global: {
          ManageServers: false, ReadServers: true, ManageUsers: false, ReadUsers: true,
          ManageStreams: false, ReadStreams: true, ManageTopics: false, ReadTopics: true,
          PollMessages: false, SendMessages: false
        },
        streams: []
      }
    });
    const reader = connect({ username: `${prefix}-reader`, password });
    try {
      await assert.rejects(
        reader.system.getMetadataChanges({}),
        (error: unknown) => error instanceof ResponseError && error.errorCode === UNAUTHORIZED && error.fromServer
      );
    } finally {
      await reader.destroy();
      await c.user.delete({ userId: user.id });
    }
  });

  after(() => {
    c.destroy();
  });
});

/**
 * Registers the keyed stream case, which needs a server with a config key:
 * a client key is refused without one.
 */
export const describeKeyedMetadataChanges = (
  label: string, prefix: string, connect: (credentials: Credentials) => Client,
  skip: string | false = false
) => describe(label, { skip }, async () => {

  const c = connect({ username: 'iggy', password: 'iggy' });

  it('given keyed stream when following its creation should expose fingerprints and no key', async () => {
    const cursor = await latestCursor(c, ['streams']);
    const key = randomBytes(32);
    const created = await c.stream.create({
      name: `${prefix}-keyed`, encryptionKey: StreamEncryptionKey.from(key)
    });
    try {
      const [record] = await followStream(c, cursor, ['streams'], createdStream(created.id, created.name), 1);
      assert.equal(record.kind, MetadataChangeKind.StreamCreated);
      assert.ok(record.kind === 'stream_created');
      const { options } = record.body;
      assert.equal(options.unreadable, false);
      assert.ok(!options.unreadable);
      const text = (name: string) => options.derivedOptions[name] ?? options.options[name];
      assert.equal(text('encryption_key_source'), EncryptionKeySource.Stream);
      assert.equal(text('encryption_key_fingerprint'), encryptionKeyFingerprint(key));
      const keks = String(text('encryption_kek_fingerprint')).split(',');
      assert.deepEqual(keks, created.encryptionKekFingerprints);
      for (const kek of keks)
        assert.match(kek, FINGERPRINT);
      for (const block of [options.options, options.derivedOptions] as ParsedOptions[]) {
        for (const name of NEVER_RETURNED)
          assert.equal(name in block, false, `${name} was returned`);
        for (const value of Object.values(block)) {
          assert.ok(!(Buffer.isBuffer(value) && value.includes(key)), 'an option holds the key');
          for (const encoded of [key.toString('base64'), key.toString('base64url'), key.toString('hex')])
            assert.ok(!String(value).includes(encoded), 'an option holds the encoded key');
        }
      }
    } finally {
      await c.stream.delete({ streamId: created.id });
    }
  });

  after(() => {
    c.destroy();
  });
});
