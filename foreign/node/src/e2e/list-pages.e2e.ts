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
import { ResponseError } from '../wire/error.utils.js';
import { PageCursor, PageOrder, UserStatus, type NamedPageQuery, type PageQuery } from '../wire/index.js';
import { getTestClient } from './test-client.utils.js';

// Cursor texts of core/common/fixtures/list_pages.json. The text of both is
// valid, so only the server can refuse them.
/** A cursor of the id walk */
const ID_SEVEN_CURSOR = 'pg1_AQcAAAAAAAAA';
/** A cursor of a kind that no server knows */
const UNKNOWN_KIND_CURSOR = 'pg1_AwcAAAAAAAAA';
const INVALID_COMMAND = 3;

const isRefusedByServer = (error: unknown) =>
  error instanceof ResponseError && error.fromServer && error.errorCode === INVALID_COMMAND;

describe('e2e -> list pages', async () => {

  const c = getTestClient();
  // Other e2e files run at the same time, so every name holds a random part.
  const prefix = `e2e-pages-${randomBytes(4).toString('hex')}`;
  const streamIds: number[] = [];
  const userIds: number[] = [];

  const createStream = async (name: string) => {
    const stream = await c.stream.create({ name: `${prefix}-${name}` });
    streamIds.push(stream.id);
    return stream;
  };

  it('e2e -> list pages::walks three streams in pages of two in each order', async () => {
    const created: number[] = [];
    for (const suffix of ['a', 'b', 'c'])
      created.push((await createStream(`walk-${suffix}`)).id);
    // A create takes the lowest free id, so a delete by another client can
    // give a later stream a lower id.
    const ascending = created.toSorted((left, right) => left - right);
    const walks: [PageQuery, number[]][] = [
      [{ order: PageOrder.Id }, ascending],
      [{ order: PageOrder.Id, descending: true }, ascending.toReversed()],
      [{ order: PageOrder.Created }, created]
    ];
    for (const [order, want] of walks) {
      const query: NamedPageQuery = { ...order, name: `${prefix}-walk`, limit: 2 };
      const first = await c.stream.listPage(query);
      assert.equal(first.items.length, 2, JSON.stringify(order));
      assert.ok(first.next, JSON.stringify(order));
      const second = await c.stream.listPage({ ...query, cursor: first.next });
      assert.equal(second.next, undefined, JSON.stringify(order));
      assert.deepEqual([...first.items, ...second.items].map((stream) => stream.id), want, JSON.stringify(order));
    }

    const streams = await c.stream.list();
    for (const id of created)
      assert.ok(streams.some((stream) => stream.id === id), `the whole list holds stream ${id}`);
  });

  it('e2e -> list pages::reads the partitions of a topic in pages of two', async () => {
    const stream = await createStream('partitions');
    const topic = await c.topic.create({ streamId: stream.id, name: 'events', partitionCount: 3 });
    const query = { streamId: stream.id, topicId: topic.id, limit: 2 };

    const first = await c.partition.listPage(query);
    assert.equal(first.items.length, 2);
    assert.ok(first.next);
    const second = await c.partition.listPage({ ...query, cursor: first.next });
    assert.equal(second.next, undefined);
    assert.deepEqual([...first.items, ...second.items].map((partition) => partition.id), [0, 1, 2]);
  });

  it('e2e -> list pages::keeps the topics, consumer groups and users whose name holds the filter', async () => {
    const stream = await createStream('names');
    const events = await c.topic.create({ streamId: stream.id, name: 'events', partitionCount: 1 });
    const readings = await c.topic.create({ streamId: stream.id, name: 'sensor-readings', partitionCount: 1 });
    const target = { streamId: stream.id, topicId: events.id };
    const billing = await c.group.create({ ...target, name: 'billing' });
    await c.group.create({ ...target, name: 'audit' });
    const username = `${prefix}-user`;
    const user = await c.user.create({ username, password: 'e2e-pages-pwd1$', status: UserStatus.Active });
    userIds.push(user.id);

    const topics = await c.topic.listPage({ streamId: stream.id, name: 'sensor' });
    assert.deepEqual(topics.items.map((topic) => topic.id), [readings.id]);
    assert.equal(topics.next, undefined);

    const groups = await c.group.listPage({ ...target, name: 'bill' });
    assert.deepEqual(groups.items.map((group) => group.id), [billing.id]);
    assert.equal(groups.next, undefined);

    const users = await c.user.listPage({ name: username });
    assert.deepEqual(users.items.map((entry) => entry.id), [user.id]);
    assert.equal(users.next, undefined);
  });

  it('e2e -> list pages::refuses pages the server cannot serve and stays usable', async () => {
    const stream = await createStream('refusals');

    await assert.rejects(
      c.stream.listPage({ cursor: PageCursor.parse(UNKNOWN_KIND_CURSOR) }),
      isRefusedByServer, 'a cursor of an unknown kind'
    );
    await assert.rejects(
      c.stream.listPage({ order: PageOrder.Created, cursor: PageCursor.parse(ID_SEVEN_CURSOR) }),
      isRefusedByServer, 'a cursor of the id walk in a created walk'
    );

    const page = await c.stream.listPage({ name: `${prefix}-refusals` });
    assert.deepEqual(page.items.map((entry) => entry.id), [stream.id]);
  });

  after(async () => {
    for (const streamId of streamIds)
      await c.stream.delete({ streamId });
    for (const userId of userIds)
      await c.user.delete({ userId });
    c.destroy();
  });
});
