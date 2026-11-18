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

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import type { ClientProvider, CommandResponse, RawClient } from '../client/client.type.js';
import { COMMAND_CODE } from './command.code.js';
import { translateErrorCode } from './error.code.js';
import { DeserializeError, ResponseError } from './error.utils.js';
import { serializeIdentifier, type Id } from './identifier.utils.js';
import {
  DEFAULT_PAGE_LIMIT, MAX_PAGE_CURSOR_SIZE, MAX_PAGE_LIMIT, MAX_PAGE_NAME_SIZE, PAGE_CURSOR_PREFIX,
  PAGE_ORDER_CODE, PageCursor, type NamedPageQuery, type Page, type PageOrder, type PageQuery
} from './page.utils.js';
import { Operation, operationForCode } from './vsr/operation.js';
import { GET_GROUPS, getGroups, getGroupsPage } from './consumer-group/get-groups.command.js';
import { GET_PARTITIONS, getPartitionsPage } from './partition/get-partitions.command.js';
import { GET_STREAMS, getStreams, getStreamsPage } from './stream/get-streams.command.js';
import { GET_TOPICS, getTopics, getTopicsPage } from './topic/get-topics.command.js';
import { GET_USERS, getUsers, getUsersPage } from './user/get-users.command.js';

type ListCommand = 'get_users' | 'get_streams' | 'get_topics' | 'get_partitions' | 'get_consumer_groups';

type FixtureId = { kind: 'numeric', value: number } | { kind: 'named', value: string };

type FixtureInput = {
  stream_id: FixtureId | null,
  topic_id: FixtureId | null,
  order: PageOrder,
  descending: boolean,
  limit: number,
  cursor: string | null,
  name: string | null
};

type FixtureItem = { id: number, name: string | null };

type FixtureResponse = {
  case: string, command: ListCommand, response_hex: string, next: string | null, items: FixtureItem[]
};

type Fixture = {
  schema_version: number,
  constants: {
    codes: Record<ListCommand, number>,
    orders: Record<string, number>,
    cursor_prefix: string,
    max_cursor_bytes: number,
    default_limit: number,
    max_limit: number,
    max_name_bytes: number,
    errors: { invalid_command: number, reply_too_large: number }
  },
  cursors: { case: string, text: string, hex: string }[],
  opaque_cursors: { case: string, text: string, hex: string }[],
  requests: { case: string, command: ListCommand, input: FixtureInput, request_hex: string }[],
  responses: FixtureResponse[],
  opaque_responses: FixtureResponse[],
  refused: {
    cursors: { case: string, text: string }[],
    queries: { case: string, input: FixtureInput }[],
    responses: { case: string, command: ListCommand, response_hex: string }[]
  }
};

const fixture: Fixture = JSON.parse(readFileSync(new URL('./fixtures/list_pages.json', import.meta.url), 'utf8'));
const { constants } = fixture;

/** `PageCursor.parse` runs outside of any command. */
const NO_COMMAND = 0;
/** `[next_len:u8 = 0][count:u32 = 0]` */
const EMPTY_LAST_PAGE = Buffer.alloc(5);

const fromHex = (hex: string) => Buffer.from(hex, 'hex');

const u32 = (value: number) => {
  const bytes = Buffer.alloc(4);
  bytes.writeUInt32LE(value);
  return bytes;
};

const reply = (data: Buffer): CommandResponse => ({ status: 0, length: data.length, data });

const fixtureResponse = (name: string): FixtureResponse => {
  const vector = fixture.responses.find((response) => response.case === name);
  assert.ok(vector, `no response vector ${name}`);
  return vector;
};

const fixtureId = (id: FixtureId | null): Id => {
  assert.ok(id, 'the command needs this parent id');
  return id.value;
};

const fixtureQuery = (input: FixtureInput): PageQuery => ({
  order: input.order,
  descending: input.descending,
  limit: input.limit,
  ...(input.cursor === null ? {} : { cursor: PageCursor.parse(input.cursor) })
});

const fixtureNamedQuery = (input: FixtureInput): NamedPageQuery => ({
  ...fixtureQuery(input),
  ...(input.name === null ? {} : { name: input.name })
});

/** The page with each item as the fixture writes it. An absent next stays absent. */
const fixturePage = <T>(page: Page<T>, item: (entry: T) => FixtureItem): Page<FixtureItem> =>
  ({ ...page, items: page.items.map(item) });

const LIST_COMMANDS: Record<ListCommand, {
  code: number,
  encode: (input: FixtureInput) => Buffer,
  decode: (data: Buffer) => Page<FixtureItem>
}> = {
  get_users: {
    code: COMMAND_CODE.GetUsers,
    encode: (input) => GET_USERS.serialize(fixtureNamedQuery(input)),
    decode: (data) => fixturePage(GET_USERS.deserialize(reply(data)), (user) => ({ id: user.id, name: user.userName }))
  },
  get_streams: {
    code: COMMAND_CODE.GetStreams,
    encode: (input) => GET_STREAMS.serialize(fixtureNamedQuery(input)),
    decode: (data) => fixturePage(GET_STREAMS.deserialize(reply(data)), (stream) => ({ id: stream.id, name: stream.name }))
  },
  get_topics: {
    code: COMMAND_CODE.GetTopics,
    encode: (input) => GET_TOPICS.serialize({ streamId: fixtureId(input.stream_id), ...fixtureNamedQuery(input) }),
    decode: (data) => fixturePage(GET_TOPICS.deserialize(reply(data)), (topic) => ({ id: topic.id, name: topic.name }))
  },
  get_partitions: {
    code: COMMAND_CODE.GetPartitions,
    encode: (input) => GET_PARTITIONS.serialize({
      streamId: fixtureId(input.stream_id), topicId: fixtureId(input.topic_id), ...fixtureQuery(input)
    }),
    decode: (data) => fixturePage(GET_PARTITIONS.deserialize(reply(data)), (partition) => ({ id: partition.id, name: null }))
  },
  get_consumer_groups: {
    code: COMMAND_CODE.GetGroups,
    encode: (input) => GET_GROUPS.serialize({
      streamId: fixtureId(input.stream_id), topicId: fixtureId(input.topic_id), ...fixtureNamedQuery(input)
    }),
    decode: (data) => fixturePage(GET_GROUPS.deserialize(reply(data)), (group) => ({ id: group.id, name: group.name }))
  }
};

const isInvalidCommand = (commandCode: number) => (error: unknown) =>
  error instanceof ResponseError && !error.fromServer && error.commandCode === commandCode &&
  error.errorCode === constants.errors.invalid_command;

/** A raw client that answers each request with the next reply and records the requests. */
const scriptedClient = (replies: Buffer[]) => {
  const sent: { code: number, payload: Buffer }[] = [];
  const client = {
    sendCommand: async (code: number, payload: Buffer): Promise<CommandResponse> => {
      sent.push({ code, payload: Buffer.from(payload) });
      const data = replies.shift();
      assert.ok(data, 'a request after the last scripted reply');
      return reply(data);
    }
  } as unknown as RawClient;
  const provider: ClientProvider = async () => client;
  return { sent, provider };
};

/** The page block of a whole list walk: id order, ascending, limit 1000, then `cursor` and no name. */
const walkBlock = (cursor: Buffer) =>
  Buffer.concat([Buffer.from([PAGE_ORDER_CODE.id, 0]), u32(MAX_PAGE_LIMIT), cursor, Buffer.from([0])]);

describe('given the shared list page vectors when encoding and decoding should match the server contract', () => {
  it('has the schema version this test reads', () => {
    assert.equal(fixture.schema_version, 1);
  });

  it('names the codes, orders, limits and errors of this SDK', () => {
    assert.deepEqual(
      constants.codes,
      Object.fromEntries(Object.entries(LIST_COMMANDS).map(([name, { code }]) => [name, code]))
    );
    for (const code of Object.values(constants.codes))
      assert.equal(operationForCode(code), Operation.NonReplicated);
    assert.deepEqual(constants.orders, PAGE_ORDER_CODE);
    assert.equal(constants.cursor_prefix, PAGE_CURSOR_PREFIX);
    assert.equal(constants.max_cursor_bytes, MAX_PAGE_CURSOR_SIZE);
    assert.equal(constants.default_limit, DEFAULT_PAGE_LIMIT);
    assert.equal(constants.max_limit, MAX_PAGE_LIMIT);
    assert.equal(constants.max_name_bytes, MAX_PAGE_NAME_SIZE);
    assert.equal(translateErrorCode(constants.errors.invalid_command), 'Invalid command');
    assert.equal(translateErrorCode(constants.errors.reply_too_large), 'The reply is larger than the reply size limit');
  });

  // The client checks only the text, so the opaque cursors that the server refuses parse too.
  it('parses and prints every cursor', () => {
    for (const vector of [...fixture.cursors, ...fixture.opaque_cursors]) {
      const cursor = PageCursor.parse(vector.text);
      assert.equal(cursor.toString(), vector.text, vector.case);
      assert.equal(JSON.stringify(cursor), JSON.stringify(vector.text), vector.case);
      assert.equal(cursor.toBytes().toString('hex'), vector.hex, vector.case);
      assert.equal(PageCursor.fromBytes(fromHex(vector.hex)).toString(), vector.text, vector.case);
    }
  });

  // The Node SDK has no HTTP transport, so http_query has no encoder to check.
  it('encodes every request', () => {
    for (const vector of fixture.requests)
      assert.equal(LIST_COMMANDS[vector.command].encode(vector.input).toString('hex'), vector.request_hex, vector.case);
  });

  it('fills the defaults of an empty query', () => {
    const firstPage = fixture.requests.find((vector) => vector.case === 'streams_first_page');
    assert.ok(firstPage);
    assert.equal(GET_STREAMS.serialize({}).toString('hex'), firstPage.request_hex);
  });

  it('decodes every reply, also a next that the server would refuse', () => {
    for (const vector of [...fixture.responses, ...fixture.opaque_responses]) {
      const page = LIST_COMMANDS[vector.command].decode(fromHex(vector.response_hex));
      assert.equal(Object.hasOwn(page, 'next'), vector.next !== null, vector.case);
      assert.deepEqual(
        { next: page.next?.toString() ?? null, items: page.items },
        { next: vector.next, items: vector.items },
        vector.case
      );
    }
  });

  it('sends an opaque next back with its bytes unchanged', () => {
    for (const vector of fixture.opaque_responses) {
      const data = fromHex(vector.response_hex);
      const page = GET_STREAMS.deserialize(reply(data));
      assert.ok(page.next, vector.case);
      // [order][descending][limit:u32][cursor_len][cursor][name_len], with the
      // [next_len][next] of the reply as the cursor.
      const expected = Buffer.concat([
        Buffer.from([PAGE_ORDER_CODE.id, 0]), u32(DEFAULT_PAGE_LIMIT), data.subarray(0, 1 + data[0]), Buffer.from([0])
      ]);
      assert.deepEqual(GET_STREAMS.serialize({ cursor: page.next }), expected, vector.case);
    }
  });
});

describe('given refused list page vectors when parsing, encoding or decoding should refuse them', () => {
  it('refuses every cursor text', () => {
    for (const vector of fixture.refused.cursors)
      assert.throws(() => PageCursor.parse(vector.text), isInvalidCommand(NO_COMMAND), vector.case);
  });

  it('refuses every query of each page method before it connects', async () => {
    let connects = 0;
    const provider: ClientProvider = async () => {
      connects += 1;
      throw new Error('must not connect');
    };
    const parents = { streamId: 1, topicId: 2 };
    const namedPageReads: [number, (query: NamedPageQuery) => Promise<unknown>][] = [
      [COMMAND_CODE.GetStreams, (query) => getStreamsPage(provider)(query)],
      [COMMAND_CODE.GetUsers, (query) => getUsersPage(provider)(query)],
      [COMMAND_CODE.GetTopics, (query) => getTopicsPage(provider)({ ...parents, ...query })],
      [COMMAND_CODE.GetGroups, (query) => getGroupsPage(provider)({ ...parents, ...query })]
    ];
    for (const vector of fixture.refused.queries) {
      for (const [code, readPage] of namedPageReads)
        await assert.rejects(readPage(fixtureNamedQuery(vector.input)), isInvalidCommand(code), vector.case);
      if (vector.input.name === null)
        await assert.rejects(
          getPartitionsPage(provider)({ ...parents, ...fixtureQuery(vector.input) }),
          isInvalidCommand(COMMAND_CODE.GetPartitions), vector.case
        );
    }
    assert.equal(connects, 0);
  });

  it('refuses every malformed reply', () => {
    for (const vector of fixture.refused.responses)
      assert.throws(
        () => LIST_COMMANDS[vector.command].decode(fromHex(vector.response_hex)), DeserializeError, vector.case
      );
  });
});

describe('given values the shared vectors cannot hold when encoding or decoding should refuse them', () => {
  it('refuses a query with an unknown order, a fractional limit, a cursor text or a lone surrogate', () => {
    const refused: [string, NamedPageQuery][] = [
      ['unknown order', { order: 'name' as PageOrder }],
      ['fractional limit', { limit: 1.5 }],
      ['cursor text', { cursor: fixture.cursors[0].text as unknown as PageCursor }],
      ['lone surrogate', { name: 'a\uD800' }]
    ];
    for (const [name, query] of refused)
      assert.throws(() => GET_STREAMS.serialize(query), isInvalidCommand(COMMAND_CODE.GetStreams), name);
  });

  // A spread skips the excess property check, so a NamedPageQuery reaches the partitions method.
  it('refuses a partitions query with a name before it sends', async () => {
    const { sent, provider } = scriptedClient([]);
    const query: NamedPageQuery = { name: 'p' };
    await assert.rejects(
      getPartitionsPage(provider)({ streamId: 1, topicId: 2, ...query }),
      isInvalidCommand(COMMAND_CODE.GetPartitions)
    );
    assert.deepEqual(sent, []);
  });

  it('sends a partitions query with an undefined name as a query without a name', () => {
    const query: NamedPageQuery = { name: undefined };
    assert.deepEqual(
      GET_PARTITIONS.serialize({ streamId: 1, topicId: 2, ...query }),
      GET_PARTITIONS.serialize({ streamId: 1, topicId: 2 })
    );
  });

  it('refuses bytes after the last item of every reply', () => {
    for (const vector of fixture.responses) {
      const data = Buffer.concat([fromHex(vector.response_hex), Buffer.from([0])]);
      assert.throws(() => LIST_COMMANDS[vector.command].decode(data), DeserializeError, vector.case);
    }
  });

  // The group decoder cuts a name short without a bounds check, so only the
  // page decoder sees that the last name of the groups reply is cut.
  it('refuses every reply cut by its last byte', () => {
    for (const vector of fixture.responses) {
      const data = fromHex(vector.response_hex);
      assert.throws(
        () => LIST_COMMANDS[vector.command].decode(data.subarray(0, data.length - 1)), DeserializeError, vector.case
      );
    }
  });

  it('refuses cursor bytes of no byte or of more than 255 bytes', () => {
    for (const size of [0, MAX_PAGE_CURSOR_SIZE + 1])
      assert.throws(() => PageCursor.fromBytes(Buffer.alloc(size, 1)), DeserializeError, `${size} bytes`);
  });
});

describe('given a page cursor when its source or its bytes change should keep its own bytes', () => {
  const [vector] = fixture.cursors;

  it('copies the bytes it is built from and the bytes it returns', () => {
    const bytes = fromHex(vector.hex);
    const cursor = PageCursor.fromBytes(bytes);
    bytes.fill(0);
    cursor.toBytes().fill(0);
    assert.equal(cursor.toString(), vector.text);
  });

  it('copies the next out of the reply', () => {
    const response = fixtureResponse('streams_with_next');
    const data = fromHex(response.response_hex);
    const page = GET_STREAMS.deserialize(reply(data));
    data.fill(0);
    assert.equal(page.next?.toString(), response.next);
  });
});

describe('given a list of several pages when reading the whole list should walk every page by id', () => {
  it('starts with the whole list request of the shared vectors', () => {
    const walk = fixture.requests.find((request) => request.case === 'streams_whole_list_walk');
    assert.ok(walk);
    assert.equal(walkBlock(Buffer.from([0])).toString('hex'), walk.request_hex);
  });

  const walks: {
    name: string, code: number, parents: Buffer, reply: string,
    list: (provider: ClientProvider) => Promise<{ id: number }[]>
  }[] = [
    {
      name: 'streams', code: COMMAND_CODE.GetStreams, parents: Buffer.alloc(0), reply: 'streams_with_next',
      list: (provider) => getStreams(provider)()
    },
    {
      name: 'topics', code: COMMAND_CODE.GetTopics, parents: serializeIdentifier('orders'),
      reply: 'topics_created_descending_with_next',
      list: (provider) => getTopics(provider)({ streamId: 'orders' })
    },
    {
      name: 'consumer groups', code: COMMAND_CODE.GetGroups,
      parents: Buffer.concat([serializeIdentifier(1), serializeIdentifier('events')]),
      reply: 'consumer_groups_with_next',
      list: (provider) => getGroups(provider)({ streamId: 1, topicId: 'events' })
    },
    {
      name: 'users', code: COMMAND_CODE.GetUsers, parents: Buffer.alloc(0), reply: 'users_with_next',
      list: (provider) => getUsers(provider)()
    }
  ];

  for (const walk of walks)
    it(`reads every page of the ${walk.name} and sends each next back`, async () => {
      const vector = fixtureResponse(walk.reply);
      const first = fromHex(vector.response_hex);
      const { sent, provider } = scriptedClient([first, EMPTY_LAST_PAGE]);
      const items = await walk.list(provider);
      assert.deepEqual(items.map((item) => item.id), vector.items.map((item) => item.id));
      assert.deepEqual(sent, [
        { code: walk.code, payload: Buffer.concat([walk.parents, walkBlock(Buffer.from([0]))]) },
        { code: walk.code, payload: Buffer.concat([walk.parents, walkBlock(first.subarray(0, 1 + first[0]))]) }
      ]);
    });
});

describe('given a server that answers the cursor it was sent when reading the whole list should fail', () => {
  it('refuses the second page and sends no third request', async () => {
    const first = fromHex(fixtureResponse('streams_with_next').response_hex);
    const { sent, provider } = scriptedClient([first, first]);
    await assert.rejects(getStreams(provider)(), {
      name: 'DeserializeError', message: 'cannot deserialize list page: the server answered the page cursor it was sent'
    });
    assert.deepEqual(sent.map((request) => request.payload), [
      walkBlock(Buffer.from([0])), walkBlock(first.subarray(0, 1 + first[0]))
    ]);
  });
});
