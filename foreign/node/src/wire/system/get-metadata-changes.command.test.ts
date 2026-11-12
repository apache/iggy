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
import type { ClientProvider } from '../../client/client.type.js';
import { COMMAND_CODE } from '../command.code.js';
import { DeserializeError, ResponseError } from '../error.utils.js';
import type { ParsedOptions } from '../options.utils.js';
import type { UserPermissions } from '../user/permissions.utils.js';
import { UserStatus } from '../user/user.utils.js';
import { Operation, operationForCode } from '../vsr/operation.js';
import {
  DEFAULT_METADATA_CHANGES_LIMIT, GET_METADATA_CHANGES, MAX_METADATA_CHANGES_LIMIT,
  MetadataChangeStart, deserializeMetadataChanges, getMetadataChanges,
  type GetMetadataChanges, type MetadataChanges
} from './get-metadata-changes.command.js';
import {
  METADATA_CHANGE_CURSOR_PREFIX, METADATA_CHANGE_CURSOR_SIZE, METADATA_CHANGE_GROUP_CODE,
  METADATA_CHANGE_KIND_CODE, METADATA_CURRENT_PAGE_TOKEN_PREFIX, MetadataChangeCursor,
  MetadataChangeGroup, MetadataChangeKind, MetadataChangePhase, MetadataChangeStatus,
  MetadataCurrentPageToken, MetadataMemberLeftReason, MetadataPermissionsUpdatedReason,
  MetadataSessionClosedReason, MetadataTokenDeletedReason, metadataChangeGroupName,
  metadataChangeKindGroup, metadataChangeKindName,
  type MetadataChangeRecord, type MetadataCurrentItem, type MetadataOptions
} from './metadata-changes.utils.js';

type QueryInput = {
  start: 'earliest' | 'latest' | 'after' | 'current',
  cursor: string | null,
  page_token: string | null,
  groups: MetadataChangeGroup[],
  since: number | null,
  until: number | null,
  limit: number
};

type Fixture = {
  schema_version: number,
  constants: {
    command_code: number,
    cursor_prefix: string,
    page_token_prefix: string,
    cursor_size: number,
    cursor_text_length: number,
    max_page_token_size: number,
    default_limit: number,
    max_limit: number,
    groups: Record<string, { code: number, parents: string[] }>,
    kinds: Record<string, { code: number, group: string }>,
    statuses: Record<string, number>,
    phases: Record<string, number>,
    start_modes: Record<string, number>,
    member_left_reasons: Record<string, number>,
    token_deleted_reasons: Record<string, number>,
    session_closed_reasons: Record<string, number>,
    permissions_updated_reasons: Record<string, number>
  },
  records: {
    case: string, group: number, kind: number, body_hex: string, record_hex: string,
    expected: Record<string, unknown>
  }[],
  items: {
    case: string, group: number, body_hex: string, item_hex: string, expected: Record<string, unknown>
  }[],
  cursors: {
    case: string, text: string, hex: string, log_id: string, op: number, checksum: string,
    ordinal: number
  }[],
  page_tokens: {
    case: string, text: string, hex: string, anchor: string, required_head: number,
    groups_mask: number, group: string, last_key_hex: string
  }[],
  requests: { case: string, input: QueryInput, request_hex: string }[],
  responses: { case: string, response_hex: string, expected: Record<string, unknown> }[],
  large_values: {
    record_hex: string, op: string, ordinal: number, timestamp: string, checksum: string,
    user_created_at: string, session: string, client_id: string
  },
  refused: {
    cursors: { case: string, text: string }[],
    page_tokens: { case: string, text: string }[],
    record_bodies: { case: string, group: number, kind: number, body_hex: string }[],
    requests: { case: string, hex: string }[],
    responses: { case: string, hex: string }[],
    queries: { case: string, input: QueryInput }[]
  }
};

const fixtureText = readFileSync(new URL('../fixtures/metadata_changes.json', import.meta.url), 'utf8');
const fixture: Fixture = JSON.parse(fixtureText);
// The cursor vector at u64::MAX does not survive a plain JSON number.
const cursorOps = [...fixtureText.matchAll(/"op": ([0-9]+),\s*"checksum"/g)].map((match) => BigInt(match[1]));
const { constants } = fixture;

const fromHex = (hex: string) => Buffer.from(hex, 'hex');
const hex128 = (value: bigint) => value.toString(16).padStart(32, '0');

const isInvalidCommand = (error: unknown) =>
  error instanceof ResponseError && error.errorCode === 3 &&
  error.commandCode === COMMAND_CODE.GetMetadataChanges && !error.fromServer;

const isMalformed = (error: unknown) => error instanceof DeserializeError;

/** An ok changes page without a log id or cursor around the given records. */
const changesPage = (...records: Buffer[]) => Buffer.concat([
  Buffer.from([0, 0, 0]), Buffer.alloc(24), Buffer.from([0, 0]),
  u32(records.length), ...records, u32(0), Buffer.alloc(2)
]);

/** An ok current page without a log id or cursor around the given items. */
const currentPage = (...items: Buffer[]) => Buffer.concat([
  Buffer.from([0, 1, 0]), Buffer.alloc(24), Buffer.from([0, 0]),
  u32(0), u32(items.length), ...items, Buffer.alloc(2)
]);

/** A record of op 1 by root around a body. */
const recordEnvelope = (group: number, kind: number, body: Buffer) => Buffer.concat([
  u64(1n), u32(0), Buffer.alloc(16), u64(7n), Buffer.from([1, 0, 0, 0, 0, group, kind]),
  u32(body.length), body
]);

const u32 = (value: number) => {
  const bytes = Buffer.alloc(4);
  bytes.writeUInt32LE(value);
  return bytes;
};

const u64 = (value: bigint) => {
  const bytes = Buffer.alloc(8);
  bytes.writeBigUInt64LE(value);
  return bytes;
};

const fixtureQuery = (input: QueryInput): GetMetadataChanges => {
  const starts = {
    earliest: () => MetadataChangeStart.Earliest,
    latest: () => MetadataChangeStart.Latest,
    after: () => MetadataChangeStart.After(MetadataChangeCursor.parse(input.cursor!)),
    current: () => MetadataChangeStart.Current(
      input.page_token === null ? undefined : MetadataCurrentPageToken.parse(input.page_token)
    )
  };
  return {
    start: starts[input.start](),
    groups: input.groups,
    since: input.since === null ? undefined : BigInt(input.since),
    until: input.until === null ? undefined : BigInt(input.until),
    limit: input.limit
  };
};

const optionText = (value: unknown): string =>
  Buffer.isBuffer(value) ? value.toString('base64') : String(value);

const kindName = (value: unknown): string => {
  if (typeof value === 'bigint') return 'uint64';
  if (typeof value === 'number') return 'uint32';
  if (typeof value === 'boolean') return 'bool';
  if (Buffer.isBuffer(value)) return 'raw';
  return 'string';
};

/** Options in the HTTP JSON form, both blocks merged with their provenance. */
const optionsView = (options: MetadataOptions) => {
  if (options.unreadable)
    return { options: null, options_unreadable: true };
  const merged: Record<string, unknown> = {};
  for (const [block, explicit] of [[options.derivedOptions, false], [options.options, true]] as [ParsedOptions, boolean][])
    for (const [key, value] of Object.entries(block))
      merged[key] = { value: optionText(value), kind: kindName(value), explicit };
  return { options: merged };
};

/** Permissions in the get_user JSON form. */
const permissionsView = (permissions: UserPermissions | null) => {
  if (permissions === null)
    return null;
  const { global } = permissions;
  return {
    global: {
      manage_servers: global.ManageServers, read_servers: global.ReadServers,
      manage_users: global.ManageUsers, read_users: global.ReadUsers,
      manage_streams: global.ManageStreams, read_streams: global.ReadStreams,
      manage_topics: global.ManageTopics, read_topics: global.ReadTopics,
      poll_messages: global.PollMessages, send_messages: global.SendMessages
    },
    streams: permissions.streams.length === 0 ? null : Object.fromEntries(permissions.streams.map((stream) => [
      String(stream.streamId),
      {
        manage_stream: stream.permissions.manageStream, read_stream: stream.permissions.readStream,
        manage_topics: stream.permissions.manageTopics, read_topics: stream.permissions.readTopics,
        poll_messages: stream.permissions.pollMessages, send_messages: stream.permissions.sendMessages,
        topics: stream.topics.length === 0 ? null : Object.fromEntries(stream.topics.map((topic) => [
          String(topic.topicId),
          {
            manage_topic: topic.permissions.manage, read_topic: topic.permissions.read,
            poll_messages: topic.permissions.pollMessages, send_messages: topic.permissions.sendMessages
          }
        ]))
      }
    ]))
  };
};

/**
 * A body in the HTTP JSON form: snake_case keys, 64-bit values as numbers
 * (every fixture vector but large_values stays below 2^53), 128-bit values as
 * hex, absent optionals left out.
 */
const bodyView = (body: Record<string, unknown>): Record<string, unknown> => {
  const view: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(body)) {
    const name = key.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
    if (key === 'options')
      Object.assign(view, optionsView(value as MetadataOptions));
    else if (key === 'permissions')
      view.permissions = permissionsView(value as UserPermissions | null);
    else if (key === 'status')
      view.status = value === UserStatus.Active ? 'active' : 'inactive';
    else if (key === 'clientId')
      view.client_id = hex128(value as bigint);
    else if (value === null && ['requestedOp', 'memberId', 'expiry'].includes(key))
      continue;
    else
      view[name] = typeof value === 'bigint' ? Number(value) : value;
  }
  return view;
};

const recordView = (record: MetadataChangeRecord) => {
  const envelope: Record<string, unknown> = {
    op: Number(record.op), ordinal: record.ordinal, checksum: hex128(record.checksum),
    timestamp: Number(record.timestamp)
  };
  if (record.actorUserId !== null)
    envelope.actor_user_id = record.actorUserId;
  if (record.kind === 'unknown')
    return {
      ...envelope, group: String(record.groupCode), kind: String(record.kindCode),
      body: record.body.toString('base64')
    };
  return { ...envelope, group: record.group, kind: record.kind, ...bodyView(record.body) };
};

const itemView = (item: MetadataCurrentItem) => item.group === 'unknown'
  ? { group: String(item.groupCode), body: item.body.toString('base64') }
  : { group: item.group, ...bodyView(item.body) };

const changesView = (changes: MetadataChanges) => ({
  status: changes.status,
  phase: changes.phase,
  log_id: changes.logId === null ? null : hex128(changes.logId),
  oldest_op: Number(changes.oldestOp),
  retained_since: Number(changes.retainedSince),
  head_op: Number(changes.headOp),
  next: changes.next?.toString() ?? null,
  has_more: changes.hasMore,
  records: changes.records.map(recordView),
  items: changes.items.map(itemView),
  current_page_token: changes.currentPageToken?.toString() ?? null
});

describe('given shared vectors when decoding should match server contract', () => {
  it('mirrors the command code, prefixes, sizes, limits and every name', () => {
    assert.equal(fixture.schema_version, 1);
    assert.equal(COMMAND_CODE.GetMetadataChanges, constants.command_code);
    assert.equal(operationForCode(constants.command_code), Operation.NonReplicated);
    assert.equal(METADATA_CHANGE_CURSOR_PREFIX, constants.cursor_prefix);
    assert.equal(METADATA_CURRENT_PAGE_TOKEN_PREFIX, constants.page_token_prefix);
    assert.equal(METADATA_CHANGE_CURSOR_SIZE, constants.cursor_size);
    assert.equal(new MetadataChangeCursor(0n, 0n, 0n, 0).toString().length, constants.cursor_text_length);
    assert.equal(DEFAULT_METADATA_CHANGES_LIMIT, constants.default_limit);
    assert.equal(MAX_METADATA_CHANGES_LIMIT, constants.max_limit);

    assert.deepEqual(
      METADATA_CHANGE_GROUP_CODE,
      Object.fromEntries(Object.entries(constants.groups).map(([name, { code }]) => [name, code]))
    );
    assert.deepEqual(Object.values(MetadataChangeGroup).sort(), Object.keys(constants.groups).sort());
    for (const [name, { code, group }] of Object.entries(constants.kinds)) {
      assert.equal(METADATA_CHANGE_KIND_CODE[name as MetadataChangeKind], code, name);
      assert.equal(metadataChangeKindName(code), name);
      assert.equal(metadataChangeKindGroup(name as MetadataChangeKind), group, name);
    }
    assert.deepEqual(Object.values(MetadataChangeKind).sort(), Object.keys(constants.kinds).sort());
    for (let code = 0; code < 256; code++) {
      assert.equal(metadataChangeGroupName(code) !== 'unknown', code >= 1 && code <= 8, `group ${code}`);
      assert.equal(
        metadataChangeKindName(code) !== 'unknown',
        Object.values(constants.kinds).some((kind) => kind.code === code),
        `kind ${code}`
      );
    }

    const names = (values: Record<string, string>) => Object.values(values).sort();
    assert.deepEqual(names(MetadataChangeStatus), Object.keys(constants.statuses).sort());
    assert.deepEqual(names(MetadataChangePhase), Object.keys(constants.phases).sort());
    assert.deepEqual(names(MetadataMemberLeftReason), Object.keys(constants.member_left_reasons).sort());
    assert.deepEqual(names(MetadataTokenDeletedReason), Object.keys(constants.token_deleted_reasons).sort());
    assert.deepEqual(names(MetadataSessionClosedReason), Object.keys(constants.session_closed_reasons).sort());
    assert.deepEqual(
      names(MetadataPermissionsUpdatedReason), Object.keys(constants.permissions_updated_reasons).sort()
    );
    for (const [mode, code] of Object.entries(constants.start_modes)) {
      const query: GetMetadataChanges = mode === 'after'
        ? { start: MetadataChangeStart.After(new MetadataChangeCursor(0n, 0n, 0n, 0)) }
        : { start: { mode } as MetadataChangeStart };
      assert.equal(GET_METADATA_CHANGES.serialize(query)[0], code, mode);
    }
  });

  it('decodes every record vector to the fields the HTTP route renders', () => {
    for (const vector of fixture.records) {
      assert.ok(vector.record_hex.endsWith(vector.body_hex), vector.case);
      const [record] = deserializeMetadataChanges(changesPage(fromHex(vector.record_hex))).records;
      assert.equal(record.groupCode, vector.group, vector.case);
      assert.equal(record.kindCode, vector.kind, vector.case);
      assert.deepEqual(recordView(record), vector.expected, vector.case);
    }
  });

  it('decodes every item vector to the fields the HTTP route renders', () => {
    for (const vector of fixture.items) {
      assert.ok(vector.item_hex.endsWith(vector.body_hex), vector.case);
      const [item] = deserializeMetadataChanges(currentPage(fromHex(vector.item_hex))).items;
      assert.equal(item.groupCode, vector.group, vector.case);
      assert.deepEqual(itemView(item), vector.expected, vector.case);
    }
  });

  it('parses and prints every cursor vector', () => {
    fixture.cursors.forEach((vector, index) => {
      const cursor = MetadataChangeCursor.parse(vector.text);
      assert.equal(hex128(cursor.logId), vector.log_id, vector.case);
      assert.equal(cursor.op, cursorOps[index], vector.case);
      assert.equal(hex128(cursor.checksum), vector.checksum, vector.case);
      assert.equal(cursor.ordinal, vector.ordinal, vector.case);
      assert.equal(cursor.toString(), vector.text, vector.case);
      assert.equal(JSON.stringify({ cursor }), JSON.stringify({ cursor: vector.text }), vector.case);
      assert.equal(cursor.toBytes().toString('hex'), vector.hex, vector.case);
    });
    assert.equal(cursorOps.length, fixture.cursors.length);
  });

  it('parses and prints every page token vector', () => {
    for (const vector of fixture.page_tokens) {
      const token = MetadataCurrentPageToken.parse(vector.text);
      assert.equal(token.anchor.toString(), vector.anchor, vector.case);
      assert.equal(token.requiredHead, BigInt(vector.required_head), vector.case);
      const mask = token.groups.reduce((bits, group) => bits | (1 << (METADATA_CHANGE_GROUP_CODE[group] - 1)), 0);
      assert.equal(mask, vector.groups_mask, vector.case);
      assert.equal(token.lastGroup, vector.group, vector.case);
      assert.equal(token.lastKey.toString('hex'), vector.last_key_hex, vector.case);
      assert.equal(token.toString(), vector.text, vector.case);
      assert.equal(token.toBytes().toString('hex'), vector.hex, vector.case);
      assert.equal(MetadataCurrentPageToken.fromBytes(fromHex(vector.hex)).toString(), vector.text, vector.case);
    }
    const longest = fixture.page_tokens.find((vector) => vector.case === 'longest_token_name');
    assert.equal(longest?.hex.length, constants.max_page_token_size * 2);
  });

  // The Node SDK has no HTTP transport, so http_query and the refused HTTP
  // queries have no encoder to check.
  it('encodes every request vector byte for byte', () => {
    for (const vector of fixture.requests)
      assert.equal(
        GET_METADATA_CHANGES.serialize(fixtureQuery(vector.input)).toString('hex'), vector.request_hex, vector.case
      );
  });

  it('decodes every response vector to the answer the HTTP route renders', () => {
    for (const vector of fixture.responses)
      assert.deepEqual(changesView(deserializeMetadataChanges(fromHex(vector.response_hex))), vector.expected, vector.case);
  });

  it('keeps 64-bit and 128-bit values above 2^53 exact', () => {
    const large = fixture.large_values;
    const [record] = deserializeMetadataChanges(changesPage(fromHex(large.record_hex))).records;
    assert.equal(record.kind, MetadataChangeKind.SessionRegistered);
    if (record.kind !== 'session_registered')
      return;
    assert.equal(record.op, BigInt(large.op));
    assert.equal(record.ordinal, large.ordinal);
    assert.equal(record.timestamp, BigInt(large.timestamp));
    assert.equal(hex128(record.checksum), large.checksum);
    assert.equal(record.body.userCreatedAt, BigInt(large.user_created_at));
    assert.equal(record.body.session, BigInt(large.session));
    assert.equal(hex128(record.body.clientId), large.client_id);
  });
});

describe('given refused vectors when decoding or validating should refuse', () => {
  it('refuses every malformed cursor text', () => {
    for (const vector of fixture.refused.cursors)
      assert.throws(() => MetadataChangeCursor.parse(vector.text), isInvalidCommand, vector.case);
  });

  it('refuses every malformed page token text', () => {
    for (const vector of fixture.refused.page_tokens)
      assert.throws(() => MetadataCurrentPageToken.parse(vector.text), isInvalidCommand, vector.case);
  });

  it('refuses every malformed record body', () => {
    for (const vector of fixture.refused.record_bodies)
      assert.throws(
        () => deserializeMetadataChanges(changesPage(recordEnvelope(vector.group, vector.kind, fromHex(vector.body_hex)))),
        isMalformed, vector.case
      );
  });

  it('cannot produce any request the server refuses', () => {
    const earliest = (): Buffer => GET_METADATA_CHANGES.serialize({});
    for (const vector of fixture.refused.requests) {
      const request = fromHex(vector.hex);
      const cursor = request.subarray(1, 1 + METADATA_CHANGE_CURSOR_SIZE);
      switch (vector.case) {
        case 'cursor_bytes_without_after':
          assert.ok(cursor.some((byte) => byte !== 0));
          assert.ok(earliest().subarray(1, 1 + METADATA_CHANGE_CURSOR_SIZE).every((byte) => byte === 0));
          break;
        case 'trailing_byte':
          assert.deepEqual(earliest(), request.subarray(0, request.length - 1));
          break;
        case 'unknown_start_mode':
          assert.throws(
            () => GET_METADATA_CHANGES.serialize({ start: { mode: `mode-${request[0]}` } as unknown as MetadataChangeStart }),
            isInvalidCommand
          );
          break;
        case 'non_canonical_cursor':
          assert.throws(
            () => new MetadataChangeCursor(
              cursor.readBigUInt64LE(0) | (cursor.readBigUInt64LE(8) << 64n), cursor.readBigUInt64LE(16),
              cursor.readBigUInt64LE(24) | (cursor.readBigUInt64LE(32) << 64n), cursor.readUInt32LE(40)
            ),
            isInvalidCommand
          );
          break;
        default:
          assert.fail(`a refused request vector this test does not cover: ${vector.case}`);
      }
    }
  });

  it('refuses every malformed response', () => {
    for (const vector of fixture.refused.responses)
      assert.throws(() => deserializeMetadataChanges(fromHex(vector.hex)), isMalformed, vector.case);
  });

  it('refuses every invalid query before network io', async () => {
    let sent = 0;
    const provider: ClientProvider = async () => {
      sent += 1;
      throw new Error('must not connect');
    };
    for (const vector of fixture.refused.queries)
      await assert.rejects(getMetadataChanges(provider)(fixtureQuery(vector.input)), isInvalidCommand, vector.case);
    assert.equal(sent, 0);
  });
});

describe('given metadata change queries when encoding should apply the request rules', () => {
  const streamsToken = MetadataCurrentPageToken.parse(fixture.page_tokens[0].text);

  it('accepts the bounds of every rule', () => {
    const one = 1n;
    const accepted: GetMetadataChanges[] = [
      { limit: 1 },
      { limit: MAX_METADATA_CHANGES_LIMIT },
      { start: MetadataChangeStart.After(new MetadataChangeCursor(1n, 0n, 0n, 0)), since: one, until: one },
      { start: MetadataChangeStart.Latest, groups: ['users', 'users'] },
      { start: MetadataChangeStart.Current(streamsToken), groups: ['streams'] },
      { start: MetadataChangeStart.Current() },
      {
        start: MetadataChangeStart.Current(),
        groups: ['consumer_group_members', 'consumer_groups', 'partitions', 'topics', 'streams']
      }
    ];
    for (const query of accepted)
      assert.ok(GET_METADATA_CHANGES.serialize(query).length > 0);
  });

  it('refuses what the server refuses', () => {
    const refused: GetMetadataChanges[] = [
      { limit: 0 },
      { limit: 1.5 },
      { groups: ['everything' as MetadataChangeGroup] },
      { since: -1n },
      { until: 1n << 64n },
      { start: MetadataChangeStart.Current(), groups: ['sessions'] },
      { start: MetadataChangeStart.Current(streamsToken) },
      { start: MetadataChangeStart.Current(null as unknown as MetadataCurrentPageToken) },
      { start: MetadataChangeStart.Current(fixture.page_tokens[0].text as unknown as MetadataCurrentPageToken) }
    ];
    for (const query of refused)
      assert.throws(() => GET_METADATA_CHANGES.serialize(query), isInvalidCommand, JSON.stringify(query, (_, value) =>
        typeof value === 'bigint' ? value.toString() : value));
  });

  it('requires every parent group of a current scan', () => {
    for (const [name, { parents }] of Object.entries(constants.groups)) {
      const alone = { start: MetadataChangeStart.Current(), groups: [name as MetadataChangeGroup] };
      if (parents.length === 0) {
        assert.ok(GET_METADATA_CHANGES.serialize(alone), name);
        continue;
      }
      assert.throws(() => GET_METADATA_CHANGES.serialize(alone), isInvalidCommand, name);
      assert.ok(GET_METADATA_CHANGES.serialize({
        ...alone, groups: [name as MetadataChangeGroup, ...parents as MetadataChangeGroup[]]
      }), name);
    }
  });

  it('keeps every requested group when duplicates exceed the count byte', () => {
    const groups: MetadataChangeGroup[] = ['topics', 'streams', 'topics', ...Array(300).fill('users')];
    const payload = GET_METADATA_CHANGES.serialize({ groups });
    const countAt = 1 + METADATA_CHANGE_CURSOR_SIZE + 1 + 1 + 4;
    assert.equal(payload[countAt], 3);
    assert.deepEqual([...payload.subarray(countAt + 1, countAt + 4)], [2, 1, 6]);
    assert.equal(payload.length, countAt + 1 + 3 + 2);
  });

  it('leaves the cursor out of a latest start and the token out of a non current start', () => {
    const latest = GET_METADATA_CHANGES.serialize({ start: MetadataChangeStart.Latest });
    const earliest = GET_METADATA_CHANGES.serialize({});
    assert.equal(latest[0], 1);
    assert.deepEqual(latest.subarray(1), earliest.subarray(1));
  });

  it('refuses canonical text forms only', () => {
    const tokenText = fixture.page_tokens[0].text;
    for (const text of [
      tokenText.slice(0, 20) + '\n' + tokenText.slice(20),
      `${tokenText}=`,
      tokenText.replace('cs1_', 'CS1_')
    ])
      assert.throws(() => MetadataCurrentPageToken.parse(text), isInvalidCommand, JSON.stringify(text));
    const cursorText = fixture.cursors[0].text;
    assert.throws(() => MetadataChangeCursor.parse(`${cursorText.slice(0, -1)}B`), isInvalidCommand);
    assert.throws(() => new MetadataChangeCursor(-1n, 1n, 0n, 0), isInvalidCommand);
    assert.throws(() => new MetadataChangeCursor(0n, 1n, 0n, 2 ** 32), isInvalidCommand);
  });

  it('copies the token key out', () => {
    const token = MetadataCurrentPageToken.parse(fixture.page_tokens[0].text);
    token.lastKey.fill(0xff);
    token.toBytes().fill(0xff);
    assert.equal(token.toString(), fixture.page_tokens[0].text);
  });
});

describe('given malformed responses when decoding should refuse every one', () => {
  it('refuses every truncated response vector', () => {
    for (const vector of fixture.responses) {
      const response = fromHex(vector.response_hex);
      for (let length = 0; length < response.length; length++)
        assert.throws(() => deserializeMetadataChanges(response.subarray(0, length)), isMalformed,
          `${vector.case} truncated to ${length} bytes`);
    }
  });

  it('refuses every truncated or extended record body', () => {
    for (const vector of fixture.records) {
      const body = fromHex(vector.body_hex);
      for (let length = 0; length < body.length; length++)
        assert.throws(
          () => deserializeMetadataChanges(changesPage(recordEnvelope(vector.group, vector.kind, body.subarray(0, length)))),
          isMalformed, `${vector.case} truncated to ${length} bytes`
        );
      assert.throws(
        () => deserializeMetadataChanges(changesPage(recordEnvelope(vector.group, vector.kind, Buffer.concat([body, Buffer.from([0])])))),
        isMalformed, `${vector.case} with a trailing byte`
      );
    }
  });

  it('refuses a huge record count without allocating it', () => {
    const payload = changesPage();
    payload.writeUInt32LE(0xffff_ffff, payload.length - 2 - 4 - 4);
    assert.throws(() => deserializeMetadataChanges(payload), isMalformed);
  });

  it('keeps an unknown kind of a known group with its raw body', () => {
    const [record] = deserializeMetadataChanges(changesPage(recordEnvelope(1, 9, Buffer.from([0xde, 0xad])))).records;
    assert.equal(record.kind, 'unknown');
    assert.equal(record.group, 'streams');
    assert.equal(record.groupCode, 1);
    assert.equal(record.kindCode, 9);
    assert.deepEqual(record.body, Buffer.from([0xde, 0xad]));
  });

  it('keeps an unknown item group with its raw body', () => {
    const [item] = deserializeMetadataChanges(currentPage(Buffer.concat([Buffer.from([200]), u32(3), Buffer.from([1, 2, 3])]))).items;
    assert.equal(item.group, 'unknown');
    assert.equal(item.groupCode, 200);
    assert.deepEqual(item.body, Buffer.from([1, 2, 3]));
  });
});

it('given_many_duplicate_groups_when_encoding_should_preserve_every_requested_group', () => {
  const groups: MetadataChangeGroup[] = Array(255).fill('streams');
  groups.push('users');
  const payload = GET_METADATA_CHANGES.serialize({ groups });
  const groupCountAt = 1 + METADATA_CHANGE_CURSOR_SIZE + 2 + 4;
  const encoded = payload.subarray(groupCountAt + 1, groupCountAt + 1 + payload[groupCountAt]);
  assert.deepEqual(new Set(encoded), new Set([1, 6]));
});
