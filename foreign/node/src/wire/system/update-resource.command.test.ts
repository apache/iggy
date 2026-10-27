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
import { ResponseError } from '../error.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { HeaderValue } from '../message/header.utils.js';
import {
  OPTION_CATALOG, StreamOptionKey, TopicOptionKey, UserOptionKey, deserializeOptions,
  type OptionEntry, type ParsedOptions
} from '../options.utils.js';
import { CREATE_STREAM, createStream } from '../stream/create-stream.command.js';
import { UPDATE_STREAM } from '../stream/update-stream.command.js';
import { StreamEncryptionKey, encryptionKeyFingerprint } from '../stream/stream-options.js';
import { deserializeToStream, type Stream } from '../stream/stream.utils.js';
import { CREATE_TOPIC } from '../topic/create-topic.command.js';
import { UPDATE_TOPIC } from '../topic/update-topic.command.js';
import {
  CompressionAlgorithm, compressionAlgorithmName, deserializeTopic, Durability, PartitionResizePolicy
} from '../topic/topic.utils.js';
import { CREATE_USER } from '../user/create-user.command.js';
import { translateErrorCode } from '../error.code.js';
import { UPDATE_USER } from '../user/update-user.command.js';
import { deserializeUser, UserStatus } from '../user/user.utils.js';
import { Command, COMMITTED_RESULT_MARKER, HEADER_SIZE, REPLY_OFFSET, REQUEST_OFFSET } from '../vsr/header.js';
import { VsrSession } from '../vsr/index.js';
import {
  Operation, isInternal, isKnownOperation, operationForCode, requestOperation
} from '../vsr/operation.js';
import { IGGY_PROTOCOL_VERSION } from '../vsr/register.js';
import { validateReplyForRequest } from '../vsr/reply.js';
import { DESCRIBE_OPTIONS, OptionsScope, type OptionSpec } from './describe-options.command.js';
import { UPDATE_RESOURCE } from './update-resource.command.js';

type FixtureError = { name: string, code: number };
type FixtureOption = { value: string, kind: string, explicit: boolean };
type FixtureOptions = Record<string, FixtureOption>;
type FixtureKey = { raw_hex?: string, base64?: string };
type Scope = 'topic' | 'stream' | 'user';
type Target = { stream_id?: Id, topic_id?: Id, user_id?: Id };
type UpdateInput = {
  name?: string, username?: string, status?: string,
  message_expiry?: string, max_topic_size?: string, reset?: string[]
};

type Fixture = {
  constants: {
    protocol_version: { packed: number },
    command_codes: Record<string, { code: number, operation: number | null }>,
    error_codes: Record<string, number>,
    retired_command_codes: Record<string, number>,
    operations: Record<string, { value: number, internal: boolean, request_operation: number }>,
    retired_operations: Record<string, number>,
    scope_codes: Record<Scope, number>,
    header_kind_codes: Record<string, number>,
    option_flags: Record<string, number>,
    user_status_codes: Record<string, number>,
    stream_key_length: number,
    never_returned_keys: string[]
  },
  catalog: {
    key: string, scopes: Scope[], storage: string, mutability: string, visibility: string,
    flags: number, described: boolean
  }[],
  describe_options: {
    scope: Scope, request_hex: string, response_hex: string,
    entries: { key: string, kind: string, flags: number, default: string | null }[]
  }[],
  stream_key: {
    key_hex: string, key_base64: string, fingerprint: string,
    invalid_keys: { case: string, input: FixtureKey, error: FixtureError }[]
  },
  update_resource: {
    case: string, scope: Scope, target: Target, input: UpdateInput, body_hex: string
  }[],
  update_resource_errors: {
    case: string, scope: Scope, target: Target, input: UpdateInput, error: FixtureError
  }[],
  create_stream: {
    case: string,
    input: { name: string, encryption_key?: FixtureKey, raw?: Record<string, string> },
    body_hex: string
  }[],
  create_topic: {
    case: string,
    input: {
      stream_id: Id, name: string, partitions_count: number, compression_algorithm: string,
      message_expiry: string, max_topic_size: string, segment_size: string,
      durability: Durability, consumer_offset_durability: Durability
    },
    body_hex: string
  }[],
  create_user: {
    case: string, input: { username: string, password: string, status: string }, body_hex: string
  }[],
  stream_responses: {
    case: string, response_hex: string,
    expected: {
      id: number, created_at: number, name: string, topics_count: number,
      size_bytes: number, messages_count: number, options: FixtureOptions,
      encryption_key_source: string, encryption_key_fingerprint: string | null,
      encryption_kek_fingerprints: string[]
    }
  }[],
  topic_responses: {
    case: string, response_hex: string,
    expected: {
      id: number, created_at: number, name: string, partitions_count: number,
      size_bytes: number, messages_count: number, options: FixtureOptions,
      compression_algorithm: string, message_expiry: string, max_topic_size: string
    }
  }[],
  user_responses: {
    case: string, response_hex: string,
    expected: {
      id: number, created_at: number, status: string, username: string, options: FixtureOptions
    }
  }[]
};

const fixture: Fixture = JSON.parse(
  readFileSync(new URL('../fixtures/resource_options.json', import.meta.url), 'utf8')
);
const { constants } = fixture;
const INVALID_ENCRYPTION_KEY = 70;
const UNSUPPORTED_OPTION_KEY = 4041;
const UPDATE_RESOURCE_CODE = constants.command_codes.update_resource.code;

const fromHex = (hex: string) => Buffer.from(hex, 'hex');
const keyInput = (key: FixtureKey): Buffer | string =>
  key.raw_hex !== undefined ? fromHex(key.raw_hex) : key.base64!;
const userStatus = (status: string) =>
  status === 'active' ? UserStatus.Active : UserStatus.Inactive;
const optionalBigInt = (value?: string) => value === undefined ? undefined : BigInt(value);

const kindName = (value: unknown): string => {
  if (typeof value === 'bigint') return 'uint64';
  if (typeof value === 'number') return 'uint32';
  if (typeof value === 'boolean') return 'bool';
  if (Buffer.isBuffer(value)) return 'raw';
  return 'string';
};

const optionText = (value: unknown): string =>
  Buffer.isBuffer(value) ? value.toString('base64') : String(value);

/** Both blocks in the fixture's HTTP form, so provenance is compared too. */
const withProvenance = (explicit: ParsedOptions, derived: ParsedOptions): FixtureOptions => {
  const merged: FixtureOptions = {};
  for (const [options, isExplicit] of [[derived, false], [explicit, true]] as const)
    for (const [key, value] of Object.entries(options))
      merged[key] = { value: optionText(value), kind: kindName(value), explicit: isExplicit };
  return merged;
};

const assertNoSecrets = (explicit: ParsedOptions, derived: ParsedOptions) => {
  for (const key of constants.never_returned_keys) {
    assert.equal(key in explicit, false, `explicit block returned ${key}`);
    assert.equal(key in derived, false, `derived block returned ${key}`);
  }
};

const encodeUpdate = (scope: Scope, target: Target, input: UpdateInput): Buffer => {
  const reset = input.reset as never[] | undefined;
  switch (scope) {
    case 'topic':
      return UPDATE_TOPIC.serialize({
        streamId: target.stream_id!, topicId: target.topic_id!, name: input.name,
        messageExpiry: optionalBigInt(input.message_expiry),
        maxTopicSize: optionalBigInt(input.max_topic_size), reset
      });
    case 'stream':
      return UPDATE_STREAM.serialize({ streamId: target.stream_id!, name: input.name, reset });
    case 'user':
      return UPDATE_USER.serialize({
        userId: target.user_id!, username: input.username,
        status: input.status === undefined ? undefined : userStatus(input.status), reset
      });
  }
};

const isClientError = (code: number, commandCode: number) => (error: unknown) =>
  error instanceof ResponseError && error.errorCode === code &&
    error.commandCode === commandCode && !error.fromServer;

const defaultText = (spec: OptionSpec): string | null => {
  if (spec.defaultValue.length === 0) return null;
  switch (spec.kind) {
    case constants.header_kind_codes.uint64: return spec.defaultValue.readBigUInt64LE(0).toString();
    case constants.header_kind_codes.uint32: return spec.defaultValue.readUInt32LE(0).toString();
    case constants.header_kind_codes.bool: return String(spec.defaultValue.readUInt8(0) === 1);
    default: return spec.defaultValue.toString();
  }
};

const specFlags = (spec: OptionSpec): number =>
  (spec.updatable ? constants.option_flags.updatable : 0) |
  (spec.resettable ? constants.option_flags.resettable : 0) |
  (spec.secret ? constants.option_flags.secret : 0) |
  (spec.readOnly ? constants.option_flags.read_only : 0) |
  (spec.critical ? constants.option_flags.critical : 0);

const kindNames = new Map(
  Object.entries(constants.header_kind_codes).map(([name, code]) => [code, name])
);

describe('given shared vectors when encoding should match server contract', () => {

  it('encodes every update vector byte for byte through the typed scope commands', () => {
    for (const vector of fixture.update_resource) {
      const encoded = encodeUpdate(vector.scope, vector.target, vector.input);
      assert.equal(encoded.toString('hex'), vector.body_hex, vector.case);
    }
    for (const command of [UPDATE_STREAM, UPDATE_TOPIC, UPDATE_USER, UPDATE_RESOURCE])
      assert.equal(command.code, UPDATE_RESOURCE_CODE);
  });

  it('refuses every invalid update before sending, with the server category', () => {
    for (const vector of fixture.update_resource_errors)
      assert.throws(
        () => encodeUpdate(vector.scope, vector.target, vector.input),
        isClientError(vector.error.code, UPDATE_RESOURCE_CODE),
        vector.case
      );
  });

  it('refuses each reset category in the server order', () => {
    const cases: [Scope, string[], number][] = [
      ['topic', ['encryption_key'], 4041],
      ['stream', ['future_option'], 4041],
      ['stream', ['encryption_key_fingerprint'], 4105],
      ['stream', ['critical_keys'], 4105],
      ['topic', ['segment_size', 'name'], 4103],
      ['topic', ['name'], 4104],
      ['user', ['username'], 4104],
      ['topic', ['name', 'encryption_key'], 4041]
    ];
    for (const [scope, reset, code] of cases)
      assert.throws(
        () => encodeUpdate(scope, { stream_id: 1, topic_id: 2, user_id: 3 }, { reset }),
        isClientError(code, UPDATE_RESOURCE_CODE),
        `${scope} reset ${reset.join(',')}`
      );
  });

  it('lets a typed field win over a raw entry of the same key', () => {
    const typed = UPDATE_TOPIC.serialize({
      streamId: 1, topicId: 2, maxTopicSize: 9007199254740993n,
      options: [{ key: 'max_topic_size', value: HeaderValue.String('1 GiB') }]
    });
    const vector = fixture.update_resource
      .find((entry) => entry.case === 'topic_max_topic_size_above_2_pow_53')!;
    assert.equal(typed.toString('hex'), vector.body_hex);
  });

  it('sends raw set keys and raw reset names for the server to decide', () => {
    const body = UPDATE_TOPIC.serialize({
      streamId: 1, topicId: 2, reset: ['name'], rawReset: ['future_option'],
      options: [{ key: 'compression_algorithm', value: HeaderValue.String('gzip') }]
    });
    for (const name of ['name', 'future_option', 'compression_algorithm'])
      assert.ok(body.includes(Buffer.from(name)), name);
    assert.doesNotThrow(() => UPDATE_STREAM.serialize({ streamId: 3, rawReset: ['name'] }));
    assert.doesNotThrow(() => UPDATE_USER.serialize({
      userId: 5, options: [{ key: 'username', value: HeaderValue.String('a') }]
    }));
    assert.throws(
      () => UPDATE_USER.serialize({ userId: 5, status: UserStatus.Active, rawReset: ['status'] }),
      isClientError(3, UPDATE_RESOURCE_CODE)
    );
    assert.throws(
      () => UPDATE_TOPIC.serialize({ streamId: 1, topicId: 2, rawReset: [] }),
      isClientError(3, UPDATE_RESOURCE_CODE)
    );
  });

  it('refuses a raw reset list its count byte or a name length byte cannot carry', () => {
    const futureNames = (count: number) =>
      Array.from({ length: count }, (_, index) => `future_${index}`);
    for (const rawReset of [futureNames(256), [''], ['x'.repeat(256)]])
      assert.throws(
        () => UPDATE_TOPIC.serialize({ streamId: 1, topicId: 2, rawReset }),
        isClientError(UNSUPPORTED_OPTION_KEY, UPDATE_RESOURCE_CODE),
        `${rawReset.length} names, first ${rawReset[0].length} bytes`
      );
    const body = UPDATE_TOPIC.serialize({ streamId: 1, topicId: 2, rawReset: futureNames(255) });
    assert.equal(body[1 + 2 * serializeIdentifier(1).length], 255);
  });

  it('writes typed and raw reset names together in ascending name order', () => {
    const body = UPDATE_TOPIC.serialize({
      streamId: 1, topicId: 2, reset: ['message_expiry', 'max_topic_size'], rawReset: ['a_future']
    });
    const resets = body.subarray(1 + 2 * serializeIdentifier(1).length);
    const names: string[] = [];
    for (let pos = 1; names.length < resets[0]; pos += 1 + resets[pos])
      names.push(resets.subarray(pos + 1, pos + 1 + resets[pos]).toString());
    assert.deepEqual(names, ['a_future', 'max_topic_size', 'message_expiry']);
  });

  it('exposes the typed surface for every public catalog row', () => {
    const rows = (scope: Scope, accept: (row: Fixture['catalog'][number]) => boolean) =>
      fixture.catalog.filter((row) => row.scopes.includes(scope) && accept(row))
        .map((row) => row.key).sort();
    const optionsAt = (body: Buffer, offset: number) => deserializeOptions(body, offset);
    const idLength = serializeIdentifier(1).length;
    const topic = { streamId: 1, name: 'events', partitionCount: 1 };
    const topicBlock = (fields: Partial<Parameters<typeof CREATE_TOPIC.serialize>[0]>) =>
      optionsAt(CREATE_TOPIC.serialize({ ...topic, ...fields }), idLength + 5 + topic.name.length);
    const typedCreate: Record<Scope, Record<string, () => ParsedOptions>> = {
      stream: {
        encryption_key: () => optionsAt(CREATE_STREAM.serialize({
          name: 'orders', encryptionKey: StreamEncryptionKey.from(fromHex(fixture.stream_key.key_hex))
        }), 1 + 'orders'.length)
      },
      topic: {
        compression_algorithm: () => topicBlock({ compressionAlgorithm: CompressionAlgorithm.Gzip }),
        message_expiry: () => topicBlock({ messageExpiry: 1n }),
        max_topic_size: () => topicBlock({ maxTopicSize: 1n }),
        segment_size: () => topicBlock({ segmentSize: 1048576n }),
        durability: () => topicBlock({ durability: Durability.Persisted }),
        consumer_offset_durability: () => topicBlock({ consumerOffsetDurability: Durability.Persisted }),
        partition_resize_policy: () =>
          topicBlock({ partitionResizePolicy: PartitionResizePolicy.Fixed }),
        messages_required_to_save: () => topicBlock({ messagesRequiredToSave: 1 }),
        size_of_messages_required_to_save: () => topicBlock({ sizeOfMessagesRequiredToSave: 1n }),
        preallocate_segments: () => topicBlock({ preallocateSegments: true })
      },
      user: {}
    };
    const baseCreate: Record<Scope, ParsedOptions> = {
      stream: optionsAt(CREATE_STREAM.serialize({ name: 'orders' }), 1 + 'orders'.length),
      topic: topicBlock({}),
      user: {}
    };
    const updateBlock = (body: Buffer, ids: number) => optionsAt(body, 1 + ids * idLength + 1);
    const typedUpdate: Record<Scope, Record<string, () => ParsedOptions>> = {
      stream: { name: () => updateBlock(UPDATE_STREAM.serialize({ streamId: 1, name: 'renamed' }), 1) },
      topic: {
        name: () => updateBlock(UPDATE_TOPIC.serialize({ streamId: 1, topicId: 1, name: 'renamed' }), 2),
        message_expiry: () =>
          updateBlock(UPDATE_TOPIC.serialize({ streamId: 1, topicId: 1, messageExpiry: 1n }), 2),
        max_topic_size: () =>
          updateBlock(UPDATE_TOPIC.serialize({ streamId: 1, topicId: 1, maxTopicSize: 1n }), 2)
      },
      user: {
        username: () => updateBlock(UPDATE_USER.serialize({ userId: 1, username: 'alice2' }), 1),
        status: () => updateBlock(UPDATE_USER.serialize({ userId: 1, status: UserStatus.Inactive }), 1)
      }
    };
    const typedReset: Record<Scope, (key: string) => Buffer> = {
      stream: (key) => UPDATE_STREAM.serialize({ streamId: 1, reset: [key as StreamOptionKey] }),
      topic: (key) => UPDATE_TOPIC.serialize({ streamId: 1, topicId: 1, reset: [key as TopicOptionKey] }),
      user: (key) => UPDATE_USER.serialize({ userId: 1, reset: [key as UserOptionKey] })
    };
    const resetMembers: Record<Scope, readonly string[]> = {
      stream: Object.values(StreamOptionKey),
      topic: Object.values(TopicOptionKey),
      user: Object.values(UserOptionKey)
    };
    const readAccessors: Record<Scope, Record<string, keyof Stream>> = {
      stream: {
        encryption_key_source: 'encryptionKeySource',
        encryption_key_fingerprint: 'encryptionKeyFingerprint',
        encryption_kek_fingerprint: 'encryptionKekFingerprints'
      },
      topic: {},
      user: {}
    };
    const stream = deserializeToStream(fromHex(fixture.stream_responses[0].response_hex)).data;

    for (const scope of ['stream', 'topic', 'user'] as const) {
      const createKeys = rows(scope, (row) => row.storage === 'map' &&
        ['public', 'secret'].includes(row.visibility) &&
        ['create_only', 'updatable'].includes(row.mutability));
      assert.deepEqual(Object.keys(typedCreate[scope]).sort(), createKeys, `${scope} create`);
      for (const [key, block] of Object.entries(typedCreate[scope])) {
        const options = block();
        assert.ok(key in options, `${scope} create ${key}`);
        assert.notDeepEqual(options[key], baseCreate[scope][key], `${scope} create ${key}`);
      }

      const updateKeys = rows(scope, (row) => row.mutability === 'updatable');
      assert.deepEqual(Object.keys(typedUpdate[scope]).sort(), updateKeys, `${scope} update`);
      for (const [key, block] of Object.entries(typedUpdate[scope]))
        assert.deepEqual(Object.keys(block()), [key], `${scope} update ${key}`);

      const resetKeys = rows(scope, (row) => (row.flags & constants.option_flags.resettable) !== 0);
      for (const key of resetKeys) {
        assert.ok(resetMembers[scope].includes(key), `${scope} reset member ${key}`);
        assert.ok(typedReset[scope](key).includes(Buffer.from(key)), `${scope} reset ${key}`);
      }

      const readOnlyKeys = rows(scope, (row) => row.visibility === 'read_only');
      assert.deepEqual(Object.keys(readAccessors[scope]).sort(), readOnlyKeys, `${scope} read`);
      for (const accessor of Object.values(readAccessors[scope]))
        assert.ok(accessor in stream, `${scope} accessor ${accessor}`);
    }
  });

  it('encodes create commands byte for byte', () => {
    for (const vector of fixture.create_stream) {
      const { name, encryption_key, raw = {} } = vector.input;
      const options: OptionEntry[] = Object.entries(raw)
        .map(([key, value]) => ({ key, value: HeaderValue.String(value) }));
      const encoded = CREATE_STREAM.serialize({
        name,
        encryptionKey: encryption_key === undefined
          ? undefined : StreamEncryptionKey.from(keyInput(encryption_key)),
        options
      });
      assert.equal(encoded.toString('hex'), vector.body_hex, vector.case);
    }
    for (const { input, body_hex, case: name } of fixture.create_topic) {
      const encoded = CREATE_TOPIC.serialize({
        streamId: input.stream_id,
        name: input.name,
        partitionCount: input.partitions_count,
        compressionAlgorithm: input.compression_algorithm === 'gzip'
          ? CompressionAlgorithm.Gzip : CompressionAlgorithm.None,
        messageExpiry: BigInt(input.message_expiry),
        maxTopicSize: BigInt(input.max_topic_size),
        segmentSize: BigInt(input.segment_size),
        durability: input.durability,
        consumerOffsetDurability: input.consumer_offset_durability
      });
      assert.equal(encoded.toString('hex'), body_hex, name);
    }
    for (const { input, body_hex, case: name } of fixture.create_user) {
      const encoded = CREATE_USER.serialize({
        username: input.username, password: input.password,
        status: userStatus(input.status), options: []
      });
      assert.equal(encoded.toString('hex'), body_hex, name);
    }
  });

  it('decodes stream responses into their projection without secrets', () => {
    for (const { case: name, response_hex, expected } of fixture.stream_responses) {
      const response = fromHex(response_hex);
      const { bytesRead, data: stream } = deserializeToStream(response);
      assert.equal(bytesRead, response.length, name);
      assert.deepEqual({
        id: stream.id,
        created_at: stream.createdAt.getTime() * 1000,
        name: stream.name,
        topics_count: stream.topicsCount,
        size_bytes: Number(stream.sizeBytes),
        messages_count: Number(stream.messagesCount),
        options: withProvenance(stream.options, stream.derivedOptions),
        encryption_key_source: stream.encryptionKeySource,
        encryption_key_fingerprint: stream.encryptionKeyFingerprint,
        encryption_kek_fingerprints: stream.encryptionKekFingerprints
      }, expected, name);
      assertNoSecrets(stream.options, stream.derivedOptions);
    }
  });

  it('decodes topic responses with the settings read from the option blocks', () => {
    for (const { case: name, response_hex, expected } of fixture.topic_responses) {
      const response = fromHex(response_hex);
      const { bytesRead, data: topic } = deserializeTopic(response);
      assert.equal(bytesRead, response.length, name);
      assert.deepEqual({
        id: topic.id,
        created_at: topic.createdAt.getTime() * 1000,
        name: topic.name,
        partitions_count: topic.partitionsCount,
        size_bytes: Number(topic.sizeBytes),
        messages_count: Number(topic.messagesCount),
        options: withProvenance(topic.options, topic.derivedOptions),
        compression_algorithm: compressionAlgorithmName(topic.compressionAlgorithm),
        message_expiry: topic.messageExpiry.toString(),
        max_topic_size: topic.maxTopicSize.toString()
      }, expected, name);
    }
  });

  it('decodes user responses with both option blocks, keeping u64 precision', () => {
    for (const { case: name, response_hex, expected } of fixture.user_responses) {
      const user = deserializeUser(fromHex(response_hex));
      assert.deepEqual({
        id: user.id,
        created_at: user.createdAt.getTime() * 1000,
        status: user.status.toLowerCase(),
        username: user.userName,
        options: withProvenance(user.options, user.derivedOptions)
      }, expected, name);
      assertNoSecrets(user.options, user.derivedOptions);
    }
  });

  it('describes every scope with the catalog flags', () => {
    for (const { scope, request_hex, response_hex, entries } of fixture.describe_options) {
      assert.equal(
        DESCRIBE_OPTIONS.serialize({ scope: constants.scope_codes[scope] as OptionsScope })
          .toString('hex'),
        request_hex
      );
      const data = fromHex(response_hex);
      const specs = DESCRIBE_OPTIONS.deserialize({ status: 0, length: data.length, data });
      assert.deepEqual(specs.map((spec) => ({
        key: spec.key,
        kind: kindNames.get(spec.kind),
        flags: specFlags(spec),
        default: defaultText(spec)
      })), entries, scope);
    }
  });

  it('mirrors the catalog in its key constants and reset rules', () => {
    assert.deepEqual([...OPTION_CATALOG.keys()], fixture.catalog.map((row) => row.key));
    for (const row of fixture.catalog) {
      const descriptor = OPTION_CATALOG.get(row.key)!;
      assert.deepEqual(
        descriptor.scopes.map((code) => Object.entries(constants.scope_codes)
          .find(([, value]) => value === code)![0]).sort(),
        [...row.scopes].sort(),
        row.key
      );
      assert.equal(descriptor.flags, row.flags, row.key);
      assert.equal(descriptor.internal, row.visibility === 'internal', row.key);
    }
    const described = (scope: Scope) => fixture.catalog
      .filter((row) => row.described && row.scopes.includes(scope)).map((row) => row.key);
    assert.deepEqual(Object.values(StreamOptionKey), described('stream'));
    assert.deepEqual(Object.values(TopicOptionKey), described('topic'));
    assert.deepEqual(Object.values(UserOptionKey), described('user'));
  });

  it('mirrors command codes, operations, scopes and the protocol version', () => {
    assert.equal(IGGY_PROTOCOL_VERSION, constants.protocol_version.packed);
    assert.equal(translateErrorCode(constants.error_codes.cannot_modify_root_user), 'Cannot modify root user');
    assert.deepEqual(
      { topic: OptionsScope.Topic, stream: OptionsScope.Stream, user: OptionsScope.User },
      constants.scope_codes
    );
    assert.deepEqual(
      { active: UserStatus.Active, inactive: UserStatus.Inactive },
      constants.user_status_codes
    );
    const codes = new Set<number>(Object.values(COMMAND_CODE));
    for (const [name, { code, operation }] of Object.entries(constants.command_codes)) {
      assert.ok(codes.has(code), name);
      assert.equal(operationForCode(code), operation ?? Operation.NonReplicated, name);
    }
    for (const [name, code] of Object.entries(constants.retired_command_codes))
      assert.equal(codes.has(code), false, `${name} code ${code} is retired`);
    for (const [name, operation] of Object.entries(constants.operations)) {
      assert.ok(isKnownOperation(operation.value), name);
      assert.equal(isInternal(operation.value), operation.internal, name);
      assert.equal(requestOperation(operation.value), operation.request_operation, name);
    }
    for (const [name, operation] of Object.entries(constants.retired_operations))
      assert.equal(isKnownOperation(operation), false, `${name} operation ${operation} is retired`);
  });

  it('computes the stream key fingerprint from raw bytes and from base64', () => {
    const { key_hex, key_base64, fingerprint } = fixture.stream_key;
    assert.equal(fromHex(key_hex).length, constants.stream_key_length);
    assert.equal(encryptionKeyFingerprint(fromHex(key_hex)), fingerprint);
    assert.equal(encryptionKeyFingerprint(key_base64), fingerprint);
  });
});

describe('given invalid key when encoding should reject before network io', () => {
  const { key_hex, key_base64, invalid_keys } = fixture.stream_key;
  const urlSafe = Buffer.alloc(32, 0xff).toString('base64url') + '=';
  const unexpectedNetwork: ClientProvider = () => {
    throw new Error('an invalid key reached the network');
  };

  const refusals = [
    ...invalid_keys.map(({ case: name, input, error }) => ({
      name, input: keyInput(input), code: error.code
    })),
    { name: 'base64_url_safe_alphabet', input: urlSafe, code: INVALID_ENCRYPTION_KEY },
    { name: 'base64_leading_space', input: ` ${key_base64}`, code: INVALID_ENCRYPTION_KEY },
    { name: 'empty_string', input: '', code: INVALID_ENCRYPTION_KEY }
  ];

  for (const { name, input, code } of refusals) {
    it(`refuses ${name}`, async () => {
      const redacted = (error: unknown) => {
        assert.ok(error instanceof ResponseError);
        assert.equal(error.errorCode, code);
        assert.equal(error.fromServer, false);
        const text = `${error.message} ${String(error.stack)}`;
        const hex = Buffer.isBuffer(input) ? input.toString('hex') : input.trim();
        assert.ok(hex.length === 0 || !text.includes(hex), 'diagnostic carries key material');
        return true;
      };
      assert.throws(() => encryptionKeyFingerprint(input), redacted);
      assert.throws(() => StreamEncryptionKey.from(input), redacted);
      await assert.rejects(
        createStream(unexpectedNetwork)({
          name: 'keyed', encryptionKey: input as unknown as StreamEncryptionKey
        }),
        redacted
      );
    });
  }

  it('accepts exactly 32 raw bytes and canonical base64 as the same key', () => {
    const raw = CREATE_STREAM.serialize({
      name: 'keyed', encryptionKey: StreamEncryptionKey.from(fromHex(key_hex))
    });
    const text = CREATE_STREAM.serialize({
      name: 'keyed', encryptionKey: StreamEncryptionKey.from(key_base64)
    });
    assert.deepEqual(raw, text);
    assert.ok(raw.subarray(raw.length - 32).equals(fromHex(key_hex)));
  });
});

describe('given resolved update reply when correlating should accept only matching request identity', () => {
  const receiptFor = (request: Buffer, operation: number): Buffer => {
    const receipt = Buffer.alloc(HEADER_SIZE);
    receipt.writeUInt32LE(HEADER_SIZE, REPLY_OFFSET.size);
    receipt[REPLY_OFFSET.command] = Command.Reply;
    receipt[REPLY_OFFSET.operation] = operation;
    receipt[REPLY_OFFSET.reserved] = COMMITTED_RESULT_MARKER;
    receipt.writeBigUInt64LE(1n, REPLY_OFFSET.op);
    receipt.writeBigUInt64LE(1n, REPLY_OFFSET.commit);
    receipt.writeBigUInt64LE(request.readBigUInt64LE(REQUEST_OFFSET.request), REPLY_OFFSET.request);
    request.copy(receipt, REPLY_OFFSET.client, REQUEST_OFFSET.client, REQUEST_OFFSET.client + 16);
    request.copy(receipt, REPLY_OFFSET.requestChecksum,
      REQUEST_OFFSET.requestChecksum, REQUEST_OFFSET.requestChecksum + 16);
    return receipt;
  };
  const refused = (error: unknown) =>
    error instanceof ResponseError && error.errorCode === 3 && error.commandCode === UPDATE_RESOURCE_CODE;

  for (const resolved of [
    Operation.UpdateStreamResolved, Operation.UpdateTopicResolved, Operation.UpdateUserResolved
  ]) {
    it(`accepts operation ${resolved} only for its own update request`, () => {
      const session = new VsrSession(7n);
      session.bind(42n);
      const vector = fixture.update_resource[0];
      const request = session.encode(UPDATE_RESOURCE_CODE, fromHex(vector.body_hex));
      assert.equal(request[REQUEST_OFFSET.operation], Operation.UpdateResource);
      const receipt = receiptFor(request, resolved);
      assert.doesNotThrow(() => validateReplyForRequest(receipt, request, UPDATE_RESOURCE_CODE));

      const otherClient = Buffer.from(receipt);
      otherClient[REPLY_OFFSET.client] ^= 1;
      const otherChecksum = Buffer.from(receipt);
      otherChecksum[REPLY_OFFSET.requestChecksum] ^= 1;
      const otherOperation = Buffer.from(receipt);
      otherOperation[REPLY_OFFSET.operation] = Operation.CreateTopicWithAssignments;
      for (const mismatch of [otherClient, otherChecksum, otherOperation])
        assert.throws(() => validateReplyForRequest(mismatch, request, UPDATE_RESOURCE_CODE), refused);

      const create = session.encode(COMMAND_CODE.CreateStream, Buffer.from([1, 0x61]));
      assert.throws(
        () => validateReplyForRequest(receiptFor(create, resolved), create, COMMAND_CODE.CreateStream),
        (error: unknown) => error instanceof ResponseError && error.errorCode === 3
      );
    });
  }
});
