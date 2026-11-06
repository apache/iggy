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

// The argument forms of conformance/README.md as SDK values. A value that the
// agent cannot read is a failure of the agent, never an answer of the SDK.

import { Failure } from './canonical.js';
import {
  CommitAfter, CommitPolicy, Consumer, DEFAULT_AUTO_COMMIT, HeaderKeyFactory, HeaderValue,
  MetadataChangeCursor, MetadataChangeStart, MetadataCurrentPageToken, OptionsScope, Partitioning,
  PollingStrategy, UserStatus, type CreateMessage, type Id, type UserPermissions
} from './iggy.js';

export type Args = Record<string, unknown>;

const U64_MAX = (1n << 64n) - 1n;

/** The arguments of a call. A field that the agent does not know fails, so a new argument is never ignored. */
export const fields = (args: unknown, known: readonly string[]): Args => {
  const object = record(args ?? {});
  const unknown = Object.keys(object).filter((key) => !known.includes(key));
  if (unknown.length > 0) throw new Failure(`unknown arguments: ${unknown.join(', ')}`);
  return object;
};

/** The value that `read` reads, or undefined for an absent or null argument. */
export const optional = <T>(value: unknown, read: (value: unknown) => T): T | undefined =>
  value === undefined || value === null ? undefined : read(value);

export const text = (value: unknown): string => {
  if (typeof value !== 'string') throw new Failure(`expected text: ${show(value)}`);
  return value;
};

export const texts = (value: unknown): string[] => list(value).map(text);

export const u32 = (value: unknown): number => {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0 || value > 0xffff_ffff)
    throw new Failure(`expected a u32: ${show(value)}`);
  return value;
};

export const u32s = (value: unknown): number[] => list(value).map(u32);

export const u64 = (value: unknown): bigint => {
  if ((typeof value !== 'number' || !Number.isInteger(value)) && typeof value !== 'bigint')
    throw new Failure(`expected a u64: ${show(value)}`);
  const number = BigInt(value);
  if (number < 0n || number > U64_MAX) throw new Failure(`expected a u64: ${show(value)}`);
  return number;
};

/** Milliseconds, as the SDK takes durations. */
export const millis = (value: unknown): number => Number(u64(value));

/** A number for an id or text for a name. */
export const identifier = (value: unknown): Id => {
  if (typeof value === 'string') return value;
  return u32(value);
};

/** Hex text, refused unless every character pair is a byte. */
export const hex = (value: unknown): Buffer => {
  const digits = text(value);
  if (!/^(?:[0-9a-fA-F]{2})*$/.test(digits)) throw new Failure(`expected hex: ${digits}`);
  return Buffer.from(digits, 'hex');
};

/** `first`, `last`, `next`, `{offset: N}` or `{timestamp: microseconds}`. */
export const strategy = (value: unknown): PollingStrategy => {
  const [key, argument] = keyed(value);
  switch (key) {
    case 'first': return PollingStrategy.First;
    case 'last': return PollingStrategy.Last;
    case 'next': return PollingStrategy.Next;
    case 'offset': return PollingStrategy.Offset(u64(argument));
    case 'timestamp': return PollingStrategy.Timestamp(u64(argument));
    default: throw new Failure(`unknown strategy ${key}`);
  }
};

/** `{consumer: id}` or `{group: id}`, and `{consumer: 0}` without one. */
export const consumer = (value: unknown): Consumer => {
  if (value === undefined || value === null) return { ...Consumer.Single, id: 0 };
  const [key, id] = keyed(value);
  switch (key) {
    case 'consumer': return { ...Consumer.Single, id: identifier(id) };
    case 'group': return Consumer.Group(identifier(id));
    default: throw new Failure(`unknown consumer kind ${key}`);
  }
};

/** `balanced`, `{partition_id: N}` or `{messages_key: text}`. */
export const partitioning = (value: unknown): Partitioning => {
  const [key, argument] = keyed(value);
  switch (key) {
    case 'balanced': return Partitioning.Balanced;
    case 'partition_id': return Partitioning.PartitionId(u32(argument));
    case 'messages_key': return Partitioning.MessageKey(text(argument));
    default: throw new Failure(`unknown partitioning ${key}`);
  }
};

/** `manual`, the default, `on_read` or `after_processing`. */
export const commit = (value: unknown): CommitPolicy => {
  switch (value ?? 'manual') {
    case 'manual': return CommitPolicy.Manual;
    case 'on_read': return CommitPolicy.OnRead;
    case 'after_processing': {
      const { interval, every } = DEFAULT_AUTO_COMMIT as Extract<CommitPolicy, { kind: 'auto' }>;
      return CommitPolicy.Auto({ interval, every, after: CommitAfter.Processing });
    }
    default: throw new Failure(`unknown commit policy ${show(value)}`);
  }
};

// The SDK reads u64 max as never expire or unlimited, and 0 as the server default.

/** `never`, `server_default` or microseconds. */
export const expiry = (value: unknown): bigint => {
  if (value === 'never') return U64_MAX;
  if (value === 'server_default') return 0n;
  return u64(value);
};

/** `unlimited`, `server_default` or bytes. */
export const topicSize = (value: unknown): bigint => {
  if (value === 'unlimited') return U64_MAX;
  if (value === 'server_default') return 0n;
  return u64(value);
};

export const userStatus = (value: unknown): UserStatus => {
  switch (value ?? 'active') {
    case 'active': return UserStatus.Active;
    case 'inactive': return UserStatus.Inactive;
    default: throw new Failure(`unknown user status ${show(value)}`);
  }
};

export const scope = (value: unknown): OptionsScope => {
  switch (value) {
    case 'stream': return OptionsScope.Stream;
    case 'topic': return OptionsScope.Topic;
    case 'user': return OptionsScope.User;
    default: throw new Failure(`unknown options scope ${show(value)}`);
  }
};

/** Global permissions, each flag false unless set. */
export const permissions = (value: unknown): UserPermissions => {
  const global = fields(fields(value, ['global']).global, [
    'manage_servers', 'read_servers', 'manage_users', 'read_users', 'manage_streams',
    'read_streams', 'manage_topics', 'read_topics', 'poll_messages', 'send_messages'
  ]);
  const flag = (name: string) => optional(global[name], boolean) ?? false;
  return {
    global: {
      ManageServers: flag('manage_servers'), ReadServers: flag('read_servers'),
      ManageUsers: flag('manage_users'), ReadUsers: flag('read_users'),
      ManageStreams: flag('manage_streams'), ReadStreams: flag('read_streams'),
      ManageTopics: flag('manage_topics'), ReadTopics: flag('read_topics'),
      PollMessages: flag('poll_messages'), SendMessages: flag('send_messages')
    },
    streams: []
  };
};

/** `earliest`, `latest`, `{after: cursor}` or `{current: page token or null}`. */
export const metadataStart = (value: unknown): MetadataChangeStart => {
  const [key, argument] = keyed(value);
  switch (key) {
    case 'earliest': return MetadataChangeStart.Earliest;
    case 'latest': return MetadataChangeStart.Latest;
    case 'after': return MetadataChangeStart.After(MetadataChangeCursor.parse(text(argument)));
    case 'current':
      return MetadataChangeStart.Current(optional(argument, (token) => MetadataCurrentPageToken.parse(text(token))));
    default: throw new Failure(`unknown metadata start ${key}`);
  }
};

/** A list of `{id, payload}` or `{id, payload_hex}`, each with optional `headers`. */
export const messages = (value: unknown): CreateMessage[] => list(value).map((item) => {
  const message = fields(item, ['id', 'payload', 'payload_hex', 'headers']);
  const id = optional(message.id, messageId);
  const headers = optional(message.headers, userHeaders);
  return {
    payload: payload(message.payload, message.payload_hex),
    ...(id === undefined ? {} : { id }),
    ...(headers === undefined ? {} : { headers })
  };
});

/** The u128 id as decimal text. */
const messageId = (value: unknown): bigint => integer(text(value), 128, false);

const payload = (utf8: unknown, hexText: unknown): Buffer => {
  if (utf8 !== undefined && hexText === undefined) return Buffer.from(text(utf8));
  if (utf8 === undefined && hexText !== undefined) return hex(hexText);
  throw new Failure('a message needs exactly one of payload and payload_hex');
};

const userHeaders = (value: unknown) => Object.entries(record(value)).map(([key, header]) => {
  const entry = fields(header, ['kind', 'value']);
  return { key: HeaderKeyFactory.String(key), value: headerValue(text(entry.kind), text(entry.value)) };
});

/** Reverses the reversible text of a header value. */
const headerValue = (kind: string, value: string): HeaderValue => {
  switch (kind) {
    case 'raw': return HeaderValue.Raw(base64(value));
    case 'string': return HeaderValue.String(value);
    case 'bool':
      if (value !== 'true' && value !== 'false') throw new Failure(`expected true or false: ${value}`);
      return HeaderValue.Bool(value === 'true');
    case 'int8': return HeaderValue.Int8(Number(integer(value, 8, true)));
    case 'int16': return HeaderValue.Int16(Number(integer(value, 16, true)));
    case 'int32': return HeaderValue.Int32(Number(integer(value, 32, true)));
    case 'int64': return HeaderValue.Int64(integer(value, 64, true));
    case 'int128': return HeaderValue.Int128(bytes128(integer(value, 128, true)));
    case 'uint8': return HeaderValue.Uint8(Number(integer(value, 8, false)));
    case 'uint16': return HeaderValue.Uint16(Number(integer(value, 16, false)));
    case 'uint32': return HeaderValue.Uint32(Number(integer(value, 32, false)));
    case 'uint64': return HeaderValue.Uint64(integer(value, 64, false));
    case 'uint128': return HeaderValue.Uint128(bytes128(integer(value, 128, false)));
    case 'float32': return HeaderValue.Float(floatBits(value, 32).readFloatLE());
    case 'float64': return HeaderValue.Double(floatBits(value, 64).readDoubleLE());
    default: throw new Failure(`unknown header kind ${kind}`);
  }
};

/** Base-10 text of an integer of `bits` bits. */
const integer = (value: string, bits: number, signed: boolean): bigint => {
  if (!/^-?[0-9]+$/.test(value)) throw new Failure(`expected an integer: ${value}`);
  const number = BigInt(value);
  const width = BigInt(bits);
  if (signed ? BigInt.asIntN(bits, number) !== number : number < 0n || number >> width !== 0n)
    throw new Failure(`${value} is out of range for ${signed ? 'int' : 'uint'}${bits}`);
  return number;
};

/** The 16 little-endian bytes of a 128-bit integer, as the SDK takes them. */
const bytes128 = (value: bigint): Buffer => {
  const unsigned = BigInt.asUintN(128, value);
  const bytes = Buffer.alloc(16);
  bytes.writeBigUInt64LE(unsigned & U64_MAX, 0);
  bytes.writeBigUInt64LE(unsigned >> 64n, 8);
  return bytes;
};

/** 0x and the IEEE-754 bits of a float in hex, as little-endian bytes. */
const floatBits = (value: string, bits: 32 | 64): Buffer => {
  if (!/^0x[0-9a-fA-F]+$/.test(value) || BigInt(value) >> BigInt(bits) !== 0n)
    throw new Failure(`expected 0x and ${bits} bits: ${value}`);
  const bytes = Buffer.alloc(bits / 8);
  if (bits === 32) bytes.writeUInt32LE(Number(BigInt(value)));
  else bytes.writeBigUInt64LE(BigInt(value));
  return bytes;
};

/** Padded standard base64, refused unless it encodes back to itself. */
const base64 = (value: string): Buffer => {
  const bytes = Buffer.from(value, 'base64');
  if (bytes.toString('base64') !== value) throw new Failure(`expected padded standard base64: ${value}`);
  return bytes;
};

const boolean = (value: unknown): boolean => {
  if (typeof value !== 'boolean') throw new Failure(`expected true or false: ${show(value)}`);
  return value;
};

const list = (value: unknown): unknown[] => {
  if (!Array.isArray(value)) throw new Failure(`expected a list: ${show(value)}`);
  return value;
};

const record = (value: unknown): Args => {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new Failure(`expected an object: ${show(value)}`);
  return value as Args;
};

/** A keyword such as `first`, or a one-key object such as `{offset: 3}`. */
const keyed = (value: unknown): [string, unknown] => {
  if (typeof value === 'string') return [value, undefined];
  const entries = value !== null && typeof value === 'object' && !Array.isArray(value) ? Object.entries(value) : [];
  if (entries.length !== 1) throw new Failure(`expected a keyword or a one-key object: ${show(value)}`);
  return entries[0];
};

const show = (value: unknown): string => JSON.stringify(value, (_key, item) =>
  typeof item === 'bigint' ? item.toString() : item) ?? String(value);
