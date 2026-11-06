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

// The canonical answers and result forms of core/conformance/src/canonical.rs.
// The orchestrator decodes each result into its form and refuses any field
// that the form does not have. Integers above 32 bits stay bigint, which the
// protocol loop writes as exact JSON numbers.

import { isUtf8 } from 'node:buffer';
import {
  FetchProgressUncertainError, LaneBlockedError, NotConnectedError, OperationNotRetainedError,
  OperationUncertainError, PARTITION_LANE_BLOCKED, ReadError, ResponseError,
  type ConsumerGroup, type ConsumerMessage, type Message, type OptionSpec, type ParsedOptions,
  type PolledBatch, type Stream, type Topic, type User, type UserPermissions
} from './iggy.js';

/** The answer to a request: exactly one of a result, an SDK error, an unsupported call and a failure. */
export type Answer =
  | { result: unknown }
  | { error: AgentError }
  | { unsupported: string }
  | { failure: string };

export type AgentError = {
  code: number,
  operation_id?: bigint,
  partition_id?: number,
  message?: string
};

/** A call that the SDK cannot make: a method, an argument value, a transport. */
export class Unsupported extends Error {}

/** A fault of the agent itself, never an answer of the SDK. */
export class Failure extends Error {}

// The IggyError codes of the SDK errors that carry no code of their own.
const NOT_CONNECTED = 61;
const OPERATION_UNCERTAIN = 14006;
const OPERATION_NOT_RETAINED = 14008;

/** The answer for what a call threw. An error without an Iggy code is a failure, not an answer of the SDK. */
export const answerFor = (thrown: unknown): Answer => {
  if (thrown instanceof Unsupported) return { unsupported: thrown.message };
  if (thrown instanceof Failure) return { failure: thrown.message };
  const error = sdkError(thrown);
  return error === undefined ? { failure: `an error without an Iggy code: ${describe(thrown)}` } : { error };
};

const sdkError = (thrown: unknown): AgentError | undefined => {
  if (thrown instanceof ReadError) {
    const error = sdkError(thrown.error);
    return error && { ...error, partition_id: error.partition_id ?? thrown.partitionId };
  }
  const message = thrown instanceof Error ? thrown.message : undefined;
  if (thrown instanceof ResponseError) return { code: thrown.errorCode, message };
  if (thrown instanceof LaneBlockedError) {
    const partition_id = thrown.errorCode === PARTITION_LANE_BLOCKED ? thrown.partitionId : undefined;
    return { code: thrown.errorCode, operation_id: thrown.operationId, partition_id, message };
  }
  if (thrown instanceof FetchProgressUncertainError)
    return { code: thrown.errorCode, operation_id: thrown.operationId, message };
  if (thrown instanceof OperationUncertainError)
    return { code: OPERATION_UNCERTAIN, operation_id: thrown.operationId, message };
  if (thrown instanceof OperationNotRetainedError)
    return { code: OPERATION_NOT_RETAINED, operation_id: thrown.operationId, message };
  if (thrown instanceof NotConnectedError) return { code: NOT_CONNECTED, message };
  return undefined;
};

const describe = (thrown: unknown): string => {
  if (!(thrown instanceof Error)) return String(thrown);
  const cause = thrown.cause === undefined ? '' : ` (cause: ${describe(thrown.cause)})`;
  return `${thrown.name}: ${thrown.message}${cause}`;
};

export const identityView = ({ userId }: { userId: number }) => ({ user_id: userId });

export const streamView = (stream: Stream) => ({
  id: stream.id,
  name: stream.name,
  created_at: micros(stream.createdAt),
  size_bytes: stream.sizeBytes,
  messages_count: stream.messagesCount,
  topics_count: stream.topicsCount,
  options: optionsView(stream.options, stream.derivedOptions)
});

// The SDK reads a stream without its topics and a group without its members.
// An empty count says that the list is empty. Any other list stays out, so
// the orchestrator reports it missing.
export const streamDetailsView = (stream: Stream) => ({
  ...streamView(stream),
  ...(stream.topicsCount === 0 ? { topics: [] } : {})
});

export const topicView = (topic: Topic) => ({
  id: topic.id,
  name: topic.name,
  created_at: micros(topic.createdAt),
  size_bytes: topic.sizeBytes,
  messages_count: topic.messagesCount,
  partitions_count: topic.partitionsCount,
  options: optionsView(topic.options, topic.derivedOptions)
});

export const topicDetailsView = (topic: Topic) => ({
  ...topicView(topic),
  partitions: topic.partitions.map((partition) => ({
    id: partition.id,
    created_at: micros(partition.createdAt),
    segments_count: partition.segmentsCount,
    current_offset: partition.currentOffset,
    size_bytes: partition.sizeBytes,
    messages_count: partition.messagesCount
  }))
});

export const groupView = (group: ConsumerGroup) => ({
  id: group.id,
  name: group.name,
  partitions_count: group.partitionsCount,
  members_count: group.membersCount,
  ...(group.membersCount === 0 ? { members: [] } : {})
});

export const polledView = (polled: PolledBatch) => ({
  partition_id: polled.partitionId,
  current_offset: polled.currentOffset,
  count: polled.count,
  messages: polled.messages.map(messageView)
});

export const receivedView = (received: ConsumerMessage) => ({
  partition_id: received.partitionId,
  message: messageView(received.message)
});

const messageView = (message: Message) => ({
  offset: message.headers.offset,
  id: String(message.headers.id),
  timestamp: micros(message.headers.timestamp),
  origin_timestamp: micros(message.headers.originTimestamp),
  checksum: message.headers.checksum,
  ...(isUtf8(message.payload) ? { payload: message.payload.toString() } : {}),
  payload_hex: message.payload.toString('hex'),
  ...(message.userHeaders.length > 0 ? { headers: headersView(message.userHeaders) } : {})
});

const headersView = (headers: Message['userHeaders']) => Object.fromEntries(headers.map(({ key, value }) =>
  [keyText(key.value), { kind: kindName(value.kind), value: valueText(value.kind, value.value) }]));

export const rawView = (reply: Buffer) => ({ payload_hex: reply.toString('hex') });

export const optionSpecView = (spec: OptionSpec) => ({
  key: spec.key,
  kind: kindName(spec.kind),
  updatable: spec.updatable,
  resettable: spec.resettable,
  secret: spec.secret,
  read_only: spec.readOnly,
  critical: spec.critical
});

export const userView = (user: User) => ({
  id: user.id,
  username: user.userName,
  status: user.status.toLowerCase(),
  created_at: micros(user.createdAt),
  permissions: user.permissions === null ? null : { global: globalView(user.permissions.global) },
  options: optionsView(user.options, user.derivedOptions)
});

export const globalView = (global: UserPermissions['global']) => ({
  manage_servers: global.ManageServers,
  read_servers: global.ReadServers,
  manage_users: global.ManageUsers,
  read_users: global.ReadUsers,
  manage_streams: global.ManageStreams,
  read_streams: global.ReadStreams,
  manage_topics: global.ManageTopics,
  read_topics: global.ReadTopics,
  poll_messages: global.PollMessages,
  send_messages: global.SendMessages
});

/** The options that the client set and those that the server derived, in one map with the flag of each. */
export const optionsView = (explicit: ParsedOptions, derived: ParsedOptions) => {
  const view: Record<string, { value: string, kind: string, explicit: boolean }> = {};
  for (const [block, isExplicit] of [[derived, false], [explicit, true]] as const)
    for (const [key, value] of Object.entries(block))
      view[key] = { value: optionText(value), kind: optionKind(value), explicit: isExplicit };
  return view;
};

type OptionValue = ParsedOptions[string];

// The SDK reads an option value without its wire kind. The server stores each
// key under the one kind of its catalog entry, and the fixture tests of the
// SDK map the JS type back to that kind.
const optionKind = (value: OptionValue): string => {
  if (typeof value === 'bigint') return 'uint64';
  if (typeof value === 'number') return 'uint32';
  if (typeof value === 'boolean') return 'bool';
  if (Buffer.isBuffer(value)) return 'raw';
  return 'string';
};

const optionText = (value: OptionValue): string => Buffer.isBuffer(value) ? value.toString('base64') : String(value);

/** The canonical names of the header kinds, at their wire code minus one. */
const KIND_NAMES = [
  'raw', 'string', 'bool', 'int8', 'int16', 'int32', 'int64', 'int128',
  'uint8', 'uint16', 'uint32', 'uint64', 'uint128', 'float32', 'float64'
] as const;

const kindName = (code: number): string => {
  const name = KIND_NAMES[code - 1];
  if (name === undefined) throw new Failure(`unknown header kind ${code}`);
  return name;
};

/**
 * The reversible text of a value that the SDK decoded under `kind`: UTF-8 for
 * strings, true or false, base-10 integers, padded standard base64 for raw
 * bytes, and 0x with the IEEE-754 bits in fixed-width lowercase hex for floats.
 */
const valueText = (kind: number, value: OptionValue): string => {
  switch (kindName(kind)) {
    case 'raw':
      return (value as Buffer).toString('base64');
    case 'int128':
      return BigInt.asIntN(128, unsigned128(value as Buffer)).toString();
    case 'uint128':
      return unsigned128(value as Buffer).toString();
    case 'float32': {
      const bits = Buffer.alloc(4);
      bits.writeFloatLE(value as number);
      return `0x${bits.readUInt32LE().toString(16).padStart(8, '0')}`;
    }
    case 'float64': {
      const bits = Buffer.alloc(8);
      bits.writeDoubleLE(value as number);
      return `0x${bits.readBigUInt64LE().toString(16).padStart(16, '0')}`;
    }
    default:
      return String(value);
  }
};

const unsigned128 = (bytes: Buffer): bigint => (bytes.readBigUInt64LE(8) << 64n) | bytes.readBigUInt64LE(0);

/** A header key as the text of its bytes, as the Rust form keys the map. */
const keyText = (value: OptionValue): string => {
  if (typeof value === 'string') return value;
  if (Buffer.isBuffer(value)) return value.toString();
  throw new Failure(`a header key that is neither text nor bytes: ${String(value)}`);
};

/** The SDK reads a timestamp as a Date, which keeps milliseconds. */
const micros = (date: Date): bigint => BigInt(date.getTime()) * 1000n;
