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

import { isUtf8 } from 'node:buffer';
import { COMMAND_CODE } from '../command.code.js';
import { DeserializeError, responseError } from '../error.utils.js';
import { deserializeOptions, type ParsedOptions } from '../options.utils.js';
import { UserStatus } from '../user/user.utils.js';
import type { StreamPerms, TopicPerms, UserPermissions } from '../user/permissions.utils.js';

/** Text prefix of a change cursor. */
export const METADATA_CHANGE_CURSOR_PREFIX = 'mc1_';

/** Text prefix of a current state page token. */
export const METADATA_CURRENT_PAGE_TOKEN_PREFIX = 'cs1_';

/** Encoded size of a change cursor. */
export const METADATA_CHANGE_CURSOR_SIZE = 44;

const CURSOR_TEXT_LENGTH = METADATA_CHANGE_CURSOR_PREFIX.length + 59;
/** A user id and a personal access token name. */
const MAX_PAGE_TOKEN_KEY_SIZE = 4 + 1 + 255;
const PAGE_TOKEN_FIXED_SIZE = METADATA_CHANGE_CURSOR_SIZE + 8 + 1 + 1 + 2;
const MAX_PAGE_TOKEN_TEXT_LENGTH = METADATA_CURRENT_PAGE_TOKEN_PREFIX.length +
  Math.ceil((PAGE_TOKEN_FIXED_SIZE + MAX_PAGE_TOKEN_KEY_SIZE) / 3) * 4;
const INVALID_COMMAND = 3;
const NON_CANONICAL_CURSOR = 'a cursor at op 0 carries no checksum and no ordinal';
const U32_MAX = 0xffff_ffff;
const U64_MASK = 0xffff_ffff_ffff_ffffn;
const U128_MAX = (1n << 128n) - 1n;

/** Family of change records and the unit of filtering. */
export const MetadataChangeGroup = {
  Streams: 'streams',
  Topics: 'topics',
  Partitions: 'partitions',
  ConsumerGroups: 'consumer_groups',
  ConsumerGroupMembers: 'consumer_group_members',
  Users: 'users',
  PersonalAccessTokens: 'personal_access_tokens',
  Sessions: 'sessions'
} as const;

export type MetadataChangeGroup = typeof MetadataChangeGroup[keyof typeof MetadataChangeGroup];

/** Kind of a change record. A new body layout is a new kind. */
export const MetadataChangeKind = {
  StreamCreated: 'stream_created',
  StreamUpdated: 'stream_updated',
  StreamDeleted: 'stream_deleted',
  TopicCreated: 'topic_created',
  TopicUpdated: 'topic_updated',
  TopicDeleted: 'topic_deleted',
  PartitionsChanged: 'partitions_changed',
  ConsumerGroupCreated: 'consumer_group_created',
  ConsumerGroupDeleted: 'consumer_group_deleted',
  ConsumerGroupMemberJoined: 'consumer_group_member_joined',
  ConsumerGroupMemberLeft: 'consumer_group_member_left',
  ConsumerGroupPartitionHandedOver: 'consumer_group_partition_handed_over',
  UserCreated: 'user_created',
  UserUpdated: 'user_updated',
  UserDeleted: 'user_deleted',
  UserPasswordChanged: 'user_password_changed',
  UserPermissionsUpdated: 'user_permissions_updated',
  PersonalAccessTokenCreated: 'personal_access_token_created',
  PersonalAccessTokenDeleted: 'personal_access_token_deleted',
  SessionRegistered: 'session_registered',
  SessionClosed: 'session_closed'
} as const;

export type MetadataChangeKind = typeof MetadataChangeKind[keyof typeof MetadataChangeKind];

/** Outcome of a change log query. */
export const MetadataChangeStatus = {
  /** Records or items after the requested position */
  Ok: 'ok',
  /** The node has not applied the requested position yet: ask again later or ask another node */
  Behind: 'behind',
  /** The node no longer holds every record after the position: rebuild from current state */
  Expired: 'expired',
  /** The position belongs to another history: rebuild from current state */
  Reset: 'reset'
} as const;

export type MetadataChangeStatus = typeof MetadataChangeStatus[keyof typeof MetadataChangeStatus];

/** Whether a page holds change records or current state items. */
export const MetadataChangePhase = {
  Changes: 'changes',
  Current: 'current'
} as const;

export type MetadataChangePhase = typeof MetadataChangePhase[keyof typeof MetadataChangePhase];

/** Why a consumer group member was removed. */
export const MetadataMemberLeftReason = {
  Left: 'left',
  SessionEnded: 'session_ended'
} as const;

export type MetadataMemberLeftReason =
  typeof MetadataMemberLeftReason[keyof typeof MetadataMemberLeftReason];

/** Why a personal access token was removed. */
export const MetadataTokenDeletedReason = {
  Deleted: 'deleted',
  Expired: 'expired'
} as const;

export type MetadataTokenDeletedReason =
  typeof MetadataTokenDeletedReason[keyof typeof MetadataTokenDeletedReason];

/** Why a session was closed. */
export const MetadataSessionClosedReason = {
  Logout: 'logout',
  Expired: 'expired',
  /** The user was deactivated or deleted */
  Revoked: 'revoked'
} as const;

export type MetadataSessionClosedReason =
  typeof MetadataSessionClosedReason[keyof typeof MetadataSessionClosedReason];

/** Why a user's public permission set changed. */
export const MetadataPermissionsUpdatedReason = {
  /** An `updatePermissions` replaced the set */
  Updated: 'updated',
  /** A stream or topic delete pruned the user's grants on it */
  ResourceDeleted: 'resource_deleted'
} as const;

export type MetadataPermissionsUpdatedReason =
  typeof MetadataPermissionsUpdatedReason[keyof typeof MetadataPermissionsUpdatedReason];

/** Wire code of every group, in code order. */
export const METADATA_CHANGE_GROUP_CODE: Readonly<Record<MetadataChangeGroup, number>> = {
  streams: 1,
  topics: 2,
  partitions: 3,
  consumer_groups: 4,
  consumer_group_members: 5,
  users: 6,
  personal_access_tokens: 7,
  sessions: 8
};

/** Wire code and group of every kind. */
export const METADATA_CHANGE_KIND_CODE: Readonly<Record<MetadataChangeKind, number>> = {
  stream_created: 1,
  stream_updated: 2,
  stream_deleted: 3,
  topic_created: 10,
  topic_updated: 11,
  topic_deleted: 12,
  partitions_changed: 20,
  consumer_group_created: 30,
  consumer_group_deleted: 31,
  consumer_group_member_joined: 40,
  consumer_group_member_left: 41,
  consumer_group_partition_handed_over: 42,
  user_created: 50,
  user_updated: 51,
  user_deleted: 52,
  user_password_changed: 53,
  user_permissions_updated: 54,
  personal_access_token_created: 60,
  personal_access_token_deleted: 61,
  session_registered: 70,
  session_closed: 71
};

const KIND_GROUP: Readonly<Record<MetadataChangeKind, MetadataChangeGroup>> = {
  stream_created: 'streams',
  stream_updated: 'streams',
  stream_deleted: 'streams',
  topic_created: 'topics',
  topic_updated: 'topics',
  topic_deleted: 'topics',
  partitions_changed: 'partitions',
  consumer_group_created: 'consumer_groups',
  consumer_group_deleted: 'consumer_groups',
  consumer_group_member_joined: 'consumer_group_members',
  consumer_group_member_left: 'consumer_group_members',
  consumer_group_partition_handed_over: 'consumer_group_members',
  user_created: 'users',
  user_updated: 'users',
  user_deleted: 'users',
  user_password_changed: 'users',
  user_permissions_updated: 'users',
  personal_access_token_created: 'personal_access_tokens',
  personal_access_token_deleted: 'personal_access_tokens',
  session_registered: 'sessions',
  session_closed: 'sessions'
};

/** Stream identity: ids are reused after a delete, the creating op is not. */
export type MetadataStreamKey = {
  streamId: number,
  streamCreatedOp: bigint
};

/** Topic identity: the stream key plus the topic id and creating op. */
export type MetadataTopicKey = MetadataStreamKey & {
  topicId: number,
  topicCreatedOp: bigint
};

/** User identity, the one sessions and tokens bind to. */
export type MetadataUserKey = {
  userId: number,
  userCreatedAt: bigint
};

/**
 * Public options of a resource after a change, as the read commands return
 * them, or a marker for stored critical state this build cannot read. A
 * mirror keeps its previous values for an unreadable resource.
 */
export type MetadataOptions =
  | { unreadable: false, options: ParsedOptions, derivedOptions: ParsedOptions }
  | { unreadable: true };

/** A stream after a create or update, and a `streams` item. */
export type MetadataStreamChange = MetadataStreamKey & {
  createdAt: bigint,
  name: string,
  options: MetadataOptions
};

/** A removed stream. `requestedOp` is the op of a deferred delete's request. */
export type MetadataStreamDeleted = MetadataStreamKey & {
  createdAt: bigint,
  name: string,
  requestedOp: bigint | null
};

/** A topic after a create or update. */
export type MetadataTopicChange = MetadataTopicKey & {
  createdAt: bigint,
  name: string,
  partitionsCount: number,
  options: MetadataOptions
};

/** A `topics` item. Partition counts travel as `partitions` items. */
export type MetadataTopicItem = MetadataTopicKey & {
  createdAt: bigint,
  name: string,
  options: MetadataOptions
};

/** A removed topic. */
export type MetadataTopicDeleted = MetadataTopicKey & {
  createdAt: bigint,
  name: string,
  requestedOp: bigint | null
};

/** A partition count change. Removed ids are ascending, empty on create. */
export type MetadataPartitionsChanged = MetadataTopicKey & {
  partitionsCount: number,
  removedPartitionIds: number[],
  requestedOp: bigint | null
};

/** A `partitions` item. */
export type MetadataPartitionsItem = MetadataTopicKey & {
  partitionsCount: number
};

/** A created consumer group with the topic's resulting group count. */
export type MetadataConsumerGroupCreated = MetadataTopicKey & {
  groupId: number,
  name: string,
  consumerGroupsCount: number
};

/** A removed consumer group with the topic's resulting group count. */
export type MetadataConsumerGroupDeleted = MetadataConsumerGroupCreated & {
  requestedOp: bigint | null
};

/** A `consumer_groups` item. */
export type MetadataConsumerGroupItem = MetadataTopicKey & {
  groupId: number,
  name: string
};

/** A member that joined, with its installed partitions. */
export type MetadataMemberJoined = MetadataTopicKey & {
  groupId: number,
  memberId: number,
  clientId: bigint,
  membersCount: number,
  partitionIds: number[]
};

/** A member that left the group. */
export type MetadataMemberLeft = MetadataTopicKey & {
  groupId: number,
  memberId: number,
  clientId: bigint,
  membersCount: number,
  reason: MetadataMemberLeftReason
};

/** A completed partition handover. `memberId` is null when the last member left. */
export type MetadataPartitionHandedOver = MetadataTopicKey & {
  groupId: number,
  partitionId: number,
  memberId: number | null
};

/** A `consumer_group_members` item. */
export type MetadataMemberItem = MetadataTopicKey & {
  groupId: number,
  memberId: number,
  clientId: bigint,
  partitionIds: number[]
};

/** A user after a create or update, and a `users` item. */
export type MetadataUserChange = MetadataUserKey & {
  username: string,
  status: UserStatus,
  permissions: UserPermissions | null,
  options: MetadataOptions
};

/** A deleted user, or one whose password changed. */
export type MetadataUserIdentity = MetadataUserKey & {
  username: string
};

/** A user's public permission set after a change. */
export type MetadataUserPermissionsUpdated = MetadataUserKey & {
  username: string,
  permissions: UserPermissions | null,
  reason: MetadataPermissionsUpdatedReason
};

/** A created personal access token, and an item. `expiry` is null for a token that never expires. */
export type MetadataTokenChange = MetadataUserKey & {
  name: string,
  expiry: bigint | null
};

/** A removed personal access token. */
export type MetadataTokenDeleted = MetadataUserKey & {
  name: string,
  reason: MetadataTokenDeletedReason
};

/** A registered session, and a `sessions` item. */
export type MetadataSession = MetadataUserKey & {
  clientId: bigint,
  session: bigint
};

/** A closed session. */
export type MetadataSessionClosed = MetadataSession & {
  reason: MetadataSessionClosedReason
};

/** The typed body of a change record, narrowed by `kind`. */
export type MetadataChange =
  | { kind: 'stream_created' | 'stream_updated', body: MetadataStreamChange }
  | { kind: 'stream_deleted', body: MetadataStreamDeleted }
  | { kind: 'topic_created' | 'topic_updated', body: MetadataTopicChange }
  | { kind: 'topic_deleted', body: MetadataTopicDeleted }
  | { kind: 'partitions_changed', body: MetadataPartitionsChanged }
  | { kind: 'consumer_group_created', body: MetadataConsumerGroupCreated }
  | { kind: 'consumer_group_deleted', body: MetadataConsumerGroupDeleted }
  | { kind: 'consumer_group_member_joined', body: MetadataMemberJoined }
  | { kind: 'consumer_group_member_left', body: MetadataMemberLeft }
  | { kind: 'consumer_group_partition_handed_over', body: MetadataPartitionHandedOver }
  | { kind: 'user_created' | 'user_updated', body: MetadataUserChange }
  | { kind: 'user_deleted' | 'user_password_changed', body: MetadataUserIdentity }
  | { kind: 'user_permissions_updated', body: MetadataUserPermissionsUpdated }
  | { kind: 'personal_access_token_created', body: MetadataTokenChange }
  | { kind: 'personal_access_token_deleted', body: MetadataTokenDeleted }
  | { kind: 'session_registered', body: MetadataSession }
  | { kind: 'session_closed', body: MetadataSessionClosed }
  /** A group or kind this build does not know, with its raw body */
  | { kind: 'unknown', body: Buffer };

/**
 * One public result of a committed metadata operation. `groupCode` and
 * `kindCode` are the wire codes, kept for a record this build does not know.
 */
export type MetadataChangeRecord = MetadataChange & {
  op: bigint,
  ordinal: number,
  /** Prepare checksum of `op` */
  checksum: bigint,
  /** Prepare timestamp of `op` in microseconds */
  timestamp: bigint,
  /** The acting user, null for a server action */
  actorUserId: number | null,
  group: MetadataChangeGroup | 'unknown',
  groupCode: number,
  kindCode: number
};

/** One current state item, narrowed by `group`. `groupCode` is the wire code. */
export type MetadataCurrentItem = { groupCode: number } & (
  | { group: 'streams', body: MetadataStreamChange }
  | { group: 'topics', body: MetadataTopicItem }
  | { group: 'partitions', body: MetadataPartitionsItem }
  | { group: 'consumer_groups', body: MetadataConsumerGroupItem }
  | { group: 'consumer_group_members', body: MetadataMemberItem }
  | { group: 'users', body: MetadataUserChange }
  | { group: 'personal_access_tokens', body: MetadataTokenChange }
  | { group: 'sessions', body: MetadataSession }
  /** A group this build does not know, with its raw body */
  | { group: 'unknown', body: Buffer }
);

const GROUP_BY_CODE = new Map<number, MetadataChangeGroup>(
  Object.entries(METADATA_CHANGE_GROUP_CODE).map(([name, code]) => [code, name as MetadataChangeGroup])
);

const KIND_BY_CODE = new Map<number, MetadataChangeKind>(
  Object.entries(METADATA_CHANGE_KIND_CODE).map(([name, code]) => [code, name as MetadataChangeKind])
);

/** Groups a current state scan of a group must include, so cascades of their deletes apply. */
const GROUP_PARENTS: Readonly<Record<MetadataChangeGroup, readonly MetadataChangeGroup[]>> = {
  streams: [],
  topics: ['streams'],
  partitions: ['topics', 'streams'],
  consumer_groups: ['topics', 'streams'],
  consumer_group_members: ['consumer_groups', 'partitions', 'topics', 'streams'],
  users: [],
  personal_access_tokens: ['users'],
  sessions: ['users']
};

const STATUS_NAMES: readonly MetadataChangeStatus[] = ['ok', 'behind', 'expired', 'reset'];
const PHASE_NAMES: readonly MetadataChangePhase[] = ['changes', 'current'];
const MEMBER_LEFT_REASONS: readonly MetadataMemberLeftReason[] = ['left', 'session_ended'];
const TOKEN_DELETED_REASONS: readonly MetadataTokenDeletedReason[] = ['deleted', 'expired'];
const SESSION_CLOSED_REASONS: readonly MetadataSessionClosedReason[] = ['logout', 'expired', 'revoked'];
const PERMISSIONS_UPDATED_REASONS: readonly MetadataPermissionsUpdatedReason[] =
  ['updated', 'resource_deleted'];

/**
 * Position of the last change record a caller consumed or scanned. Op 0 with
 * a zero checksum and ordinal names the beginning of the history `logId`
 * identifies.
 *
 * Its text form, `toString()` and `toJSON()`, is `mc1_` followed by unpadded
 * Base64url of the 44 encoded bytes.
 */
export class MetadataChangeCursor {
  /** Prepare checksum of op 1, the root of this history */
  readonly logId: bigint;
  readonly op: bigint;
  /** Prepare checksum of `op` */
  readonly checksum: bigint;
  /** Position of the record within `op` */
  readonly ordinal: number;

  /**
   * @throws ResponseError with code 3 (invalid command) for a value outside its
   * unsigned width or an op 0 that carries a checksum or an ordinal
   */
  constructor(logId: bigint, op: bigint, checksum: bigint, ordinal: number) {
    const inRange = isUnsigned(logId, U128_MAX) && isUnsigned(op, U64_MASK) &&
      isUnsigned(checksum, U128_MAX) && Number.isInteger(ordinal) && ordinal >= 0 && ordinal <= U32_MAX;
    if (!inRange || (op === 0n && (checksum !== 0n || ordinal !== 0)))
      throw invalidCommand('a change cursor holds unsigned values, and op 0 carries no checksum or ordinal');
    this.logId = logId;
    this.op = op;
    this.checksum = checksum;
    this.ordinal = ordinal;
  }

  /**
   * Reads the text form. Anything but the prefix, the exact length and
   * canonical unpadded Base64url is refused.
   *
   * @throws ResponseError with code 3 (invalid command)
   */
  static parse(text: string): MetadataChangeCursor {
    if (text.length !== CURSOR_TEXT_LENGTH)
      throw invalidCommand('a change cursor is 63 characters');
    const cursor = decodeCursor(decodeCanonicalBase64(text, METADATA_CHANGE_CURSOR_PREFIX));
    if (cursor === undefined)
      throw invalidCommand(NON_CANONICAL_CURSOR);
    return cursor;
  }

  /** The 44 wire bytes: `[log_id:u128][op:u64][checksum:u128][ordinal:u32]`, little-endian. */
  toBytes(): Buffer {
    const bytes = Buffer.alloc(METADATA_CHANGE_CURSOR_SIZE);
    writeUnsigned128(bytes, 0, this.logId);
    bytes.writeBigUInt64LE(this.op, 16);
    writeUnsigned128(bytes, 24, this.checksum);
    bytes.writeUInt32LE(this.ordinal, 40);
    return bytes;
  }

  toString(): string {
    return METADATA_CHANGE_CURSOR_PREFIX + this.toBytes().toString('base64url');
  }

  toJSON(): string {
    return this.toString();
  }
}

/**
 * Continuation of a current state scan. Send it back unchanged with the same
 * groups. Its text form is `cs1_` followed by unpadded Base64url of the
 * token payload.
 */
export class MetadataCurrentPageToken {
  readonly #payload: Buffer;

  private constructor(payload: Buffer) {
    this.#payload = payload;
  }

  /**
   * Reads the text form. Anything but the prefix, canonical unpadded Base64url
   * and a token that keeps its own invariants is refused.
   *
   * @throws ResponseError with code 3 (invalid command)
   */
  static parse(text: string): MetadataCurrentPageToken {
    if (text.length > MAX_PAGE_TOKEN_TEXT_LENGTH)
      throw invalidCommand('the page token is too long');
    const payload = decodeCanonicalBase64(text, METADATA_CURRENT_PAGE_TOKEN_PREFIX);
    const refusal = pageTokenRefusal(payload);
    if (refusal !== undefined)
      throw invalidCommand(refusal);
    return new MetadataCurrentPageToken(payload);
  }

  /**
   * Reads the token payload a response or request carries, refusing one that
   * breaks its own invariants.
   *
   * @throws DeserializeError
   */
  static fromBytes(payload: Buffer): MetadataCurrentPageToken {
    const refusal = pageTokenRefusal(payload);
    if (refusal !== undefined)
      throw new DeserializeError(`cannot deserialize metadata changes: ${refusal}`);
    return new MetadataCurrentPageToken(Buffer.from(payload));
  }

  /** The cursor captured before the scan's first page */
  get anchor(): MetadataChangeCursor {
    return decodeCursor(this.#payload)!;
  }

  /** The highest head any page of the scan reported so far */
  get requiredHead(): bigint {
    return this.#payload.readBigUInt64LE(METADATA_CHANGE_CURSOR_SIZE);
  }

  /** The groups the scan was started with, in code order */
  get groups(): MetadataChangeGroup[] {
    return groupsOfMask(this.#payload[METADATA_CHANGE_CURSOR_SIZE + 8]);
  }

  /** The group of the last item returned */
  get lastGroup(): MetadataChangeGroup {
    return GROUP_BY_CODE.get(this.#payload[METADATA_CHANGE_CURSOR_SIZE + 9])!;
  }

  /** A copy of the last item's key, in the page token key layout of its group */
  get lastKey(): Buffer {
    return Buffer.from(this.#payload.subarray(PAGE_TOKEN_FIXED_SIZE));
  }

  /** A copy of the token payload */
  toBytes(): Buffer {
    return Buffer.from(this.#payload);
  }

  toString(): string {
    return METADATA_CURRENT_PAGE_TOKEN_PREFIX + this.#payload.toString('base64url');
  }

  toJSON(): string {
    return this.toString();
  }
}

/**
 * Sequential bounds-checked reader over a response or one body. Every
 * refusal is a `DeserializeError`.
 */
export class ChangeReader {
  readonly #data: Buffer;
  #position: number;

  constructor(data: Buffer, position = 0) {
    this.#data = data;
    this.#position = position;
  }

  get position(): number {
    return this.#position;
  }

  get remaining(): number {
    return this.#data.length - this.#position;
  }

  take(count: number): Buffer {
    if (count > this.remaining)
      this.fail(`need ${count} bytes at offset ${this.#position}, have ${this.remaining}`);
    const bytes = this.#data.subarray(this.#position, this.#position + count);
    this.#position += count;
    return bytes;
  }

  u8(): number {
    return this.take(1)[0];
  }

  u16(): number {
    return this.take(2).readUInt16LE(0);
  }

  u32(): number {
    return this.take(4).readUInt32LE(0);
  }

  u64(): bigint {
    return this.take(8).readBigUInt64LE(0);
  }

  u128(): bigint {
    return readUnsigned128(this.take(16), 0);
  }

  /** A presence or boolean byte, refusing anything but 0 and 1. */
  flag(): boolean {
    const offset = this.#position;
    const value = this.u8();
    if (value > 1)
      this.fail(`flag byte ${value} at offset ${offset}`);
    return value === 1;
  }

  /** A grant byte, read the way the server's permission decoder reads it. */
  grant(): boolean {
    return this.u8() !== 0;
  }

  /** A code at or below `names.length - 1`, mapped to its name. */
  code<T>(names: readonly T[], field: string): T {
    const offset = this.#position;
    const code = this.u8();
    if (code >= names.length)
      this.fail(`unknown ${field} ${code} at offset ${offset}`);
    return names[code];
  }

  optionalU64(): bigint | null {
    return this.flag() ? this.u64() : null;
  }

  optionalU32(): number | null {
    return this.flag() ? this.u32() : null;
  }

  /** A 1 to 255 byte UTF-8 name behind its u8 length. */
  name(): string {
    const offset = this.#position;
    const length = this.u8();
    if (length === 0)
      this.fail(`empty name at offset ${offset}`);
    const bytes = this.take(length);
    if (!isUtf8(bytes))
      this.fail(`name at offset ${offset} is not UTF-8`);
    return bytes.toString('utf8');
  }

  /** `[count:u32][id:u32]*`, refusing ids that are not strictly ascending. */
  partitionIds(): number[] {
    const count = this.u32();
    const ids: number[] = [];
    for (let index = 0; index < count; index++) {
      const id = this.u32();
      if (ids.length > 0 && ids[ids.length - 1] >= id)
        this.fail(`partition ids must be strictly ascending at offset ${this.#position - 4}`);
      ids.push(id);
    }
    return ids;
  }

  /** Refuses bytes left after a body. */
  finish(): void {
    if (this.remaining !== 0)
      this.fail(`${this.remaining} trailing bytes after a change body`);
  }

  fail(detail: string): never {
    throw new DeserializeError(`cannot deserialize metadata changes: ${detail}`);
  }
}

/** The snake_case name of a group code, `unknown` for a code this build does not know. */
export const metadataChangeGroupName = (code: number): MetadataChangeGroup | 'unknown' =>
  GROUP_BY_CODE.get(code) ?? 'unknown';

/** The snake_case name of a kind code, `unknown` for a code this build does not know. */
export const metadataChangeKindName = (code: number): MetadataChangeKind | 'unknown' =>
  KIND_BY_CODE.get(code) ?? 'unknown';

/** The group every record of a known kind belongs to. */
export const metadataChangeKindGroup = (kind: MetadataChangeKind): MetadataChangeGroup =>
  KIND_GROUP[kind];

/**
 * The group mask a selection names: one bit per group code, an empty
 * selection meaning every group, `undefined` for a name this build does not
 * know.
 */
export const metadataChangeGroupsMask = (groups: readonly string[]): number | undefined => {
  if (groups.length === 0)
    return 0xff;
  let mask = 0;
  for (const group of groups) {
    const code = METADATA_CHANGE_GROUP_CODE[group as MetadataChangeGroup];
    if (code === undefined)
      return undefined;
    mask |= 1 << (code - 1);
  }
  return mask;
};

/** Whether a current state scan of the mask misses a parent group. */
export const missesParentGroup = (mask: number): boolean =>
  groupsOfMask(mask).some((group) =>
    GROUP_PARENTS[group].some((parent) => (mask & (1 << (METADATA_CHANGE_GROUP_CODE[parent] - 1))) === 0));

/**
 * Decodes a record body by its codes. A group or kind this build does not
 * know keeps the raw body and a known group name, a known kind under another
 * group is refused.
 */
export const decodeMetadataChange = (
  groupCode: number, kindCode: number, body: Buffer
): MetadataChange & { group: MetadataChangeGroup | 'unknown' } => {
  const group = GROUP_BY_CODE.get(groupCode);
  const kind = KIND_BY_CODE.get(kindCode);
  if (group === undefined || kind === undefined)
    return { kind: 'unknown', group: group ?? 'unknown', body: Buffer.from(body) };
  const reader = new ChangeReader(body);
  if (metadataChangeKindGroup(kind) !== group)
    reader.fail(`change kind ${kindCode} does not belong to group ${groupCode}`);
  const change = readChange(reader, kind);
  reader.finish();
  return { ...change, group };
};

/** Decodes an item body by its group code. A group this build does not know keeps the raw body. */
export const decodeMetadataCurrentItem = (groupCode: number, body: Buffer): MetadataCurrentItem => {
  const reader = new ChangeReader(body);
  let item: MetadataCurrentItem;
  switch (GROUP_BY_CODE.get(groupCode)) {
    case 'streams':
      item = { groupCode, group: 'streams', body: readStreamChange(reader) };
      break;
    case 'topics':
      item = {
        groupCode, group: 'topics',
        body: {
          ...readTopicKey(reader), createdAt: reader.u64(), name: reader.name(),
          options: readOptions(reader)
        }
      };
      break;
    case 'partitions':
      item = {
        groupCode, group: 'partitions',
        body: { ...readTopicKey(reader), partitionsCount: reader.u32() }
      };
      break;
    case 'consumer_groups':
      item = {
        groupCode, group: 'consumer_groups',
        body: { ...readTopicKey(reader), groupId: reader.u32(), name: reader.name() }
      };
      break;
    case 'consumer_group_members':
      item = {
        groupCode, group: 'consumer_group_members',
        body: {
          ...readTopicKey(reader), groupId: reader.u32(), memberId: reader.u32(),
          clientId: reader.u128(), partitionIds: reader.partitionIds()
        }
      };
      break;
    case 'users':
      item = { groupCode, group: 'users', body: readUserChange(reader) };
      break;
    case 'personal_access_tokens':
      item = { groupCode, group: 'personal_access_tokens', body: readTokenChange(reader) };
      break;
    case 'sessions':
      item = { groupCode, group: 'sessions', body: readSession(reader) };
      break;
    default:
      return { groupCode, group: 'unknown', body: Buffer.from(body) };
  }
  reader.finish();
  return item;
};

/** Reads a 44-byte cursor, refusing an op 0 that carries a checksum or an ordinal. */
export const readMetadataChangeCursor = (reader: ChangeReader): MetadataChangeCursor =>
  decodeCursor(reader.take(METADATA_CHANGE_CURSOR_SIZE)) ?? reader.fail(NON_CANONICAL_CURSOR);

export const metadataChangeStatus = (reader: ChangeReader): MetadataChangeStatus =>
  reader.code(STATUS_NAMES, 'status');

export const metadataChangePhase = (reader: ChangeReader): MetadataChangePhase =>
  reader.code(PHASE_NAMES, 'phase');

const readChange = (reader: ChangeReader, kind: MetadataChangeKind): MetadataChange => {
  switch (kind) {
    case 'stream_created':
    case 'stream_updated':
      return { kind, body: readStreamChange(reader) };
    case 'stream_deleted':
      return {
        kind,
        body: {
          ...readStreamKey(reader), createdAt: reader.u64(), name: reader.name(),
          requestedOp: reader.optionalU64()
        }
      };
    case 'topic_created':
    case 'topic_updated':
      return {
        kind,
        body: {
          ...readTopicKey(reader), createdAt: reader.u64(), name: reader.name(),
          partitionsCount: reader.u32(), options: readOptions(reader)
        }
      };
    case 'topic_deleted':
      return {
        kind,
        body: {
          ...readTopicKey(reader), createdAt: reader.u64(), name: reader.name(),
          requestedOp: reader.optionalU64()
        }
      };
    case 'partitions_changed':
      return {
        kind,
        body: {
          ...readTopicKey(reader), partitionsCount: reader.u32(),
          removedPartitionIds: reader.partitionIds(), requestedOp: reader.optionalU64()
        }
      };
    case 'consumer_group_created':
      return {
        kind,
        body: {
          ...readTopicKey(reader), groupId: reader.u32(), name: reader.name(),
          consumerGroupsCount: reader.u32()
        }
      };
    case 'consumer_group_deleted':
      return {
        kind,
        body: {
          ...readTopicKey(reader), groupId: reader.u32(), name: reader.name(),
          consumerGroupsCount: reader.u32(), requestedOp: reader.optionalU64()
        }
      };
    case 'consumer_group_member_joined':
      return {
        kind,
        body: {
          ...readTopicKey(reader), groupId: reader.u32(), memberId: reader.u32(),
          clientId: reader.u128(), membersCount: reader.u32(), partitionIds: reader.partitionIds()
        }
      };
    case 'consumer_group_member_left':
      return {
        kind,
        body: {
          ...readTopicKey(reader), groupId: reader.u32(), memberId: reader.u32(),
          clientId: reader.u128(), membersCount: reader.u32(),
          reason: reader.code(MEMBER_LEFT_REASONS, 'member left reason')
        }
      };
    case 'consumer_group_partition_handed_over':
      return {
        kind,
        body: {
          ...readTopicKey(reader), groupId: reader.u32(), partitionId: reader.u32(),
          memberId: reader.optionalU32()
        }
      };
    case 'user_created':
    case 'user_updated':
      return { kind, body: readUserChange(reader) };
    case 'user_deleted':
    case 'user_password_changed':
      return { kind, body: { ...readUserKey(reader), username: reader.name() } };
    case 'user_permissions_updated':
      return {
        kind,
        body: {
          ...readUserKey(reader), username: reader.name(), permissions: readPermissions(reader),
          reason: reader.code(PERMISSIONS_UPDATED_REASONS, 'permissions updated reason')
        }
      };
    case 'personal_access_token_created':
      return { kind, body: readTokenChange(reader) };
    case 'personal_access_token_deleted':
      return {
        kind,
        body: {
          ...readUserKey(reader), name: reader.name(),
          reason: reader.code(TOKEN_DELETED_REASONS, 'token deleted reason')
        }
      };
    case 'session_registered':
      return { kind, body: readSession(reader) };
    case 'session_closed':
      return {
        kind,
        body: {
          ...readSession(reader),
          reason: reader.code(SESSION_CLOSED_REASONS, 'session closed reason')
        }
      };
  }
};

const readStreamKey = (reader: ChangeReader): MetadataStreamKey => ({
  streamId: reader.u32(),
  streamCreatedOp: reader.u64()
});

const readTopicKey = (reader: ChangeReader): MetadataTopicKey => ({
  ...readStreamKey(reader),
  topicId: reader.u32(),
  topicCreatedOp: reader.u64()
});

const readUserKey = (reader: ChangeReader): MetadataUserKey => ({
  userId: reader.u32(),
  userCreatedAt: reader.u64()
});

const readStreamChange = (reader: ChangeReader): MetadataStreamChange => ({
  ...readStreamKey(reader),
  createdAt: reader.u64(),
  name: reader.name(),
  options: readOptions(reader)
});

const readUserChange = (reader: ChangeReader): MetadataUserChange => ({
  ...readUserKey(reader),
  username: reader.name(),
  status: readUserStatus(reader),
  permissions: readPermissions(reader),
  options: readOptions(reader)
});

/** A token body, where expiry 0 means the token never expires. */
const readTokenChange = (reader: ChangeReader): MetadataTokenChange => {
  const key = readUserKey(reader);
  const name = reader.name();
  const expiry = reader.u64();
  return { ...key, name, expiry: expiry === 0n ? null : expiry };
};

const readSession = (reader: ChangeReader): MetadataSession => ({
  ...readUserKey(reader),
  clientId: reader.u128(),
  session: reader.u64()
});

const readUserStatus = (reader: ChangeReader): UserStatus => {
  const offset = reader.position;
  const status = reader.u8();
  if (status !== UserStatus.Active && status !== UserStatus.Inactive)
    reader.fail(`user status ${status} at offset ${offset}`);
  return status;
};

/**
 * `[state:u8]`, then for state 0 the explicit and derived blocks exactly as
 * the stream, topic and user reads carry them.
 */
const readOptions = (reader: ChangeReader): MetadataOptions => {
  const offset = reader.position;
  const state = reader.u8();
  if (state === 1)
    return { unreadable: true };
  if (state !== 0)
    reader.fail(`options state ${state} at offset ${offset}`);
  const block = () => {
    const blockOffset = reader.position;
    const bytes = reader.take(reader.u32());
    try {
      return deserializeOptions(bytes);
    } catch (error) {
      return reader.fail(`options at offset ${blockOffset}: ${(error as Error).message}`);
    }
  };
  const options = block();
  return { unreadable: false, options, derivedOptions: block() };
};

/** `[has:u8]` and the permission set without a length prefix. */
const readPermissions = (reader: ChangeReader): UserPermissions | null => {
  if (!reader.flag())
    return null;
  const global = {
    ManageServers: reader.grant(),
    ReadServers: reader.grant(),
    ManageUsers: reader.grant(),
    ReadUsers: reader.grant(),
    ManageStreams: reader.grant(),
    ReadStreams: reader.grant(),
    ManageTopics: reader.grant(),
    ReadTopics: reader.grant(),
    PollMessages: reader.grant(),
    SendMessages: reader.grant()
  };
  const streams: StreamPerms[] = [];
  for (let hasStream = reader.grant(); hasStream; hasStream = reader.grant()) {
    const streamId = reader.u32();
    const permissions = {
      manageStream: reader.grant(),
      readStream: reader.grant(),
      manageTopics: reader.grant(),
      readTopics: reader.grant(),
      pollMessages: reader.grant(),
      sendMessages: reader.grant()
    };
    const topics: TopicPerms[] = [];
    for (let hasTopic = reader.grant(); hasTopic; hasTopic = reader.grant()) {
      const topicId = reader.u32();
      topics.push({
        topicId,
        permissions: {
          manage: reader.grant(),
          read: reader.grant(),
          pollMessages: reader.grant(),
          sendMessages: reader.grant()
        }
      });
    }
    streams.push({ streamId, permissions, topics });
  }
  return { global, streams };
};

const groupsOfMask = (mask: number): MetadataChangeGroup[] =>
  [...GROUP_BY_CODE].filter(([code]) => (mask & (1 << (code - 1))) !== 0).map(([, group]) => group);

/** Why a page token breaks its own invariants, `undefined` for a valid one. */
const pageTokenRefusal = (payload: Buffer): string | undefined => {
  if (payload.length < PAGE_TOKEN_FIXED_SIZE)
    return `a page token is at least ${PAGE_TOKEN_FIXED_SIZE} bytes`;
  const anchor = decodeCursor(payload);
  if (anchor === undefined)
    return 'page token anchor is not canonical';
  const groupsMask = payload[METADATA_CHANGE_CURSOR_SIZE + 8];
  const groupCode = payload[METADATA_CHANGE_CURSOR_SIZE + 9];
  const group = GROUP_BY_CODE.get(groupCode);
  if (group === undefined)
    return `page token names unknown group ${groupCode}`;
  const keyLength = payload.readUInt16LE(METADATA_CHANGE_CURSOR_SIZE + 10);
  if (keyLength > MAX_PAGE_TOKEN_KEY_SIZE || payload.length !== PAGE_TOKEN_FIXED_SIZE + keyLength)
    return `page token key of ${keyLength} bytes does not fill a ${payload.length} byte token`;
  if ((groupsMask & (1 << (groupCode - 1))) === 0)
    return 'page token group is not in its group mask';
  if (payload.readBigUInt64LE(METADATA_CHANGE_CURSOR_SIZE) < anchor.op)
    return 'page token head is below its anchor';
  if (!keyFitsGroup(group, payload.subarray(PAGE_TOKEN_FIXED_SIZE)))
    return `page token key of ${keyLength} bytes does not fit group ${group}`;
  return undefined;
};

const keyFitsGroup = (group: MetadataChangeGroup, key: Buffer): boolean => {
  switch (group) {
    case 'streams':
    case 'users':
      return key.length === 4;
    case 'topics':
    case 'partitions':
    case 'sessions':
      return key.length === 8;
    case 'consumer_groups':
      return key.length === 12;
    case 'consumer_group_members':
      return key.length === 16;
    case 'personal_access_tokens':
      return key.length > 5 && key[4] > 0 && key.length === 5 + key[4] && isUtf8(key.subarray(5));
  }
};

/**
 * Strips `prefix` and decodes unpadded Base64url that encodes back to the
 * same text, so one value has exactly one text form.
 */
const decodeCanonicalBase64 = (text: string, prefix: string): Buffer => {
  if (!text.startsWith(prefix))
    throw invalidCommand(`expected the ${prefix} prefix`);
  const encoded = text.slice(prefix.length);
  const bytes = Buffer.from(encoded, 'base64url');
  if (bytes.toString('base64url') !== encoded)
    throw invalidCommand('expected canonical unpadded Base64url');
  return bytes;
};

/**
 * The cursor in the first 44 bytes, `undefined` for an op 0 that carries a
 * checksum or an ordinal, so each caller raises its own error.
 */
const decodeCursor = (bytes: Buffer): MetadataChangeCursor | undefined => {
  const op = bytes.readBigUInt64LE(16);
  const checksum = readUnsigned128(bytes, 24);
  const ordinal = bytes.readUInt32LE(40);
  if (op === 0n && (checksum !== 0n || ordinal !== 0))
    return undefined;
  return new MetadataChangeCursor(readUnsigned128(bytes, 0), op, checksum, ordinal);
};

const isUnsigned = (value: bigint, maximum: bigint): boolean =>
  typeof value === 'bigint' && value >= 0n && value <= maximum;

const readUnsigned128 = (bytes: Buffer, offset: number): bigint =>
  bytes.readBigUInt64LE(offset) | (bytes.readBigUInt64LE(offset + 8) << 64n);

const writeUnsigned128 = (bytes: Buffer, offset: number, value: bigint): void => {
  bytes.writeBigUInt64LE(value & U64_MASK, offset);
  bytes.writeBigUInt64LE(value >> 64n, offset + 8);
};

const invalidCommand = (detail: string) =>
  responseError(COMMAND_CODE.GetMetadataChanges, INVALID_COMMAND, detail);
