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

import type { CommandResponse } from '../../client/index.js';
import { COMMAND_CODE } from '../command.code.js';
import { wrapCommand } from '../command.utils.js';
import { DeserializeError, responseError } from '../error.utils.js';
import {
  ChangeReader, METADATA_CHANGE_CURSOR_SIZE, METADATA_CHANGE_GROUP_CODE, MetadataChangeCursor,
  MetadataCurrentPageToken, decodeMetadataChange, decodeMetadataCurrentItem,
  metadataChangeGroupsMask, metadataChangePhase, metadataChangeStatus, missesParentGroup,
  readMetadataChangeCursor,
  type MetadataChangeGroup, type MetadataChangePhase, type MetadataChangeRecord,
  type MetadataChangeStatus, type MetadataCurrentItem
} from './metadata-changes.utils.js';

/** Records or items per page when the query does not say. */
export const DEFAULT_METADATA_CHANGES_LIMIT = 100;

/** Most records or items one page returns. */
export const MAX_METADATA_CHANGES_LIMIT = 1000;

const INVALID_COMMAND = 3;
const U64_MAX = 0xffff_ffff_ffff_ffffn;
/** The group count travels in one byte. */
const MAX_GROUPS = 255;
const START_MODE_CODE = { earliest: 0, latest: 1, after: 2, current: 3 } as const;

/** Where a change log query starts. */
export type MetadataChangeStart =
  /** The oldest operation the requested groups still hold completely */
  | { mode: 'earliest' }
  /** No records, only a cursor at the newest retained record */
  | { mode: 'latest' }
  /** The records after a cursor */
  | { mode: 'after', cursor: MetadataChangeCursor }
  /** Current state pages: no token starts a scan, the previous page's token continues it */
  | { mode: 'current', pageToken?: MetadataCurrentPageToken };

/** Builders of every start. */
export const MetadataChangeStart = {
  Earliest: { mode: 'earliest' } as MetadataChangeStart,
  Latest: { mode: 'latest' } as MetadataChangeStart,
  After: (cursor: MetadataChangeCursor): MetadataChangeStart => ({ mode: 'after', cursor }),
  Current: (pageToken?: MetadataCurrentPageToken): MetadataChangeStart => ({ mode: 'current', pageToken })
};

/**
 * One request for a page of the change log or of the current state.
 *
 * Refused with `ResponseError` code 3 (invalid command) before anything is
 * sent: a limit outside 1 to 1000, `since` above `until`, a time bound with
 * the latest or current start, an unknown group, a current scan that misses
 * a parent group, and a page token that is not a `MetadataCurrentPageToken`
 * or was issued for other groups.
 */
export type GetMetadataChanges = {
  /** Defaults to the earliest start */
  start?: MetadataChangeStart,
  /** Record families to return, every group when absent or empty */
  groups?: MetadataChangeGroup[],
  /** Keeps records stamped at or after it, microseconds since the Unix epoch */
  since?: bigint,
  /** Keeps records stamped at or before it, microseconds since the Unix epoch */
  until?: bigint,
  /** Records or items per page, 100 when absent */
  limit?: number
};

/** One page of the change log, or of the current state. */
export type MetadataChanges = {
  status: MetadataChangeStatus,
  phase: MetadataChangePhase,
  /** The node's log id, null before it applied op 1 */
  logId: bigint | null,
  /** Lowest whole operation from which the requested groups are complete */
  oldestOp: bigint,
  /** The requested groups are complete above this timestamp, 0 when nothing was evicted */
  retainedSince: bigint,
  /** Highest metadata operation the node applied */
  headOp: bigint,
  /** The cursor to send back: present on ok, echoed on behind, null on expired and reset */
  next: MetadataChangeCursor | null,
  /** More records remain after `next`, or another state page exists */
  hasMore: boolean,
  records: MetadataChangeRecord[],
  items: MetadataCurrentItem[],
  /** Continuation of a current state scan, null once it is complete */
  currentPageToken: MetadataCurrentPageToken | null
};

/**
 * Decodes a `GetMetadataChanges` response. Bodies are decoded strictly:
 * unknown reason codes, presence bytes other than 0 and 1, a known kind under
 * another group and trailing body bytes are refused, while a group or kind
 * this build does not know keeps its raw body.
 *
 * @throws DeserializeError for a malformed response
 */
export const deserializeMetadataChanges = (payload: Buffer): MetadataChanges => {
  const reader = new ChangeReader(payload);
  const status = metadataChangeStatus(reader);
  const phase = metadataChangePhase(reader);
  const logId = reader.flag() ? reader.u128() : null;
  const oldestOp = reader.u64();
  const retainedSince = reader.u64();
  const headOp = reader.u64();
  const next = reader.flag() ? readMetadataChangeCursor(reader) : null;
  const hasMore = reader.flag();

  const records: MetadataChangeRecord[] = [];
  for (let remaining = reader.u32(); remaining > 0; remaining--) {
    const op = reader.u64();
    const ordinal = reader.u32();
    const checksum = reader.u128();
    const timestamp = reader.u64();
    const actorUserId = reader.optionalU32();
    const groupCode = reader.u8();
    const kindCode = reader.u8();
    const body = reader.take(reader.u32());
    records.push({
      op, ordinal, checksum, timestamp, actorUserId, groupCode, kindCode,
      ...decodeMetadataChange(groupCode, kindCode, body)
    });
  }

  const items: MetadataCurrentItem[] = [];
  for (let remaining = reader.u32(); remaining > 0; remaining--) {
    const groupCode = reader.u8();
    items.push(decodeMetadataCurrentItem(groupCode, reader.take(reader.u32())));
  }

  const tokenLength = reader.u16();
  const currentPageToken = tokenLength === 0
    ? null
    : MetadataCurrentPageToken.fromBytes(reader.take(tokenLength));

  const holdsOtherContent = phase === 'changes'
    ? items.length > 0 || currentPageToken !== null
    : records.length > 0;
  if (holdsOtherContent)
    throw new DeserializeError(
      'cannot deserialize metadata changes: a changes page carries only records and a current page only items'
    );
  return {
    status, phase, logId, oldestOp, retainedSince, headOp, next, hasMore,
    records, items, currentPageToken
  };
};

export const GET_METADATA_CHANGES = {
  code: COMMAND_CODE.GetMetadataChanges,

  /**
   * `[start:u8][cursor:44][has_since:u8][since:u64][has_until:u8][until:u64]
   * [limit:u32][groups_count:u8][group:u8]*[token_len:u16][token]`. The cursor
   * is all zero unless the query starts after one, the token is empty unless
   * it continues a current scan, and groups keep the caller's order.
   */
  serialize: (query: GetMetadataChanges): Buffer => {
    const { start = MetadataChangeStart.Earliest, since, until } = query;
    const limit = query.limit ?? DEFAULT_METADATA_CHANGES_LIMIT;
    const groups = query.groups ?? [];
    checkQuery(start, groups, since, until, limit);

    const encodedGroups = groups.length > MAX_GROUPS ? [...new Set(groups)] : groups;
    const token = start.mode === 'current' && start.pageToken ? start.pageToken.toBytes() : null;
    const payload = Buffer.alloc(
      1 + METADATA_CHANGE_CURSOR_SIZE + 9 + 9 + 4 + 1 + encodedGroups.length + 2 + (token?.length ?? 0)
    );
    let position = payload.writeUInt8(START_MODE_CODE[start.mode], 0);
    if (start.mode === 'after')
      start.cursor.toBytes().copy(payload, position);
    position += METADATA_CHANGE_CURSOR_SIZE;
    position = writeOptionalU64(payload, position, since);
    position = writeOptionalU64(payload, position, until);
    position = payload.writeUInt32LE(limit, position);
    position = payload.writeUInt8(encodedGroups.length, position);
    for (const group of encodedGroups)
      position = payload.writeUInt8(METADATA_CHANGE_GROUP_CODE[group], position);
    position = payload.writeUInt16LE(token?.length ?? 0, position);
    if (token)
      position += token.copy(payload, position);
    return payload.subarray(0, position);
  },

  deserialize: (r: CommandResponse): MetadataChanges => deserializeMetadataChanges(r.data)
};

export const getMetadataChanges = wrapCommand<GetMetadataChanges, MetadataChanges>(
  GET_METADATA_CHANGES
);

/** Applies the request rules the server enforces. */
const checkQuery = (
  start: MetadataChangeStart, groups: readonly string[],
  since: bigint | undefined, until: bigint | undefined, limit: number
): void => {
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_METADATA_CHANGES_LIMIT)
    refuse(`limit ${limit} is outside 1 to ${MAX_METADATA_CHANGES_LIMIT}`);
  for (const bound of [since, until])
    if (bound !== undefined && (typeof bound !== 'bigint' || bound < 0n || bound > U64_MAX))
      refuse('time bounds are unsigned 64-bit microseconds');
  if (since !== undefined && until !== undefined && since > until)
    refuse('since is above until');
  const mask = metadataChangeGroupsMask(groups);
  if (mask === undefined)
    return refuse(`unknown group in ${groups.join(',')}`);
  const hasTimeBound = since !== undefined || until !== undefined;
  switch (start.mode) {
    case 'earliest':
      break;
    case 'after':
      if (!(start.cursor instanceof MetadataChangeCursor))
        refuse('an after start needs a MetadataChangeCursor');
      break;
    case 'latest':
      if (hasTimeBound)
        refuse('a latest start takes no time bound');
      break;
    case 'current':
      if (hasTimeBound)
        refuse('a current start takes no time bound');
      if (missesParentGroup(mask))
        refuse('a current scan needs the parent groups of every selected group');
      if (start.pageToken !== undefined && !(start.pageToken instanceof MetadataCurrentPageToken))
        refuse('a current start continues with a MetadataCurrentPageToken');
      if (start.pageToken !== undefined && metadataChangeGroupsMask(start.pageToken.groups) !== mask)
        refuse('the page token was issued for other groups');
      break;
    default:
      refuse(`unknown start ${(start as { mode: unknown }).mode}`);
  }
};

const writeOptionalU64 = (payload: Buffer, position: number, value: bigint | undefined): number => {
  if (value === undefined)
    return payload.writeUInt8(0, position);
  position = payload.writeUInt8(1, position);
  return payload.writeBigUInt64LE(value, position);
};

const refuse = (detail: string): never => {
  throw responseError(COMMAND_CODE.GetMetadataChanges, INVALID_COMMAND, detail);
};
