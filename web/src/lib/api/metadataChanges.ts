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

export const METADATA_CHANGE_GROUPS = [
  'streams',
  'topics',
  'partitions',
  'consumer_groups',
  'consumer_group_members',
  'users',
  'personal_access_tokens',
  'sessions'
] as const;
export const DEFAULT_METADATA_CHANGES_LIMIT = 100;
export const MAX_METADATA_CHANGES_LIMIT = 1000;
export const ROOT_USER_ID = 0;
export const MAX_SHOWN_METADATA_CHANGES = 5000;

export type MetadataChangeGroup = (typeof METADATA_CHANGE_GROUPS)[number];
export type MetadataChangeStatus = 'ok' | 'behind' | 'expired' | 'reset';
export type MetadataChangeStart = 'earliest' | 'latest';
// Values above 2^53 arrive as decimal strings, see `getJson`.
export type WireInteger = number | string;

export type MetadataChangeRecord = {
  op: WireInteger;
  ordinal: number;
  checksum: string;
  timestamp: WireInteger;
  actor_user_id?: number;
  group: string;
  kind: string;
  [field: string]: unknown;
};

export type MetadataChangesPage = {
  status: MetadataChangeStatus;
  phase: 'changes' | 'current';
  log_id: string | null;
  oldest_op: WireInteger;
  retained_since: WireInteger;
  head_op: WireInteger;
  next: string | null;
  has_more: boolean;
  records: MetadataChangeRecord[];
  items: unknown[];
  current_page_token: string | null;
};

export type MetadataChangesFilter = {
  start: MetadataChangeStart;
  groups: MetadataChangeGroup[];
  since?: number;
  until?: number;
  limit: number;
};

/** Query parameters of `GET /metadata/changes`, continuing after `cursor` when given. */
export function metadataChangesQuery(
  filter: MetadataChangesFilter,
  cursor?: string
): Record<string, string> {
  const query: Record<string, string> = cursor ? { after: cursor } : { from: filter.start };
  const groups = METADATA_CHANGE_GROUPS.filter((group) => filter.groups.includes(group));
  if (groups.length > 0) {
    query.groups = groups.join(',');
  }
  // `latest` returns a position only, the server refuses time bounds with it.
  if (cursor || filter.start === 'earliest') {
    if (filter.since !== undefined) {
      query.since = `${filter.since}`;
    }
    if (filter.until !== undefined) {
      query.until = `${filter.until}`;
    }
  }
  query.limit = `${filter.limit}`;
  return query;
}

/** Why a filter cannot be sent, or `undefined` when the server would accept it. */
export function filterProblem(filter: MetadataChangesFilter): string | undefined {
  if (
    !Number.isInteger(filter.limit) ||
    filter.limit < 1 ||
    filter.limit > MAX_METADATA_CHANGES_LIMIT
  ) {
    return `Limit must be a whole number from 1 to ${MAX_METADATA_CHANGES_LIMIT}`;
  }
  if (filter.since !== undefined && filter.until !== undefined && filter.since > filter.until) {
    return 'The start of the time range is after its end';
  }
  return undefined;
}

/** Microseconds since the epoch of a `datetime-local` input value, `undefined` when empty. */
export function microsFromDateTimeLocal(value: string): number | undefined {
  if (value.trim() === '') {
    return undefined;
  }
  const millis = new Date(value).getTime();
  return Number.isNaN(millis) ? undefined : millis * 1000;
}

/** Root and holders of the global `manage_servers` permission may read the log. */
export function canReadMetadataChanges(
  userId: number | null,
  manageServers: boolean | undefined
): boolean {
  return userId === ROOT_USER_ID || manageServers === true;
}

/** What to tell the reader about a page that carries no records to show. */
export function statusMessage(
  status: MetadataChangeStatus,
  following: boolean
): string | undefined {
  switch (status) {
    case 'ok':
      return undefined;
    case 'behind':
      return following
        ? 'This node has not applied that position yet. Retrying.'
        : 'This node has not applied that position yet. Load again shortly.';
    case 'expired':
      return 'The node no longer holds every change after this position. Load again from the earliest retained change.';
    case 'reset':
      return 'The position belongs to another history, for example a rebuilt data directory. Load again.';
  }
}

/** One line naming what a record changed. */
export function describeChange(record: MetadataChangeRecord): string {
  const text = (field: string) => (record[field] === undefined ? '?' : `${record[field]}`);
  const topic = `topic ${text('stream_id')}/${text('topic_id')}`;
  const group = `group ${text('group_id')} on ${topic}`;
  switch (record.group) {
    case 'streams':
      return `stream "${text('name')}" (${text('stream_id')})`;
    case 'topics':
      return `${topic} "${text('name')}"`;
    case 'partitions':
      return `${topic} now has ${text('partitions_count')} partitions`;
    case 'consumer_groups':
      return `${group} "${text('name')}"`;
    case 'consumer_group_members':
      if (record.kind === 'consumer_group_partition_handed_over') {
        const owner = record.member_id === undefined ? 'no member' : `member ${text('member_id')}`;
        return `partition ${text('partition_id')} of ${group} to ${owner}`;
      }
      return `member ${text('member_id')} of ${group}`;
    case 'users':
      return `user "${text('username')}" (${text('user_id')})`;
    case 'personal_access_tokens':
      return `token "${text('name')}" of user ${text('user_id')}`;
    case 'sessions':
      return `session ${text('session')} of user ${text('user_id')}`;
    default:
      return `${record.group} ${record.kind}`;
  }
}

export async function scanRecentMetadataChanges(
  filter: MetadataChangesFilter,
  readPage: (query: Record<string, string>) => Promise<MetadataChangesPage>,
  options: { capacity?: number; signal?: AbortSignal; onPage?: (count: number) => void } = {}
): Promise<{ page: MetadataChangesPage; trimmed: boolean }> {
  const capacity = options.capacity ?? MAX_SHOWN_METADATA_CHANGES;
  options.signal?.throwIfAborted();
  const boundary = await readPage(metadataChangesQuery({ ...filter, start: 'latest' }));
  options.signal?.throwIfAborted();
  if (boundary.status !== 'ok' || !boundary.next) return { page: boundary, trimmed: false };
  const end = cursorPosition(boundary.next);
  let after: string | undefined;
  let records: MetadataChangeRecord[] = [];
  let trimmed = false;
  let pages = 0;
  for (;;) {
    options.signal?.throwIfAborted();
    const page = await readPage(
      metadataChangesQuery(
        { ...filter, start: 'earliest', limit: MAX_METADATA_CHANGES_LIMIT },
        after
      )
    );
    options.signal?.throwIfAborted();
    options.onPage?.(++pages);
    if (page.status !== 'ok') return { page: { ...page, records: [] }, trimmed: false };
    if (page.log_id !== boundary.log_id) {
      return {
        page: { ...page, status: 'reset', records: [], next: null, has_more: false },
        trimmed: false
      };
    }
    const matching = page.records.filter((record) => comparePosition(record, end) <= 0);
    trimmed ||= records.length + matching.length > capacity;
    records = [...records, ...matching].slice(-capacity);
    const passedEnd = page.next !== null && comparePosition(cursorPosition(page.next), end) >= 0;
    if (!page.has_more || passedEnd) {
      // A node that has not applied the captured head stops short of it, so keep its own position.
      const reachedEnd = passedEnd || BigInt(page.head_op) >= BigInt(end.op);
      return {
        page: reachedEnd
          ? { ...page, head_op: boundary.head_op, records, next: boundary.next, has_more: false }
          : { ...page, records, has_more: false },
        trimmed
      };
    }
    if (!page.next || page.next === after)
      throw new Error('The server did not advance the change position. Load again.');
    after = page.next;
  }
}

function cursorPosition(cursor: string): { op: string; ordinal: number } {
  const bytes = Uint8Array.from(
    atob(cursor.slice(4).replaceAll('-', '+').replaceAll('_', '/')),
    (char) => char.charCodeAt(0)
  );
  if (!cursor.startsWith('mc1_') || bytes.length !== 44)
    throw new Error('The server returned an invalid change position.');
  const view = new DataView(bytes.buffer);
  return { op: view.getBigUint64(16, true).toString(), ordinal: view.getUint32(40, true) };
}

function comparePosition(
  left: { op: WireInteger; ordinal: number },
  right: { op: WireInteger; ordinal: number }
): number {
  const leftOperation = BigInt(left.op);
  const rightOperation = BigInt(right.op);
  return leftOperation === rightOperation
    ? left.ordinal - right.ordinal
    : leftOperation < rightOperation
      ? -1
      : 1;
}
