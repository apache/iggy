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

import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  MAX_METADATA_CHANGES_LIMIT,
  canReadMetadataChanges,
  describeChange,
  filterProblem,
  metadataChangesQuery,
  microsFromDateTimeLocal,
  statusMessage,
  scanRecentMetadataChanges
} from './metadataChanges.ts';

/** @type {import('./metadataChanges.ts').MetadataChangesFilter} */
const filter = { start: 'earliest', groups: [], limit: 100 };
const cursor = 'mc1_15McbiqPSw0AAAAAAAAAABsQAAAAAAAAZz2eiwP3waUAAAAAAAAAAAAAAAA';

test('given_earliest_filter_when_building_query_should_send_groups_in_code_order_and_bounds', () => {
  assert.deepEqual(
    metadataChangesQuery({
      ...filter,
      groups: /** @type {const} */ (['sessions', 'streams', 'sessions']),
      since: 5,
      until: 9
    }),
    { from: 'earliest', groups: 'streams,sessions', since: '5', until: '9', limit: '100' }
  );
});

test('given_cursor_when_building_query_should_continue_after_it_and_keep_bounds', () => {
  assert.deepEqual(
    metadataChangesQuery({ ...filter, start: /** @type {const} */ ('latest'), since: 5 }, cursor),
    {
      after: cursor,
      since: '5',
      limit: '100'
    }
  );
});

test('given_latest_filter_when_building_query_should_drop_time_bounds_the_server_refuses', () => {
  assert.deepEqual(
    metadataChangesQuery({ ...filter, start: /** @type {const} */ ('latest'), since: 5, until: 9 }),
    {
      from: 'latest',
      limit: '100'
    }
  );
});

test('given_out_of_range_limit_or_inverted_range_when_checking_should_name_the_problem', () => {
  for (const limit of [0, MAX_METADATA_CHANGES_LIMIT + 1, 1.5]) {
    assert.match(filterProblem({ ...filter, limit }) ?? '', /Limit/);
  }
  assert.match(filterProblem({ ...filter, since: 9, until: 5 }) ?? '', /time range/);
  assert.equal(filterProblem({ ...filter, since: 5, until: 5 }), undefined);
});

test('given_datetime_local_when_converting_should_return_microseconds_or_nothing', () => {
  assert.equal(
    microsFromDateTimeLocal('2026-10-23T10:00'),
    new Date('2026-10-23T10:00').getTime() * 1000
  );
  assert.equal(microsFromDateTimeLocal(''), undefined);
  assert.equal(microsFromDateTimeLocal('not a date'), undefined);
});

test('given_user_when_checking_access_should_allow_only_root_and_manage_servers', () => {
  assert.equal(canReadMetadataChanges(0, undefined), true);
  assert.equal(canReadMetadataChanges(7, true), true);
  assert.equal(canReadMetadataChanges(7, false), false);
  assert.equal(canReadMetadataChanges(null, undefined), false);
});

test('given_non_ok_status_when_describing_should_explain_what_to_do', () => {
  assert.equal(statusMessage('ok', false), undefined);
  for (const status of /** @type {const} */ (['behind', 'expired', 'reset'])) {
    assert.ok((statusMessage(status, false) ?? '').length > 0);
  }
});

test('given_behind_status_when_describing_should_promise_a_retry_only_while_following', () => {
  assert.match(statusMessage('behind', true) ?? '', /Retrying/);
  assert.doesNotMatch(statusMessage('behind', false) ?? '', /Retrying/);
  assert.match(statusMessage('behind', false) ?? '', /Load again/);
});

test('given_records_when_describing_should_name_the_changed_resource', () => {
  const envelope = { op: 4121, ordinal: 0, checksum: '00', timestamp: 1 };
  assert.equal(
    describeChange({
      ...envelope,
      group: 'streams',
      kind: 'stream_created',
      stream_id: 3,
      name: 'telemetry'
    }),
    'stream "telemetry" (3)'
  );
  assert.equal(
    describeChange({
      ...envelope,
      group: 'consumer_group_members',
      kind: 'consumer_group_partition_handed_over',
      stream_id: 3,
      topic_id: 1,
      group_id: 2,
      partition_id: 5
    }),
    'partition 5 of group 2 on topic 3/1 to no member'
  );
  assert.equal(describeChange({ ...envelope, group: '200', kind: '7', body: 'AA' }), '200 7');
});

function position(operation, ordinal = 0) {
  const bytes = Buffer.alloc(44);
  bytes.writeBigUInt64LE(1n, 0);
  bytes.writeBigUInt64LE(BigInt(operation), 16);
  bytes.writeUInt32LE(ordinal, 40);
  return `mc1_${bytes.toString('base64url')}`;
}

function change(operation, ordinal = 0) {
  return {
    op: String(operation),
    ordinal,
    timestamp: 1,
    checksum: '00',
    group: 'streams',
    kind: 'stream_created'
  };
}

/** @returns {import('./metadataChanges.ts').MetadataChangesPage} */
function changesPage(records, next, hasMore = false) {
  return {
    status: 'ok',
    phase: 'changes',
    log_id: '01',
    head_op: '10',
    oldest_op: 1,
    retained_since: 0,
    next,
    has_more: hasMore,
    records,
    items: [],
    current_page_token: null
  };
}

test('given_recent_scan_when_pages_cross_the_captured_head_should_keep_only_the_newest_bounded_records', async () => {
  const pages = [
    changesPage([], position(10)),
    changesPage([change(1), change(2)], position(2), true),
    changesPage([change(3), change(10), change(11)], position(11), true)
  ];
  const queries = [];
  const result = await scanRecentMetadataChanges(
    filter,
    async (query) => {
      queries.push(query);
      return pages.shift() ?? assert.fail('unexpected page request');
    },
    { capacity: 2 }
  );
  assert.deepEqual(
    result.page.records.map((record) => record.op),
    ['3', '10']
  );
  assert.equal(result.page.next, position(10));
  assert.equal(result.page.has_more, false);
  assert.equal(result.trimmed, true);
  assert.equal(queries[0].from, 'latest');
  assert.equal(queries[1].from, 'earliest');
  assert.equal(queries[2].after, position(2));
});

test('given_empty_filtered_pages_when_scanning_recent_should_stop_at_the_captured_cursor_without_waiting_for_new_writes', async () => {
  const large = 9007199254740993n;
  const pages = [
    { ...changesPage([], position(large, 2)), head_op: String(large) },
    changesPage([], position(large - 1n), true),
    changesPage([], position(large, 3), true)
  ];
  const result = await scanRecentMetadataChanges(
    filter,
    async () => pages.shift() ?? assert.fail('unexpected page request')
  );
  assert.equal(pages.length, 0);
  assert.deepEqual(result.page.records, []);
  assert.equal(result.page.next, position(large, 2));
});

test('given_node_behind_the_captured_head_when_scanning_recent_should_continue_from_its_own_position', async () => {
  const pages = [
    changesPage([], position(10)),
    { ...changesPage([change(4)], position(4)), head_op: '6' }
  ];
  const result = await scanRecentMetadataChanges(
    filter,
    async () => pages.shift() ?? assert.fail('unexpected page request')
  );
  assert.deepEqual(
    result.page.records.map((record) => record.op),
    ['4']
  );
  assert.equal(result.page.next, position(4));
  assert.equal(result.page.head_op, '6');
  assert.equal(result.page.has_more, false);
});

test('given_expiry_or_changed_history_during_recent_scan_should_discard_partial_results', async () => {
  for (const status of /** @type {const} */ (['expired', 'reset', 'behind', 'changed_history'])) {
    /** @type {import('./metadataChanges.ts').MetadataChangesPage[]} */
    const pages = [
      changesPage([], position(10)),
      changesPage([change(1)], position(1), true),
      {
        ...changesPage([], null),
        status: status === 'changed_history' ? 'ok' : status,
        log_id: status === 'changed_history' ? '02' : '01'
      }
    ];
    const result = await scanRecentMetadataChanges(
      filter,
      async () => pages.shift() ?? assert.fail('unexpected page request')
    );
    assert.equal(result.page.status, status === 'changed_history' ? 'reset' : status);
    assert.deepEqual(result.page.records, []);
  }
});

test('given_recent_time_filters_when_capturing_head_should_omit_bounds_only_for_latest', async () => {
  const queries = [];
  const pages = [changesPage([], position(10)), changesPage([change(4)], position(4))];
  await scanRecentMetadataChanges({ ...filter, since: 2, until: 9 }, async (query) => {
    queries.push(query);
    return pages.shift() ?? assert.fail('unexpected page request');
  });
  assert.equal(queries[0].since, undefined);
  assert.equal(queries[0].until, undefined);
  assert.equal(queries[1].since, '2');
  assert.equal(queries[1].until, '9');
});

test('given_canceled_recent_scan_should_stop_before_another_request', async () => {
  const controller = new AbortController();
  let calls = 0;
  await assert.rejects(
    scanRecentMetadataChanges(
      filter,
      async () => {
        calls++;
        controller.abort();
        return changesPage([], position(10));
      },
      { signal: controller.signal }
    ),
    { name: 'AbortError' }
  );
  assert.equal(calls, 1);
});
