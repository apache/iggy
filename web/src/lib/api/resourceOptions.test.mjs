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
  UINT64_MAX,
  isOptionExplicit,
  isStreamEncryptionKey,
  optionText,
  optionUint64,
  streamCreate,
  streamUpdate,
  topicCreate,
  topicUpdate,
  userUpdate
} from './resourceOptions.ts';

const currentTopic = { name: 'orders', messageExpiry: 0, maxTopicSize: '1073741824' };
const streamKey = Buffer.alloc(32, 7).toString('base64');

test('given_topic_reset_when_building_update_should_list_only_reset_keys', () => {
  const body = topicUpdate({
    current: currentTopic,
    next: { name: 'orders', messageExpiry: 604_800_000_000, maxTopicSize: '2 GiB' },
    reset: ['max_topic_size', 'max_topic_size']
  });

  assert.deepEqual(body, { message_expiry: '604800000000', reset: ['max_topic_size'] });
  assert.ok(Object.values(body).every((value) => value !== null));
});

test('given_changed_topic_when_building_update_should_send_only_changed_keys', () => {
  assert.deepEqual(
    topicUpdate({
      current: { ...currentTopic, messageExpiry: 60_000_000 },
      next: { name: 'orders-v2', messageExpiry: 0, maxTopicSize: ' 1073741824 ' }
    }),
    { name: 'orders-v2', message_expiry: 'unlimited' }
  );
  assert.deepEqual(topicUpdate({ current: currentTopic, next: currentTopic }), {});
});

test('given_topic_settings_when_building_create_should_encode_options_as_update_does', () => {
  const settings = { name: 'orders', messageExpiry: 3_600_000_000, maxTopicSize: ' 1 GiB ' };
  const created = topicCreate({ ...settings, partitionsCount: 3, compressionAlgorithm: 'gzip' });
  const updated = topicUpdate({
    current: { name: 'orders', messageExpiry: 1, maxTopicSize: '1' },
    next: settings
  });

  assert.deepEqual(created, {
    name: 'orders',
    partitions_count: 3,
    options: {
      compression_algorithm: 'gzip',
      message_expiry: '3600000000',
      max_topic_size: '1 GiB'
    }
  });
  assert.equal(created.options.message_expiry, updated.message_expiry);
  assert.equal(created.options.max_topic_size, updated.max_topic_size);
});

test('given_default_topic_settings_when_building_create_should_omit_default_options', () => {
  const settings = { name: 'orders', partitionsCount: 1, messageExpiry: 0 };

  assert.deepEqual(topicCreate({ ...settings, compressionAlgorithm: 'none', maxTopicSize: ' ' }), {
    name: 'orders',
    partitions_count: 1,
    options: {}
  });
  assert.deepEqual(
    topicCreate({ ...settings, compressionAlgorithm: 'none', maxTopicSize: 'unlimited' }).options,
    { max_topic_size: 'unlimited' }
  );
});

test('given_stream_key_when_building_create_should_use_nested_options_without_persistence', () => {
  assert.equal(isStreamEncryptionKey(streamKey), true);
  assert.deepEqual(streamCreate('orders', streamKey), {
    name: 'orders',
    options: { encryption_key: streamKey }
  });
  assert.deepEqual(streamCreate('orders'), { name: 'orders', options: {} });

  const invalidKeys = [
    '',
    'not base64 at all',
    Buffer.alloc(31, 7).toString('base64'),
    Buffer.alloc(33, 7).toString('base64'),
    streamKey.slice(0, -1),
    `${streamKey} `,
    `${streamKey.slice(0, -2)}d=`,
    `${Buffer.alloc(32, 0xff).toString('base64url')}=`
  ];
  for (const invalidKey of invalidKeys) {
    assert.equal(isStreamEncryptionKey(invalidKey), false, `Accepted key: ${invalidKey}`);
  }
  for (const invalidKey of invalidKeys.slice(1)) {
    assert.throws(() => streamCreate('orders', invalidKey), TypeError);
  }
});

test('given_u64_option_above_2_pow_53_when_reading_should_keep_precision', () => {
  const { options } = JSON.parse(
    '{"options":{' +
      '"max_topic_size":{"value":"9007199254740993","kind":"uint64","explicit":true},' +
      '"message_expiry":{"value":"18446744073709551615","kind":"uint64","explicit":false},' +
      '"compression_algorithm":{"value":"gzip","kind":"string","explicit":true}}}'
  );

  assert.equal(optionUint64(options, 'max_topic_size'), 9_007_199_254_740_993n);
  assert.equal(optionUint64(options, 'message_expiry'), UINT64_MAX);
  assert.equal(optionUint64(options, 'compression_algorithm'), undefined);
  assert.equal(optionUint64(options, 'segment_size'), undefined);
  assert.equal(optionUint64(undefined, 'max_topic_size'), undefined);
});

test('given_option_entries_when_reading_should_return_text_and_explicit_flag', () => {
  const options = {
    compression_algorithm: { value: 'gzip', kind: 'string', explicit: true },
    encryption_key_source: { value: 'server', kind: 'string', explicit: false }
  };

  assert.equal(optionText(options, 'compression_algorithm'), 'gzip');
  assert.equal(optionText(options, 'constructor'), undefined);
  assert.equal(isOptionExplicit(options, 'compression_algorithm'), true);
  assert.equal(isOptionExplicit(options, 'encryption_key_source'), false);
  assert.equal(isOptionExplicit(options, 'max_topic_size'), false);
});

test('given_stream_and_user_changes_when_building_update_should_send_only_changed_keys', () => {
  assert.deepEqual(streamUpdate({ current: { name: 'orders' }, next: { name: 'sales' } }), {
    name: 'sales'
  });
  assert.deepEqual(streamUpdate({ current: { name: 'orders' }, next: { name: 'orders' } }), {});
  assert.deepEqual(
    userUpdate({
      current: { username: 'alice', status: 'active' },
      next: { username: 'alice', status: 'inactive' }
    }),
    { status: 'inactive' }
  );
});
