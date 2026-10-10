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
import { it } from 'node:test';
import { translateErrorCode } from './error.code.js';

it('translates the partitions capacity error', () => {
  assert.equal(translateErrorCode(2022), 'Partitions limit reached, raise [metadata] partitions_max');
});

it('translates the fixed-topic resize error', () => {
  assert.equal(translateErrorCode(2023), 'Partition resizing is disabled for this fixed topic');
});

it('reports an unknown outcome for an aged-out request', () => {
  assert.equal(
    translateErrorCode(85),
    'Request is below the deduplication window; outcome unknown, resending may duplicate the write'
  );
});

it('translates the partition history refusal', () => {
  assert.equal(translateErrorCode(87), 'The partition history is no longer available; this request cannot execute again');
});

it('translates the lifecycle refusal', () => {
  assert.equal(translateErrorCode(88), 'A committed lifecycle operation prevents this mutation. Retry as a new request.');
});

it('translates the consumer-offset capacity error', () => {
  assert.equal(translateErrorCode(3024), 'Consumer offset limit reached for partition, raise [partition] consumer_offsets_max');
});

it('translates a partition history change', () => {
  assert.equal(translateErrorCode(3014), 'Partition history changed after the request was resolved');
});

it('translates the consumer-group error range', () => {
  assert.equal(
    translateErrorCode(5000),
    'Consumer group with ID: {0} for topic with ID: {1} was not found.'
  );
  assert.equal(translateErrorCode(5001), 'error');
  assert.equal(translateErrorCode(5002), 'Invalid consumer group ID');
  assert.equal(
    translateErrorCode(5003),
    'Consumer group with name: {0} for topic with ID: {1} was not found.'
  );
  assert.equal(
    translateErrorCode(5004),
    'Consumer group with name: {0} for topic with ID: {1} already exists.'
  );
  assert.equal(translateErrorCode(5005), 'Invalid consumer group name');
  assert.equal(
    translateErrorCode(5006),
    'Consumer group member with client ID: {0} for group with ID: {1} for topic with ID: {2} was not found.'
  );
  assert.equal(
    translateErrorCode(5007),
    'Failed to create consumer group info file for ID: {0} for topic with ID: {1} for stream with ID: {2}.'
  );
  assert.equal(
    translateErrorCode(5008),
    'Failed to delete consumer group info file for ID: {0} for topic with ID: {1} for stream with ID: {2}.'
  );
  assert.equal(
    translateErrorCode(5009),
    'Consumer group member with client ID: {0} does not own partition: {1} at the current generation (rebalance in progress).'
  );
  assert.equal(translateErrorCode(5010), 'Too many consumer groups');
  assert.equal(translateErrorCode(5011), 'error');
});
