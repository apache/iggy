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

import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import type { PartitionReader } from '../stream/partition-reader.js';
import { ConsumerKind } from '../wire/offset/offset.utils.js';
import { CommitPolicy, Consumer, Partitioning, PollingStrategy } from '../wire/index.js';
import { generateMessages } from '../tcp.sm.utils.js';
import { getTestClient } from './test-client.utils.js';

// The server fetches only Offset and Next targets, so these starts and seeks
// show whether the reader resolves the other positions with a poll.
describe('e2e -> partition-reader', async () => {
  const client = getTestClient();
  const PARTITION = 0;
  const READER_COUNT = 3;
  const BATCH_MESSAGES = 5;
  /** The server stamps each batch when it appends it, so the gap gives the batches different timestamps. */
  const BATCH_GAP_MS = 20;
  const READ_DEADLINE_MS = 10_000;
  const MICROS_PER_MS = 1_000n;
  const SECOND_BATCH_START = BigInt(BATCH_MESSAGES);
  const LAST_OFFSET = SECOND_BATCH_START + BigInt(BATCH_MESSAGES) - 1n;
  const POLICIES = [['onRead', CommitPolicy.OnRead], ['manual', CommitPolicy.Manual]] as const;

  const streamId = `e2e-partition-reader-${Date.now()}`;
  const topicId = 'e2e-partition-reader-topic';

  await client.stream.create({ name: streamId });
  await client.topic.create({ streamId, name: topicId, partitionCount: 1, compressionAlgorithm: 1 });

  after(async () => {
    await client.stream.delete({ streamId });
    await client.session.logout();
    client.destroy();
  });

  const send = (count: number) => client.message.send({
    streamId, topicId, messages: generateMessages(count), partition: Partitioning.PartitionId(PARTITION)
  });

  await send(BATCH_MESSAGES);
  await sleep(BATCH_GAP_MS);
  await send(BATCH_MESSAGES);
  const { messages } = await client.message.poll({
    streamId, topicId, consumer: Consumer.Single, partitionId: PARTITION,
    pollingStrategy: PollingStrategy.Offset(0n), count: 2 * BATCH_MESSAGES, autocommit: false
  });
  // The header drops the microseconds of a timestamp, and the gap keeps the cut value after the first batch.
  const timestampOf = (offset: bigint): bigint => {
    const message = messages.find(({ headers }) => headers.offset === offset);
    assert.ok(message, `the poll returned no message at offset ${offset}`);
    return BigInt(message.headers.timestamp.getTime()) * MICROS_PER_MS;
  };
  const secondBatchAt = timestampOf(SECOND_BATCH_START);
  assert.ok(timestampOf(SECOND_BATCH_START - 1n) < secondBatchAt, 'both batches got the same timestamp');
  const timestampStart = PollingStrategy.Timestamp(secondBatchAt);

  const open = (consumer: string, commit: CommitPolicy, start: PollingStrategy) => client.message.reader({
    streamId, topicId, consumer: { kind: ConsumerKind.Single, id: consumer }, partitions: [PARTITION], start,
    options: { count: READER_COUNT, commit }
  });

  const nextOffsets = async (reader: PartitionReader, count: number): Promise<bigint[]> => {
    const signal = AbortSignal.timeout(READ_DEADLINE_MS);
    const offsets: bigint[] = [];
    while (offsets.length < count)
      offsets.push(...(await reader.next({ signal })).messages.map(({ headers }) => headers.offset));
    return offsets.slice(0, count);
  };

  const range = (first: bigint, last: bigint): bigint[] =>
    Array.from({ length: Number(last - first) + 1 }, (_, index) => first + BigInt(index));

  for (const [policy, commit] of POLICIES) {
    it(`e2e -> partition-reader::${policy}: a timestamp start delivers the first message at the timestamp first`,
      async () => {
        const reader = await open(`${policy}-timestamp-start`, commit, timestampStart);
        try {
          assert.deepEqual(await nextOffsets(reader, 1), [SECOND_BATCH_START]);
        } finally {
          await reader.close();
        }
      });

    it(`e2e -> partition-reader::${policy}: a last start delivers the last count messages first`, async () => {
      const reader = await open(`${policy}-last-start`, commit, PollingStrategy.Last);
      try {
        assert.deepEqual(await nextOffsets(reader, READER_COUNT),
          range(LAST_OFFSET - BigInt(READER_COUNT) + 1n, LAST_OFFSET));
      } finally {
        await reader.close();
      }
    });

    it(`e2e -> partition-reader::${policy}: a timestamp seek after a next start delivers the sought message next`,
      async () => {
        const reader = await open(`${policy}-timestamp-seek`, commit, PollingStrategy.Next);
        try {
          assert.deepEqual(await nextOffsets(reader, 1), [0n]);
          await reader.seek(PARTITION, timestampStart);
          assert.deepEqual(await nextOffsets(reader, 1), [SECOND_BATCH_START]);
        } finally {
          await reader.close();
        }
      });
  }

  it('e2e -> partition-reader::onRead: the offset that the close stored wins over the timestamp start', async () => {
    const consumer = 'onRead-stored-offset';
    const first = await open(consumer, CommitPolicy.OnRead, timestampStart);
    try {
      assert.deepEqual(await nextOffsets(first, BATCH_MESSAGES), range(SECOND_BATCH_START, LAST_OFFSET));
    } finally {
      await first.close();
    }
    await send(1);
    const second = await open(consumer, CommitPolicy.OnRead, timestampStart);
    try {
      assert.deepEqual(await nextOffsets(second, 1), [LAST_OFFSET + 1n]);
    } finally {
      await second.close();
    }
  });
});
