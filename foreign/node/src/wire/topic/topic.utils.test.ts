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
import { deserializeBaseTopic, deserializePartition, deserializeTopic } from './topic.utils.js';
import { serializeOptions } from '../options.utils.js';
import { HeaderValue } from '../message/header.utils.js';
import { DeserializeError } from '../error.utils.js';

/** Same bytes as the Rust `PartitionResponse` round-trip vector. */
const PINNED_PARTITION = Buffer.from(
  '01000000004cf1238e0100000200000063000000000000000004000000000000640000000000000017000000000000000000000000000000650000000000000003010304020104',
  'hex'
);

const partitionEntry = (id: number, members: number[], inSyncMembers: number[]): Buffer => {
  const counters = Buffer.alloc(64);
  counters.writeUInt32LE(id, 0);
  counters.writeBigUInt64LE(1_710_000_000_000n, 4);
  counters.writeUInt32LE(id, 12);
  counters.writeBigUInt64LE(BigInt(id) * 10n, 16);
  counters.writeBigUInt64LE(BigInt(id) * 100n, 24);
  counters.writeBigUInt64LE(BigInt(id) * 11n, 32);
  counters.writeBigUInt64LE(BigInt(id) + 20n, 40);
  counters.writeBigUInt64LE(BigInt(id) + 30n, 48);
  counters.writeBigUInt64LE(BigInt(id) + 40n, 56);
  return Buffer.concat([
    counters,
    Buffer.from([members.length, ...members]),
    Buffer.from([inSyncMembers.length, ...inSyncMembers])
  ]);
};

const topicHeader = (partitionsCount: number): Buffer => {
  const fixed = Buffer.alloc(34);
  fixed.writeUInt32LE(9, 0);
  fixed.writeUInt32LE(partitionsCount, 12);
  fixed.writeUInt8(1, 32);
  fixed.write('t', 33);
  return Buffer.concat([fixed, Buffer.alloc(8)]);
};

describe('deserializeBaseTopic', () => {
  it('uses the 33-byte fixed layout with settings in two options blocks', () => {
    const fixed = Buffer.alloc(34);
    fixed.writeUInt32LE(7, 0);
    fixed.writeBigUInt64LE(1_710_000_000_000n, 4);
    fixed.writeUInt32LE(3, 12);
    fixed.writeBigUInt64LE(4096n, 16);
    fixed.writeBigUInt64LE(12n, 24);
    fixed.writeUInt8(1, 32);
    fixed.write('t', 33);

    const explicit = serializeOptions([
      { key: 'message_expiry', value: HeaderValue.Uint64(86_400_000_000n) }
    ]);
    const derived = serializeOptions([
      { key: 'compression_algorithm', value: HeaderValue.String('gzip') },
      { key: 'max_topic_size', value: HeaderValue.Uint64(1_000_000n) }
    ]);
    const prefix = (block: Buffer) => {
      const len = Buffer.alloc(4);
      len.writeUInt32LE(block.length, 0);
      return Buffer.concat([len, block]);
    };
    const data = Buffer.concat([fixed, prefix(explicit), prefix(derived)]);

    const { bytesRead, data: topic } = deserializeBaseTopic(data);

    assert.equal(bytesRead, data.length);
    assert.equal(topic.id, 7);
    assert.equal(topic.partitionsCount, 3);
    assert.equal(topic.messageExpiry, 86_400_000_000n);
    assert.equal(topic.compressionAlgorithm, 2);
    assert.equal(topic.maxTopicSize, 1_000_000n);
    assert.equal(topic.sizeBytes, 4096n);
    assert.equal(topic.messagesCount, 12n);
    assert.equal(topic.name, 't');
    assert.deepEqual(topic.options, { message_expiry: 86_400_000_000n });
    assert.deepEqual(topic.derivedOptions, {
      compression_algorithm: 'gzip',
      max_topic_size: 1_000_000n
    });
  });

  it('skips empty options blocks and falls back to the catalog defaults', () => {
    const fixed = Buffer.alloc(34);
    fixed.writeUInt32LE(1, 0);
    fixed.writeUInt32LE(2, 12);
    fixed.writeUInt8(1, 32);
    fixed.write('t', 33);
    const data = Buffer.concat([fixed, Buffer.alloc(8)]);

    const { bytesRead, data: topic } = deserializeBaseTopic(data);

    assert.equal(bytesRead, data.length);
    assert.equal(topic.partitionsCount, 2);
    assert.deepEqual(topic.options, {});
    assert.deepEqual(topic.derivedOptions, {});
    assert.equal(topic.compressionAlgorithm, 1);
    assert.equal(topic.messageExpiry, 0xffff_ffff_ffff_ffffn);
    assert.equal(topic.maxTopicSize, 0xffff_ffff_ffff_ffffn);
  });
});

describe('deserializePartition', () => {
  it('decodes the pinned vector with its member and in-sync lists', () => {
    assert.equal(PINNED_PARTITION.length, 71);

    const { bytesRead, data: partition } = deserializePartition(PINNED_PARTITION);

    assert.equal(bytesRead, 71);
    assert.deepEqual(partition, {
      id: 1,
      createdAt: new Date(1_710_000_000),
      segmentsCount: 2,
      currentOffset: 99n,
      sizeBytes: 1024n,
      messagesCount: 100n,
      context: { incarnation: 23n, ownerGeneration: 0n, metadataOp: 101n },
      members: [1, 3, 4],
      inSyncMembers: [1, 4]
    });
  });

  it('reads two empty node lists as a 66-byte entry', () => {
    const entry = partitionEntry(5, [], []);
    assert.equal(entry.length, 66);

    const { bytesRead, data: partition } = deserializePartition(entry);

    assert.equal(bytesRead, 66);
    assert.equal(partition.id, 5);
    assert.deepEqual(partition.members, []);
    assert.deepEqual(partition.inSyncMembers, []);
  });

  it('decodes from a non-zero position', () => {
    const data = Buffer.concat([Buffer.from([0xaa, 0xbb, 0xcc]), PINNED_PARTITION]);

    const { bytesRead, data: partition } = deserializePartition(data, 3);

    assert.equal(bytesRead, 71);
    assert.deepEqual(partition.members, [1, 3, 4]);
    assert.deepEqual(partition.inSyncMembers, [1, 4]);
  });

  it('rejects an entry cut anywhere before its last byte', () => {
    for (let length = 0; length < PINNED_PARTITION.length; length++)
      assert.throws(
        () => deserializePartition(PINNED_PARTITION.subarray(0, length)),
        DeserializeError,
        `accepted a ${length}-byte prefix`
      );
  });
});

describe('deserializeTopic', () => {
  it('advances by the bytes each partition entry consumed', () => {
    const partitions = [
      partitionEntry(1, [1, 3, 4], [1, 4]),
      partitionEntry(2, [], []),
      partitionEntry(3, [0, 1, 2], [0])
    ];
    assert.deepEqual(partitions.map((entry) => entry.length), [71, 66, 70]);
    const data = Buffer.concat([topicHeader(3), ...partitions]);

    const { bytesRead, data: topic } = deserializeTopic(data);

    assert.equal(bytesRead, data.length);
    assert.equal(topic.partitionsCount, 3);
    assert.deepEqual(
      topic.partitions.map(({ id, segmentsCount, messagesCount, context, members, inSyncMembers }) =>
        ({ id, segmentsCount, messagesCount, context, members, inSyncMembers })),
      [
        {
          id: 1, segmentsCount: 1, messagesCount: 11n,
          context: { incarnation: 21n, ownerGeneration: 31n, metadataOp: 41n },
          members: [1, 3, 4], inSyncMembers: [1, 4]
        },
        {
          id: 2, segmentsCount: 2, messagesCount: 22n,
          context: { incarnation: 22n, ownerGeneration: 32n, metadataOp: 42n },
          members: [], inSyncMembers: []
        },
        {
          id: 3, segmentsCount: 3, messagesCount: 33n,
          context: { incarnation: 23n, ownerGeneration: 33n, metadataOp: 43n },
          members: [0, 1, 2], inSyncMembers: [0]
        }
      ]
    );
  });

  it('rejects a response holding fewer partitions than the header counts', () => {
    const data = Buffer.concat([topicHeader(2), PINNED_PARTITION]);

    assert.throws(() => deserializeTopic(data), DeserializeError);
  });
});
