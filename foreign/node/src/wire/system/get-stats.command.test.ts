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
import { GET_STATS } from './get-stats.command.js';
import { DeserializeError } from '../error.utils.js';

const u32 = (value: number) => {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(value);
  return b;
};

const u64 = (value: bigint) => {
  const b = Buffer.alloc(8);
  b.writeBigUInt64LE(value);
  return b;
};

const f32 = (value: number) => {
  const b = Buffer.alloc(4);
  b.writeFloatLE(value);
  return b;
};

const str = (value: string) =>
  Buffer.concat([u32(Buffer.byteLength(value)), Buffer.from(value)]);

const cacheMetric = (streamId: number, topicId: number, partitionId: number) =>
  Buffer.concat([
    u32(streamId), u32(topicId), u32(partitionId),
    u64(1000n), u64(50n), f32(0.95)
  ]);

const buildStatsPayload = (openFiles?: { count: bigint, limit: bigint }) =>
  Buffer.concat([
    u32(1234), f32(25.5), f32(50),
    u64(1_073_741_824n), u64(8_589_934_592n), u64(4_294_967_296n),
    u64(3600n), u64(1_710_000_000_000n), u64(1_000_000n), u64(500_000n),
    u64(2_000_000n),
    u32(3), u32(10), u32(30), u32(90), u64(50_000n), u32(5), u32(2),
    str('node-1'), str('Linux'), str('6.1'), str('6.1.0'), str('0.6.0'),
    u32(600),
    u32(2), cacheMetric(1, 1, 0), cacheMetric(2, 3, 1),
    u32(16), u64(107_374_182_400n), u64(512_110_190_592n),
    ...(openFiles ? [u64(openFiles.count), u64(openFiles.limit)] : [])
  ]);

const deserialize = (data: Buffer) =>
  GET_STATS.deserialize({ status: 0, length: data.length, data });

describe('GetStats Command', () => {

  it('deserializes open files count and limit', () => {
    const stats = deserialize(
      buildStatsPayload({ count: 1_234n, limit: 1_048_576n })
    );

    assert.equal(stats.processId, 1234);
    assert.equal(stats.kernelVersion, '6.1.0');
    assert.equal(stats.openFilesCount, 1_234n);
    assert.equal(stats.openFilesLimit, 1_048_576n);
  });

  it('deserializes server version, cache metrics, threads and disk space', () => {
    const stats = deserialize(buildStatsPayload());

    assert.equal(stats.iggyServerVersion, '0.6.0');
    assert.equal(stats.iggyServerSemver, 600);
    assert.deepEqual(stats.cacheMetrics, [
      {
        streamId: 1, topicId: 1, partitionId: 0,
        hits: 1000n, misses: 50n, hitRatio: Math.fround(0.95)
      },
      {
        streamId: 2, topicId: 3, partitionId: 1,
        hits: 1000n, misses: 50n, hitRatio: Math.fround(0.95)
      }
    ]);
    assert.equal(stats.threadsCount, 16);
    assert.equal(stats.freeDiskSpace, 107_374_182_400n);
    assert.equal(stats.totalDiskSpace, 512_110_190_592n);
  });

  it('reads open files fields as 0 when the reply ends at total_disk_space', () => {
    const stats = deserialize(buildStatsPayload());

    assert.equal(stats.kernelVersion, '6.1.0');
    assert.equal(stats.openFilesCount, 0n);
    assert.equal(stats.openFilesLimit, 0n);
  });

  it('throws on truncated open files fields', () => {
    const payload = buildStatsPayload({ count: 1_234n, limit: 1_048_576n });
    for (let cut = 1; cut < 16; cut += 1)
      assert.throws(
        () => deserialize(payload.subarray(0, payload.length - cut)),
        DeserializeError
      );
  });

  it('throws on a reply truncated before total_disk_space ends', () => {
    const payload = buildStatsPayload();
    for (let length = 0; length < payload.length; length += 1)
      assert.throws(
        () => deserialize(payload.subarray(0, length)),
        DeserializeError,
        `a reply cut to ${length} bytes must not decode`
      );
  });

});
