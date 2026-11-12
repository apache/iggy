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
import { deserializeError } from '../error.utils.js';

export type CacheMetrics = {
  streamId: number,
  topicId: number,
  partitionId: number,
  hits: bigint,
  misses: bigint,
  hitRatio: number
}

export type Stats = {
  processId: number,
  cpuUsage: number,
  totalCpuUsage: number,
  memoryUsage: bigint,
  totalMemory: bigint,
  availableMemory: bigint,
  runTime: bigint,
  startTime: bigint,
  readBytes: bigint,
  writtenBytes: bigint,
  messagesSizeBytes: bigint,
  streamsCount: number,
  topicsCount: number,
  partitionsCount: number,
  segmentsCount: number,
  messagesCount: bigint,
  clientsCount: number,
  consumersGroupsCount: number,
  hostname: string,
  osName: string,
  osVersion: string,
  kernelVersion: string,
  iggyServerVersion: string,
  /** 0 when the server does not report it. */
  iggyServerSemver: number,
  cacheMetrics: CacheMetrics[],
  threadsCount: number,
  freeDiskSpace: bigint,
  totalDiskSpace: bigint,
  openFilesCount: bigint,
  openFilesLimit: bigint
}

// process_id through consumer_groups_count
const FIXED_HEAD_SIZE = 108;
// stream_id, topic_id, partition_id, hits, misses, hit_ratio
const CACHE_METRIC_SIZE = 4 + 4 + 4 + 8 + 8 + 4;
// threads_count, free_disk_space, total_disk_space
const THREADS_AND_DISK_SIZE = 4 + 8 + 8;
// open_files_count, open_files_limit
const OPEN_FILES_SIZE = 8 + 8;

const ensureLength = (b: Buffer, end: number) => {
  if (end > b.length)
    deserializeError('stats', end, b.length);
};

/** A u32 length, then that many bytes of text. */
const deserializeString = (b: Buffer, position: number) => {
  ensureLength(b, position + 4);
  const end = position + 4 + b.readUInt32LE(position);
  ensureLength(b, end);
  return { value: b.subarray(position + 4, end).toString(), end };
};

const deserializeCacheMetric = (b: Buffer, position: number): CacheMetrics => ({
  streamId: b.readUInt32LE(position),
  topicId: b.readUInt32LE(position + 4),
  partitionId: b.readUInt32LE(position + 8),
  hits: b.readBigUInt64LE(position + 12),
  misses: b.readBigUInt64LE(position + 20),
  hitRatio: b.readFloatLE(position + 28)
});

const deserializeGetStats = (b: Buffer) => {
  ensureLength(b, FIXED_HEAD_SIZE);
  const processId = b.readUInt32LE(0);
  const cpuUsage = b.readFloatLE(4);
  const totalCpuUsage = b.readFloatLE(8);
  const memoryUsage = b.readBigUInt64LE(12);
  const totalMemory = b.readBigUInt64LE(20);
  const availableMemory = b.readBigUInt64LE(28);
  const runTime = b.readBigUInt64LE(36);
  const startTime = b.readBigUInt64LE(44);
  const readBytes = b.readBigUInt64LE(52);
  const writtenBytes = b.readBigUInt64LE(60);
  const messagesSizeBytes = b.readBigUInt64LE(68);
  const streamsCount = b.readUInt32LE(76);
  const topicsCount = b.readUInt32LE(80);
  const partitionsCount = b.readUInt32LE(84);
  const segmentsCount = b.readUInt32LE(88);
  const messagesCount = b.readBigUInt64LE(92);
  const clientsCount = b.readUInt32LE(100);
  const consumersGroupsCount = b.readUInt32LE(104);

  const hostname = deserializeString(b, FIXED_HEAD_SIZE);
  const osName = deserializeString(b, hostname.end);
  const osVersion = deserializeString(b, osName.end);
  const kernelVersion = deserializeString(b, osVersion.end);
  const iggyServerVersion = deserializeString(b, kernelVersion.end);
  let position = iggyServerVersion.end;

  ensureLength(b, position + 4 + 4);
  const iggyServerSemver = b.readUInt32LE(position);
  const cacheMetricsCount = b.readUInt32LE(position + 4);
  position += 4 + 4;
  ensureLength(b, position + cacheMetricsCount * CACHE_METRIC_SIZE + THREADS_AND_DISK_SIZE);
  const cacheMetrics: CacheMetrics[] = [];
  for (let index = 0; index < cacheMetricsCount; index += 1) {
    cacheMetrics.push(deserializeCacheMetric(b, position));
    position += CACHE_METRIC_SIZE;
  }

  const threadsCount = b.readUInt32LE(position);
  const freeDiskSpace = b.readBigUInt64LE(position + 4);
  const totalDiskSpace = b.readBigUInt64LE(position + 12);
  position += THREADS_AND_DISK_SIZE;

  // Servers that predate the open-files fields end the reply here.
  let openFilesCount = 0n;
  let openFilesLimit = 0n;
  if (b.length > position) {
    ensureLength(b, position + OPEN_FILES_SIZE);
    openFilesCount = b.readBigUInt64LE(position);
    openFilesLimit = b.readBigUInt64LE(position + 8);
  }

  return {
    processId,
    cpuUsage,
    totalCpuUsage,
    memoryUsage,
    totalMemory,
    availableMemory,
    runTime,
    startTime,
    readBytes,
    writtenBytes,
    messagesSizeBytes,
    streamsCount,
    topicsCount,
    partitionsCount,
    segmentsCount,
    messagesCount,
    clientsCount,
    consumersGroupsCount,
    hostname: hostname.value,
    osName: osName.value,
    osVersion: osVersion.value,
    kernelVersion: kernelVersion.value,
    iggyServerVersion: iggyServerVersion.value,
    iggyServerSemver,
    cacheMetrics,
    threadsCount,
    freeDiskSpace,
    totalDiskSpace,
    openFilesCount,
    openFilesLimit
  };
};

export const GET_STATS = {
  code: COMMAND_CODE.GetStats,

  serialize: () => Buffer.alloc(0),

  deserialize: (r: CommandResponse): Stats => deserializeGetStats(r.data)
};


export const getStats = wrapCommand<void, Stats>(GET_STATS);
