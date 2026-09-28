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
  openFilesCount: bigint,
  openFilesLimit: bigint
}

// stream_id, topic_id, partition_id, hits, misses, hit_ratio
const CACHE_METRIC_SIZE = 4 + 4 + 4 + 8 + 8 + 4;
// threads_count, free_disk_space, total_disk_space
const THREADS_AND_DISK_SIZE = 4 + 8 + 8;
// open_files_count, open_files_limit
const OPEN_FILES_SIZE = 8 + 8;

const deserializeGetStats = (b: Buffer) => {
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

  let position = 104 + 4;
  const hostnameLength = b.readUInt32LE(position);
  const hostname = b.subarray(
    position + 4,
    position + 4 + hostnameLength
  ).toString();
  position += 4 + hostnameLength;

  const osNameLength = b.readUInt32LE(position);
  const osName = b.subarray(
    position + 4,
    position + 4 + osNameLength
  ).toString();
  position += 4 + osNameLength;

  const osVersionLength = b.readUInt32LE(position);
  const osVersion = b.subarray(
    position + 4,
    position + 4 + osVersionLength
  ).toString();
  position += 4 + osVersionLength;

  const kernelVersionLength = b.readUInt32LE(position);
  const kernelVersion = b.subarray(
    position + 4,
    position + 4 + kernelVersionLength
  ).toString();
  position += 4 + kernelVersionLength;

  // iggy_server_version, iggy_server_semver
  const iggyServerVersionLength = b.readUInt32LE(position);
  position += 4 + iggyServerVersionLength + 4;

  const cacheMetricsCount = b.readUInt32LE(position);
  position += 4 + cacheMetricsCount * CACHE_METRIC_SIZE + THREADS_AND_DISK_SIZE;
  if (position > b.length)
    deserializeError('stats', position, b.length);

  // Servers that predate the open-files fields end the reply here.
  let openFilesCount = 0n;
  let openFilesLimit = 0n;
  if (b.length > position) {
    if (b.length < position + OPEN_FILES_SIZE)
      deserializeError('stats', position + OPEN_FILES_SIZE, b.length);
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
    hostname,
    osName,
    osVersion,
    kernelVersion,
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
