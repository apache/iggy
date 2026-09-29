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

export const SNAPSHOT_COMPRESSION = {
  Stored: 1,
  Deflated: 2,
  Bzip2: 3,
  Zstd: 4,
  Lzma: 5,
  Xz: 6,
} as const;

export type SnapshotCompression =
  typeof SNAPSHOT_COMPRESSION[keyof typeof SNAPSHOT_COMPRESSION];

export const SYSTEM_SNAPSHOT_TYPE = {
  FilesystemOverview: 1,
  ProcessList: 2,
  ResourceUsage: 3,
  Test: 4,
  ServerLogs: 5,
  ServerConfig: 6,
  All: 100,
} as const;

export type SystemSnapshotType =
  typeof SYSTEM_SNAPSHOT_TYPE[keyof typeof SYSTEM_SNAPSHOT_TYPE];

export type Snapshot = {
  compression?: SnapshotCompression,
  snapshotTypes?: SystemSnapshotType[]
};

const serializeSnapshot = (payload?: Snapshot | void): Buffer => {
  const compression =
    payload && payload.compression ? payload.compression : SNAPSHOT_COMPRESSION.Deflated;
  const snapshotTypes =
    payload && payload.snapshotTypes ? payload.snapshotTypes : [SYSTEM_SNAPSHOT_TYPE.All];

  if (snapshotTypes.length > 255) {
    throw new Error('snapshotTypes count cannot exceed 255');
  }

  const buf = Buffer.alloc(2 + snapshotTypes.length);
  buf.writeUInt8(compression, 0);
  buf.writeUInt8(snapshotTypes.length, 1);
  for (let i = 0; i < snapshotTypes.length; i++) {
    buf.writeUInt8(snapshotTypes[i], 2 + i);
  }
  return buf;
};

export const SNAPSHOT = {
  code: COMMAND_CODE.GetSnapshot,

  serialize: (payload?: Snapshot | void): Buffer => serializeSnapshot(payload),

  deserialize: (r: CommandResponse): Buffer => r.data
};

export const snapshot = wrapCommand<Snapshot | void, Buffer>(SNAPSHOT);
