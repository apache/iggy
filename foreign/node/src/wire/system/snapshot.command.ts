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

export const SnapshotCompression = {
  Stored: 1,
  Deflated: 2,
  Bzip2: 3,
  Zstd: 4,
  Lzma: 5,
  Xz: 6,
} as const;

export type SnapshotCompression =
  typeof SnapshotCompression[keyof typeof SnapshotCompression];

export const SystemSnapshotType = {
  FilesystemOverview: 1,
  ProcessList: 2,
  ResourceUsage: 3,
  Test: 4,
  ServerLogs: 5,
  ServerConfig: 6,
  All: 100,
} as const;

export type SystemSnapshotType =
  typeof SystemSnapshotType[keyof typeof SystemSnapshotType];

export const SNAPSHOT_COMPRESSION = SnapshotCompression;
export const SYSTEM_SNAPSHOT_TYPE = SystemSnapshotType;

export type SnapshotOptions = {
  compression?: SnapshotCompression,
  snapshotTypes?: SystemSnapshotType[]
};

export type Snapshot = SnapshotOptions;

const serializeSnapshot = (payload?: SnapshotOptions | void): Buffer => {
  const compression =
    payload && payload.compression ? payload.compression : SnapshotCompression.Deflated;
  const snapshotTypes =
    payload && payload.snapshotTypes ? payload.snapshotTypes : [SystemSnapshotType.All];

  if (snapshotTypes.length > 255) {
    throw new Error('snapshotTypes count cannot exceed 255');
  }

  if (snapshotTypes.length > 1 && snapshotTypes.includes(SystemSnapshotType.All)) {
    throw new Error('SystemSnapshotType.All cannot be combined with specific snapshot types');
  }

  const buf = Buffer.allocUnsafe(2 + snapshotTypes.length);
  buf.writeUInt8(compression, 0);
  buf.writeUInt8(snapshotTypes.length, 1);
  for (let i = 0; i < snapshotTypes.length; i++) {
    buf.writeUInt8(snapshotTypes[i], 2 + i);
  }
  return buf;
};

export const SNAPSHOT = {
  code: COMMAND_CODE.GetSnapshot,

  serialize: serializeSnapshot,

  deserialize: (r: CommandResponse): Buffer => r.data
};

/**
 * Captures and packages the current system state as a raw ZIP archive.
 *
 * @param options - Optional configuration for snapshot compression and section types.
 * Defaults to Deflated compression and `SystemSnapshotType.All` (code 100), capturing all
 * available diagnostic sections (filesystem overview, process list, resource usage, server
 * logs, and server configuration) for complete debugging.
 * @returns A Buffer containing the raw ZIP archive bytes.
 *
 * @remarks
 * - Authentication is required, and the caller must have administrative permissions.
 * - The server enforces a single-flight mutex guard: only one snapshot can be collected
 *   at a time across the cluster.
 * - Subject to the client's default 30-second socket timeout budget.
 */
export const snapshot = wrapCommand<SnapshotOptions | void, Buffer>(SNAPSHOT);
