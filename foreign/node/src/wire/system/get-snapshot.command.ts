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

/** Compression method applied by the server to the snapshot archive. */
export const SnapshotCompression = {
  Stored: 1,
  Deflated: 2,
  Bzip2: 3,
  Zstd: 4,
  Lzma: 5,
  Xz: 6
} as const;

export type SnapshotCompression =
  typeof SnapshotCompression[keyof typeof SnapshotCompression];

/** Diagnostic sections the server can include in the snapshot archive. */
export const SnapshotType = {
  FilesystemOverview: 1,
  ProcessList: 2,
  ResourceUsage: 3,
  Test: 4,
  ServerLogs: 5,
  ServerConfig: 6,
  All: 100
} as const;

export type SnapshotType =
  typeof SnapshotType[keyof typeof SnapshotType];

export type GetSnapshot = {
  compression: SnapshotCompression,
  snapshotTypes: SnapshotType[]
};

/**
 * The raw snapshot payload: a complete ZIP archive with no additional
 * framing, exactly as the server produces it (see the GetSnapshot wire
 * spec in core/binary_protocol).
 */
export type Snapshot = Buffer;

const serializeGetSnapshot = ({
  compression,
  snapshotTypes
}: GetSnapshot) => {
  if (snapshotTypes.length > 255)
    throw new Error('GetSnapshot accepts at most 255 snapshot types');
  return Buffer.concat([
    Buffer.from([compression, snapshotTypes.length]),
    Buffer.from(snapshotTypes)
  ]);
};

export const GET_SNAPSHOT = {
  code: COMMAND_CODE.GetSnapshot,

  serialize: (args: GetSnapshot) => serializeGetSnapshot(args),

  deserialize: (r: CommandResponse): Snapshot =>
    Buffer.from(r.data)
};

export const getSnapshot = wrapCommand<GetSnapshot, Snapshot>(GET_SNAPSHOT);
