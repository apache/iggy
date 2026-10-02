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
import { COMMAND_CODE } from '../command.code.js';
import {
  SNAPSHOT,
  SnapshotCompression,
  SystemSnapshotType,
  SNAPSHOT_COMPRESSION,
  SYSTEM_SNAPSHOT_TYPE
} from './snapshot.command.js';

describe('SnapshotCommand', () => {
  it('has correct command code', () => {
    assert.equal(SNAPSHOT.code, COMMAND_CODE.GetSnapshot);
    assert.equal(SNAPSHOT.code, 11);
  });

  describe('serialize', () => {
    it('serializes default options (Deflated compression and All snapshot type)', () => {
      const buf = SNAPSHOT.serialize();
      assert.equal(buf.length, 3);
      assert.equal(buf.readUInt8(0), SnapshotCompression.Deflated);
      assert.equal(buf.readUInt8(1), 1);
      assert.equal(buf.readUInt8(2), SystemSnapshotType.All);
    });

    it('serializes custom compression and snapshot types', () => {
      const buf = SNAPSHOT.serialize({
        compression: SnapshotCompression.Stored,
        snapshotTypes: [
          SystemSnapshotType.FilesystemOverview,
          SystemSnapshotType.ServerLogs
        ]
      });
      assert.equal(buf.length, 4);
      assert.equal(buf.readUInt8(0), SnapshotCompression.Stored);
      assert.equal(buf.readUInt8(1), 2);
      assert.equal(buf.readUInt8(2), SystemSnapshotType.FilesystemOverview);
      assert.equal(buf.readUInt8(3), SystemSnapshotType.ServerLogs);
    });

    it('serializes empty snapshot types', () => {
      const buf = SNAPSHOT.serialize({
        compression: SnapshotCompression.Bzip2,
        snapshotTypes: []
      });
      assert.equal(buf.length, 2);
      assert.equal(buf.readUInt8(0), SnapshotCompression.Bzip2);
      assert.equal(buf.readUInt8(1), 0);
    });

    it('throws when snapshotTypes count exceeds 255', () => {
      const oversizedTypes = new Array(256).fill(SystemSnapshotType.Test);
      assert.throws(
        () => SNAPSHOT.serialize({ snapshotTypes: oversizedTypes }),
        /snapshotTypes count cannot exceed 255/
      );
    });

    it('throws when SystemSnapshotType.All is mixed with other types', () => {
      assert.throws(
        () =>
          SNAPSHOT.serialize({
            snapshotTypes: [
              SystemSnapshotType.All,
              SystemSnapshotType.ProcessList
            ]
          }),
        /SystemSnapshotType\.All cannot be combined with specific snapshot types/
      );
    });

    it('supports backwards-compatible enum aliases', () => {
      assert.equal(SNAPSHOT_COMPRESSION.Deflated, SnapshotCompression.Deflated);
      assert.equal(SYSTEM_SNAPSHOT_TYPE.All, SystemSnapshotType.All);
    });
  });

  describe('deserialize', () => {
    it('returns raw payload buffer', () => {
      const rawData = Buffer.from([0x50, 0x4B, 0x03, 0x04, 1, 2, 3]);
      const res = SNAPSHOT.deserialize({
        status: 0,
        length: rawData.length,
        data: rawData
      });
      assert.deepEqual(res, rawData);
    });
  });
});
