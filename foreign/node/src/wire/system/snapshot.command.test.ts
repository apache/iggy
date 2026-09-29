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
  SNAPSHOT_COMPRESSION,
  SYSTEM_SNAPSHOT_TYPE,
  type SystemSnapshotType
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
      assert.equal(buf.readUInt8(0), SNAPSHOT_COMPRESSION.Deflated);
      assert.equal(buf.readUInt8(1), 1);
      assert.equal(buf.readUInt8(2), SYSTEM_SNAPSHOT_TYPE.All);
    });

    it('serializes custom compression and snapshot types', () => {
      const buf = SNAPSHOT.serialize({
        compression: SNAPSHOT_COMPRESSION.Stored,
        snapshotTypes: [
          SYSTEM_SNAPSHOT_TYPE.FilesystemOverview,
          SYSTEM_SNAPSHOT_TYPE.ServerLogs
        ]
      });
      assert.equal(buf.length, 4);
      assert.equal(buf.readUInt8(0), SNAPSHOT_COMPRESSION.Stored);
      assert.equal(buf.readUInt8(1), 2);
      assert.equal(buf.readUInt8(2), SYSTEM_SNAPSHOT_TYPE.FilesystemOverview);
      assert.equal(buf.readUInt8(3), SYSTEM_SNAPSHOT_TYPE.ServerLogs);
    });

    it('serializes empty snapshot types', () => {
      const buf = SNAPSHOT.serialize({
        compression: SNAPSHOT_COMPRESSION.Bzip2,
        snapshotTypes: []
      });
      assert.equal(buf.length, 2);
      assert.equal(buf.readUInt8(0), SNAPSHOT_COMPRESSION.Bzip2);
      assert.equal(buf.readUInt8(1), 0);
    });

    it('throws when snapshotTypes count exceeds 255', () => {
      const oversizedTypes: SystemSnapshotType[] = new Array(256).fill(
        SYSTEM_SNAPSHOT_TYPE.Test
      );
      assert.throws(
        () => SNAPSHOT.serialize({ snapshotTypes: oversizedTypes }),
        /snapshotTypes count cannot exceed 255/
      );
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
