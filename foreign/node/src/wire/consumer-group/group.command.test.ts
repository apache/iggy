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

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { COMMAND_CODE } from '../command.code.js';
import { DELETE_GROUP } from './delete-group.command.js';
import { JOIN_GROUP } from './join-group.command.js';
import { LEAVE_GROUP } from './leave-group.command.js';

const numericTargetGroup = {
  streamId: 5,
  topicId: 2,
  groupId: 1,
};

const numericTargetGroupBytes = Buffer.from([
  // streamId: numeric (kind 1, length 4, value 5 LE)
  1, 4, 5, 0, 0, 0,
  // topicId: numeric (kind 1, length 4, value 2 LE)
  1, 4, 2, 0, 0, 0,
  // groupId: numeric (kind 1, length 4, value 1 LE)
  1, 4, 1, 0, 0, 0,
]);

const stringTargetGroup = {
  streamId: 'stream',
  topicId: 'topic',
  groupId: 'group',
};

const stringTargetGroupBytes = Buffer.from([
  // streamId: string (kind 2, length 6, bytes of "stream")
  2, 6, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d,
  // topicId: string (kind 2, length 5, bytes of "topic")
  2, 5, 0x74, 0x6f, 0x70, 0x69, 0x63,
  // groupId: string (kind 2, length 5, bytes of "group")
  2, 5, 0x67, 0x72, 0x6f, 0x75, 0x70,
]);

describe('DeleteGroup', () => {
  describe('serialize', () => {
    it('serializes numeric stream, topic, and group identifiers into exact bytes', () => {
      assert.deepEqual(DELETE_GROUP.serialize(numericTargetGroup), numericTargetGroupBytes);
    });

    it('serializes string stream, topic, and group identifiers into exact bytes', () => {
      assert.deepEqual(DELETE_GROUP.serialize(stringTargetGroup), stringTargetGroupBytes);
    });
  });

  it('uses the DeleteGroup command code', () => {
    assert.equal(DELETE_GROUP.code, COMMAND_CODE.DeleteGroup);
  });
});

describe('JoinGroup', () => {
  describe('serialize', () => {
    it('serializes numeric stream, topic, and group identifiers into exact bytes', () => {
      assert.deepEqual(JOIN_GROUP.serialize(numericTargetGroup), numericTargetGroupBytes);
    });

    it('serializes string stream, topic, and group identifiers into exact bytes', () => {
      assert.deepEqual(JOIN_GROUP.serialize(stringTargetGroup), stringTargetGroupBytes);
    });
  });

  it('uses the JoinGroup command code', () => {
    assert.equal(JOIN_GROUP.code, COMMAND_CODE.JoinGroup);
  });
});

describe('LeaveGroup', () => {
  describe('serialize', () => {
    it('serializes numeric stream, topic, and group identifiers into exact bytes', () => {
      assert.deepEqual(LEAVE_GROUP.serialize(numericTargetGroup), numericTargetGroupBytes);
    });

    it('serializes string stream, topic, and group identifiers into exact bytes', () => {
      assert.deepEqual(LEAVE_GROUP.serialize(stringTargetGroup), stringTargetGroupBytes);
    });
  });

  it('uses the LeaveGroup command code', () => {
    assert.equal(LEAVE_GROUP.code, COMMAND_CODE.LeaveGroup);
  });
});
