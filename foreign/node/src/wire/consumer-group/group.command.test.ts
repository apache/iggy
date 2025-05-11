/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DELETE_GROUP } from './delete-group.command.js';
import { JOIN_GROUP } from './join-group.command.js';
import { LEAVE_GROUP } from './leave-group.command.js';


const s1 = {
  streamId: 5,
  topicId: 2,
  groupId: 1,
};

describe('DeleteGroup', () => {
  describe('serialize', () => {
    
    it('serialize target group id into buffer', () => {
      assert.deepEqual(
        DELETE_GROUP.serialize(s1).length,
        6 + 6 + 6
      );
    });

  });
});

describe('JoinGroup', () => {
  describe('serialize', () => {
    
    it('serialize target group id into buffer', () => {
      assert.deepEqual(
        JOIN_GROUP.serialize(s1).length,
        6 + 6 + 6
      );
    });

  });
});

describe('LeaveGroup', () => {
  describe('serialize', () => {
    
    it('serialize target group id into buffer', () => {
      assert.deepEqual(
        LEAVE_GROUP.serialize(s1).length,
        6 + 6 + 6
      );
    });

  });
});
