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
import {
  isInternal,
  isKnownOperation,
  isMetadata,
  isResultFramed,
  Operation,
  operationForCode,
  requestOperation
} from './operation.js';

const replicated = new Map<number, number>([
  [COMMAND_CODE.CreateStream, Operation.CreateStream],
  [COMMAND_CODE.DeleteStream, Operation.DeleteStream],
  [COMMAND_CODE.CreateTopic, Operation.CreateTopic],
  [COMMAND_CODE.DeleteTopic, Operation.DeleteTopic],
  [COMMAND_CODE.CreatePartitions, Operation.CreatePartitions],
  [COMMAND_CODE.DeletePartitions, Operation.DeletePartitions],
  [COMMAND_CODE.DeleteSegments, Operation.DeleteSegments],
  [COMMAND_CODE.CreateGroup, Operation.CreateConsumerGroup],
  [COMMAND_CODE.DeleteGroup, Operation.DeleteConsumerGroup],
  [COMMAND_CODE.CreateUser, Operation.CreateUser],
  [COMMAND_CODE.DeleteUser, Operation.DeleteUser],
  [COMMAND_CODE.ChangePassword, Operation.ChangePassword],
  [COMMAND_CODE.UpdatePermissions, Operation.UpdatePermissions],
  [COMMAND_CODE.CreateAccessToken, Operation.CreatePersonalAccessToken],
  [COMMAND_CODE.DeleteAccessToken, Operation.DeletePersonalAccessToken],
  [COMMAND_CODE.JoinGroup, Operation.JoinConsumerGroup],
  [COMMAND_CODE.LeaveGroup, Operation.LeaveConsumerGroup],
  [COMMAND_CODE.UpdateResource, Operation.UpdateResource],
  [COMMAND_CODE.SendMessages, Operation.SendMessages],
  [COMMAND_CODE.StoreOffset, Operation.StoreConsumerOffset],
  [COMMAND_CODE.DeleteConsumerOffset, Operation.DeleteConsumerOffset]
]);

describe('VSR operation classification', () => {
  it('matches every replicated command operation', () => {
    for (const [code, operation] of replicated)
      assert.equal(operationForCode(code), operation);
  });

  it('classifies every other known command as non-replicated', () => {
    for (const code of Object.values(COMMAND_CODE)) {
      if (replicated.has(code) || code === COMMAND_CODE.LogoutUser)
        continue;
      assert.equal(operationForCode(code), Operation.NonReplicated);
    }
  });

  it('forwards unknown extension codes as non-replicated', () => {
    assert.equal(operationForCode(60_001), Operation.NonReplicated);
    assert.equal(operationForCode(1_000_104), Operation.NonReplicated);
  });

  it('routes logout through its consensus operation', () => {
    assert.equal(operationForCode(COMMAND_CODE.LogoutUser), Operation.Logout);
  });

  it('pins operation class predicates', () => {
    assert.equal(isInternal(64), true);
    assert.equal(isInternal(63), false);
    assert.equal(isInternal(Operation.CreateStream), false);
    assert.equal(isInternal(Operation.RetireSession), true);
    assert.equal(isMetadata(Operation.RetireSession), false);
    assert.equal(isResultFramed(Operation.RetireSession), false);
    assert.equal(isMetadata(Operation.CreateStream), true);
    assert.equal(isMetadata(Operation.LeaveConsumerGroup), true);
    assert.equal(isMetadata(Operation.DeleteSegments), false);
    assert.equal(isMetadata(Operation.UpdateResource), true);
    assert.equal(isResultFramed(Operation.UpdateResource), true);
    for (const resolved of [Operation.UpdateStreamResolved, Operation.UpdateTopicResolved,
      Operation.UpdateUserResolved, Operation.RewrapStreamKeys]) {
      assert.equal(isInternal(resolved), true);
      assert.equal(isMetadata(resolved), true);
      assert.equal(isResultFramed(resolved), true);
    }
    assert.equal(isMetadata(151), false);
    assert.equal(isResultFramed(Operation.StoreConsumerOffset), true);
    assert.equal(isResultFramed(Operation.DeleteConsumerOffset), true);
    assert.equal(isResultFramed(Operation.SendMessages), false);
  });

  it('maps a resolved committed operation back to the request operation', () => {
    for (const [committed, request] of [
      [Operation.CreateTopicWithAssignments, Operation.CreateTopic],
      [Operation.CreatePartitionsWithAssignments, Operation.CreatePartitions],
      [Operation.TruncatePartition, Operation.DeleteSegments],
      [Operation.UpdateStreamResolved, Operation.UpdateResource],
      [Operation.UpdateTopicResolved, Operation.UpdateResource],
      [Operation.UpdateUserResolved, Operation.UpdateResource],
      [Operation.RewrapStreamKeys, Operation.RewrapStreamKeys],
      [Operation.CreateStream, Operation.CreateStream]
    ])
      assert.equal(requestOperation(committed), request);
  });

  it('recognizes only declared operation discriminants', () => {
    assert.equal(isKnownOperation(Operation.Register), true);
    assert.equal(isKnownOperation(Operation.SendMessages), true);
    assert.equal(isKnownOperation(Operation.RetireSession), true);
    assert.equal(isKnownOperation(Operation.FinalizeSession), true);
    assert.equal(isKnownOperation(Operation.RetireConsumerGroupOwners), true);
    assert.equal(isInternal(Operation.RetireConsumerGroupOwners), true);
    for (const undeclared of [69, 76, 127, 129, 131, 133, 135, 142, 151, 159, 163, 164, 165, 170, 255])
      assert.equal(isKnownOperation(undeclared), false);
  });
});
