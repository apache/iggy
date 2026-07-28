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
import { BINARY_REQUEST_KIND } from '../command-set.js';
import { COMMAND_CODE } from '../command.code.js';
import { ResponseError } from '../error.utils.js';
import {
  isInternal,
  isKnownOperation,
  isMetadata,
  isPartition,
  isResultFramed,
  Operation,
  operationForCode
} from './operation.js';

const replicated = new Map<number, number>([
  [COMMAND_CODE.CreateStream, Operation.CreateStream],
  [COMMAND_CODE.UpdateStream, Operation.UpdateStream],
  [COMMAND_CODE.DeleteStream, Operation.DeleteStream],
  [COMMAND_CODE.PurgeStream, Operation.PurgeStream],
  [COMMAND_CODE.CreateTopic, Operation.CreateTopic],
  [COMMAND_CODE.UpdateTopic, Operation.UpdateTopic],
  [COMMAND_CODE.DeleteTopic, Operation.DeleteTopic],
  [COMMAND_CODE.PurgeTopic, Operation.PurgeTopic],
  [COMMAND_CODE.CreatePartitions, Operation.CreatePartitions],
  [COMMAND_CODE.DeletePartitions, Operation.DeletePartitions],
  [COMMAND_CODE.DeleteSegments, Operation.DeleteSegments],
  [COMMAND_CODE.CreateGroup, Operation.CreateConsumerGroup],
  [COMMAND_CODE.DeleteGroup, Operation.DeleteConsumerGroup],
  [COMMAND_CODE.CreateUser, Operation.CreateUser],
  [COMMAND_CODE.UpdateUser, Operation.UpdateUser],
  [COMMAND_CODE.DeleteUser, Operation.DeleteUser],
  [COMMAND_CODE.ChangePassword, Operation.ChangePassword],
  [COMMAND_CODE.UpdatePermissions, Operation.UpdatePermissions],
  [COMMAND_CODE.CreateAccessToken, Operation.CreatePersonalAccessToken],
  [COMMAND_CODE.DeleteAccessToken, Operation.DeletePersonalAccessToken],
  [COMMAND_CODE.JoinGroup, Operation.JoinConsumerGroup],
  [COMMAND_CODE.LeaveGroup, Operation.LeaveConsumerGroup],
  [COMMAND_CODE.SendMessages, Operation.SendMessages],
  [COMMAND_CODE.StoreOffset, Operation.StoreConsumerOffset],
  [COMMAND_CODE.DeleteConsumerOffset, Operation.DeleteConsumerOffset],
  [COMMAND_CODE.StoreOffset2, Operation.StoreConsumerOffset2],
  [COMMAND_CODE.DeleteConsumerOffset2, Operation.DeleteConsumerOffset2]
]);

describe('VSR operation classification', () => {
  it('matches every replicated command operation', () => {
    for (const [code, operation] of replicated) {
      assert.equal(operationForCode(code), operation);
      assert.equal(
        operationForCode(code, BINARY_REQUEST_KIND.Replicated),
        operation
      );
    }
  });

  it('classifies every other known command as non-replicated', () => {
    for (const code of Object.values(COMMAND_CODE)) {
      if (replicated.has(code) || code === COMMAND_CODE.LogoutUser)
        continue;
      assert.equal(operationForCode(code), Operation.NonReplicated);
    }
  });

  it('keeps standard command tables authoritative', () => {
    assert.throws(
      () => operationForCode(
        COMMAND_CODE.GetStats,
        BINARY_REQUEST_KIND.Replicated
      ),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 3
    );
    assert.throws(
      () => operationForCode(
        COMMAND_CODE.CreateStream,
        BINARY_REQUEST_KIND.NonReplicated
      ),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 3
    );
  });

  it('forwards only unknown non-replicated extensions', () => {
    assert.equal(
      operationForCode(60_001, BINARY_REQUEST_KIND.NonReplicated),
      Operation.NonReplicated
    );
    assert.throws(
      () => operationForCode(60_001, BINARY_REQUEST_KIND.Replicated),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 5
    );
    assert.throws(
      () => operationForCode(60_001),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 3
    );
  });

  it('routes logout through its consensus operation', () => {
    assert.equal(operationForCode(COMMAND_CODE.LogoutUser), Operation.Logout);
    assert.equal(
      operationForCode(
        COMMAND_CODE.LogoutUser,
        BINARY_REQUEST_KIND.Replicated
      ),
      Operation.Logout
    );
    assert.throws(
      () => operationForCode(
        COMMAND_CODE.LogoutUser,
        BINARY_REQUEST_KIND.NonReplicated
      ),
      (error: unknown) =>
        error instanceof ResponseError && error.errorCode === 3
    );
  });

  it('pins operation class predicates', () => {
    assert.equal(isInternal(64), true);
    assert.equal(isInternal(63), false);
    assert.equal(isInternal(Operation.CreateStream), false);
    assert.equal(isMetadata(Operation.CreateStream), true);
    assert.equal(isMetadata(Operation.LeaveConsumerGroup), true);
    assert.equal(isMetadata(Operation.DeleteSegments), false);
    assert.equal(isMetadata(150), false);
    assert.equal(isPartition(Operation.SendMessages), true);
    assert.equal(isPartition(159), false);
    assert.equal(isResultFramed(Operation.StoreConsumerOffset), true);
    assert.equal(isResultFramed(Operation.StoreConsumerOffset2), true);
    assert.equal(isResultFramed(Operation.DeleteConsumerOffset), true);
    assert.equal(isResultFramed(Operation.DeleteConsumerOffset2), true);
    assert.equal(isResultFramed(Operation.SendMessages), false);
  });

  it('recognizes only declared operation discriminants', () => {
    assert.equal(isKnownOperation(Operation.Register), true);
    assert.equal(isKnownOperation(Operation.SendMessages), true);
    for (const undeclared of [69, 127, 150, 159, 163, 166, 255])
      assert.equal(isKnownOperation(undeclared), false);
  });
});
