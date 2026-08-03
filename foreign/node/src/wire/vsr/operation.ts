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
//

/**
 * `Operation` values and classification, ported from
 * `core/binary_protocol/src/consensus/operation.rs` and the command table in
 * `core/binary_protocol/src/dispatch.rs`.
 */

import { COMMAND_CODE } from '../command.code.js';

/** `Operation` discriminants a client sends or receives. */
export const Operation = {
  Reserved: 0,
  Register: 1,
  NonReplicated: 2,
  Logout: 3,
  CreateTopicWithAssignments: 64,
  CreatePartitionsWithAssignments: 65,
  RemoveConsumerGroupMember: 66,
  CompleteConsumerGroupRevocation: 67,
  TruncatePartition: 68,
  CreateStream: 128,
  UpdateStream: 129,
  DeleteStream: 130,
  PurgeStream: 131,
  CreateTopic: 132,
  UpdateTopic: 133,
  DeleteTopic: 134,
  PurgeTopic: 135,
  CreatePartitions: 136,
  DeletePartitions: 137,
  DeleteSegments: 138,
  CreateConsumerGroup: 139,
  DeleteConsumerGroup: 140,
  CreateUser: 141,
  UpdateUser: 142,
  DeleteUser: 143,
  ChangePassword: 144,
  UpdatePermissions: 145,
  CreatePersonalAccessToken: 146,
  DeletePersonalAccessToken: 147,
  JoinConsumerGroup: 148,
  LeaveConsumerGroup: 149,
  SendMessages: 160,
  StoreConsumerOffset: 161,
  DeleteConsumerOffset: 162,
  StoreConsumerOffset2: 164,
  DeleteConsumerOffset2: 165
} as const;

const INTERNAL_START = 64;
const METADATA_START = 128;
const PARTITION_START = 160;

/**
 * Replicated command code to `Operation` mapping, the client half of the
 * dispatch table. Codes absent here are sent as non-replicated so the server
 * remains authoritative for extension commands unknown to this SDK build.
 */
const REPLICATED_OPERATION: ReadonlyMap<number, number> = new Map([
  [COMMAND_CODE.CreateUser, Operation.CreateUser],
  [COMMAND_CODE.DeleteUser, Operation.DeleteUser],
  [COMMAND_CODE.UpdateUser, Operation.UpdateUser],
  [COMMAND_CODE.UpdatePermissions, Operation.UpdatePermissions],
  [COMMAND_CODE.ChangePassword, Operation.ChangePassword],
  [COMMAND_CODE.CreateAccessToken, Operation.CreatePersonalAccessToken],
  [COMMAND_CODE.DeleteAccessToken, Operation.DeletePersonalAccessToken],
  [COMMAND_CODE.SendMessages, Operation.SendMessages],
  [COMMAND_CODE.StoreOffset, Operation.StoreConsumerOffset],
  [COMMAND_CODE.DeleteConsumerOffset, Operation.DeleteConsumerOffset],
  [COMMAND_CODE.StoreOffset2, Operation.StoreConsumerOffset2],
  [COMMAND_CODE.DeleteConsumerOffset2, Operation.DeleteConsumerOffset2],
  [COMMAND_CODE.CreateStream, Operation.CreateStream],
  [COMMAND_CODE.DeleteStream, Operation.DeleteStream],
  [COMMAND_CODE.UpdateStream, Operation.UpdateStream],
  [COMMAND_CODE.PurgeStream, Operation.PurgeStream],
  [COMMAND_CODE.CreateTopic, Operation.CreateTopic],
  [COMMAND_CODE.DeleteTopic, Operation.DeleteTopic],
  [COMMAND_CODE.UpdateTopic, Operation.UpdateTopic],
  [COMMAND_CODE.PurgeTopic, Operation.PurgeTopic],
  [COMMAND_CODE.CreatePartitions, Operation.CreatePartitions],
  [COMMAND_CODE.DeletePartitions, Operation.DeletePartitions],
  [COMMAND_CODE.DeleteSegments, Operation.DeleteSegments],
  [COMMAND_CODE.CreateGroup, Operation.CreateConsumerGroup],
  [COMMAND_CODE.DeleteGroup, Operation.DeleteConsumerGroup],
  [COMMAND_CODE.JoinGroup, Operation.JoinConsumerGroup],
  [COMMAND_CODE.LeaveGroup, Operation.LeaveConsumerGroup]
]);

const KNOWN_OPERATIONS: ReadonlySet<number> =
  new Set(Object.values(Operation));

/**
 * Whether the byte is a declared `Operation` discriminant. Replies carry a
 * server-controlled operation byte; Rust validates it with a checked cast,
 * so an undeclared value must be rejected, not classified by range.
 */
export const isKnownOperation = (operation: number): boolean =>
  KNOWN_OPERATIONS.has(operation);

/** Internal band, never client-sent. */
export const isInternal = (operation: number): boolean =>
  operation >= INTERNAL_START && operation < METADATA_START;

/**
 * Metadata classification is an explicit allowlist, not a range:
 * `DeleteSegments` (138) sits inside the band but resolves to a partition
 * truncation server-side, so it is deliberately excluded. Port of
 * `Operation::is_metadata`.
 */
export const isMetadata = (operation: number): boolean => {
  if (isInternal(operation)) return true;
  if (operation === Operation.DeleteSegments) return false;
  return operation >= METADATA_START &&
    operation <= Operation.LeaveConsumerGroup;
};

/** Partition band is a bare range, mirroring `Operation::is_partition`. */
export const isPartition = (operation: number): boolean =>
  operation >= PARTITION_START;

/** Whether a reply body leads with a committed result section. */
export const isResultFramed = (operation: number): boolean =>
  isMetadata(operation) ||
  operation === Operation.StoreConsumerOffset ||
  operation === Operation.StoreConsumerOffset2 ||
  operation === Operation.DeleteConsumerOffset ||
  operation === Operation.DeleteConsumerOffset2;

/**
 * Picks the header operation for a command code. Unknown extension codes are
 * sent as non-replicated with their command code in the reserved header field,
 * allowing the server to classify or reject them.
 */
export const operationForCode = (code: number): number => {
  // The dispatch table files logout as non-replicated, but VSR routes it
  // through its own consensus operation; the raw API blocks code 39 before
  // classification, so only the typed logout path reaches this branch.
  if (code === COMMAND_CODE.LogoutUser)
    return Operation.Logout;
  return REPLICATED_OPERATION.get(code) ?? Operation.NonReplicated;
};
