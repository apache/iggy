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

package vsr

import "github.com/apache/iggy/foreign/go/internal/command"

// Operation is the consensus operation discriminant, a port of
// core/binary_protocol/src/consensus/operation.rs. It picks the replication
// plane a request travels on and tells the client how to read the reply body.
type Operation uint8

// Operation discriminants a client sends or receives.
const (
	OperationReserved      Operation = 0
	OperationRegister      Operation = 1
	OperationNonReplicated Operation = 2
	OperationLogout        Operation = 3

	OperationCreateTopicWithAssignments      Operation = 64
	OperationCreatePartitionsWithAssignments Operation = 65
	OperationRemoveConsumerGroupMember       Operation = 66
	OperationCompleteConsumerGroupRevocation Operation = 67
	OperationTruncatePartition               Operation = 68

	OperationCreateStream              Operation = 128
	OperationUpdateStream              Operation = 129
	OperationDeleteStream              Operation = 130
	OperationPurgeStream               Operation = 131
	OperationCreateTopic               Operation = 132
	OperationUpdateTopic               Operation = 133
	OperationDeleteTopic               Operation = 134
	OperationPurgeTopic                Operation = 135
	OperationCreatePartitions          Operation = 136
	OperationDeletePartitions          Operation = 137
	OperationDeleteSegments            Operation = 138
	OperationCreateConsumerGroup       Operation = 139
	OperationDeleteConsumerGroup       Operation = 140
	OperationCreateUser                Operation = 141
	OperationUpdateUser                Operation = 142
	OperationDeleteUser                Operation = 143
	OperationChangePassword            Operation = 144
	OperationUpdatePermissions         Operation = 145
	OperationCreatePersonalAccessToken Operation = 146
	OperationDeletePersonalAccessToken Operation = 147
	OperationJoinConsumerGroup         Operation = 148
	OperationLeaveConsumerGroup        Operation = 149

	OperationSendMessages          Operation = 160
	OperationStoreConsumerOffset   Operation = 161
	OperationDeleteConsumerOffset  Operation = 162
	OperationStoreConsumerOffset2  Operation = 164
	OperationDeleteConsumerOffset2 Operation = 165
)

// Band boundaries. The internal band is never client-sent.
const (
	internalBandStart  = OperationCreateTopicWithAssignments
	metadataBandStart  = OperationCreateStream
	partitionBandStart = OperationSendMessages
)

// allOperations lists every declared discriminant in wire order. It backs both
// the known-operation allowlist and the parity check against the Rust enum.
var allOperations = []Operation{
	OperationReserved,
	OperationRegister,
	OperationNonReplicated,
	OperationLogout,
	OperationCreateTopicWithAssignments,
	OperationCreatePartitionsWithAssignments,
	OperationRemoveConsumerGroupMember,
	OperationCompleteConsumerGroupRevocation,
	OperationTruncatePartition,
	OperationCreateStream,
	OperationUpdateStream,
	OperationDeleteStream,
	OperationPurgeStream,
	OperationCreateTopic,
	OperationUpdateTopic,
	OperationDeleteTopic,
	OperationPurgeTopic,
	OperationCreatePartitions,
	OperationDeletePartitions,
	OperationDeleteSegments,
	OperationCreateConsumerGroup,
	OperationDeleteConsumerGroup,
	OperationCreateUser,
	OperationUpdateUser,
	OperationDeleteUser,
	OperationChangePassword,
	OperationUpdatePermissions,
	OperationCreatePersonalAccessToken,
	OperationDeletePersonalAccessToken,
	OperationJoinConsumerGroup,
	OperationLeaveConsumerGroup,
	OperationSendMessages,
	OperationStoreConsumerOffset,
	OperationDeleteConsumerOffset,
	OperationStoreConsumerOffset2,
	OperationDeleteConsumerOffset2,
}

var knownOperations = newOperationSet(allOperations)

// replicatedOperation is the client half of the server dispatch table. Codes
// absent from it travel as non-replicated with their code in the reserved
// header field, leaving the server authoritative for commands this SDK build
// does not know.
var replicatedOperation = map[uint32]Operation{
	uint32(command.CreateUserCode):            OperationCreateUser,
	uint32(command.DeleteUserCode):            OperationDeleteUser,
	uint32(command.UpdateUserCode):            OperationUpdateUser,
	uint32(command.UpdatePermissionsCode):     OperationUpdatePermissions,
	uint32(command.ChangePasswordCode):        OperationChangePassword,
	uint32(command.CreateAccessTokenCode):     OperationCreatePersonalAccessToken,
	uint32(command.DeleteAccessTokenCode):     OperationDeletePersonalAccessToken,
	uint32(command.SendMessagesCode):          OperationSendMessages,
	uint32(command.StoreOffsetCode):           OperationStoreConsumerOffset,
	uint32(command.DeleteConsumerOffsetCode):  OperationDeleteConsumerOffset,
	uint32(command.StoreOffset2Code):          OperationStoreConsumerOffset2,
	uint32(command.DeleteConsumerOffset2Code): OperationDeleteConsumerOffset2,
	uint32(command.CreateStreamCode):          OperationCreateStream,
	uint32(command.DeleteStreamCode):          OperationDeleteStream,
	uint32(command.UpdateStreamCode):          OperationUpdateStream,
	uint32(command.PurgeStreamCode):           OperationPurgeStream,
	uint32(command.CreateTopicCode):           OperationCreateTopic,
	uint32(command.DeleteTopicCode):           OperationDeleteTopic,
	uint32(command.UpdateTopicCode):           OperationUpdateTopic,
	uint32(command.PurgeTopicCode):            OperationPurgeTopic,
	uint32(command.CreatePartitionsCode):      OperationCreatePartitions,
	uint32(command.DeletePartitionsCode):      OperationDeletePartitions,
	uint32(command.DeleteSegmentsCode):        OperationDeleteSegments,
	uint32(command.CreateGroupCode):           OperationCreateConsumerGroup,
	uint32(command.DeleteGroupCode):           OperationDeleteConsumerGroup,
	uint32(command.JoinGroupCode):             OperationJoinConsumerGroup,
	uint32(command.LeaveGroupCode):            OperationLeaveConsumerGroup,
}

func newOperationSet(operations []Operation) map[Operation]struct{} {
	set := make(map[Operation]struct{}, len(operations))
	for _, operation := range operations {
		set[operation] = struct{}{}
	}
	return set
}

// ReplicatedOperation returns the consensus operation a replicated command
// code maps to.
func ReplicatedOperation(code uint32) (Operation, bool) {
	operation, ok := replicatedOperation[code]
	return operation, ok
}

// OperationForCode picks the header operation for a command code. The two
// register codes resolve to Register even though the server dispatch table
// files them as non-replicated: the SDK drives the handshake and the server
// reads the operation byte, not the code. Unknown codes resolve to
// NonReplicated so the server can classify or reject them.
func OperationForCode(code uint32) Operation {
	switch code {
	case uint32(command.LoginRegisterCode), uint32(command.LoginRegisterWithPATCode):
		return OperationRegister
	case uint32(command.LogoutUserCode):
		return OperationLogout
	}
	if operation, ok := replicatedOperation[code]; ok {
		return operation
	}
	return OperationNonReplicated
}

// IsKnownOperation reports whether the byte is a declared discriminant. A
// reply carries a server-controlled operation byte, so an undeclared value is
// rejected rather than classified by range.
func IsKnownOperation(operation Operation) bool {
	_, ok := knownOperations[operation]
	return ok
}

// IsInternal reports whether the operation belongs to the replica-internal
// band, which a client never sends.
func IsInternal(operation Operation) bool {
	return operation >= internalBandStart && operation < metadataBandStart
}

// IsMetadata reports whether the operation replicates through the metadata
// consensus group. The classification is an allowlist, not a range:
// DeleteSegments sits inside the metadata band but resolves to a partition
// truncation server-side.
func IsMetadata(operation Operation) bool {
	if IsInternal(operation) {
		return true
	}
	if operation == OperationDeleteSegments {
		return false
	}
	return operation >= metadataBandStart && operation <= OperationLeaveConsumerGroup
}

// IsPartition reports whether the operation is routed to the shard owning a
// partition.
func IsPartition(operation Operation) bool {
	return operation >= partitionBandStart
}

// IsResultFramed reports whether the reply body leads with a committed result
// section. Every metadata operation is framed; on the partition plane only the
// consumer-offset operations are, because they reject with typed errors at
// admission. SendMessages is not framed.
func IsResultFramed(operation Operation) bool {
	if IsMetadata(operation) {
		return true
	}
	switch operation {
	case OperationStoreConsumerOffset, OperationStoreConsumerOffset2,
		OperationDeleteConsumerOffset, OperationDeleteConsumerOffset2:
		return true
	default:
		return false
	}
}
