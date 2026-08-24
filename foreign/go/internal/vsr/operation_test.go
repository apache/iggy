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

import (
	"fmt"
	"testing"

	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperationForCode_ResolvesTheReplicatedTable(t *testing.T) {
	tests := []struct {
		code command.Code
		want Operation
	}{
		{code: command.CreateUserCode, want: OperationCreateUser},
		{code: command.DeleteUserCode, want: OperationDeleteUser},
		{code: command.UpdateUserCode, want: OperationUpdateUser},
		{code: command.UpdatePermissionsCode, want: OperationUpdatePermissions},
		{code: command.ChangePasswordCode, want: OperationChangePassword},
		{code: command.CreateAccessTokenCode, want: OperationCreatePersonalAccessToken},
		{code: command.DeleteAccessTokenCode, want: OperationDeletePersonalAccessToken},
		{code: command.SendMessagesCode, want: OperationSendMessages},
		{code: command.StoreOffsetCode, want: OperationStoreConsumerOffset},
		{code: command.DeleteConsumerOffsetCode, want: OperationDeleteConsumerOffset},
		{code: command.CreateStreamCode, want: OperationCreateStream},
		{code: command.DeleteStreamCode, want: OperationDeleteStream},
		{code: command.UpdateStreamCode, want: OperationUpdateStream},
		{code: command.PurgeStreamCode, want: OperationPurgeStream},
		{code: command.CreateTopicCode, want: OperationCreateTopic},
		{code: command.DeleteTopicCode, want: OperationDeleteTopic},
		{code: command.UpdateTopicCode, want: OperationUpdateTopic},
		{code: command.PurgeTopicCode, want: OperationPurgeTopic},
		{code: command.CreatePartitionsCode, want: OperationCreatePartitions},
		{code: command.DeletePartitionsCode, want: OperationDeletePartitions},
		{code: command.DeleteSegmentsCode, want: OperationDeleteSegments},
		{code: command.CreateGroupCode, want: OperationCreateConsumerGroup},
		{code: command.DeleteGroupCode, want: OperationDeleteConsumerGroup},
		{code: command.JoinGroupCode, want: OperationJoinConsumerGroup},
		{code: command.LeaveGroupCode, want: OperationLeaveConsumerGroup},
	}
	require.Len(t, tests, len(replicatedOperation), "every replicated entry is covered")
	for _, test := range tests {
		t.Run(fmt.Sprintf("code_%d", test.code), func(t *testing.T) {
			assert.Equal(t, test.want, OperationForCode(uint32(test.code)))
			mapped, ok := ReplicatedOperation(uint32(test.code))
			assert.True(t, ok)
			assert.Equal(t, test.want, mapped)
		})
	}
}

func TestOperationForCode_ResolvesTheProtocolOperations(t *testing.T) {
	assert.Equal(t, OperationRegister, OperationForCode(uint32(command.LoginRegisterCode)))
	assert.Equal(t, OperationRegister, OperationForCode(uint32(command.LoginRegisterWithPATCode)))
	assert.Equal(t, OperationLogout, OperationForCode(uint32(command.LogoutUserCode)))
}

func TestOperationForCode_FallsBackToNonReplicated(t *testing.T) {
	codes := []command.Code{
		command.PingCode,
		command.GetStatsCode,
		command.GetSnapshotFileCode,
		command.GetClusterMetadataCode,
		command.PollMessagesCode,
		command.GetOffsetCode,
		command.GetStreamCode,
		command.GetTopicsCode,
		command.SyncGroupCode,
		command.LoginUserCode,
		command.LoginWithAccessTokenCode,
	}
	for _, code := range codes {
		assert.Equal(t, OperationNonReplicated, OperationForCode(uint32(code)), "code %d", code)
	}

	assert.Equal(t, OperationNonReplicated, OperationForCode(60000), "vendor code")
	assert.Equal(t, OperationNonReplicated, OperationForCode(0))

	_, ok := ReplicatedOperation(60000)
	assert.False(t, ok)
}

func TestOperationForCode_NeverDowngradesAReplicatedCode(t *testing.T) {
	for code, operation := range replicatedOperation {
		resolved := OperationForCode(code)
		assert.NotEqual(t, OperationNonReplicated, resolved, "code %d", code)
		assert.Equal(t, operation, resolved, "code %d", code)
	}
}

func TestIsKnownOperation_RejectsUndeclaredDiscriminants(t *testing.T) {
	for _, operation := range allOperations {
		assert.True(t, IsKnownOperation(operation), "operation %d", operation)
	}

	undeclared := []Operation{4, 63, 69, 127, 150, 159, 163, 164, 165, 255}
	for _, operation := range undeclared {
		assert.False(t, IsKnownOperation(operation), "operation %d", operation)
	}
}

func TestIsInternal_CoversTheInternalBandOnly(t *testing.T) {
	assert.True(t, IsInternal(OperationCreateTopicWithAssignments))
	assert.True(t, IsInternal(OperationTruncatePartition))
	assert.False(t, IsInternal(OperationLogout))
	assert.False(t, IsInternal(OperationCreateStream))
	assert.False(t, IsInternal(OperationSendMessages))
}

func TestIsMetadata_ExcludesDeleteSegments(t *testing.T) {
	assert.False(t, IsMetadata(OperationDeleteSegments),
		"DeleteSegments sits in the band but truncates a partition")

	metadata := []Operation{
		OperationCreateTopicWithAssignments,
		OperationTruncatePartition,
		OperationCreateStream,
		OperationPurgeStream,
		OperationPurgeTopic,
		OperationCreateConsumerGroup,
		OperationCreatePersonalAccessToken,
		OperationLeaveConsumerGroup,
	}
	for _, operation := range metadata {
		assert.True(t, IsMetadata(operation), "operation %d", operation)
	}

	nonMetadata := []Operation{
		OperationReserved,
		OperationRegister,
		OperationNonReplicated,
		OperationLogout,
		OperationSendMessages,
		OperationStoreConsumerOffset,
		OperationDeleteConsumerOffset,
	}
	for _, operation := range nonMetadata {
		assert.False(t, IsMetadata(operation), "operation %d", operation)
	}
}

func TestIsPartition_CoversTheDataPlaneBand(t *testing.T) {
	assert.True(t, IsPartition(OperationSendMessages))
	assert.True(t, IsPartition(OperationStoreConsumerOffset))
	assert.True(t, IsPartition(OperationDeleteConsumerOffset))
	assert.False(t, IsPartition(OperationLeaveConsumerGroup))
	assert.False(t, IsPartition(OperationDeleteSegments))
}

func TestIsResultFramed_ExcludesSendMessages(t *testing.T) {
	assert.False(t, IsResultFramed(OperationSendMessages),
		"a send confirmation is not preceded by a result section")

	framed := []Operation{
		OperationCreateStream,
		OperationLeaveConsumerGroup,
		OperationStoreConsumerOffset,
		OperationDeleteConsumerOffset,
	}
	for _, operation := range framed {
		assert.True(t, IsResultFramed(operation), "operation %d", operation)
	}

	unframed := []Operation{
		OperationRegister,
		OperationNonReplicated,
		OperationLogout,
		OperationDeleteSegments,
	}
	for _, operation := range unframed {
		assert.False(t, IsResultFramed(operation), "operation %d", operation)
	}
}
