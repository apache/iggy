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

package tcp

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// consumerGroupBody builds one wire consumer group:
// [id u32][partitions u32][members u32][name_length u8][name].
func consumerGroupBody(id, partitions, members uint32, name string) []byte {
	body := binary.LittleEndian.AppendUint32(nil, id)
	body = binary.LittleEndian.AppendUint32(body, partitions)
	body = binary.LittleEndian.AppendUint32(body, members)
	body = append(body, byte(len(name)))
	return append(body, name...)
}

func TestGetConsumerGroups_DecodesTheRoster(t *testing.T) {
	client, serverConn := newPipeClient(t)
	body := append(
		consumerGroupBody(1, 3, 2, "billing"),
		consumerGroupBody(2, 1, 0, "audit")...)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, body)
	})

	groups, err := client.GetConsumerGroups(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 2))
	require.NoError(t, err)
	require.Len(t, groups, 2)
	assert.Equal(t, "billing", groups[0].Name)
	assert.Equal(t, uint32(3), groups[0].PartitionsCount)
	assert.Equal(t, "audit", groups[1].Name)
}

func TestGetConsumerGroup_ReportsAMissingGroupAsATypedError(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	_, err := client.GetConsumerGroup(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 2), numericIdentifier(t, 3))
	assert.ErrorIs(t, err, ierror.ErrConsumerGroupIdNotFound)
}

func TestCreateConsumerGroup_RejectsAnInvalidNameWithoutWriting(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateConsumerGroup, resultSection())
	})

	for _, name := range []string{"", strings.Repeat("x", MaxStringLength+1)} {
		_, err := client.CreateConsumerGroup(context.Background(),
			numericIdentifier(t, 1), numericIdentifier(t, 2), name)
		assert.ErrorIs(t, err, ierror.ErrInvalidConsumerGroupName, "name %q", name)
	}
	assert.Empty(t, server.recorded(), "an invalid name never reaches the wire")
}

func TestCreateConsumerGroup_DecodesTheCreatedGroup(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateConsumerGroup,
			append(resultSection(), consumerGroupBody(7, 3, 0, "billing")...))
	})

	group, err := client.CreateConsumerGroup(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 2), "billing")
	require.NoError(t, err)
	assert.Equal(t, uint32(7), group.Id)
	assert.Equal(t, "billing", group.Name)
}

func TestDeleteConsumerGroup_MarksTheGroupAsLeft(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationDeleteConsumerGroup, resultSection())
	})

	streamId := numericIdentifier(t, 1)
	topicId := numericIdentifier(t, 2)
	groupId := numericIdentifier(t, 3)
	require.NoError(t, client.DeleteConsumerGroup(
		context.Background(), streamId, topicId, groupId))

	assert.True(t, client.groups.hasLeft(newGroupKey(streamId, topicId, groupId)),
		"a poll after the delete must not recreate the membership")
}

func TestSyncConsumerGroup_ReportsANonMemberAsATypedError(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	assignment, err := client.SyncConsumerGroup(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 2), numericIdentifier(t, 3))
	assert.ErrorIs(t, err, ierror.ErrConsumerGroupMemberNotFound)
	assert.Nil(t, assignment)
}

func TestSyncConsumerGroup_KeepsAMemberWithoutPartitionsNonNil(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, assignmentBody(6))
	})

	assignment, err := client.SyncConsumerGroup(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 2), numericIdentifier(t, 3))
	require.NoError(t, err)
	require.NotNil(t, assignment, "membership and emptiness are different states")
	assert.Equal(t, uint64(6), assignment.Generation)
	assert.Empty(t, assignment.Partitions)
}
