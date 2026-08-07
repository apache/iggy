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
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assignmentBody builds the SyncConsumerGroup reply body:
// [generation u64][count u32][partition u32 x n].
func assignmentBody(generation uint64, partitions ...uint32) []byte {
	body := binary.LittleEndian.AppendUint64(nil, generation)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(partitions)))
	for _, partition := range partitions {
		body = binary.LittleEndian.AppendUint32(body, partition)
	}
	return body
}

// emptyBatchBody builds a PollMessages reply body carrying no messages:
// [partition_id u32][current_offset u64][messages_count u32].
func emptyBatchBody(partitionId uint32) []byte {
	body := binary.LittleEndian.AppendUint32(nil, partitionId)
	body = binary.LittleEndian.AppendUint64(body, 0)
	return binary.LittleEndian.AppendUint32(body, 0)
}

// polledPartition reads the explicit partition id of a recorded poll request
// that was built from numeric identifiers.
func polledPartition(t *testing.T, read request) uint32 {
	t.Helper()
	// [kind u8][consumer id 6][stream id 6][topic id 6][has_partition u8].
	require.Equal(t, byte(1), read.payload[19], "the group poll names an explicit partition")
	return binary.LittleEndian.Uint32(read.payload[20:24])
}

// groupConsumer builds the group consumer and identifiers the tests poll with.
func groupConsumer(t *testing.T) (iggcon.Identifier, iggcon.Identifier, iggcon.Consumer) {
	t.Helper()
	streamId := numericIdentifier(t, 1)
	topicId := numericIdentifier(t, 2)
	return streamId, topicId, iggcon.NewGroupConsumer(numericIdentifier(t, 3))
}

// pollOnce runs one group poll without an explicit partition.
func pollOnce(t *testing.T, client *IggyTcpClient) (*iggcon.PolledMessage, error) {
	t.Helper()
	streamId, topicId, consumer := groupConsumer(t)
	return client.PollMessages(context.Background(), streamId, topicId,
		consumer, iggcon.NextPollingStrategy(), 1, false, nil)
}

// agedGroupEntries rewinds every cached assignment past the refresh interval.
func agedGroupEntries(client *IggyTcpClient) {
	client.groups.mtx.Lock()
	defer client.groups.mtx.Unlock()
	for _, entry := range client.groups.entries {
		entry.fetchedAt = time.Now().Add(-assignmentRefreshInterval)
	}
}

func TestPollMessages_GroupPollRoundRobinsTheOwnedPartitions(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
		switch read.code() {
		case uint32(command.SyncGroupCode):
			return replyFrame(vsr.OperationNonReplicated, assignmentBody(7, 1, 3))
		default:
			return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
		}
	})

	var partitions []uint32
	for range 3 {
		polled, err := pollOnce(t, client)
		require.NoError(t, err)
		partitions = append(partitions, polled.PartitionId)
	}

	assert.Equal(t, []uint32{1, 3, 1}, partitions,
		"the cursor advances and wraps across polls")
	syncs := 0
	for _, read := range server.recorded() {
		if read.code() == uint32(command.SyncGroupCode) {
			syncs++
		}
	}
	assert.Equal(t, 1, syncs, "a fresh assignment is trusted for the whole window")
}

func TestPollMessages_GroupPollKeepsTheCursorAcrossASameGenerationRefresh(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.SyncGroupCode) {
			return replyFrame(vsr.OperationNonReplicated, assignmentBody(7, 1, 3))
		}
		return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
	})

	first, err := pollOnce(t, client)
	require.NoError(t, err)
	require.Equal(t, uint32(1), first.PartitionId)

	// Expire the cached assignment, so the next poll re-syncs and receives
	// the same generation back.
	agedGroupEntries(client)

	second, err := pollOnce(t, client)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), second.PartitionId,
		"a same-generation re-sync must not reset the round-robin cursor")
}

func TestPollMessages_GroupPollRestartsTheCursorOnANewGeneration(t *testing.T) {
	client, serverConn := newPipeClient(t)
	generation := uint64(7)
	serve(serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.SyncGroupCode) {
			return replyFrame(vsr.OperationNonReplicated, assignmentBody(generation, 1, 3))
		}
		return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
	})

	first, err := pollOnce(t, client)
	require.NoError(t, err)
	require.Equal(t, uint32(1), first.PartitionId)

	generation = 8
	agedGroupEntries(client)

	second, err := pollOnce(t, client)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), second.PartitionId,
		"a rebalance restarts the rotation from the first owned partition")
}

func TestPollMessages_GroupPollJoinsTheGroupOnTheFirstPoll(t *testing.T) {
	client, serverConn := newPipeClient(t)
	joined := false
	server := serve(serverConn, func(_ int, read request) []byte {
		switch {
		case read.code() == uint32(command.SyncGroupCode) && !joined:
			// An empty body means the client is not a member yet.
			return replyFrame(vsr.OperationNonReplicated, nil)
		case read.code() == uint32(command.SyncGroupCode):
			return replyFrame(vsr.OperationNonReplicated, assignmentBody(1, 0))
		case read.operation() == vsr.OperationJoinConsumerGroup:
			joined = true
			return replyFrame(vsr.OperationJoinConsumerGroup, resultSection())
		default:
			return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
		}
	})

	polled, err := pollOnce(t, client)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), polled.PartitionId)

	operations := make([]vsr.Operation, 0)
	for _, read := range server.recorded() {
		operations = append(operations, read.operation())
	}
	assert.Contains(t, operations, vsr.OperationJoinConsumerGroup,
		"the first poll joins on the caller's behalf")
}

func TestPollMessages_GroupPollDoesNotRejoinALeftGroup(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
		switch {
		case read.code() == uint32(command.SyncGroupCode):
			return replyFrame(vsr.OperationNonReplicated, nil)
		case read.operation() == vsr.OperationLeaveConsumerGroup:
			return replyFrame(vsr.OperationLeaveConsumerGroup, resultSection())
		default:
			return replyFrame(vsr.OperationJoinConsumerGroup, resultSection())
		}
	})

	streamId, topicId, consumer := groupConsumer(t)
	require.NoError(t, client.LeaveConsumerGroup(
		context.Background(), streamId, topicId, consumer.Id))

	_, err := pollOnce(t, client)
	assert.ErrorIs(t, err, ierror.ErrConsumerGroupMemberNotFound,
		"an explicit leave stays left until an explicit join")

	for _, read := range server.recorded() {
		assert.NotEqual(t, vsr.OperationJoinConsumerGroup, read.operation(),
			"the poll must not undo the leave")
	}
}

func TestPollMessages_GroupPollRejoinsAfterAnExplicitJoin(t *testing.T) {
	client, serverConn := newPipeClient(t)
	member := false
	serve(serverConn, func(_ int, read request) []byte {
		switch {
		case read.code() == uint32(command.SyncGroupCode) && member:
			return replyFrame(vsr.OperationNonReplicated, assignmentBody(2, 4))
		case read.code() == uint32(command.SyncGroupCode):
			return replyFrame(vsr.OperationNonReplicated, nil)
		case read.operation() == vsr.OperationLeaveConsumerGroup:
			member = false
			return replyFrame(vsr.OperationLeaveConsumerGroup, resultSection())
		case read.operation() == vsr.OperationJoinConsumerGroup:
			member = true
			return replyFrame(vsr.OperationJoinConsumerGroup, resultSection())
		default:
			return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
		}
	})

	streamId, topicId, consumer := groupConsumer(t)
	require.NoError(t, client.LeaveConsumerGroup(
		context.Background(), streamId, topicId, consumer.Id))
	require.NoError(t, client.JoinConsumerGroup(
		context.Background(), streamId, topicId, consumer.Id))

	polled, err := pollOnce(t, client)
	require.NoError(t, err)
	assert.Equal(t, uint32(4), polled.PartitionId)
}

func TestPollMessages_GroupPollReportsAnEmptyPollWhenTheMemberOwnsNothing(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.SyncGroupCode) {
			return replyFrame(vsr.OperationNonReplicated, assignmentBody(5))
		}
		t.Errorf("unexpected request with code %d", read.code())
		return nil
	})

	polled, err := pollOnce(t, client)
	require.NoError(t, err)
	assert.Equal(t, iggcon.NoAssignedPartition, polled.PartitionId)
	assert.Empty(t, polled.Messages)
}

func TestPollMessages_GroupPollReportsAnEmptyPollWhenTheRebalanceOutlastsTheAttempts(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.SyncGroupCode) {
			return replyFrame(vsr.OperationNonReplicated, assignmentBody(9, 0))
		}
		// The server marks a stale assignment with the resync sentinel.
		return replyFrame(vsr.OperationNonReplicated,
			emptyBatchBody(iggcon.ResyncRequiredPartition))
	})

	polled, err := pollOnce(t, client)
	require.NoError(t, err, "a rebalance in flight is not a failure")
	assert.Equal(t, iggcon.NoAssignedPartition, polled.PartitionId)
	assert.Empty(t, polled.Messages)

	polls := 0
	for _, read := range server.recorded() {
		if read.code() == uint32(command.PollMessagesCode) {
			polls++
		}
	}
	assert.Equal(t, groupPollAttempts, polls, "every attempt re-synced and re-polled")
}

func TestPollMessages_GroupPollResyncsAfterAFencedPoll(t *testing.T) {
	client, serverConn := newPipeClient(t)
	generation := uint64(1)
	serve(serverConn, func(_ int, read request) []byte {
		switch {
		case read.code() == uint32(command.SyncGroupCode):
			return replyFrame(vsr.OperationNonReplicated, assignmentBody(generation, 1))
		case generation == 1:
			// The member no longer owns the partition at this generation.
			generation = 2
			return statusReplyFrame(vsr.OperationNonReplicated,
				uint32(ierror.ConsumerGroupPartitionNotOwnedCode), nil)
		default:
			return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
		}
	})

	polled, err := pollOnce(t, client)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), polled.PartitionId,
		"the fenced poll re-synced and retried within the same call")
}

func TestGroupAssignmentCache_TheLeftMarkSurvivesClear(t *testing.T) {
	cache := groupAssignmentCache{}
	key := groupKey{stream: "s", topic: "t", group: "g"}

	cache.put(key, groupAssignment{generation: 1})
	cache.markLeft(key)
	cache.clear()

	_, ok := cache.get(key)
	assert.False(t, ok, "clear drops the assignment")
	assert.True(t, cache.hasLeft(key),
		"leaving is caller intent and survives a connection reset")

	cache.markJoined(key)
	assert.False(t, cache.hasLeft(key), "an explicit join clears the mark")
}
