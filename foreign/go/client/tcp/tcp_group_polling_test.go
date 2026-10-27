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
	"log/slog"
	"sync"
	"sync/atomic"
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
// [partition_id u32][current_offset u64][messages_count u32][context 24].
func emptyBatchBody(partitionId uint32) []byte {
	body := binary.LittleEndian.AppendUint32(nil, partitionId)
	body = binary.LittleEndian.AppendUint64(body, 0)
	return append(binary.LittleEndian.AppendUint32(body, 0), make([]byte, iggcon.PartitionContextSize)...)
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
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
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
	servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
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
	servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
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
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
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
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
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

func TestPollMessages_ResolvedLeaveDoesNotRejoinTheGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var leaveID atomic.Uint64
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(signedInUserID, 128)
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(signedInUserID, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.code() == uint32(command.SyncGroupCode):
			return replyFrame(vsr.OperationNonReplicated, nil)
		case read.operation() == vsr.OperationLeaveConsumerGroup:
			if leaveID.CompareAndSwap(0, read.requestID()) {
				cancel()
				return nil
			}
			assert.Equal(t, leaveID.Load(), read.requestID(), "resolve must replay the committed leave")
			return committedReply(read, 0)
		default:
			return committedReply(read, 0)
		}
	})
	client := signedInClient(t, server, true)
	streamId, topicId, consumer := groupConsumer(t)
	require.NoError(t, client.JoinConsumerGroup(context.Background(), streamId, topicId, consumer.Id))
	var uncertain ierror.OperationUncertain
	require.ErrorAs(t, client.LeaveConsumerGroup(ctx, streamId, topicId, consumer.Id), &uncertain)
	require.NoError(t, client.ResolveOperation(context.Background(), uncertain.OperationID))

	_, err := pollOnce(t, client)
	require.ErrorIs(t, err, ierror.ErrConsumerGroupMemberNotFound)
	require.Equal(t, 1, len(requestsFor(server.recorded(), vsr.OperationJoinConsumerGroup)),
		"polling after a resolved leave must not join the group again")
}

func TestPollMessages_GroupPollRejoinsAfterAnExplicitJoin(t *testing.T) {
	client, serverConn := newPipeClient(t)
	member := false
	servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
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
	servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
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

func TestPollMessages_GroupPollResyncsAnEmptyAssignmentOnTheNextPoll(t *testing.T) {
	client, serverConn := newPipeClient(t)
	// The join committed, but no partition installed the member as owner yet.
	assignment := assignmentBody(1)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.SyncGroupCode) {
			return replyFrame(vsr.OperationNonReplicated, assignment)
		}
		return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
	})

	first, err := pollOnce(t, client)
	require.NoError(t, err)
	require.Equal(t, iggcon.NoAssignedPartition, first.PartitionId)

	assignment = assignmentBody(4, 7)

	second, err := pollOnce(t, client)
	require.NoError(t, err)
	assert.Equal(t, uint32(7), second.PartitionId,
		"an empty assignment is not trusted for the refresh window")
	syncs := 0
	for _, read := range server.recorded() {
		if read.code() == uint32(command.SyncGroupCode) {
			syncs++
		}
	}
	assert.Equal(t, 2, syncs, "the poll after an empty sync syncs again")
}

func TestPollMessages_GroupPollReportsAnEmptyPollWhenTheRebalanceOutlastsTheAttempts(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
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
	servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
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

// pauseAtLog holds the goroutine that first logs message until resume closes.
type pauseAtLog struct {
	message         string
	reached, resume chan struct{}
	once            sync.Once
}

func (p *pauseAtLog) Enabled(context.Context, slog.Level) bool { return true }
func (p *pauseAtLog) WithAttrs([]slog.Attr) slog.Handler       { return p }
func (p *pauseAtLog) WithGroup(string) slog.Handler            { return p }
func (p *pauseAtLog) Handle(_ context.Context, record slog.Record) error {
	if record.Message == p.message {
		p.once.Do(func() {
			close(p.reached)
			<-p.resume
		})
	}
	return nil
}

// A recovery overtakes a group poll after its sync. The poll syncs again under
// the session of the recovery and polls, and only a failed recovery ends it.
func TestPollMessages_GroupPollSyncsAgainUnderTheSessionOfARecovery(t *testing.T) {
	for _, test := range []struct {
		name               string
		hold, refuseSignIn bool
		want               error
	}{
		{"running", true, false, nil},
		{"bound a new session", false, false, nil},
		{"failed", false, true, ierror.ErrUnauthenticated},
	} {
		t.Run(test.name, func(t *testing.T) {
			var server *testListener
			var registers atomic.Int32
			node := singleNodeHandler(t, func() string { return server.address() })
			server = listenVSR(t, nil, func(connection, index int, read request) []byte {
				switch {
				case read.code() == uint32(command.SyncGroupCode):
					return replyFrame(vsr.OperationNonReplicated, assignmentBody(7, 1))
				case read.code() == uint32(command.GetPartitionRouteCode):
					return pollRoutingReply(t, read, 0, 1)
				case read.code() == uint32(command.PollMessagesCode):
					return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
				case read.operation() == vsr.OperationRegister && registers.Add(1) > 1 && test.refuseSignIn:
					return statusReplyFrame(vsr.OperationRegister, uint32(ierror.ErrInvalidCredentials.Code()), nil)
				}
				return node(connection, index, read)
			})
			client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
			synced, resume := make(chan struct{}), make(chan struct{})
			client.logger = slog.New(&pauseAtLog{message: "Synced the consumer group assignment", reached: synced, resume: resume})
			require.NoError(t, client.Connect(context.Background()))
			done := make(chan error, 1)
			go func() { _, err := pollOnce(t, client); done <- err }()
			select {
			case <-synced:
			case <-time.After(time.Second):
				t.Fatal("the group poll did not sync its assignment")
			}
			var release func()
			if test.hold {
				release = holdRecovery(t, client)
			}
			recoverNow(client)
			if !test.hold {
				awaitRecoveryEnd(t, client)
				require.Equal(t, !test.refuseSignIn, sessionBound(client))
			}
			close(resume)
			if test.hold {
				require.Never(t, func() bool { return len(done) != 0 }, recoveryWaitDeadline, time.Millisecond,
					"the poll waits for the running recovery")
				release()
			}
			require.ErrorIs(t, <-done, test.want)
			if test.want == nil {
				reads := server.recorded()
				require.Equal(t, 2, requestCount(reads, command.SyncGroupCode), "the poll syncs again under the new session")
				require.Equal(t, 1, requestCount(reads, command.PollMessagesCode))
			}
		})
	}
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
