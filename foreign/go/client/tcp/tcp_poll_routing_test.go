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
	"fmt"
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

const testMetadataCommitOffset = 184

type primaryPollFixture struct {
	client      *IggyTcpClient
	coordinator *testListener
	primaries   []*testListener
}

func newPrimaryPollFixture(t *testing.T,
	data func(primary, connection int, read request) ([]byte, bool),
	control func(connection int, read request) ([]byte, bool),
	connectError ...error,
) *primaryPollFixture {
	t.Helper()
	fixture := &primaryPollFixture{}
	for primary := range 2 {
		fixture.primaries = append(fixture.primaries, listenVSR(t, nil, func(connection, _ int, read request) []byte {
			if data != nil {
				if reply, handled := data(primary, connection, read); handled {
					return reply
				}
			}
			switch {
			case read.operation() == vsr.OperationRegister:
				t.Error("an auxiliary connection must bind the parent session without Register")
				return statusReplyFrame(vsr.OperationRegister, uint32(ierror.ErrUnauthenticated.Code()), nil)
			case read.code() == uint32(command.BindSessionCode):
				return bindReplyFrame(7, read.sessionID())
			case read.code() == uint32(command.GetPartitionRouteCode):
				if control != nil {
					if reply, handled := control(connection, read); handled {
						return reply
					}
				}
				return pollRoutingReply(t, read, uint8(primary+1), max(1, binary.LittleEndian.Uint64(read.header[frameOffsetContext+16:])))
			case read.code() == uint32(command.PollMessagesCode):
				partition := routedPollPartition(t, read)
				require.Equal(t, primary, int(partition), "poll must reach its own partition primary")
				return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(partition))
			default:
				t.Errorf("unexpected data command %d, operation %d", read.code(), read.operation())
				return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrInvalidCommand.Code()), nil)
			}
		}))
	}
	fixture.coordinator = listenVSR(t, nil, func(connection, _ int, read request) []byte {
		if control != nil {
			if reply, handled := control(connection, read); handled {
				return reply
			}
		}
		switch {
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, uint64(100+connection))
		case read.sessionID() == 0:
			return evictionFrame(vsr.EvictionNoSession, 0, 0)
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, fixture.coordinator.address(), fixture.primaries[0].address(), fixture.primaries[1].address())
		case read.code() == uint32(command.SyncGroupCode):
			return replyFrame(vsr.OperationNonReplicated, assignmentBody(7, 0, 1))
		case read.code() == uint32(command.GetPartitionRouteCode):
			partition := routedPollPartition(t, read)
			return pollRoutingReply(t, read, uint8(partition+1), max(1, binary.LittleEndian.Uint64(read.header[frameOffsetContext+16:])))
		case read.code() == uint32(command.PollMessagesCode):
			return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
		case read.code() == uint32(command.PingCode):
			return replyFrame(vsr.OperationNonReplicated, nil)
		default:
			answer := replyFrame(read.operation(), resultSection())
			binary.LittleEndian.PutUint64(answer[testMetadataCommitOffset:], 11)
			return answer
		}
	})
	fixture.client = newDialingClient(t, fixture.coordinator.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "secret")))
	err := fixture.client.Connect(context.Background())
	if len(connectError) == 0 {
		require.NoError(t, err)
	} else {
		require.ErrorIs(t, err, connectError[0])
	}
	return fixture
}

func pollRoutingReply(t *testing.T, read request, replica uint8, watermark uint64) []byte {
	t.Helper()
	body := binary.LittleEndian.AppendUint64(nil, 0)
	body = binary.LittleEndian.AppendUint64(body, watermark)
	body = binary.LittleEndian.AppendUint32(body, 0)
	body = binary.LittleEndian.AppendUint32(body, 2)
	body = append(body, replica)
	return replyFrame(vsr.OperationNonReplicated, body)
}

func pollPrimaryPartition(ctx context.Context, client *IggyTcpClient, partition uint32) (*iggcon.PolledMessage, error) {
	stream, _ := iggcon.NewIdentifier(uint32(1))
	topic, _ := iggcon.NewIdentifier(uint32(2))
	consumer, _ := iggcon.NewIdentifier(uint32(3))
	return client.PollMessages(ctx, stream, topic, iggcon.NewGroupConsumer(consumer),
		iggcon.NextPollingStrategy(), 1, true, &partition)
}

func routedPollPartition(t *testing.T, read request) uint32 {
	t.Helper()
	if read.code() == uint32(command.GetPartitionRouteCode) {
		return binary.LittleEndian.Uint32(read.payload[len(read.payload)-4:])
	}
	require.GreaterOrEqual(t, len(read.payload), 24)
	if read.payload[19] == 0 {
		return 0
	}
	return polledPartition(t, read)
}

func bindIdentity(t *testing.T, payload []byte) []byte {
	t.Helper()
	offset := versionInfoEnd(t, payload)
	require.Len(t, payload[offset:], vsr.SessionIdentityBytes+vsr.BindSecretBytes)
	return payload[offset : offset+vsr.SessionIdentityBytes]
}

func requestCount(reads []request, code command.Code) int {
	count := 0
	for _, read := range reads {
		if read.code() == uint32(code) {
			count++
		}
	}
	return count
}

func TestPrimaryPoll_SplitPrimariesKeepCoordinatorMembershipAndReuseConnections(t *testing.T) {
	fixture := newPrimaryPollFixture(t, nil, nil)
	stream, topic, consumer := groupConsumer(t)
	require.NoError(t, fixture.client.JoinConsumerGroup(context.Background(), stream, topic, consumer.Id))
	parent := fixture.client.session.ClientID()
	session := fixture.client.session.SessionID()
	for index := range 4 {
		polled, err := fixture.client.PollMessages(context.Background(), stream, topic, consumer,
			iggcon.NextPollingStrategy(), 1, true, nil)
		require.NoError(t, err)
		require.Equal(t, uint32(index%2), polled.PartitionId)
	}
	assert.Equal(t, 1, fixture.coordinator.connections(), "poll routing must not move the group coordinator")
	assert.Equal(t, parent, fixture.client.session.ClientID())
	assert.Equal(t, session, fixture.client.session.SessionID())
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.GetPartitionRouteCode))
	assert.Equal(t, 1, requestCount(fixture.coordinator.recorded(), command.SyncGroupCode))
	assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
	for _, primary := range fixture.primaries {
		assert.Equal(t, 1, primary.connections())
		assert.GreaterOrEqual(t, requestCount(primary.recorded(), command.BindSessionCode), 1)
		var lastBind request
		assert.Equal(t, 2, requestCount(primary.recorded(), command.PollMessagesCode))
		for _, read := range primary.recorded() {
			if read.code() == uint32(command.BindSessionCode) {
				assert.Equal(t, parent, read.clientID(), "the data connection shares the coordinator identity")
				lastBind = read
				assert.Equal(t, fixture.client.session.BindSecret(), [vsr.BindSecretBytes]byte(read.payload[len(read.payload)-vsr.BindSecretBytes:]))
			}
		}
		assert.Equal(t, (consumerSession{client: parent, session: session, watermark: 11}).bytes(), bindIdentity(t, lastBind.payload))
	}
}

func TestPrimaryPoll_MetadataReplyRefreshesRouteAndAttachment(t *testing.T) {
	fixture := newPrimaryPollFixture(t, nil, nil)
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	_, err = fixture.client.SendBinaryRequest(context.Background(), uint32(command.DeleteTopicCode), nil)
	require.NoError(t, err)
	for range 2 {
		_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
		require.NoError(t, err)
	}
	var floors []uint64
	for _, read := range fixture.primaries[0].recorded() {
		if read.code() == uint32(command.BindSessionCode) {
			floors = append(floors, binary.LittleEndian.Uint64(bindIdentity(t, read.payload)[24:]))
		}
	}
	assert.Equal(t, []uint64{1, 11}, floors)
	assert.Equal(t, 1, fixture.primaries[0].connections())
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.GetPartitionRouteCode))
}

func TestPrimaryPoll_UnknownOutcomeDoesNotReplayAndOnlyLostRepliesReplaceDataConnection(t *testing.T) {
	for _, lostReply := range []bool{false, true} {
		t.Run(map[bool]string{false: "transient-not-committed", true: "lost-reply"}[lostReply], func(t *testing.T) {
			var polls atomic.Int32
			fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
				if read.code() == uint32(command.PollMessagesCode) && polls.Add(1) == 1 {
					if lostReply {
						return nil, true
					}
					return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrTransientNotCommitted.Code()), nil), true
				}
				return nil, false
			}, nil)
			_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.ErrorIs(t, err, ierror.ErrTransientNotCommitted)
			require.Equal(t, int32(1), polls.Load(), "the admitted poll is never replayed")
			_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.NoError(t, err)
			connections := 1
			if lostReply {
				connections = 2
			}
			assert.Equal(t, connections, fixture.primaries[0].connections())
			assert.Equal(t, 1, fixture.coordinator.connections())
		})
	}
}

func TestPrimaryPoll_RefusalRefreshesAttachmentWithoutMovingCoordinator(t *testing.T) {
	var polls atomic.Int32
	fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.PollMessagesCode) && polls.Add(1) == 1 {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrTransientNotAccepted.Code()), nil), true
		}
		return nil, false
	}, nil)
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.GetPartitionRouteCode))
	assert.Equal(t, 1, requestCount(fixture.primaries[0].recorded(), command.BindSessionCode))
	assert.Equal(t, 1, fixture.primaries[0].connections())
	assert.Equal(t, 1, fixture.coordinator.connections())
}

func TestPrimaryPoll_CancellationPreservesTheLinkAndBoundsLaneWaiters(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var polls atomic.Uint32
	fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.PollMessagesCode) && polls.Add(1) == 1 {
			close(entered)
			<-release
			return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(99)), true
		}
		return nil, false
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := make(chan error, 1)
	go func() { _, err := pollPrimaryPartition(ctx, fixture.client, 0); completed <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("primary did not receive the poll")
	}
	waiterCtx, waiterCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer waiterCancel()
	_, err := pollPrimaryPartition(waiterCtx, fixture.client, 0)
	require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
	cancel()
	select {
	case err := <-completed:
		require.ErrorIs(t, err, ierror.ErrTransientNotCommitted)
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt the data exchange")
	}
	close(release)
	polled, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	assert.Zero(t, polled.PartitionId, "the late canceled reply must never answer the next poll")
	assert.Equal(t, 1, fixture.primaries[0].connections())
	assert.Equal(t, 1, fixture.coordinator.connections())
}

func TestPrimaryPoll_LogoutRetiresPendingDataAndDoesNotResurrectMembership(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.PollMessagesCode) {
			close(entered)
			<-release
			return nil, true
		}
		return nil, false
	}, nil)
	completed := make(chan error, 1)
	go func() { _, err := pollPrimaryPartition(context.Background(), fixture.client, 0); completed <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("primary did not receive the poll")
	}
	require.NoError(t, fixture.client.LogoutUser(context.Background()))
	select {
	case err := <-completed:
		require.ErrorIs(t, err, ierror.ErrTransientNotCommitted)
	case <-time.After(time.Second):
		t.Fatal("logout left the data exchange running")
	}
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.ErrorIs(t, err, ierror.ErrUnauthenticated)
	assert.Empty(t, fixture.client.polls.routes)
	assert.Empty(t, fixture.client.links.slots)
	assert.False(t, fixture.client.session.Bound())
	assert.Equal(t, 1, fixture.coordinator.connections())
}

func TestPrimaryPoll_CompleteStatusRepliesKeepTheDataConnection(t *testing.T) {
	for _, status := range []ierror.IggyError{
		ierror.ErrUnauthorized, ierror.ErrConsumerGroupPartitionNotOwned, ierror.ErrInvalidCommand,
	} {
		t.Run(status.Error(), func(t *testing.T) {
			var polls atomic.Int32
			fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
				if read.code() == uint32(command.PollMessagesCode) && polls.Add(1) == 1 {
					return statusReplyFrame(vsr.OperationNonReplicated, uint32(status.Code()), nil), true
				}
				return nil, false
			}, nil)
			_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.ErrorIs(t, err, status)
			_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.NoError(t, err)
			assert.Equal(t, 1, fixture.primaries[0].connections())
			assert.Equal(t, 1, requestCount(fixture.primaries[0].recorded(), command.BindSessionCode))
			assert.Equal(t, int32(2), polls.Load())
		})
	}
}

func TestPrimaryPoll_InvalidReplyFrameDropsTheDataConnectionWithoutReplay(t *testing.T) {
	var polls atomic.Int32
	fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.PollMessagesCode) && polls.Add(1) == 1 {
			answer := replyFrame(vsr.OperationNonReplicated, nil)
			binary.LittleEndian.PutUint32(answer[frameOffsetSize:], vsr.HeaderSize-1)
			return answer, true
		}
		return nil, false
	}, nil)
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.ErrorIs(t, err, ierror.ErrTransientNotCommitted)
	require.Equal(t, int32(1), polls.Load())
	_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, fixture.primaries[0].connections())
}

func TestPrimaryPoll_DetachedLinkPreservesTheSessionAndReplaysOnlyARefusedPoll(t *testing.T) {
	for _, test := range []struct {
		name  string
		reply []byte
		want  error
	}{
		{"evicted", evictionFrame(vsr.EvictionNoSession, 0, 0), ierror.ErrTransientNotCommitted},
		{"unauthenticated", statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrUnauthenticated.Code()), nil), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			var polls atomic.Int32
			fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
				if read.code() == uint32(command.PollMessagesCode) && polls.Add(1) == 1 {
					return test.reply, true
				}
				return nil, false
			}, nil)
			_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.ErrorIs(t, err, test.want)
			require.Eventually(t, func() bool {
				fixture.client.mtx.Lock()
				defer fixture.client.mtx.Unlock()
				return fixture.client.session.Bound()
			}, time.Second, time.Millisecond)
			if test.want == nil {
				require.Equal(t, int32(2), polls.Load())
			} else {
				require.Equal(t, int32(1), polls.Load())
			}
			_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.NoError(t, err)
			assert.Equal(t, 2, fixture.primaries[0].connections())
			assert.Equal(t, 1, fixture.coordinator.connections())
		})
	}
}

func TestPrimaryPoll_ForwardedTruncateReplyRefreshesMetadataFence(t *testing.T) {
	fixture := newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
		if read.operation() == vsr.OperationDeleteSegments {
			answer := replyFrame(vsr.OperationTruncatePartition, resultSection())
			binary.LittleEndian.PutUint64(answer[testMetadataCommitOffset:], 42)
			return answer, true
		}
		return nil, false
	})
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	_, err = fixture.client.SendBinaryRequest(context.Background(), uint32(command.DeleteSegmentsCode), nil)
	require.NoError(t, err)
	_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	var floors []uint64
	for _, read := range fixture.primaries[0].recorded() {
		if read.code() == uint32(command.BindSessionCode) {
			floors = append(floors, binary.LittleEndian.Uint64(bindIdentity(t, read.payload)[24:]))
		}
	}
	assert.Equal(t, []uint64{1, 42}, floors)
	assert.Equal(t, 1, fixture.primaries[0].connections())
}

func TestPrimaryPoll_CoordinatorTrafficDoesNotStarveColdRouting(t *testing.T) {
	const workers = 8
	busy := make(chan struct{})
	var requests atomic.Int32
	fixture := newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.GetStatsCode) {
			if requests.Add(1) == workers {
				close(busy)
			}
			time.Sleep(2 * time.Millisecond)
			return replyFrame(vsr.OperationNonReplicated, nil), true
		}
		return nil, false
	})
	trafficCtx, cancel := context.WithCancel(context.Background())
	var traffic sync.WaitGroup
	for range workers {
		traffic.Go(func() {
			for trafficCtx.Err() == nil {
				_, _ = fixture.client.SendBinaryRequest(trafficCtx, uint32(command.GetStatsCode), nil)
			}
		})
	}
	defer func() { cancel(); traffic.Wait() }()
	select {
	case <-busy:
	case <-time.After(time.Second):
		t.Fatal("coordinator traffic did not start")
	}
	ctx, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stop()
	_, err := pollPrimaryPartition(ctx, fixture.client, 0)
	require.NoError(t, err, "cold routing must join the coordinator exchange queue")
}

func TestPrimaryPoll_UnknownTopologyCannotSelectLegacyPolling(t *testing.T) {
	for _, test := range []struct {
		name    string
		reply   []byte
		wantErr error
	}{
		{"unsupported", statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrFeatureUnavailable.Code()), nil), ierror.ErrFeatureUnavailable},
		{"empty", clusterMetadataFrame(t, 0), ierror.ErrInvalidCommand},
	} {
		t.Run(test.name, func(t *testing.T) {
			var failRoster atomic.Bool
			failRoster.Store(true)
			fixture := newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
				if read.code() == uint32(command.GetClusterMetadataCode) && failRoster.Load() {
					return append([]byte(nil), test.reply...), true
				}
				return nil, false
			}, test.wantErr)
			require.False(t, len(fixture.client.links.slots) > 0, "the login-time roster read failed")
			parent := fixture.client.session.ClientID()
			_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.ErrorIs(t, err, test.wantErr)
			assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
			assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.GetPartitionRouteCode))

			failRoster.Store(false)
			_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.NoError(t, err)
			require.True(t, len(fixture.client.links.slots) > 0)
			failRoster.Store(true)
			_, err = fixture.client.GetClusterMetadata(context.Background())
			require.ErrorIs(t, err, test.wantErr)
			_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.NoError(t, err, "a failed refresh must preserve known topology and warm routes")
			assert.Equal(t, 1, fixture.primaries[0].connections())
			assert.Equal(t, 1, fixture.coordinator.connections())
			assert.Equal(t, parent, fixture.client.session.ClientID())
			assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
		})
	}
}

func TestPrimaryPoll_UnknownTopologyCanRecoverAsStandalone(t *testing.T) {
	var failRoster atomic.Bool
	failRoster.Store(true)
	var fixture *primaryPollFixture
	fixture = newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.GetClusterMetadataCode) {
			if failRoster.Load() {
				return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrFeatureUnavailable.Code()), nil), true
			}
			return clusterMetadataFrame(t, 0, fixture.coordinator.address()), true
		}
		if read.code() == uint32(command.GetPartitionRouteCode) {
			return pollRoutingReply(t, read, 0, 1), true
		}
		return nil, false
	}, ierror.ErrFeatureUnavailable)
	failRoster.Store(false)
	for range 2 {
		_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
		require.NoError(t, err)
	}
	assert.True(t, len(fixture.client.links.slots) > 0)
	assert.False(t, len(fixture.client.links.slots) > 1)
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.GetClusterMetadataCode))
	assert.Equal(t, 1, requestCount(fixture.coordinator.recorded(), command.GetPartitionRouteCode))
	assert.Zero(t, fixture.primaries[0].connections())
}

func TestPrimaryPoll_LostRouteQueryLeavesReconnectToTheManagedSlot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture := newPrimaryPollFixture(t, nil, func(connection int, read request) ([]byte, bool) {
		if connection == 0 && read.code() == uint32(command.GetPartitionRouteCode) {
			cancel()
			return nil, true
		}
		return nil, false
	})
	previous := fixture.client.session.ClientID()
	session := fixture.client.session.SessionID()
	_, err := pollPrimaryPartition(ctx, fixture.client, 0)
	require.Error(t, err)
	require.Eventually(t, func() bool {
		return fixture.coordinator.connections() == 2 && fixture.client.links.replica(0) != nil
	}, time.Second, time.Millisecond, "the managed slot reconnects after the caller ends")
	assert.Equal(t, previous, fixture.client.session.ClientID())
	assert.Equal(t, session, fixture.client.session.SessionID())
	registers := 0
	for _, read := range fixture.coordinator.recorded() {
		if read.operation() == vsr.OperationRegister {
			registers++
		}
	}
	assert.Equal(t, 1, registers)
	assert.Equal(t, 1, requestCount(fixture.coordinator.recorded(), command.BindSessionCode))
	assert.Equal(t, 1, requestCount(fixture.coordinator.recorded(), command.GetPartitionRouteCode))
	assert.LessOrEqual(t, fixture.primaries[0].connections(), 2)
	for _, primary := range fixture.primaries {
		assert.Zero(t, requestCount(primary.recorded(), command.PollMessagesCode))
		for _, read := range primary.recorded() {
			assert.NotEqual(t, vsr.OperationRegister, read.operation())
		}
	}
}

func TestPrimaryPoll_ManualIdentityAndChangedCredentialsBindNewDataConnections(t *testing.T) {
	fixture := newPrimaryPollFixture(t, nil, nil)
	_, err := fixture.client.LoginUser(context.Background(), "manual", "manual-secret")
	require.NoError(t, err)
	_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	newName := "renamed"
	require.NoError(t, fixture.client.UpdateUser(context.Background(), numericIdentifier(t, 7), &newName, nil))
	named, err := iggcon.NewIdentifier(newName)
	require.NoError(t, err)
	require.NoError(t, fixture.client.ChangePassword(context.Background(), named, "manual-secret", "changed-secret"))
	_, err = pollPrimaryPartition(context.Background(), fixture.client, 1)
	require.NoError(t, err)
	for index, primary := range fixture.primaries {
		_, err = pollPrimaryPartition(context.Background(), fixture.client, uint32(index))
		require.NoError(t, err)
		payload, err := vsr.SerializeBindSession(
			(consumerSession{client: fixture.client.session.ClientID(), session: fixture.client.session.SessionID(), watermark: 11}).bytes(), iggcon.Version, fixture.client.session.BindSecret())
		require.NoError(t, err)
		var lastBind []byte
		for _, read := range primary.recorded() {
			if read.code() == uint32(command.BindSessionCode) {
				lastBind = read.payload
			}
		}
		assert.Equal(t, payload, lastBind)
	}
	configured, ok := fixture.client.signInCredentials()
	require.True(t, ok)
	assert.Equal(t, NewUsernamePasswordCredentials("iggy", "secret"), configured,
		"a manual user's update must not change the configured reconnect identity")
}

func TestPrimaryPoll_ConfiguredCredentialsFollowSuccessfulSelfUpdates(t *testing.T) {
	fixture := newPrimaryPollFixture(t, nil, nil)
	newName := "renamed"
	require.NoError(t, fixture.client.UpdateUser(context.Background(), numericIdentifier(t, 7), &newName, nil))
	name, err := iggcon.NewIdentifier(newName)
	require.NoError(t, err)
	require.NoError(t, fixture.client.ChangePassword(context.Background(), name, "secret", "changed-secret"))
	_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	expected := NewUsernamePasswordCredentials(newName, "changed-secret")
	configured, ok := fixture.client.signInCredentials()
	require.True(t, ok)
	assert.Equal(t, expected, configured)
	payload, err := vsr.SerializeBindSession((consumerSession{client: fixture.client.session.ClientID(), session: fixture.client.session.SessionID(), watermark: 11}).bytes(),
		iggcon.Version, fixture.client.session.BindSecret())
	require.NoError(t, err)
	var lastBind []byte
	for _, read := range fixture.primaries[0].recorded() {
		if read.code() == uint32(command.BindSessionCode) {
			lastBind = read.payload
		}
	}
	assert.Equal(t, payload, lastBind)
}

func TestPrimaryPoll_StaleBindingRebindsTheSameSessionBeforePolling(t *testing.T) {
	var refusedSession atomic.Uint64
	fixture := newPrimaryPollFixture(t, func(primary, connection int, read request) ([]byte, bool) {
		if primary == 0 && connection == 0 && read.code() == uint32(command.BindSessionCode) {
			refusedSession.Store(read.sessionID())
			return evictionFrame(vsr.EvictionStaleClient, 0, 0), true
		}
		return nil, false
	}, nil)
	require.Eventually(t, func() bool {
		return fixture.client.links.replica(0) != nil && fixture.client.links.replica(1) != nil
	}, time.Second, time.Millisecond)
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, fixture.primaries[0].connections())
	assert.Equal(t, 1, fixture.coordinator.connections(), "the session outlives the stale connection")
	reads := fixture.primaries[0].recorded()
	assert.Equal(t, 1, requestCount(reads, command.PollMessagesCode),
		"attachment failure is retried before any auto-commit poll is admitted")
	require.NotZero(t, refusedSession.Load())
	for _, read := range reads {
		switch read.code() {
		case uint32(command.BindSessionCode), uint32(command.PollMessagesCode):
			assert.Equal(t, refusedSession.Load(), read.sessionID(), "code %d", read.code())
		}
	}
}

func TestPrimaryPoll_InvalidBindingDoesNotPollAndRetiresConnection(t *testing.T) {
	for _, test := range []struct {
		name  string
		reply func(request) []byte
		err   error
	}{
		{
			name: "mismatched session",
			reply: func(read request) []byte {
				return bindReplyFrame(7, read.sessionID()+1)
			},
			err: ierror.ErrSessionMismatch,
		},
		{
			name: "missing response body",
			reply: func(request) []byte {
				return replyFrame(vsr.OperationNonReplicated, nil)
			},
			err: vsr.ErrTruncatedRegisterReply,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPrimaryPollFixture(t, func(primary, _ int, read request) ([]byte, bool) {
				if primary == 0 && read.code() == uint32(command.BindSessionCode) {
					return test.reply(read), true
				}
				return nil, false
			}, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, err := pollPrimaryPartition(ctx, fixture.client, 0)
			require.Error(t, err)
			require.Zero(t, requestCount(fixture.primaries[0].recorded(), command.PollMessagesCode))
			require.Greater(t, requestCount(fixture.primaries[0].recorded(), command.BindSessionCode), 0)
			require.Nil(t, fixture.client.links.replica(1), "a rejected bind never publishes a ready link")
		})
	}
}

// A backup refuses every poll, so a clustered poll without auto-commit routes
// to the primary as well.
func TestPrimaryPoll_PlainConsumerAndNonAutoCommitPollsRoute(t *testing.T) {
	fixture := newPrimaryPollFixture(t, nil, nil)
	stream, topic, group := groupConsumer(t)
	consumer := iggcon.NewSingleConsumer(group.Id)
	partition := uint32(0)
	_, err := fixture.client.PollMessages(context.Background(), stream, topic, consumer,
		iggcon.NextPollingStrategy(), 1, true, &partition)
	require.NoError(t, err)
	_, err = fixture.client.PollMessages(context.Background(), stream, topic, consumer,
		iggcon.NextPollingStrategy(), 1, true, nil)
	require.NoError(t, err)
	_, err = fixture.client.PollMessages(context.Background(), stream, topic, consumer,
		iggcon.NextPollingStrategy(), 1, false, &partition)
	require.NoError(t, err)
	assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
	assert.Equal(t, 3, requestCount(fixture.primaries[0].recorded(), command.PollMessagesCode))
}

func TestPrimaryPoll_WarmRouteDoesNotWaitForCoordinatorIOAndColdRouteHonorsDeadline(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	fixture := newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.GetStatsCode) {
			close(entered)
			<-release
			return replyFrame(vsr.OperationNonReplicated, nil), true
		}
		return nil, false
	})
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		_, err := fixture.client.SendBinaryRequest(ctx, uint32(command.GetStatsCode), nil)
		completed <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not receive the blocked request")
	}
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer pollCancel()
	_, err = pollPrimaryPartition(pollCtx, fixture.client, 0)
	require.NoError(t, err, "the warm data path does not take the coordinator exchange lock")
	coldCtx, coldCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer coldCancel()
	_, err = pollPrimaryPartition(coldCtx, fixture.client, 1)
	require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
	cancel()
	select {
	case err := <-completed:
		require.ErrorIs(t, err, ierror.ErrTransientNotCommitted)
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt the coordinator exchange")
	}
}

func TestCoordinatorRequests_RefetchTheRouteAfterARefusedContext(t *testing.T) {
	stream, topic, group := groupConsumer(t)
	consumer := iggcon.NewSingleConsumer(group.Id)
	partition := uint32(1)
	tests := []struct {
		name    string
		routing command.Code
		call    func(*IggyTcpClient) error
	}{
		{"poll", command.GetPartitionRouteCode, func(client *IggyTcpClient) error {
			_, err := client.PollMessages(context.Background(), stream, topic, consumer,
				iggcon.NextPollingStrategy(), 1, false, &partition)
			return err
		}},
		{"store offset", command.GetPartitionRouteCode, func(client *IggyTcpClient) error {
			return client.StoreConsumerOffset(context.Background(), consumer, stream, topic, 7, &partition)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			routes := uint64(0)
			server := serve(serverConn, func(_ int, read request) []byte {
				switch {
				case read.code() == uint32(test.routing):
					// Each route names a newer incarnation, so a request shows
					// which route it was stamped from.
					routes++
					answer := pollRoutingReply(t, read, 0, read.sessionID())
					binary.LittleEndian.PutUint64(answer[vsr.HeaderSize:], routes)
					return answer
				case read.incarnation() == 1:
					return statusReplyFrame(read.operation(), uint32(ierror.HistoryUnavailableCode), nil)
				case read.operation() == vsr.OperationStoreConsumerOffset:
					return replyFrame(vsr.OperationStoreConsumerOffset, resultSection())
				default:
					return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(partition))
				}
			})

			require.ErrorIs(t, test.call(client), ierror.ErrHistoryUnavailable)
			require.NoError(t, test.call(client), "the refused context must not be stamped again")
			assert.Equal(t, 2, requestCount(server.recorded(), test.routing))
		})
	}
}

// A context read at store time would let an offset polled before the
// partition was recreated commit into the new incarnation.
func TestStoreConsumerPosition_StampsTheContextOfThePoll(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationStoreConsumerOffset, resultSection())
	})
	stream, topic, consumer := groupConsumer(t)
	polled := iggcon.PartitionContext{
		Incarnation:     5,
		OwnerGeneration: 3,
		MetadataOp:      9,
	}
	position := iggcon.ConsumerPosition{PartitionId: 4, Offset: 42, Context: polled}

	require.NoError(t, client.StoreConsumerPosition(context.Background(), consumer, stream, topic, position))

	recorded := server.recorded()
	require.Len(t, recorded, 2, "the route chooses the primary but never replaces the captured context")
	var stamped iggcon.PartitionContext
	require.NoError(t, stamped.UnmarshalBinary(recorded[1].header[frameOffsetContext:frameOffsetContext+iggcon.PartitionContextSize]))
	assert.Equal(t, polled, stamped)
	want, err := (&command.StoreConsumerOffsetRequest{StreamId: stream, TopicId: topic, Consumer: consumer,
		Offset: position.Offset, PartitionId: &position.PartitionId}).MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, want, recorded[1].payload)
}

func stampedContext(t *testing.T, read request) iggcon.PartitionContext {
	t.Helper()
	var stamped iggcon.PartitionContext
	require.NoError(t, stamped.UnmarshalBinary(read.header[frameOffsetContext:frameOffsetContext+iggcon.PartitionContextSize]))
	return stamped
}

func rawSendPayload(t *testing.T, stream, topic iggcon.Identifier, partitioning iggcon.Partitioning) []byte {
	t.Helper()
	message, err := iggcon.NewIggyMessage([]byte("raw"))
	require.NoError(t, err)
	payload, err := (&command.SendMessages{StreamId: stream, TopicId: topic, Partitioning: partitioning,
		Messages: []iggcon.IggyMessage{message}}).MarshalBinary()
	require.NoError(t, err)
	return payload
}

// A raw send must carry the context its typed counterpart would, and keep it
// on every attempt of that request.
func TestSendBinaryRequest_StampsTheSendContextOnEveryAttempt(t *testing.T) {
	client, serverConn := newPipeClient(t)
	sends := 0
	server := servePartitionOperations(t, serverConn, func(_ int, _ request) []byte {
		sends++
		if sends == 1 {
			return statusReplyFrame(vsr.OperationSendMessages, uint32(ierror.TransientNotAcceptedCode), nil)
		}
		return replyFrame(vsr.OperationSendMessages, binary.LittleEndian.AppendUint32(nil, 0))
	})
	stream, topic, _ := groupConsumer(t)
	partition := uint32(3)
	payload := rawSendPayload(t, stream, topic, iggcon.PartitionId(partition))

	_, err := client.SendBinaryRequest(context.Background(), uint32(command.SendMessagesCode), payload)
	require.NoError(t, err)

	recorded := server.recorded()
	require.Len(t, recorded, 4)
	require.Equal(t, uint32(command.GetPartitionRouteCode), recorded[0].code())
	encodedStream, err := stream.MarshalBinary()
	require.NoError(t, err)
	encodedTopic, err := topic.MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, binary.LittleEndian.AppendUint32(append(encodedStream, encodedTopic...), partition), recorded[0].payload)
	assert.Equal(t, recorded[1].requestID(), recorded[3].requestID())
	for _, send := range []request{recorded[1], recorded[3]} {
		assert.Equal(t, vsr.OperationSendMessages, send.operation())
		assert.Equal(t, payload, send.payload)
		assert.Equal(t, uint64(17), send.incarnation())
	}
}

// A raw send cannot pick a partition the way the typed send does, and a zero
// context is always refused, so nothing may reach the wire.
func TestSendBinaryRequest_RefusesASendWithoutAnExplicitPartition(t *testing.T) {
	stream, topic, _ := groupConsumer(t)
	for name, partitioning := range map[string]iggcon.Partitioning{
		"balanced": iggcon.None(),
		"key":      iggcon.EntityIdUlong(7),
	} {
		t.Run(name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			server := servePartitionOperations(t, serverConn, func(_ int, _ request) []byte {
				return replyFrame(vsr.OperationSendMessages, binary.LittleEndian.AppendUint32(nil, 0))
			})

			_, err := client.SendBinaryRequest(context.Background(), uint32(command.SendMessagesCode),
				rawSendPayload(t, stream, topic, partitioning))

			require.ErrorIs(t, err, ierror.ErrFeatureUnavailable)
			var coded ierror.IggyError
			require.ErrorAs(t, err, &coded)
			assert.Equal(t, ierror.FeatureUnavailableCode, coded.Code())
			assert.Contains(t, err.Error(), "explicit partition id")
			assert.Empty(t, server.recorded())
		})
	}
}

func TestSendBinaryRequest_StampsTheRouteContextOfPollsAndOffsetWrites(t *testing.T) {
	stream, topic, group := groupConsumer(t)
	consumer := iggcon.NewSingleConsumer(group.Id)
	partition := uint32(1)
	poll, err := (&command.PollMessages{StreamId: stream, TopicId: topic, Consumer: consumer,
		PartitionId: &partition, Strategy: iggcon.NextPollingStrategy(), Count: 1}).MarshalBinary()
	require.NoError(t, err)
	store, err := (&command.StoreConsumerOffsetRequest{StreamId: stream, TopicId: topic, Consumer: consumer,
		PartitionId: &partition, Offset: 7}).MarshalBinary()
	require.NoError(t, err)
	remove, err := (&command.DeleteConsumerOffset{StreamId: stream, TopicId: topic, Consumer: consumer,
		PartitionId: &partition}).MarshalBinary()
	require.NoError(t, err)
	offsetRoute, err := (&command.GetConsumerOffset{StreamId: stream, TopicId: topic, Consumer: consumer,
		PartitionId: &partition}).MarshalBinary()
	require.NoError(t, err)
	tests := []struct {
		name         string
		code         command.Code
		payload      []byte
		routing      command.Code
		routePayload []byte
		reply        []byte
	}{
		{"poll", command.PollMessagesCode, poll, command.GetPartitionRouteCode, poll,
			replyFrame(vsr.OperationNonReplicated, emptyBatchBody(partition))},
		{"store offset", command.StoreOffsetCode, store, command.GetPartitionRouteCode, offsetRoute,
			replyFrame(vsr.OperationStoreConsumerOffset, resultSection())},
		{"delete offset", command.DeleteConsumerOffsetCode, remove, command.GetPartitionRouteCode, offsetRoute,
			replyFrame(vsr.OperationDeleteConsumerOffset, resultSection())},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			server := serve(serverConn, func(_ int, read request) []byte {
				if read.code() == uint32(test.routing) {
					answer := pollRoutingReply(t, read, 0, read.sessionID())
					binary.LittleEndian.PutUint64(answer[vsr.HeaderSize:], 17)
					return answer
				}
				return append([]byte(nil), test.reply...)
			})

			_, err := client.SendBinaryRequest(context.Background(), uint32(test.code), test.payload)
			require.NoError(t, err)

			recorded := server.recorded()
			require.Len(t, recorded, 2)
			assert.Equal(t, uint32(test.routing), recorded[0].code())
			query, err := partitionQuery(test.code, test.payload)
			require.NoError(t, err)
			assert.Equal(t, query, recorded[0].payload)
			assert.Equal(t, test.payload, recorded[1].payload)
			assert.Equal(t, uint64(17), recorded[1].incarnation())
		})
	}
}

// A continuation offset is only meaningful in the incarnation that produced
// it, so a caller context is stamped as is and the route's is never used.
func TestPollMessages_StampsTheCallerContext(t *testing.T) {
	stream, topic, group := groupConsumer(t)
	caller := iggcon.PartitionContext{Incarnation: 5, OwnerGeneration: 3, MetadataOp: 9}
	strategy := iggcon.OffsetPollingStrategy(42).WithContext(caller)
	partition := uint32(0)

	t.Run("standalone", func(t *testing.T) {
		client, serverConn := newPipeClient(t)
		server := servePartitionOperations(t, serverConn, func(_ int, _ request) []byte {
			return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(partition))
		})

		_, err := client.PollMessages(context.Background(), stream, topic, iggcon.NewSingleConsumer(group.Id),
			strategy, 1, false, &partition)
		require.NoError(t, err)

		recorded := server.recorded()
		require.Len(t, recorded, 2, "a caller context still needs a primary route")
		assert.Equal(t, caller, stampedContext(t, recorded[1]))
	})
	t.Run("primary", func(t *testing.T) {
		stamped := make(chan iggcon.PartitionContext, 1)
		fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
			if read.code() == uint32(command.PollMessagesCode) {
				stamped <- stampedContext(t, read)
			}
			return nil, false
		}, nil)

		_, err := fixture.client.PollMessages(context.Background(), stream, topic, group, strategy, 1, true, &partition)
		require.NoError(t, err)

		assert.Equal(t, caller, <-stamped)
	})
}

// Rust returns the fence of a group poll that carries a caller context: the
// owner it names is gone, so a retry under it would only be refused again.
func TestGroupPoll_ReturnsTheFenceOfACallerContext(t *testing.T) {
	var polls atomic.Int32
	fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
		if read.code() != uint32(command.PollMessagesCode) {
			return nil, false
		}
		polls.Add(1)
		return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ConsumerGroupPartitionNotOwnedCode), nil), true
	}, nil)
	stream, topic, consumer := groupConsumer(t)
	require.NoError(t, fixture.client.JoinConsumerGroup(context.Background(), stream, topic, consumer.Id))
	strategy := iggcon.NextPollingStrategy().WithContext(iggcon.PartitionContext{Incarnation: 5, OwnerGeneration: 3})

	_, err := fixture.client.PollMessages(context.Background(), stream, topic, consumer, strategy, 1, true, nil)

	require.ErrorIs(t, err, ierror.ErrConsumerGroupPartitionNotOwned)
	assert.Equal(t, int32(1), polls.Load())
}

// A write replayed on the coordinator walks the roster to the primary under a
// new client identity, which is no member of the group.
func TestOffsetWrites_GoToThePartitionPrimary(t *testing.T) {
	type write struct {
		operation vsr.Operation
		primary   int
		partition uint32
		context   iggcon.PartitionContext
		request   uint64
	}
	var mtx sync.Mutex
	var writes []write
	fixture := newPrimaryPollFixture(t, func(primary, _ int, read request) ([]byte, bool) {
		if read.operation() != vsr.OperationStoreConsumerOffset && read.operation() != vsr.OperationDeleteConsumerOffset {
			return nil, false
		}
		mtx.Lock()
		writes = append(writes, write{read.operation(), primary, polledPartition(t, read), stampedContext(t, read), read.requestID()})
		mtx.Unlock()
		return replyFrame(read.operation(), resultSection()), true
	}, func(_ int, read request) ([]byte, bool) {
		if read.code() != uint32(command.GetPartitionRouteCode) {
			return nil, false
		}
		answer := pollRoutingReply(t, read, uint8(routedPollPartition(t, read)+1), 1)
		binary.LittleEndian.PutUint64(answer[vsr.HeaderSize:], 17)
		return answer, true
	})
	stream, topic, consumer := groupConsumer(t)
	partition := uint32(1)
	position := iggcon.ConsumerPosition{PartitionId: 0, Offset: 9,
		Context: iggcon.PartitionContext{Incarnation: 5, OwnerGeneration: 3, MetadataOp: 9}}

	require.NoError(t, fixture.client.StoreConsumerOffset(context.Background(), consumer, stream, topic, 7, &partition))
	require.NoError(t, fixture.client.StoreConsumerPosition(context.Background(), consumer, stream, topic, position))
	require.NoError(t, fixture.client.DeleteConsumerOffset(context.Background(), consumer, stream, topic, &partition))

	for _, read := range fixture.coordinator.recorded() {
		assert.NotEqual(t, vsr.OperationStoreConsumerOffset, read.operation(), "an offset write must not run on the coordinator")
		assert.NotEqual(t, vsr.OperationDeleteConsumerOffset, read.operation(), "an offset write must not run on the coordinator")
	}
	fixture.client.mtx.Lock()
	next := fixture.client.session.CurrentRequestID()
	fixture.client.mtx.Unlock()
	routed := iggcon.PartitionContext{Incarnation: 17, MetadataOp: 1}
	mtx.Lock()
	defer mtx.Unlock()
	// The data connection shares the coordinator's session, so its writes take
	// the next ids of that session's counter.
	require.Len(t, writes, 3)
	assert.Less(t, writes[0].request, writes[1].request)
	assert.Less(t, writes[1].request, writes[2].request)
	assert.Equal(t, []write{
		{vsr.OperationStoreConsumerOffset, 1, 1, routed, writes[0].request},
		{vsr.OperationStoreConsumerOffset, 0, 0, position.Context, writes[1].request},
		{vsr.OperationDeleteConsumerOffset, 1, 1, routed, next - 1},
	}, writes)
}

func TestPartitionRoutingNotPrimaryRefreshesWithoutChangingRequestIdentity(t *testing.T) {
	var routes atomic.Uint32
	var attempts []request
	var mu sync.Mutex
	fixture := newPrimaryPollFixture(t, func(primary, _ int, read request) ([]byte, bool) {
		if read.code() != uint32(command.PollMessagesCode) {
			return nil, false
		}
		mu.Lock()
		attempts = append(attempts, read)
		mu.Unlock()
		if primary == 0 {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.NotPrimaryCode), nil), true
		}
		return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(0)), true
	}, func(_ int, read request) ([]byte, bool) {
		if read.code() != uint32(command.GetPartitionRouteCode) {
			return nil, false
		}
		count := routes.Add(1)
		reply := pollRoutingReply(t, read, uint8(min(count, 2)), 1)
		binary.LittleEndian.PutUint32(reply[vsr.HeaderSize+16:], count-1)
		return reply, true
	})
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	require.Equal(t, uint32(2), routes.Load())
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, attempts, 2)
	assert.Equal(t, attempts[0].requestID(), attempts[1].requestID())
	assert.Equal(t, attempts[0].clientID(), attempts[1].clientID())
	assert.Equal(t, attempts[0].sessionID(), attempts[1].sessionID())
	assert.Equal(t, stampedContext(t, attempts[0]), stampedContext(t, attempts[1]))
	assert.Equal(t, 1, fixture.coordinator.connections())
}

func TestPartitionMutationReplays57OnTheSameLinkAndPreservesUncertainty(t *testing.T) {
	const watchdogTimeout = 5 * time.Second
	for _, finalStatus := range []ierror.Code{0, ierror.TransientNotAcceptedCode, ierror.NotPrimaryCode,
		ierror.HistoryUnavailableCode, ierror.ConsumerGroupPartitionNotOwnedCode, ierror.InvalidCredentialsCode} {
		t.Run(fmt.Sprint(finalStatus), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), watchdogTimeout)
			defer cancel()
			stopAfterReplay := finalStatus == ierror.TransientNotAcceptedCode || finalStatus == ierror.NotPrimaryCode
			var writes atomic.Uint32
			fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
				if read.operation() != vsr.OperationSendMessages {
					return nil, false
				}
				if writes.Add(1) == 1 {
					return statusReplyFrame(vsr.OperationSendMessages, uint32(ierror.TransientNotCommittedCode), nil), true
				}
				if finalStatus != 0 {
					return statusReplyFrame(vsr.OperationSendMessages, uint32(finalStatus), nil), true
				}
				return replyFrame(vsr.OperationSendMessages, zeroConfirmations()), true
			}, func(_ int, read request) ([]byte, bool) {
				if read.code() != uint32(command.GetPartitionRouteCode) || writes.Load() < 2 {
					return nil, false
				}
				if stopAfterReplay {
					// This lookup proves the replayed refusal was classified before cancellation.
					cancel()
				}
				// The next mutation must meet the NotPrimary view floor before reaching the retained lane.
				reply := pollRoutingReply(t, read, 1, max(1, binary.LittleEndian.Uint64(read.header[frameOffsetContext+16:])))
				binary.LittleEndian.PutUint32(reply[vsr.HeaderSize+16:], 1)
				return reply, true
			})
			stream, topic, _ := groupConsumer(t)
			_, err := fixture.client.SendBinaryRequest(ctx, uint32(command.SendMessagesCode), rawSendPayload(t, stream, topic, iggcon.PartitionId(0)))
			require.NotErrorIs(t, ctx.Err(), context.DeadlineExceeded, "the watchdog must not terminate the replay scenario")
			if finalStatus == 0 {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ierror.ErrUncertainOutcome)
				var uncertain ierror.UncertainOutcome
				require.ErrorAs(t, err, &uncertain)
				require.NotZero(t, uncertain.OperationID)
				nextCtx, nextCancel := context.WithTimeout(context.Background(), watchdogTimeout)
				defer nextCancel()
				_, err = fixture.client.SendBinaryRequest(nextCtx, uint32(command.SendMessagesCode), rawSendPayload(t, stream, topic, iggcon.PartitionId(0)))
				var blocked ierror.LaneBlocked
				require.ErrorAs(t, err, &blocked)
				assert.Equal(t, uncertain.OperationID, blocked.OperationID)
			}
			assert.Equal(t, uint32(2), writes.Load())
			assert.GreaterOrEqual(t, requestCount(fixture.coordinator.recorded(), command.GetPartitionRouteCode), 1)
			var sent []request
			for _, read := range fixture.primaries[0].recorded() {
				if read.operation() == vsr.OperationSendMessages {
					sent = append(sent, read)
				}
			}
			require.Len(t, sent, 2)
			assert.Equal(t, sent[0].header, sent[1].header)
			assert.Equal(t, sent[0].payload, sent[1].payload)
			assert.Equal(t, 1, fixture.primaries[0].connections())
		})
	}
}

func TestPartitionRoutingUsesReplicaIDInsteadOfRosterPosition(t *testing.T) {
	var fixture *primaryPollFixture
	var reordered atomic.Bool
	fixture = newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.GetClusterMetadataCode) && reordered.Load() {
			frame := clusterMetadataFrame(t, 1, fixture.primaries[0].address(), fixture.coordinator.address(), fixture.primaries[1].address())
			var metadata iggcon.ClusterMetadata
			require.NoError(t, metadata.UnmarshalBinary(frame[vsr.HeaderSize:]))
			metadata.Nodes[0].ReplicaID = 2
			metadata.Nodes[1].ReplicaID = 0
			metadata.Nodes[2].ReplicaID = 1
			body, err := metadata.MarshalBinary()
			require.NoError(t, err)
			return replyFrame(vsr.OperationNonReplicated, body), true
		}
		if read.code() == uint32(command.GetPartitionRouteCode) {
			return pollRoutingReply(t, read, 2, 1), true
		}
		return nil, false
	})
	reordered.Store(true)
	_, err := fixture.client.GetClusterMetadata(context.Background())
	require.NoError(t, err)
	_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, requestCount(fixture.primaries[0].recorded(), command.PollMessagesCode))
	assert.LessOrEqual(t, fixture.primaries[1].connections(), 2)
}

func TestPartitionRoutingRejectsMissingMutationAndGroupPartitionsBeforeIO(t *testing.T) {
	stream, topic, group := groupConsumer(t)
	requests := []command.Command{
		&command.StoreConsumerOffsetRequest{StreamId: stream, TopicId: topic, Consumer: group},
		&command.DeleteConsumerOffset{StreamId: stream, TopicId: topic, Consumer: group},
		&command.PollMessages{StreamId: stream, TopicId: topic, Consumer: group, Strategy: iggcon.NextPollingStrategy(), Count: 1},
	}
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte { t.Error("invalid partition request reached server"); return nil })
	for _, request := range requests {
		body, err := request.MarshalBinary()
		require.NoError(t, err)
		_, err = client.SendBinaryRequest(context.Background(), uint32(request.Code()), body)
		require.ErrorIs(t, err, ierror.ErrInvalidCommand)
	}
	assert.Empty(t, server.recorded())
}

func TestPartitionRoutingSharesOneRouteAcrossSendsPollsAndOffsets(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.operation() == vsr.OperationNonReplicated {
			return replyFrame(read.operation(), nil)
		}
		return replyFrame(read.operation(), resultSection())
	})
	stream, topic, group := groupConsumer(t)
	consumer := iggcon.NewSingleConsumer(group.Id)
	partition := uint32(0)
	requests := []command.Command{
		rawRequest{code: command.SendMessagesCode, payload: rawSendPayload(t, stream, topic, iggcon.PartitionId(0))},
		&command.PollMessages{StreamId: stream, TopicId: topic, Consumer: consumer, Strategy: iggcon.NextPollingStrategy(), Count: 1},
		&command.GetConsumerOffset{StreamId: stream, TopicId: topic, Consumer: consumer},
		&command.StoreConsumerOffsetRequest{StreamId: stream, TopicId: topic, Consumer: consumer, PartitionId: &partition},
		&command.DeleteConsumerOffset{StreamId: stream, TopicId: topic, Consumer: consumer, PartitionId: &partition},
	}
	for _, request := range requests {
		_, err := client.do(context.Background(), request)
		require.NoError(t, err)
	}
	reads := server.recorded()
	assert.Equal(t, 1, requestCount(reads, command.GetPartitionRouteCode))
	seen := map[uint64]bool{}
	for _, read := range reads {
		assert.False(t, seen[read.requestID()], "all bound exchanges share unique ids")
		seen[read.requestID()] = true
		if read.code() != uint32(command.GetPartitionRouteCode) {
			assert.Zero(t, stampedContext(t, read).OwnerGeneration)
		}
	}
}

func TestPartitionRouteUnknownReplicaRefreshesRosterOncePerLookup(t *testing.T) {
	var routes atomic.Uint32
	fixture := newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
		if read.code() != uint32(command.GetPartitionRouteCode) {
			return nil, false
		}
		routes.Add(1)
		return pollRoutingReply(t, read, 42, 1), true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := pollPrimaryPartition(ctx, fixture.client, 0)
	require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
	assert.Equal(t, uint32(1), routes.Load())
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.GetClusterMetadataCode), "initial roster plus one refresh")
	for _, primary := range fixture.primaries {
		assert.LessOrEqual(t, primary.connections(), 1)
	}
}

func TestPartitionRoutingDoesNotRetryLifecycleRefusalsFromControlRequests(t *testing.T) {
	for _, code := range []command.Code{command.GetPartitionRouteCode, command.BindSessionCode} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			refuse := func(read request) ([]byte, bool) {
				if read.code() == uint32(code) {
					return statusReplyFrame(read.operation(), uint32(ierror.LifecycleBusyCode), nil), true
				}
				return nil, false
			}
			fixture := newPrimaryPollFixture(t,
				func(_, _ int, read request) ([]byte, bool) { return refuse(read) },
				func(_ int, read request) ([]byte, bool) { return refuse(read) })
			stream, topic, _ := groupConsumer(t)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, err := fixture.client.SendBinaryRequest(ctx, uint32(command.SendMessagesCode),
				rawSendPayload(t, stream, topic, iggcon.PartitionId(0)))
			if code == command.GetPartitionRouteCode {
				require.ErrorIs(t, err, ierror.ErrLifecycleBusy)
			} else {
				require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
			}
			reads := append(fixture.coordinator.recorded(), fixture.primaries[0].recorded()...)
			assert.GreaterOrEqual(t, requestCount(reads, code), 1)
			for _, read := range reads {
				assert.NotEqual(t, vsr.OperationSendMessages, read.operation())
			}
		})
	}
}

func TestPrimaryMutationLifecycleRetriesKeepContextAndUseNewRequestIDs(t *testing.T) {
	var attempts, routes atomic.Uint32
	fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
		if read.operation() != vsr.OperationSendMessages {
			return nil, false
		}
		if attempts.Add(1) < 3 {
			return statusReplyFrame(read.operation(), uint32(ierror.LifecycleBusyCode), nil), true
		}
		return replyFrame(read.operation(), resultSection()), true
	}, func(_ int, read request) ([]byte, bool) {
		if read.code() != uint32(command.GetPartitionRouteCode) {
			return nil, false
		}
		reply := pollRoutingReply(t, read, 1, 1)
		binary.LittleEndian.PutUint64(reply[vsr.HeaderSize:], uint64(routes.Add(1)))
		return reply, true
	})
	stream, topic, _ := groupConsumer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := fixture.client.SendBinaryRequest(ctx, uint32(command.SendMessagesCode),
		rawSendPayload(t, stream, topic, iggcon.PartitionId(0)))
	require.NoError(t, err)
	require.Equal(t, uint32(3), attempts.Load())
	seen := map[uint64]bool{}
	for _, read := range fixture.primaries[0].recorded() {
		if read.operation() == vsr.OperationSendMessages {
			assert.False(t, seen[read.requestID()])
			seen[read.requestID()] = true
			assert.Equal(t, uint64(1), read.incarnation())
		}
	}
}

func TestMetadataNotPrimaryRechecksLeaderWithoutWalkingTheRoster(t *testing.T) {
	var attempts atomic.Uint32
	fixture := newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
		if read.operation() != vsr.OperationCreateStream {
			return nil, false
		}
		if attempts.Add(1) == 1 {
			return statusReplyFrame(read.operation(), uint32(ierror.NotPrimaryCode), nil), true
		}
		return replyFrame(read.operation(), resultSection()), true
	})
	_, err := fixture.client.do(context.Background(), &command.CreateStream{Name: "orders"})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), attempts.Load())
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.GetClusterMetadataCode))
	assert.Equal(t, 1, fixture.coordinator.connections())
	for _, primary := range fixture.primaries {
		assert.LessOrEqual(t, primary.connections(), 1)
	}
}

func TestPartitionRouteAliasesHonorHighestViewAndRefusalFloor(t *testing.T) {
	client, peer := newPipeClient(t)
	var view atomic.Uint32
	serve(peer, func(_ int, read request) []byte {
		reply := pollRoutingReply(t, read, 0, 1)
		binary.LittleEndian.PutUint64(reply[vsr.HeaderSize:], 17)
		binary.LittleEndian.PutUint32(reply[vsr.HeaderSize+16:], view.Load())
		return reply
	})
	stream, topic, _ := groupConsumer(t)
	name, err := iggcon.NewIdentifier("orders")
	require.NoError(t, err)
	query := func(topic iggcon.Identifier) []byte {
		key := newTopicKey(stream, topic)
		return binary.LittleEndian.AppendUint32([]byte(key.stream+key.topic), 0)
	}
	first, err := client.consumerRoute(context.Background(), query(topic))
	require.NoError(t, err)
	view.Store(1)
	newer, err := client.consumerRoute(context.Background(), query(name))
	require.NoError(t, err)
	require.Equal(t, first.key, newer.key)
	view.Store(0)
	_, err = client.consumerRoute(context.Background(), query(topic))
	require.ErrorIs(t, err, ierror.ErrTransientNotAccepted, "a cached alias cannot reuse an older group view")
	client.polls.invalidate(newer, true)
	client.polls.clearRoutes()
	view.Store(1)
	_, err = client.consumerRoute(context.Background(), query(name))
	require.ErrorIs(t, err, ierror.ErrTransientNotAccepted, "metadata invalidation cannot erase a refusal floor")
	view.Store(2)
	current, err := client.consumerRoute(context.Background(), query(topic))
	require.NoError(t, err)
	require.Equal(t, uint32(2), current.view)
}

func TestPartitionRouteCacheRemainsBoundedAcrossGroupChurn(t *testing.T) {
	client, peer := newPipeClient(t)
	serve(peer, func(_ int, read request) []byte {
		reply := pollRoutingReply(t, read, 0, 1)
		partition := binary.LittleEndian.Uint32(read.payload[len(read.payload)-partitionIdSize:])
		binary.LittleEndian.PutUint64(reply[vsr.HeaderSize:], uint64(partition)+1)
		return reply
	})
	stream, topic, _ := groupConsumer(t)
	key := newTopicKey(stream, topic)
	for partition := 0; partition <= maxPollRoutes; partition++ {
		query := binary.LittleEndian.AppendUint32([]byte(key.stream+key.topic), uint32(partition))
		_, err := client.consumerRoute(context.Background(), query)
		require.NoError(t, err)
	}
	require.Len(t, client.polls.routes, maxPollRoutes)
	require.Len(t, client.polls.hints, maxPollRoutes)
}

func TestPartitionRouteFromRetiredSessionCannotPopulateCachesOrWatermark(t *testing.T) {
	client, peer := newPipeClient(t)
	client.metadataWatermark.Store(7)
	fence := client.sessionFence
	received, release := make(chan struct{}), make(chan struct{})
	serve(peer, func(_ int, read request) []byte {
		close(received)
		<-release
		reply := pollRoutingReply(t, read, 0, 99)
		binary.LittleEndian.PutUint64(reply[testMetadataCommitOffset:], 99)
		return reply
	})
	stream, topic, _ := groupConsumer(t)
	key := newTopicKey(stream, topic)
	query := binary.LittleEndian.AppendUint32([]byte(key.stream+key.topic), 0)
	done := make(chan error, 1)
	go func() { _, err := client.consumerRoute(context.Background(), query); done <- err }()
	<-received
	requestID := client.session.CurrentRequestID()
	client.resetSession(false)
	close(release)
	require.ErrorIs(t, <-done, ierror.ErrUnauthenticated)
	require.Empty(t, client.polls.routes)
	require.Empty(t, client.topics.partitionsCounts)
	require.Equal(t, uint64(7), client.metadataWatermark.Load())
	require.Equal(t, fence+1, client.sessionFence)
	require.Equal(t, requestID, client.session.CurrentRequestID())
	credentials, remembered := client.signInCredentials()
	require.True(t, remembered)
	require.Equal(t, "iggy", credentials.username)
}

func TestUncachedPartitionLookupSkipsSilentMetadataReplica(t *testing.T) {
	for _, shortFirstCall := range []bool{false, true} {
		t.Run(fmt.Sprint(shortFirstCall), func(t *testing.T) {
			fixture := newPrimaryPollFixture(t, func(primary, _ int, read request) ([]byte, bool) {
				if read.code() == uint32(command.GetPartitionRouteCode) {
					return pollRoutingReply(t, read, uint8(primary+1), 1), true
				}
				return nil, false
			}, func(_ int, read request) ([]byte, bool) {
				return []byte{}, read.code() == uint32(command.GetPartitionRouteCode)
			})
			require.Eventually(t, func() bool { return fixture.client.links.replica(1) != nil }, time.Second, time.Millisecond)
			if shortFirstCall {
				ctx, cancel := context.WithTimeout(context.Background(), managedLinkHeartbeat)
				_, err := pollPrimaryPartition(ctx, fixture.client, 0)
				cancel()
				require.Error(t, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), partitionLookupTimeout+time.Second)
			defer cancel()
			_, err := pollPrimaryPartition(ctx, fixture.client, 0)
			require.NoError(t, err)
			require.Equal(t, 1, requestCount(fixture.coordinator.recorded(), command.GetPartitionRouteCode))
			require.True(t, linkReady(fixture.client.links.metadata()))
		})
	}
}

func TestPartitionLookupUsesReadyReplicaWhenMetadataCannotAnswer(t *testing.T) {
	var requests atomic.Uint32
	fixture := newPrimaryPollFixture(t, func(primary, _ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.GetPartitionRouteCode) {
			requests.Add(1)
			return pollRoutingReply(t, read, uint8(primary+1), 1), true
		}
		return nil, false
	}, func(_ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.GetPartitionRouteCode) {
			return statusReplyFrame(read.operation(), uint32(ierror.TransientNotAcceptedCode), nil), true
		}
		return nil, false
	})
	require.Eventually(t, func() bool { return fixture.client.links.replica(1) != nil }, time.Second, time.Millisecond)
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	require.Equal(t, uint32(1), requests.Load())
	require.Equal(t, 1, requestCount(fixture.coordinator.recorded(), command.GetPartitionRouteCode))
	require.Equal(t, 1, requestCount(fixture.primaries[0].recorded(), command.PollMessagesCode))
}

func TestPartitionLookupDistinguishesLaggingReplicasFromMissingNamespaces(t *testing.T) {
	for _, missing := range []ierror.IggyError{ierror.ErrStreamIdNotFound, ierror.ErrStreamNameNotFound,
		ierror.ErrTopicIdNotFound, ierror.ErrTopicNameNotFound, ierror.ErrPartitionNotFound} {
		t.Run(fmt.Sprint(missing.Code()), func(t *testing.T) {
			t.Run("lagging_backup", func(t *testing.T) {
				var primaryLookups atomic.Uint32
				fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
					if read.code() == uint32(command.GetPartitionRouteCode) {
						return statusReplyFrame(read.operation(), uint32(missing.Code()), nil), true
					}
					return nil, false
				}, func(_ int, read request) ([]byte, bool) {
					if read.code() == uint32(command.GetPartitionRouteCode) && primaryLookups.Add(1) == 1 {
						return statusReplyFrame(read.operation(), uint32(ierror.TransientNotAcceptedCode), nil), true
					}
					return nil, false
				})
				require.Eventually(t, func() bool {
					return fixture.client.links.replica(1) != nil && fixture.client.links.replica(2) != nil
				}, managedLinkTestBudget, managedLinkPoll)
				ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
				defer cancel()

				_, err := pollPrimaryPartition(ctx, fixture.client, 0)

				require.NoError(t, err)
				assert.Equal(t, uint32(2), primaryLookups.Load())
				for _, replica := range fixture.primaries {
					assert.Equal(t, 1, requestCount(replica.recorded(), command.GetPartitionRouteCode))
				}
				assert.Equal(t, 1, requestCount(fixture.primaries[0].recorded(), command.PollMessagesCode))
				assert.Zero(t, requestCount(fixture.primaries[1].recorded(), command.PollMessagesCode))
			})
			t.Run("metadata_primary", func(t *testing.T) {
				fixture := newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
					if read.code() == uint32(command.GetPartitionRouteCode) {
						return statusReplyFrame(read.operation(), uint32(missing.Code()), nil), true
					}
					return nil, false
				})
				ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
				defer cancel()

				_, err := pollPrimaryPartition(ctx, fixture.client, 0)

				require.ErrorIs(t, err, missing)
				assert.Equal(t, 1, requestCount(fixture.coordinator.recorded(), command.GetPartitionRouteCode))
				assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
				for _, replica := range fixture.primaries {
					assert.Zero(t, requestCount(replica.recorded(), command.GetPartitionRouteCode))
					assert.Zero(t, requestCount(replica.recorded(), command.PollMessagesCode))
				}
			})
		})
	}
}

func TestPartitionRouteCannotReplaceANewerMetadataFloorOrPartitionCount(t *testing.T) {
	client, peer := newPipeClient(t)
	received, release := make(chan struct{}), make(chan struct{})
	serve(peer, func(_ int, read request) []byte {
		close(received)
		<-release
		return pollRoutingReply(t, read, 0, 7)
	})
	stream, topic, _ := groupConsumer(t)
	key := newTopicKey(stream, topic)
	query := binary.LittleEndian.AppendUint32([]byte(key.stream+key.topic), 0)
	done := make(chan error, 1)
	go func() { _, err := client.consumerRoute(context.Background(), query); done <- err }()
	<-received
	client.metadataWatermark.Store(10)
	client.topics.setPartitionsCount(key, 3)
	close(release)
	require.ErrorIs(t, <-done, ierror.ErrTransientNotAccepted)
	require.Empty(t, client.polls.routes)
	count, cached := client.topics.partitionsCount(key)
	require.True(t, cached)
	require.Equal(t, uint32(3), count)
}

func TestPartitionLookupRotatesRosterOrderAndPrefersRefusingReplicaOnce(t *testing.T) {
	var phase atomic.Uint32
	var mutex sync.Mutex
	var visited []uint8
	answer := func(replica uint8, read request) ([]byte, bool) {
		if read.code() != uint32(command.GetPartitionRouteCode) {
			return nil, false
		}
		mutex.Lock()
		visited = append(visited, replica)
		mutex.Unlock()
		if phase.Load() == 2 {
			return statusReplyFrame(read.operation(), uint32(ierror.TransientNotAcceptedCode), nil), true
		}
		reply := pollRoutingReply(t, read, 2, 1)
		binary.LittleEndian.PutUint32(reply[vsr.HeaderSize+16:], phase.Load())
		return reply, true
	}
	fixture := newPrimaryPollFixture(t, func(primary, _ int, read request) ([]byte, bool) {
		return answer(uint8(primary+1), read)
	}, func(_ int, read request) ([]byte, bool) { return answer(0, read) })
	require.Eventually(t, func() bool {
		return fixture.client.links.replica(1) != nil && fixture.client.links.replica(2) != nil
	}, time.Second, time.Millisecond)
	metadataFrame := clusterMetadataFrame(t, 0, fixture.coordinator.address(), fixture.primaries[0].address(), fixture.primaries[1].address())
	var metadata iggcon.ClusterMetadata
	require.NoError(t, metadata.UnmarshalBinary(metadataFrame[vsr.HeaderSize:]))
	metadata.Nodes[1], metadata.Nodes[2] = metadata.Nodes[2], metadata.Nodes[1]
	require.NoError(t, fixture.client.links.install(&metadata))
	stream, topic, _ := groupConsumer(t)
	key := newTopicKey(stream, topic)
	query := binary.LittleEndian.AppendUint32([]byte(key.stream+key.topic), 0)
	route, err := fixture.client.consumerRoute(context.Background(), query)
	require.NoError(t, err)
	phase.Store(1)
	fixture.client.polls.invalidate(route, false)
	route, err = fixture.client.consumerRoute(context.Background(), query)
	require.NoError(t, err)
	phase.Store(2)
	fixture.client.polls.invalidate(route, true)
	_, err = fixture.client.consumerRoute(context.Background(), query)
	require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
	phase.Store(3)
	_, err = fixture.client.consumerRoute(context.Background(), query)
	require.NoError(t, err)
	mutex.Lock()
	require.Equal(t, []uint8{0, 1, 2, 1, 0, 2}, visited)
	mutex.Unlock()
}
