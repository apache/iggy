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
	"testing/synctest"
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
				return registerReplyFrame(7, uint64(200+connection))
			case read.code() == uint32(command.AttachConsumerSessionCode):
				return replyFrame(vsr.OperationNonReplicated, nil)
			case read.code() == uint32(command.PollMessagesOnPrimaryCode):
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
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, uint64(100+connection))
		case read.sessionID() == 0:
			return evictionFrame(vsr.EvictionNoSession, 0, 0)
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, fixture.coordinator.address(), fixture.primaries[0].address(), fixture.primaries[1].address())
		case read.code() == uint32(command.SyncGroupCode):
			return replyFrame(vsr.OperationNonReplicated, assignmentBody(7, 0, 1))
		case read.code() == uint32(command.GetPollRoutingCode):
			partition := routedPollPartition(t, read)
			return pollRoutingReply(t, read, fixture.primaries[partition].address(), 1)
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
	require.NoError(t, fixture.client.Connect(context.Background()))
	return fixture
}

func pollRoutingReply(t *testing.T, read request, endpoint string, watermark uint64) []byte {
	t.Helper()
	metadata := clusterMetadataFrame(t, 0, endpoint)
	var decoded iggcon.ClusterMetadata
	require.NoError(t, decoded.UnmarshalBinary(metadata[vsr.HeaderSize:]))
	node, err := decoded.Nodes[0].MarshalBinary()
	require.NoError(t, err)
	parent := consumerSession{client: read.clientID(), session: read.sessionID(), watermark: watermark}
	return replyFrame(vsr.OperationNonReplicated, append(parent.bytes(), node...))
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
	require.GreaterOrEqual(t, len(read.payload), 24)
	if read.payload[19] == 0 {
		return 0
	}
	return polledPartition(t, read)
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
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.GetPollRoutingCode))
	assert.Equal(t, 1, requestCount(fixture.coordinator.recorded(), command.SyncGroupCode))
	assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
	for _, primary := range fixture.primaries {
		assert.Equal(t, 1, primary.connections())
		assert.Equal(t, 1, requestCount(primary.recorded(), command.AttachConsumerSessionCode))
		assert.Equal(t, 2, requestCount(primary.recorded(), command.PollMessagesOnPrimaryCode))
		for _, read := range primary.recorded() {
			if read.code() == uint32(command.AttachConsumerSessionCode) {
				assert.NotEqual(t, parent, read.clientID(), "the data connection authenticates independently")
				assert.Equal(t, (consumerSession{client: parent, session: session, watermark: 11}).bytes(), read.payload)
			}
		}
	}
}

func TestPrimaryPoll_MetadataReplyRefreshesRouteAndAttachment(t *testing.T) {
	fixture := newPrimaryPollFixture(t, nil, nil)
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	_, err = fixture.client.SendBinaryRequest(context.Background(), uint32(command.PurgeTopicCode), nil)
	require.NoError(t, err)
	for range 2 {
		_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
		require.NoError(t, err)
	}
	var floors []uint64
	for _, read := range fixture.primaries[0].recorded() {
		if read.code() == uint32(command.AttachConsumerSessionCode) {
			floors = append(floors, binary.LittleEndian.Uint64(read.payload[24:]))
		}
	}
	assert.Equal(t, []uint64{1, 11}, floors)
	assert.Equal(t, 1, fixture.primaries[0].connections())
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.GetPollRoutingCode))
}

func TestPrimaryPoll_UnknownOutcomeDoesNotReplayAndOnlyLostRepliesReplaceDataConnection(t *testing.T) {
	for _, lostReply := range []bool{false, true} {
		t.Run(map[bool]string{false: "transient-not-committed", true: "lost-reply"}[lostReply], func(t *testing.T) {
			var polls atomic.Int32
			fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
				if read.code() == uint32(command.PollMessagesOnPrimaryCode) && polls.Add(1) == 1 {
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
		if read.code() == uint32(command.PollMessagesOnPrimaryCode) && polls.Add(1) == 1 {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrTransientNotAccepted.Code()), nil), true
		}
		return nil, false
	}, nil)
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.GetPollRoutingCode))
	assert.Equal(t, 2, requestCount(fixture.primaries[0].recorded(), command.AttachConsumerSessionCode))
	assert.Equal(t, 1, fixture.primaries[0].connections())
	assert.Equal(t, 1, fixture.coordinator.connections())
}

func TestPrimaryPoll_CancellationClosesUnfinishedExchangeAndBoundsPooledWaiter(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	fixture := newPrimaryPollFixture(t, func(_, connection int, read request) ([]byte, bool) {
		if connection == 0 && read.code() == uint32(command.PollMessagesOnPrimaryCode) {
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
	require.ErrorIs(t, err, context.DeadlineExceeded)
	cancel()
	select {
	case err := <-completed:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt the data exchange")
	}
	polled, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	assert.Zero(t, polled.PartitionId, "the late canceled reply must never answer the next poll")
	assert.Equal(t, 2, fixture.primaries[0].connections())
	assert.Equal(t, 1, fixture.coordinator.connections())
}

func TestPrimaryPoll_LogoutRetiresPendingDataAndDoesNotResurrectMembership(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.PollMessagesOnPrimaryCode) {
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
	assert.Empty(t, fixture.client.polls.connections)
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
				if read.code() == uint32(command.PollMessagesOnPrimaryCode) && polls.Add(1) == 1 {
					return statusReplyFrame(vsr.OperationNonReplicated, uint32(status.Code()), nil), true
				}
				return nil, false
			}, nil)
			_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.ErrorIs(t, err, status)
			_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.NoError(t, err)
			assert.Equal(t, 1, fixture.primaries[0].connections())
			assert.Equal(t, 1, requestCount(fixture.primaries[0].recorded(), command.AttachConsumerSessionCode))
			assert.Equal(t, int32(2), polls.Load())
		})
	}
}

func TestPrimaryPoll_InvalidReplyFrameDropsTheDataConnectionWithoutReplay(t *testing.T) {
	var polls atomic.Int32
	fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.PollMessagesOnPrimaryCode) && polls.Add(1) == 1 {
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

func TestPrimaryPoll_SessionFailureReplacesDataConnectionWithoutReplay(t *testing.T) {
	for _, test := range []struct {
		name  string
		reply []byte
	}{
		{"evicted", evictionFrame(vsr.EvictionStaleClient, 0, 0)},
		{"unauthenticated", statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrUnauthenticated.Code()), nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			var polls atomic.Int32
			fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
				if read.code() == uint32(command.PollMessagesOnPrimaryCode) && polls.Add(1) == 1 {
					return test.reply, true
				}
				return nil, false
			}, nil)
			_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
			require.ErrorIs(t, err, ierror.ErrTransientNotCommitted)
			require.Equal(t, int32(1), polls.Load())
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
		if read.code() == uint32(command.AttachConsumerSessionCode) {
			floors = append(floors, binary.LittleEndian.Uint64(read.payload[24:]))
		}
	}
	assert.Equal(t, []uint64{1, 42}, floors)
	assert.Equal(t, 1, fixture.primaries[0].connections())
}

func TestPrimaryPoll_RetirementDuringAttachmentIsNotCallerCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.AttachConsumerSessionCode) {
			close(entered)
			<-release
			return nil, true
		}
		return nil, false
	}, nil)
	stream, topic, consumer := groupConsumer(t)
	partition := uint32(0)
	payload, err := (&command.PollMessages{StreamId: stream, TopicId: topic, Consumer: consumer,
		PartitionId: &partition, Strategy: iggcon.NextPollingStrategy(), Count: 1, AutoCommit: true}).MarshalBinary()
	require.NoError(t, err)
	key := string(payload[:len(payload)-pollParametersSize])
	route, err := fixture.client.pollRoute(context.Background(), key, payload)
	require.NoError(t, err)
	completed := make(chan error, 1)
	go func() {
		_, err := fixture.client.pollOnRoute(context.Background(), key, payload, route)
		completed <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("primary did not receive the attachment")
	}
	require.NoError(t, fixture.client.LogoutUser(context.Background()))
	select {
	case err := <-completed:
		require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
	case <-time.After(time.Second):
		t.Fatal("logout left the attachment running")
	}
	assert.Zero(t, requestCount(fixture.primaries[0].recorded(), command.PollMessagesOnPrimaryCode))
}

func TestPrimaryPoll_InternalBudgetDoesNotReturnCallerDeadline(t *testing.T) {
	for _, outcome := range []string{"lost-reply", "queued", "refused"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				coordinator, coordinatorConn := newPipeClient(t)
				defer func() { _ = coordinator.Close() }()
				primary, primaryConn := newPipeClient(t)
				defer func() { _ = primary.Close() }()
				coordinator.rememberLogin(NewUsernamePasswordCredentials("iggy", "secret"))
				coordinator.clustered.Store(true)
				stream, topic, consumer := groupConsumer(t)
				partition := uint32(0)
				poll := &command.PollMessages{StreamId: stream, TopicId: topic, Consumer: consumer,
					PartitionId: &partition, Strategy: iggcon.NextPollingStrategy(), Count: 1, AutoCommit: true}
				payload, err := poll.MarshalBinary()
				require.NoError(t, err)
				key := string(payload[:len(payload)-pollParametersSize])
				route := pollRoute{endpoint: "127.0.0.1:9000", parent: coordinator.pollSession.Load().parent}
				lifetime, cancel := context.WithCancel(context.Background())
				defer cancel()
				slot := &pollConnection{gate: make(chan struct{}, 1), ctx: lifetime, cancel: cancel,
					conn: primary.conn, client: primary, parent: route.parent, attached: true}
				coordinator.polls.routes = map[string]pollRoute{key: route}
				coordinator.polls.connections = map[string]*pollConnection{route.endpoint: slot}
				serve(coordinatorConn, func(_ int, read request) []byte {
					return pollRoutingReply(t, read, route.endpoint, 0)
				})
				polls := 0
				serve(primaryConn, func(_ int, read request) []byte {
					if read.code() == uint32(command.AttachConsumerSessionCode) {
						return replyFrame(vsr.OperationNonReplicated, nil)
					}
					polls++
					if outcome == "refused" {
						return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrTransientNotAccepted.Code()), nil)
					}
					return nil
				})
				if outcome == "queued" {
					slot.gate <- struct{}{}
				}
				_, err = coordinator.pollPrimary(context.Background(), poll)
				synctest.Wait()
				if outcome == "refused" {
					require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
					assert.Greater(t, polls, 1)
				} else {
					require.ErrorIs(t, err, ierror.ErrTransientNotCommitted)
					if outcome == "lost-reply" {
						assert.Equal(t, 1, polls, "an uncertain auto-commit cannot replay")
					} else {
						assert.Zero(t, polls)
					}
				}
			})
		})
	}
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
	var failRoster atomic.Bool
	failRoster.Store(true)
	fixture := newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
		if read.code() == uint32(command.GetClusterMetadataCode) && failRoster.Load() {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrFeatureUnavailable.Code()), nil), true
		}
		return nil, false
	})
	require.False(t, fixture.client.topologyKnown.Load(), "the login-time roster read failed")
	parent := fixture.client.session.ClientID()
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.ErrorIs(t, err, ierror.ErrFeatureUnavailable)
	assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
	assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.GetPollRoutingCode))

	failRoster.Store(false)
	_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	require.True(t, fixture.client.topologyKnown.Load())
	failRoster.Store(true)
	_, err = fixture.client.GetClusterMetadata(context.Background())
	require.ErrorIs(t, err, ierror.ErrFeatureUnavailable)
	_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err, "a failed refresh must preserve known topology and warm routes")
	assert.Equal(t, 1, fixture.primaries[0].connections())
	assert.Equal(t, 1, fixture.coordinator.connections())
	assert.Equal(t, parent, fixture.client.session.ClientID())
	assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
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
		return nil, false
	})
	failRoster.Store(false)
	for range 2 {
		_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
		require.NoError(t, err)
	}
	assert.True(t, fixture.client.topologyKnown.Load())
	assert.False(t, fixture.client.clustered.Load())
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
	assert.Equal(t, 2, requestCount(fixture.coordinator.recorded(), command.GetClusterMetadataCode))
	assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.GetPollRoutingCode))
	assert.Zero(t, fixture.primaries[0].connections())
}

func TestPrimaryPoll_ControlConnectionRecoversBeforeRouting(t *testing.T) {
	fixture := newPrimaryPollFixture(t, nil, func(connection int, read request) ([]byte, bool) {
		if connection == 0 && read.code() == uint32(command.GetPollRoutingCode) {
			return nil, true
		}
		return nil, false
	})
	previous := fixture.client.session.ClientID()
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	assert.NotEqual(t, previous, fixture.client.session.ClientID())
	assert.Equal(t, 2, fixture.coordinator.connections())
	assert.Equal(t, 1, fixture.primaries[0].connections())
}

func TestPrimaryPoll_ManualIdentityAndChangedCredentialsAuthenticateNewDataConnections(t *testing.T) {
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
	for index, credentials := range []Credentials{
		NewUsernamePasswordCredentials("manual", "manual-secret"),
		NewUsernamePasswordCredentials("renamed", "changed-secret"),
	} {
		payload, err := vsr.SerializeLoginRegister(credentials.username, credentials.password, iggcon.Version)
		require.NoError(t, err)
		assert.Equal(t, payload, fixture.primaries[index].recorded()[0].payload)
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
	payload, err := vsr.SerializeLoginRegister(expected.username, expected.password, iggcon.Version)
	require.NoError(t, err)
	assert.Equal(t, payload, fixture.primaries[0].recorded()[0].payload)
}

func TestPrimaryPoll_StaleAttachmentReconnectsDataWithoutRejoining(t *testing.T) {
	fixture := newPrimaryPollFixture(t, func(_, connection int, read request) ([]byte, bool) {
		if connection == 0 && read.code() == uint32(command.AttachConsumerSessionCode) {
			return evictionFrame(vsr.EvictionStaleClient, 0, 0), true
		}
		return nil, false
	}, nil)
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, fixture.primaries[0].connections())
	assert.Equal(t, 1, fixture.coordinator.connections())
	assert.Equal(t, 1, requestCount(fixture.primaries[0].recorded(), command.PollMessagesOnPrimaryCode),
		"attachment failure is retried before any auto-commit poll is admitted")
}

func TestPrimaryPoll_PlainConsumerRoutesAndNonAutoCommitStaysOnCoordinator(t *testing.T) {
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
	assert.Equal(t, 1, requestCount(fixture.coordinator.recorded(), command.PollMessagesCode))
	assert.Equal(t, 2, requestCount(fixture.primaries[0].recorded(), command.PollMessagesOnPrimaryCode))
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
	require.ErrorIs(t, err, context.DeadlineExceeded)
	cancel()
	select {
	case err := <-completed:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt the coordinator exchange")
	}
}

func TestPrimaryPoll_MetadataAcknowledgedWhileQueuedRefreshesBeforeAdmission(t *testing.T) {
	fixture := newPrimaryPollFixture(t, nil, nil)
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	fixture.client.polls.mu.Lock()
	slot := fixture.client.polls.connections[fixture.primaries[0].address()]
	fixture.client.polls.mu.Unlock()
	slot.gate <- struct{}{}
	stream, topic, consumer := groupConsumer(t)
	partition := uint32(0)
	payload, err := (&command.PollMessages{StreamId: stream, TopicId: topic, Consumer: consumer,
		Strategy: iggcon.NextPollingStrategy(), Count: 1, AutoCommit: true, PartitionId: &partition}).MarshalBinary()
	require.NoError(t, err)
	key := string(payload[:len(payload)-pollParametersSize])
	fixture.client.polls.mu.Lock()
	route := fixture.client.polls.routes[key]
	fixture.client.polls.mu.Unlock()
	completed := make(chan error, 1)
	go func() {
		_, err := fixture.client.pollOnRoute(context.Background(), key, payload, route)
		completed <- err
	}()
	_, err = fixture.client.SendBinaryRequest(context.Background(), uint32(command.PurgeTopicCode), nil)
	require.NoError(t, err)
	<-slot.gate
	select {
	case err := <-completed:
		require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
	case <-time.After(time.Second):
		t.Fatal("queued poll did not resume")
	}
	assert.Equal(t, 1, requestCount(fixture.primaries[0].recorded(), command.PollMessagesOnPrimaryCode),
		"the waiting poll cannot be admitted with the old metadata floor")
	_, err = pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	reads := fixture.primaries[0].recorded()
	var lastFloor uint64
	for _, read := range reads {
		if read.code() == uint32(command.AttachConsumerSessionCode) {
			lastFloor = binary.LittleEndian.Uint64(read.payload[24:])
		}
	}
	assert.Equal(t, uint64(11), lastFloor)
}

func TestPrimaryPoll_PoolEvictsIdleConnectionsAfterEndpointChurn(t *testing.T) {
	fixture := newPrimaryPollFixture(t, nil, nil)
	fixture.client.polls.mu.Lock()
	fixture.client.polls.connections = make(map[string]*pollConnection)
	var stale []*pollConnection
	for index := range maxPollConnections {
		ctx, cancel := context.WithCancel(context.Background())
		slot := &pollConnection{gate: make(chan struct{}, 1), ctx: ctx, cancel: cancel}
		fixture.client.polls.connections[fmt.Sprintf("retired-endpoint-%d", index)] = slot
		stale = append(stale, slot)
	}
	fixture.client.polls.mu.Unlock()
	_, err := pollPrimaryPartition(context.Background(), fixture.client, 0)
	require.NoError(t, err)
	fixture.client.polls.mu.Lock()
	assert.Len(t, fixture.client.polls.connections, maxPollConnections)
	fixture.client.polls.mu.Unlock()
	retired := 0
	for _, slot := range stale {
		if slot.ctx.Err() != nil {
			retired++
		}
	}
	assert.Equal(t, 1, retired)
}
