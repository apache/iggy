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
	"sync/atomic"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/require"
)

func committedReply(read request, code ierror.Code) []byte {
	var body []byte
	if code == 0 {
		body = resultSection()
	} else {
		body = resultSection(uint32(code))
	}
	reply := replyFrame(read.operation(), body)
	copy(reply[128:144], read.header[144:160])
	copy(reply[160:176], read.header[128:144])
	copy(reply[224:232], read.header[200:208])
	binary.LittleEndian.PutUint64(reply[176:184], 1)
	binary.LittleEndian.PutUint64(reply[184:192], 1)
	reply[220] = 1
	return reply
}

func TestDriverRetainsUnprovedRejectionAndResolvesExactCommittedResult(t *testing.T) {
	client, peer := newPipeClient(t)
	var calls atomic.Uint32
	server := serve(peer, func(_ int, read request) []byte {
		if read.code() == uint32(command.BindSessionCode) {
			return bindReplyFrame(7, read.sessionID())
		}
		switch calls.Add(1) {
		case 1:
			return statusReplyFrame(read.operation(), uint32(ierror.TransientNotCommittedCode), nil)
		case 2:
			reply := replyFrame(read.operation(), resultSection(uint32(ierror.UnauthorizedCode)))
			reply[220] = 0
			return reply
		case 3:
			return committedReply(read, ierror.UnauthorizedCode)
		default:
			return committedReply(read, 0)
		}
	})
	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	var uncertain ierror.UncertainOutcome
	require.ErrorAs(t, err, &uncertain)
	_, err = client.do(context.Background(), &command.CreateStream{Name: "blocked"})
	var blocked ierror.LaneBlocked
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, uncertain.OperationID, blocked.OperationID)
	require.ErrorIs(t, client.ResolveOperation(context.Background(), uncertain.OperationID), ierror.ErrUnauthorized)
	require.ErrorIs(t, client.ResolveOperation(context.Background(), uncertain.OperationID), ierror.ErrOperationNotRetained)
	_, err = client.do(context.Background(), &command.CreateStream{Name: "next"})
	require.NoError(t, err)
	var mutations []request
	for _, read := range server.recorded() {
		if read.operation() == vsr.OperationCreateStream {
			mutations = append(mutations, read)
		}
	}
	require.Len(t, mutations, 4)
	require.Equal(t, mutations[0].header, mutations[1].header)
	require.Equal(t, mutations[0].header, mutations[2].header)
	require.Equal(t, mutations[0].payload, mutations[2].payload)
	require.Greater(t, mutations[3].requestID(), uncertain.OperationID)
}

func TestDriverCommittedLifecycleBusyUsesNewIDOnlyBeforeUncertainty(t *testing.T) {
	for _, firstUncertain := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[firstUncertain], func(t *testing.T) {
			client, peer := newPipeClient(t)
			var calls atomic.Uint32
			server := serve(peer, func(_ int, read request) []byte {
				if read.code() == uint32(command.BindSessionCode) {
					return bindReplyFrame(7, read.sessionID())
				}
				call := calls.Add(1)
				if call == 1 && firstUncertain {
					return statusReplyFrame(read.operation(), uint32(ierror.TransientNotCommittedCode), nil)
				}
				if call == 1 || firstUncertain && call == 2 {
					return committedReply(read, ierror.LifecycleBusyCode)
				}
				return committedReply(read, 0)
			})
			_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
			if firstUncertain {
				require.ErrorIs(t, err, ierror.ErrLifecycleBusy)
			} else {
				require.NoError(t, err)
			}
			var mutations []request
			for _, read := range server.recorded() {
				if read.operation() == vsr.OperationCreateStream {
					mutations = append(mutations, read)
				}
			}
			require.Len(t, mutations, 2)
			if firstUncertain {
				require.Equal(t, mutations[0].header, mutations[1].header)
			} else {
				require.NotEqual(t, mutations[0].requestID(), mutations[1].requestID())
			}
			_, err = client.do(context.Background(), &command.CreateStream{Name: "released"})
			require.NoError(t, err)
		})
	}
}

func TestDriverLaneAdmissionDeadlineDoesNotConsumeRequestID(t *testing.T) {
	client, peer := newPipeClient(t)
	firstReceived := make(chan struct{})
	release := make(chan struct{})
	serve(peer, func(_ int, read request) []byte { close(firstReceived); <-release; return committedReply(read, 0) })
	firstDone := make(chan error, 1)
	go func() {
		_, err := client.do(context.Background(), &command.CreateStream{Name: "first"})
		firstDone <- err
	}()
	<-firstReceived
	before := client.session.CurrentRequestID()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := client.do(ctx, &command.CreateStream{Name: "queued"})
	require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
	require.Equal(t, before, client.session.CurrentRequestID())
	close(release)
	require.NoError(t, <-firstDone)
}

func TestDriverCancelledMutationRetainsItsLaneUntilAbandon(t *testing.T) {
	client, peer := newPipeClient(t)
	received := make(chan struct{})
	serve(peer, func(_ int, read request) []byte { close(received); return nil })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := client.do(ctx, &command.CreateStream{Name: "lost"}); done <- err }()
	<-received
	cancel()
	var uncertain ierror.UncertainOutcome
	require.ErrorAs(t, <-done, &uncertain)
	require.NoError(t, client.AbandonOperation(context.Background(), uncertain.OperationID))
	require.ErrorIs(t, client.AbandonOperation(context.Background(), uncertain.OperationID), ierror.ErrOperationNotRetained)
	ticket, err := client.operations.enter(context.Background(), laneKey{})
	require.NoError(t, err)
	ticket.finish(false)
}

func TestDriverProgressUncertaintyEndsCallAndReleasesLane(t *testing.T) {
	for _, autoCommit := range []bool{false, true} {
		for _, lostReply := range []bool{false, true} {
			name := fmt.Sprintf("auto_commit_%t/lost_reply_%t", autoCommit, lostReply)
			t.Run(name, func(t *testing.T) {
				client, peer := newPipeClient(t)
				received := make(chan struct{})
				var polls atomic.Uint32
				server := servePartitionOperations(t, peer, func(_ int, read request) []byte {
					if read.code() == uint32(command.PollMessagesCode) {
						polls.Add(1)
						close(received)
						if lostReply {
							return nil
						}
						return statusReplyFrame(read.operation(), uint32(ierror.TransientNotCommittedCode), nil)
					}
					return replyFrame(read.operation(), nil)
				})
				stream, _ := iggcon.NewIdentifier(uint32(1))
				topic, _ := iggcon.NewIdentifier(uint32(2))
				consumer, _ := iggcon.NewIdentifier(uint32(3))
				partition := uint32(0)
				strategy := iggcon.NextPollingStrategy()
				if autoCommit {
					strategy = iggcon.OffsetPollingStrategy(0)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					_, err := client.PollMessages(ctx, stream, topic, iggcon.NewSingleConsumer(consumer), strategy, 1, autoCommit, &partition)
					done <- err
				}()
				<-received
				if lostReply {
					cancel()
				}
				require.ErrorIs(t, <-done, ierror.ErrTransientNotCommitted)
				require.Equal(t, uint32(1), polls.Load())
				require.Equal(t, 1, requestCount(server.recorded(), command.PollMessagesCode))
				client.operations.mu.Lock()
				require.Empty(t, client.operations.retained)
				require.Empty(t, client.operations.lanes)
				require.Zero(t, client.operations.bytes)
				client.operations.mu.Unlock()
				require.NoError(t, client.Ping(context.Background()))
			})
		}
	}
}

func TestDriverPureReadRetriesUncertainReply(t *testing.T) {
	client, peer := newPipeClient(t)
	server := serve(peer, func(index int, read request) []byte {
		if index == 0 {
			return statusReplyFrame(read.operation(), uint32(ierror.TransientNotCommittedCode), nil)
		}
		return replyFrame(read.operation(), nil)
	})
	require.NoError(t, client.Ping(context.Background()))
	reads := server.recorded()
	require.Len(t, reads, 2)
	require.Equal(t, reads[0].header, reads[1].header)
}

func TestDriverLifecycleRetryKeepsLaneAheadOfQueuedMutation(t *testing.T) {
	client, peer := newPipeClient(t)
	firstReceived := make(chan struct{})
	release := make(chan struct{})
	server := serve(peer, func(index int, read request) []byte {
		if index == 0 {
			close(firstReceived)
			<-release
			return committedReply(read, ierror.LifecycleBusyCode)
		}
		return committedReply(read, 0)
	})
	first := make(chan error, 1)
	go func() { _, err := client.do(context.Background(), &command.CreateStream{Name: "first"}); first <- err }()
	<-firstReceived
	second := make(chan error, 1)
	go func() {
		_, err := client.do(context.Background(), &command.CreateStream{Name: "second"})
		second <- err
	}()
	require.Eventually(t, func() bool {
		client.operations.mu.Lock()
		defer client.operations.mu.Unlock()
		return len(client.operations.lanes[laneKey{}].waiters) == 1
	}, time.Second, time.Millisecond)
	close(release)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	reads := server.recorded()
	require.Len(t, reads, 3)
	require.Equal(t, reads[0].payload, reads[1].payload)
	require.NotEqual(t, reads[1].payload, reads[2].payload)
	require.Less(t, reads[0].requestID(), reads[1].requestID())
	require.Less(t, reads[1].requestID(), reads[2].requestID())
}

func TestPendingConfigurationRejectsInvalidLimitsBeforeDial(t *testing.T) {
	for _, limits := range [][2]int{{0, DefaultMaxPendingOperationBytes}, {1, 0}, {-1, DefaultMaxPendingOperationBytes}, {1, -1}} {
		client := NewIggyTcpClient(nil, WithServerAddress("invalid"), WithPendingOperations(limits[0], limits[1]))
		require.ErrorIs(t, client.Connect(context.Background()), ierror.ErrInvalidConfiguration)
		require.NoError(t, client.Close())
	}
}

func TestDriverOversizedMutationFailsBeforeRequestIdentityOrWrite(t *testing.T) {
	client, peer := newPipeClient(t)
	client.operations = newOperationRegistry(1, vsr.HeaderSize)
	server := serve(peer, func(_ int, read request) []byte {
		t.Error("oversized mutation reached the link")
		return committedReply(read, 0)
	})
	before := client.session.CurrentRequestID()
	_, err := client.do(context.Background(), &command.CreateStream{Name: "oversized"})
	require.ErrorIs(t, err, ierror.ErrInvalidSizeBytes)
	require.Equal(t, before, client.session.CurrentRequestID())
	require.Empty(t, server.recorded())
	require.Empty(t, client.operations.lanes)
}

func TestDriverPureReadDeadlineReportsUnknownWithoutRetainingAnOperation(t *testing.T) {
	client, peer := newPipeClient(t)
	serve(peer, func(_ int, read request) []byte {
		return statusReplyFrame(read.operation(), uint32(ierror.TransientNotCommittedCode), nil)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, client.Ping(ctx), ierror.ErrTransientNotCommitted)
	require.Empty(t, client.operations.retained)
	require.Empty(t, client.operations.lanes)
}
