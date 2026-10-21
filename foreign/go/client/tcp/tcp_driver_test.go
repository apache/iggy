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
	"net"
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

// serveWrites numbers each write in arrival order and counts the attempts that
// carry its request ID, both from 0, so a test scripts every attempt.
func serveWrites(peer net.Conn, answer func(write, attempt int, read request) []byte) *fakeServer {
	writes, attempts := make(map[uint64]int), make(map[uint64]int)
	return serve(peer, func(_ int, read request) []byte {
		id := read.requestID()
		if _, seen := writes[id]; !seen {
			writes[id] = len(writes)
		}
		attempt := attempts[id]
		attempts[id]++
		return answer(writes[id], attempt, read)
	})
}

// uncertainAnswer answers the first two attempts of a write: 57 makes it
// uncertain and an unproved rejection ends the call with UncertainOutcome.
func uncertainAnswer(attempt int, read request) []byte {
	if attempt == 0 {
		return statusReplyFrame(read.operation(), uint32(ierror.TransientNotCommittedCode), nil)
	}
	reply := replyFrame(read.operation(), resultSection(uint32(ierror.UnauthorizedCode)))
	reply[220] = 0
	return reply
}

func uncertainWrite(t *testing.T, client *IggyTcpClient) uint64 {
	t.Helper()
	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	var uncertain ierror.UncertainOutcome
	require.ErrorAs(t, err, &uncertain)
	return uncertain.OperationID
}

func createStreams(reads []request) []request {
	return requestsFor(reads, vsr.OperationCreateStream)
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
	require.ErrorIs(t, client.ResolveOperation(context.Background(), uncertain.OperationID), ierror.ErrUnauthorized)
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

func TestDriverResolversOfOneUncertainWriteShareItsProvedOutcome(t *testing.T) {
	for _, outcome := range []struct {
		name string
		code ierror.Code
		want error
	}{
		{"success", 0, nil},
		{"rejection", ierror.UnauthorizedCode, ierror.ErrUnauthorized},
		{"session_end", ierror.StaleClientCode, ierror.ErrStaleClient},
	} {
		for _, racing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/racing_%t", outcome.name, racing), func(t *testing.T) {
				client, peer := newOfflinePipeClient(t)
				replayed, release := make(chan struct{}), make(chan struct{})
				server := serveWrites(peer, func(_, attempt int, read request) []byte {
					if attempt < 2 {
						return uncertainAnswer(attempt, read)
					}
					if attempt == 2 {
						close(replayed)
						<-release
					}
					return committedReply(read, outcome.code)
				})
				id := uncertainWrite(t, client)
				first, second := make(chan error, 1), make(chan error, 1)
				resolve := func(result chan<- error) { result <- client.ResolveOperation(context.Background(), id) }
				go resolve(first)
				<-replayed
				if racing {
					go resolve(second)
					require.Never(t, func() bool { return len(second) != 0 }, 20*time.Millisecond, time.Millisecond,
						"the second resolver waits for the running replay")
					func() {
						// Held links stop the first resolver inside a session recovery,
						// so the second one sees whether the proof came before it.
						client.links.mu.Lock()
						defer client.links.mu.Unlock()
						close(release)
						require.Equal(t, outcome.want, <-second)
					}()
				} else {
					close(release)
				}
				require.Equal(t, outcome.want, <-first)
				if !racing {
					resolve(second)
					require.Equal(t, outcome.want, <-second)
				}
				require.Len(t, createStreams(server.recorded()), 3, "two attempts and one replay")
			})
		}
	}
}

func TestDriverProvedOutcomesSurviveASessionResetUntilTheyAgeOut(t *testing.T) {
	for _, reset := range []string{"resetSession", "recoverSession"} {
		t.Run(reset, func(t *testing.T) {
			client, peer := newPipeClient(t)
			client.config.reconnection.enabled = false
			client.config.serverAddress = "invalid-address"
			outcomes := []struct {
				code ierror.Code
				want error
			}{{0, nil}, {ierror.UnauthorizedCode, ierror.ErrUnauthorized}, {0, nil}}
			client.operations = newOperationRegistry(len(outcomes)-1, DefaultMaxPendingOperationBytes)
			server := serveWrites(peer, func(write, attempt int, read request) []byte {
				if attempt < 2 {
					return uncertainAnswer(attempt, read)
				}
				return committedReply(read, outcomes[write].code)
			})
			ids := make([]uint64, len(outcomes))
			for write, outcome := range outcomes {
				ids[write] = uncertainWrite(t, client)
				require.Equal(t, outcome.want, client.ResolveOperation(context.Background(), ids[write]))
			}
			if reset == "resetSession" {
				client.resetSession(true)
			} else {
				holdRecovery(t, client)
				recoverNow(client)
			}
			require.ErrorIs(t, client.ResolveOperation(context.Background(), ids[0]), ierror.ErrOperationNotRetained,
				"the oldest outcome aged out of a record of two")
			for write := 1; write < len(ids); write++ {
				require.Equal(t, outcomes[write].want, client.ResolveOperation(context.Background(), ids[write]))
			}
			require.Len(t, createStreams(server.recorded()), 3*len(outcomes), "no replay after the proofs")
		})
	}
}

func TestDriverReplayAfterAConcurrentAbandonKeepsItsLaneAndRecordsItsProof(t *testing.T) {
	for _, transient := range []bool{false, true} {
		t.Run(fmt.Sprintf("first_reply_57_%t", transient), func(t *testing.T) {
			client, peer := newPipeClient(t)
			replayed, release := make(chan struct{}), make(chan struct{})
			server := serveWrites(peer, func(write, attempt int, read request) []byte {
				switch {
				case write == 0 && attempt < 2:
					return uncertainAnswer(attempt, read)
				case write == 0 && attempt == 2:
					close(replayed)
					<-release
					if transient {
						return statusReplyFrame(read.operation(), uint32(ierror.TransientNotCommittedCode), nil)
					}
				}
				return committedReply(read, 0)
			})
			id := uncertainWrite(t, client)
			resolved := make(chan error, 1)
			go func() { resolved <- client.ResolveOperation(context.Background(), id) }()
			<-replayed
			require.NoError(t, client.AbandonOperation(context.Background(), id))
			next := make(chan error, 1)
			go func() { _, err := client.do(context.Background(), &command.CreateStream{Name: "next"}); next <- err }()
			require.Eventually(t, func() bool {
				client.operations.mu.Lock()
				defer client.operations.mu.Unlock()
				lane := client.operations.lanes[laneKey{}]
				return lane != nil && len(lane.waiters) == 1
			}, time.Second, time.Millisecond, "the next write waits for the lane of the replay")
			close(release)
			require.NoError(t, <-resolved)
			require.NoError(t, <-next)
			require.NoError(t, client.ResolveOperation(context.Background(), id))
			creates := createStreams(server.recorded())
			replays := 1
			if transient {
				replays = 2
			}
			require.Len(t, creates, 2+replays+1)
			for _, create := range creates[:len(creates)-1] {
				require.Equal(t, id, create.requestID())
			}
			require.Greater(t, creates[len(creates)-1].requestID(), id)
		})
	}
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

func TestDriverLinkAdmissionDeadlineReportsNotAccepted(t *testing.T) {
	for _, kind := range []string{"mutation", "read", "progress"} {
		t.Run(kind, func(t *testing.T) {
			client, peer := newPipeClient(t)
			server := servePartitionOperations(t, peer, func(_ int, read request) []byte {
				return replyFrame(read.operation(), emptyBatchBody(0))
			})
			stream, topic, group := groupConsumer(t)
			partition := uint32(0)
			consumer := iggcon.NewSingleConsumer(group.Id)
			_, err := client.PollMessages(context.Background(), stream, topic, consumer, iggcon.FirstPollingStrategy(), 1, false, &partition)
			require.NoError(t, err)
			before := len(server.recorded())
			link := client.links.metadata()
			for range cap(link.capacity) {
				link.capacity <- struct{}{}
			}
			defer func() {
				for range cap(link.capacity) {
					<-link.capacity
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			switch kind {
			case "mutation":
				_, err = client.do(ctx, &command.CreateStream{Name: "unsent"})
			case "read":
				err = client.Ping(ctx)
			case "progress":
				_, err = client.PollMessages(ctx, stream, topic, consumer, iggcon.NextPollingStrategy(), 1, true, &partition)
			}
			require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
			require.Len(t, server.recorded(), before)
			require.Empty(t, client.operations.retained)
		})
	}
}

func TestDriverMalformedProgressReplyReportsUnknownWithoutReplay(t *testing.T) {
	client, peer := newPipeClient(t)
	var polls atomic.Uint32
	servePartitionOperations(t, peer, func(_ int, read request) []byte {
		polls.Add(1)
		return replyFrame(vsr.OperationCreateStream, nil)
	})
	stream, topic, group := groupConsumer(t)
	partition := uint32(0)
	_, err := client.PollMessages(context.Background(), stream, topic, iggcon.NewSingleConsumer(group.Id), iggcon.NextPollingStrategy(), 1, true, &partition)
	require.ErrorIs(t, err, ierror.ErrTransientNotCommitted)
	require.Equal(t, uint32(1), polls.Load())
	require.Empty(t, client.operations.retained)
	require.Empty(t, client.operations.lanes)
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

func TestDriverUncertainMutationStaysUncertainWhenTheFenceChangesBeforeItsReplayIsSent(t *testing.T) {
	client, peer := newPipeClient(t)
	client.config.reconnection.enabled = false
	client.config.serverAddress = "invalid-address"
	link := client.links.metadata()
	unplug := make(chan struct{})
	defer close(unplug)
	firstID := make(chan uint64, 1)
	server := serve(peer, func(index int, read request) []byte {
		if read.code() == uint32(command.PingCode) {
			<-unplug
			return nil
		}
		if index == 0 {
			link.bindGate <- struct{}{}
			firstID <- read.requestID()
		}
		return statusReplyFrame(read.operation(), uint32(ierror.TransientNotCommittedCode), nil)
	})
	done := make(chan error, 1)
	go func() { _, err := client.do(context.Background(), &command.CreateStream{Name: "orders"}); done <- err }()
	id := <-firstID
	// The server holds the first plug and the writer the second, the rest fill
	// the queue, so the replay waits in the link without being written.
	for plug := range cap(link.writes) + 2 {
		link.writes <- linkTestFrame(uint64(plug))
	}
	require.Eventually(t, func() bool { link.mu.Lock(); defer link.mu.Unlock(); return len(link.pending) == 0 }, time.Second, time.Millisecond)
	<-link.bindGate
	require.Eventually(t, func() bool { link.mu.Lock(); defer link.mu.Unlock(); return link.pending[id] != nil }, time.Second, time.Millisecond)
	client.mtx.Lock()
	fence := client.sessionFence
	client.mtx.Unlock()
	client.recoverSession(fence)
	var uncertain ierror.UncertainOutcome
	require.ErrorAs(t, <-done, &uncertain)
	require.Equal(t, id, uncertain.OperationID)
	creates := 0
	for _, read := range server.recorded() {
		if read.operation() == vsr.OperationCreateStream {
			creates++
		}
	}
	require.Equal(t, 1, creates)
	require.ErrorIs(t, client.ResolveOperation(context.Background(), id), ierror.ErrOperationNotRetained)
}

func TestDriverUnsentCallFailsWithNotAcceptedOnlyWhileTheSessionRecovers(t *testing.T) {
	for _, end := range []struct {
		name string
		want error
	}{{"recovery", ierror.ErrTransientNotAccepted}, {"logout", ierror.ErrUnauthenticated}} {
		for _, kind := range []string{"mutation", "read"} {
			t.Run(end.name+"/"+kind, func(t *testing.T) {
				client, peer := newPipeClient(t)
				client.config.reconnection.enabled = false
				client.config.serverAddress = "invalid-address"
				link := client.links.metadata()
				plugged, unplug := make(chan struct{}), make(chan struct{})
				defer close(unplug)
				server := serve(peer, func(index int, _ request) []byte {
					if index == 0 {
						close(plugged)
					}
					<-unplug
					return nil
				})
				for plug := range cap(link.writes) + 2 {
					link.writes <- linkTestFrame(uint64(plug))
				}
				<-plugged
				done := make(chan error, 1)
				go func() {
					if kind == "read" {
						done <- client.Ping(context.Background())
						return
					}
					_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
					done <- err
				}()
				require.Eventually(t, func() bool { link.mu.Lock(); defer link.mu.Unlock(); return len(link.pending) == 1 }, time.Second, time.Millisecond)
				if end.name == "recovery" {
					holdRecovery(t, client)
					recoverNow(client)
				} else {
					client.resetSession(true)
					client.links.disconnect(true)
				}
				require.ErrorIs(t, <-done, end.want)
				require.Len(t, server.recorded(), 1, "only the first plug reached the server")
			})
		}
	}
}

func TestDriverSessionRecoveryFailsUnsentCallsWithNotAcceptedUntilItFails(t *testing.T) {
	client, peer := newPipeClient(t)
	client.config.reconnection.enabled = false
	client.config.serverAddress = "invalid-address"
	received := make(chan struct{}, 2)
	server := serve(peer, func(index int, _ request) []byte {
		if index < cap(received) {
			received <- struct{}{}
		}
		return nil
	})
	first := make(chan error, 1)
	go func() { _, err := client.do(context.Background(), &command.CreateStream{Name: "first"}); first <- err }()
	<-received
	read := make(chan error, 1)
	go func() { read <- client.Ping(context.Background()) }()
	<-received
	waiting := make(chan error, 1)
	go func() {
		_, err := client.do(context.Background(), &command.CreateStream{Name: "waiting"})
		waiting <- err
	}()
	require.Eventually(t, func() bool {
		client.operations.mu.Lock()
		defer client.operations.mu.Unlock()
		return len(client.operations.lanes[laneKey{}].waiters) == 1
	}, time.Second, time.Millisecond)
	release := holdRecovery(t, client)
	recoverNow(client)
	var uncertain ierror.UncertainOutcome
	require.ErrorAs(t, <-first, &uncertain)
	require.ErrorIs(t, <-read, ierror.ErrTransientNotAccepted, "a sent read cannot have changed server state")
	require.ErrorIs(t, <-waiting, ierror.ErrTransientNotAccepted)
	stream, topic, group := groupConsumer(t)
	partition := uint32(0)
	create := func() error {
		_, err := client.do(context.Background(), &command.CreateStream{Name: "unbound"})
		return err
	}
	poll := func() error {
		_, err := client.PollMessages(context.Background(), stream, topic, iggcon.NewSingleConsumer(group.Id), iggcon.FirstPollingStrategy(), 1, false, &partition)
		return err
	}
	require.ErrorIs(t, create(), ierror.ErrTransientNotAccepted)
	require.ErrorIs(t, poll(), ierror.ErrTransientNotAccepted)
	release()
	awaitRecoveryEnd(t, client)
	require.ErrorIs(t, create(), ierror.ErrUnauthenticated, "a failed recovery leaves no session to retry on")
	require.ErrorIs(t, poll(), ierror.ErrUnauthenticated)
	require.Len(t, server.recorded(), 2)
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

func TestDriverAfterSilentMetadataReplicaDiscoversThroughAnotherLink(t *testing.T) {
	const oldAddress = "127.0.0.1:19001"
	const newAddress = "127.0.0.1:19002"
	client, oldPeer := newPipeClient(t)
	oldLink := client.links.metadata()
	oldLink.endpoint = oldAddress
	client.links.slots[0].endpoint = oldAddress
	client.links.bootstrap = nil
	newPeer, connection := net.Pipe()
	defer func() { _ = newPeer.Close() }()
	newLink := newTCPLink(connection, newAddress)
	newLink.markBound(client.sessionFence, ^uint64(0))
	client.links.slots[1] = &replicaSlot{endpoint: newAddress, link: newLink, cancel: func() {}}
	client.links.rosterOrder = []uint8{0, 1}
	oldServer := serve(oldPeer, func(_ int, _ request) []byte { return nil })
	newServer := serve(newPeer, func(_ int, read request) []byte {
		if read.code() == uint32(command.GetClusterMetadataCode) {
			return clusterMetadataFrame(t, 1, oldAddress, newAddress)
		}
		return replyFrame(vsr.OperationNonReplicated, nil)
	})
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkPoll)
	defer cancel()
	require.ErrorIs(t, client.Ping(ctx), ierror.ErrTransientNotCommitted)
	require.True(t, linkReady(oldLink), "one expired call must not close other in-flight work")
	ctx, nextCancel := context.WithTimeout(context.Background(), managedLinkHeartbeat)
	defer nextCancel()
	require.NoError(t, client.Ping(ctx))
	require.Equal(t, 1, requestCount(oldServer.recorded(), command.PingCode))
	require.Equal(t, 1, requestCount(newServer.recorded(), command.GetClusterMetadataCode))
	require.Equal(t, 1, requestCount(newServer.recorded(), command.PingCode))
	require.True(t, linkReady(oldLink))
	require.Equal(t, uint64(100), client.session.SessionID())
}

func TestDetachedLinkPreservesRetainedMutation(t *testing.T) {
	for _, evicted := range []bool{false, true} {
		t.Run(fmt.Sprintf("evicted_%t", evicted), func(t *testing.T) {
			var attempts atomic.Int32
			var detach atomic.Bool
			var server *testListener
			server = listenVSR(t, nil, func(connection, index int, read request) []byte {
				if read.operation() == vsr.OperationCreateStream {
					if attempt := int(attempts.Add(1)) - 1; attempt < 2 {
						return uncertainAnswer(attempt, read)
					}
					return committedReply(read, 0)
				}
				if read.code() == uint32(command.PingCode) && detach.CompareAndSwap(true, false) {
					if evicted {
						return evictionFrame(vsr.EvictionNoSession, 0, 0)
					}
					return statusReplyFrame(read.operation(), uint32(ierror.UnauthenticatedCode), nil)
				}
				return singleNodeHandler(t, func() string { return server.address() })(connection, index, read)
			})
			client := newDialingClient(t, server.address(),
				WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")), WithHeartbeatInterval(time.Hour))
			ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
			defer cancel()
			require.NoError(t, client.Connect(ctx))
			operationID := uncertainWrite(t, client)
			detach.Store(true)
			require.NoError(t, client.Ping(ctx))
			require.NoError(t, client.ResolveOperation(ctx, operationID))
			writes := createStreams(server.recorded())
			require.Len(t, writes, 3)
			require.Equal(t, writes[0].header, writes[2].header)
			require.Equal(t, writes[0].payload, writes[2].payload)
			require.Len(t, registerRequests(server), 1)
		})
	}
}
