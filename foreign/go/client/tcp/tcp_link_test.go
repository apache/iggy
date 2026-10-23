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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/require"
)

func linkTestFrame(id uint64) *operationFrame {
	data := make([]byte, vsr.HeaderSize)
	vsr.EncodeRequestHeader((*[vsr.HeaderSize]byte)(data), vsr.RequestFields{Size: vsr.HeaderSize, Client: vsr.ClientID{Lo: 7}, Request: id, Operation: vsr.OperationNonReplicated, Session: 3, NonReplicatedCode: uint32(command.PingCode)})
	return newOperationFrame(data, nil)
}

func TestTCPLinkCorrelatesReorderedRepliesAndIsolatesCancellation(t *testing.T) {
	client, server := net.Pipe()
	link := newTCPLink(client, "pipe")
	t.Cleanup(func() { link.close(ierror.ErrClientShutdown); _ = server.Close(); link.done.Wait() })
	firstCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := linkTestFrame(1)
	defer first.release()
	second := linkTestFrame(2)
	defer second.release()
	firstResult := make(chan linkReply, 1)
	secondResult := make(chan linkReply, 1)
	go func() { reply, _ := link.exchange(firstCtx, first); firstResult <- reply }()
	readFirst, err := readRequest(server)
	require.NoError(t, err)
	go func() { reply, _ := link.exchange(context.Background(), second); secondResult <- reply }()
	readSecond, err := readRequest(server)
	require.NoError(t, err)
	cancel()
	require.ErrorIs(t, (<-firstResult).err, ierror.ErrTransientNotCommitted)
	late := replyFrame(vsr.OperationNonReplicated, []byte{1})
	echoReplyRequest(late, readFirst)
	_, err = server.Write(late)
	require.NoError(t, err)
	response := replyFrame(vsr.OperationNonReplicated, []byte{2})
	echoReplyRequest(response, readSecond)
	_, err = server.Write(response)
	require.NoError(t, err)
	select {
	case reply := <-secondResult:
		require.NoError(t, reply.err)
		require.Equal(t, []byte{2}, reply.body)
	case <-time.After(time.Second):
		t.Fatal("second exchange was disrupted by another caller's cancellation")
	}
}

func TestPendingRegistryBlocksOnlyItsLaneAndPinsQueuedBytesAfterAbandon(t *testing.T) {
	registry := newOperationRegistry(1, vsr.HeaderSize)
	ticket, err := registry.enter(context.Background(), laneKey{incarnation: 1})
	require.NoError(t, err)
	require.NoError(t, ticket.reserve(context.Background(), vsr.HeaderSize))
	data := make([]byte, vsr.HeaderSize)
	binary.LittleEndian.PutUint64(data[frameOffsetRequest:], uint64(1)<<63)
	operation := ticket.freeze(command.SendMessagesCode, nil, data)
	writer := operation.frame.acquire()
	ticket.finish(true)
	_, err = registry.enter(context.Background(), operation.key)
	var blocked ierror.LaneBlocked
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, operation.id, blocked.OperationID)
	other, err := registry.enter(context.Background(), laneKey{incarnation: 2})
	require.NoError(t, err)
	defer other.finish(false)
	require.NoError(t, registry.abandon(operation.id))
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, other.reserve(ctx, vsr.HeaderSize), ierror.ErrTransientNotAccepted)
	writer.release()
	require.NoError(t, other.reserve(context.Background(), vsr.HeaderSize))
}

func TestPendingRegistryAbandonKeepsTheLaneOfARunningReplayUntilItFinishes(t *testing.T) {
	registry := newOperationRegistry(DefaultMaxPendingOperations, DefaultMaxPendingOperationBytes)
	ticket, err := registry.enter(context.Background(), laneKey{incarnation: 1, partition: 1})
	require.NoError(t, err)
	require.NoError(t, ticket.reserve(context.Background(), vsr.HeaderSize))
	data := make([]byte, vsr.HeaderSize)
	binary.LittleEndian.PutUint64(data[frameOffsetRequest:], uint64(1)<<63)
	operation := ticket.freeze(command.SendMessagesCode, nil, data)
	ticket.finish(true)
	replay, err := registry.resume(context.Background(), operation.id)
	require.NoError(t, err)
	require.NoError(t, registry.abandon(operation.id))
	busy, cancelBusy := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelBusy()
	_, err = registry.enter(busy, operation.key)
	require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
	replay.finish(true)
	free, cancelFree := context.WithTimeout(context.Background(), time.Second)
	defer cancelFree()
	next, err := registry.enter(free, operation.key)
	require.NoError(t, err)
	next.finish(false)
	require.Empty(t, registry.lanes)
	require.Zero(t, registry.bytes)
}

func TestPendingRegistryResetKeepsPhysicalBytesUntilOldWriterFinishes(t *testing.T) {
	registry := newOperationRegistry(1, vsr.HeaderSize)
	ticket, err := registry.enter(context.Background(), laneKey{})
	require.NoError(t, err)
	require.NoError(t, ticket.reserve(context.Background(), vsr.HeaderSize))
	operation := ticket.freeze(command.CreateStreamCode, nil, make([]byte, vsr.HeaderSize))
	writer := operation.frame.acquire()
	registry.reset()
	ticket.finish(false)
	fresh, err := registry.enter(context.Background(), laneKey{})
	require.NoError(t, err)
	defer fresh.finish(false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, fresh.reserve(ctx, vsr.HeaderSize), ierror.ErrTransientNotAccepted)
	writer.release()
	require.NoError(t, fresh.reserve(context.Background(), vsr.HeaderSize))
}

func TestPendingRegistryWakesOnlyTheCallsThatCanProceed(t *testing.T) {
	registry := newOperationRegistry(DefaultMaxPendingOperations, DefaultMaxPendingOperationBytes)
	freedHolder, freedCalls := queueStandIns(t, registry, 1)
	retainedHolder, retainedCalls := queueStandIns(t, registry, 2)
	freedHolder.finish(false)
	require.Equal(t, []int{1, 0, 0}, bufferedWakeups(freedCalls), "a completion wakes only the head of its lane")
	require.Equal(t, []int{0, 0, 0}, bufferedWakeups(retainedCalls), "a completion does not wake another lane")
	registry.mu.Lock()
	registry.leaveLaneLocked(freedHolder.key, registry.lanes[freedHolder.key], freedCalls[0])
	registry.mu.Unlock()
	require.Equal(t, []int{1, 1, 0}, bufferedWakeups(freedCalls), "a head that leaves a free lane wakes the next call")
	retainOperation(t, retainedHolder)
	require.Equal(t, []int{1, 1, 1}, bufferedWakeups(retainedCalls), "a retained operation wakes every call of its lane")
}

func TestPendingRegistryGrantsBudgetInArrivalOrderToEachWaiterThatFits(t *testing.T) {
	t.Run("equal_sizes", func(t *testing.T) {
		const waiters = 4
		registry := newOperationRegistry(1, DefaultMaxPendingOperationBytes)
		holder := enterLane(t, registry, 0)
		require.NoError(t, holder.reserve(context.Background(), vsr.HeaderSize))
		granted := make(chan int, waiters)
		for waiter := range waiters {
			ticket := enterLane(t, registry, uint32(waiter+1))
			go func() {
				defer ticket.finish(false)
				if err := ticket.reserve(context.Background(), vsr.HeaderSize); err != nil {
					t.Error(err)
					return
				}
				granted <- waiter
			}()
			requireBudgetWaiters(t, registry, waiter+1)
		}
		holder.finish(false)
		require.Eventually(t, func() bool { return len(granted) == waiters }, time.Second, time.Millisecond)
		for waiter := range waiters {
			require.Equal(t, waiter, <-granted)
		}
	})
	t.Run("smaller_later_waiter", func(t *testing.T) {
		registry := newOperationRegistry(DefaultMaxPendingOperations, 3*vsr.HeaderSize)
		larger := enterLane(t, registry, 1)
		require.NoError(t, larger.reserve(context.Background(), 2*vsr.HeaderSize))
		smaller := enterLane(t, registry, 2)
		require.NoError(t, smaller.reserve(context.Background(), vsr.HeaderSize))
		first := reserveLater(context.Background(), enterLane(t, registry, 3), 2*vsr.HeaderSize)
		requireBudgetWaiters(t, registry, 1)
		second := reserveLater(context.Background(), enterLane(t, registry, 4), vsr.HeaderSize)
		requireBudgetWaiters(t, registry, 2)
		smaller.finish(false)
		require.Eventually(t, func() bool { return len(second) == 1 }, time.Second, time.Millisecond,
			"a later waiter that fits is not held back")
		require.NoError(t, <-second)
		requireBudgetWaiters(t, registry, 1)
		larger.finish(false)
		require.Eventually(t, func() bool { return len(first) == 1 }, time.Second, time.Millisecond)
		require.NoError(t, <-first)
	})
}

func TestPendingRegistryLateBudgetGrantPassesToTheNextWaiter(t *testing.T) {
	registry := newOperationRegistry(1, DefaultMaxPendingOperationBytes)
	holder := enterLane(t, registry, 0)
	require.NoError(t, holder.reserve(context.Background(), vsr.HeaderSize))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := reserveLater(ctx, enterLane(t, registry, 1), vsr.HeaderSize)
	requireBudgetWaiters(t, registry, 1)
	next := enterLane(t, registry, 2)
	second := reserveLater(context.Background(), next, vsr.HeaderSize)
	requireBudgetWaiters(t, registry, 2)
	func() {
		// The lock keeps the first waiter from running before the grant reaches
		// it, so it sees the grant only after its context ended.
		registry.mu.Lock()
		defer registry.mu.Unlock()
		cancel()
		holder.releaseReservationLocked()
		holder.reservation = nil
		registry.grantBudgetLocked()
	}()
	require.Eventually(t, func() bool { return len(first) == 1 && len(second) == 1 }, time.Second, time.Millisecond)
	require.ErrorIs(t, <-first, ierror.ErrTransientNotAccepted)
	require.NoError(t, <-second)
	next.finish(false)
	require.Zero(t, registry.bytes)
	require.Empty(t, registry.reservations)
}

func TestPendingRegistryResetEndsBudgetAndResolverWaitsAndKeepsLaneQueues(t *testing.T) {
	registry := newOperationRegistry(1, DefaultMaxPendingOperationBytes)
	// The retained operation holds the only slot, and its replay holds the lane.
	operation := retainOperation(t, enterLane(t, registry, 1))
	replay, err := registry.resume(context.Background(), operation.id)
	require.NoError(t, err)
	resolved := make(chan error, 1)
	go func() { _, err := registry.resume(context.Background(), operation.id); resolved <- err }()
	reserving := enterLane(t, registry, 2)
	reserved := reserveLater(context.Background(), reserving, vsr.HeaderSize)
	holder := enterLane(t, registry, 3)
	entered := make(chan error, 1)
	go func() {
		ticket, err := registry.enter(context.Background(), holder.key)
		if err == nil {
			ticket.finish(false)
		}
		entered <- err
	}()
	require.Eventually(t, func() bool {
		registry.mu.Lock()
		defer registry.mu.Unlock()
		return len(registry.resumers[operation.id]) == 1 && len(registry.budget) == 1 && len(registry.lanes[holder.key].waiters) == 1
	}, time.Second, time.Millisecond)
	registry.reset()
	require.Eventually(t, func() bool { return len(resolved) == 1 && len(reserved) == 1 }, time.Second, time.Millisecond,
		"a reset ends every budget and resolver wait")
	require.ErrorIs(t, <-resolved, ierror.ErrOperationNotRetained)
	require.ErrorIs(t, <-reserved, ierror.ErrUnauthenticated)
	require.Never(t, func() bool { return len(entered) != 0 }, 20*time.Millisecond, time.Millisecond,
		"a call keeps its place behind the holder of its lane")
	holder.finish(false)
	require.Eventually(t, func() bool { return len(entered) == 1 }, time.Second, time.Millisecond)
	require.NoError(t, <-entered)
	reserving.finish(false)
	replay.finish(false)
	require.Zero(t, registry.bytes)
	require.Empty(t, registry.reservations)
	require.Empty(t, registry.lanes)
	require.Empty(t, registry.resumers)
}

func TestPendingRegistryEveryBudgetReleaseGrantsAWaitingReservation(t *testing.T) {
	for _, release := range []string{"unsent_reservation", "pinned_frame", "abandon"} {
		t.Run(release, func(t *testing.T) {
			registry := newOperationRegistry(1, vsr.HeaderSize)
			holder := enterLane(t, registry, 1)
			var free func()
			switch release {
			case "unsent_reservation":
				require.NoError(t, holder.reserve(context.Background(), vsr.HeaderSize))
				free = holder.releaseOperation
			case "pinned_frame":
				require.NoError(t, holder.reserve(context.Background(), vsr.HeaderSize))
				operation := holder.freeze(command.SendMessagesCode, nil, make([]byte, vsr.HeaderSize))
				writer := operation.frame.acquire()
				holder.finish(false)
				free = writer.release
			case "abandon":
				operation := retainOperation(t, holder)
				free = func() { require.NoError(t, registry.abandon(operation.id)) }
			}
			reserved := reserveLater(context.Background(), enterLane(t, registry, 2), vsr.HeaderSize)
			requireBudgetWaiters(t, registry, 1)
			free()
			require.Eventually(t, func() bool { return len(reserved) == 1 }, time.Second, time.Millisecond)
			require.NoError(t, <-reserved)
		})
	}
}

func TestPendingRegistryWakesResolversWhenTheReplayEndsOrTheOperationIsAbandoned(t *testing.T) {
	for _, end := range []string{"replay_keeps_it", "replay_proves_it", "abandon"} {
		t.Run(end, func(t *testing.T) {
			registry := newOperationRegistry(DefaultMaxPendingOperations, DefaultMaxPendingOperationBytes)
			operation := retainOperation(t, enterLane(t, registry, 1))
			replay, err := registry.resume(context.Background(), operation.id)
			require.NoError(t, err)
			resolved := make(chan error, 1)
			go func() { _, err := registry.resume(context.Background(), operation.id); resolved <- err }()
			require.Eventually(t, func() bool {
				registry.mu.Lock()
				defer registry.mu.Unlock()
				return len(registry.resumers[operation.id]) == 1
			}, time.Second, time.Millisecond)
			var want error = ierror.ErrOperationNotRetained
			switch end {
			case "replay_keeps_it":
				replay.finish(true)
				want = nil
			case "replay_proves_it":
				replay.prove(nil)
			case "abandon":
				require.NoError(t, registry.abandon(operation.id))
			}
			require.Eventually(t, func() bool { return len(resolved) == 1 }, time.Second, time.Millisecond)
			require.ErrorIs(t, <-resolved, want)
		})
	}
}

const (
	registryBenchLanes          = 8
	registryBenchCallersPerLane = 8
)

// BenchmarkOperationRegistryLaneHandoff measures one lane handoff while each
// lane queues calls behind its holder.
func BenchmarkOperationRegistryLaneHandoff(b *testing.B) {
	registry := newOperationRegistry(DefaultMaxPendingOperations, DefaultMaxPendingOperationBytes)
	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var callers sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for caller := range registryBenchLanes * registryBenchCallersPerLane {
		key := laneKey{partition: uint32(caller % registryBenchLanes)}
		callers.Go(func() {
			for remaining.Add(-1) >= 0 {
				ticket, err := registry.enter(context.Background(), key)
				if err != nil {
					b.Error(err)
					return
				}
				ticket.finish(false)
			}
		})
	}
	callers.Wait()
}

// queueStandIns takes a lane and queues stand-ins for waiting calls behind its
// holder. A stand-in keeps its wakeup buffered, so a test can count them.
func queueStandIns(t *testing.T, registry *operationRegistry, partition uint32) (*laneTicket, []registryWaiter) {
	t.Helper()
	holder := enterLane(t, registry, partition)
	calls := []registryWaiter{newRegistryWaiter(), newRegistryWaiter(), newRegistryWaiter()}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	holder.lane.waiters = append(holder.lane.waiters, calls...)
	return holder, calls
}

func bufferedWakeups(calls []registryWaiter) []int {
	wakeups := make([]int, len(calls))
	for index, call := range calls {
		wakeups[index] = len(call)
	}
	return wakeups
}

func enterLane(t *testing.T, registry *operationRegistry, partition uint32) *laneTicket {
	t.Helper()
	ticket, err := registry.enter(context.Background(), laneKey{partition: partition})
	require.NoError(t, err)
	return ticket
}

// retainOperation ends the call of ticket as an uncertain mutation, so its
// operation stays retained on the lane.
func retainOperation(t *testing.T, ticket *laneTicket) *pendingOperation {
	t.Helper()
	require.NoError(t, ticket.reserve(context.Background(), vsr.HeaderSize))
	operation := ticket.freeze(command.SendMessagesCode, nil, make([]byte, vsr.HeaderSize))
	ticket.finish(true)
	return operation
}

func reserveLater(ctx context.Context, ticket *laneTicket, size int) <-chan error {
	reserved := make(chan error, 1)
	go func() { reserved <- ticket.reserve(ctx, size) }()
	return reserved
}

func requireBudgetWaiters(t *testing.T, registry *operationRegistry, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		registry.mu.Lock()
		defer registry.mu.Unlock()
		return len(registry.budget) == count
	}, time.Second, time.Millisecond)
}

func TestTCPLinkDeliversRepliesInServerOrderToTheirOwnWaiters(t *testing.T) {
	client, server := net.Pipe()
	link := newTCPLink(client, "pipe")
	t.Cleanup(func() { link.close(ierror.ErrClientShutdown); _ = server.Close(); link.done.Wait() })
	first, second := linkTestFrame(1), linkTestFrame(2)
	defer first.release()
	defer second.release()
	firstResult, secondResult := make(chan linkReply, 1), make(chan linkReply, 1)
	go func() { reply, _ := link.exchange(context.Background(), first); firstResult <- reply }()
	firstRead, err := readRequest(server)
	require.NoError(t, err)
	go func() { reply, _ := link.exchange(context.Background(), second); secondResult <- reply }()
	secondRead, err := readRequest(server)
	require.NoError(t, err)
	secondReply := replyFrame(vsr.OperationNonReplicated, []byte{2})
	echoReplyRequest(secondReply, secondRead)
	_, err = server.Write(secondReply)
	require.NoError(t, err)
	require.Equal(t, []byte{2}, (<-secondResult).body)
	select {
	case <-firstResult:
		t.Fatal("a later reply completed the wrong waiter")
	default:
	}
	firstReply := replyFrame(vsr.OperationNonReplicated, []byte{1})
	echoReplyRequest(firstReply, firstRead)
	_, err = server.Write(firstReply)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, (<-firstResult).body)
}

func TestTCPLinkPendingAndWriterAdmissionStayBounded(t *testing.T) {
	client, server := net.Pipe()
	link := newTCPLink(client, "pipe")
	t.Cleanup(func() { link.close(ierror.ErrClientShutdown); _ = server.Close(); link.done.Wait() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := make(chan linkReply, linkPendingCapacity)
	for id := 1; id <= linkPendingCapacity; id++ {
		frame := linkTestFrame(uint64(id))
		go func() { defer frame.release(); reply, _ := link.exchange(ctx, frame); completed <- reply }()
	}
	require.Eventually(t, func() bool {
		link.mu.Lock()
		defer link.mu.Unlock()
		return len(link.pending) == linkPendingCapacity
	}, time.Second, time.Millisecond)
	require.LessOrEqual(t, len(link.writes), linkWriterCapacity)
	frame := linkTestFrame(linkPendingCapacity + 1)
	defer frame.release()
	waiting, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer stop()
	reply, sent := link.exchange(waiting, frame)
	require.False(t, sent)
	require.ErrorIs(t, reply.err, ierror.ErrTransientNotAccepted)
	cancel()
	for range linkPendingCapacity {
		require.Error(t, (<-completed).err)
	}
}

func TestTCPLinkEvictionFailsEveryPendingRequest(t *testing.T) {
	for _, test := range []struct {
		reason vsr.EvictionReason
		want   error
	}{
		{vsr.EvictionNoSession, ierror.ErrUnauthenticated},
		// The session outlives a stale connection, so its requests see a lost link.
		{vsr.EvictionStaleClient, ierror.ErrTransientNotCommitted},
	} {
		client, server := net.Pipe()
		link := newTCPLink(client, "pipe")
		t.Cleanup(func() { link.close(ierror.ErrClientShutdown); _ = server.Close(); link.done.Wait() })
		completed := make(chan linkReply, 2)
		for id := 1; id <= 2; id++ {
			frame := linkTestFrame(uint64(id))
			go func() {
				defer frame.release()
				reply, _ := link.exchange(context.Background(), frame)
				completed <- reply
			}()
			_, err := readRequest(server)
			require.NoError(t, err)
		}
		_, err := server.Write(evictionFrame(test.reason, 0, 0))
		require.NoError(t, err)
		for range 2 {
			require.ErrorIs(t, (<-completed).err, test.want, "reason %d", test.reason)
		}
		require.False(t, linkReady(link))
	}
}
