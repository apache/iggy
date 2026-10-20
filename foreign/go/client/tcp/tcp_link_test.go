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
