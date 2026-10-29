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
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Numeric consumer, stream and topic identifiers precede the fetch limits.
const fetchLimitsAt = 19

const deferredTestTimeout = 3 * time.Second

func deferredOptions(wait, timeout time.Duration) iggcon.DeferredPollOptions {
	return iggcon.DeferredPollOptions{MaxWait: wait, MinCount: 1, MaxBytes: 1 << 20, RequestTimeout: timeout}
}

// deferredLimits reads the wait and the request timeout that a recorded
// deferred poll or fetch carries.
func deferredLimits(t *testing.T, read request) (time.Duration, time.Duration) {
	t.Helper()
	if read.code() == uint32(command.FetchMessagesCode) {
		limits := read.payload[fetchLimitsAt:]
		return wireDuration(limits), wireDuration(limits[wireDurationSize:])
	}
	require.Equal(t, uint32(command.PollMessagesDeferredCode), read.code())
	limits := read.payload[len(read.payload)-deferredParametersSize:]
	return wireDuration(limits), wireDuration(limits[deferredParametersSize-wireDurationSize:])
}

// requireCancellationOf checks that cancellation cancels parked: a header
// without a body that names the same client, session and request.
func requireCancellationOf(t *testing.T, parked, cancellation request) {
	t.Helper()
	assert.Equal(t, uint32(command.CancelDeferredRequestCode), cancellation.code())
	assert.Equal(t, vsr.OperationNonReplicated, cancellation.operation())
	assert.Empty(t, cancellation.payload)
	assert.Equal(t, uint32(vsr.HeaderSize), binary.LittleEndian.Uint32(cancellation.header[frameOffsetSize:]))
	assert.Equal(t, parked.requestID(), cancellation.requestID())
	assert.Equal(t, parked.sessionID(), cancellation.sessionID())
	assert.Equal(t, parked.clientID(), cancellation.clientID())
}

func receive[T any](t *testing.T, channel <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(deferredTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func pollDeferred(ctx context.Context, t *testing.T, client *IggyTcpClient, partition uint32, options iggcon.DeferredPollOptions) (*iggcon.PolledMessage, error) {
	consumer := iggcon.NewSingleConsumer(numericIdentifier(t, 3))
	return client.PollMessagesDeferred(ctx, numericIdentifier(t, 1), numericIdentifier(t, 2), consumer,
		iggcon.OffsetPollingStrategy(0), 1, false, &partition, options)
}

func TestPollMessagesDeferred_CancelSendsTheCancellationOnTheSameLink(t *testing.T) {
	client, serverConn := newPipeClient(t)
	parked := make(chan request, 1)
	cancellations := make(chan request, 1)
	servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		switch read.code() {
		case uint32(command.PollMessagesDeferredCode):
			parked <- read
			return nil
		case uint32(command.CancelDeferredRequestCode):
			cancellations <- read
			// A reply that crossed the cancellation names the cancelled request.
			return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(1))
		default:
			return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	polled := make(chan error, 1)
	go func() {
		_, err := pollDeferred(ctx, t, client, 1, deferredOptions(5*time.Second, 10*time.Second))
		polled <- err
	}()
	deferred := receive(t, parked, "the deferred poll")

	cancel()

	require.ErrorIs(t, receive(t, polled, "the cancelled poll"), ierror.ErrTransientNotCommitted)
	requireCancellationOf(t, deferred, receive(t, cancellations, "the cancellation"))
	partition := uint32(2)
	after, err := client.PollMessages(context.Background(), numericIdentifier(t, 1), numericIdentifier(t, 2),
		iggcon.NewSingleConsumer(numericIdentifier(t, 3)), iggcon.OffsetPollingStrategy(0), 1, false, &partition)
	require.NoError(t, err, "the link stays usable after a cancellation")
	assert.Equal(t, partition, after.PartitionId, "the late reply of the cancelled poll is dropped")
	link := client.links.bootstrap
	require.Eventually(t, func() bool {
		link.mu.Lock()
		defer link.mu.Unlock()
		return len(link.pending) == 0 && len(link.capacity) == 0
	}, deferredTestTimeout, managedLinkPoll, "the cancelled poll keeps no pending entry or capacity slot")
}

func TestPollMessages_CancelOfAnImmediatePollSendsNoCancellation(t *testing.T) {
	client, serverConn := newPipeClient(t)
	parked := make(chan struct{}, 1)
	var polls atomic.Int32
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if polls.Add(1) == 1 {
			parked <- struct{}{}
			return nil
		}
		return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(polledPartition(t, read)))
	})
	partition := uint32(1)
	poll := func(ctx context.Context) error {
		_, err := client.PollMessages(ctx, numericIdentifier(t, 1), numericIdentifier(t, 2),
			iggcon.NewSingleConsumer(numericIdentifier(t, 3)), iggcon.OffsetPollingStrategy(0), 1, false, &partition)
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	polled := make(chan error, 1)
	go func() { polled <- poll(ctx) }()
	receive(t, parked, "the poll")

	cancel()

	require.Error(t, receive(t, polled, "the cancelled poll"))
	require.NoError(t, poll(context.Background()))
	assert.Zero(t, requestCount(server.recorded(), command.CancelDeferredRequestCode))
}

func TestPollMessagesDeferred_RetriesACapacityRefusalWithinItsBudget(t *testing.T) {
	client, serverConn := newPipeClient(t)
	var attempts atomic.Int32
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if attempts.Add(1) == 1 {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.DeferredPollCapacityExceededCode), nil)
		}
		return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(1))
	})
	options := deferredOptions(2*time.Second, 5*time.Second)

	polled, err := pollDeferred(context.Background(), t, client, 1, options)

	require.NoError(t, err)
	assert.Equal(t, uint32(1), polled.PartitionId)
	var sent []request
	for _, read := range server.recorded() {
		if read.code() == uint32(command.PollMessagesDeferredCode) {
			sent = append(sent, read)
		}
	}
	require.Len(t, sent, 2, "the refused poll is sent again")
	assert.Equal(t, sent[0].requestID(), sent[1].requestID(), "an admission refusal keeps the request")
	firstWait, firstTimeout := deferredLimits(t, sent[0])
	secondWait, secondTimeout := deferredLimits(t, sent[1])
	assert.LessOrEqual(t, firstTimeout, options.RequestTimeout)
	assert.Less(t, secondTimeout, firstTimeout, "the retry spends the budget of the call")
	assert.Less(t, secondWait, firstWait)
	assert.GreaterOrEqual(t, secondTimeout-secondWait, iggcon.MinDeferredPollReplyMargin)
}

func TestPollMessagesDeferred_EndsAtItsRequestTimeoutAndCancelsThePoll(t *testing.T) {
	client, serverConn := newPipeClient(t)
	parked := make(chan request, 1)
	cancellations := make(chan request, 1)
	servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		switch read.code() {
		case uint32(command.PollMessagesDeferredCode):
			parked <- read
		case uint32(command.CancelDeferredRequestCode):
			cancellations <- read
		}
		return nil
	})
	options := deferredOptions(0, 300*time.Millisecond)
	started := time.Now()

	_, err := pollDeferred(context.Background(), t, client, 1, options)

	require.ErrorIs(t, err, ierror.ErrTransientNotCommitted)
	elapsed := time.Since(started)
	assert.GreaterOrEqual(t, elapsed, options.RequestTimeout)
	assert.Less(t, elapsed, responseReadTimeout/10, "the request timeout, not the operation budget, bounds the call")
	deferred := receive(t, parked, "the deferred poll")
	_, timeout := deferredLimits(t, deferred)
	assert.LessOrEqual(t, timeout, options.RequestTimeout)
	requireCancellationOf(t, deferred, receive(t, cancellations, "the cancellation"))
}

func TestPollMessagesDeferred_ParksAtMostTheDeferredLimit(t *testing.T) {
	client, serverConn := newPipeClient(t)
	parked := make(chan request, maxDeferredRequests+1)
	cancellations := make(chan request, maxDeferredRequests+1)
	servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		switch read.code() {
		case uint32(command.PollMessagesDeferredCode):
			parked <- read
		case uint32(command.CancelDeferredRequestCode):
			cancellations <- read
		}
		return nil
	})
	ctx, cancelAll := context.WithCancel(context.Background())
	var polls sync.WaitGroup
	defer func() {
		cancelAll()
		polls.Wait()
	}()
	start := func(ctx context.Context) {
		polls.Go(func() { _, _ = pollDeferred(ctx, t, client, 1, deferredOptions(10*time.Second, 15*time.Second)) })
	}
	first, cancelFirst := context.WithCancel(ctx)
	start(first)
	firstPoll := receive(t, parked, "the first deferred poll")
	for range maxDeferredRequests - 1 {
		start(ctx)
		receive(t, parked, "a deferred poll under the limit")
	}

	start(ctx)
	select {
	case read := <-parked:
		t.Fatalf("deferred poll %d went past the limit of %d", read.requestID(), maxDeferredRequests)
	case <-time.After(200 * time.Millisecond):
	}
	cancelFirst()

	requireCancellationOf(t, firstPoll, receive(t, cancellations, "the cancellation"))
	receive(t, parked, "the deferred poll that waited for a free slot")
}

// A parked Next or auto-commit poll holds the progress lane of its partition.
// The offset calls of that partition wait for it, and a send does not.
func TestPollMessagesDeferred_AParkedProgressPollHoldsOffsetCallsButNotSends(t *testing.T) {
	calls := newLaneCalls(t)
	for _, offset := range []struct {
		name string
		code command.Code
		call func(context.Context, *IggyTcpClient) error
	}{
		{"get", command.GetOffsetCode, calls.getOffset},
		{"store", command.StoreOffsetCode, calls.storeOffset},
		{"delete", command.DeleteConsumerOffsetCode, calls.deleteOffset},
	} {
		for _, progress := range []struct {
			name       string
			strategy   iggcon.PollingStrategy
			autoCommit bool
		}{
			{"next", iggcon.NextPollingStrategy(), false},
			{"auto_commit", iggcon.OffsetPollingStrategy(0), true},
		} {
			t.Run(offset.name+"/"+progress.name, func(t *testing.T) {
				client, serverConn := newPipeClient(t)
				parked := make(chan struct{}, 1)
				server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
					switch read.code() {
					case uint32(command.PollMessagesDeferredCode):
						parked <- struct{}{}
						return nil
					case uint32(command.CancelDeferredRequestCode):
						return nil
					case uint32(command.SendMessagesCode):
						return replyFrame(vsr.OperationSendMessages, zeroConfirmations())
					case uint32(command.GetOffsetCode):
						return replyFrame(vsr.OperationNonReplicated, consumerOffsetBody(calls.partition))
					default:
						return replyFrame(read.operation(), resultSection())
					}
				})
				// A replicated request carries its operation instead of its code.
				offsetsSent := func() int {
					if operation := vsr.OperationForCode(uint32(offset.code)); operation != vsr.OperationNonReplicated {
						return len(requestsFor(server.recorded(), operation))
					}
					return requestCount(server.recorded(), offset.code)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				polled := make(chan error, 1)
				go func() {
					_, err := client.PollMessagesDeferred(ctx, calls.stream, calls.topic, calls.consumer, progress.strategy, 1,
						progress.autoCommit, &calls.partition, deferredOptions(5*time.Second, 10*time.Second))
					polled <- err
				}()
				receive(t, parked, "the parked poll")
				offsetDone := make(chan error, 1)
				go func() { offsetDone <- offset.call(context.Background(), client) }()
				require.Eventually(t, func() bool { return progressLaneWaiters(client) == 1 },
					deferredTestTimeout, managedLinkPoll, "the offset call waits for the progress lane")

				sendCtx, cancelSend := context.WithTimeout(context.Background(), deferredTestTimeout)
				defer cancelSend()
				require.NoError(t, calls.send(sendCtx, client), "a send does not wait for the parked poll")
				assert.Zero(t, offsetsSent(), "the offset call is not sent while the poll is parked")

				cancel()
				require.ErrorIs(t, receive(t, polled, "the cancelled poll"), ierror.ErrTransientNotCommitted)
				require.NoError(t, receive(t, offsetDone, "the offset call"))
				assert.Equal(t, 1, offsetsSent())
			})
		}
	}
}

func TestSendBinaryRequest_TakesTheDeferredPathsAndRefusesACancellation(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		return replyFrame(vsr.OperationNonReplicated, nil)
	})
	streamId, topicId, consumer := groupConsumer(t)
	ctx := context.Background()
	partition := uint32(1)
	invalid := deferredOptions(time.Second, 2*time.Second)
	invalid.MinCount = 0
	refusedPoll, err := (&command.PollMessages{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: &partition,
		Strategy: iggcon.NextPollingStrategy(), Count: 1, Deferred: &invalid}).MarshalBinary()
	require.NoError(t, err)

	_, err = client.SendBinaryRequest(ctx, uint32(command.PollMessagesDeferredCode), refusedPoll)
	require.ErrorIs(t, err, ierror.ErrInvalidMessagesCount)
	_, err = client.SendBinaryRequest(ctx, uint32(command.CancelDeferredRequestCode), nil)
	require.ErrorIs(t, err, ierror.ErrInvalidCommand)
	for _, code := range []command.Code{command.PollMessagesDeferredCode, command.FetchMessagesCode, command.CancelDeferredRequestCode} {
		_, err = client.SendBinaryRequestToReplica(ctx, uint32(code), nil, 0)
		require.ErrorIs(t, err, ierror.ErrInvalidCommand, "code %d", code)
	}
	assert.Zero(t, requestCount(server.recorded(), command.PollMessagesDeferredCode))
	assert.Zero(t, requestCount(server.recorded(), command.CancelDeferredRequestCode))
}

// A deferred group poll reads one partition. ReadinessGroups and FetchMessages
// read the whole assignment.
func TestPollMessagesDeferred_RefusesAGroupPollWithoutAPartition(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		t.Errorf("unexpected command %d", read.code())
		return nil
	})
	streamId, topicId, consumer := groupConsumer(t)

	_, err := client.PollMessagesDeferred(context.Background(), streamId, topicId, consumer,
		iggcon.NextPollingStrategy(), 1, true, nil, deferredOptions(time.Second, 2*time.Second))

	require.ErrorIs(t, err, ierror.ErrInvalidCommand)
	assert.Empty(t, server.recorded())
}

// The targets of a recorded fetch follow its parameters.
const fetchTargetsAt = fetchLimitsAt + fetchParametersSize

// fetchSection is a section of a fetch reply that holds no records.
type fetchSection struct {
	partition uint32
	status    ierror.Code
	next      uint64
	count     uint32
	records   []byte
	context   iggcon.PartitionContext
}

// fetchBody builds the reply of a fetch. Each section header is [partition
// u32][status u32][current_offset u64][messages_count u32][context]
// [next_offset u64][records_len u32].
func fetchBody(sections ...fetchSection) []byte {
	body := binary.LittleEndian.AppendUint32(nil, uint32(len(sections)))
	for _, section := range sections {
		body = binary.LittleEndian.AppendUint32(body, section.partition)
		body = binary.LittleEndian.AppendUint32(body, uint32(section.status))
		body = binary.LittleEndian.AppendUint64(body, 0)
		body = binary.LittleEndian.AppendUint32(body, section.count)
		body = binary.LittleEndian.AppendUint64(body, section.context.Incarnation)
		body = binary.LittleEndian.AppendUint64(body, section.context.OwnerGeneration)
		body = binary.LittleEndian.AppendUint64(body, section.context.MetadataOp)
		body = binary.LittleEndian.AppendUint64(body, section.next)
		body = binary.LittleEndian.AppendUint32(body, uint32(len(section.records)))
	}
	for _, section := range sections {
		body = append(body, section.records...)
	}
	return body
}

// fetchTarget is a target of a recorded fetch.
type fetchTarget struct {
	partition uint32
	strategy  iggcon.MessagePolling
	value     uint64
	maxBytes  uint32
	fence     iggcon.PartitionContext
}

func fetchTargets(t *testing.T, read request) []fetchTarget {
	t.Helper()
	require.Equal(t, uint32(command.FetchMessagesCode), read.code())
	count := binary.LittleEndian.Uint32(read.payload[fetchTargetsAt-4:])
	require.Len(t, read.payload, fetchTargetsAt+int(count)*fetchTargetSize)
	targets := make([]fetchTarget, 0, count)
	for offset := fetchTargetsAt; offset < len(read.payload); offset += fetchTargetSize {
		target := read.payload[offset:]
		targets = append(targets, fetchTarget{
			partition: binary.LittleEndian.Uint32(target),
			strategy:  iggcon.MessagePolling(target[partitionIdSize]),
			value:     binary.LittleEndian.Uint64(target[partitionIdSize+1:]),
			maxBytes:  binary.LittleEndian.Uint32(target[fetchTargetMaxBytesAt:]),
			fence: iggcon.PartitionContext{
				Incarnation:     binary.LittleEndian.Uint64(target[fetchTargetIncarnationAt:]),
				OwnerGeneration: binary.LittleEndian.Uint64(target[fetchTargetIncarnationAt+8:]),
			},
		})
	}
	return targets
}

// recordedFetch is the one fetch of reads.
func recordedFetch(t *testing.T, reads []request) request {
	t.Helper()
	var fetches []request
	for _, read := range reads {
		if read.code() == uint32(command.FetchMessagesCode) {
			fetches = append(fetches, read)
		}
	}
	require.Len(t, fetches, 1)
	return fetches[0]
}

type fetchOutcome struct {
	sections []iggcon.FetchMessagesSection
	err      error
}

// startFetch fetches partitions from strategy in the background.
func startFetch(ctx context.Context, client *IggyTcpClient, calls laneCalls, strategy iggcon.PollingStrategy, autoCommit bool, partitions ...uint32) <-chan fetchOutcome {
	targets := make([]iggcon.FetchMessagesTarget, 0, len(partitions))
	for _, partition := range partitions {
		targets = append(targets, iggcon.FetchMessagesTarget{PartitionId: partition, Strategy: strategy})
	}
	fetched := make(chan fetchOutcome, 1)
	go func() {
		sections, err := client.FetchMessages(ctx, calls.stream, calls.topic, calls.consumer, targets, 10, autoCommit,
			deferredOptions(5*time.Second, 10*time.Second))
		fetched <- fetchOutcome{sections: sections, err: err}
	}()
	return fetched
}

// answerParked writes reply to the request that the server left parked.
func answerParked(t *testing.T, conn net.Conn, parked request, reply []byte) {
	t.Helper()
	echoReplyRequest(reply, parked)
	_, err := conn.Write(reply)
	require.NoError(t, err)
}

// A fetch goes to the primary of its first target in one request, because
// its caller groups the targets. Its header carries no partition context,
// because each target carries its own.
func TestFetchMessages_SendsOneRequestToThePrimaryOfItsFirstTarget(t *testing.T) {
	fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
		if read.code() != uint32(command.FetchMessagesCode) {
			return nil, false
		}
		return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 1, next: 1},
			fetchSection{partition: 0, status: ierror.NotPrimaryCode})), true
	}, nil)
	streamId, topicId, consumer := groupConsumer(t)
	next := iggcon.NextPollingStrategy()

	sections, err := fixture.client.FetchMessages(context.Background(), streamId, topicId, consumer,
		[]iggcon.FetchMessagesTarget{{PartitionId: 1, Strategy: next}, {PartitionId: 0, Strategy: next}},
		10, false, deferredOptions(0, 2*time.Second))

	require.NoError(t, err)
	require.Len(t, sections, 2)
	assert.ErrorIs(t, sections[1].Err, ierror.ErrNotPrimary)
	fetch := recordedFetch(t, fixture.primaries[1].recorded())
	assert.Equal(t, make([]byte, iggcon.PartitionContextSize), fetch.header[frameOffsetContext:frameOffsetContext+iggcon.PartitionContextSize])
	targets := fetchTargets(t, fetch)
	require.Len(t, targets, 2)
	assert.Equal(t, []uint32{1, 0}, []uint32{targets[0].partition, targets[1].partition})
	assert.Zero(t, requestCount(fixture.primaries[0].recorded(), command.FetchMessagesCode))
	assert.Zero(t, requestCount(fixture.coordinator.recorded(), command.FetchMessagesCode))
}

// Each target of a fetch may take the whole byte budget, and a target without
// a context is fenced to the incarnation of its route. The call returns the
// sections in reply order.
func TestFetchMessages_FillsTheCapAndTheFenceOfEachTarget(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.code() != uint32(command.FetchMessagesCode) {
			t.Errorf("unexpected command %d", read.code())
			return nil
		}
		return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 2, status: ierror.NotPrimaryCode},
			fetchSection{partition: 1, next: 42}))
	})
	calls := newLaneCalls(t)
	fence := iggcon.PartitionContext{Incarnation: 5, OwnerGeneration: 9, MetadataOp: 3}
	options := deferredOptions(time.Second, 3*time.Second)

	sections, err := client.FetchMessages(context.Background(), calls.stream, calls.topic, calls.consumer,
		[]iggcon.FetchMessagesTarget{
			{PartitionId: 1, Strategy: iggcon.OffsetPollingStrategy(41)},
			{PartitionId: 2, Strategy: iggcon.NextPollingStrategy().WithContext(fence)},
		}, 10, false, options)

	require.NoError(t, err)
	assert.Equal(t, []fetchTarget{
		{partition: 1, strategy: iggcon.POLLING_OFFSET, value: 41, maxBytes: options.MaxBytes, fence: iggcon.PartitionContext{Incarnation: 17}},
		{partition: 2, strategy: iggcon.POLLING_NEXT, maxBytes: options.MaxBytes, fence: iggcon.PartitionContext{Incarnation: 5, OwnerGeneration: 9}},
	}, fetchTargets(t, recordedFetch(t, server.recorded())))
	assert.Equal(t, 1, requestCount(server.recorded(), command.GetPartitionRouteCode), "a target with a context needs no route")
	require.Len(t, sections, 2)
	assert.Equal(t, uint32(2), sections[0].PartitionId)
	assert.ErrorIs(t, sections[0].Err, ierror.ErrNotPrimary)
	assert.Equal(t, uint32(1), sections[1].PartitionId)
	assert.NoError(t, sections[1].Err)
	assert.Equal(t, uint64(42), sections[1].NextOffset)
}

// A fetch that commits or starts at Next holds the progress lane of each of
// its partitions while it is parked, so the offset calls of those partitions
// wait for it. A fetch from offsets that does not commit holds no lane. Each
// takes one deferred slot.
func TestFetchMessages_AProgressFetchHoldsTheProgressLanesOfItsPartitions(t *testing.T) {
	calls := newLaneCalls(t)
	for _, tt := range []struct {
		name       string
		strategy   iggcon.PollingStrategy
		autoCommit bool
		progress   bool
	}{
		{"auto_commit", iggcon.OffsetPollingStrategy(0), true, true},
		{"next", iggcon.NextPollingStrategy(), false, true},
		{"offset", iggcon.OffsetPollingStrategy(0), false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			parked := make(chan request, 1)
			server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
				if read.code() == uint32(command.FetchMessagesCode) {
					parked <- read
					return nil
				}
				return replyFrame(read.operation(), resultSection())
			})
			storesSent := func() int {
				return len(requestsFor(server.recorded(), vsr.OperationForCode(uint32(command.StoreOffsetCode))))
			}
			fetched := startFetch(context.Background(), client, calls, tt.strategy, tt.autoCommit, 2, 1)
			fetch := receive(t, parked, "the parked fetch")
			assert.Len(t, client.deferred, 1, "a fetch takes one deferred slot")
			stored := make(chan error, 2)
			for _, partition := range []uint32{1, 2} {
				store := calls
				store.partition = partition
				go func() { stored <- store.storeOffset(context.Background(), client) }()
			}

			if tt.progress {
				require.Eventually(t, func() bool { return progressLaneWaiters(client) == 2 },
					deferredTestTimeout, managedLinkPoll, "the store of each partition waits for the fetch")
				assert.Zero(t, storesSent())
			} else {
				require.NoError(t, receive(t, stored, "a store"))
				require.NoError(t, receive(t, stored, "a store"))
			}
			answerParked(t, serverConn, fetch, replyFrame(vsr.OperationNonReplicated, fetchBody()))

			require.NoError(t, receive(t, fetched, "the fetch").err)
			if tt.progress {
				require.NoError(t, receive(t, stored, "a store"))
				require.NoError(t, receive(t, stored, "a store"))
			}
			assert.Equal(t, 2, storesSent())
		})
	}
}

// A fetch takes the progress lanes of its partitions in ascending partition
// order, so a fetch that waits for a lane holds none of the lanes above it.
func TestFetchMessages_TakesTheProgressLanesInPartitionOrder(t *testing.T) {
	calls := newLaneCalls(t)
	client, serverConn := newPipeClient(t)
	parked := make(chan request, 2)
	servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.FetchMessagesCode) {
			parked <- read
			return nil
		}
		return replyFrame(read.operation(), resultSection())
	})
	next := iggcon.NextPollingStrategy()
	first := startFetch(context.Background(), client, calls, next, false, 2, 1)
	firstFetch := receive(t, parked, "the first fetch")
	second := startFetch(context.Background(), client, calls, next, false, 3, 1)
	require.Eventually(t, func() bool { return progressLaneWaiters(client) == 1 },
		deferredTestTimeout, managedLinkPoll, "the second fetch waits for the lane of partition 1")

	store := calls
	store.partition = 3
	storeCtx, cancelStore := context.WithTimeout(context.Background(), deferredTestTimeout)
	defer cancelStore()
	require.NoError(t, store.storeOffset(storeCtx, client), "the waiting fetch holds no lane of partition 3")

	answerParked(t, serverConn, firstFetch, replyFrame(vsr.OperationNonReplicated, fetchBody()))
	require.NoError(t, receive(t, first, "the first fetch").err)
	answerParked(t, serverConn, receive(t, parked, "the second fetch"), replyFrame(vsr.OperationNonReplicated, fetchBody()))
	require.NoError(t, receive(t, second, "the second fetch").err)
}

func TestFetchMessages_NamesTheBlockedPartitionAndReleasesEarlierLanes(t *testing.T) {
	calls := newLaneCalls(t)
	calls.partition = 2
	client, serverConn := newPipeClient(t)
	attempts := make(map[uint64]int)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.FetchMessagesCode) {
			return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 2, next: 8}))
		}
		if read.code() == uint32(command.GetOffsetCode) {
			return replyFrame(vsr.OperationNonReplicated, consumerOffsetBody(1))
		}
		attempts[read.requestID()]++
		return uncertainAnswer(attempts[read.requestID()]-1, read)
	})
	ctx, cancel := context.WithTimeout(context.Background(), deferredTestTimeout)
	defer cancel()
	var uncertain ierror.OperationUncertain
	require.ErrorAs(t, calls.storeOffset(ctx, client), &uncertain)

	for _, autoCommit := range []bool{false, true} {
		outcome := receive(t, startFetch(ctx, client, calls, iggcon.NextPollingStrategy(), autoCommit, 3, 2, 1), "the blocked fetch")
		var blocked ierror.PartitionLaneBlocked
		require.ErrorAs(t, outcome.err, &blocked)
		assert.Equal(t, uint32(2), blocked.PartitionID)
		assert.Equal(t, uncertain.OperationID, blocked.OperationID)
		assert.Equal(t, ierror.PartitionLaneBlockedCode, blocked.Code())
		assert.Equal(t, ierror.Code(14009), blocked.Code())
		assert.ErrorIs(t, outcome.err, ierror.ErrPartitionLaneBlocked)
		assert.ErrorIs(t, outcome.err, ierror.ErrLaneBlocked)
		var generic ierror.LaneBlocked
		require.ErrorAs(t, outcome.err, &generic)
		assert.Equal(t, uncertain.OperationID, generic.OperationID)
		assert.Empty(t, client.deferred, "a blocked fetch releases its deferred slot")
	}
	assert.Zero(t, requestCount(server.recorded(), command.FetchMessagesCode), "a blocked fetch is never sent")
	var single ierror.LaneBlocked
	err := calls.getOffset(ctx, client)
	require.ErrorAs(t, err, &single)
	assert.IsType(t, ierror.LaneBlocked{}, err, "single-partition calls keep their error type")
	assert.Equal(t, ierror.Code(14007), single.Code())

	other := calls
	other.partition = 1
	require.NoError(t, other.getOffset(ctx, client), "the fetch releases the lane acquired before the blocked target")
	require.NoError(t, client.AbandonOperation(ctx, uncertain.OperationID))
	require.NoError(t, receive(t, startFetch(ctx, client, calls, iggcon.NextPollingStrategy(), false, 3, 2, 1), "the repaired fetch").err)
	assert.Equal(t, 1, requestCount(server.recorded(), command.FetchMessagesCode))
}

// The outcome of a fetch that commits or starts at Next is unknown once it is
// sent, so it is never sent again. A fetch from offsets that does not commit
// changes nothing, so it is sent again.
func TestFetchMessages_SendsAgainOnlyAFetchThatMovesNoProgress(t *testing.T) {
	calls := newLaneCalls(t)
	for _, tt := range []struct {
		name       string
		strategy   iggcon.PollingStrategy
		autoCommit bool
		fetches    int32
	}{
		{"auto_commit", iggcon.OffsetPollingStrategy(0), true, 1},
		{"next", iggcon.NextPollingStrategy(), false, 1},
		{"offset", iggcon.OffsetPollingStrategy(0), false, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			var fetches atomic.Int32
			servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
				if read.code() != uint32(command.FetchMessagesCode) {
					t.Errorf("unexpected command %d", read.code())
					return nil
				}
				if fetches.Add(1) == 1 {
					return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.TransientNotCommittedCode), nil)
				}
				return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 1, next: 1}))
			})

			outcome := receive(t, startFetch(context.Background(), client, calls, tt.strategy, tt.autoCommit, 1), "the fetch")

			if tt.autoCommit {
				require.ErrorIs(t, outcome.err, ierror.ErrFetchProgressUncertain)
			} else if tt.fetches == 1 {
				require.ErrorIs(t, outcome.err, ierror.ErrTransientNotCommitted)
			} else {
				require.NoError(t, outcome.err)
			}
			assert.Equal(t, tt.fetches, fetches.Load())
		})
	}
}

func TestFetchMessages_UnknownCommitBlocksEveryTargetUntilExplicitRecovery(t *testing.T) {
	calls := newLaneCalls(t)
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		switch command.Code(read.code()) {
		case command.FetchMessagesCode:
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.TransientNotCommittedCode), nil)
		case command.GetOffsetCode:
			return replyFrame(vsr.OperationNonReplicated, consumerOffsetBody(1))
		default:
			return replyFrame(read.operation(), resultSection())
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), deferredTestTimeout)
	defer cancel()
	outcome := receive(t, startFetch(ctx, client, calls, iggcon.NextPollingStrategy(), true, 3, 2, 1), "the unknown fetch")
	var coded interface{ Code() ierror.Code }
	require.ErrorAs(t, outcome.err, &coded)
	require.Equal(t, ierror.Code(14010), coded.Code())
	var recoveryID uint64
	for _, partition := range []uint32{1, 2, 3} {
		calls.partition = partition
		var blocked ierror.PartitionLaneBlocked
		require.ErrorAs(t, calls.storeOffset(ctx, client), &blocked)
		assert.Equal(t, partition, blocked.PartitionID)
		if recoveryID == 0 {
			recoveryID = blocked.OperationID
		}
		assert.Equal(t, recoveryID, blocked.OperationID)
		require.NoError(t, calls.getOffset(ctx, client), "offset inspection stays available")
	}
	assert.Zero(t, requestCount(server.recorded(), command.StoreOffsetCode))
	require.ErrorAs(t, client.ResolveOperation(ctx, recoveryID), &coded)
	assert.Equal(t, ierror.Code(14010), coded.Code())
	assert.Equal(t, 1, requestCount(server.recorded(), command.FetchMessagesCode), "resolution never replays a fetch")
	var blocked ierror.PartitionLaneBlocked
	require.ErrorAs(t, calls.storeOffset(ctx, client), &blocked)
	require.NoError(t, client.AbandonOperation(ctx, recoveryID))
	require.NoError(t, calls.storeOffset(ctx, client))
	assert.ErrorIs(t, client.AbandonOperation(ctx, recoveryID), ierror.ErrOperationNotRetained)
}

func TestFetchMessages_UnknownCommitResolvesZeroIncarnationsBeforeAdmission(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("raw_%t", raw), func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
				if read.code() == uint32(command.FetchMessagesCode) {
					return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.TransientNotCommittedCode), nil)
				}
				return replyFrame(read.operation(), resultSection())
			})
			calls := newLaneCalls(t)
			options := deferredOptions(0, 2*time.Second)
			target := iggcon.FetchMessagesTarget{PartitionId: 1, Strategy: iggcon.NextPollingStrategy()}
			target.Strategy.Context = &iggcon.PartitionContext{OwnerGeneration: 9}
			var err error
			if raw {
				body, encodeErr := (&command.FetchMessages{StreamId: calls.stream, TopicId: calls.topic, Consumer: calls.consumer,
					Targets: []command.FetchTarget{{FetchMessagesTarget: target, MaxBytes: options.MaxBytes}}, Count: 10, AutoCommit: true, Options: options}).MarshalBinary()
				require.NoError(t, encodeErr)
				original := append([]byte(nil), body...)
				_, err = client.SendBinaryRequest(context.Background(), uint32(command.FetchMessagesCode), body)
				assert.Equal(t, original, body, "raw caller bytes remain owned by the caller")
			} else {
				_, err = client.FetchMessages(context.Background(), calls.stream, calls.topic, calls.consumer,
					[]iggcon.FetchMessagesTarget{target}, 10, true, options)
			}
			var unknown ierror.FetchProgressUncertain
			require.ErrorAs(t, err, &unknown)
			fence := fetchTargets(t, recordedFetch(t, server.recorded()))[0].fence
			assert.Equal(t, uint64(17), fence.Incarnation)
			assert.Equal(t, uint64(9), fence.OwnerGeneration, "resolve only the missing incarnation")
			var blocked ierror.PartitionLaneBlocked
			require.ErrorAs(t, calls.storeOffset(context.Background(), client), &blocked)
			assert.Equal(t, unknown.OperationID, blocked.OperationID)
			assert.Zero(t, requestCount(server.recorded(), command.StoreOffsetCode))
		})
	}
}

func TestFetchMessages_UnknownCommitBlocksProgressWithAnExplicitZeroContext(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.FetchMessagesCode) {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.TransientNotCommittedCode), nil)
		}
		if read.code() == uint32(command.PollMessagesCode) || read.code() == uint32(command.PollMessagesDeferredCode) {
			return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(1))
		}
		return replyFrame(read.operation(), resultSection())
	})
	calls := newLaneCalls(t)
	outcome := receive(t, startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), true, 1), "the unknown fetch")
	var unknown ierror.FetchProgressUncertain
	require.ErrorAs(t, outcome.err, &unknown)
	fence := iggcon.PartitionContext{OwnerGeneration: 9}
	strategy := iggcon.NextPollingStrategy().WithContext(fence)
	for _, progress := range []struct {
		name string
		call func() error
	}{
		{"store", func() error {
			return client.StoreConsumerPosition(t.Context(), calls.consumer, calls.stream, calls.topic,
				iggcon.ConsumerPosition{PartitionId: 1, Offset: 7, Context: fence})
		}},
		{"poll", func() error { return calls.poll(t.Context(), client, strategy, false) }},
		{"deferred", func() error {
			_, err := client.PollMessagesDeferred(t.Context(), calls.stream, calls.topic, calls.consumer,
				strategy, 1, false, &calls.partition, deferredOptions(0, 2*time.Second))
			return err
		}},
	} {
		t.Run(progress.name, func(t *testing.T) {
			var blocked ierror.PartitionLaneBlocked
			require.ErrorAs(t, progress.call(), &blocked)
			assert.Equal(t, unknown.OperationID, blocked.OperationID)
		})
	}
	assert.Empty(t, requestsFor(server.recorded(), vsr.OperationForCode(uint32(command.StoreOffsetCode))))
	assert.Zero(t, requestCount(server.recorded(), command.PollMessagesCode))
	assert.Zero(t, requestCount(server.recorded(), command.PollMessagesDeferredCode))
}

func TestFetchMessages_ExplicitZeroContextKeepsItsWireFenceAcrossAStaleRoute(t *testing.T) {
	client, serverConn := newPipeClient(t)
	var applied atomic.Bool
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.FetchMessagesCode) {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.TransientNotCommittedCode), nil)
		}
		if read.operation() == vsr.OperationForCode(uint32(command.StoreOffsetCode)) {
			if read.incarnation() == 18 {
				applied.Store(true)
				return committedReply(read, 0)
			}
			return committedReply(read, ierror.HistoryUnavailableCode)
		}
		return replyFrame(read.operation(), nil)
	})
	calls := newLaneCalls(t)
	strategy := iggcon.NextPollingStrategy().WithContext(iggcon.PartitionContext{Incarnation: 18})
	outcome := receive(t, startFetch(context.Background(), client, calls, strategy, true, 1), "the unknown fetch from new history")
	var unknown ierror.FetchProgressUncertain
	require.ErrorAs(t, outcome.err, &unknown)
	position := iggcon.ConsumerPosition{PartitionId: 1, Offset: 7, Context: iggcon.PartitionContext{OwnerGeneration: 9, MetadataOp: 100}}
	err := client.StoreConsumerPosition(context.Background(), calls.consumer, calls.stream, calls.topic, position)
	require.ErrorIs(t, err, ierror.ErrHistoryUnavailable)
	assert.False(t, applied.Load(), "the server rejects a zero header incarnation before progress admission")
	stores := requestsFor(server.recorded(), vsr.OperationForCode(uint32(command.StoreOffsetCode)))
	require.Len(t, stores, 1)
	assert.Zero(t, stores[0].incarnation(), "local lane normalization preserves the caller's explicit wire fence")
	assert.Equal(t, position.Context.OwnerGeneration, binary.LittleEndian.Uint64(stores[0].header[frameOffsetContext+8:]))
	assert.Equal(t, position.Context.MetadataOp, binary.LittleEndian.Uint64(stores[0].header[frameOffsetContext+16:]))
	position.Context.Incarnation = 18
	var blocked ierror.PartitionLaneBlocked
	require.ErrorAs(t, client.StoreConsumerPosition(context.Background(), calls.consumer, calls.stream, calls.topic, position), &blocked)
	assert.Equal(t, unknown.OperationID, blocked.OperationID)
}

func TestFetchMessages_LiveUnknownCommitBlocksAcrossConnectionsAndSessionChanges(t *testing.T) {
	for _, cancelAfterSend := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_%t", cancelAfterSend), func(t *testing.T) {
			parked := make(chan request, 1)
			release := make(chan struct{})
			finished := make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			var fetches atomic.Int32
			fixture := newPrimaryPollFixture(t, func(primary, _ int, read request) ([]byte, bool) {
				if read.operation() == vsr.OperationSendMessages {
					return replyFrame(read.operation(), zeroConfirmations()), true
				}
				if read.operation() == vsr.OperationForCode(uint32(command.StoreOffsetCode)) ||
					read.operation() == vsr.OperationForCode(uint32(command.DeleteConsumerOffsetCode)) {
					return replyFrame(read.operation(), resultSection()), true
				}
				switch command.Code(read.code()) {
				case command.FetchMessagesCode:
					if fetches.Add(1) == 1 {
						assert.Equal(t, 0, primary)
						parked <- read
						<-release
						close(finished)
						return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 0, next: 1})), true
					}
					return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.TransientNotCommittedCode), nil), true
				case command.GetOffsetCode:
					return replyFrame(vsr.OperationNonReplicated, consumerOffsetBody(1)), true
				}
				return nil, false
			}, func(_ int, read request) ([]byte, bool) {
				if read.code() != uint32(command.GetPartitionRouteCode) {
					return nil, false
				}
				reply := routeReply(t, read, uint8(routedPollPartition(t, read)+1))
				binary.LittleEndian.PutUint64(reply[vsr.HeaderSize:], 17)
				return reply, true
			})
			awaitReplicaLinks(t, fixture)
			client := fixture.client
			calls := newLaneCalls(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fetched := make(chan error, 1)
			go func() {
				_, err := client.FetchMessages(ctx, calls.stream, calls.topic, calls.consumer,
					[]iggcon.FetchMessagesTarget{{PartitionId: 0, Strategy: iggcon.NextPollingStrategy()}, {PartitionId: 1, Strategy: iggcon.NextPollingStrategy()}},
					10, true, deferredOptions(0, 2*time.Second))
				fetched <- err
			}()
			old := receive(t, parked, "the live committing fetch")
			stored := make(chan error, 1)
			go func() { stored <- calls.storeOffset(context.Background(), client) }()
			require.Eventually(t, func() bool { return progressLaneWaiters(client) == 1 }, deferredTestTimeout, time.Millisecond)
			if cancelAfterSend {
				cancel()
			}
			var unknown ierror.FetchProgressUncertain
			require.ErrorAs(t, receive(t, fetched, "the bounded unknown fetch"), &unknown)
			var blocked ierror.PartitionLaneBlocked
			require.ErrorAs(t, receive(t, stored, "the already queued store"), &blocked)
			assert.Equal(t, uint32(1), blocked.PartitionID)
			assert.Equal(t, unknown.OperationID, blocked.OperationID)
			assert.Equal(t, old.requestID(), unknown.OperationID)
			select {
			case <-finished:
				t.Fatal("the old server read must still be live")
			default:
			}
			for _, partition := range []uint32{0, 1} {
				alias := calls
				alias.partition = partition
				alias.stream, _ = iggcon.NewIdentifier("stream")
				alias.topic, _ = iggcon.NewIdentifier("topic")
				require.ErrorAs(t, alias.storeOffset(context.Background(), client), &blocked)
				assert.Equal(t, unknown.OperationID, blocked.OperationID)
				assert.Equal(t, partition, blocked.PartitionID)
			}
			require.NoError(t, calls.getOffset(context.Background(), client))
			require.NoError(t, calls.poll(context.Background(), client, iggcon.OffsetPollingStrategy(0), false))
			require.NoError(t, calls.send(context.Background(), client))
			connections := fixture.primaries[1].connections()
			client.links.replica(2).close(ierror.ErrNotConnected)
			require.NoError(t, calls.getOffset(context.Background(), client))
			assert.Greater(t, fixture.primaries[1].connections(), connections)
			_, err := client.LoginUser(context.Background(), "iggy", "secret")
			require.NoError(t, err)
			require.ErrorAs(t, calls.deleteOffset(context.Background(), client), &blocked)
			assert.Equal(t, unknown.OperationID, blocked.OperationID)
			require.ErrorIs(t, client.ResolveOperation(context.Background(), unknown.OperationID), ierror.ErrFetchProgressUncertain)
			assert.Empty(t, requestsFor(fixture.primaries[1].recorded(), vsr.OperationForCode(uint32(command.StoreOffsetCode))))
			assert.Empty(t, requestsFor(fixture.primaries[1].recorded(), vsr.OperationForCode(uint32(command.DeleteConsumerOffsetCode))))
			assert.Equal(t, int32(1), fetches.Load(), "reconnect, sign-in and resolve never replay")
			require.NoError(t, client.StoreConsumerPosition(context.Background(), calls.consumer, calls.stream, calls.topic,
				iggcon.ConsumerPosition{PartitionId: 1, Offset: 7, Context: iggcon.PartitionContext{Incarnation: 18}}), "a new history is independent")
			require.NoError(t, client.AbandonOperation(context.Background(), unknown.OperationID))
			require.NoError(t, calls.storeOffset(context.Background(), client), "explicit recovery permits progress while the old read is live")
			select {
			case <-finished:
				t.Fatal("abandon cannot complete the old server read")
			default:
			}
			second := receive(t, startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), true, 1), "the new unknown fetch")
			var nextUnknown ierror.FetchProgressUncertain
			require.ErrorAs(t, second.err, &nextUnknown)
			assert.Greater(t, nextUnknown.OperationID, unknown.OperationID, "recovery IDs survive session replacement without reuse")
			var blockedUnknown ierror.FetchProgressUncertain
			require.ErrorAs(t, client.Close(), &blockedUnknown)
			assert.Equal(t, nextUnknown.OperationID, blockedUnknown.OperationID)
			releaseOnce.Do(func() { close(release) })
			receive(t, finished, "the old read released by the test")
		})
	}
}

func TestFetchMessages_PartialUnknownKeepsHealthyRecordsAndRawBytes(t *testing.T) {
	calls := newLaneCalls(t)
	encoded, err := (&command.SendMessages{StreamId: calls.stream, TopicId: calls.topic,
		Partitioning: iggcon.PartitionId(1), Messages: []iggcon.IggyMessage{calls.message}}).MarshalBinary()
	require.NoError(t, err)
	const metadataLengthSize = 4
	records := encoded[metadataLengthSize+int(binary.LittleEndian.Uint32(encoded)):]
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("raw_%t", raw), func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			body := fetchBody(fetchSection{partition: 3, status: ierror.TransientNotCommittedCode},
				fetchSection{partition: 1, next: 1, count: 1, records: records, context: iggcon.PartitionContext{Incarnation: 17}},
				fetchSection{partition: 2, status: ierror.RequestAlreadyAppliedCode},
				fetchSection{partition: 4, status: ierror.TransientNotAcceptedCode})
			server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
				if read.code() == uint32(command.FetchMessagesCode) {
					return replyFrame(vsr.OperationNonReplicated, body)
				}
				return replyFrame(read.operation(), resultSection())
			})
			var sections []iggcon.FetchMessagesSection
			var fetchErr error
			if raw {
				options := deferredOptions(0, 2*time.Second)
				targets := make([]command.FetchTarget, 0, 4)
				for _, partition := range []uint32{1, 2, 3, 4} {
					targets = append(targets, command.FetchTarget{FetchMessagesTarget: iggcon.FetchMessagesTarget{
						PartitionId: partition, Strategy: iggcon.NextPollingStrategy()}, MaxBytes: options.MaxBytes})
				}
				requestBody, encodeErr := (&command.FetchMessages{StreamId: calls.stream, TopicId: calls.topic, Consumer: calls.consumer,
					Targets: targets, Count: 10, AutoCommit: true, Options: options}).MarshalBinary()
				require.NoError(t, encodeErr)
				var reply []byte
				reply, fetchErr = client.SendBinaryRequest(context.Background(), uint32(command.FetchMessagesCode), requestBody)
				require.NoError(t, fetchErr)
				assert.Equal(t, body, reply)
				sections, fetchErr = binaryserialization.DeserializeFetchSections(reply, iggcon.MESSAGE_COMPRESSION_NONE)
			} else {
				outcome := receive(t, startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), true, 1, 2, 3, 4), "the partial fetch")
				sections, fetchErr = outcome.sections, outcome.err
			}
			require.NoError(t, fetchErr)
			require.Len(t, sections, 4)
			healthy, decodeErr := binaryserialization.DeserializeSectionMessages(sections[1])
			require.NoError(t, decodeErr)
			require.Len(t, healthy.Messages, 1)
			assert.Equal(t, calls.message.Payload, healthy.Messages[0].Payload)
			assert.Equal(t, uint64(1), sections[1].NextOffset)
			require.NoError(t, calls.storeOffset(context.Background(), client), "healthy committed data is independently usable")
			var handle uint64
			for _, section := range []iggcon.FetchMessagesSection{sections[0], sections[2], sections[3]} {
				blockedCalls := calls
				blockedCalls.partition = section.PartitionId
				var blocked ierror.PartitionLaneBlocked
				require.ErrorAs(t, blockedCalls.storeOffset(context.Background(), client), &blocked)
				if handle == 0 {
					handle = blocked.OperationID
				}
				assert.Equal(t, handle, blocked.OperationID)
				assert.Equal(t, section.PartitionId, blocked.PartitionID)
				if !raw {
					var unknown ierror.FetchProgressUncertain
					require.ErrorAs(t, section.Err, &unknown)
					assert.Equal(t, handle, unknown.OperationID)
				}
			}
			assert.Len(t, requestsFor(server.recorded(), vsr.OperationForCode(uint32(command.StoreOffsetCode))), 1)
			require.ErrorIs(t, client.Close(), ierror.ErrFetchProgressUncertain)
			var unknown ierror.FetchProgressUncertain
			require.ErrorAs(t, client.ResolveOperation(context.Background(), handle), &unknown)
			assert.Equal(t, handle, unknown.OperationID, "close does not abandon recovery")
		})
	}
}

func TestFetchMessages_RecoveryCapacityRefusesBeforeSendAndAbandonReleasesIt(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.operations = newOperationRegistry(2, DefaultMaxPendingOperationBytes)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.FetchMessagesCode) {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.TransientNotCommittedCode), nil)
		}
		return replyFrame(read.operation(), resultSection())
	})
	calls := newLaneCalls(t)
	var ids []uint64
	for _, partition := range []uint32{1, 2} {
		outcome := receive(t, startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), true, partition), "a retained fetch")
		var unknown ierror.FetchProgressUncertain
		require.ErrorAs(t, outcome.err, &unknown)
		ids = append(ids, unknown.OperationID)
	}
	refused := receive(t, startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), true, 3), "the capacity refusal")
	require.ErrorIs(t, refused.err, ierror.ErrTransientNotAccepted)
	assert.Equal(t, 2, requestCount(server.recorded(), command.FetchMessagesCode))
	noncommitting := receive(t, startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), false, 3), "noncommitting Next at capacity")
	require.ErrorIs(t, noncommitting.err, ierror.ErrTransientNotCommitted)
	calls.partition = 3
	require.NoError(t, calls.storeOffset(context.Background(), client), "noncommitting Next creates no retained block")
	require.NoError(t, client.AbandonOperation(context.Background(), ids[0]))
	after := receive(t, startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), true, 3), "the fetch after capacity release")
	var unknown ierror.FetchProgressUncertain
	require.ErrorAs(t, after.err, &unknown)
	assert.Greater(t, unknown.OperationID, ids[1])
	require.NoError(t, client.AbandonOperation(context.Background(), ids[1]))
	require.NoError(t, client.AbandonOperation(context.Background(), unknown.OperationID))
	client.operations.mu.Lock()
	assert.Empty(t, client.operations.fetchRetained)
	assert.Empty(t, client.operations.fetchBlocked)
	assert.Empty(t, client.operations.fetchReservations)
	assert.Empty(t, client.operations.lanes)
	assert.Zero(t, client.operations.fetchBytes)
	client.operations.maxBytes = 1
	client.operations.mu.Unlock()
	before := requestCount(server.recorded(), command.FetchMessagesCode)
	refused = receive(t, startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), true, 4), "the byte capacity refusal")
	require.ErrorIs(t, refused.err, ierror.ErrTransientNotAccepted)
	assert.Equal(t, before, requestCount(server.recorded(), command.FetchMessagesCode))
	require.NoError(t, client.Close())
}

func TestFetchMessages_CancelBeforeSendReleasesEveryAcquiredLane(t *testing.T) {
	client, serverConn := newPipeClient(t)
	parked := make(chan request, 1)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.PollMessagesDeferredCode) {
			parked <- read
			return nil
		}
		return replyFrame(read.operation(), resultSection())
	})
	calls := newLaneCalls(t)
	polled := make(chan error, 1)
	go func() {
		partition := uint32(2)
		_, err := client.PollMessagesDeferred(context.Background(), calls.stream, calls.topic, calls.consumer,
			iggcon.NextPollingStrategy(), 1, false, &partition, deferredOptions(5*time.Second, 10*time.Second))
		polled <- err
	}()
	poll := receive(t, parked, "the active progress poll")
	ctx, cancel := context.WithCancel(context.Background())
	fetched := startFetch(ctx, client, calls, iggcon.NextPollingStrategy(), true, 2, 1)
	require.Eventually(t, func() bool { return progressLaneWaiters(client) == 1 }, deferredTestTimeout, time.Millisecond)
	cancel()
	require.ErrorIs(t, receive(t, fetched, "the cancelled partially acquired fetch").err, ierror.ErrTransientNotAccepted)
	require.NoError(t, calls.storeOffset(context.Background(), client), "the acquired lower partition was released")
	assert.Zero(t, requestCount(server.recorded(), command.FetchMessagesCode))
	answerParked(t, serverConn, poll, replyFrame(vsr.OperationNonReplicated, emptyBatchBody(2)))
	require.NoError(t, receive(t, polled, "the original progress poll"))
	client.operations.mu.Lock()
	assert.Empty(t, client.operations.fetchReservations)
	assert.Empty(t, client.operations.lanes)
	assert.Zero(t, client.operations.fetchBytes)
	client.operations.mu.Unlock()
}

func TestFetchMessages_WholeAndSectionRefusalsRetainOnlyUncertainProgress(t *testing.T) {
	wholeSafe := []ierror.Code{ierror.FeatureUnavailableCode, ierror.InvalidIdentifierCode, ierror.TransientNotAcceptedCode,
		ierror.HistoryUnavailableCode, ierror.NotPrimaryCode, ierror.DeferredPollCapacityExceededCode,
		ierror.StreamIdNotFoundCode, ierror.StreamNameNotFoundCode, ierror.TopicIdNotFoundCode, ierror.TopicNameNotFoundCode,
		ierror.PartitionNotFoundCode, ierror.InvalidOffsetCode, ierror.ConsumerGroupPartitionNotOwnedCode}
	sectionSafe := []ierror.Code{ierror.InvalidSizeBytesCode, ierror.HistoryUnavailableCode, ierror.NotPrimaryCode, ierror.ConsumerGroupPartitionNotOwnedCode}
	unknown := []ierror.Code{ierror.TransientNotCommittedCode, ierror.RequestAlreadyAppliedCode, ierror.InvalidCommandCode,
		ierror.StaleClientCode, ierror.UnauthenticatedCode, ierror.UnauthorizedCode, ierror.ShardCommunicationErrorCode}
	for _, section := range []bool{false, true} {
		safe := wholeSafe
		if section {
			safe = sectionSafe
		}
		cases := append(append([]ierror.Code(nil), safe...), unknown...)
		if section {
			cases = append(cases, ierror.TransientNotAcceptedCode, ierror.DeferredPollCapacityExceededCode)
		} else {
			cases = append(cases, ierror.InvalidSizeBytesCode)
		}
		for _, status := range cases {
			t.Run(fmt.Sprintf("section_%t/status_%d", section, status), func(t *testing.T) {
				var attempts atomic.Int32
				fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
					if read.operation() == vsr.OperationForCode(uint32(command.StoreOffsetCode)) {
						return replyFrame(read.operation(), resultSection()), true
					}
					if read.code() != uint32(command.FetchMessagesCode) {
						return nil, false
					}
					if attempts.Add(1) > 1 {
						return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 1, next: 1})), true
					}
					if section {
						return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 1, status: status})), true
					}
					return statusReplyFrame(vsr.OperationNonReplicated, uint32(status), nil), true
				}, nil)
				calls := newLaneCalls(t)
				outcome := receive(t, startFetch(context.Background(), fixture.client, calls, iggcon.NextPollingStrategy(), true, 1), "the classified reply")
				isSafe := slices.Contains(safe, status)
				if isSafe {
					if outcome.err != nil {
						require.ErrorIs(t, outcome.err, ierror.FromCode(status))
					}
					require.NoError(t, calls.storeOffset(context.Background(), fixture.client))
					return
				}
				uncertain := outcome.err
				if section {
					require.NoError(t, outcome.err)
					require.Len(t, outcome.sections, 1)
					uncertain = outcome.sections[0].Err
				}
				var recovery ierror.FetchProgressUncertain
				require.ErrorAs(t, uncertain, &recovery)
				var blocked ierror.PartitionLaneBlocked
				require.ErrorAs(t, calls.storeOffset(context.Background(), fixture.client), &blocked)
				assert.Equal(t, recovery.OperationID, blocked.OperationID)
				assert.Equal(t, int32(1), attempts.Load(), "a correlated unknown reply cannot be graded into a retry")
			})
		}
	}
}

func TestFetchMessages_MalformedTablesBlockAllTargetsButRecordDecodeDoesNot(t *testing.T) {
	for _, test := range []struct {
		name      string
		body      []byte
		uncertain bool
	}{
		{"truncated", []byte{1}, true},
		{"duplicate", fetchBody(fetchSection{partition: 1}, fetchSection{partition: 1}), true},
		{"unrequested", fetchBody(fetchSection{partition: 3}), true},
		{"trailing", append(fetchBody(fetchSection{partition: 1}), 0), true},
		{"records", fetchBody(fetchSection{partition: 1, count: 1, next: 6, records: []byte{1}, context: iggcon.PartitionContext{Incarnation: 17}}), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
				if read.code() == uint32(command.FetchMessagesCode) {
					return replyFrame(vsr.OperationNonReplicated, test.body)
				}
				return replyFrame(read.operation(), resultSection())
			})
			calls := newLaneCalls(t)
			outcome := receive(t, startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), true, 1, 2), "the malformed reply")
			if test.uncertain {
				var unknown ierror.FetchProgressUncertain
				require.ErrorAs(t, outcome.err, &unknown)
				for _, partition := range []uint32{1, 2} {
					calls.partition = partition
					var blocked ierror.PartitionLaneBlocked
					require.ErrorAs(t, calls.storeOffset(context.Background(), client), &blocked)
					assert.Equal(t, unknown.OperationID, blocked.OperationID)
				}
			} else {
				require.NoError(t, outcome.err)
				require.Len(t, outcome.sections, 1)
				section := outcome.sections[0]
				_, err := binaryserialization.DeserializeSectionMessages(section)
				require.Error(t, err)
				assert.Equal(t, uint64(6), section.NextOffset)
				assert.Equal(t, uint64(17), section.Context.Incarnation)
				require.NoError(t, client.StoreConsumerPosition(context.Background(), calls.consumer, calls.stream, calls.topic,
					iggcon.ConsumerPosition{PartitionId: 1, Offset: 4, Context: section.Context}), "decode failure leaves checkpoint repair usable")
			}
			assert.Equal(t, 1, requestCount(server.recorded(), command.FetchMessagesCode))
		})
	}
}

func TestFetchMessages_ClassifiesCorrelatedReplyBeforeSessionReset(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("explicit_%t/partial_%t", explicit, partial), func(t *testing.T) {
				client, serverConn := newPipeClient(t)
				parked := make(chan request, 1)
				server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
					parked <- read
					return nil
				})
				calls := newLaneCalls(t)
				fetched := startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), true, 1, 2)
				fetch := receive(t, parked, "the committing fetch")
				sections := []fetchSection{{partition: 1, next: 7}}
				if partial {
					sections = append(sections, fetchSection{partition: 2, status: ierror.TransientNotCommittedCode})
				}
				func() {
					client.mtx.Lock()
					defer client.mtx.Unlock()
					answerParked(t, serverConn, fetch, replyFrame(vsr.OperationNonReplicated, fetchBody(sections...)))
					link := client.links.replica(0)
					require.Eventually(t, func() bool {
						link.mu.Lock()
						defer link.mu.Unlock()
						return link.pending[fetch.requestID()] == nil
					}, deferredTestTimeout, time.Millisecond, "the complete correlated reply is queued before session replacement")
					if explicit {
						client.resetSessionLocked(false)
					} else {
						// Automatic recovery replaces the session but keeps the caller epoch.
						client.session.Reset()
						client.invalidateSessionLocked()
					}
				}()
				outcome := receive(t, fetched, "the classified old-session reply")
				if explicit {
					require.ErrorIs(t, outcome.err, ierror.ErrUnauthenticated)
				} else {
					require.NoError(t, outcome.err)
					require.Len(t, outcome.sections, len(sections))
					assert.Equal(t, uint64(7), outcome.sections[0].NextOffset)
					assert.NoError(t, outcome.sections[0].Err)
				}
				client.operations.mu.Lock()
				_, healthyBlocked := client.operations.fetchBlocked[laneKey{partition: 1, incarnation: 17, progress: true}]
				unknown := client.operations.fetchBlocked[laneKey{partition: 2, incarnation: 17, progress: true}]
				client.operations.mu.Unlock()
				assert.False(t, healthyBlocked, "known progress must not turn into uncertainty")
				if partial {
					require.NotNil(t, unknown)
					require.ErrorIs(t, client.ResolveOperation(context.Background(), unknown.id), ierror.ErrFetchProgressUncertain)
				} else {
					assert.Nil(t, unknown, "an omitted sparse target stays unblocked")
					require.NoError(t, client.Close())
				}
				assert.Equal(t, 1, requestCount(server.recorded(), command.FetchMessagesCode))
			})
		}
	}
}

// A fetch refused with NotPrimary, as a whole or for some partitions, marks
// the routes of those partitions as moved, so that the next ReadinessGroups
// looks them up again.
func TestFetchMessages_NotPrimaryMarksTheRoutesOfThosePartitionsMoved(t *testing.T) {
	calls := newLaneCalls(t)
	for _, tt := range []struct {
		name    string
		reply   []byte
		err     error
		lookups int
	}{
		{"the_fetch", statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.NotPrimaryCode), nil), ierror.ErrNotPrimary, 2},
		{"a_section", replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 1, next: 1},
			fetchSection{partition: 2, status: ierror.NotPrimaryCode})), nil, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
				if read.code() != uint32(command.FetchMessagesCode) {
					t.Errorf("unexpected command %d", read.code())
					return nil
				}
				return append([]byte(nil), tt.reply...)
			})
			outcome := receive(t, startFetch(context.Background(), client, calls, iggcon.NextPollingStrategy(), false, 1, 2), "the fetch")
			if tt.err != nil {
				require.ErrorIs(t, outcome.err, tt.err)
			} else {
				require.NoError(t, outcome.err)
			}
			lookups := requestCount(server.recorded(), command.GetPartitionRouteCode)

			_, err := client.ReadinessGroups(context.Background(), calls.stream, calls.topic, []uint32{1, 2})

			require.NoError(t, err)
			assert.Equal(t, tt.lookups, requestCount(server.recorded(), command.GetPartitionRouteCode)-lookups)
			assert.Equal(t, 1, requestCount(server.recorded(), command.FetchMessagesCode), "the fetch is not sent again")
		})
	}
}

// A section that lost the history, the group owner or the namespace of its
// partition leaves the route of that partition behind, and a refusal of the
// whole fetch names no partition, so it leaves every route behind. The next
// fetch fences those partitions to the incarnations of their new routes. The
// call returns the sections that the server sent, in reply order.
func TestFetchMessages_FencesAgainThePartitionsThatLostTheirRoutes(t *testing.T) {
	calls := newLaneCalls(t)
	refusedSection := func(status ierror.Code) []byte {
		return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 3, next: 1},
			fetchSection{partition: 2, status: status}))
	}
	for _, tt := range []struct {
		name         string
		reply        []byte
		fetchErr     error
		sectionErr   error
		incarnations []uint64
	}{
		{"a_section_that_lost_its_history", refusedSection(ierror.HistoryUnavailableCode), nil, ierror.ErrHistoryUnavailable, []uint64{17, 18, 17}},
		{"a_section_that_lost_its_owner", refusedSection(ierror.ConsumerGroupPartitionNotOwnedCode), nil, ierror.ErrConsumerGroupPartitionNotOwned, []uint64{17, 18, 17}},
		{"a_section_that_lost_its_partition", refusedSection(ierror.PartitionNotFoundCode), nil, ierror.ErrPartitionNotFound, []uint64{17, 18, 17}},
		{"the_fetch_that_lost_a_history", statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.HistoryUnavailableCode), nil), ierror.ErrHistoryUnavailable, nil, []uint64{18, 18, 18}},
		{"the_fetch_that_lost_an_owner", statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ConsumerGroupPartitionNotOwnedCode), nil), ierror.ErrConsumerGroupPartitionNotOwned, nil, []uint64{18, 18, 18}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			var refused atomic.Bool
			server := serve(serverConn, func(_ int, read request) []byte {
				switch read.code() {
				case uint32(command.GetPartitionRouteCode):
					body := make([]byte, iggcon.PartitionRouteSize)
					binary.LittleEndian.PutUint64(body, 17)
					if refused.Load() {
						binary.LittleEndian.PutUint64(body, 18)
					}
					binary.LittleEndian.PutUint32(body[20:], 4)
					return replyFrame(vsr.OperationNonReplicated, body)
				case uint32(command.FetchMessagesCode):
					if refused.Swap(true) {
						return replyFrame(vsr.OperationNonReplicated, fetchBody())
					}
					return append([]byte(nil), tt.reply...)
				}
				t.Errorf("unexpected command %d", read.code())
				return nil
			})
			fetch := func() fetchOutcome {
				return receive(t, startFetch(context.Background(), client, calls, iggcon.OffsetPollingStrategy(0), false, 1, 2, 3), "the fetch")
			}

			first := fetch()
			if tt.fetchErr != nil {
				require.ErrorIs(t, first.err, tt.fetchErr)
			} else {
				require.NoError(t, first.err)
				require.Len(t, first.sections, 2)
				assert.Equal(t, []uint32{3, 2}, []uint32{first.sections[0].PartitionId, first.sections[1].PartitionId})
				assert.NoError(t, first.sections[0].Err)
				assert.ErrorIs(t, first.sections[1].Err, tt.sectionErr)
			}

			require.NoError(t, fetch().err)

			var fetches []request
			for _, read := range server.recorded() {
				if read.code() == uint32(command.FetchMessagesCode) {
					fetches = append(fetches, read)
				}
			}
			require.Len(t, fetches, 2)
			var incarnations []uint64
			for _, target := range fetchTargets(t, fetches[1]) {
				incarnations = append(incarnations, target.fence.Incarnation)
			}
			assert.Equal(t, tt.incarnations, incarnations)
		})
	}
}

// A cancel after the read of a fetch with auto-commit drops what it stored,
// so the client tells the server to drop only a fetch without auto-commit. A
// fetch with auto-commit returns its reply also after its context ended.
func TestFetchMessages_CancelSendsTheCancellationOnlyWithoutAutoCommit(t *testing.T) {
	calls := newLaneCalls(t)
	for _, tt := range []struct {
		name       string
		autoCommit bool
	}{
		{"without_auto_commit", false},
		{"auto_commit", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			parked := make(chan request, 1)
			cancellations := make(chan request, 1)
			servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
				switch read.code() {
				case uint32(command.FetchMessagesCode):
					parked <- read
				case uint32(command.CancelDeferredRequestCode):
					cancellations <- read
				default:
					t.Errorf("unexpected command %d", read.code())
				}
				return nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			fetched := startFetch(ctx, client, calls, iggcon.OffsetPollingStrategy(0), tt.autoCommit, 1)
			fetch := receive(t, parked, "the parked fetch")

			cancel()

			if !tt.autoCommit {
				require.ErrorIs(t, receive(t, fetched, "the cancelled fetch").err, ierror.ErrTransientNotCommitted)
				requireCancellationOf(t, fetch, receive(t, cancellations, "the cancellation"))
				return
			}
			select {
			case <-fetched:
				t.Fatal("the fetch with auto-commit ended before its reply")
			case <-time.After(200 * time.Millisecond):
			}
			answerParked(t, serverConn, fetch, replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 1, next: 5})))
			outcome := receive(t, fetched, "the fetch with auto-commit")
			require.NoError(t, outcome.err)
			require.Len(t, outcome.sections, 1)
			assert.Equal(t, uint64(5), outcome.sections[0].NextOffset)
			assert.Empty(t, cancellations)
		})
	}
}

// A fetch without a reply ends at its request timeout, not at the operation
// budget, also with auto-commit. Only a fetch without auto-commit is then
// cancelled on the server.
func TestFetchMessages_EndsAtItsRequestTimeoutAndCancelsOnlyAFetchWithoutAutoCommit(t *testing.T) {
	calls := newLaneCalls(t)
	for _, tt := range []struct {
		name       string
		autoCommit bool
	}{
		{"without_auto_commit", false},
		{"auto_commit", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			parked := make(chan request, 1)
			cancellations := make(chan request, 1)
			servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
				switch read.code() {
				case uint32(command.FetchMessagesCode):
					parked <- read
				case uint32(command.CancelDeferredRequestCode):
					cancellations <- read
				default:
					t.Errorf("unexpected command %d", read.code())
				}
				return nil
			})
			// The lookup before the fetch takes some of the timeout, and the
			// fetch needs the reply margin of what is left.
			options := deferredOptions(0, iggcon.MinDeferredPollReplyMargin+500*time.Millisecond)
			started := time.Now()

			_, err := client.FetchMessages(context.Background(), calls.stream, calls.topic, calls.consumer,
				[]iggcon.FetchMessagesTarget{{PartitionId: 1, Strategy: iggcon.OffsetPollingStrategy(0)}}, 10, tt.autoCommit, options)

			if tt.autoCommit {
				require.ErrorIs(t, err, ierror.ErrFetchProgressUncertain)
			} else {
				require.ErrorIs(t, err, ierror.ErrTransientNotCommitted)
			}
			elapsed := time.Since(started)
			assert.GreaterOrEqual(t, elapsed, options.RequestTimeout)
			assert.Less(t, elapsed, responseReadTimeout/10, "the request timeout, not the operation budget, bounds the call")
			fetch := receive(t, parked, "the fetch")
			if !tt.autoCommit {
				requireCancellationOf(t, fetch, receive(t, cancellations, "the cancellation"))
				return
			}
			select {
			case <-cancellations:
				t.Fatal("the fetch with auto-commit was cancelled")
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
}

// Close waits for the reply of a fetch with auto-commit, because a cancel
// after its read drops what it stored.
func TestClose_WaitsForTheReplyOfAFetchWithAutoCommit(t *testing.T) {
	calls := newLaneCalls(t)
	client, serverConn := newPipeClient(t)
	parked := make(chan request, 1)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.FetchMessagesCode) {
			parked <- read
		}
		return nil
	})
	fetched := startFetch(context.Background(), client, calls, iggcon.OffsetPollingStrategy(0), true, 1)
	fetch := receive(t, parked, "the parked fetch")
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned %v before the reply of the fetch", err)
	case <-time.After(200 * time.Millisecond):
	}

	answerParked(t, serverConn, fetch, replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 1, next: 5})))

	require.NoError(t, receive(t, fetched, "the fetch").err)
	require.NoError(t, receive(t, closed, "Close"))
	assert.Zero(t, requestCount(server.recorded(), command.CancelDeferredRequestCode))
}

// A raw fetch takes the path of FetchMessages with the caps and the fences of
// its targets as they are, and a fetch that the server refuses is not sent.
func TestSendBinaryRequest_SendsAFetchAsItIsAndRefusesWhatTheServerRefuses(t *testing.T) {
	client, serverConn := newPipeClient(t)
	reply := fetchBody(fetchSection{partition: 1, next: 4})
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.code() != uint32(command.FetchMessagesCode) {
			t.Errorf("unexpected command %d", read.code())
			return nil
		}
		return replyFrame(vsr.OperationNonReplicated, reply)
	})
	calls := newLaneCalls(t)
	fetchOf := func(options iggcon.DeferredPollOptions, targets ...command.FetchTarget) []byte {
		body, err := (&command.FetchMessages{StreamId: calls.stream, TopicId: calls.topic, Consumer: calls.consumer,
			Targets: targets, Count: 10, Options: options}).MarshalBinary()
		require.NoError(t, err)
		return body
	}
	target := func(partition uint32, strategy iggcon.PollingStrategy, maxBytes uint32) command.FetchTarget {
		return command.FetchTarget{FetchMessagesTarget: iggcon.FetchMessagesTarget{PartitionId: partition, Strategy: strategy}, MaxBytes: maxBytes}
	}
	options := deferredOptions(0, 2*time.Second)
	offset := iggcon.OffsetPollingStrategy(3).WithContext(iggcon.PartitionContext{Incarnation: 5, OwnerGeneration: 9})
	fetch := fetchOf(options, target(1, offset, iggcon.MinDeferredPollMaxBytes))

	answer, err := client.SendBinaryRequest(context.Background(), uint32(command.FetchMessagesCode), fetch)

	require.NoError(t, err)
	assert.Equal(t, reply, answer)
	assert.Equal(t, fetch[fetchTargetsAt:], recordedFetch(t, server.recorded()).payload[fetchTargetsAt:],
		"the targets keep their caps and fences")
	tooMany := make([]command.FetchTarget, iggcon.MaxPartitionsPerRequest+1)
	for index := range tooMany {
		tooMany[index] = target(uint32(index), offset, options.MaxBytes)
	}
	for _, tt := range []struct {
		name string
		body []byte
		err  error
	}{
		{"no targets", fetchOf(options), ierror.ErrInvalidCommand},
		{"more targets than one request takes", fetchOf(options, tooMany...), ierror.ErrInvalidCommand},
		{"a partition named twice", fetchOf(options, target(1, offset, options.MaxBytes), target(1, offset, options.MaxBytes)), ierror.ErrInvalidCommand},
		{"a start point other than Next or Offset", fetchOf(options, target(1, iggcon.FirstPollingStrategy(), options.MaxBytes)), ierror.ErrInvalidCommand},
		{"a timeout without the reply margin at zero wait", fetchOf(deferredOptions(0, 500*time.Millisecond), target(1, offset, options.MaxBytes)), ierror.ErrInvalidCommand},
		{"exactly the reply margin at zero wait", fetchOf(deferredOptions(0, time.Second), target(1, offset, options.MaxBytes)), ierror.ErrInvalidCommand},
		{"a fractional microsecond above the reply margin", fetchOf(deferredOptions(0, time.Second+999*time.Nanosecond), target(1, offset, options.MaxBytes)), ierror.ErrInvalidCommand},
		{"a target cap below the smallest", fetchOf(options, target(1, offset, iggcon.MinDeferredPollMaxBytes-1)), ierror.ErrInvalidSizeBytes},
		{"a target cap over the cap of the fetch", fetchOf(options, target(1, offset, options.MaxBytes+1)), ierror.ErrInvalidSizeBytes},
		{"a cut target", fetch[:len(fetch)-1], ierror.ErrInvalidCommand},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.SendBinaryRequest(context.Background(), uint32(command.FetchMessagesCode), tt.body)
			require.ErrorIs(t, err, tt.err)
		})
	}
	_, err = client.SendBinaryRequestToReplica(context.Background(), uint32(command.FetchMessagesCode), fetch, 0)
	require.ErrorIs(t, err, ierror.ErrInvalidCommand)
	assert.Equal(t, 1, requestCount(server.recorded(), command.FetchMessagesCode))
}

func TestFetchMessages_RefusesWhatTheServerRefusesWithoutSending(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		t.Errorf("unexpected command %d", read.code())
		return nil
	})
	calls := newLaneCalls(t)
	next := iggcon.NextPollingStrategy()
	one := []iggcon.FetchMessagesTarget{{PartitionId: 1, Strategy: next}}
	tooMany := make([]iggcon.FetchMessagesTarget, iggcon.MaxPartitionsPerRequest+1)
	for index := range tooMany {
		tooMany[index] = iggcon.FetchMessagesTarget{PartitionId: uint32(index), Strategy: next}
	}
	options := deferredOptions(0, 2*time.Second)
	tooSmall := options
	tooSmall.MaxBytes = iggcon.MinDeferredPollMaxBytes - 1
	for _, tt := range []struct {
		name    string
		targets []iggcon.FetchMessagesTarget
		count   uint32
		options iggcon.DeferredPollOptions
		err     error
	}{
		{"no targets", nil, 10, options, ierror.ErrInvalidCommand},
		{"more targets than one request takes", tooMany, 10, options, ierror.ErrInvalidCommand},
		{"a partition named twice", []iggcon.FetchMessagesTarget{{PartitionId: 1, Strategy: next}, {PartitionId: 1, Strategy: next}}, 10, options, ierror.ErrInvalidCommand},
		{"a start point other than Next or Offset", []iggcon.FetchMessagesTarget{{PartitionId: 1, Strategy: iggcon.LastPollingStrategy()}}, 10, options, ierror.ErrInvalidCommand},
		{"a timeout without the reply margin when it does not wait", one, 10, deferredOptions(0, 500*time.Millisecond), ierror.ErrInvalidCommand},
		{"exactly the reply margin when it does not wait", one, 10, deferredOptions(0, time.Second), ierror.ErrInvalidCommand},
		{"a fractional microsecond above the reply margin", one, 10, deferredOptions(0, time.Second+999*time.Nanosecond), ierror.ErrInvalidCommand},
		{"a count below the minimum count", one, 0, options, ierror.ErrInvalidMessagesCount},
		{"a byte budget below the smallest", one, 10, tooSmall, ierror.ErrInvalidSizeBytes},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.FetchMessages(context.Background(), calls.stream, calls.topic, calls.consumer, tt.targets, tt.count, true, tt.options)
			require.ErrorIs(t, err, tt.err)
		})
	}
	assert.Empty(t, server.recorded(), "a refused fetch sends nothing, not even a lookup")
}

// The server ends the reads of a fetch within the reply margin before its
// timeout, so a fetch cut to the deadline of its call keeps the margin, and a
// fetch whose call ends within the margin is not sent.
func TestFetchMessages_KeepsTheReplyMarginWithinTheDeadlineOfItsCall(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 1, next: 1}))
	})
	calls := newLaneCalls(t)
	fetchWithin := func(deadline time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), deadline)
		defer cancel()
		_, err := client.FetchMessages(ctx, calls.stream, calls.topic, calls.consumer,
			[]iggcon.FetchMessagesTarget{{PartitionId: 1, Strategy: iggcon.OffsetPollingStrategy(0)}}, 10, false,
			deferredOptions(5*time.Second, 10*time.Second))
		return err
	}

	require.NoError(t, fetchWithin(3*time.Second))
	require.ErrorIs(t, fetchWithin(iggcon.MinDeferredPollReplyMargin/2), ierror.ErrTransientNotAccepted)

	wait, timeout := deferredLimits(t, recordedFetch(t, server.recorded()))
	assert.LessOrEqual(t, timeout, 3*time.Second)
	assert.Equal(t, timeout-iggcon.MinDeferredPollReplyMargin, wait)
}

func TestFetchMessages_SpendsSetupTimeWithoutRevalidatingTheInitialTimeout(t *testing.T) {
	for _, raw := range []bool{false, true} {
		for _, elapsed := range []time.Duration{500 * time.Millisecond, time.Second, time.Second + time.Microsecond} {
			t.Run(fmt.Sprintf("raw_%t/setup_%s", raw, elapsed), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					client, serverConn := newPipeClient(t)
					server := servePartitionOperations(t, serverConn, func(_ int, _ request) []byte {
						return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 1, next: 1}))
					})
					calls := newLaneCalls(t)
					options := deferredOptions(0, 2*time.Second)
					target := iggcon.FetchMessagesTarget{PartitionId: 1, Strategy: iggcon.OffsetPollingStrategy(0)}
					body, err := (&command.FetchMessages{StreamId: calls.stream, TopicId: calls.topic, Consumer: calls.consumer,
						Targets: []command.FetchTarget{{FetchMessagesTarget: target, MaxBytes: options.MaxBytes}}, Count: 10, Options: options}).MarshalBinary()
					require.NoError(t, err)
					for range cap(client.deferred) {
						client.deferred <- struct{}{}
					}
					fetched := make(chan error, 1)
					go func() {
						if raw {
							_, err := client.SendBinaryRequest(context.Background(), uint32(command.FetchMessagesCode), body)
							fetched <- err
							return
						}
						_, err := client.FetchMessages(context.Background(), calls.stream, calls.topic, calls.consumer,
							[]iggcon.FetchMessagesTarget{target}, 10, false, options)
						fetched <- err
					}()
					synctest.Wait()
					time.Sleep(elapsed)
					for range cap(client.deferred) {
						<-client.deferred
					}

					err = receive(t, fetched, "the fetch after deferred admission")

					if elapsed > time.Second {
						require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
						assert.Zero(t, requestCount(server.recorded(), command.FetchMessagesCode))
						return
					}
					require.NoError(t, err)
					wait, timeout := deferredLimits(t, recordedFetch(t, server.recorded()))
					assert.Zero(t, wait)
					assert.Equal(t, options.RequestTimeout-elapsed, timeout)
				})
			})
		}
	}
}

// Limits that are not whole microseconds keep the reply margin that the wire
// carries, so the client neither refuses the fetch nor sends one that the
// server refuses.
func TestFetchMessages_KeepsTheReplyMarginOfFractionalLimits(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		return replyFrame(vsr.OperationNonReplicated, fetchBody(fetchSection{partition: 1, next: 1}))
	})
	calls := newLaneCalls(t)

	_, err := client.FetchMessages(context.Background(), calls.stream, calls.topic, calls.consumer,
		[]iggcon.FetchMessagesTarget{{PartitionId: 1, Strategy: iggcon.OffsetPollingStrategy(0)}}, 10, false,
		deferredOptions(time.Second+999*time.Nanosecond, 2*time.Second))

	require.NoError(t, err)
	wait, timeout := deferredLimits(t, recordedFetch(t, server.recorded()))
	assert.Positive(t, wait)
	assert.GreaterOrEqual(t, timeout-wait, iggcon.MinDeferredPollReplyMargin)
}

// ReadinessGroups groups the partitions by primary in primary order, keeps
// their order within a group, and splits a group that one request cannot
// take.
func TestReadinessGroups_GroupsThePartitionsByPrimaryInRequestSizedGroups(t *testing.T) {
	t.Run("by_primary", func(t *testing.T) {
		fixture := newPrimaryPollFixture(t, nil, func(_ int, read request) ([]byte, bool) {
			if read.code() != uint32(command.GetPartitionRouteCode) {
				return nil, false
			}
			return routeReply(t, read, uint8(routedPollPartition(t, read)%2+1)), true
		})
		streamId, topicId, _ := groupConsumer(t)

		groups, err := fixture.client.ReadinessGroups(context.Background(), streamId, topicId, []uint32{5, 2, 3, 4})

		require.NoError(t, err)
		assert.Equal(t, [][]uint32{{2, 4}, {5, 3}}, groups)
	})
	t.Run("request_sized", func(t *testing.T) {
		client, serverConn := newPipeClient(t)
		servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
			t.Errorf("unexpected command %d", read.code())
			return nil
		})
		calls := newLaneCalls(t)
		partitions := make([]uint32, iggcon.MaxPartitionsPerRequest+1)
		for index := range partitions {
			partitions[index] = uint32(index)
		}

		groups, err := client.ReadinessGroups(context.Background(), calls.stream, calls.topic, partitions)

		require.NoError(t, err)
		assert.Equal(t, [][]uint32{partitions[:iggcon.MaxPartitionsPerRequest], partitions[iggcon.MaxPartitionsPerRequest:]}, groups)
	})
}
