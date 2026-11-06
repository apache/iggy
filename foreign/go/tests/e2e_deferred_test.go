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

package tests_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client/reader"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// longWait is far longer than any reply the cases expect, so a call that
	// waits for it fails the prompt bound.
	longWait = 10 * time.Second
	// promptReply bounds a call that a message ends long before longWait.
	promptReply = 2 * time.Second
	// serverParkedLimit is how many deferred requests the server parks for one
	// session on one shard.
	serverParkedLimit = 64
)

// countOptions read up to count messages at once, and store the offset of the last one with onRead.
func countOptions(count uint32, onRead bool) iggcon.PollOptions {
	options := iggcon.DefaultPollOptions()
	options.Count = count
	if onRead {
		options.Commit = iggcon.OnReadCommit()
	}
	return options
}

func longWaitOptions(count uint32) iggcon.PollOptions {
	options := iggcon.WaitPollOptions(longWait)
	options.Count = count
	return options
}

// A member that owns three partitions reads the one that holds messages
// without waiting on the idle ones: the reader reads each readiness group with
// one request, wherever the primaries of the partitions are.
func TestE2E_ReaderReadsThePartitionThatHoldsMessages(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 3)
	group := newGroup(t, connected, streamId, topicId)
	partitionReader, err := reader.NewPartitionReader(ctx, connected, streamId, topicId, iggcon.NewGroupConsumer(group),
		reader.WithPollOptions(longWaitOptions(10)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = partitionReader.Close(context.Background()) })
	require.Eventually(t, func() bool { return len(partitionReader.Partitions()) == 3 }, promptReply, 10*time.Millisecond)

	for round := range 3 {
		busy := uint32(2 - round)
		_, err := connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(busy), testMessages(t, 1))
		require.NoError(t, err)
		started := time.Now()
		readCtx, cancel := context.WithTimeout(ctx, longWait)
		polled, err := partitionReader.Next(readCtx)
		cancel()

		require.NoError(t, err)
		assert.Less(t, time.Since(started), promptReply, "round %d waited on an idle partition", round)
		assert.Equal(t, busy, polled.PartitionId, "round %d", round)
		require.Len(t, polled.Messages, 1, "round %d", round)
	}
}

// A partition with a backlog holds messages at every read, and a late message
// of another partition is still read: the server allots each read target its
// share, and the reader reads the backlog one message at a time.
func TestE2E_ReaderFindsAPartitionBehindABacklog(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 3)
	const backlogSize = 50
	_, err := connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(0), testMessages(t, backlogSize))
	require.NoError(t, err)
	partitionReader, err := reader.NewPartitionReader(ctx, connected, streamId, topicId, singleConsumer(t),
		reader.WithPartitions(0, 2), reader.WithStart(iggcon.FirstPollingStrategy()),
		reader.WithPollOptions(longWaitOptions(1)), reader.WithPrefetchMessages(1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = partitionReader.Close(context.Background()) })
	first, err := partitionReader.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), first.PartitionId)
	_, err = connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(2), testMessages(t, 1))
	require.NoError(t, err)

	late := false
	for range backlogSize {
		readCtx, cancel := context.WithTimeout(ctx, longWait)
		polled, err := partitionReader.Next(readCtx)
		cancel()
		require.NoError(t, err)
		if polled.PartitionId == 2 {
			late = true
			break
		}
	}

	assert.True(t, late, "the late message waits behind the backlog")
}

// A group consumer that commits after processing resumes after the last
// message its handler processed, and a message whose handler failed is read again.
func TestE2E_ConsumerResumesAfterWhatItsHandlerProcessed(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 3)
	for partition := range uint32(3) {
		_, err := connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(partition), testMessages(t, 2))
		require.NoError(t, err)
	}
	group := iggcon.NewGroupConsumer(newGroup(t, connected, streamId, topicId))
	auto := iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryMessage}, After: iggcon.CommitAfterProcessing}
	options := iggcon.WaitPollOptions(time.Second)
	options.Commit = iggcon.AutoCommitPolicy(auto)
	first, err := reader.NewIggyConsumer(ctx, connected, streamId, topicId, group, reader.WithPollOptions(options))
	require.NoError(t, err)
	failure := errors.New("stop after four")
	var handled []uint64
	err = first.Consume(ctx, func(_ context.Context, message reader.ReceivedMessage) error {
		if len(handled) == 3 {
			return failure
		}
		handled = append(handled, uint64(message.PartitionID)<<32|message.Checkpoint.Offset())
		return nil
	})
	require.ErrorIs(t, err, failure)
	require.NoError(t, first.Shutdown(ctx))

	second, err := reader.NewIggyConsumer(ctx, connected, streamId, topicId, group, reader.WithPollOptions(options))
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Shutdown(context.Background()) })
	rest := make(map[uint64]bool)
	for len(rest) < 3 {
		readCtx, cancel := context.WithTimeout(ctx, longWait)
		message, err := second.Next(readCtx)
		cancel()
		require.NoError(t, err)
		rest[uint64(message.PartitionID)<<32|message.Checkpoint.Offset()] = true
	}
	for _, key := range handled {
		assert.False(t, rest[key], "a processed message is not read again")
	}
}

// A poll that stores its offset on read stores the offset of its last message.
func TestE2E_PollWithOnReadStoresItsLastOffset(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 1)
	_, err := connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(0), testMessages(t, 3))
	require.NoError(t, err)
	consumer := singleConsumer(t)
	options := iggcon.DefaultPollOptions()
	options.Count = 2
	options.Commit = iggcon.OnReadCommit()

	polled, err := connected.PollMessages(ctx, streamId, topicId, 0, consumer, iggcon.NextPollingStrategy(), options)

	require.NoError(t, err)
	require.Len(t, polled.Messages, 2)
	partition := uint32(0)
	offset, err := connected.GetConsumerOffset(ctx, consumer, streamId, topicId, &partition)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), offset.StoredOffset)
	continuation, ok := polled.Continuation()
	require.True(t, ok)
	_, err = connected.PollMessages(ctx, streamId, topicId, 1, consumer, continuation, iggcon.DefaultPollOptions())
	require.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestE2E_DeferredPollReturnsWhenAMessageArrives(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 1)
	consumer := singleConsumer(t)
	partition := uint32(0)
	const sendDelay = 300 * time.Millisecond
	go func() {
		time.Sleep(sendDelay)
		_, err := connected.SendMessages(context.Background(), streamId, topicId, iggcon.PartitionId(partition), testMessages(t, 1))
		assert.NoError(t, err)
	}()
	started := time.Now()

	// The parked Next poll holds the progress lane of the partition, and the
	// send of the same client does not wait for that lane.
	polled, err := connected.PollMessages(ctx, streamId, topicId, partition, consumer,
		iggcon.NextPollingStrategy(), longWaitOptions(10))

	require.NoError(t, err)
	elapsed := time.Since(started)
	assert.GreaterOrEqual(t, elapsed, sendDelay, "the poll waits for the message")
	assert.Less(t, elapsed, promptReply, "the send of the same client wakes its parked poll")
	require.Len(t, polled.Messages, 1)

	quiet := iggcon.WaitPollOptions(sendDelay)
	quiet.Count = 10
	started = time.Now()
	polled, err = connected.PollMessages(ctx, streamId, topicId, partition, consumer,
		iggcon.OffsetPollingStrategy(1), quiet)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(started), quiet.MaxWait, "an idle poll waits for MaxWait")
	assert.Empty(t, polled.Messages)
}

// The server parks a bounded number of deferred requests for a session. A
// cancellation that did not reach it would leave those slots taken until
// each wait passed, and the last poll would be refused until its deadline.
func TestE2E_CancelledDeferredPollsFreeTheirServerSlots(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 1)
	consumer := singleConsumer(t)
	partition := uint32(0)
	parkedPoll := iggcon.WaitPollOptions(30 * time.Second)
	parkedPoll.Count = 1
	const parkFor = 50 * time.Millisecond
	// The route lookup happens before the loop, so each poll reaches the
	// server well before it is cancelled.
	_, err := connected.PollMessages(ctx, streamId, topicId, partition, consumer, iggcon.OffsetPollingStrategy(0), iggcon.DefaultPollOptions())
	require.NoError(t, err)

	for range serverParkedLimit + 8 {
		// A deadline would shorten the wait to fit it, so the poll is
		// cancelled instead.
		cancelled, cancel := context.WithCancel(ctx)
		stop := time.AfterFunc(parkFor, cancel)
		_, err := connected.PollMessages(cancelled, streamId, topicId, partition, consumer,
			iggcon.OffsetPollingStrategy(0), parkedPoll)
		stop.Stop()
		cancel()
		require.ErrorIs(t, err, ierror.ErrTransientNotCommitted, "the poll ends with its context")
	}
	_, err = connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(partition), testMessages(t, 1))
	require.NoError(t, err)
	started := time.Now()

	polled, err := connected.PollMessages(ctx, streamId, topicId, partition, consumer,
		iggcon.OffsetPollingStrategy(0), longWaitOptions(1))

	require.NoError(t, err)
	assert.Less(t, time.Since(started), promptReply)
	require.Len(t, polled.Messages, 1)
}

// A disconnect while a committing poll waits on the server leaves its outcome
// unknown, so the partition takes no committing poll and no offset store until
// the caller abandons that poll, also after a reconnect and a new sign-in.
func TestE2E_LostCommittingPollBlocksItsPartitionUntilAbandoned(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 1)
	consumer := singleConsumer(t)
	partition := uint32(0)
	// The route lookup happens first, so the committing poll reaches the server before the disconnect.
	_, err := connected.PollMessages(ctx, streamId, topicId, partition, consumer, iggcon.OffsetPollingStrategy(0), iggcon.DefaultPollOptions())
	require.NoError(t, err)
	committing := longWaitOptions(1)
	committing.Commit = iggcon.OnReadCommit()
	lost := make(chan error, 1)
	go func() {
		_, err := connected.PollMessages(ctx, streamId, topicId, partition, consumer, iggcon.NextPollingStrategy(), committing)
		lost <- err
	}()
	time.Sleep(200 * time.Millisecond)
	disconnecting, ok := connected.(interface{ Unwrap() iggcon.Client }).Unwrap().(interface{ Disconnect() error })
	require.True(t, ok)

	require.NoError(t, disconnecting.Disconnect())

	var unknown ierror.FetchProgressUncertain
	select {
	case err := <-lost:
		require.ErrorAs(t, err, &unknown)
	case <-time.After(promptReply):
		t.Fatal("the committing poll outlived the disconnect")
	}
	require.NoError(t, connected.Connect(ctx))
	_, err = connected.LoginUser(ctx, rootUsername, rootPassword)
	require.NoError(t, err)
	_, err = connected.PollMessages(ctx, streamId, topicId, partition, consumer, iggcon.NextPollingStrategy(), countOptions(1, true))
	var blocked ierror.PartitionLaneBlocked
	require.ErrorAs(t, err, &blocked)
	assert.Equal(t, unknown.OperationID, blocked.OperationID)
	require.NoError(t, connected.AbandonOperation(ctx, unknown.OperationID))
	_, err = connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(partition), testMessages(t, 1))
	require.NoError(t, err)
	polled, err := connected.PollMessages(ctx, streamId, topicId, partition, consumer, iggcon.NextPollingStrategy(), countOptions(1, true))
	require.NoError(t, err)
	assert.Len(t, polled.Messages, 1)
}

// Close cancels a committing read that waits on the server and takes its one
// reply, so it ends in about a round trip instead of after the wait.
func TestE2E_OnReadCloseCancelsItsParkedRead(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 1)
	_, err := connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(0), testMessages(t, 1))
	require.NoError(t, err)
	options := longWaitOptions(1)
	options.Commit = iggcon.OnReadCommit()
	partitionReader, err := reader.NewPartitionReader(ctx, connected, streamId, topicId, singleConsumer(t),
		reader.WithPartitions(0), reader.WithPollOptions(options))
	require.NoError(t, err)
	readCtx, cancel := context.WithTimeout(ctx, promptReply)
	_, err = partitionReader.Next(readCtx)
	cancel()
	require.NoError(t, err)
	parked := make(chan error, 1)
	go func() {
		_, err := partitionReader.Next(context.Background())
		parked <- err
	}()
	time.Sleep(300 * time.Millisecond)
	started := time.Now()

	require.NoError(t, partitionReader.Close(ctx))

	assert.Less(t, time.Since(started), promptReply, "the close waited out the parked read")
	require.ErrorIs(t, <-parked, ierror.ErrClientShutdown)
}

// newGroup creates a consumer group on the topic, which a reader or a consumer then joins.
func newGroup(t *testing.T, connected iggcon.Client, streamId, topicId iggcon.Identifier) iggcon.Identifier {
	t.Helper()
	group, err := connected.CreateConsumerGroup(context.Background(), streamId, topicId, "deferred-readers")
	require.NoError(t, err)
	id, err := iggcon.NewIdentifier(group.Id)
	require.NoError(t, err)
	return id
}

func singleConsumer(t *testing.T) iggcon.Consumer {
	t.Helper()
	id, err := iggcon.NewIdentifier(uint32(1))
	require.NoError(t, err)
	return iggcon.NewSingleConsumer(id)
}
