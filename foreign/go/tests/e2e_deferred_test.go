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
	"slices"
	"testing"
	"time"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
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

func longDeferredOptions() iggcon.DeferredPollOptions {
	options := iggcon.DefaultDeferredPollOptions()
	options.MaxWait = longWait
	options.RequestTimeout = longWait + iggcon.MinDeferredPollReplyMargin
	return options
}

// A member that owns three partitions reads the one that holds messages
// without waiting on the idle ones: one fetch names every partition of its
// readiness group, and each partition continues from its last section.
func TestE2E_FetchMessagesReadsThePartitionThatHoldsMessages(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 3)
	groupId, partitions := soleMember(t, connected, streamId, topicId)
	require.Len(t, partitions, 3)
	busy := partitions[len(partitions)-1]
	targets := assignmentTargets(t, connected, streamId, topicId, partitions)

	for round := range 3 {
		_, err := connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(busy), testMessages(t, 1))
		require.NoError(t, err)
		started := time.Now()

		var read map[uint32][]iggcon.IggyMessage
		read, targets = fetchAssignment(t, connected, streamId, topicId, groupId, targets, 10)

		assert.Less(t, time.Since(started), promptReply, "round %d waited on an idle partition", round)
		require.Len(t, read, 1, "round %d read another partition", round)
		require.Len(t, read[busy], 1, "round %d", round)
		assert.Equal(t, uint64(round), read[busy][0].Header.Offset)
	}
}

// The server serves the targets of a fetch in request order, and a member
// moves each partition that it read to the end of its next fetch. So a late
// message waits behind a backlog for one fetch at most.
func TestE2E_FetchMessagesFindsAPartitionBehindABacklog(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 3)
	groupId, partitions := soleMember(t, connected, streamId, topicId)
	require.Len(t, partitions, 3)
	targets := assignmentTargets(t, connected, streamId, topicId, partitions)
	backlog, late := targets[0].PartitionId, targets[1].PartitionId
	const backlogSize = 10
	_, err := connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(backlog), testMessages(t, backlogSize))
	require.NoError(t, err)
	// A member that commits each message fetches one at a time.
	const batchSize = 1
	read, targets := fetchAssignment(t, connected, streamId, topicId, groupId, targets, batchSize)
	require.Len(t, read[backlog], 1, "the first target in request order is read first")
	_, err = connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(late), testMessages(t, 1))
	require.NoError(t, err)

	read, _ = fetchAssignment(t, connected, streamId, topicId, groupId, targets, batchSize)

	assert.Len(t, read[late], 1, "the late message waits behind the backlog")
	assert.Empty(t, read[backlog])
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
	polled, err := connected.PollMessagesDeferred(ctx, streamId, topicId, consumer,
		iggcon.NextPollingStrategy(), 10, false, &partition, longDeferredOptions())

	require.NoError(t, err)
	elapsed := time.Since(started)
	assert.GreaterOrEqual(t, elapsed, sendDelay, "the poll waits for the message")
	assert.Less(t, elapsed, promptReply, "the send of the same client wakes its parked poll")
	require.Len(t, polled.Messages, 1)

	quiet := iggcon.DefaultDeferredPollOptions()
	quiet.MaxWait = sendDelay
	started = time.Now()
	polled, err = connected.PollMessagesDeferred(ctx, streamId, topicId, consumer,
		iggcon.OffsetPollingStrategy(1), 10, false, &partition, quiet)
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
	parkedPoll := iggcon.DefaultDeferredPollOptions()
	parkedPoll.MaxWait = 30 * time.Second
	parkedPoll.RequestTimeout = parkedPoll.MaxWait + iggcon.MinDeferredPollReplyMargin
	const parkFor = 50 * time.Millisecond
	// The route lookup happens before the loop, so each poll reaches the
	// server well before it is cancelled.
	_, err := connected.PollMessages(ctx, streamId, topicId, consumer, iggcon.OffsetPollingStrategy(0), 1, false, &partition)
	require.NoError(t, err)

	for range serverParkedLimit + 8 {
		// A deadline would shorten the wait to fit it, so the poll is
		// cancelled instead.
		cancelled, cancel := context.WithCancel(ctx)
		stop := time.AfterFunc(parkFor, cancel)
		_, err := connected.PollMessagesDeferred(cancelled, streamId, topicId, consumer,
			iggcon.OffsetPollingStrategy(0), 1, false, &partition, parkedPoll)
		stop.Stop()
		cancel()
		require.ErrorIs(t, err, ierror.ErrTransientNotCommitted, "the poll ends with its context")
	}
	_, err = connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(partition), testMessages(t, 1))
	require.NoError(t, err)
	started := time.Now()

	polled, err := connected.PollMessagesDeferred(ctx, streamId, topicId, consumer,
		iggcon.OffsetPollingStrategy(0), 1, false, &partition, longDeferredOptions())

	require.NoError(t, err)
	assert.Less(t, time.Since(started), promptReply)
	require.Len(t, polled.Messages, 1)
}

// soleMember makes connected the only member of a new consumer group, which
// then owns every partition of the topic. It returns the group and those
// partitions.
func soleMember(t *testing.T, connected iggcon.Client, streamId, topicId iggcon.Identifier) (iggcon.Identifier, []uint32) {
	t.Helper()
	ctx := context.Background()
	group, err := connected.CreateConsumerGroup(ctx, streamId, topicId, "go-e2e-deferred-group")
	require.NoError(t, err)
	groupId, err := iggcon.NewIdentifier(group.Id)
	require.NoError(t, err)
	require.NoError(t, connected.JoinConsumerGroup(ctx, streamId, topicId, groupId))
	waitForGroupAssignment(t, connected, streamId, topicId, groupId)
	assignment, err := connected.SyncConsumerGroup(ctx, streamId, topicId, groupId)
	require.NoError(t, err)
	return groupId, assignment.Partitions
}

// assignmentTargets reads every partition of an assignment from Next, in the
// order of its readiness group.
func assignmentTargets(t *testing.T, connected iggcon.Client, streamId, topicId iggcon.Identifier, partitions []uint32) []iggcon.FetchMessagesTarget {
	t.Helper()
	groups, err := connected.ReadinessGroups(context.Background(), streamId, topicId, partitions)
	require.NoError(t, err)
	require.Len(t, groups, 1, "one server is the primary of every partition")
	targets := make([]iggcon.FetchMessagesTarget, 0, len(groups[0]))
	for _, partition := range groups[0] {
		targets = append(targets, iggcon.FetchMessagesTarget{PartitionId: partition, Strategy: iggcon.NextPollingStrategy()})
	}
	return targets
}

// fetchAssignment fetches targets once with auto-commit. It returns the
// messages of each partition that it read, and the targets of the next fetch:
// each continues from its section, and the partitions that it read move to
// the end, because the server serves the targets in request order.
func fetchAssignment(t *testing.T, connected iggcon.Client, streamId, topicId, groupId iggcon.Identifier,
	targets []iggcon.FetchMessagesTarget, count uint32) (map[uint32][]iggcon.IggyMessage, []iggcon.FetchMessagesTarget) {
	t.Helper()
	sections, err := connected.FetchMessages(context.Background(), streamId, topicId, iggcon.NewGroupConsumer(groupId),
		targets, count, true, longDeferredOptions())
	require.NoError(t, err)
	read := make(map[uint32][]iggcon.IggyMessage)
	next := slices.Clone(targets)
	for _, section := range sections {
		polled, err := binaryserialization.DeserializeSectionMessages(section)
		require.NoError(t, err, "partition %d", section.PartitionId)
		index := slices.IndexFunc(next, func(target iggcon.FetchMessagesTarget) bool {
			return target.PartitionId == section.PartitionId
		})
		require.GreaterOrEqual(t, index, 0, "partition %d", section.PartitionId)
		next[index].Strategy = iggcon.OffsetPollingStrategy(section.NextOffset).WithContext(section.Context)
		if len(polled.Messages) > 0 {
			read[section.PartitionId] = polled.Messages
		}
	}
	rotated := make([]iggcon.FetchMessagesTarget, 0, len(next))
	for _, served := range []bool{false, true} {
		for _, target := range next {
			if _, wasRead := read[target.PartitionId]; wasRead == served {
				rotated = append(rotated, target)
			}
		}
	}
	return read, rotated
}

func singleConsumer(t *testing.T) iggcon.Consumer {
	t.Helper()
	id, err := iggcon.NewIdentifier(uint32(1))
	require.NoError(t, err)
	return iggcon.NewSingleConsumer(id)
}
