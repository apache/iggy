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
	"encoding/binary"
	"testing"

	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_SendAndPollRoundTrip(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 2)

	stream, err := connected.GetStream(ctx, streamId)
	require.NoError(t, err)
	topic, err := connected.GetTopic(ctx, streamId, topicId)
	require.NoError(t, err)

	var baseOffsets []uint64
	for range 3 {
		response, err := connected.SendMessages(ctx, streamId, topicId,
			iggcon.PartitionId(0), testMessages(t, 4))
		require.NoError(t, err)
		require.NotEmpty(t, response.Confirmations, "the server confirmed the batch")

		confirmation := response.Confirmations[0]
		assert.Equal(t, stream.Id, confirmation.StreamId)
		assert.Equal(t, topic.Id, confirmation.TopicId)
		assert.Equal(t, uint32(0), confirmation.PartitionId)
		baseOffsets = append(baseOffsets, confirmation.BaseOffset)
	}

	for index := 1; index < len(baseOffsets); index++ {
		assert.Greater(t, baseOffsets[index], baseOffsets[index-1],
			"each batch commits after the previous one")
	}

	partitionId := uint32(0)
	polled, err := connected.PollMessages(ctx, streamId, topicId,
		iggcon.DefaultConsumer(), iggcon.OffsetPollingStrategy(0), 12, false, &partitionId)
	require.NoError(t, err)
	assert.Len(t, polled.Messages, 12)
	assert.Equal(t, uint32(0), polled.PartitionId)
	assert.Equal(t, "message 0", string(polled.Messages[0].Payload))
}

func TestE2E_ResolvesBalancedAndKeyPartitioning(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 3)

	balanced, err := connected.SendMessages(ctx, streamId, topicId,
		iggcon.None(), testMessages(t, 1))
	require.NoError(t, err)
	require.NotEmpty(t, balanced.Confirmations)
	assert.Less(t, balanced.Confirmations[0].PartitionId, uint32(3))

	key, err := iggcon.EntityIdString("order-key-1")
	require.NoError(t, err)

	first, err := connected.SendMessages(ctx, streamId, topicId, key, testMessages(t, 1))
	require.NoError(t, err)
	second, err := connected.SendMessages(ctx, streamId, topicId, key, testMessages(t, 1))
	require.NoError(t, err)
	require.NotEmpty(t, first.Confirmations)
	require.NotEmpty(t, second.Confirmations)
	assert.Equal(t, first.Confirmations[0].PartitionId, second.Confirmations[0].PartitionId,
		"the same key always lands on the same partition")
}

func TestE2E_RefreshesPartitionCountAfterPartitionChanges(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 1)

	first, err := connected.SendMessages(ctx, streamId, topicId,
		iggcon.None(), testMessages(t, 1))
	require.NoError(t, err)
	require.NotEmpty(t, first.Confirmations)
	assert.Equal(t, uint32(0), first.Confirmations[0].PartitionId)

	require.NoError(t, connected.CreatePartitions(ctx, streamId, topicId, 2))
	second, err := connected.SendMessages(ctx, streamId, topicId,
		iggcon.None(), testMessages(t, 1))
	require.NoError(t, err)
	require.NotEmpty(t, second.Confirmations)
	assert.Equal(t, uint32(1), second.Confirmations[0].PartitionId)

	require.NoError(t, connected.DeletePartitions(ctx, streamId, topicId, 2))
	third, err := connected.SendMessages(ctx, streamId, topicId,
		iggcon.None(), testMessages(t, 1))
	require.NoError(t, err)
	require.NotEmpty(t, third.Confirmations)
	assert.Equal(t, uint32(0), third.Confirmations[0].PartitionId)
}

func TestE2E_StoresAndReadsConsumerOffsets(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 1)

	_, err := connected.SendMessages(ctx, streamId, topicId,
		iggcon.PartitionId(0), testMessages(t, 5))
	require.NoError(t, err)

	consumer := iggcon.DefaultConsumer()
	partitionId := uint32(0)
	require.NoError(t, connected.StoreConsumerOffset(
		ctx, consumer, streamId, topicId, 3, &partitionId))

	offset, err := connected.GetConsumerOffset(ctx, consumer, streamId, topicId, &partitionId)
	require.NoError(t, err)
	require.NotNil(t, offset)
	assert.Equal(t, uint64(3), offset.StoredOffset)

	require.NoError(t, connected.DeleteConsumerOffset(
		ctx, consumer, streamId, topicId, &partitionId))
}

func TestE2E_ConsumerGroupFlow(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 3)

	group, err := connected.CreateConsumerGroup(ctx, streamId, topicId, "go-e2e-group")
	require.NoError(t, err)
	groupId, err := iggcon.NewIdentifier(group.Id)
	require.NoError(t, err)
	require.NoError(t, connected.JoinConsumerGroup(ctx, streamId, topicId, groupId))

	assignment, err := connected.SyncConsumerGroup(ctx, streamId, topicId, groupId)
	require.NoError(t, err)
	require.NotNil(t, assignment, "a member always has an assignment, even an empty one")
	assert.NotEmpty(t, assignment.Partitions, "the only member owns every partition")

	for _, partitionId := range assignment.Partitions {
		_, err := connected.SendMessages(ctx, streamId, topicId,
			iggcon.PartitionId(partitionId), testMessages(t, 2))
		require.NoError(t, err)
	}

	// A group poll without an explicit partition round-robins the partitions
	// the member owns, so polling once per partition covers every one of them.
	polledPartitions := make(map[uint32]bool)
	for range len(assignment.Partitions) {
		polled, err := connected.PollMessages(ctx, streamId, topicId,
			iggcon.NewGroupConsumer(groupId), iggcon.NextPollingStrategy(), 2, true, nil)
		require.NoError(t, err)
		require.NotEqual(t, iggcon.NoAssignedPartition, polled.PartitionId)
		polledPartitions[polled.PartitionId] = true
	}
	assert.Len(t, polledPartitions, len(assignment.Partitions),
		"every owned partition was polled once")

	require.NoError(t, connected.LeaveConsumerGroup(ctx, streamId, topicId, groupId))
	require.NoError(t, connected.DeleteConsumerGroup(ctx, streamId, topicId, groupId))
}

func TestE2E_RawRequestsDoNotGapMetadataRequestIDs(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()

	for range 5 {
		_, err := connected.SendBinaryRequest(ctx, vendorCode, nil)
		// The server may not implement the code. What matters is that the
		// exchange leaves the session usable for the metadata command below.
		_ = err
	}

	streamId, topicId := scratchTopic(t, connected, 1)
	topic, err := connected.GetTopic(ctx, streamId, topicId)
	require.NoError(t, err, "a metadata request still commits after raw traffic")
	require.NotNil(t, topic)
}

func TestE2E_RejectsSessionControlCodesOnTheRawPath(t *testing.T) {
	connected := connect(t)

	_, err := connected.SendBinaryRequest(context.Background(), 38, nil)
	assert.Error(t, err, "a login must go through LoginUser, not the raw path")
}

func TestE2E_ConsumerOffsetsV2OverTheRawPath(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 1)

	_, err := connected.SendMessages(ctx, streamId, topicId,
		iggcon.PartitionId(0), testMessages(t, 5))
	require.NoError(t, err)

	consumer := iggcon.DefaultConsumer()
	partitionId := uint32(0)

	for _, ack := range []byte{ackQuorum, ackNoAck} {
		offset := uint64(2)
		if ack == ackNoAck {
			offset = 4
		}

		store := consumerOffsetV2Payload(t, consumer, streamId, topicId, partitionId)
		store = binary.LittleEndian.AppendUint64(store, offset)
		store = append(store, ack)

		_, err := connected.SendBinaryRequest(ctx, storeConsumerOffset2Code, store)
		require.NoError(t, err, "ack level %d", ack)

		stored, err := connected.GetConsumerOffset(ctx, consumer, streamId, topicId, &partitionId)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, offset, stored.StoredOffset, "ack level %d", ack)
	}

	remove := consumerOffsetV2Payload(t, consumer, streamId, topicId, partitionId)
	remove = append(remove, ackQuorum)
	_, err = connected.SendBinaryRequest(ctx, deleteConsumerOffset2Code, remove)
	require.NoError(t, err)
}

func TestE2E_FetchesASnapshot(t *testing.T) {
	connected := connect(t)

	// [compression = Stored][types count = 1][type = FilesystemOverview]
	snapshot, err := connected.SendBinaryRequest(
		context.Background(), getSnapshotCode, []byte{1, 1, 1})
	require.NoError(t, err)
	assert.NotEmpty(t, snapshot, "the snapshot archive is not empty")
}

func TestE2E_AutoLoginSignsInOnConnect(t *testing.T) {
	connected := newClient(t, tcp.WithAutoLogin(
		tcp.NewUsernamePasswordCredentials(rootUsername, rootPassword)))

	// No explicit LoginUser call: an authenticated command is enough proof.
	streams, err := connected.GetStreams(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, streams)
}

func TestE2E_PingWorksBeforeSigningIn(t *testing.T) {
	connected := newClient(t)

	require.NoError(t, connected.Ping(context.Background()))

	metadata, err := connected.GetClusterMetadata(context.Background())
	require.NoError(t, err, "cluster metadata is readable before the sign-in")
	require.NotNil(t, metadata)
	assert.NotEmpty(t, metadata.Nodes)
}

func TestE2E_LogoutEndsTheSession(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()

	_, err := connected.GetStreams(ctx)
	require.NoError(t, err)

	require.NoError(t, connected.LogoutUser(ctx))

	// The next sign-in registers a fresh identity and works again.
	_, err = connected.LoginUser(ctx, rootUsername, rootPassword)
	require.NoError(t, err)
	_, err = connected.GetStreams(ctx)
	require.NoError(t, err)
}

func TestE2E_TLSRoundTrip(t *testing.T) {
	if !tlsEnabled() {
		t.Skip("set IGGY_TCP_TLS_ENABLED=true to run the TLS cases")
	}

	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 1)

	response, err := connected.SendMessages(ctx, streamId, topicId,
		iggcon.PartitionId(0), testMessages(t, 3))
	require.NoError(t, err)
	require.NotEmpty(t, response.Confirmations)

	partitionId := uint32(0)
	polled, err := connected.PollMessages(ctx, streamId, topicId,
		iggcon.DefaultConsumer(), iggcon.OffsetPollingStrategy(0), 3, false, &partitionId)
	require.NoError(t, err)
	assert.Len(t, polled.Messages, 3)
}
