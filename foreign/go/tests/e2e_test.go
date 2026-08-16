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
	"fmt"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
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

func TestE2E_OnlyPingWorksBeforeSigningIn(t *testing.T) {
	connected := newClient(t)

	require.NoError(t, connected.Ping(context.Background()))

	// Ping is the only command the server answers on an unbound connection.
	// The cluster roster is auth-gated so that an unauthenticated reader cannot
	// enumerate the private network topology.
	metadata, err := connected.GetClusterMetadata(context.Background())
	require.ErrorIs(t, err, ierror.ErrUnauthenticated)
	assert.Nil(t, metadata, "no roster leaks to an unauthenticated reader")
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

func TestE2E_TopicOptionsRoundTripAndCatalog(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()

	specs, err := connected.DescribeOptions(ctx, iggcon.OptionsScopeTopic)
	require.NoError(t, err)
	byKey := map[string]iggcon.OptionSpec{}
	for _, spec := range specs {
		byKey[spec.Key] = spec
	}
	require.Contains(t, byKey, "enforce_fsync", "the catalog lists the keys create accepts")
	require.Contains(t, byKey, "segment_size")
	assert.NotEmpty(t, byKey["segment_size"].Description)
	assert.Equal(t, iggcon.Uint64, byKey["segment_size"].DefaultValue.Kind)

	// Scopes with no catalog keys answer with an empty list rather than failing.
	streamSpecs, err := connected.DescribeOptions(ctx, iggcon.OptionsScopeStream)
	require.NoError(t, err)
	assert.Empty(t, streamSpecs)

	name := fmt.Sprintf("go-e2e-options-%d", time.Now().UnixNano())
	stream, err := connected.CreateStream(ctx, name)
	require.NoError(t, err)
	streamId, err := iggcon.NewIdentifier(stream.Id)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connected.DeleteStream(context.Background(), streamId) })

	// A key with no parameter of its own rides the variadic options block.
	created, err := connected.CreateTopic(ctx, streamId, name, 1,
		iggcon.CompressionAlgorithmNone, iggcon.Duration(0), 0,
		iggcon.HeaderEntry{
			Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("enforce_fsync")},
			Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("true")},
		})
	require.NoError(t, err)

	topicId, err := iggcon.NewIdentifier(created.Id)
	require.NoError(t, err)
	topic, err := connected.GetTopic(ctx, streamId, topicId)
	require.NoError(t, err)

	fsync, ok := topic.Options["enforce_fsync"]
	require.True(t, ok, "an explicitly set key is reported as explicit, got %v", topic.Options)
	// Create admission re-encodes the block from its own parse, so the stored
	// value carries the key's canonical kind whatever kind the client sent it
	// as: this string "true" comes back as a Bool.
	assert.Equal(t, iggcon.Bool, fsync.Kind)
	assert.Equal(t, []byte{1}, fsync.Value)
	// Keys the client left alone are resolved by admission and reported apart.
	require.Contains(t, topic.DerivedOptions, "max_topic_size")
	assert.NotContains(t, topic.DerivedOptions, "enforce_fsync")

	// A key outside the catalog is refused by name.
	_, err = connected.CreateTopic(ctx, streamId, name+"-bad", 1,
		iggcon.CompressionAlgorithmNone, iggcon.Duration(0), 0,
		iggcon.HeaderEntry{
			Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("not_a_real_option")},
			Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("1")},
		})
	require.Error(t, err)
}

func TestE2E_TypedTopicOptionsMatchTheCatalog(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()

	// Values inside the bounds the catalog reports: a 512-byte multiple segment
	// size at the floor, a non-zero message threshold, preallocation off so
	// nothing is reserved on disk.
	typed := []iggcon.HeaderEntry{
		iggcon.SegmentSizeOption(1024 * 1024),
		iggcon.EnforceFsyncOption(true),
		iggcon.MessagesRequiredToSaveOption(7),
		iggcon.SizeOfMessagesRequiredToSaveOption(4096),
		iggcon.PreallocateSegmentsOption(false),
	}

	specs, err := connected.DescribeOptions(ctx, iggcon.OptionsScopeTopic)
	require.NoError(t, err)
	catalog := map[string]iggcon.OptionSpec{}
	for _, spec := range specs {
		catalog[spec.Key] = spec
	}
	for _, entry := range typed {
		key := string(entry.Key.Value)
		spec, found := catalog[key]
		require.True(t, found, "%q is not a catalog key", key)
		assert.Equal(t, spec.DefaultValue.Kind, entry.Value.Kind,
			"%q must be sent in the kind the catalog gives it", key)
	}

	name := fmt.Sprintf("go-e2e-typed-options-%d", time.Now().UnixNano())
	stream, err := connected.CreateStream(ctx, name)
	require.NoError(t, err)
	streamId, err := iggcon.NewIdentifier(stream.Id)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connected.DeleteStream(context.Background(), streamId) })

	created, err := connected.CreateTopic(ctx, streamId, name, 1,
		iggcon.CompressionAlgorithmNone, iggcon.Duration(0), 0, typed...)
	require.NoError(t, err)

	topicId, err := iggcon.NewIdentifier(created.Id)
	require.NoError(t, err)
	topic, err := connected.GetTopic(ctx, streamId, topicId)
	require.NoError(t, err)

	for _, entry := range typed {
		key := string(entry.Key.Value)
		stored, found := topic.Options[key]
		require.True(t, found, "%q was set explicitly, got %v", key, topic.Options)
		// Admission re-encodes the block in each key's canonical kind, which is
		// the kind the constructor already sent, so the bytes survive unchanged.
		assert.Equal(t, entry.Value.Kind, stored.Kind, key)
		assert.Equal(t, entry.Value.Value, stored.Value, key)
		assert.NotContains(t, topic.DerivedOptions, key)
	}

	// A flush threshold of zero messages can never trip, so it is refused.
	_, err = connected.CreateTopic(ctx, streamId, name+"-zero-flush", 1,
		iggcon.CompressionAlgorithmNone, iggcon.Duration(0), 0,
		iggcon.MessagesRequiredToSaveOption(0))
	require.Error(t, err)

	// A segment size off the 512-byte grid is refused, but zero is not: it
	// leaves the key derived.
	_, err = connected.CreateTopic(ctx, streamId, name+"-odd-segment", 1,
		iggcon.CompressionAlgorithmNone, iggcon.Duration(0), 0,
		iggcon.SegmentSizeOption(1024*1024+1))
	require.Error(t, err)

	zeroSegment := iggcon.SegmentSizeOption(0)
	derived, err := connected.CreateTopic(ctx, streamId, name+"-zero-segment", 1,
		iggcon.CompressionAlgorithmNone, iggcon.Duration(0), 0, zeroSegment)
	require.NoError(t, err)
	segmentSizeKey := string(zeroSegment.Key.Value)
	assert.NotContains(t, derived.Options, segmentSizeKey)
	assert.Contains(t, derived.DerivedOptions, segmentSizeKey)
}
