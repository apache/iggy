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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_StreamKeyProjection(t *testing.T) {
	t.Run("given_stream_key_when_creating_should_read_matching_projection", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()
		key := randomStreamKey(t)
		fingerprint, err := iggcon.EncryptionKeyFingerprint(key)
		require.NoError(t, err)

		created, err := connected.CreateStream(ctx, uniqueName("keyed"), &iggcon.StreamOptions{EncryptionKey: key})
		require.NoError(t, err)
		streamId := scratchStream(t, connected, created.Id)

		fetched, err := connected.GetStream(ctx, streamId)
		require.NoError(t, err)
		streams, err := connected.GetStreams(ctx)
		require.NoError(t, err)
		listed := findStream(t, streams, created.Id)

		for source, stream := range map[string]iggcon.Stream{"create": created.Stream, "get": fetched.Stream, "list": listed} {
			requireProjection(t, source, stream, iggcon.StreamEncryptionSourceStream)
			got, ok := stream.EncryptionKeyFingerprint()
			require.True(t, ok, source)
			assert.Equal(t, fingerprint, got, source)
			requireNoKeyMaterial(t, source, stream, key)
		}
		assert.Equal(t, created.EncryptionKekFingerprints(), fetched.EncryptionKekFingerprints())
	})
	t.Run("given_keyed_stream_when_following_its_creation_should_expose_fingerprints_and_no_key", checkKeyedStreamChange)
}

func TestE2E_KeylessStream(t *testing.T) {
	t.Run("given_keyless_stream_when_reading_should_report_server_generated_key", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()

		created, err := connected.CreateStream(ctx, uniqueName("keyless"), nil)
		require.NoError(t, err)
		streamId := scratchStream(t, connected, created.Id)

		fetched, err := connected.GetStream(ctx, streamId)
		require.NoError(t, err)
		for source, stream := range map[string]iggcon.Stream{"create": created.Stream, "get": fetched.Stream} {
			requireProjection(t, source, stream, iggcon.StreamEncryptionSourceServer)
			fingerprint, ok := stream.EncryptionKeyFingerprint()
			require.True(t, ok, source)
			assert.Len(t, fingerprint, 16, source)
			requireNoKeyMaterial(t, source, stream, nil)
		}
	})
}

func TestE2E_StreamRename(t *testing.T) {
	t.Run("given_stream_rename_when_updating_should_preserve_encryption_projection", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()
		key := randomStreamKey(t)
		fingerprint, err := iggcon.EncryptionKeyFingerprint(key)
		require.NoError(t, err)

		created, err := connected.CreateStream(ctx, uniqueName("rename"), &iggcon.StreamOptions{EncryptionKey: key})
		require.NoError(t, err)
		streamId := scratchStream(t, connected, created.Id)
		partitions := uint32(1)
		topic, err := connected.CreateTopic(ctx, streamId, "events", &iggcon.TopicOptions{PartitionsCount: &partitions})
		require.NoError(t, err)
		topicId, err := iggcon.NewIdentifier(topic.Id)
		require.NoError(t, err)
		payload := []byte("written before the rename")
		message, err := iggcon.NewIggyMessage(payload)
		require.NoError(t, err)
		_, err = connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(0), []iggcon.IggyMessage{message})
		require.NoError(t, err)

		newName := uniqueName("renamed")
		require.NoError(t, connected.UpdateStream(ctx, streamId, iggcon.StreamUpdate{Name: &newName}))

		renamed, err := connected.GetStream(ctx, streamId)
		require.NoError(t, err)
		assert.Equal(t, created.Id, renamed.Id)
		assert.Equal(t, newName, renamed.Name)
		requireProjection(t, "renamed", renamed.Stream, iggcon.StreamEncryptionSourceStream)
		got, ok := renamed.EncryptionKeyFingerprint()
		require.True(t, ok)
		assert.Equal(t, fingerprint, got)
		assert.Equal(t, created.EncryptionKekFingerprints(), renamed.EncryptionKekFingerprints())

		partitionId := uint32(0)
		polled, err := connected.PollMessages(ctx, streamId, topicId,
			iggcon.DefaultConsumer(), iggcon.OffsetPollingStrategy(0), 1, false, &partitionId)
		require.NoError(t, err)
		require.Len(t, polled.Messages, 1)
		assert.Equal(t, payload, polled.Messages[0].Payload)

		err = connected.UpdateStream(ctx, streamId, iggcon.StreamUpdate{Reset: []iggcon.StreamOptionKey{iggcon.StreamOptionEncryptionKey}})
		assert.ErrorIs(t, err, ierror.ErrOptionNotUpdatable)
	})
}

func TestE2E_TopicReset(t *testing.T) {
	t.Run("given_topic_reset_when_updating_should_preserve_unmentioned_values", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()
		created, err := connected.CreateStream(ctx, uniqueName("topic-reset"), nil)
		require.NoError(t, err)
		streamId := scratchStream(t, connected, created.Id)

		partitions := uint32(1)
		compression := iggcon.CompressionAlgorithmGzip
		expiry := iggcon.Duration(3_600_000_000)
		maxTopicSize := uint64(1 << 30)
		name := uniqueName("topic")
		topic, err := connected.CreateTopic(ctx, streamId, name, &iggcon.TopicOptions{
			PartitionsCount:      &partitions,
			CompressionAlgorithm: &compression,
			MessageExpiry:        &expiry,
			MaxTopicSize:         &maxTopicSize,
		})
		require.NoError(t, err)
		topicId, err := iggcon.NewIdentifier(topic.Id)
		require.NoError(t, err)

		newExpiry := iggcon.Duration(7_200_000_000)
		require.NoError(t, connected.UpdateTopic(ctx, streamId, topicId, iggcon.TopicUpdate{
			MessageExpiry: &newExpiry,
			Reset:         []iggcon.TopicOptionKey{iggcon.TopicOptionMaxTopicSize},
		}))

		updated, err := connected.GetTopic(ctx, streamId, topicId)
		require.NoError(t, err)
		assert.Equal(t, name, updated.Name)
		assert.Equal(t, newExpiry, updated.MessageExpiry)
		assert.Contains(t, updated.Options, string(iggcon.TopicOptionMessageExpiry))
		assert.Equal(t, uint64(math.MaxUint64), updated.MaxTopicSize)
		assert.NotContains(t, updated.Options, string(iggcon.TopicOptionMaxTopicSize))
		assert.Contains(t, updated.DerivedOptions, string(iggcon.TopicOptionMaxTopicSize))
		assert.Equal(t, uint8(iggcon.CompressionAlgorithmGzip), updated.CompressionAlgorithm)
		assert.Equal(t, iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("gzip")}, updated.Options[string(iggcon.TopicOptionCompressionAlgorithm)])

		err = connected.UpdateTopic(ctx, streamId, topicId, iggcon.TopicUpdate{Options: []iggcon.HeaderEntry{{
			Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(iggcon.TopicOptionCompressionAlgorithm)},
			Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("none")},
		}}})
		assert.ErrorIs(t, err, ierror.ErrOptionNotUpdatable)
		err = connected.UpdateTopic(ctx, streamId, topicId, iggcon.TopicUpdate{
			Reset: []iggcon.TopicOptionKey{iggcon.TopicOptionCompressionAlgorithm},
		})
		assert.ErrorIs(t, err, ierror.ErrOptionNotUpdatable)

		unchanged, err := connected.GetTopic(ctx, streamId, topicId)
		require.NoError(t, err)
		assert.Equal(t, updated.Options, unchanged.Options)
		assert.Equal(t, updated.DerivedOptions, unchanged.DerivedOptions)
	})
}

func TestE2E_UserOptions(t *testing.T) {
	t.Run("given_user_options_when_creating_and_updating_should_preserve_user_behavior", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()

		first := scratchUser(t, connected, uniqueUsername("first"))
		second := scratchUser(t, connected, uniqueUsername("second"))
		assert.Empty(t, first.Options)
		assert.Empty(t, first.DerivedOptions)

		renamed := uniqueUsername("renamed")
		inactive := iggcon.Inactive
		firstId, err := iggcon.NewIdentifier(first.Id)
		require.NoError(t, err)
		require.NoError(t, connected.UpdateUser(ctx, firstId, iggcon.UserUpdate{Username: &renamed, Status: &inactive}))

		updated, err := connected.GetUser(ctx, firstId)
		require.NoError(t, err)
		assert.Equal(t, renamed, updated.Username)
		assert.Equal(t, iggcon.Inactive, updated.Status)
		assert.Empty(t, updated.Options)
		assert.Empty(t, updated.DerivedOptions)

		secondId, err := iggcon.NewIdentifier(second.Id)
		require.NoError(t, err)
		err = connected.UpdateUser(ctx, secondId, iggcon.UserUpdate{Username: &renamed, Status: &inactive})
		require.Error(t, err, "a taken username is refused")
		unchanged, err := connected.GetUser(ctx, secondId)
		require.NoError(t, err)
		assert.Equal(t, second.Username, unchanged.Username)
		assert.Equal(t, iggcon.Active, unchanged.Status, "a refused patch applies nothing")

		err = connected.UpdateUser(ctx, secondId, iggcon.UserUpdate{Reset: []iggcon.UserOptionKey{iggcon.UserOptionStatus}})
		assert.ErrorIs(t, err, ierror.ErrOptionNotResettable)
		err = connected.UpdateUser(ctx, secondId, iggcon.UserUpdate{})
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	})
}

func randomStreamKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, iggcon.StreamEncryptionKeyLength)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return key
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("go-e2e-%s-%d", prefix, time.Now().UnixNano())
}

// uniqueUsername stays inside the 50-byte username bound.
func uniqueUsername(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func scratchStream(t *testing.T, connected iggcon.Client, id uint32) iggcon.Identifier {
	t.Helper()
	streamId, err := iggcon.NewIdentifier(id)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connected.DeleteStream(context.Background(), streamId) })
	return streamId
}

func scratchUser(t *testing.T, connected iggcon.Client, username string) *iggcon.UserInfoDetails {
	t.Helper()
	user, err := connected.CreateUser(context.Background(), username, "user-password", iggcon.Active, nil, &iggcon.UserOptions{})
	require.NoError(t, err)
	userId, err := iggcon.NewIdentifier(user.Id)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connected.DeleteUser(context.Background(), userId) })
	return user
}

func findStream(t *testing.T, streams []iggcon.Stream, id uint32) iggcon.Stream {
	t.Helper()
	for _, stream := range streams {
		if stream.Id == id {
			return stream
		}
	}
	require.FailNow(t, "the stream is not listed", "id %d", id)
	return iggcon.Stream{}
}

func requireProjection(t *testing.T, source string, stream iggcon.Stream, want iggcon.StreamEncryptionSource) {
	t.Helper()
	got, ok := stream.EncryptionKeySource()
	require.True(t, ok, source)
	assert.Equal(t, want, got, source)
	keks := stream.EncryptionKekFingerprints()
	require.NotEmpty(t, keks, source)
	for _, kek := range keks {
		assert.Len(t, kek, 16, source)
	}
}

func requireNoKeyMaterial(t *testing.T, source string, stream iggcon.Stream, key []byte) {
	t.Helper()
	for _, options := range []map[string]iggcon.HeaderValue{stream.Options, stream.DerivedOptions} {
		for _, secret := range []string{"encryption_key", "encryption_key_wrapped", "critical_keys"} {
			assert.NotContains(t, options, secret, source)
		}
		if key == nil {
			continue
		}
		for name, value := range options {
			assert.False(t, bytes.Contains(value.Value, key), "%s: %s carries the key", source, name)
			assert.NotContains(t, string(value.Value), base64.StdEncoding.EncodeToString(key), "%s: %s", source, name)
		}
	}
}
