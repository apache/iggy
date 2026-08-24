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
	"strings"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// streamDetailsBody builds a stream reply with no topics:
// [id u32][created_at u64][topics u32][size u64][messages u64][name_len u8][name]
// [options_len u32 = 0].
func streamDetailsBody(id uint32, name string) []byte {
	body := binary.LittleEndian.AppendUint32(nil, id)
	body = binary.LittleEndian.AppendUint64(body, 1700000000)
	body = binary.LittleEndian.AppendUint32(body, 0)
	body = binary.LittleEndian.AppendUint64(body, 0)
	body = binary.LittleEndian.AppendUint64(body, 0)
	body = append(body, byte(len(name)))
	body = append(body, name...)
	return binary.LittleEndian.AppendUint32(body, 0)
}

func TestCreateStream_DecodesTheCreatedStream(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateStream,
			append(resultSection(), streamDetailsBody(9, "orders")...))
	})

	stream, err := client.CreateStream(context.Background(), "orders")
	require.NoError(t, err)
	assert.Equal(t, uint32(9), stream.Id)
	assert.Equal(t, "orders", stream.Name)
}

func TestCreateStream_RejectsAnInvalidNameWithoutWriting(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateStream, resultSection())
	})

	for _, name := range []string{"", strings.Repeat("x", MaxStringLength+1)} {
		_, err := client.CreateStream(context.Background(), name)
		assert.ErrorIs(t, err, ierror.ErrInvalidStreamName, "name %q", name)
	}
	assert.Empty(t, server.recorded())
}

func TestGetStream_ReportsAMissingStreamAsATypedError(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	_, err := client.GetStream(context.Background(), numericIdentifier(t, 42))
	assert.ErrorIs(t, err, ierror.ErrStreamIdNotFound)
}

func TestGetStreams_DecodesTheRoster(t *testing.T) {
	client, serverConn := newPipeClient(t)
	body := append(streamDetailsBody(1, "orders"), streamDetailsBody(2, "billing")...)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, body)
	})

	streams, err := client.GetStreams(context.Background())
	require.NoError(t, err)
	require.Len(t, streams, 2)
	assert.Equal(t, "orders", streams[0].Name)
	assert.Equal(t, "billing", streams[1].Name)
}

func TestUpdateStream_RejectsAnInvalidNameWithoutWriting(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationUpdateStream, resultSection())
	})

	for _, name := range []string{"", strings.Repeat("x", MaxStringLength+1)} {
		err := client.UpdateStream(context.Background(), numericIdentifier(t, 1), name)
		assert.ErrorIs(t, err, ierror.ErrInvalidStreamName, "name %q", name)
	}
	assert.Empty(t, server.recorded())
}

func TestUpdateStream_AcceptsACommittedRename(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationUpdateStream, resultSection())
	})

	assert.NoError(t, client.UpdateStream(
		context.Background(), numericIdentifier(t, 1), "renamed"))
}

func TestCreatePartitions_InvalidatesTheCachedPartitionCount(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, read request) []byte {
		switch read.operation() {
		case vsr.OperationCreatePartitions:
			return replyFrame(vsr.OperationCreatePartitions, resultSection())
		case vsr.OperationDeletePartitions:
			return replyFrame(vsr.OperationDeletePartitions, resultSection())
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})

	streamId := numericIdentifier(t, 1)
	topicId := numericIdentifier(t, 2)
	key := newTopicKey(streamId, topicId)
	client.topics.setPartitionsCount(key, 3)

	require.NoError(t, client.CreatePartitions(context.Background(), streamId, topicId, 2))
	_, cached := client.topics.partitionsCount(key)
	assert.False(t, cached, "a partition change must force a metadata reread")

	client.topics.setPartitionsCount(key, 5)
	require.NoError(t, client.DeletePartitions(context.Background(), streamId, topicId, 2))
	_, cached = client.topics.partitionsCount(key)
	assert.False(t, cached)
}

func TestDeleteTopic_DropsTheTopicCacheEntry(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationDeleteTopic, resultSection())
	})

	streamId := numericIdentifier(t, 1)
	topicId := numericIdentifier(t, 2)
	key := newTopicKey(streamId, topicId)
	client.topics.setPartitionsCount(key, 3)
	client.topics.nextBalanced(key, 3)

	require.NoError(t, client.DeleteTopic(context.Background(), streamId, topicId))
	_, cached := client.topics.partitionsCount(key)
	assert.False(t, cached)
	assert.Equal(t, uint32(0), client.topics.nextBalanced(key, 3),
		"the balanced cursor restarts with the recreated topic")
}
