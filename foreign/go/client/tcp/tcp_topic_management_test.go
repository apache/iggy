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

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTopic_DecodesTheCreatedTopic(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateTopic,
			append(resultSection(), topicDetailsBody(t, 3)...))
	})

	topic, err := client.CreateTopic(context.Background(),
		numericIdentifier(t, 1), "orders", 3, 0, iggcon.Duration(0), 0, nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), topic.PartitionsCount)
	assert.Equal(t, "orders", topic.Name)
}

func TestCreateTopic_RejectsInvalidArgumentsWithoutWriting(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateTopic, resultSection())
	})

	streamId := numericIdentifier(t, 1)
	zeroReplication := uint8(0)
	tests := []struct {
		name        string
		topicName   string
		partitions  uint32
		replication *uint8
		want        error
	}{
		{name: "empty name", topicName: "", partitions: 1,
			want: ierror.ErrInvalidTopicName},
		{name: "oversized name", topicName: strings.Repeat("x", MaxStringLength+1),
			partitions: 1, want: ierror.ErrInvalidTopicName},
		{name: "too many partitions", topicName: "orders",
			partitions: MaxPartitionCount + 1, want: ierror.ErrTooManyPartitions},
		{name: "zero replication factor", topicName: "orders", partitions: 1,
			replication: &zeroReplication, want: ierror.ErrInvalidReplicationFactor},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.CreateTopic(context.Background(), streamId,
				test.topicName, test.partitions, 0, iggcon.Duration(0), 0, test.replication)
			assert.ErrorIs(t, err, test.want)
		})
	}
	assert.Empty(t, server.recorded())
}

func TestUpdateTopic_RejectsInvalidArgumentsWithoutWriting(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationUpdateTopic, resultSection())
	})

	streamId := numericIdentifier(t, 1)
	topicId := numericIdentifier(t, 2)
	zeroReplication := uint8(0)

	err := client.UpdateTopic(context.Background(), streamId, topicId,
		"", 0, iggcon.Duration(0), 0, nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidTopicName)
	err = client.UpdateTopic(context.Background(), streamId, topicId,
		"orders", 0, iggcon.Duration(0), 0, &zeroReplication)
	assert.ErrorIs(t, err, ierror.ErrInvalidReplicationFactor)
	assert.Empty(t, server.recorded())
}

func TestUpdateTopic_AcceptsACommittedUpdate(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationUpdateTopic, resultSection())
	})

	assert.NoError(t, client.UpdateTopic(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 2),
		"renamed", 0, iggcon.Duration(0), 0, nil))
}

func TestGetTopics_DecodesTheRoster(t *testing.T) {
	client, serverConn := newPipeClient(t)
	body := binary.LittleEndian.AppendUint32(nil, 2)
	body = append(body, topicDetailsBody(t, 3)...)
	body = append(body, topicDetailsBody(t, 5)...)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, body)
	})

	topics, err := client.GetTopics(context.Background(), numericIdentifier(t, 1))
	require.NoError(t, err)
	require.Len(t, topics, 2)
	assert.Equal(t, uint32(3), topics[0].PartitionsCount)
	assert.Equal(t, uint32(5), topics[1].PartitionsCount)
}

func TestGetTopic_ReportsAMissingTopicAsATypedError(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	_, err := client.GetTopic(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 2))
	assert.ErrorIs(t, err, ierror.ErrTopicIdNotFound)
}
