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
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicCache_DropStreamRestartsItsCursorsAndForgetsEveryCount(t *testing.T) {
	cache := topicCache{}
	doomed := topicKey{stream: "doomed", topic: "orders"}
	doomedSibling := topicKey{stream: "doomed", topic: "billing"}
	survivor := topicKey{stream: "kept", topic: "orders"}
	keys := []topicKey{doomed, doomedSibling, survivor}

	for _, key := range keys {
		cache.setPartitionsCount(key, 4)
		cache.nextBalanced(key, 4)
	}

	cache.dropStream("doomed")

	for _, key := range keys {
		_, ok := cache.partitionsCount(key)
		assert.False(t, ok, "the key of another stream may alias the deleted one: %+v", key)
	}
	assert.Equal(t, uint32(1), cache.nextBalanced(survivor, 4),
		"the surviving cursor keeps its position")
	assert.Equal(t, uint32(0), cache.nextBalanced(doomed, 4),
		"the dropped cursor restarts")
}

func TestTopologyChanges_DropTheCachedDiscoveryOfEveryAlias(t *testing.T) {
	ctx := context.Background()
	streamId := numericIdentifier(t, 1)
	topicId := numericIdentifier(t, 2)
	streamName, err := iggcon.NewIdentifier("orders")
	require.NoError(t, err)
	topicName, err := iggcon.NewIdentifier("eu")
	require.NoError(t, err)
	// One topic, cached once under its numeric ids and once under its names.
	aliases := []topicKey{newTopicKey(streamId, topicId), newTopicKey(streamName, topicName)}
	changes := []struct {
		name      string
		operation vsr.Operation
		apply     func(client *IggyTcpClient) error
	}{
		{name: "stream delete", operation: vsr.OperationDeleteStream,
			apply: func(client *IggyTcpClient) error { return client.DeleteStream(ctx, streamId) }},
		{name: "topic delete", operation: vsr.OperationDeleteTopic,
			apply: func(client *IggyTcpClient) error { return client.DeleteTopic(ctx, streamId, topicId) }},
		{name: "partition create", operation: vsr.OperationCreatePartitions,
			apply: func(client *IggyTcpClient) error { return client.CreatePartitions(ctx, streamId, topicId, 1) }},
		{name: "partition delete", operation: vsr.OperationDeletePartitions,
			apply: func(client *IggyTcpClient) error { return client.DeletePartitions(ctx, streamId, topicId, 1) }},
	}
	for _, change := range changes {
		t.Run(change.name, func(t *testing.T) {
			client, serverConn := newPipeClient(t)
			serve(serverConn, func(_ int, _ request) []byte {
				return replyFrame(change.operation, resultSection())
			})
			for _, key := range aliases {
				client.topics.setPartitionsCount(key, 3)
				client.polls.routes = map[string]pollRoute{"cached": {}}
			}

			require.NoError(t, change.apply(client))

			for _, key := range aliases {
				_, cached := client.topics.partitionsCount(key)
				assert.False(t, cached, "the count cached under %+v", key)
				assert.Empty(t, client.polls.routes)
			}
		})
	}
}

func TestDeleteStream_DropsTheCachedTopicsOfTheStream(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		switch {
		case read.operation() == vsr.OperationSendMessages:
			return replyFrame(vsr.OperationSendMessages, zeroConfirmations())
		case read.operation() == vsr.OperationDeleteStream:
			return replyFrame(vsr.OperationDeleteStream, resultSection())
		default:
			return replyFrame(vsr.OperationNonReplicated, topicDetailsBody(t, 2))
		}
	})

	streamId := numericIdentifier(t, 1)
	topicId := numericIdentifier(t, 1)
	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)

	balancedSend := func() {
		_, err := client.SendMessages(context.Background(),
			streamId, topicId, iggcon.None(), []iggcon.IggyMessage{message})
		require.NoError(t, err)
	}
	balancedSend()
	require.NoError(t, client.DeleteStream(context.Background(), streamId))
	balancedSend()

	metadataReads := 0
	for _, read := range server.recorded() {
		if read.code() == uint32(command.GetPartitionRouteCode) {
			metadataReads++
		}
	}
	assert.Equal(t, 2, metadataReads,
		"a send after the stream delete rereads the topic instead of trusting the dead cache")
}

func TestBalancedSendRetriesThePartitionCountRouteWithoutChangingCoordinator(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.metadataWatermark.Store(42)
	routes := 0
	server := serve(serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.GetPartitionRouteCode) {
			routes++
			assert.Equal(t, uint64(42), stampedContext(t, read).MetadataOp)
			if routes == 1 {
				return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.TransientNotAcceptedCode), nil)
			}
			return pollRoutingReply(t, read, 0, 42)
		}
		return replyFrame(vsr.OperationSendMessages, zeroConfirmations())
	})
	stream, topic, _ := groupConsumer(t)
	message, err := iggcon.NewIggyMessage([]byte("balanced"))
	require.NoError(t, err)

	_, err = client.SendMessages(context.Background(), stream, topic, iggcon.None(), []iggcon.IggyMessage{message})

	require.NoError(t, err)
	reads := server.recorded()
	require.Len(t, reads, 3)
	assert.Equal(t, 2, requestCount(reads, command.GetPartitionRouteCode))
	assert.Equal(t, uint32(0), reads[2].partitionID(t))
	assert.Equal(t, reads[0].clientID(), reads[2].clientID())
}

func TestPartitionRouteRefreshReplacesTheCachedPartitionCount(t *testing.T) {
	client, serverConn := newPipeClient(t)
	routes, polls := 0, 0
	serve(serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.GetPartitionRouteCode) {
			routes++
			answer := pollRoutingReply(t, read, 0, 1)
			binary.LittleEndian.PutUint32(answer[vsr.HeaderSize+20:], uint32(routes+1))
			return answer
		}
		polls++
		if polls == 2 {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.NotPrimaryCode), nil)
		}
		return replyFrame(vsr.OperationNonReplicated, emptyBatchBody(0))
	})
	stream, topic, group := groupConsumer(t)
	consumer := iggcon.NewSingleConsumer(group.Id)
	for _, expected := range []uint32{2, 3} {
		_, err := client.PollMessages(context.Background(), stream, topic, consumer,
			iggcon.NextPollingStrategy(), 1, false, nil)
		require.NoError(t, err)
		count, exists := client.topics.partitionsCount(newTopicKey(stream, topic))
		require.True(t, exists)
		assert.Equal(t, expected, count)
	}
}
