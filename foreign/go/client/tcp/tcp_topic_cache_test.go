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
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicCache_DropStreamForgetsEveryTopicUnderTheStream(t *testing.T) {
	cache := topicCache{}
	doomed := topicKey{stream: "doomed", topic: "orders"}
	doomedSibling := topicKey{stream: "doomed", topic: "billing"}
	survivor := topicKey{stream: "kept", topic: "orders"}

	for _, key := range []topicKey{doomed, doomedSibling, survivor} {
		cache.setPartitionsCount(key, 4)
		cache.nextBalanced(key, 4)
	}

	cache.dropStream("doomed")

	_, ok := cache.partitionsCount(doomed)
	assert.False(t, ok)
	_, ok = cache.partitionsCount(doomedSibling)
	assert.False(t, ok)
	count, ok := cache.partitionsCount(survivor)
	assert.True(t, ok, "topics of other streams stay cached")
	assert.Equal(t, uint32(4), count)
	assert.Equal(t, uint32(1), cache.nextBalanced(survivor, 4),
		"the surviving cursor keeps its position")
	assert.Equal(t, uint32(0), cache.nextBalanced(doomed, 4),
		"the dropped cursor restarts")
}

func TestDeleteStream_DropsTheCachedTopicsOfTheStream(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
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
		if read.code() == uint32(command.GetTopicCode) {
			metadataReads++
		}
	}
	assert.Equal(t, 2, metadataReads,
		"a send after the stream delete rereads the topic instead of trusting the dead cache")
}
