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
	"sync/atomic"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// streamDetailsBody builds a stream reply with no topics:
// [id u32][created_at u64][topics u32][size u64][messages u64][name_len u8][name]
// [options_len u32 = 0][derived_options_len u32 = 0].
func streamDetailsBody(id uint32, name string) []byte {
	body := binary.LittleEndian.AppendUint32(nil, id)
	body = binary.LittleEndian.AppendUint64(body, 1700000000)
	body = binary.LittleEndian.AppendUint32(body, 0)
	body = binary.LittleEndian.AppendUint64(body, 0)
	body = binary.LittleEndian.AppendUint64(body, 0)
	body = append(body, byte(len(name)))
	body = append(body, name...)
	body = binary.LittleEndian.AppendUint32(body, 0)
	return binary.LittleEndian.AppendUint32(body, 0)
}

func TestCreateStream_DecodesTheCreatedStream(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateStream,
			append(resultSection(), streamDetailsBody(9, "orders")...))
	})

	stream, err := client.CreateStream(context.Background(), "orders", nil)
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
		_, err := client.CreateStream(context.Background(), name, nil)
		assert.ErrorIs(t, err, ierror.ErrInvalidStreamName, "name %q", name)
	}
	assert.Empty(t, server.recorded())
}

func TestCreateStream_RejectsAnInvalidKeyWithoutWriting(t *testing.T) {
	t.Run("given_invalid_key_when_encoding_should_reject_before_network_io", func(t *testing.T) {
		client, serverConn := newPipeClient(t)
		server := serve(serverConn, func(_ int, _ request) []byte {
			return replyFrame(vsr.OperationCreateStream, resultSection())
		})

		for _, key := range [][]byte{{}, make([]byte, 31), make([]byte, 33)} {
			_, err := client.CreateStream(context.Background(), "orders", &iggcon.StreamOptions{EncryptionKey: key})
			assert.ErrorIs(t, err, ierror.ErrInvalidEncryptionKey, "%d bytes", len(key))
		}
		assert.Empty(t, server.recorded())
	})
}

func TestGetStream_ReportsAMissingStreamAsATypedError(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	_, err := client.GetStream(context.Background(), numericIdentifier(t, 42))
	assert.ErrorIs(t, err, ierror.ErrStreamIdNotFound)
}

// pageBody builds a list reply: [next_len u8][next][count u32][items]. A nil
// next marks the last page.
func pageBody(next []byte, items ...[]byte) []byte {
	body := append([]byte{byte(len(next))}, next...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(items)))
	for _, item := range items {
		body = append(body, item...)
	}
	return body
}

func TestGetStreams_WalksEveryPage(t *testing.T) {
	client, serverConn := newPipeClient(t)
	cursorBytes := []byte{1, 2, 0, 0, 0, 0, 0, 0, 0}
	pages := [][]byte{
		pageBody(cursorBytes, streamDetailsBody(1, "orders"), streamDetailsBody(2, "billing")),
		pageBody(nil, streamDetailsBody(3, "audit")),
	}
	server := serve(serverConn, func(index int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, pages[index])
	})

	streams, err := client.GetStreams(context.Background())
	require.NoError(t, err)
	names := make([]string, 0, len(streams))
	for _, stream := range streams {
		names = append(names, stream.Name)
	}
	assert.Equal(t, []string{"orders", "billing", "audit"}, names)

	query := iggcon.PageQuery{Order: iggcon.PageOrderID, Limit: iggcon.MaxPageLimit}
	firstPage, err := query.AppendBinary(nil)
	require.NoError(t, err)
	var cursor iggcon.PageCursor
	require.NoError(t, cursor.UnmarshalBinary(cursorBytes))
	query.Cursor = &cursor
	secondPage, err := query.AppendBinary(nil)
	require.NoError(t, err)
	requests := server.recorded()
	require.Len(t, requests, len(pages))
	assert.Equal(t, firstPage, requests[0].payload)
	assert.Equal(t, secondPage, requests[1].payload, "the second request carries the next of the first page")
}

func TestGetStreams_FailsWhenTheServerRepeatsTheCursor(t *testing.T) {
	client, serverConn := newPipeClient(t)
	cursorBytes := []byte{1, 2, 0, 0, 0, 0, 0, 0, 0}
	server := serve(serverConn, func(index int, _ request) []byte {
		// A last page ends the walk of a broken guard, so the test fails and
		// does not hang.
		if index >= 2 {
			return replyFrame(vsr.OperationNonReplicated, pageBody(nil))
		}
		return replyFrame(vsr.OperationNonReplicated, pageBody(cursorBytes, streamDetailsBody(1, "orders")))
	})

	_, err := client.GetStreams(context.Background())
	assert.ErrorIs(t, err, ierror.ErrInvalidFormat)
	assert.Len(t, server.recorded(), 2, "the walk stops at the page that repeats the cursor it was sent")
}

// A walk is one call, so a caller sign-in between two pages ends it with 40.
// The test drives the walk helper, because only its page callback can sign in
// exactly between two pages.
func TestReadWholeList_ACallerSignInBetweenPagesEndsTheWalk(t *testing.T) {
	cursorBytes := []byte{1, 2, 0, 0, 0, 0, 0, 0, 0}
	var listed atomic.Int32
	var server *testListener
	node := singleNodeHandler(t, func() string { return server.address() })
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		if read.code() != uint32(command.GetStreamsCode) {
			return node(connection, index, read)
		}
		if listed.Add(1) == 1 {
			return replyFrame(vsr.OperationNonReplicated, pageBody(cursorBytes, streamDetailsBody(1, "orders")))
		}
		return replyFrame(vsr.OperationNonReplicated, pageBody(nil, streamDetailsBody(2, "billing")))
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))

	streams, err := readWholeList(ctx, client, func(walk context.Context, query iggcon.PageQuery) (*iggcon.Page[iggcon.Stream], error) {
		if query.Cursor != nil {
			_, err := client.LoginUser(ctx, "other", "password")
			require.NoError(t, err)
		}
		return client.GetStreamsPage(walk, iggcon.NamedPageQuery{PageQuery: query})
	})
	require.ErrorIs(t, err, ierror.ErrUnauthenticated)
	assert.Nil(t, streams, "the walk returns no partial list")
	assert.Equal(t, 1, requestCount(server.recorded(), command.GetStreamsCode), "no page went out under the new sign-in")

	_, err = client.GetStreamsPage(ctx, iggcon.NamedPageQuery{PageQuery: iggcon.NewPageQuery()})
	require.NoError(t, err, "a page call after the sign-in is a call of the new sign-in")
}

func TestGetStreamsPage_RefusesAnInvalidQueryWithoutWriting(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, pageBody(nil))
	})

	_, err := client.GetStreamsPage(context.Background(), iggcon.NamedPageQuery{})
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	assert.Empty(t, server.recorded())
}

func TestUpdateStream_RejectsAnInvalidNameWithoutWriting(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationUpdateStreamResolved, resultSection())
	})

	for _, name := range []string{"", strings.Repeat("x", MaxStringLength+1)} {
		err := client.UpdateStream(context.Background(), numericIdentifier(t, 1), iggcon.StreamUpdate{Name: &name})
		assert.ErrorIs(t, err, ierror.ErrInvalidStreamName, "name %q", name)
	}
	err := client.UpdateStream(context.Background(), numericIdentifier(t, 1),
		iggcon.StreamUpdate{Reset: []iggcon.StreamOptionKey{iggcon.StreamOptionName}})
	assert.ErrorIs(t, err, ierror.ErrOptionNotResettable)
	assert.Empty(t, server.recorded())
}

func TestUpdateStream_AcceptsACommittedRename(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
		reply := committedReply(read, 0)
		reply[replyFrameOffsetOperation] = byte(vsr.OperationUpdateStreamResolved)
		return reply
	})

	name := "renamed"
	assert.NoError(t, client.UpdateStream(
		context.Background(), numericIdentifier(t, 1), iggcon.StreamUpdate{Name: &name}))
	assert.Len(t, requestsFor(server.recorded(), vsr.OperationUpdateResource), 1)
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
