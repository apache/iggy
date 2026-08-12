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
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func numericIdentifier(t *testing.T, id uint32) iggcon.Identifier {
	t.Helper()
	identifier, err := iggcon.NewIdentifier(id)
	require.NoError(t, err)
	return identifier
}

func TestExchange_RejectsANilContext(t *testing.T) {
	client, _ := newPipeClient(t)

	_, err := client.SendBinaryRequest(nil, uint32(command.PingCode), nil) //nolint:staticcheck
	assert.ErrorIs(t, err, ierror.ErrNilContext)
}

func TestExchange_ReportsAContextThatIsAlreadyDone(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, expiredCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expiredCancel()

	tests := []struct {
		name string
		ctx  context.Context
		want error
	}{
		{name: "cancelled", ctx: cancelled, want: context.Canceled},
		{name: "expired", ctx: expired, want: context.DeadlineExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, _ := newPipeClient(t)
			_, err := client.SendBinaryRequest(test.ctx, uint32(command.PingCode), nil)
			assert.ErrorIs(t, err, test.want)
		})
	}
}

func TestExchange_RefusesToSendWhileNotConnected(t *testing.T) {
	tests := []struct {
		name  string
		state iggcon.TransportState
		want  error
	}{
		{name: "shutdown", state: iggcon.TransportStateShutdown, want: ierror.ErrClientShutdown},
		{name: "disconnected", state: iggcon.TransportStateDisconnected, want: ierror.ErrNotConnected},
		{name: "connecting", state: iggcon.TransportStateConnecting, want: ierror.ErrNotConnected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, _ := newPipeClient(t)
			client.transportState = test.state
			client.config.reconnection.enabled = false

			_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
			assert.ErrorIs(t, err, test.want)
		})
	}
}

func TestExchange_SendsAFrameTheServerCanRead(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, []byte{1, 2, 3})
	})

	response, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3}, response)

	recorded := server.recorded()
	require.Len(t, recorded, 1)
	assert.Equal(t, vsr.OperationNonReplicated, recorded[0].operation())
	assert.Equal(t, uint32(command.PingCode), recorded[0].code())
	assert.Equal(t, uint64(100), recorded[0].sessionID())
	assert.False(t, recorded[0].clientID().IsZero())
}

func TestExchange_ForwardsAVendorCodeInTheReservedField(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	_, err := client.SendBinaryRequest(context.Background(), 60000, []byte{7, 7})
	require.NoError(t, err)

	recorded := server.recorded()
	require.Len(t, recorded, 1)
	assert.Equal(t, uint32(60000), recorded[0].code())
	assert.Equal(t, []byte{7, 7}, recorded[0].payload)
}

func TestExchange_KeepsRepeatedVendorCodesFromGappingMetadataRequestIDs(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
		if read.operation() == vsr.OperationCreateStream {
			return replyFrame(vsr.OperationCreateStream, resultSection())
		}
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	for range 3 {
		_, err := client.SendBinaryRequest(context.Background(), 60000, nil)
		require.NoError(t, err)
	}
	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	require.NoError(t, err)

	recorded := server.recorded()
	require.Len(t, recorded, 4)
	for index := range 3 {
		assert.Equal(t, uint64(1), recorded[index].requestID(),
			"a non-replicated request reads the watermark without consuming it")
	}
	assert.Equal(t, uint64(1), recorded[3].requestID(),
		"the metadata request still takes the first id")
}

func TestSendBinaryRequest_RejectsSessionControlCodesWithoutWriting(t *testing.T) {
	codes := []command.Code{
		command.LoginUserCode,
		command.LogoutUserCode,
		command.LoginRegisterCode,
		command.LoginWithAccessTokenCode,
		command.LoginRegisterWithPATCode,
	}
	for _, code := range codes {
		client, serverConn := newPipeClient(t)
		server := serve(serverConn, func(_ int, _ request) []byte {
			return replyFrame(vsr.OperationNonReplicated, nil)
		})

		_, err := client.SendBinaryRequest(context.Background(), uint32(code), nil)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand, "code %d", code)
		assert.Empty(t, server.recorded(), "code %d reached the wire", code)
	}
}

func TestExchange_StripsTheResultSectionOfAMetadataReply(t *testing.T) {
	client, serverConn := newPipeClient(t)
	payload := []byte{9, 8, 7}
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateStream, append(resultSection(), payload...))
	})

	response, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	require.NoError(t, err)
	assert.Equal(t, payload, response)
}

func TestExchange_SurfacesACommittedRejection(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateStream,
			resultSection(uint32(ierror.StreamNameAlreadyExistsCode)))
	})

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	assert.ErrorIs(t, err, ierror.FromCode(ierror.StreamNameAlreadyExistsCode))
}

func TestExchange_SurfacesAHeaderStatusAndIgnoresTheBody(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		// A denial ships no body, so a decoder that read one would misparse.
		return statusReplyFrame(vsr.OperationCreateStream,
			uint32(ierror.UnauthorizedCode), []byte{0xFF, 0xFF, 0xFF, 0xFF})
	})

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	assert.ErrorIs(t, err, ierror.ErrUnauthorized)
}

func TestExchange_ReplaysTheIdenticalFrameWhileTheServerAnswersNotCommitted(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(index int, _ request) []byte {
		if index < 2 {
			return statusReplyFrame(vsr.OperationCreateStream,
				uint32(ierror.TransientNotCommittedCode), nil)
		}
		return replyFrame(vsr.OperationCreateStream, resultSection())
	})

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	require.NoError(t, err)

	recorded := server.recorded()
	require.Len(t, recorded, 3)
	assert.Equal(t, recorded[0].header, recorded[1].header,
		"a replay must carry the same client and request id for the server to deduplicate it")
	assert.Equal(t, recorded[0].header, recorded[2].header)
	assert.Equal(t, recorded[0].payload, recorded[2].payload)
}

func TestExchange_GivesUpOnNotCommittedWhenTheBudgetExpires(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return statusReplyFrame(vsr.OperationCreateStream,
			uint32(ierror.TransientNotCommittedCode), nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := client.do(ctx, &command.CreateStream{Name: "orders"})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestExchange_EscalatesNotAcceptedToALeaderRecheck(t *testing.T) {
	client, serverConn := newPipeClient(t)
	// A single-node roster short-circuits the redirect, so the request keeps
	// replaying on this connection until the caller's budget runs out.
	server := serve(serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.GetClusterMetadataCode) {
			return clusterMetadataFrame(t, 0, "127.0.0.1:8090")
		}
		return statusReplyFrame(vsr.OperationCreateStream,
			uint32(ierror.TransientNotAcceptedCode), nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client.config.reconnection.enabled = false

	_, err := client.do(ctx, &command.CreateStream{Name: "orders"})
	require.Error(t, err)

	var sawMetadata bool
	for _, recorded := range server.recorded() {
		if recorded.code() == uint32(command.GetClusterMetadataCode) {
			sawMetadata = true
		}
	}
	assert.True(t, sawMetadata, "the client re-checked leadership")
}

func TestExchange_ResetsTheSessionOnAnEviction(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	before := client.session.ClientID()
	serve(serverConn, func(_ int, _ request) []byte {
		return evictionFrame(vsr.EvictionStaleClient, 0, 0)
	})

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	assert.ErrorIs(t, err, ierror.ErrStaleClient)

	var eviction *vsr.EvictionError
	assert.ErrorAs(t, err, &eviction)
	assert.False(t, client.session.Bound(), "the fence no longer holds")
	assert.NotEqual(t, before, client.session.ClientID(), "a new identity is minted")
	assert.Equal(t, iggcon.SessionStateUnauthenticated, client.sessionState)
}

func TestExchange_MapsEveryEvictionReasonThatReachesTheCaller(t *testing.T) {
	tests := []struct {
		reason vsr.EvictionReason
		want   error
	}{
		{reason: vsr.EvictionInvalidCredentials, want: ierror.ErrInvalidCredentials},
		{reason: vsr.EvictionInvalidToken, want: ierror.ErrInvalidPersonalAccessToken},
		{reason: vsr.EvictionMalformedLogin, want: ierror.ErrInvalidFormat},
		{reason: vsr.EvictionInvalidRequestBody, want: ierror.ErrInvalidCommand},
	}
	for _, test := range tests {
		client, serverConn := newPipeClient(t)
		client.config.reconnection.enabled = false
		serve(serverConn, func(_ int, _ request) []byte {
			return evictionFrame(test.reason, 0, 0)
		})

		_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
		assert.ErrorIs(t, err, test.want, "reason %d", test.reason)
	}
}

func TestExchange_InvalidatesTheConnectionOnAFrameShorterThanItsHeader(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	serve(serverConn, func(_ int, _ request) []byte {
		frame := replyFrame(vsr.OperationNonReplicated, nil)
		binary.LittleEndian.PutUint32(frame[frameOffsetSize:], vsr.HeaderSize-1)
		return frame
	})

	_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState,
		"the stream is at an unknown boundary, so the connection is dropped")
}

func TestExchange_RejectsAFrameAboveTheTransportLimitWithoutReadingItsBody(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	serve(serverConn, func(_ int, _ request) []byte {
		frame := replyFrame(vsr.OperationNonReplicated, nil)
		binary.LittleEndian.PutUint32(frame[frameOffsetSize:], vsr.MaxFrameSize+1)
		return frame
	})

	_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
}

func TestExchange_InvalidatesTheConnectionWhenTheReplyIsCutShort(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	serve(serverConn, func(_ int, _ request) []byte {
		_ = serverConn.Close()
		return nil
	})

	_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
	assert.ErrorIs(t, err, ierror.ErrDisconnected)
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
}

func TestExchange_InvalidatesTheConnectionWhenTheContextIsCancelledMidRead(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	ctx, cancel := context.WithCancel(context.Background())
	serve(serverConn, func(_ int, _ request) []byte {
		cancel()
		return nil
	})

	_, err := client.SendBinaryRequest(ctx, uint32(command.PingCode), nil)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
}

func TestSendMessages_DecodesTheConfirmations(t *testing.T) {
	client, serverConn := newPipeClient(t)
	confirmations := binary.LittleEndian.AppendUint32(nil, 1)
	confirmations = binary.LittleEndian.AppendUint32(confirmations, 3)
	confirmations = binary.LittleEndian.AppendUint32(confirmations, 2)
	confirmations = binary.LittleEndian.AppendUint32(confirmations, 1)
	confirmations = binary.LittleEndian.AppendUint64(confirmations, 42)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationSendMessages, confirmations)
	})

	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	response, err := client.SendMessages(context.Background(),
		numericIdentifier(t, 3), numericIdentifier(t, 2),
		iggcon.PartitionId(1), []iggcon.IggyMessage{message})
	require.NoError(t, err)

	assert.Equal(t, []iggcon.SendMessagesConfirmation{
		{StreamId: 3, TopicId: 2, PartitionId: 1, BaseOffset: 42},
	}, response.Confirmations)

	recorded := server.recorded()
	require.Len(t, recorded, 1)
	assert.Equal(t, vsr.OperationSendMessages, recorded[0].operation())
	assert.Equal(t, uint64(3)<<32|uint64(2)<<20|1, recorded[0].namespace())
	assert.Equal(t, uint64(1), recorded[0].requestID(),
		"a partition request reads the watermark without consuming it")
}

func TestSendMessages_RejectsAnEmptyReplyBody(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	serve(serverConn, func(_ int, _ request) []byte {
		// The server answers a replicated request on a dead session with an
		// empty status-0 reply. Nothing was written, so reading it as a
		// successful send would silently drop the batch.
		return replyFrame(vsr.OperationSendMessages, nil)
	})

	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	_, err = client.SendMessages(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 1),
		iggcon.PartitionId(0), []iggcon.IggyMessage{message})
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestSendMessages_AcceptsAZeroCountConfirmationBody(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationSendMessages, zeroConfirmations())
	})

	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	response, err := client.SendMessages(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 1),
		iggcon.PartitionId(0), []iggcon.IggyMessage{message})
	require.NoError(t, err)
	assert.Empty(t, response.Confirmations)
}

// zeroConfirmations builds a confirmation section with a zero entry count.
func zeroConfirmations() []byte {
	return binary.LittleEndian.AppendUint32(nil, 0)
}

func TestSendMessages_DegradesAnUnreadableConfirmationBody(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		// The batch already committed, so a decode failure must not surface as
		// an error a caller would retry into a duplicate write.
		return replyFrame(vsr.OperationSendMessages, []byte{1, 0, 0, 0, 0xFF})
	})

	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	response, err := client.SendMessages(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 1),
		iggcon.PartitionId(0), []iggcon.IggyMessage{message})
	require.NoError(t, err)
	assert.Empty(t, response.Confirmations)
}

func TestSendMessages_ResolvesKeyPartitioningToAnExplicitPartition(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
		if read.operation() == vsr.OperationSendMessages {
			return replyFrame(vsr.OperationSendMessages, zeroConfirmations())
		}
		return replyFrame(vsr.OperationNonReplicated, topicDetailsBody(t, 4))
	})

	key, err := iggcon.EntityIdString("order-key-1")
	require.NoError(t, err)
	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)

	_, err = client.SendMessages(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 1), key, []iggcon.IggyMessage{message})
	require.NoError(t, err)

	recorded := server.recorded()
	require.Len(t, recorded, 2)
	assert.Equal(t, uint32(command.GetTopicCode), recorded[0].code())
	// 0x0D3FE0E1 modulo four partitions.
	assert.Equal(t, uint64(1)<<32|uint64(1)<<20|1, recorded[1].namespace())
}

func TestSendMessages_RoundRobinsBalancedPartitioning(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
		if read.operation() == vsr.OperationSendMessages {
			return replyFrame(vsr.OperationSendMessages, zeroConfirmations())
		}
		return replyFrame(vsr.OperationNonReplicated, topicDetailsBody(t, 3))
	})

	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	for range 4 {
		_, err = client.SendMessages(context.Background(),
			numericIdentifier(t, 1), numericIdentifier(t, 1),
			iggcon.None(), []iggcon.IggyMessage{message})
		require.NoError(t, err)
	}

	var partitions []uint64
	for _, recorded := range server.recorded() {
		if recorded.operation() == vsr.OperationSendMessages {
			partitions = append(partitions, recorded.namespace()&0xFFFFF)
		}
	}
	assert.Equal(t, []uint64{0, 1, 2, 0}, partitions)

	metadataRequests := 0
	for _, recorded := range server.recorded() {
		if recorded.code() == uint32(command.GetTopicCode) {
			metadataRequests++
		}
	}
	assert.Equal(t, 1, metadataRequests, "the partition count is cached after the first read")
}

func TestSendMessages_RejectsAnEmptyBatch(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationSendMessages, nil)
	})

	_, err := client.SendMessages(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 1), iggcon.PartitionId(0), nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidMessagesCount)
	assert.Empty(t, server.recorded())
}

func TestCanReplay_RefusesOnlyReplicatedRequestsWithAnUnknownOutcome(t *testing.T) {
	tests := []struct {
		name string
		code uint32
		err  error
		want bool
	}{
		{name: "register replays by design",
			code: uint32(command.LoginRegisterCode), err: ierror.ErrDisconnected, want: true},
		{name: "non-replicated read replays",
			code: uint32(command.PingCode), err: ierror.ErrDisconnected, want: true},
		{name: "replicated write never sent replays",
			code: uint32(command.CreateStreamCode), err: ierror.ErrNotConnected, want: true},
		{name: "replicated write refused by the server replays",
			code: uint32(command.CreateStreamCode), err: ierror.ErrUnauthenticated, want: true},
		{name: "replicated write on a stale client replays",
			code: uint32(command.CreateStreamCode), err: ierror.ErrStaleClient, want: true},
		{name: "replicated write with a lost reply is refused",
			code: uint32(command.CreateStreamCode), err: ierror.ErrDisconnected, want: false},
		{name: "send with a lost reply is refused",
			code: uint32(command.SendMessagesCode), err: ierror.ErrDisconnected, want: false},
		{name: "token mint with a lost reply is refused",
			code: uint32(command.CreateAccessTokenCode), err: ierror.ErrDisconnected, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, canReplay(test.code, test.err))
		})
	}
}

// topicDetailsBody builds the reply body of GetTopic for a topic with the
// given partition count and no partitions listed.
func topicDetailsBody(t *testing.T, partitionsCount uint32) []byte {
	t.Helper()

	const nameOffset = 50
	name := "orders"
	body := make([]byte, nameOffset+1+len(name))
	binary.LittleEndian.PutUint32(body[0:4], 1)
	binary.LittleEndian.PutUint32(body[12:16], partitionsCount)
	body[nameOffset] = byte(len(name))
	copy(body[nameOffset+1:], name)
	return body
}
