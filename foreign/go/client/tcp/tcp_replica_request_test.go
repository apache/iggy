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
	"net"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	firstReplicaAddress  = "127.0.0.1:19101"
	secondReplicaAddress = "127.0.0.1:19102"
)

func TestSendBinaryRequestToReplica_SendsOnlyToTheChosenReplicaWithTheMetadataWatermark(t *testing.T) {
	client, first, second := newTwoReplicaClient(t)
	client.metadataWatermark.Store(42)
	firstServer := serve(first, func(_ int, _ request) []byte { return replyFrame(vsr.OperationNonReplicated, nil) })
	secondServer := serve(second, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, []byte("replica-1"))
	})

	response, err := client.SendBinaryRequestToReplica(context.Background(), uint32(command.GetStatsCode), nil, 1)

	require.NoError(t, err)
	assert.Equal(t, []byte("replica-1"), response)
	assert.Equal(t, 0, requestCount(firstServer.recorded(), command.GetStatsCode))
	reads := secondServer.recorded()
	require.Equal(t, 1, requestCount(reads, command.GetStatsCode))
	assert.Equal(t, uint64(42), binary.LittleEndian.Uint64(reads[0].header[frameOffsetContext+16:]))
}

func TestSendBinaryRequestToReplica_ReturnsNotPrimaryWithoutTryingAnotherReplica(t *testing.T) {
	client, first, second := newTwoReplicaClient(t)
	firstServer := serve(first, func(_ int, _ request) []byte { return replyFrame(vsr.OperationNonReplicated, nil) })
	secondServer := serve(second, func(_ int, _ request) []byte {
		return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.NotPrimaryCode), nil)
	})

	_, err := client.SendBinaryRequestToReplica(context.Background(), uint32(command.GetStatsCode), nil, 1)

	require.ErrorIs(t, err, ierror.ErrNotPrimary)
	assert.Equal(t, 1, requestCount(secondServer.recorded(), command.GetStatsCode))
	assert.Equal(t, 0, requestCount(firstServer.recorded(), command.GetStatsCode))
}

func TestSendBinaryRequestToReplica_RetriesATransientRefusalOnTheSameReplica(t *testing.T) {
	client, first, second := newTwoReplicaClient(t)
	firstServer := serve(first, func(_ int, _ request) []byte { return replyFrame(vsr.OperationNonReplicated, nil) })
	secondServer := serve(second, func(index int, _ request) []byte {
		if index == 0 {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.TransientNotAcceptedCode), nil)
		}
		return replyFrame(vsr.OperationNonReplicated, []byte("served"))
	})

	response, err := client.SendBinaryRequestToReplica(context.Background(), uint32(command.GetStatsCode), nil, 1)

	require.NoError(t, err)
	assert.Equal(t, []byte("served"), response)
	assert.Equal(t, 2, requestCount(secondServer.recorded(), command.GetStatsCode))
	assert.Equal(t, 0, requestCount(firstServer.recorded(), command.GetStatsCode))
}

func TestSendBinaryRequestToReplica_FailsAReplicaTheRosterDoesNotNameAfterOneRosterRead(t *testing.T) {
	client, first, second := newTwoReplicaClient(t)
	roster := func(_ int, read request) []byte {
		if read.code() == uint32(command.GetClusterMetadataCode) {
			return clusterMetadataFrame(t, 0, firstReplicaAddress, secondReplicaAddress)
		}
		return replyFrame(vsr.OperationNonReplicated, nil)
	}
	firstServer := serve(first, roster)
	secondServer := serve(second, roster)

	_, err := client.SendBinaryRequestToReplica(context.Background(), uint32(command.GetStatsCode), nil, 9)

	require.ErrorIs(t, err, ierror.ErrInvalidIdentifier)
	reads := append(firstServer.recorded(), secondServer.recorded()...)
	assert.Equal(t, 1, requestCount(reads, command.GetClusterMetadataCode))
	assert.Equal(t, 0, requestCount(reads, command.GetStatsCode))
}

func TestSendBinaryRequestToReplica_FailsAnUnknownReplicaRightAfterTheRosterRead(t *testing.T) {
	client, first, second := newTwoReplicaClient(t)
	roster := func(_ int, read request) []byte {
		if read.code() == uint32(command.GetClusterMetadataCode) {
			return clusterMetadataFrame(t, 0, firstReplicaAddress, secondReplicaAddress)
		}
		return replyFrame(vsr.OperationNonReplicated, nil)
	}
	serve(first, roster)
	serve(second, roster)
	const calls = 40

	started := time.Now()
	for range calls {
		_, err := client.SendBinaryRequestToReplica(context.Background(), uint32(command.GetStatsCode), nil, 9)
		require.ErrorIs(t, err, ierror.ErrInvalidIdentifier)
	}

	// A retry pause before the answer averages half of replayInterval per call.
	assert.Less(t, time.Since(started), calls*replayInterval/5)
}

func TestSendBinaryRequestToReplica_RefusesReplicatedCommandsPartitionReadsAndSessionControlWithoutWriting(t *testing.T) {
	client, first, second := newTwoReplicaClient(t)
	firstServer := serve(first, func(_ int, _ request) []byte { return nil })
	secondServer := serve(second, func(_ int, _ request) []byte { return nil })

	for _, code := range []command.Code{
		command.CreateStreamCode,
		command.SendMessagesCode,
		command.StoreOffsetCode,
		command.PollMessagesCode,
		command.LoginUserCode,
		command.LoginRegisterCode,
	} {
		_, err := client.SendBinaryRequestToReplica(context.Background(), uint32(code), nil, 1)
		require.ErrorIs(t, err, ierror.ErrInvalidCommand, "code %d", code)
	}
	stream, topic, group := groupConsumer(t)
	partition := uint32(1)
	offsetRead, err := (&command.GetConsumerOffset{StreamId: stream, TopicId: topic,
		Consumer: iggcon.NewSingleConsumer(group.Id), PartitionId: &partition}).MarshalBinary()
	require.NoError(t, err)
	_, err = client.SendBinaryRequestToReplica(context.Background(), uint32(command.GetOffsetCode), offsetRead, 1)
	require.ErrorIs(t, err, ierror.ErrInvalidCommand)
	assert.Empty(t, firstServer.recorded())
	assert.Empty(t, secondServer.recorded())
}

// newTwoReplicaClient is a pipe client bound on two replicas, 0 the metadata
// primary and 1 a backup, at the addresses a roster names them by.
func newTwoReplicaClient(t *testing.T) (*IggyTcpClient, net.Conn, net.Conn) {
	t.Helper()
	client, first := newPipeClient(t)
	client.links.metadata().endpoint = firstReplicaAddress
	client.links.slots[0].endpoint = firstReplicaAddress
	client.links.bootstrap = nil
	second, connection := net.Pipe()
	t.Cleanup(func() { _ = second.Close() })
	link := newTCPLink(connection, secondReplicaAddress)
	link.markBound(client.sessionFence)
	client.links.slots[1] = &replicaSlot{endpoint: secondReplicaAddress, link: link, cancel: func() {}}
	client.links.rosterOrder = []uint8{0, 1}
	return client, first, second
}
