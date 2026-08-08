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

package vsr

import (
	"encoding/binary"
	"fmt"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// numericIdentifier builds the [kind u8][len u8][value u32] prefix.
func numericIdentifier(id uint32) []byte {
	out := []byte{identifierKindNumeric, 4}
	return binary.LittleEndian.AppendUint32(out, id)
}

// namedIdentifier builds the [kind u8][len u8][utf-8] prefix.
func namedIdentifier(name string) []byte {
	out := []byte{identifierKindString, byte(len(name))}
	return append(out, name...)
}

// sendMessagesPayload builds [metadata len u32][stream][topic][partitioning].
func sendMessagesPayload(stream, topic, partitioning []byte) []byte {
	metadata := make([]byte, 0, len(stream)+len(topic)+len(partitioning))
	metadata = append(metadata, stream...)
	metadata = append(metadata, topic...)
	metadata = append(metadata, partitioning...)
	payload := binary.LittleEndian.AppendUint32(nil, uint32(len(metadata)))
	return append(payload, metadata...)
}

func partitionIDPartitioning(id uint32) []byte {
	out := []byte{partitioningPartitionID, 4}
	return binary.LittleEndian.AppendUint32(out, id)
}

// consumerOffsetPayload builds the peeked prefix shared by codes 121 to 124.
func consumerOffsetPayload(stream, topic []byte, partitionID uint32, hasPartition bool) []byte {
	payload := []byte{1}
	payload = append(payload, numericIdentifier(1)...)
	payload = append(payload, stream...)
	payload = append(payload, topic...)
	if hasPartition {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	return binary.LittleEndian.AppendUint32(payload, partitionID)
}

func TestPackNamespace_PacksTheTripleAtItsShifts(t *testing.T) {
	tests := []struct {
		name                        string
		stream, topic, partitionKey uint32
		want                        uint64
	}{
		{name: "zero", want: 0},
		{name: "partition only", partitionKey: 1, want: 1},
		{name: "topic only", topic: 1, want: 1 << 20},
		{name: "stream only", stream: 1, want: 1 << 32},
		{name: "triple", stream: 3, topic: 2, partitionKey: 7, want: 3<<32 | 2<<20 | 7},
		{
			name:         "maximum",
			stream:       MaxStreams - 1,
			topic:        MaxTopics - 1,
			partitionKey: MaxPartitions - 1,
			want:         4095<<32 | 4095<<20 | 999999,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := PackNamespace(test.stream, test.topic, test.partitionKey)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestPackNamespace_RejectsOutOfRangeFields(t *testing.T) {
	tests := []struct {
		name                        string
		stream, topic, partitionKey uint32
	}{
		{name: "stream", stream: MaxStreams},
		{name: "topic", topic: MaxTopics},
		{name: "partition", partitionKey: MaxPartitions},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := PackNamespace(test.stream, test.topic, test.partitionKey)
			assert.ErrorIs(t, err, ierror.ErrInvalidIdentifier)
		})
	}
}

func TestNamespaceShifts_MatchTheWireContract(t *testing.T) {
	assert.Equal(t, 12, streamBits)
	assert.Equal(t, 12, topicBits)
	assert.Equal(t, 20, partitionBits)
	assert.Equal(t, 0, partitionShift)
	assert.Equal(t, 20, topicShift)
	assert.Equal(t, 32, streamShift)
	assert.Equal(t, uint64(1)<<63, MetadataConsensusNamespace)
}

func TestNamespaceForRequest_RoutesControlPlaneToTheMetadataSentinel(t *testing.T) {
	for _, operation := range []Operation{OperationRegister, OperationLogout} {
		got, err := NamespaceForRequest(uint32(command.LoginRegisterCode), nil, operation)
		require.NoError(t, err)
		assert.Equal(t, MetadataConsensusNamespace, got)
	}
}

func TestNamespaceForRequest_RoutesReadsAndMetadataToZero(t *testing.T) {
	tests := []struct {
		name      string
		code      command.Code
		operation Operation
	}{
		{name: "ping", code: command.PingCode, operation: OperationNonReplicated},
		{name: "poll", code: command.PollMessagesCode, operation: OperationNonReplicated},
		{name: "create stream", code: command.CreateStreamCode, operation: OperationCreateStream},
		{name: "join group", code: command.JoinGroupCode, operation: OperationJoinConsumerGroup},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NamespaceForRequest(uint32(test.code), []byte{0xFF}, test.operation)
			require.NoError(t, err)
			assert.Zero(t, got)
		})
	}
}

func TestNamespaceForRequest_RejectsAnUnpeekablePartitionOperation(t *testing.T) {
	_, err := NamespaceForRequest(uint32(command.PollMessagesCode), nil, OperationSendMessages+64)
	assert.ErrorIs(t, err, ierror.ErrFeatureUnavailable)
}

func TestNamespaceForRequest_SendMessages(t *testing.T) {
	t.Run("packs numeric identifiers", func(t *testing.T) {
		payload := sendMessagesPayload(
			numericIdentifier(3), numericIdentifier(2), partitionIDPartitioning(7))
		got, err := NamespaceForRequest(
			uint32(command.SendMessagesCode), payload, OperationSendMessages)
		require.NoError(t, err)
		assert.Equal(t, uint64(3<<32|2<<20|7), got)
	})

	t.Run("ignores message bytes past the metadata region", func(t *testing.T) {
		payload := sendMessagesPayload(
			numericIdentifier(1), numericIdentifier(1), partitionIDPartitioning(0))
		payload = append(payload, 0xDE, 0xAD, 0xBE, 0xEF)
		got, err := NamespaceForRequest(
			uint32(command.SendMessagesCode), payload, OperationSendMessages)
		require.NoError(t, err)
		assert.Equal(t, uint64(1<<32|1<<20), got)
	})

	t.Run("defers a named stream to the server", func(t *testing.T) {
		payload := sendMessagesPayload(
			namedIdentifier("orders"), numericIdentifier(2), partitionIDPartitioning(7))
		got, err := NamespaceForRequest(
			uint32(command.SendMessagesCode), payload, OperationSendMessages)
		require.NoError(t, err)
		assert.Zero(t, got)
	})

	t.Run("defers a named topic to the server", func(t *testing.T) {
		payload := sendMessagesPayload(
			numericIdentifier(3), namedIdentifier("events"), partitionIDPartitioning(7))
		got, err := NamespaceForRequest(
			uint32(command.SendMessagesCode), payload, OperationSendMessages)
		require.NoError(t, err)
		assert.Zero(t, got)
	})

	t.Run("rejects partitioning the client cannot route", func(t *testing.T) {
		for _, kind := range []byte{0, 1, 3} {
			payload := sendMessagesPayload(
				numericIdentifier(1), numericIdentifier(1), []byte{kind, 4, 0, 0, 0, 0})
			_, err := NamespaceForRequest(
				uint32(command.SendMessagesCode), payload, OperationSendMessages)
			assert.ErrorIs(t, err, ierror.ErrFeatureUnavailable, "kind %d", kind)
		}
	})

	t.Run("rejects a partition value of the wrong width", func(t *testing.T) {
		payload := sendMessagesPayload(
			numericIdentifier(1), numericIdentifier(1),
			[]byte{partitioningPartitionID, 2, 0, 0})
		_, err := NamespaceForRequest(
			uint32(command.SendMessagesCode), payload, OperationSendMessages)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	})

	t.Run("rejects an out-of-range identifier", func(t *testing.T) {
		payload := sendMessagesPayload(
			numericIdentifier(MaxStreams), numericIdentifier(1), partitionIDPartitioning(0))
		_, err := NamespaceForRequest(
			uint32(command.SendMessagesCode), payload, OperationSendMessages)
		assert.ErrorIs(t, err, ierror.ErrInvalidIdentifier)
	})

	t.Run("rejects an out-of-range partition", func(t *testing.T) {
		payload := sendMessagesPayload(
			numericIdentifier(1), numericIdentifier(1), partitionIDPartitioning(MaxPartitions))
		_, err := NamespaceForRequest(
			uint32(command.SendMessagesCode), payload, OperationSendMessages)
		assert.ErrorIs(t, err, ierror.ErrInvalidIdentifier)
	})

	t.Run("rejects every truncation", func(t *testing.T) {
		payload := sendMessagesPayload(
			numericIdentifier(1), numericIdentifier(2), partitionIDPartitioning(3))
		for length := range len(payload) {
			_, err := NamespaceForRequest(
				uint32(command.SendMessagesCode), payload[:length], OperationSendMessages)
			assert.Error(t, err, "truncated to %d bytes", length)
		}
	})

	t.Run("rejects a metadata length past the payload", func(t *testing.T) {
		payload := binary.LittleEndian.AppendUint32(nil, 1024)
		payload = append(payload, numericIdentifier(1)...)
		_, err := NamespaceForRequest(
			uint32(command.SendMessagesCode), payload, OperationSendMessages)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	})
}

func TestNamespaceForRequest_ConsumerOffsets(t *testing.T) {
	variants := []struct {
		code      command.Code
		operation Operation
	}{
		{code: command.StoreOffsetCode, operation: OperationStoreConsumerOffset},
		{code: command.DeleteConsumerOffsetCode, operation: OperationDeleteConsumerOffset},
		{code: command.StoreOffset2Code, operation: OperationStoreConsumerOffset2},
		{code: command.DeleteConsumerOffset2Code, operation: OperationDeleteConsumerOffset2},
	}

	for _, variant := range variants {
		t.Run(fmt.Sprintf("code_%d", variant.code), func(t *testing.T) {
			runConsumerOffsetNamespaceCases(t, uint32(variant.code), variant.operation)
		})
	}
}

func runConsumerOffsetNamespaceCases(t *testing.T, code uint32, operation Operation) {
	t.Helper()

	t.Run("packs the triple", func(t *testing.T) {
		payload := consumerOffsetPayload(
			numericIdentifier(5), numericIdentifier(4), 9, true)
		got, err := NamespaceForRequest(code, payload, operation)
		require.NoError(t, err)
		assert.Equal(t, uint64(5<<32|4<<20|9), got)
	})

	t.Run("ignores the trailing offset and ack bytes", func(t *testing.T) {
		payload := consumerOffsetPayload(
			numericIdentifier(5), numericIdentifier(4), 9, true)
		payload = binary.LittleEndian.AppendUint64(payload, 42)
		payload = append(payload, 1)
		got, err := NamespaceForRequest(code, payload, operation)
		require.NoError(t, err)
		assert.Equal(t, uint64(5<<32|4<<20|9), got)
	})

	t.Run("rejects a missing explicit partition", func(t *testing.T) {
		payload := consumerOffsetPayload(
			numericIdentifier(5), numericIdentifier(4), 0, false)
		_, err := NamespaceForRequest(code, payload, operation)
		assert.ErrorIs(t, err, ierror.ErrInvalidIdentifier)
	})

	t.Run("defers a named stream to the server", func(t *testing.T) {
		payload := consumerOffsetPayload(
			namedIdentifier("orders"), numericIdentifier(4), 9, true)
		got, err := NamespaceForRequest(code, payload, operation)
		require.NoError(t, err)
		assert.Zero(t, got)
	})

	t.Run("rejects an unknown consumer kind", func(t *testing.T) {
		payload := consumerOffsetPayload(
			numericIdentifier(5), numericIdentifier(4), 9, true)
		payload[0] = 3
		_, err := NamespaceForRequest(code, payload, operation)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	})

	t.Run("rejects every truncation", func(t *testing.T) {
		payload := consumerOffsetPayload(
			numericIdentifier(5), numericIdentifier(4), 9, true)
		for length := range len(payload) {
			_, err := NamespaceForRequest(code, payload[:length], operation)
			assert.Error(t, err, "truncated to %d bytes", length)
		}
	})
}

func TestNamespaceForRequest_DeleteSegments(t *testing.T) {
	t.Run("packs the triple", func(t *testing.T) {
		payload := append(numericIdentifier(6), numericIdentifier(5)...)
		payload = binary.LittleEndian.AppendUint32(payload, 4)
		got, err := NamespaceForRequest(
			uint32(command.DeleteSegmentsCode), payload, OperationDeleteSegments)
		require.NoError(t, err)
		assert.Equal(t, uint64(6<<32|5<<20|4), got)
	})

	t.Run("rejects every truncation", func(t *testing.T) {
		payload := append(numericIdentifier(6), numericIdentifier(5)...)
		payload = binary.LittleEndian.AppendUint32(payload, 4)
		for length := range len(payload) {
			_, err := NamespaceForRequest(
				uint32(command.DeleteSegmentsCode), payload[:length], OperationDeleteSegments)
			assert.Error(t, err, "truncated to %d bytes", length)
		}
	})
}

func TestPeekIdentifier_RejectsMalformedPrefixes(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "empty", payload: nil},
		{name: "kind only", payload: []byte{identifierKindNumeric}},
		{name: "numeric of the wrong width", payload: []byte{identifierKindNumeric, 2, 0, 0}},
		{name: "empty name", payload: []byte{identifierKindString, 0}},
		{name: "unknown kind", payload: []byte{9, 4, 0, 0, 0, 0}},
		{name: "value past the end", payload: []byte{identifierKindNumeric, 4, 0, 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := peekIdentifier(test.payload, 0)
			assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
		})
	}
}

func TestPeekIdentifier_ReportsTheConsumedLength(t *testing.T) {
	numeric, err := peekIdentifier(numericIdentifier(9), 0)
	require.NoError(t, err)
	assert.Equal(t, uint32(9), numeric.numeric)
	assert.False(t, numeric.named)
	assert.Equal(t, 6, numeric.length)

	named, err := peekIdentifier(namedIdentifier("abc"), 0)
	require.NoError(t, err)
	assert.True(t, named.named)
	assert.Equal(t, 5, named.length)
}
