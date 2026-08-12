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

package binaryserialization

import (
	"encoding/binary"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// confirmationPayload builds [count u32] followed by one entry per argument.
func confirmationPayload(entries ...iggcon.SendMessagesConfirmation) []byte {
	payload := binary.LittleEndian.AppendUint32(nil, uint32(len(entries)))
	for _, entry := range entries {
		payload = binary.LittleEndian.AppendUint32(payload, entry.StreamId)
		payload = binary.LittleEndian.AppendUint32(payload, entry.TopicId)
		payload = binary.LittleEndian.AppendUint32(payload, entry.PartitionId)
		payload = binary.LittleEndian.AppendUint64(payload, entry.BaseOffset)
	}
	return payload
}

func TestDeserializeSendMessagesConfirmations_DecodesTheGoldenVector(t *testing.T) {
	// From core/binary_protocol/src/responses/messages/send_messages.rs.
	golden := []byte{
		0x01, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	require.Len(t, golden, 24)

	response, err := DeserializeSendMessagesConfirmations(golden)
	require.NoError(t, err)
	assert.Equal(t, []iggcon.SendMessagesConfirmation{
		{StreamId: 1, TopicId: 2, PartitionId: 3, BaseOffset: 4},
	}, response.Confirmations)
}

func TestDeserializeSendMessagesConfirmations_DecodesMultipleEntries(t *testing.T) {
	entries := []iggcon.SendMessagesConfirmation{
		{StreamId: 1, TopicId: 2, PartitionId: 0, BaseOffset: 0},
		{StreamId: 1, TopicId: 2, PartitionId: 1, BaseOffset: 1 << 40},
		{StreamId: 9, TopicId: 8, PartitionId: 7, BaseOffset: 0xFFFFFFFFFFFFFFFF},
	}

	response, err := DeserializeSendMessagesConfirmations(confirmationPayload(entries...))
	require.NoError(t, err)
	assert.Equal(t, entries, response.Confirmations)
}

func TestDeserializeSendMessagesConfirmations_TreatsAnEmptyPayloadAsSuccess(t *testing.T) {
	response, err := DeserializeSendMessagesConfirmations(nil)
	require.NoError(t, err)
	assert.Empty(t, response.Confirmations)
}

func TestDeserializeSendMessagesConfirmations_AcceptsAZeroCount(t *testing.T) {
	response, err := DeserializeSendMessagesConfirmations(confirmationPayload())
	require.NoError(t, err)
	assert.Empty(t, response.Confirmations)
}

func TestDeserializeSendMessagesConfirmations_RejectsEveryTruncation(t *testing.T) {
	payload := confirmationPayload(
		iggcon.SendMessagesConfirmation{StreamId: 1, TopicId: 2, PartitionId: 3, BaseOffset: 4})

	for length := 1; length < len(payload); length++ {
		_, err := DeserializeSendMessagesConfirmations(payload[:length])
		assert.Error(t, err, "truncated to %d bytes", length)
	}
}

func TestDeserializeSendMessagesConfirmations_RejectsTrailingBytes(t *testing.T) {
	payload := append(confirmationPayload(
		iggcon.SendMessagesConfirmation{StreamId: 1}), 0x00)

	_, err := DeserializeSendMessagesConfirmations(payload)
	assert.Error(t, err)
}

func TestDeserializeSendMessagesConfirmations_RejectsAShortCountWord(t *testing.T) {
	_, err := DeserializeSendMessagesConfirmations([]byte{0, 0, 0, 0, 0})
	assert.Error(t, err)
}

func TestDeserializeSendMessagesConfirmations_DoesNotAllocateOnABogusCount(t *testing.T) {
	payload := binary.LittleEndian.AppendUint32(nil, 0xFFFFFFFF)

	_, err := DeserializeSendMessagesConfirmations(payload)
	assert.Error(t, err)
}

// assignmentPayload builds [generation u64][count u32][partition u32 x n].
func assignmentPayload(generation uint64, partitions ...uint32) []byte {
	payload := binary.LittleEndian.AppendUint64(nil, generation)
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(partitions)))
	for _, partition := range partitions {
		payload = binary.LittleEndian.AppendUint32(payload, partition)
	}
	return payload
}

func TestDeserializeConsumerGroupAssignment_DecodesTheGolden(t *testing.T) {
	assignment, err := DeserializeConsumerGroupAssignment(assignmentPayload(7, 0, 2, 4))
	require.NoError(t, err)
	require.NotNil(t, assignment)
	assert.Equal(t, uint64(7), assignment.Generation)
	assert.Equal(t, []uint32{0, 2, 4}, assignment.Partitions)
}

func TestDeserializeConsumerGroupAssignment_ReportsANonMemberAsATypedError(t *testing.T) {
	assignment, err := DeserializeConsumerGroupAssignment(nil)
	assert.ErrorIs(t, err, ierror.ErrConsumerGroupMemberNotFound)
	assert.Nil(t, assignment)
}

func TestDeserializeConsumerGroupAssignment_AcceptsAnEmptyAssignment(t *testing.T) {
	assignment, err := DeserializeConsumerGroupAssignment(assignmentPayload(3))
	require.NoError(t, err)
	require.NotNil(t, assignment)
	assert.Equal(t, uint64(3), assignment.Generation)
	assert.Empty(t, assignment.Partitions)
}

func TestDeserializeConsumerGroupAssignment_RejectsEveryTruncation(t *testing.T) {
	payload := assignmentPayload(7, 0, 2)

	for length := 1; length < len(payload); length++ {
		_, err := DeserializeConsumerGroupAssignment(payload[:length])
		assert.Error(t, err, "truncated to %d bytes", length)
	}
}

func TestDeserializeConsumerGroupAssignment_RejectsTrailingBytes(t *testing.T) {
	payload := append(assignmentPayload(7, 0), 0x00)

	_, err := DeserializeConsumerGroupAssignment(payload)
	assert.Error(t, err)
}

func TestDeserializeConsumerGroupAssignment_DoesNotAllocateOnABogusCount(t *testing.T) {
	payload := binary.LittleEndian.AppendUint64(nil, 1)
	payload = binary.LittleEndian.AppendUint32(payload, 0xFFFFFFFF)

	_, err := DeserializeConsumerGroupAssignment(payload)
	assert.Error(t, err)
}

// pollPayload builds a poll reply body carrying the given messages.
func pollPayload(t *testing.T, partitionId uint32, payloads ...[]byte) []byte {
	t.Helper()

	body := binary.LittleEndian.AppendUint32(nil, partitionId)
	body = binary.LittleEndian.AppendUint64(body, 42)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(payloads)))
	for _, payload := range payloads {
		message, err := iggcon.NewIggyMessage(payload)
		require.NoError(t, err)
		headerBytes, err := message.Header.AppendBinary(nil)
		require.NoError(t, err)
		body = append(body, headerBytes...)
		body = append(body, message.Payload...)
	}
	return body
}

func TestDeserializeFetchMessagesResponse_DecodesABatch(t *testing.T) {
	payload := pollPayload(t, 3, []byte("first"), []byte("second"))

	polled, err := DeserializeFetchMessagesResponse(payload, iggcon.MESSAGE_COMPRESSION_NONE)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), polled.PartitionId)
	assert.Equal(t, uint64(42), polled.CurrentOffset)
	require.Len(t, polled.Messages, 2)
	assert.Equal(t, []byte("first"), polled.Messages[0].Payload)
	assert.Equal(t, []byte("second"), polled.Messages[1].Payload)
}

func TestDeserializeFetchMessagesResponse_TreatsAnEmptyPayloadAsAnEmptyBatch(t *testing.T) {
	polled, err := DeserializeFetchMessagesResponse(nil, iggcon.MESSAGE_COMPRESSION_NONE)
	require.NoError(t, err)
	assert.Empty(t, polled.Messages)
}

func TestDeserializeFetchMessagesResponse_RejectsEveryTruncation(t *testing.T) {
	payload := pollPayload(t, 1, []byte("first"), []byte("second"))

	for length := 1; length < len(payload); length++ {
		_, err := DeserializeFetchMessagesResponse(
			payload[:length], iggcon.MESSAGE_COMPRESSION_NONE)
		assert.Error(t, err,
			"a reply truncated to %d bytes must not decode as a shorter batch", length)
	}
}

func TestDeserializeFetchMessagesResponse_RejectsACountAboveTheDecodedMessages(t *testing.T) {
	payload := pollPayload(t, 1, []byte("only"))
	binary.LittleEndian.PutUint32(payload[12:16], 5)

	_, err := DeserializeFetchMessagesResponse(payload, iggcon.MESSAGE_COMPRESSION_NONE)
	assert.Error(t, err,
		"a declared count the body does not satisfy must surface, not silently shrink")
}

func TestDeserializeFetchMessagesResponse_RejectsOverrunningUserHeaders(t *testing.T) {
	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	message.Header.UserHeaderLength = 64

	body := binary.LittleEndian.AppendUint32(nil, 1)
	body = binary.LittleEndian.AppendUint64(body, 0)
	body = binary.LittleEndian.AppendUint32(body, 1)
	headerBytes, err := message.Header.AppendBinary(nil)
	require.NoError(t, err)
	body = append(body, headerBytes...)
	body = append(body, message.Payload...)

	_, err = DeserializeFetchMessagesResponse(body, iggcon.MESSAGE_COMPRESSION_NONE)
	assert.Error(t, err, "user headers past the body must not panic or pass")
}

func TestDeserializeToTopic_PinsTheFieldOrderOfTheWireLayout(t *testing.T) {
	const nameOffset = 50
	name := "orders"
	payload := make([]byte, nameOffset+1+len(name))
	binary.LittleEndian.PutUint32(payload[0:4], 11)
	binary.LittleEndian.PutUint64(payload[4:12], 1700000000)
	binary.LittleEndian.PutUint32(payload[12:16], 3)
	binary.LittleEndian.PutUint64(payload[16:24], 60_000_000)
	payload[24] = 2
	binary.LittleEndian.PutUint64(payload[25:33], 1<<30)
	payload[33] = 1
	binary.LittleEndian.PutUint64(payload[34:42], 4096)
	binary.LittleEndian.PutUint64(payload[42:50], 12)
	payload[nameOffset] = byte(len(name))
	copy(payload[nameOffset+1:], name)

	topic, readBytes, err := DeserializeToTopic(payload, 0)
	require.NoError(t, err)

	assert.Equal(t, uint32(11), topic.Id)
	assert.Equal(t, uint64(1700000000), topic.CreatedAt)
	assert.Equal(t, uint32(3), topic.PartitionsCount)
	assert.Equal(t, iggcon.Duration(60_000_000), topic.MessageExpiry,
		"message expiry is the u64 at +16")
	assert.Equal(t, uint8(2), topic.CompressionAlgorithm,
		"compression is the u8 at +24")
	assert.Equal(t, uint64(1<<30), topic.MaxTopicSize)
	assert.Equal(t, uint8(1), topic.ReplicationFactor)
	assert.Equal(t, uint64(4096), topic.Size)
	assert.Equal(t, uint64(12), topic.MessagesCount)
	assert.Equal(t, name, topic.Name)
	assert.Equal(t, len(payload), readBytes)
}
