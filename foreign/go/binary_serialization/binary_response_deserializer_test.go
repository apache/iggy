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
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestDeserializeFetchMessagesResponse_EmptyPayload(t *testing.T) {
	response, err := DeserializeFetchMessagesResponse([]byte{}, iggcon.MESSAGE_COMPRESSION_NONE)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if response.PartitionId != 0 || response.CurrentOffset != 0 {
		t.Fatalf("expected zero metadata, got partitionId=%d currentOffset=%d", response.PartitionId, response.CurrentOffset)
	}
	if len(response.Messages) != 0 {
		t.Fatalf("expected no messages, got %d", len(response.Messages))
	}
}

func TestDeserializeFetchMessagesResponse_ParsesSingleMessage(t *testing.T) {
	var msgID iggcon.MessageID
	copy(msgID[:], []byte("abcdefghijklmnop"))
	messagePayload := []byte("hello")
	userHeaders := []byte{0xAA, 0xBB}

	header := iggcon.MessageHeader{
		Checksum:         123,
		Id:               msgID,
		Offset:           11,
		Timestamp:        22,
		OriginTimestamp:  33,
		UserHeaderLength: uint32(len(userHeaders)),
		PayloadLength:    uint32(len(messagePayload)),
		Reserved:         44,
	}

	payload := make([]byte, 16)
	binary.LittleEndian.PutUint32(payload[0:4], 9)   // partition ID
	binary.LittleEndian.PutUint64(payload[4:12], 99) // current offset
	binary.LittleEndian.PutUint32(payload[12:16], 1) // messages count
	payload = append(payload, header.ToBytes()...)
	payload = append(payload, messagePayload...)
	payload = append(payload, userHeaders...)

	response, err := DeserializeFetchMessagesResponse(payload, iggcon.MESSAGE_COMPRESSION_NONE)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if response.PartitionId != 9 {
		t.Fatalf("partition id mismatch, expected 9 got %d", response.PartitionId)
	}
	if response.CurrentOffset != 99 {
		t.Fatalf("current offset mismatch, expected 99 got %d", response.CurrentOffset)
	}
	if response.MessageCount != 1 {
		t.Fatalf("message count mismatch, expected 1 got %d", response.MessageCount)
	}
	if len(response.Messages) != 1 {
		t.Fatalf("expected exactly one message, got %d", len(response.Messages))
	}
	if !bytes.Equal(response.Messages[0].Payload, messagePayload) {
		t.Fatalf("payload mismatch, expected %v got %v", messagePayload, response.Messages[0].Payload)
	}
	if !bytes.Equal(response.Messages[0].UserHeaders, userHeaders) {
		t.Fatalf("user headers mismatch, expected %v got %v", userHeaders, response.Messages[0].UserHeaders)
	}
}

func TestDeserializeFetchMessagesResponse_TruncatedHeaderBody_IgnoresMessage(t *testing.T) {
	payload := make([]byte, 16+iggcon.MessageHeaderSize-1)
	binary.LittleEndian.PutUint32(payload[12:16], 1) // declared message count

	response, err := DeserializeFetchMessagesResponse(payload, iggcon.MESSAGE_COMPRESSION_NONE)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(response.Messages) != 0 {
		t.Fatalf("expected no messages from truncated payload, got %d", len(response.Messages))
	}
}

func TestDeserializeFetchMessagesResponse_TruncatedMessagePayload_IgnoresMessage(t *testing.T) {
	var msgID iggcon.MessageID
	copy(msgID[:], []byte("abcdefghijklmnop"))
	header := iggcon.MessageHeader{
		Id:            msgID,
		PayloadLength: 10, // claims 10 bytes
	}

	payload := make([]byte, 16)
	binary.LittleEndian.PutUint32(payload[12:16], 1) // declared message count
	payload = append(payload, header.ToBytes()...)
	payload = append(payload, []byte{0x01, 0x02, 0x03}...) // only 3 bytes available

	response, err := DeserializeFetchMessagesResponse(payload, iggcon.MESSAGE_COMPRESSION_NONE)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(response.Messages) != 0 {
		t.Fatalf("expected no messages when payload is truncated, got %d", len(response.Messages))
	}
}

func TestDeserializeLogInResponse(t *testing.T) {
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload[0:4], 42)

	result := DeserializeLogInResponse(payload)
	if result == nil {
		t.Fatalf("expected non-nil result")
	}
	if result.UserId != 42 {
		t.Fatalf("expected UserId=42, got %d", result.UserId)
	}
}

func TestDeserializeOffset_EmptyPayload(t *testing.T) {
	result := DeserializeOffset([]byte{})
	if result != nil {
		t.Fatalf("expected nil for empty payload, got %+v", result)
	}
}

func TestDeserializeOffset_ValidPayload(t *testing.T) {
	payload := make([]byte, 20)
	binary.LittleEndian.PutUint32(payload[0:4], 1)
	binary.LittleEndian.PutUint64(payload[4:12], 100)
	binary.LittleEndian.PutUint64(payload[12:20], 50)

	result := DeserializeOffset(payload)
	if result == nil {
		t.Fatalf("expected non-nil result")
	}
	if result.PartitionId != 1 {
		t.Fatalf("expected PartitionId=1, got %d", result.PartitionId)
	}
	if result.CurrentOffset != 100 {
		t.Fatalf("expected CurrentOffset=100, got %d", result.CurrentOffset)
	}
	if result.StoredOffset != 50 {
		t.Fatalf("expected StoredOffset=50, got %d", result.StoredOffset)
	}
}

func buildStreamPayload(id uint32, createdAt uint64, topicsCount uint32, sizeBytes uint64, messagesCount uint64, name string) []byte {
	nameBytes := []byte(name)
	payload := make([]byte, 33+len(nameBytes))
	binary.LittleEndian.PutUint32(payload[0:4], id)
	binary.LittleEndian.PutUint64(payload[4:12], createdAt)
	binary.LittleEndian.PutUint32(payload[12:16], topicsCount)
	binary.LittleEndian.PutUint64(payload[16:24], sizeBytes)
	binary.LittleEndian.PutUint64(payload[24:32], messagesCount)
	payload[32] = byte(len(nameBytes))
	copy(payload[33:], nameBytes)
	return payload
}

func TestDeserializeToStream(t *testing.T) {
	payload := buildStreamPayload(7, 1234567890, 3, 4096, 500, "test")

	stream, readBytes := DeserializeToStream(payload, 0)
	if stream.Id != 7 {
		t.Fatalf("expected Id=7, got %d", stream.Id)
	}
	if stream.CreatedAt != 1234567890 {
		t.Fatalf("expected CreatedAt=1234567890, got %d", stream.CreatedAt)
	}
	if stream.TopicsCount != 3 {
		t.Fatalf("expected TopicsCount=3, got %d", stream.TopicsCount)
	}
	if stream.SizeBytes != 4096 {
		t.Fatalf("expected SizeBytes=4096, got %d", stream.SizeBytes)
	}
	if stream.MessagesCount != 500 {
		t.Fatalf("expected MessagesCount=500, got %d", stream.MessagesCount)
	}
	if stream.Name != "test" {
		t.Fatalf("expected Name='test', got '%s'", stream.Name)
	}
	expectedReadBytes := 4 + 8 + 4 + 8 + 8 + 1 + 4
	if readBytes != expectedReadBytes {
		t.Fatalf("expected readBytes=%d, got %d", expectedReadBytes, readBytes)
	}
}

func TestDeserializeStreams(t *testing.T) {
	entry1 := buildStreamPayload(1, 100, 2, 1024, 10, "stream1")
	entry2 := buildStreamPayload(2, 200, 5, 2048, 20, "stream2")
	payload := append(entry1, entry2...)

	streams := DeserializeStreams(payload)
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(streams))
	}
	if streams[0].Name != "stream1" {
		t.Fatalf("expected first stream name='stream1', got '%s'", streams[0].Name)
	}
	if streams[1].Name != "stream2" {
		t.Fatalf("expected second stream name='stream2', got '%s'", streams[1].Name)
	}
}

func buildTopicPayload(id uint32, createdAt uint64, partitionsCount uint32, messageExpiry uint64, compressionAlgo byte, maxTopicSize uint64, replicationFactor byte, size uint64, messagesCount uint64, name string) []byte {
	nameBytes := []byte(name)
	payload := make([]byte, 51+len(nameBytes))
	binary.LittleEndian.PutUint32(payload[0:4], id)
	binary.LittleEndian.PutUint64(payload[4:12], createdAt)
	binary.LittleEndian.PutUint32(payload[12:16], partitionsCount)
	binary.LittleEndian.PutUint64(payload[16:24], messageExpiry)
	payload[24] = compressionAlgo
	binary.LittleEndian.PutUint64(payload[25:33], maxTopicSize)
	payload[33] = replicationFactor
	binary.LittleEndian.PutUint64(payload[34:42], size)
	binary.LittleEndian.PutUint64(payload[42:50], messagesCount)
	payload[50] = byte(len(nameBytes))
	copy(payload[51:], nameBytes)
	return payload
}

func TestDeserializeToTopic(t *testing.T) {
	payload := buildTopicPayload(10, 9999, 4, 60000, 2, 1048576, 3, 8192, 777, "topic1")

	topic, readBytes, err := DeserializeToTopic(payload, 0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if topic.Id != 10 {
		t.Fatalf("expected Id=10, got %d", topic.Id)
	}
	if topic.CreatedAt != 9999 {
		t.Fatalf("expected CreatedAt=9999, got %d", topic.CreatedAt)
	}
	if topic.PartitionsCount != 4 {
		t.Fatalf("expected PartitionsCount=4, got %d", topic.PartitionsCount)
	}
	if uint64(topic.MessageExpiry) != 60000 {
		t.Fatalf("expected MessageExpiry=60000, got %d", topic.MessageExpiry)
	}
	if topic.CompressionAlgorithm != 2 {
		t.Fatalf("expected CompressionAlgorithm=2, got %d", topic.CompressionAlgorithm)
	}
	if topic.MaxTopicSize != 1048576 {
		t.Fatalf("expected MaxTopicSize=1048576, got %d", topic.MaxTopicSize)
	}
	if topic.ReplicationFactor != 3 {
		t.Fatalf("expected ReplicationFactor=3, got %d", topic.ReplicationFactor)
	}
	if topic.Size != 8192 {
		t.Fatalf("expected Size=8192, got %d", topic.Size)
	}
	if topic.MessagesCount != 777 {
		t.Fatalf("expected MessagesCount=777, got %d", topic.MessagesCount)
	}
	if topic.Name != "topic1" {
		t.Fatalf("expected Name='topic1', got '%s'", topic.Name)
	}
	expectedReadBytes := 4 + 8 + 4 + 8 + 8 + 8 + 8 + 1 + 1 + 1 + 6
	if readBytes != expectedReadBytes {
		t.Fatalf("expected readBytes=%d, got %d", expectedReadBytes, readBytes)
	}
}

func TestDeserializeTopics(t *testing.T) {
	entry1 := buildTopicPayload(1, 100, 2, 1000, 1, 4096, 1, 512, 50, "topicA")
	entry2 := buildTopicPayload(2, 200, 3, 2000, 0, 8192, 2, 1024, 100, "topicB")
	payload := append(entry1, entry2...)

	topics, err := DeserializeTopics(payload)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(topics))
	}
	if topics[0].Name != "topicA" {
		t.Fatalf("expected first topic name='topicA', got '%s'", topics[0].Name)
	}
	if topics[1].Name != "topicB" {
		t.Fatalf("expected second topic name='topicB', got '%s'", topics[1].Name)
	}
}

func TestDeserializePartition(t *testing.T) {
	payload := make([]byte, 40)
	binary.LittleEndian.PutUint32(payload[0:4], 5)
	binary.LittleEndian.PutUint64(payload[4:12], 1111)
	binary.LittleEndian.PutUint32(payload[12:16], 8)
	binary.LittleEndian.PutUint64(payload[16:24], 99)
	binary.LittleEndian.PutUint64(payload[24:32], 2048)
	binary.LittleEndian.PutUint64(payload[32:40], 300)

	partition, readBytes := DeserializePartition(payload, 0)
	if partition.Id != 5 {
		t.Fatalf("expected Id=5, got %d", partition.Id)
	}
	if partition.CreatedAt != 1111 {
		t.Fatalf("expected CreatedAt=1111, got %d", partition.CreatedAt)
	}
	if partition.SegmentsCount != 8 {
		t.Fatalf("expected SegmentsCount=8, got %d", partition.SegmentsCount)
	}
	if partition.CurrentOffset != 99 {
		t.Fatalf("expected CurrentOffset=99, got %d", partition.CurrentOffset)
	}
	if partition.SizeBytes != 2048 {
		t.Fatalf("expected SizeBytes=2048, got %d", partition.SizeBytes)
	}
	if partition.MessagesCount != 300 {
		t.Fatalf("expected MessagesCount=300, got %d", partition.MessagesCount)
	}
	if readBytes != 40 {
		t.Fatalf("expected readBytes=40, got %d", readBytes)
	}
}

func buildConsumerGroupPayload(id uint32, partitionsCount uint32, membersCount uint32, name string) []byte {
	nameBytes := []byte(name)
	payload := make([]byte, 13+len(nameBytes))
	binary.LittleEndian.PutUint32(payload[0:4], id)
	binary.LittleEndian.PutUint32(payload[4:8], partitionsCount)
	binary.LittleEndian.PutUint32(payload[8:12], membersCount)
	payload[12] = byte(len(nameBytes))
	copy(payload[13:], nameBytes)
	return payload
}

func TestDeserializeToConsumerGroup(t *testing.T) {
	payload := buildConsumerGroupPayload(11, 6, 2, "grp1")

	cg, readBytes := DeserializeToConsumerGroup(payload, 0)
	if cg == nil {
		t.Fatalf("expected non-nil consumer group")
	}
	if cg.Id != 11 {
		t.Fatalf("expected Id=11, got %d", cg.Id)
	}
	if cg.PartitionsCount != 6 {
		t.Fatalf("expected PartitionsCount=6, got %d", cg.PartitionsCount)
	}
	if cg.MembersCount != 2 {
		t.Fatalf("expected MembersCount=2, got %d", cg.MembersCount)
	}
	if cg.Name != "grp1" {
		t.Fatalf("expected Name='grp1', got '%s'", cg.Name)
	}
	expectedReadBytes := 12 + 1 + 4
	if readBytes != expectedReadBytes {
		t.Fatalf("expected readBytes=%d, got %d", expectedReadBytes, readBytes)
	}
}

func TestDeserializeConsumerGroups(t *testing.T) {
	entry1 := buildConsumerGroupPayload(1, 3, 1, "group1")
	entry2 := buildConsumerGroupPayload(2, 5, 4, "group2")
	payload := append(entry1, entry2...)

	groups := DeserializeConsumerGroups(payload)
	if len(groups) != 2 {
		t.Fatalf("expected 2 consumer groups, got %d", len(groups))
	}
	if groups[0].Name != "group1" {
		t.Fatalf("expected first group name='group1', got '%s'", groups[0].Name)
	}
	if groups[1].Name != "group2" {
		t.Fatalf("expected second group name='group2', got '%s'", groups[1].Name)
	}
}

func TestDeserializeToConsumerGroupMember(t *testing.T) {
	payload := make([]byte, 16)
	binary.LittleEndian.PutUint32(payload[0:4], 77)
	binary.LittleEndian.PutUint32(payload[4:8], 2)
	binary.LittleEndian.PutUint32(payload[8:12], 10)
	binary.LittleEndian.PutUint32(payload[12:16], 20)

	member, readBytes := DeserializeToConsumerGroupMember(payload, 0)
	if member.ID != 77 {
		t.Fatalf("expected ID=77, got %d", member.ID)
	}
	if member.PartitionsCount != 2 {
		t.Fatalf("expected PartitionsCount=2, got %d", member.PartitionsCount)
	}
	if len(member.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(member.Partitions))
	}
	if member.Partitions[0] != 10 {
		t.Fatalf("expected first partition=10, got %d", member.Partitions[0])
	}
	if member.Partitions[1] != 20 {
		t.Fatalf("expected second partition=20, got %d", member.Partitions[1])
	}
	expectedReadBytes := 4 + 4 + 2*4
	if readBytes != expectedReadBytes {
		t.Fatalf("expected readBytes=%d, got %d", expectedReadBytes, readBytes)
	}
}

func TestDeserializeUsers_EmptyPayload(t *testing.T) {
	_, err := DeserializeUsers([]byte{})
	if err == nil {
		t.Fatalf("expected error for empty payload")
	}
}

func TestDeserializeUsers_ValidPayload(t *testing.T) {
	username := "admin"
	usernameBytes := []byte(username)
	payload := make([]byte, 14+len(usernameBytes))
	binary.LittleEndian.PutUint32(payload[0:4], 1)
	binary.LittleEndian.PutUint64(payload[4:12], 5555)
	payload[12] = 1 // Active
	payload[13] = byte(len(usernameBytes))
	copy(payload[14:], usernameBytes)

	users, err := DeserializeUsers(payload)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(users))
	}
	if users[0].Id != 1 {
		t.Fatalf("expected Id=1, got %d", users[0].Id)
	}
	if users[0].CreatedAt != 5555 {
		t.Fatalf("expected CreatedAt=5555, got %d", users[0].CreatedAt)
	}
	if users[0].Status != iggcon.Active {
		t.Fatalf("expected Status=Active, got %v", users[0].Status)
	}
	if users[0].Username != "admin" {
		t.Fatalf("expected Username='admin', got '%s'", users[0].Username)
	}
}

func TestDeserializeClients_EmptyPayload(t *testing.T) {
	clients, err := DeserializeClients([]byte{})
	if err != nil {
		t.Fatalf("expected no error for empty payload, got %v", err)
	}
	if len(clients) != 0 {
		t.Fatalf("expected 0 clients, got %d", len(clients))
	}
}

func TestDeserializeClients_ValidPayload(t *testing.T) {
	address := "1.2.3.4:8090"
	addressBytes := []byte(address)
	headerSize := 4 + 4 + 1 + 4 + len(addressBytes) + 4
	payload := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(payload[0:4], 99)
	binary.LittleEndian.PutUint32(payload[4:8], 7)
	payload[8] = 1 // Tcp
	binary.LittleEndian.PutUint32(payload[9:13], uint32(len(addressBytes)))
	copy(payload[13:13+len(addressBytes)], addressBytes)
	pos := 13 + len(addressBytes)
	binary.LittleEndian.PutUint32(payload[pos:pos+4], 3)

	clients, err := DeserializeClients(payload)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("expected 1 client, got %d", len(clients))
	}
	if clients[0].ID != 99 {
		t.Fatalf("expected ID=99, got %d", clients[0].ID)
	}
	if clients[0].UserID != 7 {
		t.Fatalf("expected UserID=7, got %d", clients[0].UserID)
	}
	if clients[0].Transport != string(iggcon.Tcp) {
		t.Fatalf("expected Transport='tcp', got '%s'", clients[0].Transport)
	}
	if clients[0].Address != address {
		t.Fatalf("expected Address='%s', got '%s'", address, clients[0].Address)
	}
	if clients[0].ConsumerGroupsCount != 3 {
		t.Fatalf("expected ConsumerGroupsCount=3, got %d", clients[0].ConsumerGroupsCount)
	}
}

func TestDeserializeAccessToken(t *testing.T) {
	token := "abc123"
	tokenBytes := []byte(token)
	payload := make([]byte, 1+len(tokenBytes))
	payload[0] = byte(len(tokenBytes))
	copy(payload[1:], tokenBytes)

	result, err := DeserializeAccessToken(payload)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Token != token {
		t.Fatalf("expected Token='%s', got '%s'", token, result.Token)
	}
}

func TestDeserializeAccessTokens_EmptyPayload(t *testing.T) {
	_, err := DeserializeAccessTokens([]byte{})
	if !errors.Is(err, ierror.ErrEmptyMessagePayload) {
		t.Fatalf("expected ErrEmptyMessagePayload, got %v", err)
	}
}

func TestDeserializeAccessTokens_ValidPayload(t *testing.T) {
	name := "token1"
	nameBytes := []byte(name)
	payload := make([]byte, 1+len(nameBytes)+8)
	payload[0] = byte(len(nameBytes))
	copy(payload[1:1+len(nameBytes)], nameBytes)
	binary.LittleEndian.PutUint64(payload[1+len(nameBytes):], 1000000)

	tokens, err := DeserializeAccessTokens(payload)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Name != name {
		t.Fatalf("expected Name='%s', got '%s'", name, tokens[0].Name)
	}
	if tokens[0].Expiry == nil {
		t.Fatalf("expected non-nil Expiry")
	}
}
