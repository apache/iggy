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
)

func TestDeserializeStream(t *testing.T) {
	payload := make([]byte, 96)
	pos := 0

	// Stream header: id(4) + createdAt(8) + topicsCount(4) + sizeBytes(8) + messagesCount(8) + nameLen(1) + name(N)
	binary.LittleEndian.PutUint32(payload[pos:], 1) // id = 1
	pos += 4
	binary.LittleEndian.PutUint64(payload[pos:], 100) // createdAt = 100
	pos += 8
	binary.LittleEndian.PutUint32(payload[pos:], 1) // topicsCount = 1
	pos += 4
	binary.LittleEndian.PutUint64(payload[pos:], 2048) // sizeBytes = 2048
	pos += 8
	binary.LittleEndian.PutUint64(payload[pos:], 10) // messagesCount = 10
	pos += 8
	payload[pos] = 6 // nameLen = 6
	pos++
	copy(payload[pos:], "stream") // name = "stream"
	pos += 6

	// Embedded topic: id(4) + createdAt(8) + partitionsCount(4) + messageExpiry(8) +
	//   compressionAlg(1) + maxTopicSize(8) + replicationFactor(1) + size(8) + messagesCount(8) + nameLen(1) + name(N)
	binary.LittleEndian.PutUint32(payload[pos:], 1) // id = 1
	pos += 4
	binary.LittleEndian.PutUint64(payload[pos:], 200) // createdAt = 200
	pos += 8
	binary.LittleEndian.PutUint32(payload[pos:], 2) // partitionsCount = 2
	pos += 4
	binary.LittleEndian.PutUint64(payload[pos:], 0) // messageExpiry = 0 (none)
	pos += 8
	payload[pos] = 0 // compressionAlgorithm = 0 (none)
	pos++
	binary.LittleEndian.PutUint64(payload[pos:], 0) // maxTopicSize = 0 (unlimited)
	pos += 8
	payload[pos] = 1 // replicationFactor = 1
	pos++
	binary.LittleEndian.PutUint64(payload[pos:], 1024) // size = 1024
	pos += 8
	binary.LittleEndian.PutUint64(payload[pos:], 5) // messagesCount = 5
	pos += 8
	payload[pos] = 6 // nameLen = 6
	pos++
	copy(payload[pos:], "topic1") // name = "topic1"

	result, err := DeserializeStream(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Id != 1 {
		t.Fatalf("expected stream id 1, got %d", result.Id)
	}
	if result.Name != "stream" {
		t.Fatalf("expected stream name 'stream', got '%s'", result.Name)
	}
	if result.CreatedAt != 100 {
		t.Fatalf("expected createdAt 100, got %d", result.CreatedAt)
	}
	if result.SizeBytes != 2048 {
		t.Fatalf("expected sizeBytes 2048, got %d", result.SizeBytes)
	}
	if result.MessagesCount != 10 {
		t.Fatalf("expected messagesCount 10, got %d", result.MessagesCount)
	}
	if len(result.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(result.Topics))
	}
	if result.Topics[0].Name != "topic1" {
		t.Fatalf("expected topic name 'topic1', got '%s'", result.Topics[0].Name)
	}
	if result.Topics[0].PartitionsCount != 2 {
		t.Fatalf("expected partitionsCount 2, got %d", result.Topics[0].PartitionsCount)
	}
}

func TestDeserializeTopic(t *testing.T) {
	payload := make([]byte, 98)
	pos := 0

	// Topic header: id(4) + createdAt(8) + partitionsCount(4) + messageExpiry(8) +
	//   compressionAlg(1) + maxTopicSize(8) + replicationFactor(1) + size(8) + messagesCount(8) + nameLen(1) + name(N)
	binary.LittleEndian.PutUint32(payload[pos:], 1) // id = 1
	pos += 4
	binary.LittleEndian.PutUint64(payload[pos:], 200) // createdAt = 200
	pos += 8
	binary.LittleEndian.PutUint32(payload[pos:], 1) // partitionsCount = 1
	pos += 4
	binary.LittleEndian.PutUint64(payload[pos:], 0) // messageExpiry = 0 (none)
	pos += 8
	payload[pos] = 0 // compressionAlgorithm = 0 (none)
	pos++
	binary.LittleEndian.PutUint64(payload[pos:], 0) // maxTopicSize = 0 (unlimited)
	pos += 8
	payload[pos] = 1 // replicationFactor = 1
	pos++
	binary.LittleEndian.PutUint64(payload[pos:], 1024) // size = 1024
	pos += 8
	binary.LittleEndian.PutUint64(payload[pos:], 5) // messagesCount = 5
	pos += 8
	payload[pos] = 7 // nameLen = 7
	pos++
	copy(payload[pos:], "mytopic") // name = "mytopic"
	pos += 7

	// Partition entry: id(4) + createdAt(8) + segmentsCount(4) + currentOffset(8) + sizeBytes(8) + messagesCount(8)
	binary.LittleEndian.PutUint32(payload[pos:], 1) // id = 1
	pos += 4
	binary.LittleEndian.PutUint64(payload[pos:], 300) // createdAt = 300
	pos += 8
	binary.LittleEndian.PutUint32(payload[pos:], 3) // segmentsCount = 3
	pos += 4
	binary.LittleEndian.PutUint64(payload[pos:], 99) // currentOffset = 99
	pos += 8
	binary.LittleEndian.PutUint64(payload[pos:], 512) // sizeBytes = 512
	pos += 8
	binary.LittleEndian.PutUint64(payload[pos:], 7) // messagesCount = 7

	result, err := DeserializeTopic(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Name != "mytopic" {
		t.Fatalf("expected topic name 'mytopic', got '%s'", result.Name)
	}
	if result.Id != 1 {
		t.Fatalf("expected topic id 1, got %d", result.Id)
	}
	if result.Size != 1024 {
		t.Fatalf("expected topic size 1024, got %d", result.Size)
	}
	if result.MessagesCount != 5 {
		t.Fatalf("expected messagesCount 5, got %d", result.MessagesCount)
	}
	if len(result.Partitions) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(result.Partitions))
	}
	if result.Partitions[0].CurrentOffset != 99 {
		t.Fatalf("expected currentOffset 99, got %d", result.Partitions[0].CurrentOffset)
	}
	if result.Partitions[0].SegmentsCount != 3 {
		t.Fatalf("expected segmentsCount 3, got %d", result.Partitions[0].SegmentsCount)
	}
	if result.Partitions[0].SizeBytes != 512 {
		t.Fatalf("expected sizeBytes 512, got %d", result.Partitions[0].SizeBytes)
	}
	if result.Partitions[0].MessagesCount != 7 {
		t.Fatalf("expected messagesCount 7, got %d", result.Partitions[0].MessagesCount)
	}
}

func TestDeserializeConsumerGroup_WithMembers(t *testing.T) {
	payload := make([]byte, 34)
	pos := 0

	// Consumer group header: id(4) + partitionsCount(4) + membersCount(4) + nameLen(1) + name(N)
	binary.LittleEndian.PutUint32(payload[pos:], 1) // id = 1
	pos += 4
	binary.LittleEndian.PutUint32(payload[pos:], 2) // partitionsCount = 2
	pos += 4
	binary.LittleEndian.PutUint32(payload[pos:], 1) // membersCount = 1
	pos += 4
	payload[pos] = 5 // nameLen = 5
	pos++
	copy(payload[pos:], "grp-1") // name = "grp-1"
	pos += 5

	// Member entry: id(4) + partitionsCount(4) + partitions(4*N)
	binary.LittleEndian.PutUint32(payload[pos:], 10) // memberId = 10
	pos += 4
	binary.LittleEndian.PutUint32(payload[pos:], 2) // partitionsCount = 2
	pos += 4
	binary.LittleEndian.PutUint32(payload[pos:], 1) // partition[0] = 1
	pos += 4
	binary.LittleEndian.PutUint32(payload[pos:], 2) // partition[1] = 2

	result := DeserializeConsumerGroup(payload)
	if result.Name != "grp-1" {
		t.Fatalf("expected group name 'grp-1', got '%s'", result.Name)
	}
	if result.Id != 1 {
		t.Fatalf("expected group id 1, got %d", result.Id)
	}
	if result.PartitionsCount != 2 {
		t.Fatalf("expected partitionsCount 2, got %d", result.PartitionsCount)
	}
	if result.MembersCount != 1 {
		t.Fatalf("expected membersCount 1, got %d", result.MembersCount)
	}
	if len(result.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(result.Members))
	}
	if result.Members[0].ID != 10 {
		t.Fatalf("expected member id 10, got %d", result.Members[0].ID)
	}
	if len(result.Members[0].Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(result.Members[0].Partitions))
	}
	if result.Members[0].Partitions[0] != 1 || result.Members[0].Partitions[1] != 2 {
		t.Fatalf("expected partitions [1,2], got %v", result.Members[0].Partitions)
	}
}

func TestDeserializeUser_WithPermissions(t *testing.T) {
	// User: id=42(4) + createdAt=999(8) + status=1(1) + usernameLen=5(1) + "admin"(5) = 19 bytes
	// hasPermissions=1(1) + permLen=11(4) + permissions(11) = 16 bytes
	payload := make([]byte, 35)
	pos := 0

	binary.LittleEndian.PutUint32(payload[pos:], 42)
	pos += 4
	binary.LittleEndian.PutUint64(payload[pos:], 999)
	pos += 8
	payload[pos] = 1 // status Active in wire format
	pos++
	payload[pos] = 5
	pos++
	copy(payload[pos:], "admin")
	pos += 5

	payload[pos] = 1 // hasPermissions
	pos++
	binary.LittleEndian.PutUint32(payload[pos:], 11) // permission block length
	pos += 4

	// 10 global permission bytes, all true
	for i := 0; i < 10; i++ {
		payload[pos+i] = 1
	}
	pos += 10
	payload[pos] = 0 // hasStreams = false

	result, err := DeserializeUser(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Username != "admin" {
		t.Fatalf("expected username 'admin', got '%s'", result.Username)
	}
	if result.Id != 42 {
		t.Fatalf("expected user id 42, got %d", result.Id)
	}
	if result.CreatedAt != 999 {
		t.Fatalf("expected createdAt 999, got %d", result.CreatedAt)
	}
	if result.Permissions == nil {
		t.Fatalf("expected permissions to be non-nil")
	}
	if !result.Permissions.Global.ManageServers {
		t.Fatalf("expected ManageServers to be true")
	}
	if !result.Permissions.Global.ReadServers {
		t.Fatalf("expected ReadServers to be true")
	}
	if !result.Permissions.Global.ManageUsers {
		t.Fatalf("expected ManageUsers to be true")
	}
	if !result.Permissions.Global.ReadUsers {
		t.Fatalf("expected ReadUsers to be true")
	}
	if !result.Permissions.Global.ManageStreams {
		t.Fatalf("expected ManageStreams to be true")
	}
	if !result.Permissions.Global.ReadStreams {
		t.Fatalf("expected ReadStreams to be true")
	}
	if !result.Permissions.Global.ManageTopics {
		t.Fatalf("expected ManageTopics to be true")
	}
	if !result.Permissions.Global.ReadTopics {
		t.Fatalf("expected ReadTopics to be true")
	}
	if !result.Permissions.Global.PollMessages {
		t.Fatalf("expected PollMessages to be true")
	}
	if !result.Permissions.Global.SendMessages {
		t.Fatalf("expected SendMessages to be true")
	}
}

func TestDeserializeUser_WithoutPermissions(t *testing.T) {
	// User: id=42(4) + createdAt=999(8) + status=1(1) + usernameLen=5(1) + "admin"(5) = 19 bytes
	// hasPermissions=0(1) = 1 byte
	payload := make([]byte, 20)
	pos := 0

	binary.LittleEndian.PutUint32(payload[pos:], 42)
	pos += 4
	binary.LittleEndian.PutUint64(payload[pos:], 999)
	pos += 8
	payload[pos] = 1 // status Active in wire format
	pos++
	payload[pos] = 5
	pos++
	copy(payload[pos:], "admin")
	pos += 5

	payload[pos] = 0 // hasPermissions = false

	result, err := DeserializeUser(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Username != "admin" {
		t.Fatalf("expected username 'admin', got '%s'", result.Username)
	}
	if result.Id != 42 {
		t.Fatalf("expected user id 42, got %d", result.Id)
	}
	if result.Permissions != nil {
		t.Fatalf("expected permissions to be nil, got %+v", result.Permissions)
	}
}

func TestDeserializeClient_WithConsumerGroups(t *testing.T) {
	addr := "127.0.0.1:8090"
	addrLen := len(addr) // 14
	// Client header: id(4) + userId(4) + transport(1) + addrLen(4) + addr(14) + cgCount(4) = 31
	// Consumer group info: streamId(4) + topicId(4) + groupId(4) = 12
	payload := make([]byte, 31+12)
	pos := 0

	binary.LittleEndian.PutUint32(payload[pos:], 1)
	pos += 4
	binary.LittleEndian.PutUint32(payload[pos:], 2)
	pos += 4
	payload[pos] = 1 // Tcp
	pos++
	binary.LittleEndian.PutUint32(payload[pos:], uint32(addrLen))
	pos += 4
	copy(payload[pos:], addr)
	pos += addrLen
	binary.LittleEndian.PutUint32(payload[pos:], 1) // consumerGroupsCount
	pos += 4

	// Consumer group info
	binary.LittleEndian.PutUint32(payload[pos:], 1)
	pos += 4
	binary.LittleEndian.PutUint32(payload[pos:], 2)
	pos += 4
	binary.LittleEndian.PutUint32(payload[pos:], 3)

	result := DeserializeClient(payload)
	if result.ID != 1 {
		t.Fatalf("expected client id 1, got %d", result.ID)
	}
	if result.UserID != 2 {
		t.Fatalf("expected user id 2, got %d", result.UserID)
	}
	if result.Transport != "tcp" {
		t.Fatalf("expected transport 'tcp', got '%s'", result.Transport)
	}
	if result.Address != "127.0.0.1:8090" {
		t.Fatalf("expected address '127.0.0.1:8090', got '%s'", result.Address)
	}
	if result.ConsumerGroupsCount != 1 {
		t.Fatalf("expected consumerGroupsCount 1, got %d", result.ConsumerGroupsCount)
	}
	// make([]ConsumerGroupInfo, 1) pre-fills one zero entry, then append adds the real one
	if len(result.ConsumerGroups) != 2 {
		t.Fatalf("expected 2 consumer group entries (1 zero + 1 real), got %d", len(result.ConsumerGroups))
	}
	cg := result.ConsumerGroups[1]
	if cg.StreamId != 1 {
		t.Fatalf("expected streamId 1, got %d", cg.StreamId)
	}
	if cg.TopicId != 2 {
		t.Fatalf("expected topicId 2, got %d", cg.TopicId)
	}
	if cg.GroupId != 3 {
		t.Fatalf("expected groupId 3, got %d", cg.GroupId)
	}
}

func TestDeserializeUsers_MultipleUsers(t *testing.T) {
	buildUser := func(id uint32, createdAt uint64, status byte, name string) []byte {
		buf := make([]byte, 14+len(name))
		binary.LittleEndian.PutUint32(buf[0:], id)
		binary.LittleEndian.PutUint64(buf[4:], createdAt)
		buf[12] = status
		buf[13] = byte(len(name))
		copy(buf[14:], name)
		return buf
	}

	user1 := buildUser(1, 100, 1, "alice")
	user2 := buildUser(2, 200, 2, "bob")
	payload := append(user1, user2...)

	result, err := DeserializeUsers(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 users, got %d", len(result))
	}
	if result[0].Username != "alice" || result[0].Id != 1 {
		t.Fatalf("user[0] mismatch: got id=%d name=%s", result[0].Id, result[0].Username)
	}
	if result[1].Username != "bob" || result[1].Status != iggcon.Inactive {
		t.Fatalf("user[1] mismatch: got name=%s status=%v", result[1].Username, result[1].Status)
	}
}

func TestDeserializeUsers_EmptyPayload(t *testing.T) {
	_, err := DeserializeUsers([]byte{})
	if err == nil {
		t.Fatalf("expected error for empty payload")
	}
}

func TestDeserializeUsers_InvalidStatus(t *testing.T) {
	buf := make([]byte, 18)
	binary.LittleEndian.PutUint32(buf[0:], 1)
	binary.LittleEndian.PutUint64(buf[4:], 100)
	buf[12] = 99 // invalid status
	buf[13] = 4
	copy(buf[14:], "test")
	_, err := DeserializeUsers(buf)
	if err == nil {
		t.Fatalf("expected error for invalid user status")
	}
}

func TestDeserializeAccessTokens_Valid(t *testing.T) {
	name := "my-token"
	entry := make([]byte, 1+len(name)+8)
	entry[0] = byte(len(name))
	copy(entry[1:], name)
	binary.LittleEndian.PutUint64(entry[1+len(name):], 1000000000)

	result, err := DeserializeAccessTokens(entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 token, got %d", len(result))
	}
	if result[0].Name != "my-token" {
		t.Fatalf("expected name 'my-token', got '%s'", result[0].Name)
	}
	if result[0].Expiry == nil {
		t.Fatalf("expected non-nil expiry")
	}
}

func TestDeserializeAccessTokens_EmptyPayload(t *testing.T) {
	_, err := DeserializeAccessTokens([]byte{})
	if err == nil {
		t.Fatalf("expected error for empty payload")
	}
}

func TestDeserializeClients_MultipleClients(t *testing.T) {
	buildClient := func(id, userId uint32, transport byte, addr string) []byte {
		buf := make([]byte, 4+4+1+4+len(addr)+4)
		pos := 0
		binary.LittleEndian.PutUint32(buf[pos:], id)
		pos += 4
		binary.LittleEndian.PutUint32(buf[pos:], userId)
		pos += 4
		buf[pos] = transport
		pos++
		binary.LittleEndian.PutUint32(buf[pos:], uint32(len(addr)))
		pos += 4
		copy(buf[pos:], addr)
		pos += len(addr)
		binary.LittleEndian.PutUint32(buf[pos:], 0) // no consumer groups
		return buf
	}

	c1 := buildClient(1, 10, 1, "127.0.0.1:8090")
	c2 := buildClient(2, 20, 2, "127.0.0.1:8091")
	payload := append(c1, c2...)

	result, err := DeserializeClients(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 clients, got %d", len(result))
	}
	if result[0].Transport != "tcp" {
		t.Fatalf("expected tcp, got %s", result[0].Transport)
	}
	if result[1].Transport != "quic" {
		t.Fatalf("expected quic, got %s", result[1].Transport)
	}
}

func TestDeserializeClients_EmptyPayload(t *testing.T) {
	result, err := DeserializeClients([]byte{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 clients, got %d", len(result))
	}
}

func TestDeserializePermissions_WithStreamsAndTopics(t *testing.T) {
	var buf []byte

	// 10 global permission bytes (all true)
	for i := 0; i < 10; i++ {
		buf = append(buf, 1)
	}

	// hasStreams = 1 (index 10; the outer `index += 1` skips past this)
	buf = append(buf, 1)

	// stream id = 42 (starts at index 11 after index += 1)
	streamIdBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(streamIdBytes, 42)
	buf = append(buf, streamIdBytes...)

	// 6 stream permission bytes (index 15-20)
	buf = append(buf, 1, 1, 1, 1, 1, 1)

	// hasTopics for this stream = 1 (index 21; inner `index += 1` skips past this)
	buf = append(buf, 1)

	// topic id = 7 (starts at index 22 after inner index += 1)
	topicIdBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(topicIdBytes, 7)
	buf = append(buf, topicIdBytes...)

	// 4 topic permission bytes (index 26-29)
	buf = append(buf, 1, 0, 1, 0)

	// hasMoreTopics = 0 (index 30)
	buf = append(buf, 0)

	// hasMoreStreams = 0 (index 31, after outer `index += 1`)
	buf = append(buf, 0)

	result := deserializePermissions(buf)
	if result == nil {
		t.Fatalf("expected non-nil permissions")
	}
	if !result.Global.ManageServers {
		t.Fatalf("expected ManageServers true")
	}
	if !result.Global.SendMessages {
		t.Fatalf("expected SendMessages true")
	}

	streamPerms, ok := result.Streams[42]
	if !ok {
		t.Fatalf("expected stream 42 in permissions map")
	}
	if !streamPerms.ManageStream {
		t.Fatalf("expected ManageStream true for stream 42")
	}

	topicPerms, ok := streamPerms.Topics[7]
	if !ok {
		t.Fatalf("expected topic 7 in stream 42 permissions")
	}
	if !topicPerms.ManageTopic {
		t.Fatalf("expected ManageTopic true for topic 7")
	}
	if topicPerms.ReadTopic {
		t.Fatalf("expected ReadTopic false for topic 7")
	}
	if !topicPerms.PollMessages {
		t.Fatalf("expected PollMessages true for topic 7")
	}
	if topicPerms.SendMessages {
		t.Fatalf("expected SendMessages false for topic 7")
	}
}

func TestDeserializePermissions_NoStreams(t *testing.T) {
	var buf []byte
	for i := 0; i < 10; i++ {
		buf = append(buf, 0)
	}
	buf = append(buf, 0) // hasStreams = false

	result := deserializePermissions(buf)
	if result == nil {
		t.Fatalf("expected non-nil permissions")
	}
	if result.Global.ManageServers {
		t.Fatalf("expected ManageServers false")
	}
	if len(result.Streams) != 0 {
		t.Fatalf("expected 0 streams, got %d", len(result.Streams))
	}
}

func TestDeserializePermissions_StreamWithNoTopics(t *testing.T) {
	var buf []byte
	for i := 0; i < 10; i++ {
		buf = append(buf, 1)
	}
	buf = append(buf, 1) // hasStreams (index 10; skipped by outer `index += 1`)

	// stream id = 5 (starts at index 11)
	streamIdBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(streamIdBytes, 5)
	buf = append(buf, streamIdBytes...)

	buf = append(buf, 1, 0, 1, 0, 1, 0) // 6 stream perms (index 15-20)

	buf = append(buf, 0) // hasTopics = false (index 21)
	buf = append(buf, 0) // hasMoreStreams = false (index 22, after `index += 1`)

	result := deserializePermissions(buf)
	streamPerms, ok := result.Streams[5]
	if !ok {
		t.Fatalf("expected stream 5")
	}
	if len(streamPerms.Topics) != 0 {
		t.Fatalf("expected 0 topics")
	}
	if !streamPerms.ManageStream {
		t.Fatalf("expected ManageStream true")
	}
	if streamPerms.ReadStream {
		t.Fatalf("expected ReadStream false")
	}
}
