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
	"encoding/binary"
	"errors"
	"net"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
)

func buildResponse(status uint32, payload []byte) []byte {
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], status)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(payload)))
	return append(header, payload...)
}

func newTestClient(response []byte) (*IggyTcpClient, func()) {
	serverConn, clientConn := net.Pipe()

	client := &IggyTcpClient{
		conn:                 clientConn,
		state:                iggcon.StateConnected,
		currentServerAddress: "127.0.0.1:8090",
		clientAddress:        "127.0.0.1:12345",
		config:               defaultTcpClientConfig(),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		lenBuf := make([]byte, 4)
		if _, err := serverConn.Read(lenBuf); err != nil {
			return
		}
		msgLen := int(binary.LittleEndian.Uint32(lenBuf))
		remaining := make([]byte, msgLen)
		total := 0
		for total < msgLen {
			n, err := serverConn.Read(remaining[total:])
			if err != nil {
				return
			}
			total += n
		}
		_, _ = serverConn.Write(response)
	}()

	cleanup := func() {
		clientConn.Close()
		serverConn.Close()
		<-done
	}
	return client, cleanup
}

func buildStreamPayload(id uint32, createdAt uint64, topicsCount uint32, sizeBytes uint64, messagesCount uint64, name string) []byte {
	buf := make([]byte, 33+len(name))
	binary.LittleEndian.PutUint32(buf[0:4], id)
	binary.LittleEndian.PutUint64(buf[4:12], createdAt)
	binary.LittleEndian.PutUint32(buf[12:16], topicsCount)
	binary.LittleEndian.PutUint64(buf[16:24], sizeBytes)
	binary.LittleEndian.PutUint64(buf[24:32], messagesCount)
	buf[32] = byte(len(name))
	copy(buf[33:], name)
	return buf
}

func buildTopicPayload(id uint32, createdAt uint64, partitionsCount uint32, messageExpiry uint64, compressionAlgorithm byte, maxTopicSize uint64, replicationFactor byte, size uint64, messagesCount uint64, name string) []byte {
	buf := make([]byte, 51+len(name))
	binary.LittleEndian.PutUint32(buf[0:4], id)
	binary.LittleEndian.PutUint64(buf[4:12], createdAt)
	binary.LittleEndian.PutUint32(buf[12:16], partitionsCount)
	binary.LittleEndian.PutUint64(buf[16:24], messageExpiry)
	buf[24] = compressionAlgorithm
	binary.LittleEndian.PutUint64(buf[25:33], maxTopicSize)
	buf[33] = replicationFactor
	binary.LittleEndian.PutUint64(buf[34:42], size)
	binary.LittleEndian.PutUint64(buf[42:50], messagesCount)
	buf[50] = byte(len(name))
	copy(buf[51:], name)
	return buf
}

func buildConsumerGroupPayload(id, partitionsCount, membersCount uint32, name string) []byte {
	buf := make([]byte, 13+len(name))
	binary.LittleEndian.PutUint32(buf[0:4], id)
	binary.LittleEndian.PutUint32(buf[4:8], partitionsCount)
	binary.LittleEndian.PutUint32(buf[8:12], membersCount)
	buf[12] = byte(len(name))
	copy(buf[13:], name)
	return buf
}

func buildClientInfoPayload(id, userId uint32, transport byte, address string, consumerGroupsCount uint32) []byte {
	buf := make([]byte, 13+len(address)+4)
	binary.LittleEndian.PutUint32(buf[0:4], id)
	binary.LittleEndian.PutUint32(buf[4:8], userId)
	buf[8] = transport
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(address)))
	copy(buf[13:13+len(address)], address)
	binary.LittleEndian.PutUint32(buf[13+len(address):], consumerGroupsCount)
	return buf
}

func buildOffsetPayload(partitionId uint32, currentOffset, storedOffset uint64) []byte {
	buf := make([]byte, 20)
	binary.LittleEndian.PutUint32(buf[0:4], partitionId)
	binary.LittleEndian.PutUint64(buf[4:12], currentOffset)
	binary.LittleEndian.PutUint64(buf[12:20], storedOffset)
	return buf
}

func mustIdentifier(id uint32) iggcon.Identifier {
	ident, err := iggcon.NewIdentifier(id)
	if err != nil {
		panic(err)
	}
	return ident
}

// --- Tests ---

func TestPing(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.Ping()
	if err != nil {
		t.Fatalf("Ping() returned unexpected error: %v", err)
	}
}

func TestPing_ServerError(t *testing.T) {
	resp := buildResponse(3, nil) // InvalidCommand code = 3
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.Ping()
	if err == nil {
		t.Fatalf("Ping() expected error, got nil")
	}

	var iggyErr ierror.IggyError
	if !errors.As(err, &iggyErr) {
		t.Fatalf("expected IggyError, got %T: %v", err, err)
	}
	if iggyErr.Code() != 3 {
		t.Fatalf("expected error code 3 (InvalidCommand), got %d", iggyErr.Code())
	}
}

func TestGetStreams(t *testing.T) {
	streamBytes := buildStreamPayload(1, 100, 2, 1024, 50, "test")
	resp := buildResponse(0, streamBytes)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	streams, err := client.GetStreams()
	if err != nil {
		t.Fatalf("GetStreams() error: %v", err)
	}
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if streams[0].Id != 1 {
		t.Fatalf("expected stream id=1, got %d", streams[0].Id)
	}
	if streams[0].CreatedAt != 100 {
		t.Fatalf("expected createdAt=100, got %d", streams[0].CreatedAt)
	}
	if streams[0].TopicsCount != 2 {
		t.Fatalf("expected topicsCount=2, got %d", streams[0].TopicsCount)
	}
	if streams[0].SizeBytes != 1024 {
		t.Fatalf("expected sizeBytes=1024, got %d", streams[0].SizeBytes)
	}
	if streams[0].MessagesCount != 50 {
		t.Fatalf("expected messagesCount=50, got %d", streams[0].MessagesCount)
	}
	if streams[0].Name != "test" {
		t.Fatalf("expected name='test', got '%s'", streams[0].Name)
	}
}

func TestGetStream(t *testing.T) {
	streamBytes := buildStreamPayload(1, 200, 1, 2048, 100, "mystream")
	topicBytes := buildTopicPayload(10, 300, 3, 0, 1, 0, 1, 512, 25, "mytopic")
	payload := append(streamBytes, topicBytes...)
	resp := buildResponse(0, payload)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	details, err := client.GetStream(mustIdentifier(1))
	if err != nil {
		t.Fatalf("GetStream() error: %v", err)
	}
	if details.Id != 1 {
		t.Fatalf("expected stream id=1, got %d", details.Id)
	}
	if details.Name != "mystream" {
		t.Fatalf("expected name='mystream', got '%s'", details.Name)
	}
	if len(details.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(details.Topics))
	}
	if details.Topics[0].Id != 10 {
		t.Fatalf("expected topic id=10, got %d", details.Topics[0].Id)
	}
	if details.Topics[0].Name != "mytopic" {
		t.Fatalf("expected topic name='mytopic', got '%s'", details.Topics[0].Name)
	}
}

func TestGetStream_EmptyBuffer(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	_, err := client.GetStream(mustIdentifier(1))
	if err == nil {
		t.Fatalf("expected ErrStreamIdNotFound, got nil")
	}
	if !errors.Is(err, ierror.ErrStreamIdNotFound) {
		t.Fatalf("expected ErrStreamIdNotFound, got: %v", err)
	}
}

func TestCreateStream_ValidatesNameLength(t *testing.T) {
	client := &IggyTcpClient{state: iggcon.StateConnected}

	_, err := client.CreateStream("")
	if err == nil {
		t.Fatalf("expected ErrInvalidStreamName for empty name, got nil")
	}
	if !errors.Is(err, ierror.ErrInvalidStreamName) {
		t.Fatalf("expected ErrInvalidStreamName, got: %v", err)
	}
}

func TestCreateStream(t *testing.T) {
	streamBytes := buildStreamPayload(5, 999, 0, 0, 0, "newstream")
	resp := buildResponse(0, streamBytes)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	details, err := client.CreateStream("newstream")
	if err != nil {
		t.Fatalf("CreateStream() error: %v", err)
	}
	if details.Id != 5 {
		t.Fatalf("expected id=5, got %d", details.Id)
	}
	if details.Name != "newstream" {
		t.Fatalf("expected name='newstream', got '%s'", details.Name)
	}
}

func TestUpdateStream(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.UpdateStream(mustIdentifier(1), "updated")
	if err != nil {
		t.Fatalf("UpdateStream() unexpected error: %v", err)
	}
}

func TestUpdateStream_EmptyName(t *testing.T) {
	client := &IggyTcpClient{state: iggcon.StateConnected}

	err := client.UpdateStream(mustIdentifier(1), "")
	if !errors.Is(err, ierror.ErrInvalidStreamName) {
		t.Fatalf("expected ErrInvalidStreamName, got: %v", err)
	}
}

func TestDeleteStream(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.DeleteStream(mustIdentifier(1))
	if err != nil {
		t.Fatalf("DeleteStream() unexpected error: %v", err)
	}
}

func TestGetTopics(t *testing.T) {
	topicBytes := buildTopicPayload(1, 100, 2, 0, 1, 0, 1, 256, 10, "topic1")
	resp := buildResponse(0, topicBytes)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	topics, err := client.GetTopics(mustIdentifier(1))
	if err != nil {
		t.Fatalf("GetTopics() error: %v", err)
	}
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}
	if topics[0].Id != 1 {
		t.Fatalf("expected topic id=1, got %d", topics[0].Id)
	}
	if topics[0].Name != "topic1" {
		t.Fatalf("expected name='topic1', got '%s'", topics[0].Name)
	}
	if topics[0].PartitionsCount != 2 {
		t.Fatalf("expected partitionsCount=2, got %d", topics[0].PartitionsCount)
	}
}

func TestCreateTopic_ValidatesName(t *testing.T) {
	client := &IggyTcpClient{state: iggcon.StateConnected}

	rf := uint8(1)
	_, err := client.CreateTopic(mustIdentifier(1), "", 1, iggcon.CompressionAlgorithmNone, 0, 0, &rf)
	if !errors.Is(err, ierror.ErrInvalidTopicName) {
		t.Fatalf("expected ErrInvalidTopicName, got: %v", err)
	}
}

func TestCreateTopic_ValidatesPartitionCount(t *testing.T) {
	client := &IggyTcpClient{state: iggcon.StateConnected}

	rf := uint8(1)
	_, err := client.CreateTopic(mustIdentifier(1), "t", 1001, iggcon.CompressionAlgorithmNone, 0, 0, &rf)
	if !errors.Is(err, ierror.ErrTooManyPartitions) {
		t.Fatalf("expected ErrTooManyPartitions, got: %v", err)
	}
}

func TestCreateTopic_ValidatesReplicationFactor(t *testing.T) {
	client := &IggyTcpClient{state: iggcon.StateConnected}

	rf := uint8(0)
	_, err := client.CreateTopic(mustIdentifier(1), "t", 1, iggcon.CompressionAlgorithmNone, 0, 0, &rf)
	if !errors.Is(err, ierror.ErrInvalidReplicationFactor) {
		t.Fatalf("expected ErrInvalidReplicationFactor, got: %v", err)
	}
}

func TestDeleteTopic(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.DeleteTopic(mustIdentifier(1), mustIdentifier(1))
	if err != nil {
		t.Fatalf("DeleteTopic() unexpected error: %v", err)
	}
}

func TestCreatePartitions(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.CreatePartitions(mustIdentifier(1), mustIdentifier(1), 3)
	if err != nil {
		t.Fatalf("CreatePartitions() unexpected error: %v", err)
	}
}

func TestDeletePartitions(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.DeletePartitions(mustIdentifier(1), mustIdentifier(1), 2)
	if err != nil {
		t.Fatalf("DeletePartitions() unexpected error: %v", err)
	}
}

func TestSendMessages_ValidatesEmptyMessages(t *testing.T) {
	client := &IggyTcpClient{state: iggcon.StateConnected}

	err := client.SendMessages(mustIdentifier(1), mustIdentifier(1), iggcon.None(), []iggcon.IggyMessage{})
	if !errors.Is(err, ierror.ErrInvalidMessagesCount) {
		t.Fatalf("expected ErrInvalidMessagesCount, got: %v", err)
	}
}

func TestSendMessages(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	msg, err := iggcon.NewIggyMessage([]byte("hello"))
	if err != nil {
		t.Fatalf("NewIggyMessage() error: %v", err)
	}

	err = client.SendMessages(mustIdentifier(1), mustIdentifier(1), iggcon.None(), []iggcon.IggyMessage{msg})
	if err != nil {
		t.Fatalf("SendMessages() unexpected error: %v", err)
	}
}

func TestPollMessages(t *testing.T) {
	payloadData := []byte("hello world")
	header := iggcon.MessageHeader{
		Checksum:         42,
		Offset:           0,
		Timestamp:        1000,
		OriginTimestamp:  2000,
		UserHeaderLength: 0,
		PayloadLength:    uint32(len(payloadData)),
	}
	headerBytes := header.ToBytes()

	pollPayload := make([]byte, 16+len(headerBytes)+len(payloadData))
	binary.LittleEndian.PutUint32(pollPayload[0:4], 1)   // partitionId
	binary.LittleEndian.PutUint64(pollPayload[4:12], 99) // currentOffset
	binary.LittleEndian.PutUint32(pollPayload[12:16], 1) // messagesCount
	copy(pollPayload[16:16+len(headerBytes)], headerBytes)
	copy(pollPayload[16+len(headerBytes):], payloadData)

	resp := buildResponse(0, pollPayload)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	partId := uint32(1)
	result, err := client.PollMessages(
		mustIdentifier(1),
		mustIdentifier(1),
		iggcon.DefaultConsumer(),
		iggcon.OffsetPollingStrategy(0),
		10,
		false,
		&partId,
	)
	if err != nil {
		t.Fatalf("PollMessages() error: %v", err)
	}
	if result.PartitionId != 1 {
		t.Fatalf("expected partitionId=1, got %d", result.PartitionId)
	}
	if result.CurrentOffset != 99 {
		t.Fatalf("expected currentOffset=99, got %d", result.CurrentOffset)
	}
	if len(result.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result.Messages))
	}
	if string(result.Messages[0].Payload) != "hello world" {
		t.Fatalf("expected payload 'hello world', got '%s'", string(result.Messages[0].Payload))
	}
}

func TestGetConsumerOffset(t *testing.T) {
	payload := buildOffsetPayload(1, 100, 50)
	resp := buildResponse(0, payload)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	partId := uint32(1)
	offset, err := client.GetConsumerOffset(iggcon.DefaultConsumer(), mustIdentifier(1), mustIdentifier(1), &partId)
	if err != nil {
		t.Fatalf("GetConsumerOffset() error: %v", err)
	}
	if offset.PartitionId != 1 {
		t.Fatalf("expected partitionId=1, got %d", offset.PartitionId)
	}
	if offset.CurrentOffset != 100 {
		t.Fatalf("expected currentOffset=100, got %d", offset.CurrentOffset)
	}
	if offset.StoredOffset != 50 {
		t.Fatalf("expected storedOffset=50, got %d", offset.StoredOffset)
	}
}

func TestStoreConsumerOffset(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	partId := uint32(1)
	err := client.StoreConsumerOffset(iggcon.DefaultConsumer(), mustIdentifier(1), mustIdentifier(1), 42, &partId)
	if err != nil {
		t.Fatalf("StoreConsumerOffset() unexpected error: %v", err)
	}
}

func TestDeleteConsumerOffset(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	partId := uint32(1)
	err := client.DeleteConsumerOffset(iggcon.DefaultConsumer(), mustIdentifier(1), mustIdentifier(1), &partId)
	if err != nil {
		t.Fatalf("DeleteConsumerOffset() unexpected error: %v", err)
	}
}

func TestGetConsumerGroups(t *testing.T) {
	cgPayload := buildConsumerGroupPayload(1, 3, 2, "group1")
	resp := buildResponse(0, cgPayload)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	groups, err := client.GetConsumerGroups(mustIdentifier(1), mustIdentifier(1))
	if err != nil {
		t.Fatalf("GetConsumerGroups() error: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 consumer group, got %d", len(groups))
	}
	if groups[0].Id != 1 {
		t.Fatalf("expected group id=1, got %d", groups[0].Id)
	}
	if groups[0].Name != "group1" {
		t.Fatalf("expected name='group1', got '%s'", groups[0].Name)
	}
	if groups[0].PartitionsCount != 3 {
		t.Fatalf("expected partitionsCount=3, got %d", groups[0].PartitionsCount)
	}
	if groups[0].MembersCount != 2 {
		t.Fatalf("expected membersCount=2, got %d", groups[0].MembersCount)
	}
}

func TestCreateConsumerGroup_ValidatesName(t *testing.T) {
	client := &IggyTcpClient{state: iggcon.StateConnected}

	_, err := client.CreateConsumerGroup(mustIdentifier(1), mustIdentifier(1), "")
	if !errors.Is(err, ierror.ErrInvalidConsumerGroupName) {
		t.Fatalf("expected ErrInvalidConsumerGroupName, got: %v", err)
	}
}

func TestJoinConsumerGroup(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.JoinConsumerGroup(mustIdentifier(1), mustIdentifier(1), mustIdentifier(1))
	if err != nil {
		t.Fatalf("JoinConsumerGroup() unexpected error: %v", err)
	}
}

func TestLeaveConsumerGroup(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.LeaveConsumerGroup(mustIdentifier(1), mustIdentifier(1), mustIdentifier(1))
	if err != nil {
		t.Fatalf("LeaveConsumerGroup() unexpected error: %v", err)
	}
}

func TestDeleteConsumerGroup(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.DeleteConsumerGroup(mustIdentifier(1), mustIdentifier(1), mustIdentifier(1))
	if err != nil {
		t.Fatalf("DeleteConsumerGroup() unexpected error: %v", err)
	}
}

func TestLogoutUser(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.LogoutUser()
	if err != nil {
		t.Fatalf("LogoutUser() unexpected error: %v", err)
	}
}

func TestGetClients(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	clients, err := client.GetClients()
	if err != nil {
		t.Fatalf("GetClients() error: %v", err)
	}
	if len(clients) != 0 {
		t.Fatalf("expected 0 clients, got %d", len(clients))
	}
}

func TestGetClient(t *testing.T) {
	payload := buildClientInfoPayload(42, 7, 1, "192.168.1.1:5000", 0)
	resp := buildResponse(0, payload)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	info, err := client.GetClient(42)
	if err != nil {
		t.Fatalf("GetClient() error: %v", err)
	}
	if info.ClientInfo.ID != 42 {
		t.Fatalf("expected client id=42, got %d", info.ClientInfo.ID)
	}
	if info.ClientInfo.UserID != 7 {
		t.Fatalf("expected userId=7, got %d", info.ClientInfo.UserID)
	}
	if info.ClientInfo.Address != "192.168.1.1:5000" {
		t.Fatalf("expected address='192.168.1.1:5000', got '%s'", info.ClientInfo.Address)
	}
	if info.ClientInfo.Transport != string(iggcon.Tcp) {
		t.Fatalf("expected transport='tcp', got '%s'", info.ClientInfo.Transport)
	}
}

func TestGetConnectionInfo(t *testing.T) {
	client := &IggyTcpClient{
		state:                iggcon.StateConnected,
		currentServerAddress: "10.0.0.1:9090",
	}

	info := client.GetConnectionInfo()
	if info.Protocol != iggcon.Tcp {
		t.Fatalf("expected protocol=Tcp, got %s", info.Protocol)
	}
	if info.ServerAddress != "10.0.0.1:9090" {
		t.Fatalf("expected address='10.0.0.1:9090', got '%s'", info.ServerAddress)
	}
}

func TestClose(t *testing.T) {
	_, clientConn := net.Pipe()
	client := &IggyTcpClient{
		conn:  clientConn,
		state: iggcon.StateConnected,
	}

	err := client.Close()
	if err != nil {
		t.Fatalf("Close() unexpected error: %v", err)
	}
	if client.state != iggcon.StateShutdown {
		t.Fatalf("expected state=Shutdown after Close, got %s", client.state)
	}
}

func TestCreatePayload(t *testing.T) {
	message := []byte{0xAA, 0xBB}
	result := createPayload(message, command.PingCode)

	expectedLen := uint32(len(message) + 4)
	gotLen := binary.LittleEndian.Uint32(result[0:4])
	if gotLen != expectedLen {
		t.Fatalf("expected message length=%d, got %d", expectedLen, gotLen)
	}

	gotCmd := binary.LittleEndian.Uint32(result[4:8])
	if gotCmd != uint32(command.PingCode) {
		t.Fatalf("expected command code=%d, got %d", command.PingCode, gotCmd)
	}

	if result[8] != 0xAA || result[9] != 0xBB {
		t.Fatalf("expected message bytes [0xAA, 0xBB], got [0x%X, 0x%X]", result[8], result[9])
	}

	if len(result) != 10 {
		t.Fatalf("expected total length=10, got %d", len(result))
	}
}

func TestGetDefaultOptions(t *testing.T) {
	opts := GetDefaultOptions()
	if opts.config.serverAddress != "127.0.0.1:8090" {
		t.Fatalf("expected default address='127.0.0.1:8090', got '%s'", opts.config.serverAddress)
	}
	if opts.config.tlsEnabled {
		t.Fatalf("expected TLS disabled by default")
	}
}

func TestWithServerAddress(t *testing.T) {
	opts := GetDefaultOptions()
	opt := WithServerAddress("10.0.0.1:9999")
	opt(&opts)

	if opts.config.serverAddress != "10.0.0.1:9999" {
		t.Fatalf("expected address='10.0.0.1:9999', got '%s'", opts.config.serverAddress)
	}
}

func TestWithTLS(t *testing.T) {
	opts := GetDefaultOptions()
	opt := WithTLS()
	opt(&opts)

	if !opts.config.tlsEnabled {
		t.Fatalf("expected TLS to be enabled after WithTLS()")
	}
}

func TestNewCredentials(t *testing.T) {
	creds := NewUsernamePasswordCredentials("admin", "secret")
	if creds.username != "admin" {
		t.Fatalf("expected username='admin', got '%s'", creds.username)
	}
	if creds.password != "secret" {
		t.Fatalf("expected password='secret', got '%s'", creds.password)
	}

	tokenCreds := NewPersonalAccessTokenCredentials("my-token-123")
	if tokenCreds.personalAccessToken != "my-token-123" {
		t.Fatalf("expected token='my-token-123', got '%s'", tokenCreds.personalAccessToken)
	}
}

func TestCreatePersonalAccessToken(t *testing.T) {
	tokenStr := "abc123def456"
	tokenPayload := make([]byte, 1+len(tokenStr))
	tokenPayload[0] = byte(len(tokenStr))
	copy(tokenPayload[1:], tokenStr)
	resp := buildResponse(0, tokenPayload)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	token, err := client.CreatePersonalAccessToken("mytoken", 3600)
	if err != nil {
		t.Fatalf("CreatePersonalAccessToken() error: %v", err)
	}
	if token.Token != "abc123def456" {
		t.Fatalf("expected token='abc123def456', got '%s'", token.Token)
	}
}

func TestDeletePersonalAccessToken(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	err := client.DeletePersonalAccessToken("mytoken")
	if err != nil {
		t.Fatalf("DeletePersonalAccessToken() unexpected error: %v", err)
	}
}

func TestGetPersonalAccessTokens(t *testing.T) {
	name := "tkn"
	entry := make([]byte, 1+len(name)+8)
	entry[0] = byte(len(name))
	copy(entry[1:], name)
	binary.LittleEndian.PutUint64(entry[1+len(name):], 5000000)
	resp := buildResponse(0, entry)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	tokens, err := client.GetPersonalAccessTokens()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Name != "tkn" {
		t.Fatalf("expected token name 'tkn', got '%s'", tokens[0].Name)
	}
}

func TestGetConsumerGroup(t *testing.T) {
	name := "cg-1"
	buf := make([]byte, 13+len(name))
	binary.LittleEndian.PutUint32(buf[0:], 5)
	binary.LittleEndian.PutUint32(buf[4:], 2)
	binary.LittleEndian.PutUint32(buf[8:], 0) // no members
	buf[12] = byte(len(name))
	copy(buf[13:], name)
	resp := buildResponse(0, buf)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	sid, _ := iggcon.NewIdentifier(uint32(1))
	tid, _ := iggcon.NewIdentifier(uint32(2))
	gid, _ := iggcon.NewIdentifier(uint32(5))
	result, err := client.GetConsumerGroup(sid, tid, gid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ConsumerGroup.Name != "cg-1" {
		t.Fatalf("expected name 'cg-1', got '%s'", result.ConsumerGroup.Name)
	}
}

func TestGetConsumerGroup_EmptyBuffer(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	sid, _ := iggcon.NewIdentifier(uint32(1))
	tid, _ := iggcon.NewIdentifier(uint32(2))
	gid, _ := iggcon.NewIdentifier(uint32(5))
	_, err := client.GetConsumerGroup(sid, tid, gid)
	if !errors.Is(err, ierror.ErrConsumerGroupIdNotFound) {
		t.Fatalf("expected ErrConsumerGroupIdNotFound, got %v", err)
	}
}

func TestWithTLSValidateCertificate(t *testing.T) {
	opts := GetDefaultOptions()
	WithTLS(WithTLSValidateCertificate(false))(&opts)
	if opts.config.tls.tlsValidateCertificate {
		t.Fatalf("expected validate=false")
	}
	if !opts.config.tlsEnabled {
		t.Fatalf("expected TLS enabled")
	}
}

func TestNewAutoLogin(t *testing.T) {
	creds := NewUsernamePasswordCredentials("user", "pass")
	al := NewAutoLogin(creds)
	if !al.enabled {
		t.Fatalf("expected enabled=true")
	}
	if al.credentials.username != "user" {
		t.Fatalf("expected username 'user', got '%s'", al.credentials.username)
	}
	if al.credentials.password != "pass" {
		t.Fatalf("expected password 'pass', got '%s'", al.credentials.password)
	}
}

func TestDisconnect(t *testing.T) {
	_, clientConn := net.Pipe()
	client := &IggyTcpClient{
		conn:  clientConn,
		state: iggcon.StateConnected,
	}
	err := client.disconnect()
	if err != nil {
		t.Fatalf("disconnect() unexpected error: %v", err)
	}
	if client.state != iggcon.StateDisconnected {
		t.Fatalf("expected state Disconnected")
	}
}

func TestDisconnect_AlreadyDisconnected(t *testing.T) {
	client := &IggyTcpClient{
		state: iggcon.StateDisconnected,
	}
	err := client.disconnect()
	if err != nil {
		t.Fatalf("disconnect() unexpected error: %v", err)
	}
}

func TestShutdown_AlreadyShutdown(t *testing.T) {
	client := &IggyTcpClient{
		state: iggcon.StateShutdown,
	}
	err := client.shutdown()
	if err != nil {
		t.Fatalf("shutdown() unexpected error: %v", err)
	}
}

func newMultiResponseTestClient(responses [][]byte) (*IggyTcpClient, func()) {
	serverConn, clientConn := net.Pipe()

	client := &IggyTcpClient{
		conn:                 clientConn,
		state:                iggcon.StateConnected,
		currentServerAddress: "127.0.0.1:8090",
		clientAddress:        "127.0.0.1:12345",
		config:               defaultTcpClientConfig(),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, resp := range responses {
			lenBuf := make([]byte, 4)
			if _, err := serverConn.Read(lenBuf); err != nil {
				return
			}
			msgLen := int(binary.LittleEndian.Uint32(lenBuf))
			remaining := make([]byte, msgLen)
			total := 0
			for total < msgLen {
				n, err := serverConn.Read(remaining[total:])
				if err != nil {
					return
				}
				total += n
			}
			if _, err := serverConn.Write(resp); err != nil {
				return
			}
		}
	}()

	cleanup := func() {
		clientConn.Close()
		serverConn.Close()
		<-done
	}
	return client, cleanup
}

func TestLoginUser(t *testing.T) {
	identityPayload := make([]byte, 4)
	binary.LittleEndian.PutUint32(identityPayload[0:], 42)
	loginResp := buildResponse(0, identityPayload)

	// GetClusterMetadata response: nameLen(4) + name + nodesCount(4)=0
	clusterName := "c"
	metaPayload := make([]byte, 4+len(clusterName)+4)
	binary.LittleEndian.PutUint32(metaPayload[0:], uint32(len(clusterName)))
	copy(metaPayload[4:], clusterName)
	binary.LittleEndian.PutUint32(metaPayload[4+len(clusterName):], 0)
	metaResp := buildResponse(0, metaPayload)

	client, cleanup := newMultiResponseTestClient([][]byte{loginResp, metaResp})
	defer cleanup()

	identity, err := client.LoginUser("admin", "password")
	if err != nil {
		t.Fatalf("LoginUser() unexpected error: %v", err)
	}
	if identity.UserId != 42 {
		t.Fatalf("expected userId=42, got %d", identity.UserId)
	}
}

func TestLoginWithPersonalAccessToken(t *testing.T) {
	identityPayload := make([]byte, 4)
	binary.LittleEndian.PutUint32(identityPayload[0:], 7)
	loginResp := buildResponse(0, identityPayload)

	clusterName := "c"
	metaPayload := make([]byte, 4+len(clusterName)+4)
	binary.LittleEndian.PutUint32(metaPayload[0:], uint32(len(clusterName)))
	copy(metaPayload[4:], clusterName)
	binary.LittleEndian.PutUint32(metaPayload[4+len(clusterName):], 0)
	metaResp := buildResponse(0, metaPayload)

	client, cleanup := newMultiResponseTestClient([][]byte{loginResp, metaResp})
	defer cleanup()

	identity, err := client.LoginWithPersonalAccessToken("my-token")
	if err != nil {
		t.Fatalf("LoginWithPersonalAccessToken() unexpected error: %v", err)
	}
	if identity.UserId != 7 {
		t.Fatalf("expected userId=7, got %d", identity.UserId)
	}
}

func TestGetUser(t *testing.T) {
	name := "admin"
	userPart := make([]byte, 14+len(name))
	binary.LittleEndian.PutUint32(userPart[0:], 1)
	binary.LittleEndian.PutUint64(userPart[4:], 999)
	userPart[12] = 1 // Active
	userPart[13] = byte(len(name))
	copy(userPart[14:], name)

	payload := append(userPart, 0) // hasPermissions=0
	resp := buildResponse(0, payload)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	uid, _ := iggcon.NewIdentifier(uint32(1))
	result, err := client.GetUser(uid)
	if err != nil {
		t.Fatalf("GetUser() unexpected error: %v", err)
	}
	if result.UserInfo.Username != "admin" {
		t.Fatalf("expected username 'admin', got '%s'", result.UserInfo.Username)
	}
}

func TestGetUser_EmptyBuffer(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	uid, _ := iggcon.NewIdentifier(uint32(1))
	_, err := client.GetUser(uid)
	if !errors.Is(err, ierror.ErrResourceNotFound) {
		t.Fatalf("expected ErrResourceNotFound, got %v", err)
	}
}

func TestGetUsers(t *testing.T) {
	name := "user1"
	buf := make([]byte, 14+len(name))
	binary.LittleEndian.PutUint32(buf[0:], 10)
	binary.LittleEndian.PutUint64(buf[4:], 500)
	buf[12] = 1
	buf[13] = byte(len(name))
	copy(buf[14:], name)
	resp := buildResponse(0, buf)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	result, err := client.GetUsers()
	if err != nil {
		t.Fatalf("GetUsers() unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 user, got %d", len(result))
	}
	if result[0].Username != "user1" {
		t.Fatalf("expected 'user1', got '%s'", result[0].Username)
	}
}

func TestCreateUser(t *testing.T) {
	name := "newuser"
	buf := make([]byte, 14+len(name)+1)
	binary.LittleEndian.PutUint32(buf[0:], 5)
	binary.LittleEndian.PutUint64(buf[4:], 300)
	buf[12] = 1
	buf[13] = byte(len(name))
	copy(buf[14:], name)
	buf[14+len(name)] = 0 // no permissions
	resp := buildResponse(0, buf)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	result, err := client.CreateUser("newuser", "pass123", iggcon.Active, nil)
	if err != nil {
		t.Fatalf("CreateUser() unexpected error: %v", err)
	}
	if result.UserInfo.Username != "newuser" {
		t.Fatalf("expected 'newuser', got '%s'", result.UserInfo.Username)
	}
}

func TestUpdateUser(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	uid, _ := iggcon.NewIdentifier(uint32(1))
	newName := "updated"
	err := client.UpdateUser(uid, &newName, nil)
	if err != nil {
		t.Fatalf("UpdateUser() unexpected error: %v", err)
	}
}

func TestDeleteUser(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	uid, _ := iggcon.NewIdentifier(uint32(1))
	err := client.DeleteUser(uid)
	if err != nil {
		t.Fatalf("DeleteUser() unexpected error: %v", err)
	}
}

func TestUpdatePermissions(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	uid, _ := iggcon.NewIdentifier(uint32(1))
	err := client.UpdatePermissions(uid, nil)
	if err != nil {
		t.Fatalf("UpdatePermissions() unexpected error: %v", err)
	}
}

func TestChangePassword(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	uid, _ := iggcon.NewIdentifier(uint32(1))
	err := client.ChangePassword(uid, "old", "new")
	if err != nil {
		t.Fatalf("ChangePassword() unexpected error: %v", err)
	}
}

func TestGetClusterMetadata(t *testing.T) {
	// Build ClusterMetadata binary: nameLen(4) + name + nodesCount(4)
	name := "cluster-1"
	buf := make([]byte, 4+len(name)+4)
	binary.LittleEndian.PutUint32(buf[0:], uint32(len(name)))
	copy(buf[4:], name)
	binary.LittleEndian.PutUint32(buf[4+len(name):], 0) // 0 nodes
	resp := buildResponse(0, buf)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	result, err := client.GetClusterMetadata()
	if err != nil {
		t.Fatalf("GetClusterMetadata() unexpected error: %v", err)
	}
	if result.Name != "cluster-1" {
		t.Fatalf("expected name 'cluster-1', got '%s'", result.Name)
	}
	if len(result.Nodes) != 0 {
		t.Fatalf("expected 0 nodes, got %d", len(result.Nodes))
	}
}

func TestUpdateTopic(t *testing.T) {
	resp := buildResponse(0, nil)
	client, cleanup := newTestClient(resp)
	defer cleanup()

	sid, _ := iggcon.NewIdentifier(uint32(1))
	tid, _ := iggcon.NewIdentifier(uint32(2))
	err := client.UpdateTopic(sid, tid, "newname", 0, 0, 0, nil)
	if err != nil {
		t.Fatalf("UpdateTopic() unexpected error: %v", err)
	}
}

func TestUpdateTopic_InvalidName(t *testing.T) {
	client := &IggyTcpClient{}
	sid, _ := iggcon.NewIdentifier(uint32(1))
	tid, _ := iggcon.NewIdentifier(uint32(2))
	err := client.UpdateTopic(sid, tid, "", 0, 0, 0, nil)
	if !errors.Is(err, ierror.ErrInvalidTopicName) {
		t.Fatalf("expected ErrInvalidTopicName, got %v", err)
	}
}

func TestUpdateTopic_InvalidReplicationFactor(t *testing.T) {
	client := &IggyTcpClient{}
	sid, _ := iggcon.NewIdentifier(uint32(1))
	tid, _ := iggcon.NewIdentifier(uint32(2))
	zero := uint8(0)
	err := client.UpdateTopic(sid, tid, "name", 0, 0, 0, &zero)
	if !errors.Is(err, ierror.ErrInvalidReplicationFactor) {
		t.Fatalf("expected ErrInvalidReplicationFactor, got %v", err)
	}
}

func TestSendMessages_InvalidPartitioning(t *testing.T) {
	client := &IggyTcpClient{}
	sid, _ := iggcon.NewIdentifier(uint32(1))
	tid, _ := iggcon.NewIdentifier(uint32(2))
	msgs := make([]iggcon.IggyMessage, 1)
	msgs[0], _ = iggcon.NewIggyMessage([]byte("data"))

	longValue := make([]byte, 256)
	badPart := iggcon.Partitioning{Kind: iggcon.PartitionIdKind, Value: longValue}
	err := client.SendMessages(sid, tid, badPart, msgs)
	if !errors.Is(err, ierror.ErrInvalidKeyValueLength) {
		t.Fatalf("expected ErrInvalidKeyValueLength, got %v", err)
	}
}

func TestWriteError(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	client := &IggyTcpClient{
		conn:  clientConn,
		state: iggcon.StateConnected,
	}
	clientConn.Close()
	serverConn.Close()

	_, err := client.sendAndFetchResponse([]byte{0}, command.PingCode)
	if err == nil {
		t.Fatalf("expected write error")
	}
}
