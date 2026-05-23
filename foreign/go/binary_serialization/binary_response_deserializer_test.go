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
	"fmt"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func buildFetchPayload(payloadBody []byte) []byte {
	header := iggcon.NewMessageHeader(iggcon.MessageID{}, uint32(len(payloadBody)), 0)
	headerBytes := header.ToBytes()

	// 4 (partitionId) + 8 (currentOffset) + 4 (messagesCount) + header + body
	buf := make([]byte, 16+len(headerBytes)+len(payloadBody))
	binary.LittleEndian.PutUint32(buf[0:4], 1)
	binary.LittleEndian.PutUint64(buf[4:12], 0)
	binary.LittleEndian.PutUint32(buf[12:16], 1)
	copy(buf[16:], headerBytes)
	copy(buf[16+len(headerBytes):], payloadBody)
	return buf
}

func TestDeserializeFetchMessages_MalformedS2ReturnsError(t *testing.T) {
	garbage := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03,
		0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8,
		0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
		0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0, 0xF0, 0x00}
	payload := buildFetchPayload(garbage)

	result, err := DeserializeFetchMessagesResponse(payload, iggcon.MESSAGE_COMPRESSION_S2)

	if result != nil {
		t.Errorf("expected nil result, got %+v", result)
	}
	if err == nil {
		t.Fatal("expected error for malformed S2 payload, got nil")
	}
	if !strings.Contains(err.Error(), "failed to decode s2 payload") {
		t.Errorf("error message %q does not mention S2 decode failure", err.Error())
	}
}

func TestDeserializeFetchMessages_NoCompressionSkipsS2(t *testing.T) {
	body := []byte("hello world")
	payload := buildFetchPayload(body)

	result, err := DeserializeFetchMessagesResponse(payload, iggcon.MESSAGE_COMPRESSION_NONE)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result.Messages))
	}
	if string(result.Messages[0].Payload) != "hello world" {
		t.Errorf("payload = %q, want %q", result.Messages[0].Payload, "hello world")
	}
}

func TestDeserializeFetchMessages_EmptyPayload(t *testing.T) {
	result, err := DeserializeFetchMessagesResponse([]byte{}, iggcon.MESSAGE_COMPRESSION_S2)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Messages) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(result.Messages))
	}
}

func encodeStream(id uint32, createdAt uint64, topicsCount uint32, sizeBytes, messagesCount uint64, name string) []byte {
	nameBytes := []byte(name)
	buf := make([]byte, streamFixedSize+len(nameBytes))
	binary.LittleEndian.PutUint32(buf[0:4], id)
	binary.LittleEndian.PutUint64(buf[4:12], createdAt)
	binary.LittleEndian.PutUint32(buf[12:16], topicsCount)
	binary.LittleEndian.PutUint64(buf[16:24], sizeBytes)
	binary.LittleEndian.PutUint64(buf[24:32], messagesCount)
	buf[32] = byte(len(nameBytes))
	copy(buf[33:], nameBytes)
	return buf
}

func assertStream(t *testing.T, label string, got iggcon.Stream, wantId uint32, wantCreatedAt uint64, wantTopicsCount uint32, wantSizeBytes, wantMessagesCount uint64, wantName string) {
	t.Helper()
	if got.Id != wantId {
		t.Errorf("%s.Id = %d, want %d", label, got.Id, wantId)
	}
	if got.CreatedAt != wantCreatedAt {
		t.Errorf("%s.CreatedAt = %d, want %d", label, got.CreatedAt, wantCreatedAt)
	}
	if got.TopicsCount != wantTopicsCount {
		t.Errorf("%s.TopicsCount = %d, want %d", label, got.TopicsCount, wantTopicsCount)
	}
	if got.SizeBytes != wantSizeBytes {
		t.Errorf("%s.SizeBytes = %d, want %d", label, got.SizeBytes, wantSizeBytes)
	}
	if got.MessagesCount != wantMessagesCount {
		t.Errorf("%s.MessagesCount = %d, want %d", label, got.MessagesCount, wantMessagesCount)
	}
	if got.Name != wantName {
		t.Errorf("%s.Name = %q, want %q", label, got.Name, wantName)
	}
}

func TestDeserializeToStream_SingleStream(t *testing.T) {
	payload := encodeStream(42, 1_710_000_000, 5, 2048, 100, "my-stream")

	stream, readBytes, err := DeserializeToStream(payload, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if readBytes != len(payload) {
		t.Fatalf("readBytes = %d, want %d", readBytes, len(payload))
	}
	assertStream(t, "stream", stream, 42, 1_710_000_000, 5, 2048, 100, "my-stream")
}

func TestDeserializeToStream_TruncatedHeader(t *testing.T) {
	payload := make([]byte, streamFixedSize-1)
	_, _, err := DeserializeToStream(payload, 0)
	if err == nil {
		t.Fatal("expected error for truncated header, got nil")
	}
}

func TestDeserializeToStream_TruncatedName(t *testing.T) {
	buf := make([]byte, streamFixedSize)
	buf[32] = 10
	_, _, err := DeserializeToStream(buf, 0)
	if err == nil {
		t.Fatal("expected error for truncated name, got nil")
	}
}

func TestDeserializeStreams_Empty(t *testing.T) {
	streams, err := DeserializeStreams([]byte{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(streams) != 0 {
		t.Fatalf("expected 0 streams, got %d", len(streams))
	}
}

func TestDeserializeStreams_MultipleStreams(t *testing.T) {
	var payload []byte
	payload = append(payload, encodeStream(1, 100, 2, 512, 50, "stream-one")...)
	payload = append(payload, encodeStream(2, 200, 0, 0, 0, "s2")...)
	payload = append(payload, encodeStream(3, 300, 1, 1024, 10, "third")...)

	streams, err := DeserializeStreams(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(streams) != 3 {
		t.Fatalf("expected 3 streams, got %d", len(streams))
	}

	assertStream(t, "stream[0]", streams[0], 1, 100, 2, 512, 50, "stream-one")
	assertStream(t, "stream[1]", streams[1], 2, 200, 0, 0, 0, "s2")
	assertStream(t, "stream[2]", streams[2], 3, 300, 1, 1024, 10, "third")
}

func TestDeserializeStreams_CorruptedPayload(t *testing.T) {
	good := encodeStream(1, 100, 0, 0, 0, "ok")
	truncated := make([]byte, streamFixedSize-5)
	payload := append(good, truncated...)

	_, err := DeserializeStreams(payload)
	if err == nil {
		t.Fatal("expected error for corrupted payload, got nil")
	}
}

// Regression test for issue #3130: payloads > 64KB produced corrupted
// stream lists because no bounds checking was performed.
func TestDeserializeStreams_LargePayloadOver64KB(t *testing.T) {
	const targetSize = 70_000
	var payload []byte
	var id uint32

	for len(payload) < targetSize {
		id++
		name := fmt.Sprintf("stream-with-a-longer-name-for-padding-%d", id)
		payload = append(payload, encodeStream(id, uint64(id)*1000, id%10, uint64(id)*512, uint64(id)*5, name)...)
	}

	if len(payload) <= 1<<16 {
		t.Fatalf("payload size %d is not > 64KB; increase stream count or name length", len(payload))
	}

	streams, err := DeserializeStreams(payload)
	if err != nil {
		t.Fatalf("unexpected error deserializing %d-byte payload: %v", len(payload), err)
	}

	if uint32(len(streams)) != id {
		t.Fatalf("expected %d streams, got %d", id, len(streams))
	}

	for i, s := range streams {
		expectedId := uint32(i + 1)
		expectedName := fmt.Sprintf("stream-with-a-longer-name-for-padding-%d", expectedId)
		assertStream(t, fmt.Sprintf("stream[%d]", i), s,
			expectedId, uint64(expectedId)*1000, expectedId%10,
			uint64(expectedId)*512, uint64(expectedId)*5, expectedName)
	}
}

func TestDeserializeStreams_MaxLengthName(t *testing.T) {
	name := make([]byte, 255)
	for i := range name {
		name[i] = 'a' + byte(i%26)
	}
	payload := encodeStream(1, 999, 3, 4096, 200, string(name))

	streams, err := DeserializeStreams(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if streams[0].Name != string(name) {
		t.Errorf("Name length = %d, want 255", len(streams[0].Name))
	}
}
