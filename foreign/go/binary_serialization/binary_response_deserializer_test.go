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
