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

package iggcon

import (
	"bytes"
	"testing"
)

func TestMessageHeader_ToBytesFromBytes_RoundTrip(t *testing.T) {
	var id MessageID
	copy(id[:], []byte("1234567890abcdef"))

	header := MessageHeader{
		Checksum:         55,
		Id:               id,
		Offset:           123,
		Timestamp:        456,
		OriginTimestamp:  789,
		UserHeaderLength: 4,
		PayloadLength:    6,
		Reserved:         999,
	}

	encoded := header.ToBytes()
	if len(encoded) != MessageHeaderSize {
		t.Fatalf("invalid encoded length, expected %d got %d", MessageHeaderSize, len(encoded))
	}

	decoded, err := MessageHeaderFromBytes(encoded)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if decoded.Checksum != header.Checksum {
		t.Fatalf("checksum mismatch, expected %d got %d", header.Checksum, decoded.Checksum)
	}
	if !bytes.Equal(decoded.Id[:], header.Id[:]) {
		t.Fatalf("id mismatch, expected %v got %v", header.Id, decoded.Id)
	}
	if decoded.Offset != header.Offset {
		t.Fatalf("offset mismatch, expected %d got %d", header.Offset, decoded.Offset)
	}
	if decoded.Timestamp != header.Timestamp {
		t.Fatalf("timestamp mismatch, expected %d got %d", header.Timestamp, decoded.Timestamp)
	}
	if decoded.OriginTimestamp != header.OriginTimestamp {
		t.Fatalf("origin timestamp mismatch, expected %d got %d", header.OriginTimestamp, decoded.OriginTimestamp)
	}
	if decoded.UserHeaderLength != header.UserHeaderLength {
		t.Fatalf("user header length mismatch, expected %d got %d", header.UserHeaderLength, decoded.UserHeaderLength)
	}
	if decoded.PayloadLength != header.PayloadLength {
		t.Fatalf("payload length mismatch, expected %d got %d", header.PayloadLength, decoded.PayloadLength)
	}
	if decoded.Reserved != header.Reserved {
		t.Fatalf("reserved mismatch, expected %d got %d", header.Reserved, decoded.Reserved)
	}
}

func TestMessageHeaderFromBytes_InvalidLength(t *testing.T) {
	_, err := MessageHeaderFromBytes(make([]byte, MessageHeaderSize-1))
	if err == nil {
		t.Fatal("expected error for invalid message header size")
	}
}

func TestNewMessageHeader_SetsExpectedFields(t *testing.T) {
	var id MessageID
	copy(id[:], []byte("abcdefghijklmnop"))

	header := NewMessageHeader(id, 8, 3)
	if !bytes.Equal(header.Id[:], id[:]) {
		t.Fatalf("id mismatch, expected %v got %v", id, header.Id)
	}
	if header.PayloadLength != 8 {
		t.Fatalf("payload length mismatch, expected 8 got %d", header.PayloadLength)
	}
	if header.UserHeaderLength != 3 {
		t.Fatalf("user header length mismatch, expected 3 got %d", header.UserHeaderLength)
	}
	if header.OriginTimestamp == 0 {
		t.Fatal("expected origin timestamp to be populated")
	}
}
