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
	"testing"
)

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

func TestDeserializeToStream_SingleStream(t *testing.T) {
	payload := encodeStream(42, 1_710_000_000, 5, 2048, 100, "my-stream")

	stream, readBytes, err := DeserializeToStream(payload, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if readBytes != len(payload) {
		t.Fatalf("readBytes = %d, want %d", readBytes, len(payload))
	}
	if stream.Id != 42 {
		t.Errorf("Id = %d, want 42", stream.Id)
	}
	if stream.CreatedAt != 1_710_000_000 {
		t.Errorf("CreatedAt = %d, want 1710000000", stream.CreatedAt)
	}
	if stream.TopicsCount != 5 {
		t.Errorf("TopicsCount = %d, want 5", stream.TopicsCount)
	}
	if stream.SizeBytes != 2048 {
		t.Errorf("SizeBytes = %d, want 2048", stream.SizeBytes)
	}
	if stream.MessagesCount != 100 {
		t.Errorf("MessagesCount = %d, want 100", stream.MessagesCount)
	}
	if stream.Name != "my-stream" {
		t.Errorf("Name = %q, want %q", stream.Name, "my-stream")
	}
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

	if streams[0].Id != 1 || streams[0].Name != "stream-one" {
		t.Errorf("stream[0] = {Id:%d, Name:%q}, want {Id:1, Name:\"stream-one\"}", streams[0].Id, streams[0].Name)
	}
	if streams[1].Id != 2 || streams[1].Name != "s2" {
		t.Errorf("stream[1] = {Id:%d, Name:%q}, want {Id:2, Name:\"s2\"}", streams[1].Id, streams[1].Name)
	}
	if streams[2].Id != 3 || streams[2].Name != "third" {
		t.Errorf("stream[2] = {Id:%d, Name:%q}, want {Id:3, Name:\"third\"}", streams[2].Id, streams[2].Name)
	}
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
		if s.Id != expectedId {
			t.Errorf("stream[%d].Id = %d, want %d", i, s.Id, expectedId)
		}
		if s.Name != expectedName {
			t.Errorf("stream[%d].Name = %q, want %q", i, s.Name, expectedName)
		}
		if s.CreatedAt != uint64(expectedId)*1000 {
			t.Errorf("stream[%d].CreatedAt = %d, want %d", i, s.CreatedAt, uint64(expectedId)*1000)
		}
		if s.TopicsCount != expectedId%10 {
			t.Errorf("stream[%d].TopicsCount = %d, want %d", i, s.TopicsCount, expectedId%10)
		}
		if s.SizeBytes != uint64(expectedId)*512 {
			t.Errorf("stream[%d].SizeBytes = %d, want %d", i, s.SizeBytes, uint64(expectedId)*512)
		}
		if s.MessagesCount != uint64(expectedId)*5 {
			t.Errorf("stream[%d].MessagesCount = %d, want %d", i, s.MessagesCount, uint64(expectedId)*5)
		}
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
