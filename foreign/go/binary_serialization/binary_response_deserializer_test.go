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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

func buildFetchPayload(payloadBody []byte) []byte {
	buf := binary.LittleEndian.AppendUint32(nil, 1)
	buf = binary.LittleEndian.AppendUint64(buf, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 1)
	buf = append(buf, make([]byte, iggcon.PartitionContextSize)...)
	return appendBatchRecord(buf, 0, 0, 0, batchFrame{payload: payloadBody})
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
	return encodeStreamWithOptions(id, createdAt, topicsCount, sizeBytes, messagesCount, name, nil, nil)
}

// encodeStreamWithOptions appends the explicit and derived options blocks,
// each behind its u32 length prefix.
func encodeStreamWithOptions(id uint32, createdAt uint64, topicsCount uint32, sizeBytes, messagesCount uint64, name string, explicit, derived []byte) []byte {
	nameBytes := []byte(name)
	buf := make([]byte, streamFixedSize+len(nameBytes))
	binary.LittleEndian.PutUint32(buf[0:4], id)
	binary.LittleEndian.PutUint64(buf[4:12], createdAt)
	binary.LittleEndian.PutUint32(buf[12:16], topicsCount)
	binary.LittleEndian.PutUint64(buf[16:24], sizeBytes)
	binary.LittleEndian.PutUint64(buf[24:32], messagesCount)
	buf[32] = byte(len(nameBytes))
	copy(buf[33:], nameBytes)
	for _, block := range [][]byte{explicit, derived} {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(block)))
		buf = append(buf, block...)
	}
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

func TestDeserializeToStream_TruncatedOptions(t *testing.T) {
	full := encodeStream(1, 100, 0, 0, 0, "s")
	for cut := 1; cut <= 8; cut++ {
		_, _, err := DeserializeToStream(full[:len(full)-cut], 0)
		if err == nil {
			t.Fatalf("expected error for options prefix cut by %d bytes, got nil", cut)
		}
	}
}

func TestDeserializeToStream_WithOptions(t *testing.T) {
	options := iggcon.GetHeadersBytes([]iggcon.HeaderEntry{{
		Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("future_key")},
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("value")},
	}})
	derived := iggcon.GetHeadersBytes([]iggcon.HeaderEntry{{
		Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("encryption_key_source")},
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("server")},
	}})
	payload := encodeStreamWithOptions(7, 500, 0, 0, 0, "with-options", options, derived)

	stream, readBytes, err := DeserializeToStream(payload, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if readBytes != len(payload) {
		t.Fatalf("readBytes = %d, want %d", readBytes, len(payload))
	}
	value, ok := stream.Options["future_key"]
	if !ok {
		t.Fatalf("Options = %v, want key %q", stream.Options, "future_key")
	}
	if value.Kind != iggcon.String || string(value.Value) != "value" {
		t.Errorf("Options[future_key] = %+v, want String %q", value, "value")
	}
	if _, explicit := stream.Options["encryption_key_source"]; explicit {
		t.Errorf("Options = %v, a derived key must stay out of the explicit map", stream.Options)
	}
	if source, ok := stream.EncryptionKeySource(); !ok || source != iggcon.StreamEncryptionSourceServer {
		t.Errorf("EncryptionKeySource = %q, %t, want %q from the derived block", source, ok, iggcon.StreamEncryptionSourceServer)
	}
}

func TestReadLengthPrefixed_RejectsALengthPastTheBuffer(t *testing.T) {
	// 0xFFFFFFFF is the case a 64-bit-only reading of the guard misses: where
	// int is 32 bits it converts to -1, and without the negative branch the
	// remaining-bytes check passes and the slice panics.
	lengths := []uint32{0x7FFFFFFF, 0x80000000, 0xFFFFFFFF}
	for _, length := range lengths {
		payload := binary.LittleEndian.AppendUint32(nil, length)
		payload = append(payload, "value"...)

		block, consumed, err := readLengthPrefixed(payload, 0, "field")
		if err == nil {
			t.Errorf("length 0x%X: expected an error, got block %v consuming %d bytes",
				length, block, consumed)
		}
	}
}

func TestDeserializeToStream_RejectsAnOptionsLengthPastTheBuffer(t *testing.T) {
	for _, prefix := range []string{"explicit", "derived"} {
		payload := encodeStream(1, 100, 0, 0, 0, "s")
		offset := len(payload) - 4
		if prefix == "explicit" {
			offset -= 4
		}
		binary.LittleEndian.PutUint32(payload[offset:], 0xFFFFFFFF)

		if _, _, err := DeserializeToStream(payload, 0); err == nil {
			t.Fatalf("expected an error for a %s options length of 0xFFFFFFFF, got nil", prefix)
		}
	}
}

func TestDeserializeStreamsPage_Empty(t *testing.T) {
	page, err := DeserializeStreamsPage(streamsPage())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 0 || page.Next != nil {
		t.Fatalf("expected an empty last page, got %d streams and next %v", len(page.Items), page.Next)
	}
}

func TestDeserializeStreamsPage_MultipleStreams(t *testing.T) {
	payload := streamsPage(
		encodeStream(1, 100, 2, 512, 50, "stream-one"),
		encodeStream(2, 200, 0, 0, 0, "s2"),
		encodeStream(3, 300, 1, 1024, 10, "third"),
	)

	page, err := DeserializeStreamsPage(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	streams := page.Items
	if len(streams) != 3 {
		t.Fatalf("expected 3 streams, got %d", len(streams))
	}

	assertStream(t, "stream[0]", streams[0], 1, 100, 2, 512, 50, "stream-one")
	assertStream(t, "stream[1]", streams[1], 2, 200, 0, 0, 0, "s2")
	assertStream(t, "stream[2]", streams[2], 3, 300, 1, 1024, 10, "third")
}

func TestDeserializeStreamsPage_CorruptedPayload(t *testing.T) {
	good := encodeStream(1, 100, 0, 0, 0, "ok")
	truncated := make([]byte, streamFixedSize-5)

	_, err := DeserializeStreamsPage(streamsPage(good, truncated))
	if !errors.Is(err, ierror.ErrInvalidFormat) {
		t.Fatalf("expected ErrInvalidFormat for corrupted payload, got %v", err)
	}
}

// Regression test for issue #3130: payloads > 64KB produced corrupted
// stream lists because no bounds checking was performed.
func TestDeserializeStreamsPage_LargePayloadOver64KB(t *testing.T) {
	const targetSize = 70_000
	var encoded [][]byte
	var size int
	var id uint32

	for size < targetSize {
		id++
		name := fmt.Sprintf("stream-with-a-longer-name-for-padding-%d", id)
		stream := encodeStream(id, uint64(id)*1000, id%10, uint64(id)*512, uint64(id)*5, name)
		encoded = append(encoded, stream)
		size += len(stream)
	}
	payload := streamsPage(encoded...)

	if len(payload) <= 1<<16 {
		t.Fatalf("payload size %d is not > 64KB; increase stream count or name length", len(payload))
	}

	page, err := DeserializeStreamsPage(payload)
	if err != nil {
		t.Fatalf("unexpected error deserializing %d-byte payload: %v", len(payload), err)
	}

	streams := page.Items
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

func TestDeserializeStreamsPage_MaxLengthName(t *testing.T) {
	name := make([]byte, 255)
	for i := range name {
		name[i] = 'a' + byte(i%26)
	}
	payload := streamsPage(encodeStream(1, 999, 3, 4096, 200, string(name)))

	page, err := DeserializeStreamsPage(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	streams := page.Items
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if streams[0].Name != string(name) {
		t.Errorf("Name length = %d, want 255", len(streams[0].Name))
	}
}

// streamsPage frames encoded streams as the last page of a GetStreams reply:
// no next cursor, then the count.
func streamsPage(streams ...[]byte) []byte {
	payload := binary.LittleEndian.AppendUint32([]byte{0}, uint32(len(streams)))
	for _, stream := range streams {
		payload = append(payload, stream...)
	}
	return payload
}

func TestBoundedCapacity_CapsDeclaredCount(t *testing.T) {
	tests := []struct {
		name        string
		declared    uint32
		remaining   int
		minItemSize int
		want        int
	}{
		{"declared below what fits", 2, 10 * topicMinimumSize, topicMinimumSize, 2},
		{"declared above what fits", 1 << 31, 3 * topicMinimumSize, topicMinimumSize, 3},
		{"nothing left to read", 1 << 31, 0, topicMinimumSize, 0},
		{"zero item size", 1 << 31, 1024, 0, 0},
	}
	for _, test := range tests {
		got := boundedCapacity(test.declared, test.remaining, test.minItemSize)
		if got != test.want {
			t.Errorf("%s: boundedCapacity(%d, %d, %d) = %d, want %d",
				test.name, test.declared, test.remaining, test.minItemSize, got, test.want)
		}
	}
}

func TestDeserializeStream_LyingTopicsCountDoesNotPreallocate(t *testing.T) {
	// A u32 topics count with no topics behind it: the decode must fail on the
	// missing element rather than reserve a slice for the declared count.
	payload := encodeStream(1, 100, 1<<31, 0, 0, "s")

	if _, err := DeserializeStream(payload); err == nil {
		t.Fatal("expected error for topics count with no elements, got nil")
	}
}

func TestDeserializeAccessTokens_ReadsExpiryAsMicroseconds(t *testing.T) {
	const expiryMicros = 1_792_000_000_123_456
	appendToken := func(payload []byte, name string, expiry uint64) []byte {
		payload = append(payload, byte(len(name)))
		payload = append(payload, name...)
		return binary.LittleEndian.AppendUint64(payload, expiry)
	}
	payload := appendToken(appendToken(nil, "never", 0), "expiring", expiryMicros)

	tokens, err := DeserializeAccessTokens(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 {
		t.Fatalf("got %d tokens, want 2: %+v", len(tokens), tokens)
	}
	if tokens[0].Name != "never" || tokens[0].Expiry != nil {
		t.Errorf("token without expiry decoded as %s expiring at %v, want never with no expiry", tokens[0].Name, tokens[0].Expiry)
	}
	if tokens[1].Name != "expiring" || tokens[1].Expiry == nil || !tokens[1].Expiry.Equal(time.UnixMicro(expiryMicros)) {
		t.Errorf("token with expiry decoded as %s expiring at %v, want expiring at %v", tokens[1].Name, tokens[1].Expiry, time.UnixMicro(expiryMicros))
	}
}

func TestDeserializeAccessTokens_EmptyPayloadIsAnEmptyList(t *testing.T) {
	tokens, err := DeserializeAccessTokens(nil)
	if err != nil {
		t.Fatalf("a user without tokens got error %v, want an empty list", err)
	}
	if len(tokens) != 0 {
		t.Fatalf("got %+v, want no tokens", tokens)
	}
}

func TestDeserializeLogInResponse_ValidAndTruncated(t *testing.T) {
	valid := binary.LittleEndian.AppendUint32(nil, 42)
	got, err := DeserializeLogInResponse(valid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || got.UserId != 42 {
		t.Fatalf("got %+v, want UserId = 42", got)
	}

	for cut := 0; cut < len(valid); cut++ {
		truncated := valid[:cut]
		_, err := DeserializeLogInResponse(truncated)
		if err == nil {
			t.Fatalf("expected error for payload cut to %d bytes, got nil", cut)
		}
		if !errors.Is(err, ierror.ErrTruncatedPayload) {
			t.Fatalf("expected ErrTruncatedPayload, got %v", err)
		}
	}
}

func TestDeserializeOffset_ValidAndTruncated(t *testing.T) {
	valid := binary.LittleEndian.AppendUint32(nil, 1)
	valid = binary.LittleEndian.AppendUint64(valid, 100)
	valid = binary.LittleEndian.AppendUint64(valid, 90)

	got, err := DeserializeOffset(valid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || got.PartitionId != 1 || got.CurrentOffset != 100 || got.StoredOffset != 90 {
		t.Fatalf("got %+v, want PartitionId=1, Current=100, Stored=90", got)
	}

	empty, err := DeserializeOffset(nil)
	if err != nil || empty != nil {
		t.Fatalf("empty payload: got (%+v, %v), want (nil, nil)", empty, err)
	}
	empty2, err := DeserializeOffset([]byte{})
	if err != nil || empty2 != nil {
		t.Fatalf("zero-length payload: got (%+v, %v), want (nil, nil)", empty2, err)
	}

	for cut := 1; cut < len(valid); cut++ {
		truncated := valid[:cut]
		_, err := DeserializeOffset(truncated)
		if err == nil {
			t.Fatalf("expected error for payload cut to %d bytes, got nil", cut)
		}
		if !errors.Is(err, ierror.ErrTruncatedPayload) {
			t.Fatalf("expected ErrTruncatedPayload, got %v", err)
		}
	}
}

func TestDeserializeToConsumerGroupMember_ValidAndTruncated(t *testing.T) {
	valid := binary.LittleEndian.AppendUint32(nil, 10)
	valid = binary.LittleEndian.AppendUint32(valid, 2)
	valid = binary.LittleEndian.AppendUint32(valid, 101)
	valid = binary.LittleEndian.AppendUint32(valid, 102)

	member, readBytes, err := DeserializeToConsumerGroupMember(valid, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if readBytes != len(valid) {
		t.Errorf("readBytes = %d, want %d", readBytes, len(valid))
	}
	if member.ID != 10 || member.PartitionsCount != 2 || len(member.Partitions) != 2 ||
		member.Partitions[0] != 101 || member.Partitions[1] != 102 {
		t.Errorf("got member %+v", member)
	}

	for cut := 0; cut < len(valid); cut++ {
		truncated := valid[:cut]
		_, _, err := DeserializeToConsumerGroupMember(truncated, 0)
		if err == nil {
			t.Fatalf("expected error for payload cut to %d bytes, got nil", cut)
		}
		if !errors.Is(err, ierror.ErrTruncatedPayload) {
			t.Fatalf("expected ErrTruncatedPayload, got %v", err)
		}
	}

	// Oversized partition count
	oversized := binary.LittleEndian.AppendUint32(nil, 1)
	oversized = binary.LittleEndian.AppendUint32(oversized, 1<<30)
	if _, _, err := DeserializeToConsumerGroupMember(oversized, 0); err == nil {
		t.Fatal("expected error for oversized partitions count, got nil")
	}
}

func TestDeserializeConsumerGroup_ValidAndTruncated(t *testing.T) {
	name := "group1"
	valid := binary.LittleEndian.AppendUint32(nil, 1)
	valid = binary.LittleEndian.AppendUint32(valid, 4)
	valid = binary.LittleEndian.AppendUint32(valid, 1)
	valid = append(valid, byte(len(name)))
	valid = append(valid, name...)
	// Member 1
	valid = binary.LittleEndian.AppendUint32(valid, 10)
	valid = binary.LittleEndian.AppendUint32(valid, 1)
	valid = binary.LittleEndian.AppendUint32(valid, 101)

	details, err := DeserializeConsumerGroup(valid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if details.ConsumerGroup.Name != name || details.ConsumerGroup.MembersCount != 1 || len(details.Members) != 1 {
		t.Fatalf("got %+v", details)
	}

	for cut := 0; cut < len(valid); cut++ {
		truncated := valid[:cut]
		_, err := DeserializeConsumerGroup(truncated)
		if err == nil {
			t.Fatalf("expected error for payload cut to %d bytes, got nil", cut)
		}
	}
}

func TestDeserializePermissions_ValidAndTruncated(t *testing.T) {
	// Global: 10 bytes (all 1), hasStreams: 1
	valid := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	// Stream: streamId=5, 6 perms, hasTopics: 1
	valid = binary.LittleEndian.AppendUint32(valid, 5)
	valid = append(valid, 1, 0, 1, 0, 1, 0) // 6 stream perms
	valid = append(valid, 1)                // hasTopics: 1
	// Topic: topicId=9, 4 perms, topic delimiter: 0
	valid = binary.LittleEndian.AppendUint32(valid, 9)
	valid = append(valid, 1, 1, 0, 0) // 4 topic perms
	valid = append(valid, 0)          // topic delimiter 0 (end of topics)
	valid = append(valid, 0)          // stream delimiter 0 (end of streams)

	perms, err := deserializePermissions(valid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !perms.Global.ManageServers || len(perms.Streams) != 1 {
		t.Fatalf("got %+v", perms)
	}
	streamPerm, ok := perms.Streams[5]
	if !ok || !streamPerm.ManageStream || streamPerm.ReadStream || len(streamPerm.Topics) != 1 {
		t.Fatalf("stream 5: %+v", streamPerm)
	}
	topicPerm, ok := streamPerm.Topics[9]
	if !ok || !topicPerm.ManageTopic || topicPerm.PollMessages {
		t.Fatalf("topic 9: %+v", topicPerm)
	}

	for cut := 0; cut < len(valid); cut++ {
		truncated := valid[:cut]
		_, err := deserializePermissions(truncated)
		if err == nil {
			t.Fatalf("expected error for payload cut to %d bytes, got nil", cut)
		}
		if !errors.Is(err, ierror.ErrTruncatedPayload) {
			t.Fatalf("expected ErrTruncatedPayload, got %v", err)
		}
	}

	// Invalid topic delimiter (2 instead of 0 or 1)
	corruptDelim := make([]byte, len(valid))
	copy(corruptDelim, valid)
	corruptDelim[len(corruptDelim)-2] = 2
	if _, err := deserializePermissions(corruptDelim); err == nil {
		t.Fatal("expected error for invalid delimiter, got nil")
	}
}

func TestDeserializeUser_ValidAndTruncated(t *testing.T) {
	username := "alice"
	// User: id(4), createdAt(8), status(1), usernameLength(1), username, options(4), derivedOptions(4), hasPermissions(1)
	valid := binary.LittleEndian.AppendUint32(nil, 7)
	valid = binary.LittleEndian.AppendUint64(valid, 1000)
	valid = append(valid, 1) // status: Active
	valid = append(valid, byte(len(username)))
	valid = append(valid, username...)
	valid = binary.LittleEndian.AppendUint32(valid, 0) // options len: 0
	valid = binary.LittleEndian.AppendUint32(valid, 0) // derived options len: 0
	valid = append(valid, 1)                           // hasPermissions: 1

	permsPayload := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} // 10 global + hasStreams=0
	valid = binary.LittleEndian.AppendUint32(valid, uint32(len(permsPayload)))
	valid = append(valid, permsPayload...)

	user, err := DeserializeUser(valid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if user.UserInfo.Id != 7 || user.UserInfo.Username != username || user.Permissions == nil {
		t.Fatalf("got %+v", user)
	}

	// Truncated at every possible length
	for cut := 0; cut < len(valid); cut++ {
		truncated := valid[:cut]
		_, err := DeserializeUser(truncated)
		if err == nil {
			t.Fatalf("expected error for user cut to %d bytes, got nil", cut)
		}
	}

	// User without permissions (hasPermissions = 0)
	noPerms := binary.LittleEndian.AppendUint32(nil, 8)
	noPerms = binary.LittleEndian.AppendUint64(noPerms, 2000)
	noPerms = append(noPerms, 2) // status: Inactive
	noPerms = append(noPerms, byte(len(username)))
	noPerms = append(noPerms, username...)
	noPerms = binary.LittleEndian.AppendUint32(noPerms, 0)
	noPerms = binary.LittleEndian.AppendUint32(noPerms, 0)
	noPerms = append(noPerms, 0) // hasPermissions: 0

	userNoPerm, err := DeserializeUser(noPerms)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if userNoPerm.UserInfo.Id != 8 || userNoPerm.Permissions != nil {
		t.Fatalf("got %+v", userNoPerm)
	}

	// Permissions length that points past the buffer
	bogusPermLen := make([]byte, len(valid))
	copy(bogusPermLen, valid)
	binary.LittleEndian.PutUint32(bogusPermLen[len(valid)-len(permsPayload)-4:], 1000)
	if _, err := DeserializeUser(bogusPermLen); err == nil {
		t.Fatal("expected error for bogus permissions length, got nil")
	}
}

func TestClientInfo_ValidAndTruncated(t *testing.T) {
	addr := "127.0.0.1:8090"
	valid := binary.LittleEndian.AppendUint32(nil, 1)  // id
	valid = binary.LittleEndian.AppendUint32(valid, 2) // userId
	valid = append(valid, 1)                           // transport: Tcp
	valid = binary.LittleEndian.AppendUint32(valid, uint32(len(addr)))
	valid = append(valid, addr...)
	valid = binary.LittleEndian.AppendUint32(valid, 1) // consumerGroupsCount: 1
	// 1 consumer group: streamId(4), topicId(4), groupId(4)
	valid = binary.LittleEndian.AppendUint32(valid, 10)
	valid = binary.LittleEndian.AppendUint32(valid, 20)
	valid = binary.LittleEndian.AppendUint32(valid, 30)

	// Test DeserializeClient
	clientDetails, err := DeserializeClient(valid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if clientDetails.ClientInfo.ID != 1 || clientDetails.ClientInfo.Address != addr || len(clientDetails.ConsumerGroups) != 1 {
		t.Fatalf("got %+v", clientDetails)
	}

	for cut := 0; cut < len(valid); cut++ {
		truncated := valid[:cut]
		_, err := DeserializeClient(truncated)
		if err == nil {
			t.Fatalf("expected error for DeserializeClient cut to %d bytes, got nil", cut)
		}
	}

	// Test MapClientInfo
	clientInfoBytes := valid[:13+len(addr)+4]
	client, readBytes, err := MapClientInfo(clientInfoBytes, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if readBytes != len(clientInfoBytes) || client.Address != addr {
		t.Fatalf("got %+v, readBytes %d", client, readBytes)
	}

	for cut := 0; cut < len(clientInfoBytes); cut++ {
		truncated := clientInfoBytes[:cut]
		_, _, err := MapClientInfo(truncated, 0)
		if err == nil {
			t.Fatalf("expected error for MapClientInfo cut to %d bytes, got nil", cut)
		}
		if !errors.Is(err, ierror.ErrTruncatedPayload) {
			t.Fatalf("expected ErrTruncatedPayload, got %v", err)
		}
	}

	// Test DeserializeClients (multiple clients)
	twoClients := append(append([]byte(nil), clientInfoBytes...), clientInfoBytes...)
	clients, err := DeserializeClients(twoClients)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(clients) != 2 {
		t.Fatalf("expected 2 clients, got %d", len(clients))
	}

	empty, err := DeserializeClients(nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("expected empty slice, got (%+v, %v)", empty, err)
	}

	for cut := 1; cut < len(twoClients); cut++ {
		if cut == len(clientInfoBytes) {
			continue // exactly 1 complete client, which is a valid payload
		}
		truncated := twoClients[:cut]
		_, err := DeserializeClients(truncated)
		if err == nil {
			t.Fatalf("expected error for DeserializeClients cut to %d bytes, got nil", cut)
		}
	}
}

func TestDeserializeAccessToken_ValidAndTruncated(t *testing.T) {
	token := "secret-pat-token"
	valid := append([]byte{byte(len(token))}, token...)

	got, err := DeserializeAccessToken(valid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Token != token {
		t.Fatalf("got %q, want %q", got.Token, token)
	}

	for cut := 0; cut < len(valid); cut++ {
		truncated := valid[:cut]
		_, err := DeserializeAccessToken(truncated)
		if err == nil {
			t.Fatalf("expected error for token cut to %d bytes, got nil", cut)
		}
		if !errors.Is(err, ierror.ErrTruncatedPayload) {
			t.Fatalf("expected ErrTruncatedPayload, got %v", err)
		}
	}

	// Token length pointing past end
	oversized := []byte{100, 'a', 'b', 'c'}
	if _, err := DeserializeAccessToken(oversized); err == nil {
		t.Fatal("expected error for oversized token length, got nil")
	}
}

func TestDeserializeAccessTokens_Truncated(t *testing.T) {
	appendToken := func(payload []byte, name string, expiry uint64) []byte {
		payload = append(payload, byte(len(name)))
		payload = append(payload, name...)
		return binary.LittleEndian.AppendUint64(payload, expiry)
	}
	valid := appendToken(nil, "token1", 123456789)

	for cut := 1; cut < len(valid); cut++ {
		truncated := valid[:cut]
		_, err := DeserializeAccessTokens(truncated)
		if err == nil {
			t.Fatalf("expected error for tokens cut to %d bytes, got nil", cut)
		}
	}
}

func FuzzDeserializeLogInResponse(f *testing.F) {
	f.Add([]byte{0x01, 0x02, 0x03, 0x04})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DeserializeLogInResponse(data)
	})
}

func FuzzDeserializeOffset(f *testing.F) {
	f.Add(make([]byte, 20))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DeserializeOffset(data)
	})
}

func FuzzDeserializeToConsumerGroupMember(f *testing.F) {
	f.Add(make([]byte, 8))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DeserializeToConsumerGroupMember(data, 0)
	})
}

func FuzzDeserializeConsumerGroup(f *testing.F) {
	f.Add(make([]byte, 13))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DeserializeConsumerGroup(data)
	})
}

func FuzzDeserializeUser(f *testing.F) {
	f.Add(make([]byte, 20))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DeserializeUser(data)
	})
}

func FuzzDeserializeClient(f *testing.F) {
	f.Add(make([]byte, 17))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DeserializeClient(data)
	})
}

func FuzzDeserializeClients(f *testing.F) {
	f.Add(make([]byte, 17))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DeserializeClients(data)
	})
}

func FuzzDeserializeAccessToken(f *testing.F) {
	f.Add([]byte{4, 't', 'e', 's', 't'})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DeserializeAccessToken(data)
	})
}

func FuzzDeserializeAccessTokens(f *testing.F) {
	f.Add([]byte{4, 't', 'e', 's', 't', 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DeserializeAccessTokens(data)
	})
}
