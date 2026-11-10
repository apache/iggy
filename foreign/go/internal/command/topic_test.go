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

package command

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestSerialize_CreateTopic_ServerDefaults(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	partitions := uint32(2)
	compression := iggcon.CompressionAlgorithmNone
	request := CreateTopic{
		StreamId: streamId,
		Name:     "topic",
		Options:  iggcon.TopicOptions{PartitionsCount: &partitions, CompressionAlgorithm: &compression},
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateTopic: %v", err)
	}

	expected := []byte{
		0x02,                               // StreamId Kind (StringId)
		0x06,                               // StreamId Length (6)
		0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // StreamId Value ("stream")
		0x02, 0x00, 0x00, 0x00, // PartitionsCount (2)
		0x05,                         // Name Length (5)
		0x74, 0x6F, 0x70, 0x69, 0x63, // Name ("topic")
		// Options travel in ascending key order.
		2, 26, 0, 0, 0, 'c', 'o', 'n', 's', 'u', 'm', 'e', 'r', '_', 'o', 'f', 'f', 's', 'e', 't', '_', 'd', 'u', 'r', 'a', 'b', 'i', 'l', 'i', 't', 'y',
		2, 10, 0, 0, 0, 'r', 'e', 'p', 'l', 'i', 'c', 'a', 't', 'e', 'd',
		2, 10, 0, 0, 0, 'd', 'u', 'r', 'a', 'b', 'i', 'l', 'i', 't', 'y',
		2, 10, 0, 0, 0, 'r', 'e', 'p', 'l', 'i', 'c', 'a', 't', 'e', 'd',
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("CreateTopic serialization failed. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerialize_CreateTopic_NonDefaultsBecomeOptions(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(1))
	partitions := uint32(4)
	compression := iggcon.CompressionAlgorithmGzip
	expiry := 100 * iggcon.Microsecond
	maxTopicSize := uint64(1 << 30)
	request := CreateTopic{
		StreamId: streamId,
		Name:     "topic",
		Options: iggcon.TopicOptions{
			PartitionsCount:      &partitions,
			CompressionAlgorithm: &compression,
			MessageExpiry:        &expiry,
			MaxTopicSize:         &maxTopicSize,
		},
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateTopic: %v", err)
	}

	// [stream_id: kind+len+4][partitions_count:4][name_len][name] then options to end.
	const streamIdSize = 2 + 4
	if partitions := binary.LittleEndian.Uint32(serialized[streamIdSize:]); partitions != 4 {
		t.Errorf("partitions_count fixed field = %d, want 4", partitions)
	}

	optionsOffset := streamIdSize + 4 + 1 + len(request.Name)
	options, err := iggcon.DeserializeHeaders(serialized[optionsOffset:])
	if err != nil {
		t.Fatalf("Options block is not valid TLV: %v", err)
	}

	byKey := make(map[string]iggcon.HeaderValue, len(options))
	for _, entry := range options {
		if entry.Key.Kind != iggcon.String {
			t.Errorf("Option key %q has kind %d, want String", entry.Key.Value, entry.Key.Kind)
		}
		byKey[string(entry.Key.Value)] = entry.Value
	}

	if len(byKey) != 5 {
		t.Fatalf("expected 5 options, got %d: %v", len(byKey), byKey)
	}
	if _, found := byKey["partitions_count"]; found {
		t.Error("partitions_count rides the fixed field, not the options block")
	}
	compressionValue := byKey[string(iggcon.TopicOptionCompressionAlgorithm)]
	if compressionValue.Kind != iggcon.String || string(compressionValue.Value) != "gzip" {
		t.Errorf("compression_algorithm = %+v, want String %q", compressionValue, "gzip")
	}
	expiryValue := byKey[string(iggcon.TopicOptionMessageExpiry)]
	if expiryValue.Kind != iggcon.Uint64 || binary.LittleEndian.Uint64(expiryValue.Value) != 100 {
		t.Errorf("message_expiry = %+v, want Uint64 100", expiryValue)
	}
	maxSize := byKey[string(iggcon.TopicOptionMaxTopicSize)]
	if maxSize.Kind != iggcon.Uint64 || binary.LittleEndian.Uint64(maxSize.Value) != 1<<30 {
		t.Errorf("max_topic_size = %+v, want Uint64 %d", maxSize, 1<<30)
	}
}

func TestSerialize_UpdateTopic(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier(uint32(1))
	name := "update_topic"
	expiry := 100 * iggcon.Microsecond
	maxTopicSize := uint64(100)
	request := UpdateTopic{
		StreamId:      streamId,
		TopicId:       topicId,
		Name:          &name,
		MessageExpiry: &expiry,
		MaxTopicSize:  &maxTopicSize,
	}

	serialized1, err := request.MarshalBinary()
	if err != nil {
		t.Errorf("Failed to serialize UpdateTopic: %v", err)
	}

	expected := []byte{
		0x01,                               // Scope (topic)
		0x02,                               // StreamId Kind (StringId)
		0x06,                               // StreamId Length (6)
		0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // StreamId Value ("stream")
		0x01,                   // TopicId Kind (NumericId)
		0x04,                   // TopicId Length (4)
		0x01, 0x00, 0x00, 0x00, // TopicId Value (1)
		0x00, // Reset count
		// The name and the settings ride the options block in ascending key order.
		0x02, 0x0E, 0x00, 0x00, 0x00, // key kind (String), key length (14)
		0x6D, 0x61, 0x78, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, 0x5F, 0x73, 0x69, 0x7A, 0x65, // "max_topic_size"
		0x0C, 0x08, 0x00, 0x00, 0x00, // value kind (Uint64), value length (8)
		0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 100
		0x02, 0x0E, 0x00, 0x00, 0x00, // key kind (String), key length (14)
		0x6D, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x5F, 0x65, 0x78, 0x70, 0x69, 0x72, 0x79, // "message_expiry"
		0x0C, 0x08, 0x00, 0x00, 0x00, // value kind (Uint64), value length (8)
		0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 100
		0x02, 0x04, 0x00, 0x00, 0x00, // key kind (String), key length (4)
		0x6E, 0x61, 0x6D, 0x65, // "name"
		0x02, 0x0C, 0x00, 0x00, 0x00, // value kind (String), value length (12)
		0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, // "update_topic"
	}

	if !bytes.Equal(serialized1, expected) {
		t.Errorf("Test case 1 failed. \nExpected:\t%v\nGot:\t\t%v", expected, serialized1)
	}
}

func TestCreateTopic_CallerOptionsRideTheBlockAndTypedFieldsWin(t *testing.T) {
	streamId, err := iggcon.NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	maxTopicSize := uint64(4096)
	command := CreateTopic{
		StreamId: streamId,
		Name:     "t",
		Options: iggcon.TopicOptions{MaxTopicSize: &maxTopicSize, Raw: []iggcon.HeaderEntry{
			{
				Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("durability")},
				Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("persisted")},
			},
			// The typed field already covers this key, so the caller's entry is
			// dropped: a duplicate key makes the server refuse the whole block.
			{
				Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("max_topic_size")},
				Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("1 GiB")},
			},
		}},
	}

	options, err := command.options()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	byKey := map[string]iggcon.HeaderValue{}
	for _, entry := range options {
		key := string(entry.Key.Value)
		if _, duplicate := byKey[key]; duplicate {
			t.Fatalf("key %q appears twice in the options block", key)
		}
		byKey[key] = entry.Value
	}
	if _, ok := byKey["durability"]; !ok {
		t.Error("a caller-supplied key must reach the options block")
	}
	if got := byKey["max_topic_size"]; got.Kind != iggcon.Uint64 {
		t.Errorf("max_topic_size kind = %v, want the typed field's Uint64", got.Kind)
	}
}

func TestCreateTopic_TypedOptionConstructorsReachTheBlock(t *testing.T) {
	streamId, err := iggcon.NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	command := CreateTopic{
		StreamId: streamId,
		Name:     "topic",
		Options: iggcon.TopicOptions{Raw: []iggcon.HeaderEntry{
			iggcon.SegmentSizeOption(1 << 20),
			iggcon.DurabilityOption(iggcon.DurabilityPersisted),
			iggcon.MessagesRequiredToSaveOption(7),
			iggcon.SizeOfMessagesRequiredToSaveOption(4096),
			iggcon.PreallocateSegmentsOption(false),
			iggcon.ReplicationFactorOption(3),
		}},
	}

	serialized, err := command.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to serialize CreateTopic: %v", err)
	}

	const streamIdSize = 2 + 4
	optionsOffset := streamIdSize + 4 + 1 + len(command.Name)
	options, err := iggcon.DeserializeHeaders(serialized[optionsOffset:])
	if err != nil {
		t.Fatalf("options block is not valid TLV: %v", err)
	}
	byKey := make(map[string]iggcon.HeaderValue, len(options))
	for _, entry := range options {
		byKey[string(entry.Key.Value)] = entry.Value
	}

	// Literal keys and value bytes: they are the wire contract the constructors
	// are pinned to, so restating them here is the assertion.
	want := []struct {
		key   string
		kind  iggcon.HeaderKind
		value []byte
	}{
		{"segment_size", iggcon.Uint64, []byte{0, 0, 16, 0, 0, 0, 0, 0}},
		{"durability", iggcon.String, []byte("persisted")},
		{"consumer_offset_durability", iggcon.String, []byte("replicated")},
		{"messages_required_to_save", iggcon.Uint32, []byte{7, 0, 0, 0}},
		{"size_of_messages_required_to_save", iggcon.Uint64, []byte{0, 16, 0, 0, 0, 0, 0, 0}},
		{"preallocate_segments", iggcon.Bool, []byte{0}},
		{"replication_factor", iggcon.Uint32, []byte{3, 0, 0, 0}},
	}
	if len(byKey) != len(want) {
		t.Fatalf("options block carries %d keys, want %d: %v", len(byKey), len(want), byKey)
	}
	for _, expected := range want {
		got, found := byKey[expected.key]
		if !found {
			t.Errorf("key %q is missing from the options block", expected.key)
			continue
		}
		if got.Kind != expected.kind {
			t.Errorf("key %q kind = %d, want %d", expected.key, got.Kind, expected.kind)
		}
		if !bytes.Equal(got.Value, expected.value) {
			t.Errorf("key %q value = %v, want %v", expected.key, got.Value, expected.value)
		}
	}
}

func TestCreateTopic_ReplicationFactorFieldRidesTheBlockAsUint32(t *testing.T) {
	streamId, err := iggcon.NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	replicationFactor := uint32(2)
	command := CreateTopic{
		StreamId: streamId,
		Name:     "topic",
		Options:  iggcon.TopicOptions{ReplicationFactor: &replicationFactor},
	}

	serialized, err := command.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to serialize CreateTopic: %v", err)
	}

	const streamIdSize = 2 + 4
	optionsOffset := streamIdSize + 4 + 1 + len(command.Name)
	options, err := iggcon.DeserializeHeaders(serialized[optionsOffset:])
	if err != nil {
		t.Fatalf("options block is not valid TLV: %v", err)
	}
	var found bool
	for _, entry := range options {
		if string(entry.Key.Value) != "replication_factor" {
			continue
		}
		found = true
		if entry.Value.Kind != iggcon.Uint32 {
			t.Errorf("replication_factor kind = %d, want Uint32", entry.Value.Kind)
		}
		if !bytes.Equal(entry.Value.Value, []byte{2, 0, 0, 0}) {
			t.Errorf("replication_factor value = %v, want [2 0 0 0]", entry.Value.Value)
		}
	}
	if !found {
		t.Errorf("replication_factor is missing from the options block: %v", options)
	}
}

func TestCreateTopic_SegmentMaxAgeFieldRidesTheBlockAsUint64(t *testing.T) {
	streamId, err := iggcon.NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const streamIdSize = 2 + 4
	segmentOptions := func(age iggcon.Duration) []iggcon.HeaderEntry {
		t.Helper()
		command := CreateTopic{
			StreamId: streamId,
			Name:     "topic",
			Options:  iggcon.TopicOptions{SegmentMaxAge: &age},
		}
		serialized, err := command.MarshalBinary()
		if err != nil {
			t.Fatalf("failed to serialize CreateTopic: %v", err)
		}
		options, err := iggcon.DeserializeHeaders(serialized[streamIdSize+4+1+len(command.Name):])
		if err != nil {
			t.Fatalf("options block is not valid TLV: %v", err)
		}
		var entries []iggcon.HeaderEntry
		for _, entry := range options {
			if string(entry.Key.Value) == string(iggcon.TopicOptionSegmentMaxAge) {
				entries = append(entries, entry)
			}
		}
		return entries
	}

	entries := segmentOptions(iggcon.Duration(3_600_000_000))
	if len(entries) != 1 {
		t.Fatalf("segment_max_age entries = %v, want exactly one", entries)
	}
	if entries[0].Value.Kind != iggcon.Uint64 || binary.LittleEndian.Uint64(entries[0].Value.Value) != 3_600_000_000 {
		t.Errorf("segment_max_age = %+v, want Uint64 3600000000", entries[0].Value)
	}
	if entries := segmentOptions(iggcon.IggyExpiryServerDefault); len(entries) != 0 {
		t.Errorf("the server-default sentinel must leave segment_max_age derived, got %v", entries)
	}
}

func TestCreateTopic_TieringFieldsRideTheBlock(t *testing.T) {
	streamId, err := iggcon.NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const streamIdSize = 2 + 4
	tieringOptions := func(options iggcon.TopicOptions) map[string]iggcon.HeaderValue {
		t.Helper()
		command := CreateTopic{StreamId: streamId, Name: "topic", Options: options}
		serialized, err := command.MarshalBinary()
		if err != nil {
			t.Fatalf("failed to serialize CreateTopic: %v", err)
		}
		entries, err := iggcon.DeserializeHeaders(serialized[streamIdSize+4+1+len(command.Name):])
		if err != nil {
			t.Fatalf("options block is not valid TLV: %v", err)
		}
		byKey := make(map[string]iggcon.HeaderValue, len(entries))
		for _, entry := range entries {
			byKey[string(entry.Key.Value)] = entry.Value
		}
		return byKey
	}

	tieredStorage := false
	localRetentionAge := iggcon.Duration(21_600_000_000)
	localRetentionSize := uint64(10 << 30)
	options := tieringOptions(iggcon.TopicOptions{
		TieredStorage:      &tieredStorage,
		LocalRetentionAge:  &localRetentionAge,
		LocalRetentionSize: &localRetentionSize,
	})
	if value := options[string(iggcon.TopicOptionTieredStorage)]; value.Kind != iggcon.Bool || !bytes.Equal(value.Value, []byte{0}) {
		t.Errorf("tiered_storage = %+v, want Bool false", value)
	}
	if value := options[string(iggcon.TopicOptionLocalRetentionAge)]; value.Kind != iggcon.Uint64 ||
		binary.LittleEndian.Uint64(value.Value) != 21_600_000_000 {
		t.Errorf("local_retention_age = %+v, want Uint64 21600000000", value)
	}
	if value := options[string(iggcon.TopicOptionLocalRetentionSize)]; value.Kind != iggcon.Uint64 ||
		binary.LittleEndian.Uint64(value.Value) != 10<<30 {
		t.Errorf("local_retention_size = %+v, want Uint64 %d", value, 10<<30)
	}

	serverDefaultAge := iggcon.IggyExpiryServerDefault
	serverDefaultSize := uint64(0)
	options = tieringOptions(iggcon.TopicOptions{
		LocalRetentionAge:  &serverDefaultAge,
		LocalRetentionSize: &serverDefaultSize,
	})
	for _, key := range []iggcon.TopicOptionKey{
		iggcon.TopicOptionTieredStorage,
		iggcon.TopicOptionLocalRetentionAge,
		iggcon.TopicOptionLocalRetentionSize,
	} {
		if value, sent := options[string(key)]; sent {
			t.Errorf("the server-default sentinel must leave %s derived, got %+v", key, value)
		}
	}
}

func TestUpdateTopic_TieringFieldsRideTheBlock(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(1))
	topicId, _ := iggcon.NewIdentifier(uint32(2))
	const prefixSize = 1 + (2 + 4) + (2 + 4) + 1
	tieredStorage := true
	localRetentionAge := iggcon.Duration(21_600_000_000)
	localRetentionSize := uint64(10 << 30)
	request := UpdateTopic{
		StreamId:           streamId,
		TopicId:            topicId,
		TieredStorage:      &tieredStorage,
		LocalRetentionAge:  &localRetentionAge,
		LocalRetentionSize: &localRetentionSize,
	}
	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to serialize UpdateTopic: %v", err)
	}
	entries, err := iggcon.DeserializeHeaders(serialized[prefixSize:])
	if err != nil {
		t.Fatalf("options block is not valid TLV: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %v, want the three tiering keys", entries)
	}
	byKey := make(map[string]iggcon.HeaderValue, len(entries))
	for _, entry := range entries {
		byKey[string(entry.Key.Value)] = entry.Value
	}
	if value := byKey[string(iggcon.TopicOptionTieredStorage)]; value.Kind != iggcon.Bool || !bytes.Equal(value.Value, []byte{1}) {
		t.Errorf("tiered_storage = %+v, want Bool true", value)
	}
	if value := byKey[string(iggcon.TopicOptionLocalRetentionAge)]; value.Kind != iggcon.Uint64 ||
		binary.LittleEndian.Uint64(value.Value) != 21_600_000_000 {
		t.Errorf("local_retention_age = %+v, want Uint64 21600000000", value)
	}
	if value := byKey[string(iggcon.TopicOptionLocalRetentionSize)]; value.Kind != iggcon.Uint64 ||
		binary.LittleEndian.Uint64(value.Value) != 10<<30 {
		t.Errorf("local_retention_size = %+v, want Uint64 %d", value, 10<<30)
	}

	reset := UpdateTopic{
		StreamId: streamId,
		TopicId:  topicId,
		Reset:    []iggcon.TopicOptionKey{iggcon.TopicOptionLocalRetentionAge, iggcon.TopicOptionLocalRetentionSize},
	}
	if _, err := reset.MarshalBinary(); err != nil {
		t.Errorf("resetting the local retention keys must be accepted, got %v", err)
	}
}

func TestUpdateTopic_TieredStorageCannotBeTurnedOffOrReset(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(1))
	topicId, _ := iggcon.NewIdentifier(uint32(2))
	tieredStorage := false
	requests := map[string]UpdateTopic{
		"off":   {StreamId: streamId, TopicId: topicId, TieredStorage: &tieredStorage},
		"reset": {StreamId: streamId, TopicId: topicId, Reset: []iggcon.TopicOptionKey{iggcon.TopicOptionTieredStorage}},
	}
	for name, request := range requests {
		_, err := request.MarshalBinary()
		var refused ierror.OptionNotUpdatable
		if !errors.As(err, &refused) || refused.Key != string(iggcon.TopicOptionTieredStorage) {
			t.Errorf("%s: error = %v, want OptionNotUpdatable for tiered_storage", name, err)
		}
	}
}

func TestCreateTopic_NonStringOptionKeyIsRejected(t *testing.T) {
	streamId, err := iggcon.NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	command := CreateTopic{
		StreamId: streamId,
		Name:     "t",
		Options: iggcon.TopicOptions{Raw: []iggcon.HeaderEntry{{
			Key:   iggcon.HeaderKey{Kind: iggcon.Uint32, Value: []byte{1, 0, 0, 0}},
			Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("x")},
		}}},
	}

	if _, err := command.MarshalBinary(); err == nil {
		t.Error("expected an error for a non-string option key, got nil")
	}
}

func TestCreateTopic_OutOfByteOptionValueKindIsRejected(t *testing.T) {
	streamId, err := iggcon.NewIdentifier(uint32(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	command := CreateTopic{
		StreamId: streamId,
		Name:     "t",
		Options: iggcon.TopicOptions{Raw: []iggcon.HeaderEntry{{
			Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("future_option")},
			Value: iggcon.HeaderValue{Kind: 257, Value: []byte("x")},
		}}},
	}

	// 257 truncates to 1, which would let a reader decode the value as Raw.
	if _, err := command.MarshalBinary(); !errors.Is(err, ierror.ErrInvalidOptionValue) {
		t.Fatalf("got %v, want invalid option value", err)
	}
}
