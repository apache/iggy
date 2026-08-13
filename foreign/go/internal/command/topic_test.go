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
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func TestSerialize_CreateTopic_ServerDefaults(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	request := CreateTopic{
		StreamId:             streamId,
		Name:                 "topic",
		PartitionsCount:      2,
		CompressionAlgorithm: iggcon.CompressionAlgorithmNone,
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
		// options: empty, every default is derived server-side
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("CreateTopic serialization failed. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerialize_CreateTopic_NonDefaultsBecomeOptions(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(1))
	replicationFactor := uint8(3)
	request := CreateTopic{
		StreamId:             streamId,
		Name:                 "topic",
		PartitionsCount:      4,
		CompressionAlgorithm: iggcon.CompressionAlgorithmGzip,
		MessageExpiry:        100 * iggcon.Microsecond,
		MaxTopicSize:         1 << 30,
		ReplicationFactor:    &replicationFactor,
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

	if len(byKey) != 3 {
		t.Fatalf("expected 3 options, got %d: %v", len(byKey), byKey)
	}
	if _, found := byKey["partitions_count"]; found {
		t.Error("partitions_count rides the fixed field, not the options block")
	}
	compression := byKey[topicOptionCompressionAlgorithm]
	if compression.Kind != iggcon.String || string(compression.Value) != "gzip" {
		t.Errorf("compression_algorithm = %+v, want String %q", compression, "gzip")
	}
	expiry := byKey[topicOptionMessageExpiry]
	if expiry.Kind != iggcon.Uint64 || binary.LittleEndian.Uint64(expiry.Value) != 100 {
		t.Errorf("message_expiry = %+v, want Uint64 100", expiry)
	}
	maxSize := byKey[topicOptionMaxTopicSize]
	if maxSize.Kind != iggcon.Uint64 || binary.LittleEndian.Uint64(maxSize.Value) != 1<<30 {
		t.Errorf("max_topic_size = %+v, want Uint64 %d", maxSize, 1<<30)
	}
}

func TestSerialize_UpdateTopic(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier(uint32(1))
	request := UpdateTopic{
		StreamId:      streamId,
		TopicId:       topicId,
		Name:          "update_topic",
		MessageExpiry: 100 * iggcon.Microsecond,
		MaxTopicSize:  100,
	}

	serialized1, err := request.MarshalBinary()
	if err != nil {
		t.Errorf("Failed to serialize UpdateTopic: %v", err)
	}

	expected := []byte{
		0x02,                               // StreamId Kind (StringId)
		0x06,                               // StreamId Length (2)
		0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // StreamId Value ("stream")
		0x01,                   // TopicId Kind (NumericId)
		0x04,                   // TopicId Length (4)
		0x01, 0x00, 0x00, 0x00, // TopicId Value (1)
		0x00,                                           // compression algorithm
		0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Message Expiry (100)
		0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Max Topic Size (100)
		0x00,                                                                   // Replication factor
		0x0C,                                                                   // Name Length (12)
		0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, // Name ("update_topic")
	}

	if !bytes.Equal(serialized1, expected) {
		t.Errorf("Test case 1 failed. \nExpected:\t%v\nGot:\t\t%v", expected, serialized1)
	}
}
