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
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// TestSerialize_CreatePartitions_NumericIds tests serialization with numeric identifiers
func TestSerialize_CreatePartitions_NumericIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(123))
	topicId, _ := iggcon.NewIdentifier(uint32(456))

	cmd := CreatePartitions{
		StreamId:        streamId,
		TopicId:         topicId,
		PartitionsCount: 10,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreatePartitions: %v", err)
	}

	expected := []byte{
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x7B, 0x00, 0x00, 0x00, // StreamId Value = 123 (little endian)
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0xC8, 0x01, 0x00, 0x00, // TopicId Value = 456 (little endian)
		0x0A, 0x00, 0x00, 0x00, // PartitionsCount = 10 (little endian)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreatePartitions_StringIds tests serialization with string identifiers
func TestSerialize_CreatePartitions_StringIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("my_stream")
	topicId, _ := iggcon.NewIdentifier("my_topic")

	cmd := CreatePartitions{
		StreamId:        streamId,
		TopicId:         topicId,
		PartitionsCount: 5,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreatePartitions: %v", err)
	}

	expected := []byte{
		0x02,                                                 // StreamId Kind = StringId
		0x09,                                                 // StreamId Length = 9
		0x6D, 0x79, 0x5F, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // "my_stream"
		0x02,                                           // TopicId Kind = StringId
		0x08,                                           // TopicId Length = 8
		0x6D, 0x79, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, // "my_topic"
		0x05, 0x00, 0x00, 0x00, // PartitionsCount = 5 (little endian)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreatePartitions_MixedIds tests serialization with mixed identifier types
func TestSerialize_CreatePartitions_MixedIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(42))
	topicId, _ := iggcon.NewIdentifier("test")

	cmd := CreatePartitions{
		StreamId:        streamId,
		TopicId:         topicId,
		PartitionsCount: 100,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreatePartitions with mixed IDs: %v", err)
	}

	expected := []byte{
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x2A, 0x00, 0x00, 0x00, // StreamId Value = 42 (little endian)
		0x02,                   // TopicId Kind = StringId
		0x04,                   // TopicId Length = 4
		0x74, 0x65, 0x73, 0x74, // "test"
		0x64, 0x00, 0x00, 0x00, // PartitionsCount = 100 (little endian)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreatePartitions_ZeroCount tests edge case with zero partitions count
func TestSerialize_CreatePartitions_ZeroCount(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier("topic")

	cmd := CreatePartitions{
		StreamId:        streamId,
		TopicId:         topicId,
		PartitionsCount: 0,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreatePartitions with zero count: %v", err)
	}

	expected := []byte{
		0x02,                               // StreamId Kind = StringId
		0x06,                               // StreamId Length = 6
		0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // "stream"
		0x02,                         // TopicId Kind = StringId
		0x05,                         // TopicId Length = 5
		0x74, 0x6F, 0x70, 0x69, 0x63, // "topic"
		0x00, 0x00, 0x00, 0x00, // PartitionsCount = 0 (little endian)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeletePartitions_NumericIds tests DeletePartitions with numeric identifiers
func TestSerialize_DeletePartitions_NumericIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(1))
	topicId, _ := iggcon.NewIdentifier(uint32(2))

	cmd := DeletePartitions{
		StreamId:        streamId,
		TopicId:         topicId,
		PartitionsCount: 3,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeletePartitions: %v", err)
	}

	expected := []byte{
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x01, 0x00, 0x00, 0x00, // StreamId Value = 1 (little endian)
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x02, 0x00, 0x00, 0x00, // TopicId Value = 2 (little endian)
		0x03, 0x00, 0x00, 0x00, // PartitionsCount = 3 (little endian)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeletePartitions_StringIds tests DeletePartitions with string identifiers
func TestSerialize_DeletePartitions_StringIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("prod_stream")
	topicId, _ := iggcon.NewIdentifier("events")

	cmd := DeletePartitions{
		StreamId:        streamId,
		TopicId:         topicId,
		PartitionsCount: 2,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeletePartitions: %v", err)
	}

	expected := []byte{
		0x02,                                                             // StreamId Kind = StringId
		0x0B,                                                             // StreamId Length = 11
		0x70, 0x72, 0x6F, 0x64, 0x5F, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // "prod_stream"
		0x02,                               // TopicId Kind = StringId
		0x06,                               // TopicId Length = 6
		0x65, 0x76, 0x65, 0x6E, 0x74, 0x73, // "events"
		0x02, 0x00, 0x00, 0x00, // PartitionsCount = 2 (little endian)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeletePartitions_MaxCount tests edge case with maximum uint32 partitions count
func TestSerialize_DeletePartitions_MaxCount(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(999))
	topicId, _ := iggcon.NewIdentifier(uint32(888))

	cmd := DeletePartitions{
		StreamId:        streamId,
		TopicId:         topicId,
		PartitionsCount: 4294967295, // Max uint32 value
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeletePartitions with max count: %v", err)
	}

	expected := []byte{
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0xE7, 0x03, 0x00, 0x00, // StreamId Value = 999 (little endian)
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x78, 0x03, 0x00, 0x00, // TopicId Value = 888 (little endian)
		0xFF, 0xFF, 0xFF, 0xFF, // PartitionsCount = 4294967295 (little endian)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}
