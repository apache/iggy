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

// Helper function to create pointer to uint32
func uint32Ptr(v uint32) *uint32 {
	return &v
}

// TestSerialize_StoreConsumerOffsetRequest_WithPartition tests StoreConsumerOffset with partition specified
func TestSerialize_StoreConsumerOffsetRequest_WithPartition(t *testing.T) {
	consumerId, _ := iggcon.NewIdentifier(uint32(42))
	streamId, _ := iggcon.NewIdentifier("stream1")
	topicId, _ := iggcon.NewIdentifier(uint32(10))
	partitionId := uint32Ptr(5)

	cmd := StoreConsumerOffsetRequest{
		Consumer:    iggcon.NewSingleConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: partitionId,
		Offset:      1000,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize StoreConsumerOffsetRequest: %v", err)
	}

	expected := []byte{
		0x01,                   // Consumer Kind = Single
		0x01,                   // ConsumerId Kind = NumericId
		0x04,                   // ConsumerId Length = 4
		0x2A, 0x00, 0x00, 0x00, // ConsumerId Value = 42
		0x02,                                     // StreamId Kind = StringId
		0x07,                                     // StreamId Length = 7
		0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x31, // "stream1"
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x0A, 0x00, 0x00, 0x00, // TopicId Value = 10
		0x01,                   // hasPartition = 1
		0x05, 0x00, 0x00, 0x00, // PartitionId = 5
		0xE8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Offset = 1000 (uint64)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_StoreConsumerOffsetRequest_WithoutPartition tests StoreConsumerOffset without partition
func TestSerialize_StoreConsumerOffsetRequest_WithoutPartition(t *testing.T) {
	consumerId, _ := iggcon.NewIdentifier("consumer1")
	streamId, _ := iggcon.NewIdentifier(uint32(1))
	topicId, _ := iggcon.NewIdentifier(uint32(2))

	cmd := StoreConsumerOffsetRequest{
		Consumer:    iggcon.NewGroupConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: nil, // No partition specified
		Offset:      5000,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize StoreConsumerOffsetRequest without partition: %v", err)
	}

	expected := []byte{
		0x02,                                                 // Consumer Kind = Group
		0x02,                                                 // ConsumerId Kind = StringId
		0x09,                                                 // ConsumerId Length = 9
		0x63, 0x6F, 0x6E, 0x73, 0x75, 0x6D, 0x65, 0x72, 0x31, // "consumer1"
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x01, 0x00, 0x00, 0x00, // StreamId Value = 1
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x02, 0x00, 0x00, 0x00, // TopicId Value = 2
		0x00,                   // hasPartition = 0
		0x00, 0x00, 0x00, 0x00, // PartitionId = 0 (default when not set)
		0x88, 0x13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Offset = 5000 (uint64)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_StoreConsumerOffsetRequest_ZeroOffset tests with zero offset
func TestSerialize_StoreConsumerOffsetRequest_ZeroOffset(t *testing.T) {
	consumerId, _ := iggcon.NewIdentifier(uint32(1))
	streamId, _ := iggcon.NewIdentifier("s")
	topicId, _ := iggcon.NewIdentifier("t")

	cmd := StoreConsumerOffsetRequest{
		Consumer:    iggcon.NewSingleConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: uint32Ptr(0),
		Offset:      0,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize StoreConsumerOffsetRequest with zero offset: %v", err)
	}

	expected := []byte{
		0x01,                   // Consumer Kind = Single
		0x01,                   // ConsumerId Kind = NumericId
		0x04,                   // ConsumerId Length = 4
		0x01, 0x00, 0x00, 0x00, // ConsumerId Value = 1
		0x02,                   // StreamId Kind = StringId
		0x01,                   // StreamId Length = 1
		0x73,                   // "s"
		0x02,                   // TopicId Kind = StringId
		0x01,                   // TopicId Length = 1
		0x74,                   // "t"
		0x01,                   // hasPartition = 1
		0x00, 0x00, 0x00, 0x00, // PartitionId = 0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Offset = 0 (uint64)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_StoreConsumerOffsetRequest_MaxOffset tests with maximum uint64 offset
func TestSerialize_StoreConsumerOffsetRequest_MaxOffset(t *testing.T) {
	consumerId, _ := iggcon.NewIdentifier(uint32(999))
	streamId, _ := iggcon.NewIdentifier(uint32(100))
	topicId, _ := iggcon.NewIdentifier(uint32(200))

	cmd := StoreConsumerOffsetRequest{
		Consumer:    iggcon.NewSingleConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: uint32Ptr(10),
		Offset:      18446744073709551615, // Max uint64
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize StoreConsumerOffsetRequest with max offset: %v", err)
	}

	expected := []byte{
		0x01,                   // Consumer Kind = Single
		0x01,                   // ConsumerId Kind = NumericId
		0x04,                   // ConsumerId Length = 4
		0xE7, 0x03, 0x00, 0x00, // ConsumerId Value = 999
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x64, 0x00, 0x00, 0x00, // StreamId Value = 100
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0xC8, 0x00, 0x00, 0x00, // TopicId Value = 200
		0x01,                   // hasPartition = 1
		0x0A, 0x00, 0x00, 0x00, // PartitionId = 10
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // Offset = max uint64
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetConsumerOffset_WithPartition tests GetConsumerOffset with partition
func TestSerialize_GetConsumerOffset_WithPartition(t *testing.T) {
	consumerId, _ := iggcon.NewIdentifier("grp1")
	streamId, _ := iggcon.NewIdentifier("events")
	topicId, _ := iggcon.NewIdentifier(uint32(5))

	cmd := GetConsumerOffset{
		Consumer:    iggcon.NewGroupConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: uint32Ptr(3),
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetConsumerOffset: %v", err)
	}

	expected := []byte{
		0x02,                   // Consumer Kind = Group
		0x02,                   // ConsumerId Kind = StringId
		0x04,                   // ConsumerId Length = 4
		0x67, 0x72, 0x70, 0x31, // "grp1"
		0x02,                               // StreamId Kind = StringId
		0x06,                               // StreamId Length = 6
		0x65, 0x76, 0x65, 0x6E, 0x74, 0x73, // "events"
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x05, 0x00, 0x00, 0x00, // TopicId Value = 5
		0x01,                   // hasPartition = 1
		0x03, 0x00, 0x00, 0x00, // PartitionId = 3
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetConsumerOffset_WithoutPartition tests GetConsumerOffset without partition
func TestSerialize_GetConsumerOffset_WithoutPartition(t *testing.T) {
	consumerId, _ := iggcon.NewIdentifier(uint32(123))
	streamId, _ := iggcon.NewIdentifier(uint32(1))
	topicId, _ := iggcon.NewIdentifier(uint32(2))

	cmd := GetConsumerOffset{
		Consumer:    iggcon.NewSingleConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: nil,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetConsumerOffset without partition: %v", err)
	}

	expected := []byte{
		0x01,                   // Consumer Kind = Single
		0x01,                   // ConsumerId Kind = NumericId
		0x04,                   // ConsumerId Length = 4
		0x7B, 0x00, 0x00, 0x00, // ConsumerId Value = 123
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x01, 0x00, 0x00, 0x00, // StreamId Value = 1
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x02, 0x00, 0x00, 0x00, // TopicId Value = 2
		0x00,                   // hasPartition = 0
		0x00, 0x00, 0x00, 0x00, // PartitionId = 0 (default)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetConsumerOffset_MixedIds tests with mixed identifier types
func TestSerialize_GetConsumerOffset_MixedIds(t *testing.T) {
	consumerId, _ := iggcon.NewIdentifier(uint32(42))
	streamId, _ := iggcon.NewIdentifier("prod")
	topicId, _ := iggcon.NewIdentifier("logs")

	cmd := GetConsumerOffset{
		Consumer:    iggcon.NewSingleConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: uint32Ptr(7),
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetConsumerOffset with mixed IDs: %v", err)
	}

	expected := []byte{
		0x01,                   // Consumer Kind = Single
		0x01,                   // ConsumerId Kind = NumericId
		0x04,                   // ConsumerId Length = 4
		0x2A, 0x00, 0x00, 0x00, // ConsumerId Value = 42
		0x02,                   // StreamId Kind = StringId
		0x04,                   // StreamId Length = 4
		0x70, 0x72, 0x6F, 0x64, // "prod"
		0x02,                   // TopicId Kind = StringId
		0x04,                   // TopicId Length = 4
		0x6C, 0x6F, 0x67, 0x73, // "logs"
		0x01,                   // hasPartition = 1
		0x07, 0x00, 0x00, 0x00, // PartitionId = 7
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeleteConsumerOffset_WithPartition tests DeleteConsumerOffset with partition
func TestSerialize_DeleteConsumerOffset_WithPartition(t *testing.T) {
	consumerId, _ := iggcon.NewIdentifier("consumer_grp")
	streamId, _ := iggcon.NewIdentifier(uint32(10))
	topicId, _ := iggcon.NewIdentifier(uint32(20))

	cmd := DeleteConsumerOffset{
		Consumer:    iggcon.NewGroupConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: uint32Ptr(15),
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeleteConsumerOffset: %v", err)
	}

	expected := []byte{
		0x02,                                                                   // Consumer Kind = Group
		0x02,                                                                   // ConsumerId Kind = StringId
		0x0C,                                                                   // ConsumerId Length = 12
		0x63, 0x6F, 0x6E, 0x73, 0x75, 0x6D, 0x65, 0x72, 0x5F, 0x67, 0x72, 0x70, // "consumer_grp"
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x0A, 0x00, 0x00, 0x00, // StreamId Value = 10
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x14, 0x00, 0x00, 0x00, // TopicId Value = 20
		0x01,                   // hasPartition = 1
		0x0F, 0x00, 0x00, 0x00, // PartitionId = 15
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeleteConsumerOffset_WithoutPartition tests DeleteConsumerOffset without partition
func TestSerialize_DeleteConsumerOffset_WithoutPartition(t *testing.T) {
	consumerId, _ := iggcon.NewIdentifier(uint32(7))
	streamId, _ := iggcon.NewIdentifier("analytics")
	topicId, _ := iggcon.NewIdentifier("metrics")

	cmd := DeleteConsumerOffset{
		Consumer:    iggcon.NewSingleConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: nil,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeleteConsumerOffset without partition: %v", err)
	}

	expected := []byte{
		0x01,                   // Consumer Kind = Single
		0x01,                   // ConsumerId Kind = NumericId
		0x04,                   // ConsumerId Length = 4
		0x07, 0x00, 0x00, 0x00, // ConsumerId Value = 7
		0x02,                                                 // StreamId Kind = StringId
		0x09,                                                 // StreamId Length = 9
		0x61, 0x6E, 0x61, 0x6C, 0x79, 0x74, 0x69, 0x63, 0x73, // "analytics"
		0x02,                                     // TopicId Kind = StringId
		0x07,                                     // TopicId Length = 7
		0x6D, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, // "metrics"
		0x00,                   // hasPartition = 0
		0x00, 0x00, 0x00, 0x00, // PartitionId = 0 (default)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeleteConsumerOffset_MaxPartition tests with maximum partition ID
func TestSerialize_DeleteConsumerOffset_MaxPartition(t *testing.T) {
	consumerId, _ := iggcon.NewIdentifier(uint32(1))
	streamId, _ := iggcon.NewIdentifier(uint32(2))
	topicId, _ := iggcon.NewIdentifier(uint32(3))

	cmd := DeleteConsumerOffset{
		Consumer:    iggcon.NewSingleConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: uint32Ptr(4294967295), // Max uint32
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeleteConsumerOffset with max partition: %v", err)
	}

	expected := []byte{
		0x01,                   // Consumer Kind = Single
		0x01,                   // ConsumerId Kind = NumericId
		0x04,                   // ConsumerId Length = 4
		0x01, 0x00, 0x00, 0x00, // ConsumerId Value = 1
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x02, 0x00, 0x00, 0x00, // StreamId Value = 2
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x03, 0x00, 0x00, 0x00, // TopicId Value = 3
		0x01,                   // hasPartition = 1
		0xFF, 0xFF, 0xFF, 0xFF, // PartitionId = 4294967295 (max uint32)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}
