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

// TestSerialize_CreateConsumerGroup_NumericIds tests CreateConsumerGroup with numeric identifiers
func TestSerialize_CreateConsumerGroup_NumericIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(1))
	topicId, _ := iggcon.NewIdentifier(uint32(2))

	cmd := CreateConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		Name: "group1",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateConsumerGroup: %v", err)
	}

	expected := []byte{
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x01, 0x00, 0x00, 0x00, // StreamId Value = 1
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x02, 0x00, 0x00, 0x00, // TopicId Value = 2
		0x06,                               // Name Length = 6
		0x67, 0x72, 0x6F, 0x75, 0x70, 0x31, // "group1"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreateConsumerGroup_StringIds tests CreateConsumerGroup with string identifiers
func TestSerialize_CreateConsumerGroup_StringIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("my_stream")
	topicId, _ := iggcon.NewIdentifier("my_topic")

	cmd := CreateConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		Name: "consumers",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateConsumerGroup: %v", err)
	}

	expected := []byte{
		0x02,                                                 // StreamId Kind = StringId
		0x09,                                                 // StreamId Length = 9
		0x6D, 0x79, 0x5F, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // "my_stream"
		0x02,                                           // TopicId Kind = StringId
		0x08,                                           // TopicId Length = 8
		0x6D, 0x79, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, // "my_topic"
		0x09,                                                 // Name Length = 9
		0x63, 0x6F, 0x6E, 0x73, 0x75, 0x6D, 0x65, 0x72, 0x73, // "consumers"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreateConsumerGroup_EmptyName tests edge case with empty group name
func TestSerialize_CreateConsumerGroup_EmptyName(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier("topic")

	cmd := CreateConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		Name: "",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreateConsumerGroup with empty name: %v", err)
	}

	expected := []byte{
		0x02,                               // StreamId Kind = StringId
		0x06,                               // StreamId Length = 6
		0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // "stream"
		0x02,                         // TopicId Kind = StringId
		0x05,                         // TopicId Length = 5
		0x74, 0x6F, 0x70, 0x69, 0x63, // "topic"
		0x00, // Name Length = 0
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetConsumerGroups_NumericIds tests GetConsumerGroups with numeric identifiers
func TestSerialize_GetConsumerGroups_NumericIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(10))
	topicId, _ := iggcon.NewIdentifier(uint32(20))

	cmd := GetConsumerGroups{
		StreamId: streamId,
		TopicId:  topicId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetConsumerGroups: %v", err)
	}

	expected := []byte{
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x0A, 0x00, 0x00, 0x00, // StreamId Value = 10
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x14, 0x00, 0x00, 0x00, // TopicId Value = 20
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetConsumerGroups_StringIds tests GetConsumerGroups with string identifiers
func TestSerialize_GetConsumerGroups_StringIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("events")
	topicId, _ := iggcon.NewIdentifier("logs")

	cmd := GetConsumerGroups{
		StreamId: streamId,
		TopicId:  topicId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetConsumerGroups: %v", err)
	}

	expected := []byte{
		0x02,                               // StreamId Kind = StringId
		0x06,                               // StreamId Length = 6
		0x65, 0x76, 0x65, 0x6E, 0x74, 0x73, // "events"
		0x02,                   // TopicId Kind = StringId
		0x04,                   // TopicId Length = 4
		0x6C, 0x6F, 0x67, 0x73, // "logs"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetConsumerGroup_NumericIds tests GetConsumerGroup with all numeric identifiers
func TestSerialize_GetConsumerGroup_NumericIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(100))
	topicId, _ := iggcon.NewIdentifier(uint32(200))
	groupId, _ := iggcon.NewIdentifier(uint32(300))

	cmd := GetConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetConsumerGroup: %v", err)
	}

	expected := []byte{
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x64, 0x00, 0x00, 0x00, // StreamId Value = 100
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0xC8, 0x00, 0x00, 0x00, // TopicId Value = 200
		0x01,                   // GroupId Kind = NumericId
		0x04,                   // GroupId Length = 4
		0x2C, 0x01, 0x00, 0x00, // GroupId Value = 300
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetConsumerGroup_StringIds tests GetConsumerGroup with all string identifiers
func TestSerialize_GetConsumerGroup_StringIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream1")
	topicId, _ := iggcon.NewIdentifier("topic1")
	groupId, _ := iggcon.NewIdentifier("group1")

	cmd := GetConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetConsumerGroup: %v", err)
	}

	expected := []byte{
		0x02,                                     // StreamId Kind = StringId
		0x07,                                     // StreamId Length = 7
		0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x31, // "stream1"
		0x02,                               // TopicId Kind = StringId
		0x06,                               // TopicId Length = 6
		0x74, 0x6F, 0x70, 0x69, 0x63, 0x31, // "topic1"
		0x02,                               // GroupId Kind = StringId
		0x06,                               // GroupId Length = 6
		0x67, 0x72, 0x6F, 0x75, 0x70, 0x31, // "group1"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetConsumerGroup_MixedIds tests GetConsumerGroup with mixed identifier types
func TestSerialize_GetConsumerGroup_MixedIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(42))
	topicId, _ := iggcon.NewIdentifier("events")
	groupId, _ := iggcon.NewIdentifier(uint32(999))

	cmd := GetConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetConsumerGroup with mixed IDs: %v", err)
	}

	expected := []byte{
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x2A, 0x00, 0x00, 0x00, // StreamId Value = 42
		0x02,                               // TopicId Kind = StringId
		0x06,                               // TopicId Length = 6
		0x65, 0x76, 0x65, 0x6E, 0x74, 0x73, // "events"
		0x01,                   // GroupId Kind = NumericId
		0x04,                   // GroupId Length = 4
		0xE7, 0x03, 0x00, 0x00, // GroupId Value = 999
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_JoinConsumerGroup tests JoinConsumerGroup serialization
func TestSerialize_JoinConsumerGroup(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("prod")
	topicId, _ := iggcon.NewIdentifier("orders")
	groupId, _ := iggcon.NewIdentifier(uint32(5))

	cmd := JoinConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize JoinConsumerGroup: %v", err)
	}

	expected := []byte{
		0x02,                   // StreamId Kind = StringId
		0x04,                   // StreamId Length = 4
		0x70, 0x72, 0x6F, 0x64, // "prod"
		0x02,                               // TopicId Kind = StringId
		0x06,                               // TopicId Length = 6
		0x6F, 0x72, 0x64, 0x65, 0x72, 0x73, // "orders"
		0x01,                   // GroupId Kind = NumericId
		0x04,                   // GroupId Length = 4
		0x05, 0x00, 0x00, 0x00, // GroupId Value = 5
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_LeaveConsumerGroup tests LeaveConsumerGroup serialization
func TestSerialize_LeaveConsumerGroup(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(7))
	topicId, _ := iggcon.NewIdentifier(uint32(8))
	groupId, _ := iggcon.NewIdentifier("my_group")

	cmd := LeaveConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LeaveConsumerGroup: %v", err)
	}

	expected := []byte{
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x07, 0x00, 0x00, 0x00, // StreamId Value = 7
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x08, 0x00, 0x00, 0x00, // TopicId Value = 8
		0x02,                                           // GroupId Kind = StringId
		0x08,                                           // GroupId Length = 8
		0x6D, 0x79, 0x5F, 0x67, 0x72, 0x6F, 0x75, 0x70, // "my_group"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeleteConsumerGroup_NumericIds tests DeleteConsumerGroup with numeric identifiers
func TestSerialize_DeleteConsumerGroup_NumericIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(11))
	topicId, _ := iggcon.NewIdentifier(uint32(22))
	groupId, _ := iggcon.NewIdentifier(uint32(33))

	cmd := DeleteConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeleteConsumerGroup: %v", err)
	}

	expected := []byte{
		0x01,                   // StreamId Kind = NumericId
		0x04,                   // StreamId Length = 4
		0x0B, 0x00, 0x00, 0x00, // StreamId Value = 11
		0x01,                   // TopicId Kind = NumericId
		0x04,                   // TopicId Length = 4
		0x16, 0x00, 0x00, 0x00, // TopicId Value = 22
		0x01,                   // GroupId Kind = NumericId
		0x04,                   // GroupId Length = 4
		0x21, 0x00, 0x00, 0x00, // GroupId Value = 33
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeleteConsumerGroup_StringIds tests DeleteConsumerGroup with string identifiers
func TestSerialize_DeleteConsumerGroup_StringIds(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("analytics")
	topicId, _ := iggcon.NewIdentifier("metrics")
	groupId, _ := iggcon.NewIdentifier("deprecated")

	cmd := DeleteConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeleteConsumerGroup: %v", err)
	}

	expected := []byte{
		0x02,                                                 // StreamId Kind = StringId
		0x09,                                                 // StreamId Length = 9
		0x61, 0x6E, 0x61, 0x6C, 0x79, 0x74, 0x69, 0x63, 0x73, // "analytics"
		0x02,                                     // TopicId Kind = StringId
		0x07,                                     // TopicId Length = 7
		0x6D, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, // "metrics"
		0x02,                                                       // GroupId Kind = StringId
		0x0A,                                                       // GroupId Length = 10
		0x64, 0x65, 0x70, 0x72, 0x65, 0x63, 0x61, 0x74, 0x65, 0x64, // "deprecated"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}
