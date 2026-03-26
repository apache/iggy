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

func TestCreateConsumerGroup_MarshalBinary(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier(uint32(5))

	request := CreateConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		Name: "test-group",
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x02, 0x06, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // stream id = "stream"
		0x01, 0x04, 0x05, 0x00, 0x00, 0x00, // topic id = 5
		0x0A,                                                       // name length = 10
		0x74, 0x65, 0x73, 0x74, 0x2D, 0x67, 0x72, 0x6F, 0x75, 0x70, // "test-group"
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestGetConsumerGroup_MarshalBinary(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier(uint32(5))
	groupId, _ := iggcon.NewIdentifier("grp")

	request := GetConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected, _ := iggcon.MarshalIdentifiers(streamId, topicId, groupId)

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestGetConsumerGroups_MarshalBinary(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier(uint32(5))

	request := GetConsumerGroups{
		StreamId: streamId,
		TopicId:  topicId,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected, _ := iggcon.MarshalIdentifiers(streamId, topicId)

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestJoinConsumerGroup_MarshalBinary(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier(uint32(5))
	groupId, _ := iggcon.NewIdentifier("grp")

	request := JoinConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected, _ := iggcon.MarshalIdentifiers(streamId, topicId, groupId)

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestLeaveConsumerGroup_MarshalBinary(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier(uint32(5))
	groupId, _ := iggcon.NewIdentifier("grp")

	request := LeaveConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected, _ := iggcon.MarshalIdentifiers(streamId, topicId, groupId)

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestDeleteConsumerGroup_MarshalBinary(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier(uint32(5))
	groupId, _ := iggcon.NewIdentifier("grp")

	request := DeleteConsumerGroup{
		TopicPath: TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected, _ := iggcon.MarshalIdentifiers(streamId, topicId, groupId)

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}
