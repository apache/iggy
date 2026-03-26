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

func TestStoreConsumerOffsetRequest_MarshalBinary_WithoutPartition(t *testing.T) {
	streamID, _ := iggcon.NewIdentifier("stream")
	topicID, _ := iggcon.NewIdentifier(uint32(7))
	groupID, _ := iggcon.NewIdentifier("groupA")

	request := StoreConsumerOffsetRequest{
		StreamId: streamID,
		TopicId:  topicID,
		Consumer: iggcon.NewGroupConsumer(groupID),
		Offset:   42,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x02,                                           // Consumer kind (group)
		0x02, 0x06, 0x67, 0x72, 0x6F, 0x75, 0x70, 0x41, // Consumer id = "groupA"
		0x02, 0x06, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // Stream id = "stream"
		0x01, 0x04, 0x07, 0x00, 0x00, 0x00, // Topic id = 7
		0x00,                   // hasPartition
		0x00, 0x00, 0x00, 0x00, // partition (0)
		0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset (42)
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestGetConsumerOffset_MarshalBinary_WithPartition(t *testing.T) {
	streamID, _ := iggcon.NewIdentifier("stream")
	topicID, _ := iggcon.NewIdentifier(uint32(7))
	consumerID, _ := iggcon.NewIdentifier(uint32(1))
	partitionID := uint32(3)

	request := GetConsumerOffset{
		StreamId:    streamID,
		TopicId:     topicID,
		Consumer:    iggcon.NewSingleConsumer(consumerID),
		PartitionId: &partitionID,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x01,                               // Consumer kind (single)
		0x01, 0x04, 0x01, 0x00, 0x00, 0x00, // Consumer id = 1
		0x02, 0x06, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // Stream id = "stream"
		0x01, 0x04, 0x07, 0x00, 0x00, 0x00, // Topic id = 7
		0x01,                   // hasPartition
		0x03, 0x00, 0x00, 0x00, // partition (3)
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestDeleteConsumerOffset_MarshalBinary_WithPartition(t *testing.T) {
	streamID, _ := iggcon.NewIdentifier("stream")
	topicID, _ := iggcon.NewIdentifier(uint32(7))
	consumerID, _ := iggcon.NewIdentifier(uint32(1))
	partitionID := uint32(5)

	request := DeleteConsumerOffset{
		Consumer:    iggcon.NewSingleConsumer(consumerID),
		StreamId:    streamID,
		TopicId:     topicID,
		PartitionId: &partitionID,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x01,                               // Consumer kind (single)
		0x01, 0x04, 0x01, 0x00, 0x00, 0x00, // Consumer id = 1
		0x02, 0x06, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // Stream id = "stream"
		0x01, 0x04, 0x07, 0x00, 0x00, 0x00, // Topic id = 7
		0x01,                   // hasPartition
		0x05, 0x00, 0x00, 0x00, // partition (5)
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}
