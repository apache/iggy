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
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func TestSerialize_TcpFetchMessagesRequest(t *testing.T) {
	partitionId := uint32(123)
	consumerId, _ := iggcon.NewIdentifier(uint32(42))
	streamId, _ := iggcon.NewIdentifier("test_stream_id")
	topicId, _ := iggcon.NewIdentifier("test_topic_id")
	// Create a sample TcpFetchMessagesRequest
	request := TcpFetchMessagesRequest{
		Consumer:    iggcon.NewSingleConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: &partitionId,
		Strategy:    iggcon.FirstPollingStrategy(),
		Count:       100,
		AutoCommit:  true,
	}

	// Serialize the request
	serialized := request.Serialize()

	// Expected serialized bytes based on the provided sample request
	expected := []byte{
		0x01,                 // Consumer Kind
		0x01,                 // ConsumerId Kind (NumericId)
		0x04,                 // ConsumerId Length (4)
		0x2A, 0x00, 0x0, 0x0, // ConsumerId

		0x02,                                                                               // StreamId Kind (StringId)
		0x0E,                                                                               // StreamId Length (14)
		0x74, 0x65, 0x73, 0x74, 0x5F, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x5F, 0x69, 0x64, // StreamId

		0x02,                                                                         // TopicId Kind (StringId)
		0x0D,                                                                         // TopicId Length (13)
		0x74, 0x65, 0x73, 0x74, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, 0x5F, 0x69, 0x64, // TopicId

		0x01,                   // Partition present
		0x7B, 0x00, 0x00, 0x00, // PartitionId (123)
		0x03,                                           // PollingStrategy Kind
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // PollingStrategy Value (0)
		0x64, 0x00, 0x00, 0x00, // Count (100)
		0x01, // AutoCommit
	}

	// Check if the serialized bytes match the expected bytes
	if !areBytesEqual(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func areBytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
