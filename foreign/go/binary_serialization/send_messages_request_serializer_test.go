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
	"github.com/google/uuid"
)

func TestSerialize_SendMessagesRequest(t *testing.T) {
	message1 := generateTestMessage("data1")
	streamId, _ := iggcon.NewIdentifier("test_stream_id")
	topicId, _ := iggcon.NewIdentifier("test_topic_id")
	request := TcpSendMessagesRequest{
		StreamId:     streamId,
		TopicId:      topicId,
		Partitioning: iggcon.PartitionId(1),
		Messages: []iggcon.IggyMessage{
			message1,
		},
	}

	// Serialize the request
	serialized := request.Serialize(iggcon.MESSAGE_COMPRESSION_NONE)

	// Expected serialized bytes based on the provided sample request
	expected := []byte{
		0x29, 0x0, 0x0, 0x0, // metadataLength
		0x02,                                                                               // StreamId Kind (StringId)
		0x0E,                                                                               // StreamId Length (14)
		0x74, 0x65, 0x73, 0x74, 0x5F, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x5F, 0x69, 0x64, // StreamId

		0x02,                                                                         // TopicId Kind (StringId)
		0x0D,                                                                         // TopicId Length (13)
		0x74, 0x65, 0x73, 0x74, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, 0x5F, 0x69, 0x64, // TopicId
		0x02,                   // PartitionIdKind
		0x04,                   // Partitioning Length
		0x01, 0x00, 0x00, 0x00, // PartitionId (123)
		0x01, 0x0, 0x0, 0x0, // MessageCount
		0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // Index (16*1) bytes
	}
	expected = append(expected, message1.Header.ToBytes()...)
	expected = append(expected, message1.Payload...)
	expected = append(expected, message1.UserHeaders...)

	// Check if the serialized bytes match the expected bytes
	if !areBytesEqual(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func createDefaultMessageHeaders() map[iggcon.HeaderKey]iggcon.HeaderValue {
	return map[iggcon.HeaderKey]iggcon.HeaderValue{
		{Value: "HeaderKey1"}: {Kind: iggcon.String, Value: []byte("Value 1")},
		{Value: "HeaderKey2"}: {Kind: iggcon.Uint32, Value: []byte{0x01, 0x02, 0x03, 0x04}},
	}
}

func generateTestMessage(payload string) iggcon.IggyMessage {
	msg, _ := iggcon.NewIggyMessage(
		[]byte(payload),
		iggcon.WithID(uuid.New()),
		iggcon.WithUserHeaders(createDefaultMessageHeaders()))
	return msg
}
