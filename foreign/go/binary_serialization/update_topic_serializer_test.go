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

func TestSerialize_UpdateTopic(t *testing.T) {
	request := TcpUpdateTopicRequest{
		StreamId:      iggcon.NewIdentifier("stream"),
		TopicId:       iggcon.NewIdentifier(1),
		Name:          "update_topic",
		MessageExpiry: 100000,
		MaxTopicSize:  100,
	}

	serialized1 := request.Serialize()

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

	if !areBytesEqual(serialized1, expected) {
		t.Errorf("Test case 1 failed. \nExpected:\t%v\nGot:\t\t%v", expected, serialized1)
	}
}
