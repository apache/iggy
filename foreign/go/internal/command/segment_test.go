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

func TestSerialize_DeleteSegments(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier(uint32(1))
	request := DeleteSegments{
		StreamId:      streamId,
		TopicId:       topicId,
		PartitionId:   2,
		SegmentsCount: 3,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Errorf("Failed to serialize DeleteSegments: %v", err)
	}

	expected := []byte{
		0x02,                               // StreamId Kind (StringId)
		0x06,                               // StreamId Length (6)
		0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // StreamId Value ("stream")
		0x01,                   // TopicId Kind (NumericId)
		0x04,                   // TopicId Length (4)
		0x01, 0x00, 0x00, 0x00, // TopicId Value (1)
		0x02, 0x00, 0x00, 0x00, // PartitionId (2)
		0x03, 0x00, 0x00, 0x00, // SegmentsCount (3)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Test case failed. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestDeleteSegments_Code(t *testing.T) {
	request := DeleteSegments{}

	if request.Code() != DeleteSegmentsCode {
		t.Errorf("Expected command code %v, got %v", DeleteSegmentsCode, request.Code())
	}
}
