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

func TestCreatePartitions_MarshalBinary(t *testing.T) {
	streamID, _ := iggcon.NewIdentifier("stream")
	topicID, _ := iggcon.NewIdentifier(uint32(7))

	request := CreatePartitions{
		StreamId:        streamID,
		TopicId:         topicID,
		PartitionsCount: 2,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x02, 0x06, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // Stream id = "stream"
		0x01, 0x04, 0x07, 0x00, 0x00, 0x00, // Topic id = 7
		0x02, 0x00, 0x00, 0x00, // PartitionsCount = 2
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestDeletePartitions_MarshalBinary(t *testing.T) {
	streamID, _ := iggcon.NewIdentifier("stream")
	topicID, _ := iggcon.NewIdentifier(uint32(7))

	request := DeletePartitions{
		StreamId:        streamID,
		TopicId:         topicID,
		PartitionsCount: 1,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []byte{
		0x02, 0x06, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // Stream id = "stream"
		0x01, 0x04, 0x07, 0x00, 0x00, 0x00, // Topic id = 7
		0x01, 0x00, 0x00, 0x00, // PartitionsCount = 1
	}

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}
