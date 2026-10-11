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

func TestConsumerOffsetCommandsMarshalBinary(t *testing.T) {
	consumerID, err := iggcon.NewIdentifier(uint32(0x01020304))
	if err != nil {
		t.Fatal(err)
	}
	streamID, err := iggcon.NewIdentifier(uint32(0x05060708))
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := iggcon.NewIdentifier(uint32(0x090a0b0c))
	if err != nil {
		t.Fatal(err)
	}
	groupID, err := iggcon.NewIdentifier("cg")
	if err != nil {
		t.Fatal(err)
	}
	namedStreamID, err := iggcon.NewIdentifier("s")
	if err != nil {
		t.Fatal(err)
	}
	namedTopicID, err := iggcon.NewIdentifier("t")
	if err != nil {
		t.Fatal(err)
	}

	singleConsumer := iggcon.NewSingleConsumer(consumerID)
	groupConsumer := iggcon.NewGroupConsumer(groupID)
	partitionID := uint32(0x0d0e0f10)
	zeroPartitionID := uint32(0)

	for _, test := range []struct {
		name    string
		request Command
		code    Code
		want    []byte
	}{
		{
			name:    "get with partition",
			request: &GetConsumerOffset{Consumer: singleConsumer, StreamId: streamID, TopicId: topicID, PartitionId: &partitionID},
			code:    120,
			want:    []byte{1, 1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5, 1, 4, 12, 11, 10, 9, 1, 16, 15, 14, 13},
		},
		{
			name:    "get without partition",
			request: &GetConsumerOffset{Consumer: groupConsumer, StreamId: namedStreamID, TopicId: namedTopicID},
			code:    120,
			want:    []byte{2, 2, 2, 'c', 'g', 2, 1, 's', 2, 1, 't', 0, 0, 0, 0, 0},
		},
		{
			name:    "get partition zero",
			request: &GetConsumerOffset{Consumer: singleConsumer, StreamId: streamID, TopicId: topicID, PartitionId: &zeroPartitionID},
			code:    120,
			want:    []byte{1, 1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5, 1, 4, 12, 11, 10, 9, 1, 0, 0, 0, 0},
		},
		{
			name:    "store with partition",
			request: &StoreConsumerOffsetRequest{Consumer: singleConsumer, StreamId: streamID, TopicId: topicID, PartitionId: &partitionID, Offset: 0x1122334455667788},
			code:    121,
			want:    []byte{1, 1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5, 1, 4, 12, 11, 10, 9, 1, 16, 15, 14, 13, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 1},
		},
		{
			name:    "store without partition",
			request: &StoreConsumerOffsetRequest{Consumer: groupConsumer, StreamId: namedStreamID, TopicId: namedTopicID, Offset: 0},
			code:    121,
			want:    []byte{2, 2, 2, 'c', 'g', 2, 1, 's', 2, 1, 't', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		},
		{
			name:    "delete with partition",
			request: &DeleteConsumerOffset{Consumer: singleConsumer, StreamId: streamID, TopicId: topicID, PartitionId: &partitionID},
			code:    122,
			want:    []byte{1, 1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5, 1, 4, 12, 11, 10, 9, 1, 16, 15, 14, 13, 1},
		},
		{
			name:    "delete without partition",
			request: &DeleteConsumerOffset{Consumer: groupConsumer, StreamId: namedStreamID, TopicId: namedTopicID},
			code:    122,
			want:    []byte{2, 2, 2, 'c', 'g', 2, 1, 's', 2, 1, 't', 0, 0, 0, 0, 0, 1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.request.Code(); got != test.code {
				t.Fatalf("command code = %d, want %d", got, test.code)
			}
			got, err := test.request.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("command body = %v, want %v", got, test.want)
			}
		})
	}
}
