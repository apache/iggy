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

func TestConsumerGroupCommandsMarshalBinary(t *testing.T) {
	streamID, err := iggcon.NewIdentifier(uint32(0x01020304))
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := iggcon.NewIdentifier(uint32(0x05060708))
	if err != nil {
		t.Fatal(err)
	}
	groupID, err := iggcon.NewIdentifier(uint32(0x090a0b0c))
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
	namedGroupID, err := iggcon.NewIdentifier("g")
	if err != nil {
		t.Fatal(err)
	}

	numericPath := TopicPath{StreamId: streamID, TopicId: topicID}
	namedPath := TopicPath{StreamId: namedStreamID, TopicId: namedTopicID}

	for _, test := range []struct {
		name    string
		request Command
		code    Code
		want    []byte
	}{
		{
			name:    "create with numeric identifiers",
			request: &CreateConsumerGroup{TopicPath: numericPath, Name: "team"},
			code:    602,
			want:    []byte{1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5, 4, 't', 'e', 'a', 'm'},
		},
		{
			name:    "create with named identifiers",
			request: &CreateConsumerGroup{TopicPath: namedPath, Name: "team"},
			code:    602,
			want:    []byte{2, 1, 's', 2, 1, 't', 4, 't', 'e', 'a', 'm'},
		},
		{
			name:    "get group",
			request: &GetConsumerGroup{TopicPath: numericPath, GroupId: namedGroupID},
			code:    600,
			want:    []byte{1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5, 2, 1, 'g'},
		},
		{
			name:    "get groups with numeric identifiers",
			request: &GetConsumerGroups{StreamId: streamID, TopicId: topicID},
			code:    601,
			want:    []byte{1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5},
		},
		{
			name:    "get groups with named identifiers",
			request: &GetConsumerGroups{StreamId: namedStreamID, TopicId: namedTopicID},
			code:    601,
			want:    []byte{2, 1, 's', 2, 1, 't'},
		},
		{
			name:    "join group",
			request: &JoinConsumerGroup{TopicPath: namedPath, GroupId: groupID},
			code:    604,
			want:    []byte{2, 1, 's', 2, 1, 't', 1, 4, 12, 11, 10, 9},
		},
		{
			name:    "leave group",
			request: &LeaveConsumerGroup{TopicPath: numericPath, GroupId: namedGroupID},
			code:    605,
			want:    []byte{1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5, 2, 1, 'g'},
		},
		{
			name:    "sync group",
			request: &SyncConsumerGroup{TopicPath: namedPath, GroupId: groupID},
			code:    606,
			want:    []byte{2, 1, 's', 2, 1, 't', 1, 4, 12, 11, 10, 9},
		},
		{
			name:    "delete group",
			request: &DeleteConsumerGroup{TopicPath: numericPath, GroupId: namedGroupID},
			code:    603,
			want:    []byte{1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5, 2, 1, 'g'},
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
