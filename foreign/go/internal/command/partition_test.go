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

func TestPartitionCommandsMarshalBinary(t *testing.T) {
	streamID, err := iggcon.NewIdentifier(uint32(0x01020304))
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := iggcon.NewIdentifier(uint32(0x05060708))
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

	for _, test := range []struct {
		name    string
		request Command
		code    Code
		want    []byte
	}{
		{
			name:    "create numeric identifiers",
			request: &CreatePartitions{StreamId: streamID, TopicId: topicID, PartitionsCount: 0x090a0b0c},
			code:    402,
			want:    []byte{1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5, 12, 11, 10, 9},
		},
		{
			name:    "create named identifiers",
			request: &CreatePartitions{StreamId: namedStreamID, TopicId: namedTopicID, PartitionsCount: 0x01020304},
			code:    402,
			want:    []byte{2, 1, 's', 2, 1, 't', 4, 3, 2, 1},
		},
		{
			name:    "delete numeric identifiers",
			request: &DeletePartitions{StreamId: streamID, TopicId: topicID, PartitionsCount: 0x090a0b0c},
			code:    403,
			want:    []byte{1, 4, 4, 3, 2, 1, 1, 4, 8, 7, 6, 5, 12, 11, 10, 9},
		},
		{
			name:    "delete named identifiers",
			request: &DeletePartitions{StreamId: namedStreamID, TopicId: namedTopicID, PartitionsCount: 0x01020304},
			code:    403,
			want:    []byte{2, 1, 's', 2, 1, 't', 4, 3, 2, 1},
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
