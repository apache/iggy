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

package benchmarks

import (
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
)

func newPollRequest(b *testing.B) *command.PollMessages {
	b.Helper()
	consumerId, _ := iggcon.NewIdentifier(uint32(42))
	streamId, _ := iggcon.NewIdentifier("test_stream_id")
	topicId, _ := iggcon.NewIdentifier("test_topic_id")
	pid := uint32(1)
	return &command.PollMessages{
		Consumer:    iggcon.NewSingleConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: &pid,
		Strategy:    iggcon.NextPollingStrategy(),
		Count:       1000,
		AutoCommit:  true,
	}
}

// BenchmarkPollMessagesMarshalBinary measures the legacy hot path: a fresh
// []byte allocated per call.
func BenchmarkPollMessagesMarshalBinary(b *testing.B) {
	req := newPollRequest(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := req.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

// BenchmarkPollMessagesAppendBinary measures the new hot path: encoding into
// a caller-owned, reusable buffer.
func BenchmarkPollMessagesAppendBinary(b *testing.B) {
	req := newPollRequest(b)
	buf := make([]byte, 0, req.MarshalledSize())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := req.AppendBinary(buf[:0])
		if err != nil {
			b.Fatal(err)
		}
		buf = out
	}
}

// BenchmarkIdentifierMarshalBinary measures the still-allocating
// MarshalBinary path on the constant Identifier values used per poll.
func BenchmarkIdentifierMarshalBinary(b *testing.B) {
	id, _ := iggcon.NewIdentifier("test_stream_id")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := id.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

// BenchmarkIdentifierAppendBinary measures the zero-alloc encoder using the
// cached wire bytes set in NewIdentifier.
func BenchmarkIdentifierAppendBinary(b *testing.B) {
	id, _ := iggcon.NewIdentifier("test_stream_id")
	buf := make([]byte, 0, id.MarshalledSize())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := id.AppendBinary(buf[:0])
		if err != nil {
			b.Fatal(err)
		}
		buf = out
	}
}
