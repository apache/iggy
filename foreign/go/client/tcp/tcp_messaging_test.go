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

package tcp

import (
	"context"
	"encoding/binary"
	"testing"
	"unsafe"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// buildPolledMessageResponse returns a valid PollMessages response body
// containing a single message with the given payload.
func buildPolledMessageResponse(payload []byte) []byte {
	hdr := iggcon.MessageHeader{PayloadLength: uint32(len(payload))}
	hdrBytes := hdr.ToBytes()

	// [partitionId:4][currentOffset:8][messageCount:4][header:64][payload:N]
	buf := make([]byte, 4+8+4+len(hdrBytes)+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], 1)
	binary.LittleEndian.PutUint64(buf[4:12], 0)
	binary.LittleEndian.PutUint32(buf[12:16], 1)
	copy(buf[16:], hdrBytes)
	copy(buf[16+len(hdrBytes):], payload)
	return buf
}

func basePtr(b []byte) uintptr {
	if len(b) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(&b[0]))
}

func TestPollMessagesInto_given_repeated_polls_when_payload_size_is_constant_should_reuse_buffer(t *testing.T) {
	c, serverConn := newTestClient(t)

	payload := []byte("hello iggy")
	responseBody := buildPolledMessageResponse(payload)

	streamId, _ := iggcon.NewIdentifier(uint32(1))
	topicId, _ := iggcon.NewIdentifier(uint32(1))
	consumerId, _ := iggcon.NewIdentifier(uint32(1))

	var buf []byte
	var firstPtr uintptr

	for i := range 3 {
		go serverRespond(t, serverConn, 0, responseBody)

		var polled *iggcon.PolledMessage
		var err error
		polled, buf, err = c.PollMessagesInto(
			context.Background(),
			streamId,
			topicId,
			iggcon.NewSingleConsumer(consumerId),
			iggcon.NextPollingStrategy(),
			1,
			false,
			nil,
			buf,
		)
		if err != nil {
			t.Fatalf("poll %d: unexpected error: %v", i, err)
		}
		if len(polled.Messages) != 1 {
			t.Fatalf("poll %d: expected 1 message, got %d", i, len(polled.Messages))
		}
		if string(polled.Messages[0].Payload) != string(payload) {
			t.Errorf("poll %d: got payload %q, want %q", i, polled.Messages[0].Payload, payload)
		}

		ptr := basePtr(buf)
		if i == 0 {
			firstPtr = ptr
		} else if ptr != firstPtr {
			t.Errorf("poll %d: buffer reallocated (addr changed from %x to %x)", i, firstPtr, ptr)
		}
	}
}
