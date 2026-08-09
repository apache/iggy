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

package vsr

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeRequestHeader_WritesEveryFieldAtItsCanonicalOffset(t *testing.T) {
	var header [HeaderSize]byte
	EncodeRequestHeader(&header, RequestFields{
		Size:              HeaderSize + 3,
		Client:            ClientID{Lo: 0x99AABBCCDDEEFF00, Hi: 0x1122334455667788},
		Request:           0x0102030405060708,
		Operation:         OperationNonReplicated,
		Namespace:         0x8877665544332211,
		Session:           0x1020304050607080,
		NonReplicatedCode: 60001,
	})

	assert.Equal(t, uint32(HeaderSize+3), binary.LittleEndian.Uint32(header[requestOffsetSize:]))
	assert.Equal(t, byte(FrameRequest), header[requestOffsetCommand])
	assert.Equal(t, uint64(0x99AABBCCDDEEFF00), binary.LittleEndian.Uint64(header[requestOffsetClient:]))
	assert.Equal(t, uint64(0x1122334455667788), binary.LittleEndian.Uint64(header[requestOffsetClient+8:]))
	assert.Equal(t, uint64(0x0102030405060708), binary.LittleEndian.Uint64(header[requestOffsetRequest:]))
	assert.Equal(t, byte(OperationNonReplicated), header[requestOffsetOperation])
	assert.Equal(t, uint64(0x8877665544332211), binary.LittleEndian.Uint64(header[requestOffsetNamespace:]))
	assert.Equal(t, uint64(0x1020304050607080), binary.LittleEndian.Uint64(header[requestOffsetSession:]))
	assert.Equal(t, uint32(60001), binary.LittleEndian.Uint32(header[requestOffsetReserved:]))
	assert.Zero(t, binary.LittleEndian.Uint64(header[requestOffsetTimestamp:]), "timestamp stays zero")
}

func TestEncodeRequestHeader_LeavesEveryUnwrittenByteZero(t *testing.T) {
	var header [HeaderSize]byte
	EncodeRequestHeader(&header, RequestFields{
		Size:      HeaderSize,
		Client:    ClientID{Lo: 1},
		Operation: OperationRegister,
		Namespace: MetadataConsensusNamespace,
	})

	var expected [HeaderSize]byte
	binary.LittleEndian.PutUint32(expected[requestOffsetSize:], HeaderSize)
	expected[requestOffsetCommand] = byte(FrameRequest)
	binary.LittleEndian.PutUint64(expected[requestOffsetClient:], 1)
	expected[requestOffsetOperation] = byte(OperationRegister)
	binary.LittleEndian.PutUint64(expected[requestOffsetNamespace:], MetadataConsensusNamespace)

	assert.Equal(t, expected, header)
}

func TestEncodeRequestHeader_ZeroesAReusedBuffer(t *testing.T) {
	var header [HeaderSize]byte
	for i := range header {
		header[i] = 0xFF
	}
	EncodeRequestHeader(&header, RequestFields{
		Size:      HeaderSize,
		Client:    ClientID{Lo: 7},
		Operation: OperationRegister,
	})

	assert.Zero(t, binary.LittleEndian.Uint32(header[requestOffsetReserved:]))
	assert.Zero(t, binary.LittleEndian.Uint64(header[0:8]), "checksum stays zero")
	assert.Zero(t, binary.LittleEndian.Uint64(header[requestOffsetTimestamp:]))
}

func TestEncodeRequestHeader_SupportsMaximumWidthFields(t *testing.T) {
	var header [HeaderSize]byte
	EncodeRequestHeader(&header, RequestFields{
		Size:      math.MaxUint32,
		Client:    ClientID{Lo: math.MaxUint64, Hi: math.MaxUint64},
		Request:   math.MaxUint64,
		Operation: OperationSendMessages,
		Namespace: math.MaxUint64,
		Session:   math.MaxUint64,
	})

	assert.Equal(t, uint32(math.MaxUint32), binary.LittleEndian.Uint32(header[requestOffsetSize:]))
	assert.Equal(t, uint64(math.MaxUint64), binary.LittleEndian.Uint64(header[requestOffsetRequest:]))
	assert.Equal(t, uint64(math.MaxUint64), binary.LittleEndian.Uint64(header[requestOffsetSession:]))
	assert.Equal(t, uint64(math.MaxUint64), binary.LittleEndian.Uint64(header[requestOffsetNamespace:]))
}

func TestEncodeRequestHeader_OmitsTheReservedCodeForReplicatedOperations(t *testing.T) {
	var header [HeaderSize]byte
	EncodeRequestHeader(&header, RequestFields{
		Size:      HeaderSize,
		Client:    ClientID{Lo: 1},
		Operation: OperationCreateStream,
		Request:   1,
		Session:   9,
	})

	assert.Zero(t, binary.LittleEndian.Uint32(header[requestOffsetReserved:]))
}

func TestEncodeRequestHeader_ProducesTheGoldenFrame(t *testing.T) {
	var header [HeaderSize]byte
	EncodeRequestHeader(&header, RequestFields{
		Size:      HeaderSize + 4,
		Client:    ClientID{Lo: 0x0807060504030201, Hi: 0x100F0E0D0C0B0A09},
		Request:   2,
		Operation: OperationCreateStream,
		Namespace: 0,
		Session:   11,
	})

	golden := map[int][]byte{
		48:  {0x04, 0x01, 0x00, 0x00},
		60:  {0x05},
		128: {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		136: {0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10},
		168: {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		176: {0x80},
		192: {0x0B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
	}
	written := 0
	for offset, want := range golden {
		assert.Equal(t, want, header[offset:offset+len(want)], "at offset %d", offset)
		written += len(want)
	}

	nonZero := 0
	for _, b := range header {
		if b != 0 {
			nonZero++
		}
	}
	assert.Equal(t, 22, nonZero, "only the golden fields carry nonzero bytes")
}

func TestPeekCommand_ReadsTheFrameDiscriminant(t *testing.T) {
	tests := []struct {
		name  string
		value byte
		want  FrameCommand
	}{
		{name: "request", value: 5, want: FrameRequest},
		{name: "reply", value: 8, want: FrameReply},
		{name: "eviction", value: 13, want: FrameEviction},
		{name: "unknown", value: 99, want: FrameCommand(99)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var header [HeaderSize]byte
			header[replyOffsetCommand] = test.value
			assert.Equal(t, test.want, PeekCommand(&header))
		})
	}
}

func TestReplyReaders_ReadTheirCanonicalOffsets(t *testing.T) {
	var header [HeaderSize]byte
	binary.LittleEndian.PutUint32(header[replyOffsetSize:], HeaderSize+9)
	binary.LittleEndian.PutUint32(header[replyOffsetStatus:], 42)
	header[replyOffsetOperation] = byte(OperationCreateTopic)

	assert.Equal(t, uint32(HeaderSize+9), ReadSize(&header))
	assert.Equal(t, uint32(42), ReadStatus(&header))
	assert.Equal(t, OperationCreateTopic, ReadReplyOperation(&header))
}

func TestReadEviction_ReadsTheReasonAndProtocolWindow(t *testing.T) {
	var header [HeaderSize]byte
	header[evictionOffsetCommand] = byte(FrameEviction)
	binary.LittleEndian.PutUint32(header[evictionOffsetServerProtocolVersion:], 12288)
	binary.LittleEndian.PutUint32(header[evictionOffsetServerProtocolVersionMin:], 11264)
	header[evictionOffsetReason] = byte(EvictionIncompatibleProtocol)

	eviction := ReadEviction(&header)
	assert.Equal(t, EvictionIncompatibleProtocol, eviction.Reason)
	assert.Equal(t, uint32(12288), eviction.ServerProtocolVersion)
	assert.Equal(t, uint32(11264), eviction.ServerProtocolVersionMin)
}

func TestClientID_IsZero(t *testing.T) {
	assert.True(t, ClientID{}.IsZero())
	assert.False(t, ClientID{Lo: 1}.IsZero())
	assert.False(t, ClientID{Hi: 1}.IsZero())
}

func TestHeaderOffsets_MatchTheWireContract(t *testing.T) {
	require.Equal(t, 256, HeaderSize)

	assert.Equal(t, 48, requestOffsetSize)
	assert.Equal(t, 60, requestOffsetCommand)
	assert.Equal(t, 128, requestOffsetClient)
	assert.Equal(t, 160, requestOffsetTimestamp)
	assert.Equal(t, 168, requestOffsetRequest)
	assert.Equal(t, 176, requestOffsetOperation)
	assert.Equal(t, 184, requestOffsetNamespace)
	assert.Equal(t, 192, requestOffsetSession)
	assert.Equal(t, 204, requestOffsetReserved)

	assert.Equal(t, 48, replyOffsetSize)
	assert.Equal(t, 60, replyOffsetCommand)
	assert.Equal(t, 208, replyOffsetOperation)
	assert.Equal(t, 216, replyOffsetNamespace)
	assert.Equal(t, 224, replyOffsetStatus)
	assert.Equal(t, replyOffsetNamespace+8, replyOffsetStatus, "status follows namespace")

	assert.Equal(t, 60, evictionOffsetCommand)
	assert.Equal(t, 128, evictionOffsetClient)
	assert.Equal(t, 144, evictionOffsetServerProtocolVersion)
	assert.Equal(t, 148, evictionOffsetServerProtocolVersionMin)
	assert.Equal(t, 255, evictionOffsetReason)
}
