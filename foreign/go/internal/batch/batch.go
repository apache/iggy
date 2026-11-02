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

// Package batch implements the canonical message batch record shared by
// produce request bodies and poll reply bodies: a 256-byte batch header
// followed by message frames of [48-byte header][payload][user headers].
package batch

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/zeebo/xxh3"
)

// HeaderSize is the size of the batch header. Bytes past the declared
// fields are reserved and stay zero.
const HeaderSize = 256

// MessageHeaderSize is the size of a message frame header.
const MessageHeaderSize = 48

// Header is the batch header. A producer leaves PartitionId, BaseOffset,
// and BaseTimestamp zero; the server stamps them.
type Header struct {
	PartitionId     uint64
	BaseOffset      uint64
	BaseTimestamp   uint64
	OriginTimestamp uint64
	BatchLength     uint64
	BatchChecksum   uint64
	MessageCount    uint32
}

// DecodeHeader reads a batch header from the front of data.
func DecodeHeader(data []byte) (Header, error) {
	if len(data) < HeaderSize {
		return Header{}, fmt.Errorf("batch header needs %d bytes, got %d", HeaderSize, len(data))
	}
	header := Header{
		PartitionId:     binary.LittleEndian.Uint64(data[0:8]),
		BaseOffset:      binary.LittleEndian.Uint64(data[8:16]),
		BaseTimestamp:   binary.LittleEndian.Uint64(data[16:24]),
		OriginTimestamp: binary.LittleEndian.Uint64(data[24:32]),
		BatchLength:     binary.LittleEndian.Uint64(data[32:40]),
		BatchChecksum:   binary.LittleEndian.Uint64(data[40:48]),
		MessageCount:    binary.LittleEndian.Uint32(data[48:52]),
	}
	if header.BatchLength < HeaderSize {
		return Header{}, fmt.Errorf(
			"batch length %d does not cover the %d-byte batch header", header.BatchLength, HeaderSize)
	}
	return header, nil
}

// EncodeInto writes the header into the first HeaderSize bytes of b and
// zeroes the reserved tail.
func (h Header) EncodeInto(b []byte) {
	clear(b[:HeaderSize])
	binary.LittleEndian.PutUint64(b[0:8], h.PartitionId)
	binary.LittleEndian.PutUint64(b[8:16], h.BaseOffset)
	binary.LittleEndian.PutUint64(b[16:24], h.BaseTimestamp)
	binary.LittleEndian.PutUint64(b[24:32], h.OriginTimestamp)
	binary.LittleEndian.PutUint64(b[32:40], h.BatchLength)
	binary.LittleEndian.PutUint64(b[40:48], h.BatchChecksum)
	binary.LittleEndian.PutUint32(b[48:52], h.MessageCount)
}

// Checksum computes the batch checksum: XXH3-64 over the six header meta
// fields followed by every frame's stored 8-byte checksum field in message
// order. Message bodies are bound transitively through the frame checksums.
func (h Header) Checksum(frameChecksums []byte) uint64 {
	input := make([]byte, 0, 44+len(frameChecksums))
	input = binary.LittleEndian.AppendUint64(input, h.PartitionId)
	input = binary.LittleEndian.AppendUint64(input, h.BaseOffset)
	input = binary.LittleEndian.AppendUint64(input, h.BaseTimestamp)
	input = binary.LittleEndian.AppendUint64(input, h.OriginTimestamp)
	input = binary.LittleEndian.AppendUint64(input, h.BatchLength)
	input = binary.LittleEndian.AppendUint32(input, h.MessageCount)
	input = append(input, frameChecksums...)
	return xxh3.Hash(input)
}

// MessageHeader is a message frame header. OffsetDelta and TimestampDelta
// resolve against the batch header's stamped bases; Checksum is XXH3-64
// over frame[8:48] followed by the payload and the user headers.
type MessageHeader struct {
	Checksum          uint64
	Id                [16]byte
	OffsetDelta       uint32
	TimestampDelta    uint32
	UserHeadersLength uint32
	PayloadLength     uint32
}

// DecodeMessageHeader reads a frame header from the front of data,
// rejecting nonzero reserved bytes.
func DecodeMessageHeader(data []byte) (MessageHeader, error) {
	if len(data) < MessageHeaderSize {
		return MessageHeader{}, fmt.Errorf(
			"message frame header needs %d bytes, got %d", MessageHeaderSize, len(data))
	}
	if binary.LittleEndian.Uint64(data[40:48]) != 0 {
		return MessageHeader{}, errors.New("message frame reserved bytes must be zero")
	}
	return MessageHeader{
		Checksum:          binary.LittleEndian.Uint64(data[0:8]),
		Id:                [16]byte(data[8:24]),
		OffsetDelta:       binary.LittleEndian.Uint32(data[24:28]),
		TimestampDelta:    binary.LittleEndian.Uint32(data[28:32]),
		UserHeadersLength: binary.LittleEndian.Uint32(data[32:36]),
		PayloadLength:     binary.LittleEndian.Uint32(data[36:40]),
	}, nil
}
