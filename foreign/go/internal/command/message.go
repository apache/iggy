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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/batch"
	"github.com/google/uuid"
	"github.com/klauspost/compress/s2"
	"github.com/zeebo/xxh3"
)

type SendMessages struct {
	Compression iggcon.IggyMessageCompression

	StreamId     iggcon.Identifier    `json:"streamId"`
	TopicId      iggcon.Identifier    `json:"topicId"`
	Partitioning iggcon.Partitioning  `json:"partitioning"`
	Messages     []iggcon.IggyMessage `json:"messages"`
}

func (s *SendMessages) Code() Code {
	return SendMessagesCode
}

// zeroBatchHeader is the blank batch header reserved ahead of the message
// frames and backpatched once every frame checksum is known.
var zeroBatchHeader [batch.HeaderSize]byte

func (s *SendMessages) MarshalBinary() ([]byte, error) {
	return s.AppendBinary(nil)
}

// AppendBinary encodes the batch straight into b: [metadata_length u32]
// [stream id][topic id][partitioning][messages_count u32], then one canonical
// batch record: a 256-byte batch header followed by one frame per message.
func (s *SendMessages) AppendBinary(b []byte) ([]byte, error) {
	s.compressPayloads()

	metadataStart := len(b)
	b = binary.LittleEndian.AppendUint32(b, 0)
	var err error
	if b, err = s.StreamId.AppendBinary(b); err != nil {
		return b, err
	}
	if b, err = s.TopicId.AppendBinary(b); err != nil {
		return b, err
	}
	if b, err = s.Partitioning.AppendBinary(b); err != nil {
		return b, err
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s.Messages)))
	metadataLength := len(b) - metadataStart - 4
	binary.LittleEndian.PutUint32(b[metadataStart:], uint32(metadataLength))

	var originTimestamp uint64
	for i := range s.Messages {
		if i == 0 || s.Messages[i].Header.OriginTimestamp < originTimestamp {
			originTimestamp = s.Messages[i].Header.OriginTimestamp
		}
	}

	headerStart := len(b)
	b = append(b, zeroBatchHeader[:]...)

	blobStart := len(b)
	frameChecksums := make([]byte, 0, len(s.Messages)*8)
	for i := range s.Messages {
		message := &s.Messages[i]
		// The id sits under the frame checksum, so it must exist before the
		// frame is hashed; the server never mints ids.
		if message.Header.Id == (iggcon.MessageID{}) {
			id, err := uuid.NewRandom()
			if err != nil {
				return b, err
			}
			message.Header.Id = iggcon.MessageID(id)
		}
		// The header lengths and the appended slices must agree, or every
		// message boundary after a mismatch mis-frames; deriving both from
		// the same slice makes the disagreement impossible.
		message.Header.PayloadLength = uint32(len(message.Payload))
		message.Header.UserHeaderLength = uint32(len(message.UserHeaders))
		timestampDelta := message.Header.OriginTimestamp - originTimestamp
		if timestampDelta > math.MaxUint32 {
			return b, fmt.Errorf(
				"message origin timestamp %d runs more than %d microseconds past the batch's earliest %d",
				message.Header.OriginTimestamp, uint64(math.MaxUint32), originTimestamp)
		}

		frameStart := len(b)
		b = binary.LittleEndian.AppendUint64(b, 0)
		b = append(b, message.Header.Id[:]...)
		b = binary.LittleEndian.AppendUint32(b, uint32(i))
		b = binary.LittleEndian.AppendUint32(b, uint32(timestampDelta))
		b = binary.LittleEndian.AppendUint32(b, message.Header.UserHeaderLength)
		b = binary.LittleEndian.AppendUint32(b, message.Header.PayloadLength)
		b = binary.LittleEndian.AppendUint64(b, 0)
		b = append(b, message.Payload...)
		b = append(b, message.UserHeaders...)

		checksum := xxh3.Hash(b[frameStart+8:])
		binary.LittleEndian.PutUint64(b[frameStart:], checksum)
		message.Header.Checksum = checksum
		frameChecksums = binary.LittleEndian.AppendUint64(frameChecksums, checksum)
	}

	batchHeader := batch.Header{
		OriginTimestamp: originTimestamp,
		BatchLength:     uint64(batch.HeaderSize + len(b) - blobStart),
		MessageCount:    uint32(len(s.Messages)),
	}
	batchHeader.BatchChecksum = batchHeader.Checksum(frameChecksums)
	batchHeader.EncodeInto(b[headerStart:blobStart])
	return b, nil
}

// compressPayloads compresses each payload in place. The header length is
// updated through the slice index: writing it to a range copy would leave the
// wire header claiming the uncompressed length, and the encoder would then
// mis-frame every message that follows.
func (s *SendMessages) compressPayloads() {
	switch s.Compression {
	case iggcon.MESSAGE_COMPRESSION_S2,
		iggcon.MESSAGE_COMPRESSION_S2_BETTER,
		iggcon.MESSAGE_COMPRESSION_S2_BEST:
	default:
		return
	}

	for i := range s.Messages {
		payload := s.Messages[i].Payload
		if len(payload) < 32 {
			continue
		}
		switch s.Compression {
		case iggcon.MESSAGE_COMPRESSION_S2:
			s.Messages[i].Payload = s2.Encode(nil, payload)
		case iggcon.MESSAGE_COMPRESSION_S2_BETTER:
			s.Messages[i].Payload = s2.EncodeBetter(nil, payload)
		case iggcon.MESSAGE_COMPRESSION_S2_BEST:
			s.Messages[i].Payload = s2.EncodeBest(nil, payload)
		}
		s.Messages[i].Header.PayloadLength = uint32(len(s.Messages[i].Payload))
	}
}

type PollMessages struct {
	StreamId    iggcon.Identifier      `json:"streamId"`
	TopicId     iggcon.Identifier      `json:"topicId"`
	Consumer    iggcon.Consumer        `json:"consumer"`
	PartitionId *uint32                `json:"partitionId"`
	Strategy    iggcon.PollingStrategy `json:"pollingStrategy"`
	Count       uint32                 `json:"count"`
	AutoCommit  bool                   `json:"autoCommit"`
}

func (m *PollMessages) Code() Code {
	return PollMessagesCode
}

func (m *PollMessages) AppendBinary(b []byte) ([]byte, error) {
	b = append(b, byte(m.Consumer.Kind))
	var err error
	if b, err = m.Consumer.Id.AppendBinary(b); err != nil {
		return nil, err
	}
	if b, err = m.StreamId.AppendBinary(b); err != nil {
		return nil, err
	}
	if b, err = m.TopicId.AppendBinary(b); err != nil {
		return nil, err
	}
	if m.PartitionId != nil {
		b = append(b, 1)
		b = binary.LittleEndian.AppendUint32(b, *m.PartitionId)
	} else {
		b = append(b, 0, 0, 0, 0, 0)
	}
	b = append(b, byte(m.Strategy.Kind))
	b = binary.LittleEndian.AppendUint64(b, m.Strategy.Value)
	b = binary.LittleEndian.AppendUint32(b, m.Count)
	if m.AutoCommit {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return b, nil
}

func (m *PollMessages) MarshalBinary() ([]byte, error) {
	return m.AppendBinary(nil)
}
