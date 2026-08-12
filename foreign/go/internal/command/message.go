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

	"github.com/apache/iggy/foreign/go/contracts"
	"github.com/klauspost/compress/s2"
)

const (
	partitionPresenceSize = 1
	partitionFieldSize    = 4
	partitionStrategySize = partitionPresenceSize + partitionFieldSize + 1
	offsetSize            = 12
	commitFlagSize        = 1
	indexSize             = 16
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

// zeroIndex is the blank per-message index entry reserved ahead of the
// message section and filled in as messages are appended.
var zeroIndex [indexSize]byte

func (s *SendMessages) MarshalBinary() ([]byte, error) {
	return s.AppendBinary(nil)
}

// AppendBinary encodes the batch straight into b: [metadata_length u32]
// [stream id][topic id][partitioning][messages_count u32], the per-message
// index section, then each message as header, payload, user headers.
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

	indexesStart := len(b)
	for range s.Messages {
		b = append(b, zeroIndex[:]...)
	}

	msgSize := uint32(0)
	for i := range s.Messages {
		message := &s.Messages[i]
		// The header lengths and the appended slices must agree, or every
		// message boundary after a mismatch mis-frames; deriving both from
		// the same slice makes the disagreement impossible.
		message.Header.PayloadLength = uint32(len(message.Payload))
		message.Header.UserHeaderLength = uint32(len(message.UserHeaders))
		if b, err = message.Header.AppendBinary(b); err != nil {
			return b, err
		}
		b = append(b, message.Payload...)
		b = append(b, message.UserHeaders...)

		msgSize += iggcon.MessageHeaderSize +
			message.Header.PayloadLength + message.Header.UserHeaderLength
		binary.LittleEndian.PutUint32(b[indexesStart+i*indexSize+4:], msgSize)
	}

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
