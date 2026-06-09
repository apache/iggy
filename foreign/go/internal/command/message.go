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

func (s *SendMessages) MarshalBinary() ([]byte, error) {
	for i, message := range s.Messages {
		switch s.Compression {
		case iggcon.MESSAGE_COMPRESSION_S2:
			if len(message.Payload) < 32 {
				break
			}
			s.Messages[i].Payload = s2.Encode(nil, message.Payload)
			message.Header.PayloadLength = uint32(len(message.Payload))
		case iggcon.MESSAGE_COMPRESSION_S2_BETTER:
			if len(message.Payload) < 32 {
				break
			}
			s.Messages[i].Payload = s2.EncodeBetter(nil, message.Payload)
			message.Header.PayloadLength = uint32(len(message.Payload))
		case iggcon.MESSAGE_COMPRESSION_S2_BEST:
			if len(message.Payload) < 32 {
				break
			}
			s.Messages[i].Payload = s2.EncodeBest(nil, message.Payload)
			message.Header.PayloadLength = uint32(len(message.Payload))
		}
	}

	streamIdBytes, err := s.StreamId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	topicIdBytes, err := s.TopicId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	partitioningBytes, err := s.Partitioning.MarshalBinary()
	if err != nil {
		return nil, err
	}
	metadataLenFieldSize := 4 // uint32
	messageCount := len(s.Messages)
	messagesCountFieldSize := 4 // uint32
	metadataLen := len(streamIdBytes) +
		len(topicIdBytes) +
		len(partitioningBytes) +
		messagesCountFieldSize
	indexesSize := messageCount * indexSize
	messageBytesCount := calculateMessageBytesCount(s.Messages)
	totalSize := metadataLenFieldSize +
		len(streamIdBytes) +
		len(topicIdBytes) +
		len(partitioningBytes) +
		messagesCountFieldSize +
		indexesSize +
		messageBytesCount

	bytes := make([]byte, totalSize)

	position := 0

	//metadata
	binary.LittleEndian.PutUint32(bytes[:4], uint32(metadataLen))
	position = 4
	//ids
	copy(bytes[position:position+len(streamIdBytes)], streamIdBytes)
	position += len(streamIdBytes)
	copy(bytes[position:position+len(topicIdBytes)], topicIdBytes)
	position += len(topicIdBytes)

	//partitioning
	copy(bytes[position:position+len(partitioningBytes)], partitioningBytes)
	position += len(partitioningBytes)
	binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(messageCount))
	position += 4

	currentIndexPosition := position
	for i := 0; i < indexesSize; i++ {
		bytes[position+i] = 0
	}
	position += indexesSize

	msgSize := uint32(0)
	for _, message := range s.Messages {
		copy(bytes[position:position+iggcon.MessageHeaderSize], message.Header.ToBytes())
		copy(bytes[position+iggcon.MessageHeaderSize:position+iggcon.MessageHeaderSize+int(message.Header.PayloadLength)], message.Payload)
		position += iggcon.MessageHeaderSize + int(message.Header.PayloadLength)
		copy(bytes[position:position+int(message.Header.UserHeaderLength)], message.UserHeaders)
		position += int(message.Header.UserHeaderLength)

		msgSize += iggcon.MessageHeaderSize + message.Header.PayloadLength + message.Header.UserHeaderLength

		binary.LittleEndian.PutUint32(bytes[currentIndexPosition:currentIndexPosition+4], 0)
		binary.LittleEndian.PutUint32(bytes[currentIndexPosition+4:currentIndexPosition+8], uint32(msgSize))
		binary.LittleEndian.PutUint32(bytes[currentIndexPosition+8:currentIndexPosition+12], 0)
		currentIndexPosition += indexSize
	}

	return bytes, nil
}

func calculateMessageBytesCount(messages []iggcon.IggyMessage) int {
	count := 0
	for _, msg := range messages {
		count += iggcon.MessageHeaderSize + len(msg.Payload) + len(msg.UserHeaders)
	}
	return count
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
