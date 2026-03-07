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

package iggcon

import (
	"encoding/binary"

	"github.com/klauspost/compress/s2"
)

type SendMessages struct {
	Compression IggyMessageCompression

	StreamId     Identifier    `json:"streamId"`
	TopicId      Identifier    `json:"topicId"`
	Partitioning Partitioning  `json:"partitioning"`
	Messages     []IggyMessage `json:"messages"`
}

const indexSize = 16

func (s *SendMessages) Code() CommandCode {
	return SendMessagesCode
}

func (s *SendMessages) MarshalBinary() ([]byte, error) {
	for i, message := range s.Messages {
		switch s.Compression {
		case MESSAGE_COMPRESSION_S2:
			if len(message.Payload) < 32 {
				break
			}
			s.Messages[i].Payload = s2.Encode(nil, message.Payload)
			message.Header.PayloadLength = uint32(len(message.Payload))
		case MESSAGE_COMPRESSION_S2_BETTER:
			if len(message.Payload) < 32 {
				break
			}
			s.Messages[i].Payload = s2.EncodeBetter(nil, message.Payload)
			message.Header.PayloadLength = uint32(len(message.Payload))
		case MESSAGE_COMPRESSION_S2_BEST:
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
		copy(bytes[position:position+MessageHeaderSize], message.Header.ToBytes())
		copy(bytes[position+MessageHeaderSize:position+MessageHeaderSize+int(message.Header.PayloadLength)], message.Payload)
		position += MessageHeaderSize + int(message.Header.PayloadLength)
		copy(bytes[position:position+int(message.Header.UserHeaderLength)], message.UserHeaders)
		position += int(message.Header.UserHeaderLength)

		msgSize += MessageHeaderSize + message.Header.PayloadLength + message.Header.UserHeaderLength

		binary.LittleEndian.PutUint32(bytes[currentIndexPosition:currentIndexPosition+4], 0)
		binary.LittleEndian.PutUint32(bytes[currentIndexPosition+4:currentIndexPosition+8], uint32(msgSize))
		binary.LittleEndian.PutUint32(bytes[currentIndexPosition+8:currentIndexPosition+12], 0)
		currentIndexPosition += indexSize
	}

	return bytes, nil
}

func calculateMessageBytesCount(messages []IggyMessage) int {
	count := 0
	for _, msg := range messages {
		count += MessageHeaderSize + len(msg.Payload) + len(msg.UserHeaders)
	}
	return count
}
