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

package binaryserialization

import (
	"encoding/binary"

	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	"github.com/klauspost/compress/s2"
)

type TcpSendMessagesRequest struct {
	iggcon.SendMessagesRequest
}

func (request *TcpSendMessagesRequest) Serialize(compression iggcon.IggyMessageCompression) []byte {
	for i, message := range request.Messages {
		switch compression {
		case iggcon.MESSAGE_COMPRESSION_S2:
			if len(message.Payload) < 32 {
				break
			}
			request.Messages[i].Payload = s2.Encode(nil, message.Payload)
		case iggcon.MESSAGE_COMPRESSION_S2_BETTER:
			if len(message.Payload) < 32 {
				break
			}
			request.Messages[i].Payload = s2.EncodeBetter(nil, message.Payload)
		case iggcon.MESSAGE_COMPRESSION_S2_BEST:
			if len(message.Payload) < 32 {
				break
			}
			request.Messages[i].Payload = s2.EncodeBest(nil, message.Payload)
		}
	}

	streamTopicIdLength := 2 + request.StreamId.Length + 2 + request.TopicId.Length
	messageBytesCount := calculateMessageBytesCount(request.Messages)
	totalSize := streamTopicIdLength + messageBytesCount + request.Partitioning.Length + 2
	bytes := make([]byte, totalSize)
	position := 0
	//ids
	copy(bytes[position:2+request.StreamId.Length], SerializeIdentifier(request.StreamId))
	copy(bytes[position+2+request.StreamId.Length:streamTopicIdLength], SerializeIdentifier(request.TopicId))
	position = streamTopicIdLength

	//partitioning
	bytes[streamTopicIdLength] = byte(request.Partitioning.Kind)
	bytes[streamTopicIdLength+1] = byte(request.Partitioning.Length)
	copy(bytes[streamTopicIdLength+2:streamTopicIdLength+2+request.Partitioning.Length], []byte(request.Partitioning.Value))
	position = streamTopicIdLength + 2 + request.Partitioning.Length

	emptyHeaders := make([]byte, 4)

	for _, message := range request.Messages {
		copy(bytes[position:position+16], message.Id[:])
		if message.Headers != nil {
			headersBytes := getHeadersBytes(message.Headers)
			binary.LittleEndian.PutUint32(bytes[position+16:position+20], uint32(len(headersBytes)))
			copy(bytes[position+20:position+20+len(headersBytes)], headersBytes)
			position += len(headersBytes) + 20
		} else {
			copy(bytes[position+16:position+16+len(emptyHeaders)], emptyHeaders)
			position += 20
		}

		binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(len(message.Payload)))
		copy(bytes[position+4:position+4+len(message.Payload)], message.Payload)
		position += len(message.Payload) + 4
	}

	return bytes
}

func calculateMessageBytesCount(messages []iggcon.Message) int {
	count := 0
	for _, msg := range messages {
		count += 16 + 4 + len(msg.Payload) + 4
		for key, header := range msg.Headers {
			count += 4 + len(key.Value) + 1 + 4 + len(header.Value)
		}
	}
	return count
}

func getHeadersBytes(headers map[iggcon.HeaderKey]iggcon.HeaderValue) []byte {
	headersLength := 0
	for key, header := range headers {
		headersLength += 4 + len(key.Value) + 1 + 4 + len(header.Value)
	}
	headersBytes := make([]byte, headersLength)
	position := 0
	for key, value := range headers {
		headerBytes := getBytesFromHeader(key, value)
		copy(headersBytes[position:position+len(headerBytes)], headerBytes)
		position += len(headerBytes)
	}
	return headersBytes
}

func getBytesFromHeader(key iggcon.HeaderKey, value iggcon.HeaderValue) []byte {
	headerBytesLength := 4 + len(key.Value) + 1 + 4 + len(value.Value)
	headerBytes := make([]byte, headerBytesLength)

	binary.LittleEndian.PutUint32(headerBytes[:4], uint32(len(key.Value)))
	copy(headerBytes[4:4+len(key.Value)], key.Value)

	headerBytes[4+len(key.Value)] = byte(value.Kind)

	binary.LittleEndian.PutUint32(headerBytes[4+len(key.Value)+1:4+len(key.Value)+1+4], uint32(len(value.Value)))
	copy(headerBytes[4+len(key.Value)+1+4:], value.Value)

	return headerBytes
}
