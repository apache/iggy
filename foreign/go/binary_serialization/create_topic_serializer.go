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
)

type TcpCreateTopicRequest struct {
	iggcon.CreateTopicRequest
}

func (request *TcpCreateTopicRequest) Serialize() []byte {
	streamIdBytes := SerializeIdentifier(request.StreamId)
	nameBytes := []byte(request.Name)

	totalLength := len(streamIdBytes) + // StreamId
		4 + // TopicId
		4 + // PartitionsCount
		1 + // CompressionAlgorithm
		8 + // MessageExpiry
		8 + // MaxTopicSize
		1 + // ReplicationFactor
		1 + // Name length
		len(nameBytes) // Name
	bytes := make([]byte, totalLength)

	position := 0

	// StreamId
	copy(bytes[position:], streamIdBytes)
	position += len(streamIdBytes)

	// TopicId
	binary.LittleEndian.PutUint32(bytes[position:], uint32(request.TopicId))
	position += 4

	// PartitionsCount
	binary.LittleEndian.PutUint32(bytes[position:], uint32(request.PartitionsCount))
	position += 4

	// CompressionAlgorithm
	bytes[position] = request.CompressionAlgorithm
	position++

	// MessageExpiry
	binary.LittleEndian.PutUint64(bytes[position:], uint64(request.MessageExpiry.Microseconds()))
	position += 8

	// MaxTopicSize
	binary.LittleEndian.PutUint64(bytes[position:], request.MaxTopicSize)
	position += 8

	// ReplicationFactor
	bytes[position] = request.ReplicationFactor
	position++

	// Name
	bytes[position] = byte(len(nameBytes))
	position++
	copy(bytes[position:], nameBytes)

	return bytes
}
