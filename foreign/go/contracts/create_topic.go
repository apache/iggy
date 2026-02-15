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
)

type CreateTopic struct {
	StreamId             Identifier           `json:"streamId"`
	PartitionsCount      uint32               `json:"partitionsCount"`
	CompressionAlgorithm CompressionAlgorithm `json:"compressionAlgorithm"`
	MessageExpiry        Duration             `json:"messageExpiry"`
	MaxTopicSize         uint64               `json:"maxTopicSize"`
	Name                 string               `json:"name"`
	ReplicationFactor    *uint8               `json:"replicationFactor"`
}

func (t *CreateTopic) Code() CommandCode {
	return CreateTopicCode
}

func (t *CreateTopic) MarshalBinary() ([]byte, error) {
	if t.ReplicationFactor == nil {
		t.ReplicationFactor = new(uint8)
	}

	streamIdBytes, err := t.StreamId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	nameBytes := []byte(t.Name)

	totalLength := len(streamIdBytes) + // StreamId
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

	// PartitionsCount
	binary.LittleEndian.PutUint32(bytes[position:], t.PartitionsCount)
	position += 4

	// CompressionAlgorithm
	bytes[position] = byte(t.CompressionAlgorithm)
	position++

	// MessageExpiry
	binary.LittleEndian.PutUint64(bytes[position:], uint64(t.MessageExpiry))
	position += 8

	// MaxTopicSize
	binary.LittleEndian.PutUint64(bytes[position:], t.MaxTopicSize)
	position += 8

	// ReplicationFactor
	bytes[position] = *t.ReplicationFactor
	position++

	// Name
	bytes[position] = byte(len(nameBytes))
	position++
	copy(bytes[position:], nameBytes)

	return bytes, nil
}
