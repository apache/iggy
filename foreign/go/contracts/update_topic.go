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

type UpdateTopic struct {
	StreamId             Identifier           `json:"streamId"`
	TopicId              Identifier           `json:"topicId"`
	CompressionAlgorithm CompressionAlgorithm `json:"compressionAlgorithm"`
	MessageExpiry        Duration             `json:"messageExpiry"`
	MaxTopicSize         uint64               `json:"maxTopicSize"`
	ReplicationFactor    *uint8               `json:"replicationFactor"`
	Name                 string               `json:"name"`
}

func (u *UpdateTopic) Code() CommandCode {
	return UpdateTopicCode
}

func (u *UpdateTopic) MarshalBinary() ([]byte, error) {
	if u.ReplicationFactor == nil {
		u.ReplicationFactor = new(uint8)
	}
	streamIdBytes, err := u.StreamId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	topicIdBytes, err := u.TopicId.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, 19+len(streamIdBytes)+len(topicIdBytes)+len(u.Name))

	offset := 0

	offset += copy(buffer[offset:], streamIdBytes)
	offset += copy(buffer[offset:], topicIdBytes)

	buffer[offset] = byte(u.CompressionAlgorithm)
	offset++

	binary.LittleEndian.PutUint64(buffer[offset:], uint64(u.MessageExpiry))
	offset += 8

	binary.LittleEndian.PutUint64(buffer[offset:], u.MaxTopicSize)
	offset += 8

	buffer[offset] = *u.ReplicationFactor
	offset++

	buffer[offset] = uint8(len(u.Name))
	offset++

	copy(buffer[offset:], u.Name)

	return buffer, nil
}
