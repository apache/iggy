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

const (
	partitionPresenceSize = 1
	partitionFieldSize    = 4
	partitionStrategySize = partitionPresenceSize + partitionFieldSize + 1
	offsetSize            = 12
	commitFlagSize        = 1
)

type PollMessages struct {
	StreamId    Identifier      `json:"streamId"`
	TopicId     Identifier      `json:"topicId"`
	Consumer    Consumer        `json:"consumer"`
	PartitionId *uint32         `json:"partitionId"`
	Strategy    PollingStrategy `json:"pollingStrategy"`
	Count       uint32          `json:"count"`
	AutoCommit  bool            `json:"autoCommit"`
}

func (m *PollMessages) Code() CommandCode {
	return PollMessagesCode
}

func (m *PollMessages) MarshalBinary() ([]byte, error) {
	consumerIdBytes, err := m.Consumer.Id.MarshalBinary()
	if err != nil {
		return nil, err
	}
	streamIdBytes, err := m.StreamId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	topicIdBytes, err := m.TopicId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	messageSize := 1 + len(consumerIdBytes) + len(streamIdBytes) + len(topicIdBytes) + partitionStrategySize + offsetSize + commitFlagSize
	bytes := make([]byte, messageSize)

	bytes[0] = byte(m.Consumer.Kind)
	position := 1
	copy(bytes[position:position+len(consumerIdBytes)], consumerIdBytes)
	position += len(consumerIdBytes)

	copy(bytes[position:position+len(streamIdBytes)], streamIdBytes)
	position += len(streamIdBytes)
	copy(bytes[position:position+len(topicIdBytes)], topicIdBytes)
	position += len(topicIdBytes)
	if m.PartitionId != nil {
		bytes[position] = 1
		binary.LittleEndian.PutUint32(bytes[position+1:position+1+4], *m.PartitionId)
	} else {
		bytes[position] = 0
		binary.LittleEndian.PutUint32(bytes[position+1:position+1+4], 0)
	}
	bytes[position+1+4] = byte(m.Strategy.Kind)

	position += partitionStrategySize
	binary.LittleEndian.PutUint64(bytes[position:position+8], m.Strategy.Value)
	binary.LittleEndian.PutUint32(bytes[position+8:position+12], m.Count)

	position += offsetSize

	if m.AutoCommit {
		bytes[position] = 1
	} else {
		bytes[position] = 0
	}

	return bytes, nil
}
