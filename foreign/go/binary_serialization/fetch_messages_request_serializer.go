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

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

const (
	partitionStrategySize = 5
	offsetSize            = 12
	commitFlagSize        = 1
)

type TcpFetchMessagesRequest struct {
	StreamId    iggcon.Identifier      `json:"streamId"`
	TopicId     iggcon.Identifier      `json:"topicId"`
	Consumer    iggcon.Consumer        `json:"consumer"`
	PartitionId *uint32                `json:"partitionId"`
	Strategy    iggcon.PollingStrategy `json:"pollingStrategy"`
	Count       uint32                 `json:"count"`
	AutoCommit  bool                   `json:"autoCommit"`
}

func (request *TcpFetchMessagesRequest) Serialize() []byte {
	if request.PartitionId == nil {
		request.PartitionId = new(uint32)
	}
	streamTopicIdLength := 2 + request.StreamId.Length + 2 + request.TopicId.Length
	messageSize := 2 + request.Consumer.Id.Length + streamTopicIdLength + partitionStrategySize + offsetSize + commitFlagSize + 1
	bytes := make([]byte, messageSize)

	bytes[0] = byte(request.Consumer.Kind)
	copy(bytes[1:3+request.Consumer.Id.Length], SerializeIdentifier(request.Consumer.Id))

	position := 3 + request.Consumer.Id.Length

	copy(bytes[position:position+streamTopicIdLength], SerializeIdentifiers(request.StreamId, request.TopicId))

	position += streamTopicIdLength
	binary.LittleEndian.PutUint32(bytes[position:position+4], *request.PartitionId)
	bytes[position+4] = byte(request.Strategy.Kind)

	position += partitionStrategySize
	binary.LittleEndian.PutUint64(bytes[position:position+8], request.Strategy.Value)
	binary.LittleEndian.PutUint32(bytes[position+8:position+12], request.Count)

	position += offsetSize

	if request.AutoCommit {
		bytes[position] = 1
	} else {
		bytes[position] = 0
	}

	return bytes
}
