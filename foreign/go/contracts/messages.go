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
	"github.com/google/uuid"
)

type FetchMessagesRequest struct {
	StreamId        Identifier      `json:"streamId"`
	TopicId         Identifier      `json:"topicId"`
	Consumer        Consumer        `json:"consumer"`
	PartitionId     int             `json:"partitionId"`
	PollingStrategy PollingStrategy `json:"pollingStrategy"`
	Count           int             `json:"count"`
	AutoCommit      bool            `json:"autoCommit"`
}

type FetchMessagesResponse struct {
	PartitionId   int
	CurrentOffset uint64
	Messages      []MessageResponse
	MessageCount  int
}

type MessageResponse struct {
	Offset    uint64                    `json:"offset"`
	Timestamp uint64                    `json:"timestamp"`
	Checksum  uint32                    `json:"checksum"`
	Id        uuid.UUID                 `json:"id"`
	Payload   []byte                    `json:"payload"`
	Headers   map[HeaderKey]HeaderValue `json:"headers,omitempty"`
	State     MessageState              `json:"state"`
}

type MessageState int

const (
	MessageStateAvailable MessageState = iota
	MessageStateUnavailable
	MessageStatePoisoned
	MessageStateMarkedForDeletion
)

type SendMessagesRequest struct {
	StreamId     Identifier   `json:"streamId"`
	TopicId      Identifier   `json:"topicId"`
	Partitioning Partitioning `json:"partitioning"`
	Messages     []Message    `json:"messages"`
}

type Message struct {
	Id      uuid.UUID
	Payload []byte
	Headers map[HeaderKey]HeaderValue
}

func NewMessage(payload []byte, headers map[HeaderKey]HeaderValue) Message {
	return Message{
		Id:      uuid.New(),
		Payload: payload,
		Headers: headers,
	}
}
