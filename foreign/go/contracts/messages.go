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
	PartitionId   uint32
	CurrentOffset uint64
	MessageCount  uint32
	Messages      []IggyMessage
}

type SendMessagesRequest struct {
	StreamId     Identifier    `json:"streamId"`
	TopicId      Identifier    `json:"topicId"`
	Partitioning Partitioning  `json:"partitioning"`
	Messages     []IggyMessage `json:"messages"`
}

type ReceivedMessage struct {
	Message       IggyMessage
	CurrentOffset uint64
	PartitionId   uint32
}

type IggyMessage struct {
	Header      MessageHeader
	Payload     []byte
	UserHeaders []byte
}

func NewIggyMessage(id uuid.UUID, payload []byte) IggyMessage {
	return IggyMessage{
		Header:  NewMessageHeader(id, uint32(len(payload)), 0),
		Payload: payload,
	}
}

func NewIggyMessageWithHeaders(id uuid.UUID, payload []byte, userHeaders map[HeaderKey]HeaderValue) IggyMessage {
	userHeaderBytes := GetHeadersBytes(userHeaders)
	messageHeader := NewMessageHeader(id, uint32(len(payload)), 0)
	messageHeader.UserHeaderLength = uint32(len(userHeaderBytes))
	iggyMessage := IggyMessage{
		Header:      messageHeader,
		Payload:     payload,
		UserHeaders: userHeaderBytes,
	}
	return iggyMessage
}
