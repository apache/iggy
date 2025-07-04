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

package tcp

import (
	"time"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	. "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

func (tms *IggyTcpClient) GetTopics(streamId Identifier) ([]Topic, error) {
	message := binaryserialization.SerializeIdentifier(streamId)
	buffer, err := tms.sendAndFetchResponse(message, GetTopicsCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeTopics(buffer)
}

func (tms *IggyTcpClient) GetTopic(streamId Identifier, topicId Identifier) (*TopicDetails, error) {
	message := binaryserialization.SerializeIdentifiers(streamId, topicId)
	buffer, err := tms.sendAndFetchResponse(message, GetTopicCode)
	if err != nil {
		return nil, err
	}
	if len(buffer) == 0 {
		return nil, ierror.TopicIdNotFound
	}

	topic, err := binaryserialization.DeserializeTopic(buffer)
	if err != nil {
		return nil, err
	}

	return topic, nil
}

func (tms *IggyTcpClient) CreateTopic(
	streamId Identifier,
	name string,
	partitionsCount int,
	compressionAlgorithm uint8,
	messageExpiry time.Duration,
	maxTopicSize uint64,
	replicationFactor *uint8,
	topicId *int,
) (*TopicDetails, error) {
	if MaxStringLength < len(name) {
		return nil, ierror.TextTooLong("topic_name")
	}
	serializedRequest := binaryserialization.TcpCreateTopicRequest{
		StreamId:             streamId,
		Name:                 name,
		PartitionsCount:      partitionsCount,
		CompressionAlgorithm: compressionAlgorithm,
		MessageExpiry:        messageExpiry,
		MaxTopicSize:         maxTopicSize,
		ReplicationFactor:    replicationFactor,
		TopicId:              topicId,
	}
	buffer, err := tms.sendAndFetchResponse(serializedRequest.Serialize(), CreateTopicCode)
	if err != nil {
		return nil, err
	}
	topic, err := binaryserialization.DeserializeTopic(buffer)
	return topic, err
}

func (tms *IggyTcpClient) UpdateTopic(
	streamId Identifier,
	topicId Identifier,
	name string,
	compressionAlgorithm uint8,
	messageExpiry time.Duration,
	maxTopicSize uint64,
	replicationFactor *uint8,
) error {
	if MaxStringLength < len(name) {
		return ierror.TextTooLong("topic_name")
	}
	serializedRequest := binaryserialization.TcpUpdateTopicRequest{
		StreamId:             streamId,
		TopicId:              topicId,
		CompressionAlgorithm: compressionAlgorithm,
		MessageExpiry:        messageExpiry,
		MaxTopicSize:         maxTopicSize,
		ReplicationFactor:    replicationFactor,
		Name:                 name}
	_, err := tms.sendAndFetchResponse(serializedRequest.Serialize(), UpdateTopicCode)
	return err
}

func (tms *IggyTcpClient) DeleteTopic(streamId, topicId Identifier) error {
	message := binaryserialization.SerializeIdentifiers(streamId, topicId)
	_, err := tms.sendAndFetchResponse(message, DeleteTopicCode)
	return err
}
