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
	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	. "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

func (tms *IggyTcpClient) GetTopics(streamId Identifier) ([]TopicResponse, error) {
	message := binaryserialization.SerializeIdentifier(streamId)
	buffer, err := tms.sendAndFetchResponse(message, GetTopicsCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeTopics(buffer)
}

func (tms *IggyTcpClient) GetTopicById(streamId Identifier, topicId Identifier) (*TopicResponse, error) {
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

func (tms *IggyTcpClient) CreateTopic(request CreateTopicRequest) error {
	if MaxStringLength < len(request.Name) {
		return ierror.TextTooLong("topic_name")
	}
	serializedRequest := binaryserialization.TcpCreateTopicRequest{CreateTopicRequest: request}
	_, err := tms.sendAndFetchResponse(serializedRequest.Serialize(), CreateTopicCode)
	return err
}

func (tms *IggyTcpClient) UpdateTopic(request UpdateTopicRequest) error {
	if MaxStringLength < len(request.Name) {
		return ierror.TextTooLong("topic_name")
	}
	serializedRequest := binaryserialization.TcpUpdateTopicRequest{UpdateTopicRequest: request}
	_, err := tms.sendAndFetchResponse(serializedRequest.Serialize(), UpdateTopicCode)
	return err
}

func (tms *IggyTcpClient) DeleteTopic(streamId, topicId Identifier) error {
	message := binaryserialization.SerializeIdentifiers(streamId, topicId)
	_, err := tms.sendAndFetchResponse(message, DeleteTopicCode)
	return err
}
