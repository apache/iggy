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

func (tms *IggyTcpClient) GetConsumerGroups(streamId, topicId Identifier) ([]ConsumerGroup, error) {
	message := binaryserialization.SerializeIdentifiers(streamId, topicId)
	buffer, err := tms.sendAndFetchResponse(message, GetGroupsCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeConsumerGroups(buffer), err
}

func (tms *IggyTcpClient) GetConsumerGroup(streamId, topicId, groupId Identifier) (*ConsumerGroupDetails, error) {
	message := binaryserialization.SerializeIdentifiers(streamId, topicId, groupId)
	buffer, err := tms.sendAndFetchResponse(message, GetGroupCode)
	if err != nil {
		return nil, err
	}
	if len(buffer) == 0 {
		return nil, ierror.ConsumerGroupIdNotFound
	}

	consumerGroupDetails := binaryserialization.DeserializeConsumerGroup(buffer)
	return consumerGroupDetails, err
}

func (tms *IggyTcpClient) CreateConsumerGroup(streamId Identifier, topicId Identifier, name string, groupId *uint32) (*ConsumerGroupDetails, error) {
	if MaxStringLength < len(name) {
		return nil, ierror.TextTooLong("consumer_group_name")
	}
	message := binaryserialization.CreateGroup(CreateConsumerGroupRequest{
		StreamId:        streamId,
		TopicId:         topicId,
		ConsumerGroupId: groupId,
		Name:            name,
	})
	buffer, err := tms.sendAndFetchResponse(message, CreateGroupCode)
	if err != nil {
		return nil, err
	}
	consumerGroup := binaryserialization.DeserializeConsumerGroup(buffer)
	return consumerGroup, err
}

func (tms *IggyTcpClient) DeleteConsumerGroup(streamId Identifier, topicId Identifier, groupId Identifier) error {
	message := binaryserialization.SerializeIdentifiers(streamId, topicId, groupId)
	_, err := tms.sendAndFetchResponse(message, DeleteGroupCode)
	return err
}

func (tms *IggyTcpClient) JoinConsumerGroup(streamId Identifier, topicId Identifier, groupId Identifier) error {
	message := binaryserialization.SerializeIdentifiers(streamId, topicId, groupId)
	_, err := tms.sendAndFetchResponse(message, JoinGroupCode)
	return err
}

func (tms *IggyTcpClient) LeaveConsumerGroup(streamId Identifier, topicId Identifier, groupId Identifier) error {
	message := binaryserialization.SerializeIdentifiers(streamId, topicId, groupId)
	_, err := tms.sendAndFetchResponse(message, LeaveGroupCode)
	return err
}
