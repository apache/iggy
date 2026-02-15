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

type ConsumerGroup struct {
	Id              uint32 `json:"id"`
	Name            string `json:"name"`
	PartitionsCount uint32 `json:"partitionsCount"`
	MembersCount    uint32 `json:"membersCount"`
}

type ConsumerGroupDetails struct {
	ConsumerGroup
	Members []ConsumerGroupMember
}

type ConsumerGroupMember struct {
	ID              uint32
	PartitionsCount uint32
	Partitions      []uint32
}

type CreateConsumerGroup struct {
	StreamId Identifier `json:"streamId"`
	TopicId  Identifier `json:"topicId"`
	Name     string     `json:"name"`
}

func (c *CreateConsumerGroup) Code() CommandCode {
	return CreateGroupCode
}

func (c *CreateConsumerGroup) MarshalBinary() ([]byte, error) {
	streamIdBytes, err := c.StreamId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	topicIdBytes, err := c.TopicId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	offset := len(streamIdBytes) + len(topicIdBytes)
	bytes := make([]byte, offset+1+len(c.Name))
	copy(bytes[0:len(streamIdBytes)], streamIdBytes)
	copy(bytes[len(streamIdBytes):offset], topicIdBytes)
	bytes[offset] = byte(len(c.Name))
	copy(bytes[offset+1:], c.Name)
	return bytes, nil
}

type DeleteConsumerGroup struct {
	StreamId        Identifier `json:"streamId"`
	TopicId         Identifier `json:"topicId"`
	ConsumerGroupId Identifier `json:"consumerGroupId"`
}

func (d *DeleteConsumerGroup) Code() CommandCode {
	return DeleteGroupCode
}

func (d *DeleteConsumerGroup) MarshalBinary() ([]byte, error) {
	return marshalIdentifiers(d.StreamId, d.TopicId, d.ConsumerGroupId)
}

type JoinConsumerGroup struct {
	StreamId        Identifier `json:"streamId"`
	TopicId         Identifier `json:"topicId"`
	ConsumerGroupId Identifier `json:"consumerGroupId"`
}

func (j *JoinConsumerGroup) Code() CommandCode {
	return JoinGroupCode
}

func (j *JoinConsumerGroup) MarshalBinary() ([]byte, error) {
	return marshalIdentifiers(j.StreamId, j.TopicId, j.ConsumerGroupId)
}

type LeaveConsumerGroup struct {
	StreamId        Identifier `json:"streamId"`
	TopicId         Identifier `json:"topicId"`
	ConsumerGroupId Identifier `json:"consumerGroupId"`
}

func (l *LeaveConsumerGroup) Code() CommandCode {
	return LeaveGroupCode
}

func (l *LeaveConsumerGroup) MarshalBinary() ([]byte, error) {
	return marshalIdentifiers(l.StreamId, l.TopicId, l.ConsumerGroupId)
}

type GetConsumerGroup struct {
	StreamId Identifier
	TopicId  Identifier
	GroupId  Identifier
}

func (g *GetConsumerGroup) Code() CommandCode {
	return GetGroupCode
}

func (g *GetConsumerGroup) MarshalBinary() ([]byte, error) {
	return marshalIdentifiers(g.StreamId, g.TopicId, g.GroupId)
}

type GetConsumerGroups struct {
	StreamId Identifier
	TopicId  Identifier
}

func (g *GetConsumerGroups) Code() CommandCode {
	return GetGroupsCode
}

func (g *GetConsumerGroups) MarshalBinary() ([]byte, error) {
	return marshalIdentifiers(g.StreamId, g.TopicId)
}

type ConsumerGroupInfo struct {
	StreamId        uint32 `json:"streamId"`
	TopicId         uint32 `json:"topicId"`
	ConsumerGroupId uint32 `json:"consumerGroupId"`
}
