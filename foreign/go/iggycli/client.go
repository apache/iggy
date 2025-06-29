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

package iggycli

import . "github.com/apache/iggy/foreign/go/contracts"

type Client interface {
	GetStream(request GetStreamRequest) (*StreamResponse, error)
	GetStreams() ([]StreamResponse, error)
	CreateStream(request CreateStreamRequest) error
	UpdateStream(request UpdateStreamRequest) error
	DeleteStream(id Identifier) error

	GetTopic(streamId, topicId Identifier) (*TopicDetails, error)
	GetTopics(streamId Identifier) ([]Topic, error)
	CreateTopic(request CreateTopicRequest) error
	UpdateTopic(request UpdateTopicRequest) error
	DeleteTopic(streamId, topicId Identifier) error

	SendMessages(request SendMessagesRequest) error
	PollMessages(request PollMessageRequest) (*PollMessageResponse, error)

	StoreConsumerOffset(request StoreConsumerOffsetRequest) error
	GetConsumerOffset(request GetConsumerOffsetRequest) (*ConsumerOffsetInfo, error)
	GetConsumerGroups(streamId Identifier, topicId Identifier) ([]ConsumerGroupResponse, error)
	GetConsumerGroup(streamId, topicId, groupId Identifier) (*ConsumerGroupResponse, error)
	CreateConsumerGroup(request CreateConsumerGroupRequest) error
	DeleteConsumerGroup(request DeleteConsumerGroupRequest) error
	JoinConsumerGroup(request JoinConsumerGroupRequest) error
	LeaveConsumerGroup(request LeaveConsumerGroupRequest) error

	CreatePartitions(request CreatePartitionsRequest) error
	DeletePartitions(request DeletePartitionsRequest) error

	GetUser(identifier Identifier) (*UserInfoDetails, error)
	GetUsers() ([]*UserInfo, error)
	CreateUser(request CreateUserRequest) error
	UpdateUser(request UpdateUserRequest) error
	UpdatePermissions(request UpdatePermissionsRequest) error
	ChangePassword(request ChangePasswordRequest) error
	DeleteUser(identifier Identifier) error

	CreatePersonalAccessToken(request CreatePersonalAccessTokenRequest) (*AccessToken, error)
	DeletePersonalAccessToken(request DeletePersonalAccessTokenRequest) error
	GetPersonalAccessTokens() ([]PersonalAccessTokenInfo, error)
	LoginWithPersonalAccessToken(request LoginWithPersonalAccessTokenRequest) (*LoginUserResponse, error)

	LoginUser(request LoginUserRequest) (*LoginUserResponse, error)
	LogoutUser() error

	GetStats() (*Stats, error)
	Ping() error

	GetClients() ([]ClientResponse, error)
	GetClient(clientId int) (*ClientResponse, error)
}
