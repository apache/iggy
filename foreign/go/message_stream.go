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

package iggy

import . "github.com/apache/iggy/foreign/go/contracts"

type MessageStream interface {
	GetStreamById(request GetStreamRequest) (*StreamResponse, error)
	GetStreams() ([]StreamResponse, error)
	CreateStream(request CreateStreamRequest) error
	UpdateStream(request UpdateStreamRequest) error
	DeleteStream(id Identifier) error

	GetTopicById(streamId, topicId Identifier) (*TopicResponse, error)
	GetTopics(streamId Identifier) ([]TopicResponse, error)
	CreateTopic(request CreateTopicRequest) error
	UpdateTopic(request UpdateTopicRequest) error
	DeleteTopic(streamId, topicId Identifier) error

	SendMessages(request SendMessagesRequest) error
	PollMessages(request FetchMessagesRequest) (*FetchMessagesResponse, error)

	StoreOffset(request StoreOffsetRequest) error
	GetOffset(request GetOffsetRequest) (*OffsetResponse, error)

	GetConsumerGroups(streamId Identifier, topicId Identifier) ([]ConsumerGroupResponse, error)
	GetConsumerGroupById(streamId, topicId, groupId Identifier) (*ConsumerGroupResponse, error)
	CreateConsumerGroup(request CreateConsumerGroupRequest) error
	DeleteConsumerGroup(request DeleteConsumerGroupRequest) error
	JoinConsumerGroup(request JoinConsumerGroupRequest) error
	LeaveConsumerGroup(request LeaveConsumerGroupRequest) error

	CreatePartition(request CreatePartitionsRequest) error
	DeletePartition(request DeletePartitionRequest) error

	GetUser(identifier Identifier) (*UserResponse, error)
	GetUsers() ([]*UserResponse, error)
	CreateUser(request CreateUserRequest) error
	UpdateUser(request UpdateUserRequest) error
	UpdateUserPermissions(request UpdateUserPermissionsRequest) error
	ChangePassword(request ChangePasswordRequest) error
	DeleteUser(identifier Identifier) error

	CreateAccessToken(request CreateAccessTokenRequest) (*AccessToken, error)
	DeleteAccessToken(request DeleteAccessTokenRequest) error
	GetAccessTokens() ([]AccessTokenResponse, error)

	LogIn(request LogInRequest) (*LogInResponse, error)
	LogInWithAccessToken(request LogInAccessTokenRequest) (*LogInResponse, error)
	LogOut() error

	GetStats() (*Stats, error)
	Ping() error

	GetClients() ([]ClientResponse, error)
	GetClientById(clientId int) (*ClientResponse, error)
}
