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

import "context"

type Client interface {
	// Close closes the client and releases all the resources.
	Close() error

	// GetConnectionInfo returns the current connection information including protocol and server address
	GetConnectionInfo() *ConnectionInfo

	// GetClusterMetadata get the metadata of the cluster including node information, roles, and status.
	// Authentication is required.
	GetClusterMetadata(ctx context.Context) (*ClusterMetadata, error)

	// GetStream get the info about a specific stream by unique ID or name.
	// Authentication is required, and the permission to read the streams.
	GetStream(ctx context.Context, streamId Identifier) (*StreamDetails, error)

	// GetStreams get the info about all the streams.
	// Authentication is required, and the permission to read the streams.
	GetStreams(ctx context.Context) ([]Stream, error)

	// CreateStream create a new stream.
	// Authentication is required, and the permission to manage the streams.
	CreateStream(ctx context.Context, name string) (*StreamDetails, error)

	// UpdateStream update a stream by unique ID or name.
	// Authentication is required, and the permission to manage the streams.
	UpdateStream(ctx context.Context, streamId Identifier, name string) error

	// DeleteStream delete a stream by unique ID or name.
	// Authentication is required, and the permission to manage the streams.
	DeleteStream(ctx context.Context, id Identifier) error

	// GetTopic Get the info about a specific topic by unique ID or name.
	// Authentication is required, and the permission to read the topics.
	GetTopic(ctx context.Context, streamId, topicId Identifier) (*TopicDetails, error)

	// GetTopics get the info about all the topics.
	// Authentication is required, and the permission to read the topics.
	GetTopics(ctx context.Context, streamId Identifier) ([]Topic, error)

	// CreateTopic create a new topic.
	// Authentication is required, and the permission to manage the topics.
	CreateTopic(
		ctx context.Context,
		streamId Identifier,
		name string,
		partitionsCount uint32,
		compressionAlgorithm CompressionAlgorithm,
		messageExpiry Duration,
		maxTopicSize uint64,
		replicationFactor *uint8,
	) (*TopicDetails, error)

	// UpdateTopic update a topic by unique ID or name.
	// Authentication is required, and the permission to manage the topics.
	UpdateTopic(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		name string,
		compressionAlgorithm CompressionAlgorithm,
		messageExpiry Duration,
		maxTopicSize uint64,
		replicationFactor *uint8,
	) error

	// DeleteTopic delete a topic by unique ID or name.
	// Authentication is required, and the permission to manage the topics.
	DeleteTopic(ctx context.Context, streamId, topicId Identifier) error

	// SendMessages sends messages using specified partitioning strategy to the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to send the messages.
	SendMessages(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		partitioning Partitioning,
		messages []IggyMessage,
	) error

	// PollMessages poll given amount of messages using the specified consumer and strategy from the specified stream and topic by unique IDs or names.
	// Authentication is required, and the permission to poll the messages.
	PollMessages(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		consumer Consumer,
		strategy PollingStrategy,
		count uint32,
		autoCommit bool,
		partitionId *uint32,
	) (*PolledMessage, error)

	// StoreConsumerOffset store the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to poll the messages.
	StoreConsumerOffset(
		ctx context.Context,
		consumer Consumer,
		streamId Identifier,
		topicId Identifier,
		offset uint64,
		partitionId *uint32,
	) error

	// GetConsumerOffset get the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to poll the messages.
	GetConsumerOffset(
		ctx context.Context,
		consumer Consumer,
		streamId Identifier,
		topicId Identifier,
		partitionId *uint32,
	) (*ConsumerOffsetInfo, error)

	// GetConsumerGroups get the info about all the consumer groups for the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to read the streams or topics.
	GetConsumerGroups(ctx context.Context, streamId Identifier, topicId Identifier) ([]ConsumerGroup, error)

	// DeleteConsumerOffset delete the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to poll the messages.
	DeleteConsumerOffset(
		ctx context.Context,
		consumer Consumer,
		streamId Identifier,
		topicId Identifier,
		partitionId *uint32,
	) error

	// GetConsumerGroup get the info about a specific consumer group by unique ID or name for the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to read the streams or topics.
	GetConsumerGroup(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		groupId Identifier,
	) (*ConsumerGroupDetails, error)

	// CreateConsumerGroup create a new consumer group for the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to manage the streams or topics.
	CreateConsumerGroup(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		name string,
	) (*ConsumerGroupDetails, error)

	// DeleteConsumerGroup delete a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to manage the streams or topics.
	DeleteConsumerGroup(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		groupId Identifier,
	) error

	// JoinConsumerGroup join a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to read the streams or topics.
	JoinConsumerGroup(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		groupId Identifier,
	) error

	// LeaveConsumerGroup leave a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to read the streams or topics.
	LeaveConsumerGroup(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		groupId Identifier,
	) error

	// CreatePartitions create new N partitions for a topic by unique ID or name.
	// For example, given a topic with 3 partitions, if you create 2 partitions, the topic will have 5 partitions (from 1 to 5).
	// Authentication is required, and the permission to manage the partitions.
	CreatePartitions(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		partitionsCount uint32,
	) error

	// DeletePartitions delete last N partitions for a topic by unique ID or name.
	// For example, given a topic with 5 partitions, if you delete 2 partitions, the topic will have 3 partitions left (from 1 to 3).
	// Authentication is required, and the permission to manage the partitions.
	DeletePartitions(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		partitionsCount uint32,
	) error

	// GetUser get the info about a specific user by unique ID or username.
	// Authentication is required, and the permission to read the users, unless the provided user ID is the same as the authenticated user.
	GetUser(ctx context.Context, identifier Identifier) (*UserInfoDetails, error)

	// GetUsers get the info about all the users.
	// Authentication is required, and the permission to read the users.
	GetUsers(ctx context.Context) ([]UserInfo, error)

	// CreateUser create a new user.
	// Authentication is required, and the permission to manage the users.
	CreateUser(
		ctx context.Context,
		username string,
		password string,
		status UserStatus,
		permissions *Permissions,
	) (*UserInfoDetails, error)

	// UpdateUser update a user by unique ID or username.
	// Authentication is required, and the permission to manage the users.
	UpdateUser(
		ctx context.Context,
		userID Identifier,
		username *string,
		status *UserStatus,
	) error

	// UpdatePermissions update the permissions of a user by unique ID or username.
	// Authentication is required, and the permission to manage the users.
	UpdatePermissions(ctx context.Context, userID Identifier, permissions *Permissions) error

	// ChangePassword change the password of a user by unique ID or username.
	// Authentication is required, and the permission to manage the users, unless the provided user ID is the same as the authenticated user.
	ChangePassword(
		ctx context.Context,
		userID Identifier,
		currentPassword string,
		newPassword string,
	) error

	// DeleteUser delete a user by unique ID or username.
	// Authentication is required, and the permission to manage the users.
	DeleteUser(ctx context.Context, identifier Identifier) error

	// CreatePersonalAccessToken create a new personal access token for the currently authenticated user.
	CreatePersonalAccessToken(ctx context.Context, name string, expiry uint32) (*RawPersonalAccessToken, error)

	// DeletePersonalAccessToken delete a personal access token of the currently authenticated user by unique token name.
	DeletePersonalAccessToken(ctx context.Context, name string) error

	// GetPersonalAccessTokens get the info about all the personal access tokens of the currently authenticated user.
	GetPersonalAccessTokens(ctx context.Context) ([]PersonalAccessTokenInfo, error)

	// LoginWithPersonalAccessToken login the user with the provided personal access token.
	LoginWithPersonalAccessToken(ctx context.Context, token string) (*IdentityInfo, error)

	// LoginUser login a user by username and password.
	LoginUser(ctx context.Context, username string, password string) (*IdentityInfo, error)

	// LogoutUser logout the currently authenticated user.
	LogoutUser(ctx context.Context) error

	// GetStats get the stats of the system such as PID, memory usage, streams count etc.
	// Authentication is required, and the permission to read the server info.
	GetStats(ctx context.Context) (*Stats, error)

	// Ping the server to check if it's alive.
	Ping(ctx context.Context) error

	// GetClients get the info about all the currently connected clients (not to be confused with the users).
	// Authentication is required, and the permission to read the server info.
	GetClients(ctx context.Context) ([]ClientInfo, error)

	// GetClient get the info about a specific client by unique ID (not to be confused with the user).
	// Authentication is required, and the permission to read the server info.
	GetClient(ctx context.Context, clientId uint32) (*ClientInfoDetails, error)
}
