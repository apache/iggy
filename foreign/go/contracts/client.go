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

// Client provides access to Iggy over the configured transport.
//
// On a signed-in TCP coordinator connection, cancellation during a socket write
// waits for the write to finish within the 30 s request budget. Once written,
// cancellation returns the context error promptly while the reply is drained
// in the background. The request can still commit, but is not resent after the
// caller gives up. The session and consumer group membership remain. Later
// requests wait for the reply or the remainder of its budget; waiting for the
// exchange gate honors each request's own context. Close interrupts
// the exchange. If the budget expires, the connection is dropped; reconnecting
// creates a new session and consumers must rejoin their groups.
//
// A cancel in flight during sign-in, logout or an exchange without a bound
// session drops that connection. A cancel in flight on a primary data connection
// drops only that connection, preserving the coordinator session and group
// membership.
type Client interface {
	// Connect establishes the connection to the server.
	Connect(ctx context.Context) error

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

	// DescribeOptions get the option catalog for a resource scope: every key its
	// create command accepts, with the kind, default and bounds of each.
	// Authentication is required.
	DescribeOptions(ctx context.Context, scope OptionsScope) ([]OptionSpec, error)

	// CreateTopic create a new topic.
	// Authentication is required, and the permission to manage the topics.
	//
	// options carries topic option keys with no parameter of their own, for a key
	// the server catalog gained after this build shipped; call DescribeOptions to
	// see which keys a server accepts. A parameter above wins on collision.
	CreateTopic(
		ctx context.Context,
		streamId Identifier,
		name string,
		partitionsCount uint32,
		compressionAlgorithm CompressionAlgorithm,
		messageExpiry Duration,
		maxTopicSize uint64,
		options ...HeaderEntry,
	) (*TopicDetails, error)

	// UpdateTopic update a topic by unique ID or name.
	// Authentication is required, and the permission to manage the topics.
	//
	// options carries keys with no parameter of their own. The server refuses any
	// key an update may not change, by name.
	UpdateTopic(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		name string,
		compressionAlgorithm CompressionAlgorithm,
		messageExpiry Duration,
		maxTopicSize uint64,
		options ...HeaderEntry,
	) error

	// DeleteTopic delete a topic by unique ID or name.
	// Authentication is required, and the permission to manage the topics.
	DeleteTopic(ctx context.Context, streamId, topicId Identifier) error

	// SendMessages sends messages using specified partitioning strategy to the given stream and topic by unique IDs or names.
	// Authentication is required, and the permission to send the messages.
	//
	// The returned confirmations report where the server committed the batch.
	// A send whose reply is lost to a dropped connection returns
	// ErrDisconnected without a replay: a reconnect registers a fresh client
	// identity, so the server could not deduplicate the replay against a
	// batch that may have already committed. Retrying such a send is the
	// caller's decision and may write the batch twice.
	SendMessages(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		partitioning Partitioning,
		messages []IggyMessage,
	) (*SendMessagesResponse, error)

	// PollMessages poll given amount of messages using the specified consumer and strategy from the specified stream and topic by unique IDs or names.
	// Authentication is required, and the permission to poll the messages.
	//
	// Clustered auto-commit polls use persistent partition-primary connections
	// while the coordinator retains group membership. Only explicit refusal
	// before admission is retried. ErrTransientNotCommitted, or cancellation
	// after sending a poll, can mean the offset advanced without a reply.
	// These polls are never replayed automatically after an unknown outcome.
	// Servers must support primary routing and consumer-session attachment.
	//
	// Standalone auto-commit polls are not replayed after a lost reply. With
	// Next, the offset may already have advanced, so retrying can skip the
	// unread batch.
	//
	// In a cluster, polls without auto-commit read the coordinator replica.
	// Next polls may temporarily use the previous offset after a routed store
	// or delete, repeating messages or skipping the intended rewind or restart.
	//
	// A group poll that names no partition is orchestrated client-side and
	// has three outcomes:
	//   - err == nil with PartitionId == NoAssignedPartition and an empty
	//     batch: nothing to read right now, poll again. This covers a member
	//     that owns no partition and a rebalance still settling.
	//   - ErrConsumerGroupMemberNotFound: the client is not a member, for
	//     example after an explicit LeaveConsumerGroup. JoinConsumerGroup
	//     restores membership.
	//   - err == nil with a real partition id: the poll completed; the batch may be empty.
	//
	// Uncompressed payloads and user headers alias the reply buffer; copy
	// those bytes to retain only the message you need. S2 decompression
	// allocates a separate payload buffer.
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
	//
	// In a cluster, the write goes to the partition primary, like an
	// auto-commit poll. The client asks the coordinator for the route and sends
	// the write on a data connection attached to the coordinator session. The
	// client must reach the advertised TCP address of the primary. If the write
	// loses its reply, the call returns ErrTransientNotCommitted, not
	// ErrDisconnected. The outcome is then unknown, and storing the same offset
	// again is safe.
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
	//
	// In a cluster, the read goes to the replica on the coordinator node. Right
	// after a write on a primary on another node, it can return the previous
	// offset for a short time. Next polls without auto-commit share this lag;
	// see PollMessages.
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
	//
	// In a cluster, the delete takes the same route as StoreConsumerOffset. If
	// the delete loses its reply, the call returns ErrTransientNotCommitted, not
	// ErrDisconnected, and the outcome is then unknown.
	// Retrying after a lost reply can return nil or ErrConsumerOffsetNotFound
	// (status 3021). Both mean that no offset is stored.
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
	// Later polls do not automatically rejoin this group, even if the leave
	// returns an error. An explicit JoinConsumerGroup clears that intent.
	LeaveConsumerGroup(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		groupId Identifier,
	) error

	// SyncConsumerGroup fetch the partitions this client currently owns in a consumer group,
	// together with the group generation, for the given stream and topic by unique IDs or names.
	// Returns ErrConsumerGroupMemberNotFound when the client is not a member
	// of the group. A member that owns no partition gets a non-nil assignment
	// with an empty Partitions slice.
	// Authentication is required, and the permission to read the streams or topics.
	SyncConsumerGroup(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		groupId Identifier,
	) (*ConsumerGroupAssignment, error)

	// CreatePartitions create new N partitions for a topic by unique ID or name.
	// For example, given a topic with 3 partitions, if you create 2 partitions, the topic will have 5 partitions (from 0 to 4).
	// Authentication is required, and the permission to manage the partitions.
	CreatePartitions(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		partitionsCount uint32,
	) error

	// DeletePartitions delete last N partitions for a topic by unique ID or name.
	// For example, given a topic with 5 partitions, if you delete 2 partitions, the topic will have 3 partitions left (from 0 to 2).
	// Authentication is required, and the permission to manage the partitions.
	DeletePartitions(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		partitionsCount uint32,
	) error

	// DeleteSegments deletes N segments from a topic partition by stream and topic unique IDs or names.
	// Authentication is required, and the permission to manage the partitions.
	DeleteSegments(
		ctx context.Context,
		streamId Identifier,
		topicId Identifier,
		partitionId uint32,
		segmentsCount uint32,
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
	// A canceled username update can still commit while the TCP client's
	// remembered credentials remain unchanged. Explicitly sign in with the new
	// username before relying on automatic reconnect after such a change.
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
	// A canceled change can still commit while the TCP client's remembered
	// password remains unchanged. Explicitly sign in with the new password
	// before relying on automatic reconnect after such a change.
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

	// SendBinaryRequest sends a command code and payload and returns the raw response body.
	// Session-control codes return ierror.ErrInvalidCommand without writing to the connection.
	SendBinaryRequest(ctx context.Context, code uint32, payload []byte) ([]byte, error)
}
