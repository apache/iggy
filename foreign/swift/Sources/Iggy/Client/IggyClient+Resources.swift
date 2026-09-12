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

// MARK: - Streams

extension IggyClient {
    /// A stream by id or name, with its topics, or nil if it does not exist.
    public func getStream(_ streamID: Identifier) async throws -> StreamDetails? {
        try await requireAuthenticated()
        let body = try await send(.getStream, Requests.identifierOnly(streamID))
        return body.isEmpty ? nil : try Responses.streamDetails(body[...])
    }

    /// Every stream.
    public func getStreams() async throws -> [Stream] {
        try await requireAuthenticated()
        let body = try await send(.getStreams)
        return body.isEmpty ? [] : try Responses.streams(body[...])
    }

    /// Creates a stream and returns it.
    public func createStream(name: String) async throws -> StreamDetails {
        try await requireAuthenticated()
        return try Responses.streamDetails(try await send(.createStream, try Requests.createStream(name: name))[...])
    }

    /// Renames a stream.
    public func updateStream(_ streamID: Identifier, name: String, options: StreamUpdateOptions = StreamUpdateOptions()) async throws {
        try await requireAuthenticated()
        _ = try await send(.updateStream, try Requests.updateStream(streamID: streamID, name: name, options: try options.encode()))
    }

    public func deleteStream(_ streamID: Identifier) async throws {
        try await requireAuthenticated()
        _ = try await send(.deleteStream, Requests.identifierOnly(streamID))
    }

    /// Removes every message of every topic in the stream.
    public func purgeStream(_ streamID: Identifier) async throws {
        try await requireAuthenticated()
        _ = try await send(.purgeStream, Requests.identifierOnly(streamID))
    }
}

// MARK: - Topics

extension IggyClient {
    /// A topic by id or name, with its partitions, or nil if it does not exist.
    public func getTopic(streamID: Identifier, topicID: Identifier) async throws -> TopicDetails? {
        try await requireAuthenticated()
        let body = try await send(.getTopic, Requests.streamAndTopic(streamID, topicID))
        return body.isEmpty ? nil : try Responses.topicDetails(body[...])
    }

    /// Every topic of a stream.
    public func getTopics(streamID: Identifier) async throws -> [Topic] {
        try await requireAuthenticated()
        let body = try await send(.getTopics, Requests.identifierOnly(streamID))
        return body.isEmpty ? [] : try Responses.topics(body[...])
    }

    /// Creates a topic and returns it. Every knob rides `options`; an absent
    /// key resolves against the server's defaults.
    public func createTopic(streamID: Identifier, name: String, options: TopicCreateOptions = TopicCreateOptions()) async throws -> TopicDetails {
        try await requireAuthenticated()
        let payload = try Requests.createTopic(
            streamID: streamID, partitionsCount: options.partitionsCount ?? TopicCreateOptions.defaultPartitionsCount,
            name: name, options: try options.encode())
        return try Responses.topicDetails(try await send(.createTopic, payload)[...])
    }

    /// Renames a topic and updates its updatable options.
    public func updateTopic(streamID: Identifier, topicID: Identifier, name: String, options: TopicUpdateOptions = TopicUpdateOptions()) async throws {
        try await requireAuthenticated()
        _ = try await send(.updateTopic, try Requests.updateTopic(streamID: streamID, topicID: topicID, name: name, options: try options.encode()))
    }

    public func deleteTopic(streamID: Identifier, topicID: Identifier) async throws {
        try await requireAuthenticated()
        _ = try await send(.deleteTopic, Requests.streamAndTopic(streamID, topicID))
        await core.groupState.invalidatePartitionCount(.init(stream: streamID, topic: topicID))
    }

    /// Removes every message of the topic.
    public func purgeTopic(streamID: Identifier, topicID: Identifier) async throws {
        try await requireAuthenticated()
        _ = try await send(.purgeTopic, Requests.streamAndTopic(streamID, topicID))
    }
}

// MARK: - Partitions and segments

extension IggyClient {
    /// Appends `count` partitions to a topic.
    public func createPartitions(streamID: Identifier, topicID: Identifier, count: UInt32) async throws {
        try await requireAuthenticated()
        _ = try await send(.createPartitions, Requests.partitions(streamID: streamID, topicID: topicID, partitionsCount: count))
        await core.groupState.invalidatePartitionCount(.init(stream: streamID, topic: topicID))
    }

    /// Removes the last `count` partitions of a topic.
    public func deletePartitions(streamID: Identifier, topicID: Identifier, count: UInt32) async throws {
        try await requireAuthenticated()
        _ = try await send(.deletePartitions, Requests.partitions(streamID: streamID, topicID: topicID, partitionsCount: count))
        await core.groupState.invalidatePartitionCount(.init(stream: streamID, topic: topicID))
    }

    /// Removes the oldest `count` closed segments of a partition.
    public func deleteSegments(streamID: Identifier, topicID: Identifier, partitionID: UInt32, count: UInt32) async throws {
        try await requireAuthenticated()
        _ = try await send(.deleteSegments, Requests.deleteSegments(streamID: streamID, topicID: topicID, partitionID: partitionID, segmentsCount: count))
    }
}

// MARK: - Users

extension IggyClient {
    /// A user by id or username, with its permissions, or nil if unknown.
    public func getUser(_ userID: Identifier) async throws -> UserInfoDetails? {
        try await requireAuthenticated()
        let body = try await send(.getUser, Requests.identifierOnly(userID))
        return body.isEmpty ? nil : try Responses.userDetails(body[...])
    }

    public func getUsers() async throws -> [UserInfo] {
        try await requireAuthenticated()
        let body = try await send(.getUsers)
        return body.isEmpty ? [] : try Responses.users(body[...])
    }

    /// Creates a user and returns it.
    public func createUser(username: String, password: String, status: UserStatus = .active, permissions: Permissions? = nil) async throws -> UserInfoDetails {
        try await requireAuthenticated()
        let payload = try Requests.createUser(username: username, password: password, status: status, permissions: permissions)
        return try Responses.userDetails(try await send(.createUser, payload)[...])
    }

    public func deleteUser(_ userID: Identifier) async throws {
        try await requireAuthenticated()
        _ = try await send(.deleteUser, Requests.identifierOnly(userID))
    }

    /// Changes a user's name and/or status; nil leaves a field as is.
    public func updateUser(
        _ userID: Identifier, username: String? = nil, status: UserStatus? = nil, options: UserUpdateOptions = UserUpdateOptions()
    ) async throws {
        try await requireAuthenticated()
        _ = try await send(.updateUser, try Requests.updateUser(userID: userID, username: username, status: status, options: try options.encode()))
    }

    /// Replaces a user's permissions; nil removes them all.
    public func updatePermissions(_ userID: Identifier, permissions: Permissions?) async throws {
        try await requireAuthenticated()
        _ = try await send(.updatePermissions, Requests.updatePermissions(userID: userID, permissions: permissions))
    }

    /// Changes a password. When it is the signed-in or auto-login user, the
    /// credentials used by later reconnects switch to the new password.
    public func changePassword(_ userID: Identifier, currentPassword: String, newPassword: String) async throws {
        try await requireAuthenticated()
        _ = try await send(.changePassword, try Requests.changePassword(userID: userID, currentPassword: currentPassword, newPassword: newPassword))
        await core.refreshSessionPassword(user: userID, newPassword: newPassword)
    }
}

// MARK: - Personal access tokens

extension IggyClient {
    /// The signed-in user's tokens. The secrets are not included.
    public func getPersonalAccessTokens() async throws -> [PersonalAccessTokenInfo] {
        try await requireAuthenticated()
        let body = try await send(.getPersonalAccessTokens)
        return body.isEmpty ? [] : try Responses.personalAccessTokens(body[...])
    }

    /// Creates a token for the signed-in user. The secret is only returned
    /// here.
    public func createPersonalAccessToken(name: String, expiry: PersonalAccessTokenExpiry = .never) async throws -> RawPersonalAccessToken {
        try await requireAuthenticated()
        return try Responses.rawPersonalAccessToken(
            try await send(.createPersonalAccessToken, try Requests.createPersonalAccessToken(name: name, expiry: expiry))[...])
    }

    public func deletePersonalAccessToken(name: String) async throws {
        try await requireAuthenticated()
        _ = try await send(.deletePersonalAccessToken, try Requests.nameOnly(name))
    }
}

// MARK: - Consumer groups

extension IggyClient {
    /// A consumer group with its members, or nil if it does not exist.
    public func getConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws -> ConsumerGroupDetails? {
        try await requireAuthenticated()
        let body = try await send(.getConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID))
        return body.isEmpty ? nil : try Responses.consumerGroupDetails(body[...])
    }

    public func getConsumerGroups(streamID: Identifier, topicID: Identifier) async throws -> [ConsumerGroup] {
        try await requireAuthenticated()
        let body = try await send(.getConsumerGroups, Requests.streamAndTopic(streamID, topicID))
        return body.isEmpty ? [] : try Responses.consumerGroups(body[...])
    }

    /// Creates a consumer group and returns it.
    public func createConsumerGroup(streamID: Identifier, topicID: Identifier, name: String) async throws -> ConsumerGroupDetails {
        try await requireAuthenticated()
        return try Responses.consumerGroupDetails(
            try await send(.createConsumerGroup, try Requests.createConsumerGroup(streamID: streamID, topicID: topicID, name: name))[...])
    }

    public func deleteConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws {
        try await requireAuthenticated()
        _ = try await send(.deleteConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID))
        let key = ConsumerGroupState.GroupKey(stream: streamID, topic: topicID, group: groupID)
        await core.groupState.invalidateAssignment(key)
        await core.groupState.deregisterGroup(key)
    }

    /// Joins a group. Joining changes the assignment and the group
    /// generation, so any cached assignment is dropped.
    public func joinConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws {
        try await requireAuthenticated()
        _ = try await send(.joinConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID))
        await core.groupState.invalidateAssignment(.init(stream: streamID, topic: topicID, group: groupID))
    }

    public func leaveConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws {
        try await requireAuthenticated()
        _ = try await send(.leaveConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID))
        let key = ConsumerGroupState.GroupKey(stream: streamID, topic: topicID, group: groupID)
        await core.groupState.invalidateAssignment(key)
        await core.groupState.deregisterGroup(key)
    }

    /// Fetches this member's partition assignment from the coordinator and
    /// caches it for group polls. Nil when this client is not a member.
    @discardableResult
    public func syncConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws -> ConsumerGroupAssignment? {
        try await requireAuthenticated()
        return try await syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: groupID)
    }
}

// MARK: - Consumer offsets

extension IggyClient {
    /// Stores the offset of a consumer on a partition. A consumer-group
    /// consumer stores it for the group.
    public func storeConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, offset: UInt64) async throws {
        try await requireAuthenticated()
        _ = try await send(
            .storeConsumerOffset,
            Requests.storeConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, offset: offset, ack: .quorum))
    }

    /// The stored offset of a consumer on a partition, or nil when none is
    /// stored.
    public func getConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?) async throws -> ConsumerOffsetInfo? {
        try await requireAuthenticated()
        let body = try await send(
            .getConsumerOffset, Requests.getConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID))
        return body.isEmpty ? nil : try Responses.consumerOffset(body[...])
    }

    public func deleteConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?) async throws {
        try await requireAuthenticated()
        _ = try await send(
            .deleteConsumerOffset,
            Requests.deleteConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, ack: .quorum))
    }
}
