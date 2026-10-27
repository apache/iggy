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
        try await getStream(streamID, lineage: nil)
    }

    /// ``getStream(_:)`` for a call pinned to `captured`.
    func getStream(_ streamID: Identifier, lineage captured: UInt64?) async throws -> StreamDetails? {
        let lineage = try await requireAuthenticated(under: captured)
        let body = try await send(.getStream, Requests.identifierOnly(streamID), lineage: lineage)
        return body.isEmpty ? nil : try Responses.streamDetails(body[...])
    }

    /// Every stream.
    public func getStreams() async throws -> [StreamInfo] {
        let lineage = try await requireAuthenticated()
        let body = try await send(.getStreams, lineage: lineage)
        return body.isEmpty ? [] : try Responses.streams(body[...])
    }

    /// Creates a stream and returns it. A key in ``StreamCreateOptions/encryptionKey``
    /// is refused before anything is sent unless it is exactly 32 bytes.
    public func createStream(name: String, options: StreamCreateOptions = StreamCreateOptions()) async throws -> StreamDetails {
        try await createStream(name: name, options: options, lineage: nil)
    }

    /// ``createStream(name:options:)`` for a call pinned to `captured`.
    func createStream(name: String, options: StreamCreateOptions = StreamCreateOptions(), lineage captured: UInt64?) async throws -> StreamDetails {
        let payload = try Requests.createStream(name: name, options: try options.encode())
        let lineage = try await requireAuthenticated(under: captured)
        return try Responses.streamDetails(try await send(.createStream, payload, lineage: lineage)[...])
    }

    /// Updates a stream: renames it today. Unmentioned keys keep their values.
    public func updateStream(_ streamID: Identifier, options: StreamUpdateOptions) async throws {
        let payload = try Requests.updateResource(.stream(streamID), try options.toPatch())
        let lineage = try await requireAuthenticated()
        _ = try await send(.updateResource, payload, lineage: lineage)
    }

    /// Deletes a stream with every topic and message in it.
    public func deleteStream(_ streamID: Identifier) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(.deleteStream, Requests.identifierOnly(streamID), lineage: lineage)
        core.groupState.invalidateTopicDiscovery()
    }
}

// MARK: - Topics

extension IggyClient {
    /// A topic by id or name, with its partitions, or nil if it does not exist.
    public func getTopic(streamID: Identifier, topicID: Identifier) async throws -> TopicDetails? {
        let lineage = try await requireAuthenticated()
        return try await getTopic(streamID: streamID, topicID: topicID, lineage: lineage)
    }

    /// ``getTopic(streamID:topicID:)`` as a request of a call that started
    /// under `lineage`.
    func getTopic(streamID: Identifier, topicID: Identifier, lineage: UInt64) async throws -> TopicDetails? {
        let body = try await send(.getTopic, Requests.streamAndTopic(streamID, topicID), lineage: lineage)
        guard !body.isEmpty else {
            return nil
        }
        let details = try Responses.topicDetails(body[...])
        // Client-side partitioning and the sends that follow read the count
        // and the partition contexts from here.
        core.groupState.setTopicPartitions(.init(stream: streamID, topic: topicID), partitions: details.partitions)
        return details
    }

    /// Every topic of a stream.
    public func getTopics(streamID: Identifier) async throws -> [Topic] {
        let lineage = try await requireAuthenticated()
        let body = try await send(.getTopics, Requests.identifierOnly(streamID), lineage: lineage)
        return body.isEmpty ? [] : try Responses.topics(body[...])
    }

    /// Creates a topic and returns it. Every knob rides `options`; an absent
    /// key resolves against the server's defaults.
    public func createTopic(streamID: Identifier, name: String, options: TopicCreateOptions = TopicCreateOptions()) async throws -> TopicDetails {
        try await createTopic(streamID: streamID, name: name, options: options, lineage: nil)
    }

    /// ``createTopic(streamID:name:options:)`` for a call pinned to `captured`.
    func createTopic(
        streamID: Identifier, name: String, options: TopicCreateOptions = TopicCreateOptions(), lineage captured: UInt64?
    ) async throws -> TopicDetails {
        let lineage = try await requireAuthenticated(under: captured)
        let payload = try Requests.createTopic(
            streamID: streamID, partitionsCount: options.partitionsCount ?? TopicCreateOptions.defaultPartitionsCount,
            name: name, options: try options.encode())
        return try Responses.topicDetails(try await send(.createTopic, payload, lineage: lineage)[...])
    }

    /// Renames a topic, sets its updatable options, or resets them to their
    /// defaults. Unmentioned keys keep their values.
    public func updateTopic(streamID: Identifier, topicID: Identifier, options: TopicUpdateOptions) async throws {
        let payload = try Requests.updateResource(.topic(stream: streamID, topic: topicID), try options.toPatch())
        let lineage = try await requireAuthenticated()
        _ = try await send(.updateResource, payload, lineage: lineage)
    }

    /// Deletes a topic with every message in it.
    public func deleteTopic(streamID: Identifier, topicID: Identifier) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(.deleteTopic, Requests.streamAndTopic(streamID, topicID), lineage: lineage)
        core.groupState.invalidateTopicDiscovery()
    }
}

// MARK: - Partitions and segments

extension IggyClient {
    /// Appends `count` partitions to a topic.
    public func createPartitions(streamID: Identifier, topicID: Identifier, count: UInt32) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(.createPartitions, Requests.partitions(streamID: streamID, topicID: topicID, partitionsCount: count), lineage: lineage)
        core.groupState.invalidateTopicDiscovery()
    }

    /// Removes the last `count` partitions of a topic.
    public func deletePartitions(streamID: Identifier, topicID: Identifier, count: UInt32) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(.deletePartitions, Requests.partitions(streamID: streamID, topicID: topicID, partitionsCount: count), lineage: lineage)
        core.groupState.invalidateTopicDiscovery()
    }

    /// Removes the oldest `count` closed segments of a partition.
    public func deleteSegments(streamID: Identifier, topicID: Identifier, partitionID: UInt32, count: UInt32) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(
            .deleteSegments, Requests.deleteSegments(streamID: streamID, topicID: topicID, partitionID: partitionID, segmentsCount: count), lineage: lineage)
    }
}

// MARK: - Consumer groups

extension IggyClient {
    /// A consumer group with its members, or nil if it does not exist.
    public func getConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws -> ConsumerGroupDetails? {
        let lineage = try await requireAuthenticated()
        let body = try await send(.getConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID), lineage: lineage)
        return body.isEmpty ? nil : try Responses.consumerGroupDetails(body[...])
    }

    /// Every consumer group of a topic.
    public func getConsumerGroups(streamID: Identifier, topicID: Identifier) async throws -> [ConsumerGroup] {
        let lineage = try await requireAuthenticated()
        let body = try await send(.getConsumerGroups, Requests.streamAndTopic(streamID, topicID), lineage: lineage)
        return body.isEmpty ? [] : try Responses.consumerGroups(body[...])
    }

    /// Creates a consumer group and returns it.
    public func createConsumerGroup(streamID: Identifier, topicID: Identifier, name: String) async throws -> ConsumerGroupDetails {
        let lineage = try await requireAuthenticated()
        return try Responses.consumerGroupDetails(
            try await send(.createConsumerGroup, try Requests.createConsumerGroup(streamID: streamID, topicID: topicID, name: name), lineage: lineage)[...])
    }

    /// Deletes a consumer group with its stored offsets.
    public func deleteConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(.deleteConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID), lineage: lineage)
    }

    /// Joins a consumer group. Joining changes the member's assignment and
    /// the group generation, so the next group poll syncs the assignment.
    public func joinConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(.joinConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID), lineage: lineage)
        core.groupState.invalidateAssignment(.init(stream: streamID, topic: topicID, group: groupID))
    }

    /// Leaves a consumer group. The heartbeat stops syncing its assignment.
    public func leaveConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(.leaveConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID), lineage: lineage)
        let key = ConsumerGroupState.GroupKey(stream: streamID, topic: topicID, group: groupID)
        core.groupState.invalidateAssignment(key)
        core.groupState.deregisterGroup(key)
    }
}

// MARK: - Users

extension IggyClient {
    /// A user by id or username, with its permissions, or nil if unknown.
    public func getUser(_ userID: Identifier) async throws -> UserInfoDetails? {
        let lineage = try await requireAuthenticated()
        let body = try await send(.getUser, Requests.identifierOnly(userID), lineage: lineage)
        return body.isEmpty ? nil : try Responses.userDetails(body[...])
    }

    /// Every user.
    public func getUsers() async throws -> [UserInfo] {
        let lineage = try await requireAuthenticated()
        let body = try await send(.getUsers, lineage: lineage)
        return body.isEmpty ? [] : try Responses.users(body[...])
    }

    /// Creates a user and returns it.
    public func createUser(
        username: String, password: String, status: UserStatus = .active, permissions: Permissions? = nil, options: UserCreateOptions = UserCreateOptions()
    ) async throws -> UserInfoDetails {
        let payload = try Requests.createUser(username: username, password: password, status: status, permissions: permissions, options: try options.encode())
        let lineage = try await requireAuthenticated()
        return try Responses.userDetails(try await send(.createUser, payload, lineage: lineage)[...])
    }

    /// Deletes a user and the tokens it owns.
    public func deleteUser(_ userID: Identifier) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(.deleteUser, Requests.identifierOnly(userID), lineage: lineage)
    }

    /// Changes a user's name and/or status. Renaming the signed-in or
    /// auto-login user moves the credentials later reconnects sign in with.
    public func updateUser(_ userID: Identifier, options: UserUpdateOptions) async throws {
        let payload = try Requests.updateResource(.user(userID), try options.toPatch())
        let lineage = try await requireAuthenticated()
        _ = try await send(.updateResource, payload, lineage: lineage)
    }

    /// Replaces a user's permissions; nil removes them all.
    public func updatePermissions(_ userID: Identifier, permissions: Permissions?) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(.updatePermissions, Requests.updatePermissions(userID: userID, permissions: permissions), lineage: lineage)
    }

    /// Changes a password. When it is the signed-in or auto-login user, the
    /// credentials used by later reconnects switch to the new password.
    public func changePassword(_ userID: Identifier, currentPassword: String, newPassword: String) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(
            .changePassword, try Requests.changePassword(userID: userID, currentPassword: currentPassword, newPassword: newPassword), lineage: lineage)
    }
}

// MARK: - Personal access tokens

extension IggyClient {
    /// The signed-in user's tokens. The secrets are not included.
    public func getPersonalAccessTokens() async throws -> [PersonalAccessTokenInfo] {
        let lineage = try await requireAuthenticated()
        let body = try await send(.getPersonalAccessTokens, lineage: lineage)
        return body.isEmpty ? [] : try Responses.personalAccessTokens(body[...])
    }

    /// Creates a token for the signed-in user. The secret is only returned
    /// here.
    public func createPersonalAccessToken(name: String, expiry: PersonalAccessTokenExpiry = .never) async throws -> RawPersonalAccessToken {
        let lineage = try await requireAuthenticated()
        return try Responses.rawPersonalAccessToken(
            try await send(.createPersonalAccessToken, try Requests.createPersonalAccessToken(name: name, expiry: expiry), lineage: lineage)[...])
    }

    /// Deletes one of the signed-in user's tokens by name.
    public func deletePersonalAccessToken(name: String) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await send(.deletePersonalAccessToken, try Requests.nameOnly(name), lineage: lineage)
    }
}
