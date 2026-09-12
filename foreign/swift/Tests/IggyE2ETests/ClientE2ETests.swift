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

import Foundation
import Iggy
import Testing

@Suite("Sessions", requiresServer)
struct SessionE2ETests {
    @Test func onlyPingWorksBeforeSigningIn() async throws {
        try await E2EEnvironment.withSession(login: false) { session in
            let client = session.client
            #expect(await client.state == .connected)
            try await client.ping()
            await #expect(throws: IggyError(.unauthenticated)) {
                try await client.getStreams()
            }
            // The roster is auth-gated so an unauthenticated reader cannot map
            // the private network.
            await #expect(throws: IggyError(.unauthenticated)) {
                try await client.getClusterMetadata()
            }
        }
    }

    @Test func loginLogoutAndRelogin() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            #expect(await client.state == .authenticated)
            let me = try await client.getMe()
            #expect(me.userID == 0)
            #expect(me.transport == "TCP")
            _ = try await client.getStreams()

            try await client.logout()
            #expect(await client.state == .connected)
            await #expect(throws: IggyError(.unauthenticated)) {
                try await client.getStreams()
            }

            let identity = try await client.login(username: E2EEnvironment.rootUsername, password: E2EEnvironment.rootPassword)
            #expect(identity.userID == 0)
            _ = try await client.getStreams()
        }
    }

    @Test func autoLoginSignsInOnConnect() async throws {
        try await E2EEnvironment.withSession(autoLogin: .usernamePassword(username: E2EEnvironment.rootUsername, password: E2EEnvironment.rootPassword)) {
            session in
            let client = session.client
            #expect(await client.state == .authenticated)
            _ = try await client.getStreams()
        }
    }

    @Test func connectionStringSignsIn() async throws {
        let address = E2EEnvironment.address!
        var connectionString = "iggy://\(E2EEnvironment.rootUsername):\(E2EEnvironment.rootPassword)@\(address)?reconnection_interval=200ms"
        if let tls = E2EEnvironment.tlsOptions {
            connectionString += "&tls=true&tls_domain=\(tls.domain!)&tls_ca_file=\(tls.caFile!)"
        }
        let client = try IggyClient(connectionString: connectionString)
        try await client.connect()
        #expect(await client.state == .authenticated)
        try await client.ping()
        try await client.shutdown()
    }

    @Test func invalidCredentialsAreRejected() async throws {
        try await E2EEnvironment.withSession(login: false) { session in
            let client = session.client
            await #expect(throws: IggyError.self) {
                try await client.login(username: "nobody", password: "wrong-password")
            }
            #expect(await client.state == .connected)
            try await client.ping()
        }
    }

    @Test func personalAccessTokenRoundTrip() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let name = E2EEnvironment.uniqueName("swift-pat")
            let token = try await client.createPersonalAccessToken(name: name, expiry: .after(.seconds(3600)))
            #expect(!token.token.isEmpty)
            let listed = try await client.getPersonalAccessTokens()
            let created = try #require(listed.first { $0.name == name })
            #expect(created.expiresAt != nil)

            let other = try await session.secondaryClient()
            let identity = try await other.login(personalAccessToken: token.token)
            #expect(identity.userID == 0)
            _ = try await other.getStreams()

            try await client.deletePersonalAccessToken(name: name)
            #expect(try await client.getPersonalAccessTokens().contains { $0.name == name } == false)
        }
    }

    @Test func eventsReportTheLifecycle() async throws {
        let client = IggyClient(
            configuration: E2EEnvironment.configuration(
                autoLogin: .usernamePassword(username: E2EEnvironment.rootUsername, password: E2EEnvironment.rootPassword)))
        let events = await client.events
        try await client.connect()
        try await client.logout()
        try await client.disconnect()
        try await client.shutdown()
        var received: [DiagnosticEvent] = []
        for await event in events {
            received.append(event)
        }
        #expect(received == [.connected, .signedIn, .signedOut, .disconnected, .shutdown])
    }

    @Test func shutdownIsTerminal() async throws {
        let client = try await E2EEnvironment.connect()
        try await client.shutdown()
        #expect(await client.state == .shutdown)
        await #expect(throws: IggyError(.clientShutdown)) {
            try await client.connect()
        }
        await #expect(throws: IggyError(.clientShutdown)) {
            try await client.ping()
        }
    }

    @Test func disconnectKeepsTheClientUsable() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            try await client.disconnect()
            #expect(await client.state == .disconnected)
            // No credentials survive an explicit disconnect, so nothing replays.
            await #expect(throws: IggyError(.notConnected)) {
                try await client.ping()
            }
            try await client.connect()
            try await client.ping()
            _ = try await client.login(username: E2EEnvironment.rootUsername, password: E2EEnvironment.rootPassword)
            _ = try await client.getStreams()
        }
    }
}

@Suite("Streams, topics, and partitions", requiresServer)
struct ResourceE2ETests {
    @Test func streamCRUD() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let name = E2EEnvironment.uniqueName("swift-stream")
            let created = try await client.createStream(name: name)
            #expect(created.name == name)
            #expect(created.topicsCount == 0)
            let id = Identifier(numeric: created.id)

            let byID = try #require(try await client.getStream(id))
            #expect(byID.name == name)
            let byName = try #require(try await client.getStream(Identifier(named: name)))
            #expect(byName.id == created.id)
            #expect(try await client.getStreams().contains { $0.id == created.id })

            let renamed = E2EEnvironment.uniqueName("swift-renamed")
            try await client.updateStream(id, name: renamed)
            #expect(try await client.getStream(id)?.name == renamed)

            try await client.purgeStream(id)
            try await client.deleteStream(id)
            // The server hands out the lowest free id, so a parallel test can
            // already own it again: assert on the name, which is unique.
            #expect(try await client.getStream(Identifier(named: renamed)) == nil)
            await #expect(throws: IggyError.self) {
                try await client.deleteStream(Identifier(named: renamed))
            }
        }
    }

    @Test func duplicateStreamNamesAreRejected() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let name = E2EEnvironment.uniqueName("swift-dup")
            let stream = try await client.createStream(name: name)
            await session.track(stream: try Identifier(named: name))
            do {
                _ = try await client.createStream(name: name)
                Issue.record("expected the duplicate to be rejected")
            } catch let error as IggyError {
                #expect(error.code == .streamNameAlreadyExists)
            }
        }
    }

    @Test func topicCRUDWithOptionsAndCatalog() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let name = E2EEnvironment.uniqueName("swift-topic")
            let stream = try await client.createStream(name: name)
            let streamID = Identifier(numeric: stream.id)
            await session.track(stream: try Identifier(named: name))

            let specs = try await client.describeOptions(scope: .topic)
            let keys = Set(specs.map(\.key))
            #expect(keys.contains(TopicOptionKey.enforceFsync))
            #expect(keys.contains(TopicOptionKey.segmentSize))
            #expect(try #require(specs.first { $0.key == TopicOptionKey.segmentSize }).kind == .uint64)
            #expect(try await client.describeOptions(scope: .stream).isEmpty)

            let options = TopicCreateOptions(
                partitionsCount: 3, compressionAlgorithm: .gzip, messageExpiry: .after(.seconds(3600)),
                maxTopicSize: .bytes(10_737_418_240), enforceFsync: true)
            let created = try await client.createTopic(streamID: streamID, name: name, options: options)
            #expect(created.name == name)
            #expect(created.partitionsCount == 3)
            #expect(created.partitions.map(\.id) == [0, 1, 2])
            #expect(created.compressionAlgorithm == .gzip)
            #expect(created.messageExpiry == .after(.seconds(3600)))
            #expect(created.maxTopicSize == .bytes(10_737_418_240))
            let topicID = Identifier(numeric: created.id)

            let fetched = try #require(try await client.getTopic(streamID: streamID, topicID: topicID))
            #expect(fetched.options[TopicOptionKey.enforceFsync] == .explicit(.bool(true)))
            // Keys the client left alone are resolved by admission and reported
            // as derived.
            #expect(fetched.options[TopicOptionKey.segmentSize]?.explicit == false)
            #expect(try await client.getTopics(streamID: streamID).map(\.id) == [created.id])
            #expect(try await client.getStream(streamID)?.topics.first?.name == name)

            let renamed = E2EEnvironment.uniqueName("swift-topic-renamed")
            try await client.updateTopic(
                streamID: streamID, topicID: topicID, name: renamed,
                options: TopicUpdateOptions(compressionAlgorithm: CompressionAlgorithm.none, messageExpiry: .never))
            let updated = try #require(try await client.getTopic(streamID: streamID, topicID: topicID))
            #expect(updated.name == renamed)
            #expect(updated.compressionAlgorithm == CompressionAlgorithm.none)
            #expect(updated.messageExpiry == .never)

            try await client.createPartitions(streamID: streamID, topicID: topicID, count: 2)
            #expect(try await client.getTopic(streamID: streamID, topicID: topicID)?.partitionsCount == 5)
            try await client.deletePartitions(streamID: streamID, topicID: topicID, count: 4)
            #expect(try await client.getTopic(streamID: streamID, topicID: topicID)?.partitionsCount == 1)

            try await client.purgeTopic(streamID: streamID, topicID: topicID)
            try await client.deleteTopic(streamID: streamID, topicID: topicID)
            #expect(try await client.getTopic(streamID: streamID, topicID: topicID) == nil)
        }
    }

    @Test func unknownOptionKeysAreRefused() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let name = E2EEnvironment.uniqueName("swift-badopt")
            let stream = try await client.createStream(name: name)
            let streamID = Identifier(numeric: stream.id)
            await session.track(stream: try Identifier(named: name))
            await #expect(throws: IggyError.self) {
                try await client.createTopic(streamID: streamID, name: name, options: TopicCreateOptions(raw: ["not_a_real_option": "1"]))
            }
            // A string value for a typed key is parsed by admission and reported
            // with its canonical kind.
            let created = try await client.createTopic(streamID: streamID, name: name, options: TopicCreateOptions(raw: [TopicOptionKey.enforceFsync: "true"]))
            let topic = try #require(try await client.getTopic(streamID: streamID, topicID: Identifier(numeric: created.id)))
            #expect(topic.options[TopicOptionKey.enforceFsync] == .explicit(.bool(true)))
        }
    }

    @Test func consumerGroupCRUD() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 2)
            let name = E2EEnvironment.uniqueName("swift-group")
            let group = try await client.createConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, name: name)
            #expect(group.name == name)
            #expect(group.partitionsCount == 2)
            #expect(group.members.isEmpty)
            let groupID = Identifier(numeric: group.id)
            #expect(try await client.getConsumerGroups(streamID: scratch.stream, topicID: scratch.topic).map(\.id) == [group.id])
            #expect(try await client.getConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: Identifier(named: name))?.id == group.id)

            try await client.joinConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID)
            let joined = try #require(try await client.getConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID))
            #expect(joined.membersCount == 1)
            #expect(joined.members.first?.partitions.sorted() == [0, 1])
            let me = try await client.getMe()
            #expect(me.consumerGroups.contains { $0.groupID == group.id })

            try await client.leaveConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID)
            #expect(try await client.getConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID)?.membersCount == 0)
            try await client.deleteConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID)
            #expect(try await client.getConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID) == nil)
        }
    }
}

@Suite("Users", requiresServer)
struct UserE2ETests {
    @Test func userLifecycle() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let username = String(E2EEnvironment.uniqueName("swift-user").prefix(50))
            let permissions = Permissions(
                global: GlobalPermissions(readServers: true, readStreams: true, readTopics: true, pollMessages: true, sendMessages: true))
            let created = try await client.createUser(username: username, password: "secret-1", status: .active, permissions: permissions)
            #expect(created.username == username)
            #expect(created.status == .active)
            #expect(created.permissions?.global.readStreams == true)
            #expect(created.permissions?.global.manageStreams == false)
            let userID = Identifier(numeric: created.id)
            #expect(try await client.getUsers().contains { $0.id == created.id })
            #expect(try await client.getUser(Identifier(named: username))?.id == created.id)

            // The new user can sign in and is bound by its permissions.
            let other = try await session.secondaryClient()
            let identity = try await other.login(username: username, password: "secret-1")
            #expect(identity.userID == created.id)
            _ = try await other.getStreams()
            await #expect(throws: IggyError(.unauthorized)) {
                try await other.createStream(name: E2EEnvironment.uniqueName("swift-forbidden"))
            }

            try await client.changePassword(userID, currentPassword: "secret-1", newPassword: "secret-2")
            try await client.updateUser(userID, username: username + "-renamed", status: .inactive)
            let updated = try #require(try await client.getUser(userID))
            #expect(updated.username == username + "-renamed")
            #expect(updated.status == .inactive)
            try await client.updatePermissions(userID, permissions: nil)
            #expect(try await client.getUser(userID)?.permissions == nil)
            try await client.deleteUser(userID)
            #expect(try await client.getUser(userID) == nil)
        }
    }

    @Test func invalidUsernamesFailBeforeTheWire() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            await #expect(throws: IggyError(.invalidUsername)) {
                try await client.createUser(username: "ab", password: "secret")
            }
            await #expect(throws: IggyError(.invalidPassword)) {
                try await client.createUser(username: "valid-name", password: "ab")
            }
        }
    }
}

@Suite("Messaging", requiresServer)
struct MessagingE2ETests {
    @Test func sendAndPollRoundTrip() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 2)

            var baseOffsets: [UInt64] = []
            for _ in 0..<3 {
                let response = try await client.sendMessages(
                    streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(4))
                let confirmation = try #require(response.confirmations.first)
                #expect(confirmation.streamID == scratch.streamID)
                #expect(confirmation.topicID == scratch.topicID)
                #expect(confirmation.partitionID == 0)
                baseOffsets.append(confirmation.baseOffset)
            }
            #expect(baseOffsets == [0, 4, 8])

            let polled = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, consumer: .default, strategy: .offset(0), count: 12, autoCommit: false)
            #expect(polled.partitionID == 0)
            #expect(polled.currentOffset == 11)
            #expect(polled.messages.count == 12)
            #expect(polled.messages.map(\.offset) == Array(0..<12))
            #expect(polled.messages[0].payloadString == "message 0")
            #expect(polled.messages[11].payloadString == "message 3")
            #expect(polled.messages.allSatisfy { !$0.id.isZero })
            #expect(polled.messages.allSatisfy { $0.timestamp.microseconds > 0 && $0.originTimestamp.microseconds > 0 })
            #expect(polled.messages.allSatisfy { $0.checksum != 0 })
            let ids = Set(polled.messages.map(\.id))
            #expect(ids.count == 12)

            let last = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, consumer: .default, strategy: .last, count: 1, autoCommit: false)
            #expect(last.messages.map(\.offset) == [11])
            let tail = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, consumer: .default, strategy: .offset(10), count: 100, autoCommit: false)
            #expect(tail.messages.map(\.offset) == [10, 11])
            let empty = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 1, consumer: .default, strategy: .first, count: 10, autoCommit: false)
            #expect(empty.messages.isEmpty)
        }
    }

    @Test func userHeadersAndIdsRoundTrip() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)

            let id = MessageID(uuid: UUID())
            let headers: UserHeaders = ["trace-id": "abc-123", "attempt": .uint32(3), "urgent": true, "ratio": 0.5, "raw": try .raw([1, 2, 3])]
            let message = try IggyMessage("with headers", id: id, userHeaders: headers)
            let binary = try IggyMessage(payload: [0x00, 0xFF, 0x10, 0x80])
            _ = try await client.sendMessages(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: [message, binary])

            let polled = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, consumer: .default, strategy: .first, count: 10, autoCommit: false)
            #expect(polled.messages.count == 2)
            let first = polled.messages[0]
            #expect(first.id == id)
            #expect(first.payloadString == "with headers")
            #expect(first.userHeaders == headers)
            #expect(first[header: "trace-id"]?.stringValue == "abc-123")
            #expect(first[header: "attempt"]?.uint32Value == 3)
            #expect(polled.messages[1].payload == [0x00, 0xFF, 0x10, 0x80])
            #expect(polled.messages[1].userHeaders == nil)
        }
    }

    @Test func balancedAndKeyedPartitioning() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 3)

            var balancedPartitions: [UInt32] = []
            for _ in 0..<3 {
                let response = try await client.sendMessages(
                    streamID: scratch.stream, topicID: scratch.topic, partitioning: .balanced, messages: try E2EEnvironment.messages(1))
                balancedPartitions.append(try #require(response.confirmations.first).partitionID)
            }
            #expect(Set(balancedPartitions) == [0, 1, 2])

            let first = try await client.sendMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .messagesKey("order-key-1"), messages: try E2EEnvironment.messages(1))
            let second = try await client.sendMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .messagesKey("order-key-1"), messages: try E2EEnvironment.messages(1))
            #expect(try #require(first.confirmations.first).partitionID == (try #require(second.confirmations.first).partitionID))
        }
    }

    @Test func partitionCountRefreshesAfterPartitionChanges() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)

            let first = try await client.sendMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .balanced, messages: try E2EEnvironment.messages(1))
            #expect(try #require(first.confirmations.first).partitionID == 0)
            try await client.createPartitions(streamID: scratch.stream, topicID: scratch.topic, count: 2)
            let second = try await client.sendMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .balanced, messages: try E2EEnvironment.messages(1))
            #expect(try #require(second.confirmations.first).partitionID == 1)
            try await client.deletePartitions(streamID: scratch.stream, topicID: scratch.topic, count: 2)
            let third = try await client.sendMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .balanced, messages: try E2EEnvironment.messages(1))
            #expect(try #require(third.confirmations.first).partitionID == 0)
        }
    }

    @Test func consumerOffsets() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            _ = try await client.sendMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(5))

            let consumer = Consumer.consumer("swift-offsets")
            #expect(try await client.getConsumerOffset(consumer: consumer, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0) == nil)
            try await client.storeConsumerOffset(consumer: consumer, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, offset: 3)
            let stored = try #require(try await client.getConsumerOffset(consumer: consumer, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0))
            #expect(stored.storedOffset == 3)
            #expect(stored.currentOffset == 4)
            #expect(stored.partitionID == 0)

            let next = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, consumer: consumer, strategy: .next, count: 10, autoCommit: true)
            #expect(next.messages.map(\.offset) == [4])
            #expect(try await client.getConsumerOffset(consumer: consumer, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0)?.storedOffset == 4)

            try await client.deleteConsumerOffset(consumer: consumer, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0)
            #expect(try await client.getConsumerOffset(consumer: consumer, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0) == nil)
            await #expect(throws: IggyError(.consumerOffsetNotFound)) {
                try await client.deleteConsumerOffset(consumer: consumer, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0)
            }
        }
    }

    @Test func consumerGroupPolling() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 3)
            let group = try await client.createConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, name: "swift-e2e-group")
            let groupID = Identifier(numeric: group.id)

            // Polling a group this client never joined is an error, not silence.
            await expectIggyError(.consumerGroupMemberNotFound) {
                _ = try await client.pollMessages(
                    streamID: scratch.stream, topicID: scratch.topic, partitionID: nil, consumer: .group(groupID), strategy: .next, count: 2, autoCommit: true)
            }

            try await client.joinConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID)
            let assignment = try #require(try await client.syncConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID))
            #expect(assignment.partitions.sorted() == [0, 1, 2])
            for partition in assignment.partitions {
                _ = try await client.sendMessages(
                    streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(partition), messages: try E2EEnvironment.messages(2))
            }
            // A group poll without a partition round-robins the owned partitions.
            var polledPartitions = Set<UInt32>()
            for _ in 0..<assignment.partitions.count {
                let polled = try await client.pollMessages(
                    streamID: scratch.stream, topicID: scratch.topic, partitionID: nil, consumer: .group(groupID), strategy: .next, count: 2, autoCommit: true)
                #expect(polled.partitionID != PolledMessages.noAssignedPartition)
                #expect(polled.messages.count == 2)
                polledPartitions.insert(polled.partitionID)
            }
            #expect(polledPartitions.count == assignment.partitions.count)
            try await client.leaveConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID)
            try await client.deleteConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID)
        }
    }

    @Test func flushAndSegmentDeletion() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            _ = try await client.sendMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(3))
            // The current server answers the flush with feature_unavailable; the
            // command still round-trips and the error surfaces typed.
            do {
                try await client.flushUnsavedBuffer(streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, fsync: true)
            } catch let error as IggyError where error.code == .featureUnavailable {}
            // Only closed segments can be deleted; a fresh partition has one open
            // segment, so the request is accepted and removes nothing.
            try await client.deleteSegments(streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, count: 1)
            let topic = try #require(try await client.getTopic(streamID: scratch.stream, topicID: scratch.topic))
            #expect(topic.partitions[0].messagesCount == 3)
        }
    }

    @Test func largePayloadsAndBigBatches() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let big = try IggyMessage(payload: [UInt8](repeating: 0xAB, count: 512 * 1024))
            let many = try (0..<1000).map { try IggyMessage("message \($0)") }
            _ = try await client.sendMessages(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: [big] + many)
            let polled = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, consumer: .default, strategy: .first, count: 1001, autoCommit: false)
            #expect(polled.messages.count == 1001)
            #expect(polled.messages[0].payload.count == 512 * 1024)
            #expect(polled.messages[1000].payloadString == "message 999")
        }
    }

    @Test func emptyBatchesAreRejectedLocally() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            await #expect(throws: IggyError(.invalidMessagesCount)) {
                try await client.sendMessages(streamID: 1, topicID: 1, partitioning: .balanced, messages: [])
            }
            await #expect(throws: IggyError.self) {
                try await client.sendMessages(streamID: "no-such-stream", topicID: 1, partitioning: .balanced, messages: ["x"])
            }
        }
    }
}

@Suite("System", requiresServer)
struct SystemE2ETests {
    @Test func statsClientsAndCluster() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let stats = try await client.getStats()
            #expect(stats.processID > 0)
            #expect(!stats.iggyServerVersion.isEmpty)
            #expect(stats.totalMemory > 0)

            let me = try await client.getMe()
            let clients = try await client.getClients()
            #expect(clients.contains { $0.clientID == me.clientID })
            let fetched = try #require(try await client.getClient(id: me.clientID))
            #expect(fetched.address == me.address)
            #expect(try await client.getClient(id: UInt32.max - 7) == nil)

            let metadata = try await client.getClusterMetadata()
            #expect(!metadata.nodes.isEmpty)
            #expect(metadata.nodes.allSatisfy { $0.endpoints.tcp != 0 })
        }
    }

    @Test func snapshot() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let snapshot = try await client.snapshot(compression: .stored, types: [.filesystemOverview])
            #expect(!snapshot.data.isEmpty)
        }
    }

    @Test func rawRequests() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            // Raw traffic never consumes a metadata request id, so the session stays
            // usable for the replicated command that follows.
            for _ in 0..<5 {
                _ = try? await client.sendRawRequest(code: 60_000, payload: [])
            }
            #expect(try await client.sendRawRequest(code: CommandCode.ping.rawValue, payload: []).isEmpty)
            #expect(!(try await client.sendRawRequest(code: CommandCode.getStats.rawValue, payload: [])).isEmpty)
            await #expect(throws: IggyError(.invalidCommand)) {
                try await client.sendRawRequest(code: 60_000, payload: [])
            }
            await #expect(throws: IggyError.self) {
                try await client.sendRawRequest(code: CommandCode.loginUser.rawValue, payload: [])
            }
            let scratch = try await session.scratchTopic(partitions: 1)
            #expect(try await client.getTopic(streamID: scratch.stream, topicID: scratch.topic) != nil)
        }
    }
}
