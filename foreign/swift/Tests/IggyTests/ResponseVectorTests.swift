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

import Testing

@testable import Iggy

/// Every decoder reads bytes the Rust response encoders produced for known
/// values, so a layout drift on either side fails here.
@Suite("Response decoders match the Rust golden vectors")
struct ResponseVectorTests {
    let golden = GoldenFixture.shared
    let createdAt = IggyTimestamp(microseconds: 1_710_000_000_000_000)

    func body(_ name: String) -> ArraySlice<UInt8> {
        golden.bytes(name)[...]
    }

    func expectSampleTopic(_ topic: Topic, id: UInt32, name: String) {
        #expect(topic.id == id)
        #expect(topic.name == name)
        #expect(topic.createdAt == createdAt)
        #expect(topic.partitionsCount == 3)
        #expect(topic.messageExpiry == .after(.seconds(604_800)))
        #expect(topic.compressionAlgorithm == CompressionAlgorithm.none)
        #expect(topic.maxTopicSize == .bytes(1_073_741_824))
        #expect(topic.sizeBytes == 4096)
        #expect(topic.messagesCount == 100)
        #expect(topic.options["enforce_fsync"] == .explicit(.bool(true)))
        #expect(topic.options["segment_size"] == .explicit(.uint64(1_073_741_824)))
        #expect(topic.options["max_topic_size"] == .derived(.uint64(UInt64.max)))
    }

    @Test func streams() throws {
        let details = try Responses.streamDetails(body("response.get_stream"))
        #expect(details.id == 1)
        #expect(details.name == "my-stream")
        #expect(details.createdAt == createdAt)
        #expect(details.topicsCount == 2)
        #expect(details.sizeBytes == 2048)
        #expect(details.messagesCount == 200)
        #expect(details.topics.count == 2)
        expectSampleTopic(details.topics[0], id: 1, name: "topic-a")
        expectSampleTopic(details.topics[1], id: 2, name: "topic-b")

        let streams = try Responses.streams(body("response.get_streams"))
        #expect(streams.map(\.id) == [1, 2])
        #expect(streams.map(\.topicsCount) == [2, 0])
        #expect(streams.allSatisfy { $0.options.isEmpty })
        #expect(try Responses.streams([]).isEmpty)
    }

    @Test func topics() throws {
        let details = try Responses.topicDetails(body("response.get_topic"))
        expectSampleTopic(
            Topic(
                id: details.id, createdAt: details.createdAt, name: details.name, sizeBytes: details.sizeBytes, messageExpiry: details.messageExpiry,
                compressionAlgorithm: details.compressionAlgorithm, maxTopicSize: details.maxTopicSize, messagesCount: details.messagesCount,
                partitionsCount: details.partitionsCount, options: details.options), id: 1, name: "my-topic")
        #expect(details.partitions.map(\.id) == [0, 1, 2])
        #expect(details.partitions[0].currentOffset == 99)
        #expect(details.partitions[0].segmentsCount == 2)
        #expect(details.partitions[2].messagesCount == 8)

        let topics = try Responses.topics(body("response.get_topics"))
        #expect(topics.map(\.name) == ["events", "logs"])
        #expect(try Responses.topics(body("response.get_topics.empty")).isEmpty)
    }

    @Test func consumerGroups() throws {
        let details = try Responses.consumerGroupDetails(body("response.consumer_group_details"))
        #expect(details.id == 1)
        #expect(details.name == "my-group")
        #expect(details.partitionsCount == 6)
        #expect(details.members.map(\.partitions) == [[0, 1, 2], [3, 4, 5]])

        let groups = try Responses.consumerGroups(body("response.get_consumer_groups"))
        #expect(groups.map(\.name) == ["group-a", "group-b"])
        #expect(groups.map(\.membersCount) == [2, 0])

        let assignment = try Responses.consumerGroupAssignment(body("response.sync_consumer_group"))
        #expect(assignment == ConsumerGroupAssignment(generation: 7, partitions: [0, 2, 4]))
    }

    @Test func consumerOffset() throws {
        let offset = try Responses.consumerOffset(body("response.consumer_offset"))
        #expect(offset == ConsumerOffsetInfo(partitionID: 1, currentOffset: 1000, storedOffset: 500))
    }

    @Test func clients() throws {
        let details = try Responses.clientDetails(body("response.client_details"))
        #expect(details.clientID == 1)
        #expect(details.userID == 10)
        #expect(details.transport == "TCP")
        #expect(details.address == "127.0.0.1:8080")
        #expect(details.consumerGroups == [ConsumerGroupInfo(streamID: 1, topicID: 2, groupID: 3), ConsumerGroupInfo(streamID: 4, topicID: 5, groupID: 6)])

        let anonymous = try Responses.clientDetails(body("response.client_details.no_user"))
        #expect(anonymous.userID == nil)
        #expect(anonymous.consumerGroups.isEmpty)

        let clients = try Responses.clients(body("response.get_clients"))
        #expect(clients.map(\.clientID) == [1, 2])
        #expect(clients.map(\.transport) == ["TCP", "QUIC"])
    }

    @Test func users() throws {
        let details = try Responses.userDetails(body("response.user_details"))
        #expect(details.id == 1)
        #expect(details.username == "admin")
        #expect(details.status == .active)
        let permissions = try #require(details.permissions)
        #expect(permissions.global.manageServers)
        #expect(!permissions.global.manageUsers)
        #expect(permissions.streams[1]?.topics[10]?.sendMessages == true)
        #expect(permissions.streams[1]?.topics[20]?.manageTopic == false)
        #expect(permissions.streams[2]?.manageTopics == true)
        #expect(permissions.streams[2]?.topics.isEmpty == true)

        let plain = try Responses.userDetails(body("response.user_details.no_permissions"))
        #expect(plain.permissions == nil)

        let users = try Responses.users(body("response.get_users"))
        #expect(users.map(\.username) == ["admin", "alice"])
        #expect(users.map(\.status) == [.active, .inactive])
    }

    @Test func loginAndIdentity() throws {
        let login = try Responses.loginRegister(body("response.login_register"))
        #expect(login.userID == 42)
        #expect(login.session == 100)
        #expect(login.serverProtocolVersion == .current)
        #expect(login.serverVersion == "0.11.0")
        #expect(try Responses.identity(body("response.identity")) == IdentityInfo(userID: 42))
    }

    @Test func personalAccessTokens() throws {
        #expect(try Responses.rawPersonalAccessToken(body("response.raw_personal_access_token")).token == "raw-secret-token-value")
        let tokens = try Responses.personalAccessTokens(body("response.get_personal_access_tokens"))
        #expect(
            tokens == [
                PersonalAccessTokenInfo(name: "token-a", expiresAt: createdAt),
                PersonalAccessTokenInfo(name: "token-b", expiresAt: nil),
            ])
    }

    @Test func stats() throws {
        let stats = try Responses.stats(body("response.stats"))
        #expect(stats.processID == 1234)
        #expect(stats.cpuUsage == 25.5)
        #expect(stats.totalCPUUsage == 50)
        #expect(stats.memoryUsage == 1_073_741_824)
        #expect(stats.runTime == .seconds(0.0036))
        #expect(stats.startTime == createdAt)
        #expect(stats.messagesCount == 50_000)
        #expect(stats.hostname == "node-1")
        #expect(stats.osName == "Linux")
        #expect(stats.kernelVersion == "6.1.0")
        #expect(stats.iggyServerVersion == "0.11.0")
        #expect(stats.iggyServerSemver == 11_264)
        #expect(stats.cacheMetrics.count == 1)
        #expect(stats.cacheMetrics[0].hits == 1000)
        #expect(stats.threadsCount == 16)
        #expect(stats.totalDiskSpace == 512_110_190_592)
    }

    @Test func clusterMetadata() throws {
        let metadata = try Responses.clusterMetadata(body("response.cluster_metadata"))
        #expect(metadata.name == "prod-cluster")
        #expect(metadata.nodes.count == 2)
        #expect(metadata.nodes[0].role == .leader)
        #expect(metadata.nodes[0].status == .healthy)
        #expect(metadata.nodes[0].tcpAddress == "10.0.0.1:8090")
        #expect(metadata.nodes[1].role == .follower)
        #expect(metadata.nodes[1].status == .unreachable)
        #expect(metadata.nodes[1].tcpAddress == "[fd00::2]:8091")
        #expect(metadata.nodes[1].endpoints.quic == 0)
    }

    @Test func optionSpecs() throws {
        let specs = try Responses.optionSpecs(body("response.describe_options"))
        #expect(specs.map(\.key) == ["segment_size", "enforce_fsync"])
        #expect(specs[0].kind == .uint64)
        #expect(specs[0].defaultValue == UInt64(1_073_741_824).littleEndianBytes)
        #expect(specs[0].description == "Segment size")
        #expect(specs[1].kind == .bool)
        #expect(specs[1].description.isEmpty)
    }

    @Test func sendConfirmations() throws {
        let response = try Responses.sendConfirmations(body("response.send_messages"))
        #expect(
            response.confirmations == [
                SendConfirmation(streamID: 1, topicID: 2, partitionID: 3, baseOffset: 4),
                SendConfirmation(streamID: 1, topicID: 2, partitionID: 0, baseOffset: 42),
            ])
        #expect(try Responses.sendConfirmations(body("response.send_messages.empty")).confirmations.isEmpty)
        #expect(try Responses.sendConfirmations([]).confirmations.isEmpty)
        #expect(throws: IggyError.self) {
            try Responses.sendConfirmations(golden.bytes("response.send_messages")[...] + [0xFF])
        }
    }

    @Test func polledMessages() throws {
        let polled = try Responses.polledMessages(body("response.poll_messages"), verifyChecksums: true)
        #expect(polled.partitionID == 7)
        #expect(polled.currentOffset == 102)
        #expect(polled.count == 3)
        #expect(polled.messages.count == 3)
        #expect(polled.messages.map(\.id) == [11, 12, 13])
        #expect(polled.messages.map(\.offset) == [100, 101, 102])
        #expect(polled.messages.map(\.timestamp.microseconds) == [5_000, 5_000, 6_000])
        #expect(polled.messages.map(\.originTimestamp.microseconds) == [1_000, 1_010, 2_000])
        #expect(polled.messages.map(\.payloadString) == ["a", "b", "c"])
        #expect(polled.messages[1].userHeaders == ["k": "v"])
        #expect(polled.messages[0].userHeaders == nil)

        let empty = try Responses.polledMessages(body("response.poll_messages.empty"))
        #expect(empty.messages.isEmpty)

        var corrupted = golden.bytes("response.poll_messages")
        corrupted[corrupted.count - 1] ^= 0xFF
        #expect(throws: IggyError.self) {
            try Responses.polledMessages(corrupted[...], verifyChecksums: true)
        }
        var truncated = golden.bytes("response.poll_messages")
        truncated.removeLast()
        #expect(throws: IggyError.self) {
            try Responses.polledMessages(truncated[...])
        }
    }

    @Test func sendBatchRoundTripsThroughThePollDecoder() throws {
        let sent = golden.bytes("request.send_messages")
        let metadataLength = Int(UInt32(littleEndianBytes: sent[0..<4]))
        let batch = sent[(4 + metadataLength)...]
        let messages = try Batch.decodeMessages(batch, expectedCount: 3, verifyChecksums: true)
        #expect(messages.map(\.payloadString) == ["first", "second", ""])
        #expect(messages[1].userHeaders?["attempt"] == .uint32(3))
        #expect(messages[2].id == UInt128Value(low: 3, high: 0xDEAD_BEEF_0000_0000))
    }
}
