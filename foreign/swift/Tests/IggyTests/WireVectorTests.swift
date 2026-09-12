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

/// Every encoder is compared against bytes the Rust protocol crate produced
/// for the same inputs, so the Swift SDK is checked against the reference
/// rather than against itself.
@Suite("Request encoders match the Rust golden vectors")
struct RequestVectorTests {
    let golden = GoldenFixture.shared

    func encode(_ id: Identifier) -> [UInt8] {
        Requests.identifierOnly(id)
    }

    func encodeConsumer(_ consumer: Consumer) -> [UInt8] {
        var writer = ByteWriter()
        consumer.encode(into: &writer)
        return writer.bytes
    }

    func encodePartitioning(_ partitioning: Partitioning) -> [UInt8] {
        var writer = ByteWriter()
        partitioning.encode(into: &writer)
        return writer.bytes
    }

    func encodeStrategy(_ strategy: PollingStrategy) -> [UInt8] {
        var writer = ByteWriter()
        strategy.encode(into: &writer)
        return writer.bytes
    }

    var sampleOptions: ResourceOptions {
        ["enforce_fsync": .explicit(.bool(true)), "segment_size": .explicit(.uint64(1_073_741_824))]
    }

    var samplePermissions: Permissions {
        Permissions(
            global: GlobalPermissions(manageServers: true, readServers: true, readUsers: true, readStreams: true, readTopics: true, pollMessages: true),
            streams: [
                1: StreamPermissions(
                    manageStream: true, readStream: true, readTopics: true, pollMessages: true,
                    topics: [
                        10: TopicPermissions(manageTopic: true, readTopic: true, sendMessages: true),
                        20: TopicPermissions(readTopic: true, pollMessages: true, sendMessages: true),
                    ]),
                2: StreamPermissions(readStream: true, manageTopics: true, sendMessages: true),
            ])
    }

    @Test func primitives() throws {
        #expect(encode(1) == golden.bytes("identifier.numeric.1"))
        #expect(encode("my-stream") == golden.bytes("identifier.named.my-stream"))
        #expect(encodeConsumer(.consumer(42)) == golden.bytes("consumer.numeric.42"))
        #expect(encodeConsumer(.group("my-group")) == golden.bytes("consumer_group.named.my-group"))
        #expect(encodePartitioning(.balanced) == golden.bytes("partitioning.balanced"))
        #expect(encodePartitioning(.partition(7)) == golden.bytes("partitioning.partition_id.7"))
        #expect(encodePartitioning(.messagesKey("user-123")) == golden.bytes("partitioning.messages_key.user-123"))
        #expect(encodeStrategy(.offset(100)) == golden.bytes("polling.offset.100"))
        #expect(encodeStrategy(.timestamp(IggyTimestamp(microseconds: 1_700_000_000_000))) == golden.bytes("polling.timestamp.1700000000000"))
        #expect(encodeStrategy(.first) == golden.bytes("polling.first"))
        #expect(encodeStrategy(.last) == golden.bytes("polling.last"))
        #expect(encodeStrategy(.next) == golden.bytes("polling.next"))
        #expect(try OptionsBlock.encode(sampleOptions) == golden.bytes("options.golden"))
        #expect(samplePermissions.encode() == golden.bytes("permissions.sample"))
        var writer = ByteWriter()
        Requests.versionInfo(into: &writer)
        #expect(writer.bytes == golden.bytes("version_info"))
    }

    @Test func streams() throws {
        #expect(try Requests.createStream(name: "test-stream") == golden.bytes("request.create_stream"))
        #expect(
            try Requests.createStream(name: "test-stream", options: try OptionsBlock.encode(sampleOptions)) == golden.bytes("request.create_stream.options"))
        #expect(encode(5) == golden.bytes("request.delete_stream"))
        #expect(encode("my-stream") == golden.bytes("request.get_stream"))
        #expect(golden.bytes("request.get_streams").isEmpty)
        #expect(encode(1) == golden.bytes("request.purge_stream"))
        #expect(try Requests.updateStream(streamID: "old-name", name: "new-name", options: []) == golden.bytes("request.update_stream"))
    }

    @Test func topics() throws {
        #expect(
            try Requests.createTopic(streamID: 1, partitionsCount: 3, name: "orders", options: try OptionsBlock.encode(sampleOptions))
                == golden.bytes("request.create_topic"))
        #expect(Requests.streamAndTopic(1, 5) == golden.bytes("request.delete_topic"))
        #expect(Requests.streamAndTopic("my-stream", "my-topic") == golden.bytes("request.get_topic"))
        #expect(encode(42) == golden.bytes("request.get_topics"))
        #expect(Requests.streamAndTopic(1, 3) == golden.bytes("request.purge_topic"))
        let options = try OptionsBlock.encode(["message_expiry": .explicit(.uint64(604_800_000_000))])
        #expect(try Requests.updateTopic(streamID: 1, topicID: 2, name: "updated-topic", options: options) == golden.bytes("request.update_topic"))
    }

    @Test func partitionsAndSegments() {
        #expect(Requests.partitions(streamID: 1, topicID: 2, partitionsCount: 5) == golden.bytes("request.create_partitions"))
        #expect(Requests.partitions(streamID: "stream", topicID: "topic", partitionsCount: 2) == golden.bytes("request.delete_partitions"))
        #expect(Requests.deleteSegments(streamID: 1, topicID: 2, partitionID: 3, segmentsCount: 10) == golden.bytes("request.delete_segments"))
    }

    @Test func consumerGroups() throws {
        #expect(try Requests.createConsumerGroup(streamID: 1, topicID: 2, name: "grp") == golden.bytes("request.create_consumer_group"))
        #expect(Requests.consumerGroup(streamID: 1, topicID: 2, groupID: 3) == golden.bytes("request.delete_consumer_group"))
        #expect(Requests.consumerGroup(streamID: "stream-1", topicID: "topic-1", groupID: "group-1") == golden.bytes("request.get_consumer_group"))
        #expect(Requests.streamAndTopic(1, 2) == golden.bytes("request.get_consumer_groups"))
        #expect(Requests.consumerGroup(streamID: 1, topicID: 2, groupID: 3) == golden.bytes("request.join_consumer_group"))
        #expect(Requests.consumerGroup(streamID: 1, topicID: 2, groupID: "g") == golden.bytes("request.leave_consumer_group"))
        #expect(Requests.consumerGroup(streamID: "stream-1", topicID: "topic-1", groupID: "group-1") == golden.bytes("request.sync_consumer_group"))
    }

    @Test func consumerOffsets() {
        #expect(
            Requests.storeConsumerOffset(consumer: .consumer(1), streamID: 10, topicID: 20, partitionID: 5, offset: 12_345, ack: .quorum)
                == golden.bytes("request.store_consumer_offset"))
        #expect(
            Requests.storeConsumerOffset(consumer: .group(3), streamID: 1, topicID: 1, partitionID: nil, offset: UInt64.max, ack: .noAck)
                == golden.bytes("request.store_consumer_offset.no_partition"))
        #expect(
            Requests.getConsumerOffset(consumer: .consumer("my-consumer"), streamID: "stream-1", topicID: "topic-1", partitionID: 0)
                == golden.bytes("request.get_consumer_offset"))
        #expect(
            Requests.deleteConsumerOffset(consumer: .consumer(1), streamID: 10, topicID: 20, partitionID: 5, ack: .quorum)
                == golden.bytes("request.delete_consumer_offset"))
    }

    @Test func messages() throws {
        #expect(
            Requests.pollMessages(consumer: .consumer(1), streamID: 10, topicID: 20, partitionID: 5, strategy: .offset(100), count: 50, autoCommit: true)
                == golden.bytes("request.poll_messages"))
        #expect(
            Requests.pollMessages(consumer: .group(3), streamID: 1, topicID: 1, partitionID: nil, strategy: .first, count: 10, autoCommit: false)
                == golden.bytes("request.poll_messages.group"))
        #expect(Requests.flushUnsavedBuffer(streamID: 1, topicID: 2, partitionID: 3, fsync: true) == golden.bytes("request.flush_unsaved_buffer"))

        let headers: UserHeaders = ["trace-id": "abc", "attempt": .uint32(3)]
        #expect(HeaderTLV.encode(headers) == golden.bytes("user_headers.sample"))
        let messages = [
            Batch.OutgoingMessage(id: 1, originTimestamp: 1_000, payload: Array("first".utf8), userHeaders: nil),
            Batch.OutgoingMessage(id: 2, originTimestamp: 1_500, payload: Array("second".utf8), userHeaders: HeaderTLV.encode(headers)),
            Batch.OutgoingMessage(id: UInt128Value(low: 3, high: 0xDEAD_BEEF_0000_0000), originTimestamp: 1_000, payload: [], userHeaders: nil),
        ]
        let encoded = try Batch.encodeSend(streamID: 1, topicID: 2, partitioning: .partition(0), messages: messages)
        #expect(encoded == golden.bytes("request.send_messages"))
    }

    @Test func users() throws {
        #expect(
            try Requests.createUser(username: "admin", password: "p@ssw0rd", status: .active, permissions: samplePermissions)
                == golden.bytes("request.create_user"))
        #expect(
            try Requests.createUser(username: "user", password: "secret123", status: .inactive, permissions: nil)
                == golden.bytes("request.create_user.no_permissions"))
        #expect(encode("old-user") == golden.bytes("request.delete_user"))
        #expect(encode(42) == golden.bytes("request.get_user"))
        #expect(golden.bytes("request.get_users").isEmpty)
        #expect(try Requests.updateUser(userID: 1, username: "new-name", status: .inactive, options: []) == golden.bytes("request.update_user"))
        #expect(try Requests.updateUser(userID: 5, username: nil, status: nil, options: []) == golden.bytes("request.update_user.none"))
        #expect(Requests.updatePermissions(userID: 1, permissions: samplePermissions) == golden.bytes("request.update_permissions"))
        #expect(Requests.updatePermissions(userID: "admin", permissions: nil) == golden.bytes("request.update_permissions.none"))
        #expect(try Requests.changePassword(userID: 1, currentPassword: "old-pass", newPassword: "new-pass-123") == golden.bytes("request.change_password"))
        #expect(try Requests.loginRegister(username: "iggy", password: "iggy") == golden.bytes("request.login_register"))
        #expect(try Requests.loginRegisterWithPersonalAccessToken(token: "pat-abc123def456") == golden.bytes("request.login_register_with_pat"))
        #expect(golden.bytes("request.logout_user").isEmpty)
    }

    @Test func personalAccessTokens() throws {
        #expect(
            try Requests.createPersonalAccessToken(name: "my-token", expiry: .after(.microseconds(3600)))
                == golden.bytes("request.create_personal_access_token"))
        #expect(try Requests.nameOnly("my-token") == golden.bytes("request.delete_personal_access_token"))
        #expect(golden.bytes("request.get_personal_access_tokens").isEmpty)
    }

    @Test func system() throws {
        #expect(golden.bytes("request.ping").isEmpty)
        #expect(golden.bytes("request.get_stats").isEmpty)
        #expect(golden.bytes("request.get_me").isEmpty)
        #expect(Requests.getClient(clientID: 42) == golden.bytes("request.get_client"))
        #expect(golden.bytes("request.get_clients").isEmpty)
        #expect(golden.bytes("request.get_cluster_metadata").isEmpty)
        #expect(try Requests.getSnapshot(compression: .deflated, types: [.filesystemOverview, .serverLogs]) == golden.bytes("request.get_snapshot"))
        #expect(Requests.describeOptions(scope: .topic) == golden.bytes("request.describe_options"))
    }

    @Test func topicOptions() throws {
        let create = TopicCreateOptions(
            partitionsCount: 3, compressionAlgorithm: .gzip, messageExpiry: .after(.seconds(604_800)),
            maxTopicSize: .bytes(1_073_741_824), segmentSizeBytes: 1_048_576, enforceFsync: true,
            messagesRequiredToSave: 7, sizeOfMessagesRequiredToSaveBytes: 4096, preallocateSegments: false)
        #expect(try create.encode() == golden.bytes("options.topic_create.full"))
        let update = TopicUpdateOptions(compressionAlgorithm: CompressionAlgorithm.none, messageExpiry: .never, maxTopicSize: .unlimited)
        #expect(try update.encode() == golden.bytes("options.topic_update.full"))
        let raw = TopicCreateOptions(raw: ["enforce_fsync": "true"])
        #expect(try raw.encode() == golden.bytes("options.topic_create.raw"))
    }

    @Test func errorCodesMatchTheServerTable() {
        let known = Dictionary(uniqueKeysWithValues: IggyErrorCode.allCases.map { ($0.rawValue, $0.name) })
        #expect(known.count == golden.errors.count)
        for entry in golden.errors {
            #expect(known[entry.code] == entry.name, "code \(entry.code)")
        }
    }

    @Test func protocolVersionMatchesTheCrate() {
        #expect(ProtocolVersion.current.packed == golden.protocolVersion)
        #expect(ProtocolVersion.current.description == "0.11.0")
    }
}

@Suite("Consensus headers match the Rust golden vectors")
struct VSRHeaderVectorTests {
    let golden = GoldenFixture.shared
    let clientID = UInt128Value(low: 0xFEDC_BA98_7654_3210, high: 0x0123_4567_89AB_CDEF)

    func header(_ request: VSRFrame.EncodedRequest) -> [UInt8] {
        Array(request.bytes[..<VSRFrame.headerSize])
    }

    @Test func registerHeader() throws {
        var session = ConsensusSession(clientID: clientID)
        let request = try VSRFrame.encodeRequest(session: &session, code: CommandCode.loginRegister.rawValue, payload: [1, 2, 3])
        #expect(header(request) == golden.bytes("vsr.request.register"))
        #expect(Array(request.bytes[VSRFrame.headerSize...]) == [1, 2, 3])
        #expect(request.requestID == 0)
        #expect(request.operation == .register)
    }

    @Test func pingHeaderBeforeAndAfterBinding() throws {
        var session = ConsensusSession(clientID: clientID)
        let unbound = try VSRFrame.encodeRequest(session: &session, code: CommandCode.ping.rawValue, payload: [])
        #expect(header(unbound) == golden.bytes("vsr.request.ping"))
        _ = session.beginRegister()
        try session.bind(99)
        _ = try session.nextRequestID()
        _ = try session.nextRequestID()
        let bound = try VSRFrame.encodeRequest(session: &session, code: CommandCode.ping.rawValue, payload: [])
        #expect(header(bound) == golden.bytes("vsr.request.ping.bound"))
    }

    @Test func metadataHeaderStampsTheChecksum() throws {
        var session = ConsensusSession(clientID: clientID)
        _ = session.beginRegister()
        try session.bind(99)
        let payload = golden.bytes("payload.create_stream")
        #expect(payload == (try Requests.createStream(name: "stream")))
        let request = try VSRFrame.encodeRequest(session: &session, code: CommandCode.createStream.rawValue, payload: payload)
        #expect(header(request) == golden.bytes("vsr.request.create_stream"))
        #expect(request.requestID == 1)
        #expect(VSRFrame.stampedRequestID(request.bytes) == 1)
    }

    @Test func partitionHeaderLeavesTheChecksumZero() throws {
        var session = ConsensusSession(clientID: clientID)
        _ = session.beginRegister()
        try session.bind(99)
        _ = try session.nextRequestID()
        let request = try VSRFrame.encodeRequest(session: &session, code: CommandCode.sendMessages.rawValue, payload: [9, 9, 9, 9])
        #expect(header(request) == golden.bytes("vsr.request.send_messages"))
        #expect(request.requestID == 2)
    }

    @Test func logoutAndUnknownCodes() throws {
        var session = ConsensusSession(clientID: clientID)
        _ = session.beginRegister()
        try session.bind(99)
        for _ in 0..<3 {
            _ = try session.nextRequestID()
        }
        let logout = try VSRFrame.encodeRequest(session: &session, code: CommandCode.logoutUser.rawValue, payload: [])
        #expect(header(logout) == golden.bytes("vsr.request.logout"))
        #expect(logout.operation == .logout)
        let unknown = try VSRFrame.encodeRequest(session: &session, code: 60_000, payload: [])
        #expect(header(unknown) == golden.bytes("vsr.request.unknown_code"))
        #expect(unknown.operation == .nonReplicated)
    }

    @Test func replyHeaders() throws {
        let ok = golden.bytes("vsr.reply.ok")[...]
        #expect(VSRFrame.peekCommand(ok) == .reply)
        #expect(VSRFrame.readSize(ok) == UInt32(VSRFrame.headerSize + 12))
        #expect(VSRFrame.readReplyRequestID(ok) == 7)
        #expect(VSRFrame.readReplyStatus(ok) == 0)
        #expect(VSRFrame.readReplyOperation(ok) == .createStream)

        let denied = golden.bytes("vsr.reply.denied")[...]
        #expect(throws: IggyError(.unauthorized)) {
            try VSRFrame.decodeReply(header: denied, body: [])
        }
    }

    @Test func evictionHeaders() {
        let stale = golden.bytes("vsr.eviction.stale_client")[...]
        #expect(VSRFrame.peekCommand(stale) == .eviction)
        #expect(VSRFrame.readEviction(stale).error == IggyError(.staleClient))

        let incompatible = golden.bytes("vsr.eviction.incompatible_protocol")[...]
        let eviction = VSRFrame.readEviction(incompatible)
        #expect(eviction.reason == .incompatibleProtocol)
        #expect(eviction.serverProtocolVersion == golden.protocolVersion)
        #expect(eviction.serverProtocolVersionMin == golden.protocolVersionMin)
        #expect(eviction.error.code == .incompatibleProtocolVersion)
        #expect(eviction.error.context == "client 0.11.0, server accepts [0.11.0, 0.11.0]")
    }
}
