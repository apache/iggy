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

/// Every model encoder is compared against bytes the Rust protocol crate
/// produced for the same values, so the Swift SDK is checked against the
/// reference rather than against itself.
@Suite("Model encoders match the Rust golden vectors")
struct ModelVectorTests {
    let golden = GoldenFixture.shared

    func encode(_ id: Identifier) throws -> [UInt8] {
        var writer = ByteWriter()
        try id.encode(into: &writer)
        #expect(writer.bytes.count == id.encodedSize)
        return writer.bytes
    }

    func encode(_ consumer: Consumer) throws -> [UInt8] {
        var writer = ByteWriter()
        try consumer.encode(into: &writer)
        return writer.bytes
    }

    func encode(_ partitioning: Partitioning) throws -> [UInt8] {
        var writer = ByteWriter()
        try partitioning.encode(into: &writer)
        #expect(writer.bytes.count == partitioning.encodedSize)
        return writer.bytes
    }

    func encode(_ strategy: PollingStrategy) -> [UInt8] {
        var writer = ByteWriter()
        strategy.encode(into: &writer)
        return writer.bytes
    }

    @Test func partitionContext() throws {
        let context = PartitionContext(incarnation: 17, ownerGeneration: 9, metadataOp: 52)
        var writer = ByteWriter()
        context.encode(into: &writer)
        #expect(writer.bytes == golden.bytes("partition.context"))
        var reader = ByteReader(writer.bytes)
        #expect(try PartitionContext.decode(from: &reader) == context)
        #expect(reader.isAtEnd)
        for length in 0..<writer.bytes.count {
            var truncated = ByteReader(Array(writer.bytes.prefix(length)))
            #expect(throws: WireError.self) { try PartitionContext.decode(from: &truncated) }
        }
    }

    @Test func identifiers() throws {
        #expect(try encode(1) == golden.bytes("identifier.numeric.1"))
        #expect(try encode("my-stream") == golden.bytes("identifier.named.my-stream"))
    }

    @Test func consumers() throws {
        #expect(try encode(.consumer(42)) == golden.bytes("consumer.numeric.42"))
        #expect(try encode(.group("my-group")) == golden.bytes("consumer_group.named.my-group"))
    }

    @Test func partitioning() throws {
        #expect(try encode(.balanced) == golden.bytes("partitioning.balanced"))
        #expect(try encode(.partition(7)) == golden.bytes("partitioning.partition_id.7"))
        #expect(try encode(.messagesKey("user-123")) == golden.bytes("partitioning.messages_key.user-123"))
    }

    @Test func pollingStrategies() {
        #expect(encode(.offset(100)) == golden.bytes("polling.offset.100"))
        #expect(encode(.timestamp(IggyTimestamp(microseconds: 1_700_000_000_000))) == golden.bytes("polling.timestamp.1700000000000"))
        #expect(encode(.first) == golden.bytes("polling.first"))
        #expect(encode(.last) == golden.bytes("polling.last"))
        #expect(encode(.next) == golden.bytes("polling.next"))
    }

    @Test func userHeaders() {
        let headers: UserHeaders = ["trace-id": "abc", "attempt": .uint32(3)]
        #expect(HeaderTLV.encode(headers) == golden.bytes("user_headers.sample"))
    }

    @Test func resourceOptions() throws {
        let options: ResourceOptions = ["durability": .explicit("persisted"), "segment_size": .explicit(.uint64(1_073_741_824))]
        #expect(try OptionsBlock.encode(options) == golden.bytes("options.golden"))
    }

    @Test func topicOptions() throws {
        let create = TopicCreateOptions(
            partitionsCount: 3, compressionAlgorithm: .gzip, messageExpiry: .after(.seconds(604_800)),
            maxTopicSize: .bytes(1_073_741_824), segmentSizeBytes: 1_048_576, durability: .persisted,
            consumerOffsetDurability: .replicated, messagesRequiredToSave: 7, sizeOfMessagesRequiredToSaveBytes: 4096,
            preallocateSegments: false)
        #expect(try create.encode() == golden.bytes("options.topic_create.full"))
        let update = TopicUpdateOptions(compressionAlgorithm: CompressionAlgorithm.none, messageExpiry: .never, maxTopicSize: .unlimited)
        #expect(try update.encode() == golden.bytes("options.topic_update.full"))
        let raw = TopicCreateOptions(raw: ["durability": "persisted"])
        #expect(try raw.encode() == golden.bytes("options.topic_create.raw"))
    }

    @Test func permissions() {
        let permissions = Permissions(
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
        #expect(permissions.encode() == golden.bytes("permissions.sample"))
    }
}
