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

@Suite("Partition reader", requiresServer)
struct ReaderE2ETests {
    private static let options = PollOptions(count: 3, maxWait: .seconds(1))

    private static func storedOffset(
        _ client: IggyClient, _ consumer: Consumer, _ scratch: TestSession.ScratchTopic, partition: UInt32 = 0
    ) async throws -> UInt64? {
        try await client.getConsumerOffset(consumer: consumer, streamID: scratch.stream, topicID: scratch.topic, partitionID: partition)?.storedOffset
    }

    /// With the manual policy only a commit stores, and the close stores
    /// nothing more.
    @Test func aManualReaderStoresOnlyWhatItCommits() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(3))
            let consumer = Consumer.consumer(try Identifier(named: E2EEnvironment.uniqueName("swift-e2e-manual")))
            let reader = try await client.reader(
                streamID: scratch.stream, topicID: scratch.topic, consumer: consumer,
                configuration: ReaderConfiguration(partitions: [0], options: Self.options))
            let batch = try #require(try await reader.next())
            #expect(batch.messages.map(\.offset) == [0, 1, 2])
            #expect(try await Self.storedOffset(client, consumer, scratch) == nil)
            try await reader.commit(try #require(batch.checkpoint(at: 1)))
            #expect(try await Self.storedOffset(client, consumer, scratch) == 1)
            try await reader.close()
            #expect(try await Self.storedOffset(client, consumer, scratch) == 1)
        }
    }

    /// A seek starts the partition again where it says, and a continuation
    /// of another partition is refused.
    @Test func aSeekReadsAgainFromItsStart() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 2)
            try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(3))
            let reader = try await client.reader(
                streamID: scratch.stream, topicID: scratch.topic, consumer: .consumer(try Identifier(named: E2EEnvironment.uniqueName("swift-e2e-seek"))),
                configuration: ReaderConfiguration(partitions: [0, 1], start: .first, options: Self.options))
            let batch = try #require(try await reader.next())
            #expect(batch.messages.map(\.offset) == [0, 1, 2])
            await expectIggyError(.invalidCommand) {
                try await reader.seek(partitionID: 1, to: batch.continuation)
            }
            try await reader.seek(partitionID: 0, to: .offset(1))
            #expect(try #require(try await reader.next()).messages.map(\.offset) == [1, 2])
            try await reader.close()
        }
    }

    /// With the onRead policy the server stores each read. A seek to an
    /// offset stores the offset before it first, so the reads that follow go
    /// on from there.
    @Test func anOnReadSeekStoresTheOffsetBeforeItsStart() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(3))
            let consumer = Consumer.consumer(try Identifier(named: E2EEnvironment.uniqueName("swift-e2e-on-read")))
            var options = Self.options
            options.commit = .onRead
            let reader = try await client.reader(
                streamID: scratch.stream, topicID: scratch.topic, consumer: consumer, configuration: ReaderConfiguration(partitions: [0], options: options))
            #expect(try #require(try await reader.next()).messages.map(\.offset) == [0, 1, 2])
            #expect(try await Self.storedOffset(client, consumer, scratch) == 2)
            try await reader.seek(partitionID: 0, to: .offset(1))
            #expect(try await Self.storedOffset(client, consumer, scratch) == 0)
            #expect(try #require(try await reader.next()).messages.map(\.offset) == [1, 2])
            #expect(try await Self.storedOffset(client, consumer, scratch) == 2)
            try await reader.close()
        }
    }

    /// A first message larger than `maxBytes` comes alone in one reply, to a
    /// single poll and to a reader alike.
    @Test func aMessageLargerThanMaxBytesComesAlone() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let large = try IggyMessage(payload: Array(repeating: UInt8(7), count: 4096))
            try await client.sendBatch(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: [large, try IggyMessage("small")])
            let options = PollOptions(count: 10, maxBytes: 1024)
            let polled = try await client.pollMessages(streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, strategy: .first, options: options)
            #expect(polled.messages.map(\.payload.count) == [4096])
            let rest = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, strategy: polled.continuation, options: options)
            #expect(rest.messages.map(\.payloadString) == ["small"])

            let reader = try await client.reader(
                streamID: scratch.stream, topicID: scratch.topic, consumer: .consumer(try Identifier(named: E2EEnvironment.uniqueName("swift-e2e-large"))),
                configuration: ReaderConfiguration(partitions: [0], start: .first, options: options))
            var offsets: [UInt64] = []
            while offsets.count < 2 {
                offsets += try #require(try await reader.next()).messages.map(\.offset)
            }
            #expect(offsets == [0, 1])
            try await reader.close()
        }
    }
}
