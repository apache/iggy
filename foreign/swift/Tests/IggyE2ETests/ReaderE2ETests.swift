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
    /// The server stamps a whole batch with one timestamp, so this pause
    /// gives the second batch of ``sendTwoBatches(_:_:)`` a later one.
    private static let batchGap: Duration = .milliseconds(20)

    private static func storedOffset(
        _ client: IggyClient, _ consumer: Consumer, _ scratch: TestSession.ScratchTopic, partition: UInt32 = 0
    ) async throws -> UInt64? {
        try await client.getConsumerOffset(consumer: consumer, streamID: scratch.stream, topicID: scratch.topic, partitionID: partition)?.storedOffset
    }

    /// Sends offsets 0 to 4 and, after ``batchGap``, offsets 5 to 9. Returns
    /// the timestamp that the server gave offset 5.
    private static func sendTwoBatches(_ client: IggyClient, _ scratch: TestSession.ScratchTopic) async throws -> IggyTimestamp {
        try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(5))
        try await Task.sleep(for: batchGap)
        try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(5))
        let polled = try await client.pollMessages(
            streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, strategy: .first, options: PollOptions(count: 10))
        try #require(polled.messages.map(\.offset) == Array(0..<10))
        let timestamps = polled.messages.map(\.timestamp)
        // A timestamp position reads from the first message stamped at or after it.
        try #require(timestamps[4] < timestamps[5])
        return timestamps[5]
    }

    /// Each case reads under a consumer of its own, so no offset that another
    /// case stored moves its start.
    private static func newConsumer() throws -> Consumer {
        .consumer(try Identifier(named: E2EEnvironment.uniqueName("swift-e2e-start")))
    }

    private static func reader(
        _ client: IggyClient, _ scratch: TestSession.ScratchTopic, _ consumer: Consumer, start: PollingStrategy, commit: CommitPolicy
    ) async throws -> PartitionReader {
        var options = Self.options
        options.commit = commit
        return try await client.reader(
            streamID: scratch.stream, topicID: scratch.topic, consumer: consumer,
            configuration: ReaderConfiguration(partitions: [0], start: start, options: options))
    }

    /// The offsets of the first `count` messages that `reader` delivers. A
    /// reader waits for messages without an end, so the read fails the test
    /// at `timeout` instead.
    private static func offsets(_ reader: PartitionReader, _ count: Int, timeout: Duration = .seconds(30)) async throws -> [UInt64] {
        try await withThrowingTaskGroup(of: [UInt64]?.self) { group in
            group.addTask {
                var offsets: [UInt64] = []
                while offsets.count < count, let batch = try await reader.next() {
                    offsets += batch.messages.map(\.offset)
                }
                return Array(offsets.prefix(count))
            }
            group.addTask {
                try await Task.sleep(for: timeout)
                return nil
            }
            defer { group.cancelAll() }
            guard let offsets = try await group.next() ?? nil else {
                Issue.record("read fewer than \(count) messages within \(timeout)")
                return []
            }
            return offsets
        }
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

    /// The server fetches only from an offset or after the stored offset, so
    /// the reader reads a timestamp or `last` position with a poll. A
    /// timestamp start and a timestamp seek then read from the first message
    /// stamped at or after the timestamp, and a `last` start from the last
    /// `count` messages.
    @Test(arguments: [CommitPolicy.onRead, .manual])
    func timestampAndLastPositionsReadFromTheMessageTheyName(commit: CommitPolicy) async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let secondBatch = try await Self.sendTwoBatches(client, scratch)

            let fromTimestamp = try await Self.reader(client, scratch, try Self.newConsumer(), start: .timestamp(secondBatch), commit: commit)
            #expect(try await Self.offsets(fromTimestamp, 1) == [5])
            try await fromTimestamp.close()

            let fromLast = try await Self.reader(client, scratch, try Self.newConsumer(), start: .last, commit: commit)
            #expect(try await Self.offsets(fromLast, 3) == [7, 8, 9])
            try await fromLast.close()

            let seeking = try await Self.reader(client, scratch, try Self.newConsumer(), start: .next, commit: commit)
            #expect(try await Self.offsets(seeking, 1) == [0])
            try await seeking.seek(partitionID: 0, to: .timestamp(secondBatch))
            #expect(try await Self.offsets(seeking, 1) == [5])
            try await seeking.close()
        }
    }

    /// With `onRead` a start applies only to a partition without a stored
    /// offset, and the close leaves the offset at the last delivered message,
    /// so the next reader of the consumer goes on after it.
    @Test func aStoredOffsetWinsOverATimestampStart() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let secondBatch = try await Self.sendTwoBatches(client, scratch)
            let consumer = try Self.newConsumer()

            let first = try await Self.reader(client, scratch, consumer, start: .timestamp(secondBatch), commit: .onRead)
            #expect(try await Self.offsets(first, 5) == [5, 6, 7, 8, 9])
            try await first.close()
            #expect(try await Self.storedOffset(client, consumer, scratch) == 9)

            try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(1))
            let second = try await Self.reader(client, scratch, consumer, start: .timestamp(secondBatch), commit: .onRead)
            #expect(try await Self.offsets(second, 1) == [10])
            try await second.close()
        }
    }
}
