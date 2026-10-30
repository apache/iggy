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

@Suite("Deferred polls", requiresServer)
struct DeferredPollE2ETests {
    private static let parking: Duration = .seconds(10)
    /// Far below ``parking``, so only a read that the message woke returns
    /// in time.
    private static let prompt: Duration = .seconds(2)

    /// One member owns all three partitions and its reader waits on all of
    /// them, one read per primary. Before each read another partition gets
    /// a message, so a read that missed it would wait out its ten seconds.
    @Test func aReaderReadsThePartitionThatGotAMessage() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 3)
            let group = try Identifier(named: E2EEnvironment.uniqueName("swift-e2e-ready"))
            let reader = try await client.reader(
                streamID: scratch.stream, topicID: scratch.topic, consumer: .group(group),
                configuration: ReaderConfiguration(options: PollOptions(count: 10, maxWait: Self.parking, commit: .onRead)))
            #expect(
                try await E2EEnvironment.eventually {
                    try await client.getConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: group)?.members.first?.partitions.count == 3
                })
            for (round, partition) in [UInt32(2), 0, 1].enumerated() {
                try await client.sendBatch(
                    streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(partition), messages: [try IggyMessage("round \(round)")])
                let started = ContinuousClock.now
                let batch = try #require(try await reader.next())
                let elapsed = ContinuousClock.now - started
                #expect(elapsed < Self.prompt, "round \(round) took \(elapsed)")
                #expect(batch.partitionID == partition, "round \(round)")
                #expect(batch.messages.map(\.payloadString) == ["round \(round)"], "round \(round)")
            }
            try await reader.close()
        }
    }

    /// A read of many partitions takes them in order while its count leaves
    /// room, so a partition with a backlog would hide the partitions after
    /// it. The reader moves the partitions that a read served to the end of
    /// the next one, so a message on another partition gets through before
    /// the backlog drains, whichever primaries hold the partitions.
    @Test func aBacklogDoesNotHideAMessageOnAnotherPartition() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 2)
            let backlog = try (0..<1000).map { try IggyMessage("backlog \($0)") }
            try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: backlog)
            try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(1), messages: ["late"])
            let reader = try await client.reader(
                streamID: scratch.stream, topicID: scratch.topic, consumer: .consumer(try Identifier(named: E2EEnvironment.uniqueName("swift-e2e-backlog"))),
                configuration: ReaderConfiguration(partitions: [0, 1], start: .first, options: PollOptions(count: 1, maxWait: .seconds(1))))
            var payloads: [String] = []
            for _ in 0..<4 {
                payloads += try #require(try await reader.next()).messages.map(\.payloadString)
            }
            #expect(payloads.contains("late"), "\(payloads)")
            try await reader.close()
        }
    }

    /// A consumer parked on an idle partition hands a message over as soon
    /// as it is sent, not once its wait ends.
    @Test func aConsumerHandsOverAMessageSentAfterAnIdlePeriodAtOnce() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let consumer = try client.consumer(
                name: E2EEnvironment.uniqueName("swift-e2e-idle"), streamID: scratch.stream, topicID: scratch.topic, partitions: [0],
                configuration: ConsumerConfiguration(pollOptions: .wait(Self.parking)))
            try await consumer.initialize()
            let reading = Task { try await consumer.next() }
            try await Task.sleep(for: .seconds(3))
            let sent = ContinuousClock.now
            try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: ["after idle"])
            let received = try await reading.value
            let elapsed = ContinuousClock.now - sent
            #expect(elapsed < Self.prompt, "took \(elapsed)")
            #expect(received?.message.payloadString == "after idle")
            await consumer.shutdown()
        }
    }

    /// The server drops a cancelled poll without a reply, so the next poll
    /// on the same client gets its own answer.
    @Test func aCancelledParkedPollLeavesTheClientInStep() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let poll = { @Sendable in
                try await client.pollMessages(
                    streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, strategy: .next,
                    options: PollOptions(count: 10, maxWait: Self.parking, commit: .onRead))
            }
            let parked = Task { try await poll() }
            try await Task.sleep(for: .milliseconds(500))
            parked.cancel()
            await #expect(throws: CancellationError.self) { try await parked.value }
            try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: ["after cancel"])
            let started = ContinuousClock.now
            let polled = try await poll()
            let elapsed = ContinuousClock.now - started
            #expect(elapsed < Self.prompt, "took \(elapsed)")
            #expect(polled.messages.map(\.payloadString) == ["after cancel"])
        }
    }

    /// The poll asks for 2 messages and finds 1, so it stays parked until the
    /// cancellation, which ends it on the server without a commit. A poll
    /// that the server still held would expire after `maxWait` and commit the
    /// offset of the message it found.
    @Test func aCancelledCommittingPollStoresNoOffset() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            try await client.sendBatch(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: ["unseen"])
            let options = PollOptions(count: 10, maxWait: .seconds(1), minCount: 2, commit: .onRead)
            let parked = Task {
                try await client.pollMessages(streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, strategy: .next, options: options)
            }
            try await Task.sleep(for: .milliseconds(200))
            parked.cancel()
            await #expect(throws: CancellationError.self) { try await parked.value }
            try await Task.sleep(for: options.maxWait + .seconds(1))
            let polled = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, strategy: .next, options: PollOptions(count: 10))
            #expect(polled.messages.map(\.payloadString) == ["unseen"])
        }
    }
}
