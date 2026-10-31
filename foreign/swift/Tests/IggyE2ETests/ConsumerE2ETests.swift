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

/// Reads `count` messages, failing the test at `timeout`: an empty poll is
/// not the end of a consumer, so reading alone never stops.
private func read(_ consumer: IggyConsumer, _ count: Int, timeout: Duration = .seconds(30)) async throws -> [ReceivedMessage] {
    try await withThrowingTaskGroup(of: [ReceivedMessage]?.self) { group in
        group.addTask {
            var received: [ReceivedMessage] = []
            while received.count < count, let message = try await consumer.next() {
                received.append(message)
            }
            return received
        }
        group.addTask {
            try await Task.sleep(for: timeout)
            return nil
        }
        defer { group.cancelAll() }
        guard let received = try await group.next() ?? nil else {
            Issue.record("read fewer than \(count) messages within \(timeout)")
            return []
        }
        return received
    }
}

@Suite("Consumer", requiresServer)
struct ConsumerE2ETests {
    /// Each policy stores the position after the last message handed over
    /// by the shutdown: the server with the read and the shutdown moving it
    /// back over what was not handed over, or the consumer flushing what it
    /// counted. A second consumer of the same name resumes after it.
    @Test(arguments: [CommitPolicy.onRead, .auto(.default), .auto(AutoCommit(interval: .seconds(60), after: .delivery))])
    func aConsumerResumesAfterTheLastMessageHandedOver(commit: CommitPolicy) async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let name = E2EEnvironment.uniqueName("swift-e2e-reader")
            var batch = try E2EEnvironment.messages(5)
            try await client.sendMessages(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: &batch)
            let configuration = ConsumerConfiguration(pollOptions: PollOptions(count: 5, maxWait: .seconds(1), commit: commit))

            let first = try client.consumer(name: name, streamID: scratch.stream, topicID: scratch.topic, partitions: [0], configuration: configuration)
            try await first.initialize()
            let received = try await read(first, 3)
            #expect(received.map(\.message.payloadString) == (0..<3).map { "message \($0)" })
            #expect(received.map(\.partitionID) == Array(repeating: 0, count: 3))
            #expect(received.last?.currentOffset == 4)
            await first.shutdown()
            let stored = try await client.getConsumerOffset(
                consumer: .consumer(try Identifier(named: name)), streamID: scratch.stream, topicID: scratch.topic, partitionID: 0)
            #expect(stored?.storedOffset == 2, "\(commit)")

            let second = try client.consumer(name: name, streamID: scratch.stream, topicID: scratch.topic, partitions: [0], configuration: configuration)
            try await second.initialize()
            #expect(try await read(second, 2).map(\.message.offset) == [3, 4], "\(commit)")
            await second.shutdown()
        }
    }

    /// A member joins its group, reads the partitions the server assigns,
    /// and leaves the group on shutdown.
    @Test func aGroupMemberReadsEveryAssignedPartition() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 2)
            for partition: UInt32 in [0, 1] {
                var batch = try E2EEnvironment.messages(2, prefix: "partition \(partition)")
                try await client.sendMessages(streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(partition), messages: &batch)
            }
            let name = E2EEnvironment.uniqueName("swift-e2e-group")
            let member = try client.consumerGroup(
                name: name, streamID: scratch.stream, topicID: scratch.topic, configuration: ConsumerConfiguration(pollOptions: .wait(.seconds(1))))
            try await member.initialize()
            let received = try await read(member, 4)
            #expect(Set(received.map(\.partitionID)) == [0, 1])
            #expect(Set(received.map(\.message.payloadString)) == ["partition 0 0", "partition 0 1", "partition 1 0", "partition 1 1"])
            await member.shutdown()
            let groupID = try Identifier(named: name)
            // The server applies the leave asynchronously.
            #expect(
                try await E2EEnvironment.eventually {
                    try await client.getConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: groupID)?.membersCount == 0
                })
        }
    }
}
