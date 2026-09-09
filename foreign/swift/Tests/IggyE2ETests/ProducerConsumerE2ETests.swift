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

@Suite("Producer and consumer", requiresServer)
struct ProducerConsumerE2ETests {
    @Test func directProducerCreatesResourcesAndConsumerReadsThem() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let name = E2EEnvironment.uniqueName("swift-pc")
            let stream = try Identifier(named: name)
            let topic = try Identifier(named: name)
            let producer = client.producer(
                stream: stream, topic: topic, configuration: ProducerConfiguration(partitioning: .partition(0), topicPartitionsCount: 2))
            try await producer.initialize()
            await session.track(stream: stream)
            #expect(try await client.getTopic(streamID: stream, topicID: topic)?.partitionsCount == 2)

            let response = try await producer.send(try E2EEnvironment.messages(10))
            #expect(response.confirmations.map(\.baseOffset) == [0])
            _ = try await producer.send(try IggyMessage("single"))

            let consumer = try client.consumer(
                name: "reader", stream: stream, topic: topic, partition: 0,
                configuration: ConsumerConfiguration(pollingStrategy: .first, batchLength: 4, pollInterval: .milliseconds(10), autoCommit: .disabled))
            try await consumer.initialize()
            var received: [ReceivedMessage] = []
            for try await message in consumer {
                received.append(message)
                if received.count == 11 {
                    break
                }
            }
            #expect(received.map(\.message.offset) == Array(0..<11))
            #expect(received.last?.message.payloadString == "single")
            #expect(received.allSatisfy { $0.currentOffset == 10 && $0.partitionID == 0 })
            #expect(await consumer.lastConsumedOffset(partitionID: 0) == 10)
            await consumer.shutdown()
            await producer.shutdown()
            #expect(try await consumer.next() == nil)
        }
    }

    @Test func backgroundProducerBatchesAndFlushesOnShutdown() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let options = BackgroundSendOptions(batchLength: 50, lingerTime: .milliseconds(20))
            let producer = client.producer(
                stream: scratch.stream, topic: scratch.topic, configuration: ProducerConfiguration(partitioning: .partition(0), mode: .background(options)))
            try await producer.initialize()
            for index in 0..<120 {
                let response = try await producer.send(try IggyMessage("bg \(index)"))
                #expect(response.confirmations.isEmpty)
            }
            await producer.shutdown()
            let polled = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, consumer: .default, strategy: .first, count: 200, autoCommit: false)
            #expect(polled.messages.map(\.payloadString) == (0..<120).map { "bg \($0)" })
            await expectIggyError(.producerClosed) { try await producer.send(try IggyMessage("late")) }
        }
    }

    @Test func consumerGroupSharesPartitionsAndCommitsOffsets() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 3)
            for partition in 0..<3 {
                _ = try await client.sendMessages(
                    streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(UInt32(partition)),
                    messages: try E2EEnvironment.messages(5, prefix: "p\(partition)"))
            }
            let groupName = "swift-e2e-workers"
            let consumer = try client.consumerGroup(
                name: groupName, stream: scratch.stream, topic: scratch.topic,
                configuration: ConsumerConfiguration(
                    pollingStrategy: .next, batchLength: 2, pollInterval: .milliseconds(10), autoCommit: .when(.consumingAllMessages)))
            try await consumer.initialize()
            let group = try #require(try await client.getConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: Identifier(named: groupName)))
            #expect(group.membersCount == 1)

            var byPartition: [UInt32: [UInt64]] = [:]
            var seen = 0
            for try await received in consumer {
                byPartition[received.partitionID, default: []].append(received.message.offset)
                seen += 1
                if seen == 15 {
                    break
                }
            }
            #expect(byPartition.keys.sorted() == [0, 1, 2])
            #expect(byPartition.values.allSatisfy { $0 == [0, 1, 2, 3, 4] })
            await consumer.shutdown()
            for partition in UInt32(0)..<3 {
                let stored = try await client.getConsumerOffset(
                    consumer: .group(Identifier(named: groupName)), streamID: scratch.stream, topicID: scratch.topic, partitionID: partition)
                #expect(stored?.storedOffset == 4, "partition \(partition)")
            }
            #expect(
                try await client.getConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, groupID: Identifier(named: groupName))?.membersCount == 0)

            // A second member resumes after the stored offsets.
            _ = try await client.sendMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(1), messages: try E2EEnvironment.messages(1, prefix: "late"))
            let resumed = try client.consumerGroup(
                name: groupName, stream: scratch.stream, topic: scratch.topic,
                configuration: ConsumerConfiguration(pollInterval: .milliseconds(10), autoCommit: .disabled))
            try await resumed.initialize()
            let late = try #require(try await resumed.next())
            #expect(late.partitionID == 1)
            #expect(late.message.offset == 5)
            await resumed.shutdown()
        }
    }

    @Test func autoCommitOnPollAndManualOffsets() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            _ = try await client.sendMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(6))
            let consumer = try client.consumer(
                name: "committer", stream: scratch.stream, topic: scratch.topic, partition: 0,
                configuration: ConsumerConfiguration(batchLength: 4, pollInterval: .milliseconds(10), autoCommit: .when(.pollingMessages)))
            try await consumer.initialize()
            _ = try await consumer.next()
            let consumerID = Consumer.consumer("committer")
            // The poll committed the whole first batch.
            #expect(
                try await client.getConsumerOffset(consumer: consumerID, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0)?.storedOffset == 3)
            _ = try await consumer.next()
            await consumer.shutdown()
            // Shutdown moves the server back to the last message handed over.
            #expect(
                try await client.getConsumerOffset(consumer: consumerID, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0)?.storedOffset == 1)

            let manual = try client.consumer(
                name: "committer", stream: scratch.stream, topic: scratch.topic, partition: 0,
                configuration: ConsumerConfiguration(pollInterval: .milliseconds(10), autoCommit: .disabled))
            try await manual.initialize()
            let next = try #require(try await manual.next())
            #expect(next.message.offset == 2)
            try await manual.storeOffset(next.message.offset)
            #expect(
                try await client.getConsumerOffset(consumer: consumerID, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0)?.storedOffset == 2)
            try await manual.deleteOffset()
            #expect(try await client.getConsumerOffset(consumer: consumerID, streamID: scratch.stream, topicID: scratch.topic, partitionID: 0) == nil)
            await manual.shutdown()
        }
    }

    @Test func consumerWaitsForATopicTheProducerCreates() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let name = E2EEnvironment.uniqueName("swift-late")
            let stream = try Identifier(named: name)
            let topic = try Identifier(named: name)
            let consumer = try client.consumer(
                name: "early", stream: stream, topic: topic, partition: 0,
                configuration: ConsumerConfiguration(
                    pollingStrategy: .first, pollInterval: .milliseconds(10), autoCommit: .disabled, initRetries: 20, initRetryInterval: .milliseconds(50)))
            let initialization = Task { try await consumer.initialize() }
            try await Task.sleep(for: .milliseconds(120))
            let producer = client.producer(stream: stream, topic: topic)
            try await producer.initialize()
            await session.track(stream: stream)
            try await initialization.value
            _ = try await producer.send(try IggyMessage("first"))
            #expect(try await consumer.next()?.message.payloadString == "first")
            await consumer.shutdown()
            await producer.shutdown()
        }
    }

    @Test func consumerSurvivesAReconnect() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            _ = try await client.sendMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitioning: .partition(0), messages: try E2EEnvironment.messages(2))
            let consumer = try client.consumer(
                name: "survivor", stream: scratch.stream, topic: scratch.topic, partition: 0,
                configuration: ConsumerConfiguration(
                    pollingStrategy: .first, batchLength: 1, pollInterval: .milliseconds(10), autoCommit: .disabled, pollingRetryInterval: .milliseconds(20)))
            try await consumer.initialize()
            #expect(try await consumer.next()?.message.offset == 0)
            // A sign-out and sign-in on the same client is the lifecycle a
            // reconnect produces: polling pauses, then resumes.
            try await client.logout()
            _ = try await client.login(username: E2EEnvironment.rootUsername, password: E2EEnvironment.rootPassword)
            #expect(try await consumer.next()?.message.offset == 1)
            await consumer.shutdown()
        }
    }

    @Test func producerDoesNotCreateWhenDisabled() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let name = E2EEnvironment.uniqueName("swift-nocreate")
            let producer = client.producer(
                stream: try Identifier(named: name), topic: try Identifier(named: name), configuration: ProducerConfiguration(createStreamIfNotExists: false))
            await expectIggyError(.streamNameNotFound) { try await producer.initialize() }
            #expect(try await client.getStream(Identifier(named: name)) == nil)
        }
    }
}
