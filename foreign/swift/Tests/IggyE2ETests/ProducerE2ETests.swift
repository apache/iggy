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

@Suite("Producer", requiresServer)
struct ProducerE2ETests {
    @Test func aDirectProducerCreatesItsTopicAndConfirmsEveryRequest() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let name = E2EEnvironment.uniqueName("swift-e2e-producer")
            let stream = try Identifier(named: name)
            await session.track(stream: stream)
            let configuration = ProducerConfiguration(
                partitioning: .partition(1), topicPartitionsCount: 2, mode: .direct(DirectSendOptions(batchLength: 2)))
            let producer = try client.producer(streamID: stream, topicID: try Identifier(named: name), configuration: configuration)
            try await producer.initialize()
            let topic = try #require(try await client.getTopic(streamID: stream, topicID: try Identifier(named: name)))
            #expect(topic.partitionsCount == 2)

            let response = try await producer.send(try E2EEnvironment.messages(5))
            #expect(response.confirmations.map(\.partitionID) == [1, 1, 1])
            #expect(response.confirmations.map(\.baseOffset) == [0, 2, 4])
            let polled = try await client.pollMessages(
                streamID: stream, topicID: try Identifier(named: name), partitionID: 1, strategy: .first, count: 10, autoCommit: false)
            #expect(polled.messages.map(\.payloadString) == (0..<5).map { "message \($0)" })
            await producer.shutdown()
        }
    }

    @Test func aBackgroundProducerWritesWhatItHoldsOnShutdown() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let options = BackgroundSendOptions(lingerTime: .seconds(60))
            let producer = try client.producer(
                streamID: scratch.stream, topicID: scratch.topic, configuration: ProducerConfiguration(partitioning: .partition(0), mode: .background(options)))
            try await producer.initialize()
            for index in 0..<3 {
                #expect(try await producer.send(try E2EEnvironment.messages(2, prefix: "batch \(index)")).confirmations.isEmpty)
            }
            await producer.shutdown()
            let polled = try await client.pollMessages(
                streamID: scratch.stream, topicID: scratch.topic, partitionID: 0, strategy: .first, count: 10, autoCommit: false)
            #expect(polled.messages.count == 6)
            #expect(polled.messages.first?.payloadString == "batch 0 0")
            #expect(polled.messages.last?.payloadString == "batch 2 1")
        }
    }

    @Test func aMissingNumericStreamCannotBeCreated() async throws {
        try await E2EEnvironment.withSession { session in
            let producer = try session.client.producer(streamID: Identifier(numeric: UInt32.max - 1), topicID: "missing")
            await expectIggyError(.streamIdNotFound) { try await producer.initialize() }
        }
    }
}
