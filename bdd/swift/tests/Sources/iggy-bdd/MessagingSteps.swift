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

/// The background every feature shares.
enum CommonSteps {
    static func register(into registry: inout StepRegistry) {
        registry.step("I have a running Iggy server") { world, _ in
            world.serverAddress = try Environment.serverAddress()
        }
        registry.step("I am authenticated as the root user") { world, _ in
            guard let address = world.serverAddress else {
                throw StepFailure("the server address was not read; the background is out of order")
            }
            let client = try await world.connect(to: address)
            try await client.ping()
            let root = try Environment.rootCredentials()
            _ = try await client.login(username: root.username, password: root.password)
        }
    }
}

/// `basic_messaging.feature`.
enum BasicMessagingSteps {
    static func register(into registry: inout StepRegistry) {
        registry.step("I have no streams in the system") { world, _ in
            let streams = try await world.requireClient().getStreams()
            try check(streams.isEmpty, "the system should have no streams initially, found \(streams.count)")
        }
        registry.step("I create a stream with name \"([^\"]*)\"") { world, groups in
            let stream = try await world.requireClient().createStream(name: groups[0])
            world.lastStreamID = stream.id
            world.lastStreamName = stream.name
        }
        registry.step("the stream should be created successfully") { world, _ in
            try check(world.lastStreamID != nil, "the stream should have been created")
        }
        registry.step("the stream should have name \"([^\"]*)\"") { world, groups in
            try check(world.lastStreamName == groups[0], "expected stream name \(groups[0]), got \(world.lastStreamName ?? "none")")
        }
        registry.step("I create a topic with name \"([^\"]*)\" in stream (\\d+) with (\\d+) partitions") { world, groups in
            let topic = try await world.requireClient().createTopic(
                streamID: Identifier(numeric: try number(groups[1])), name: groups[0],
                options: TopicCreateOptions(partitionsCount: try number(groups[2]), messageExpiry: .never))
            world.lastTopicID = topic.id
            world.lastTopicName = topic.name
            world.lastTopicPartitions = topic.partitionsCount
        }
        registry.step("the topic should be created successfully") { world, _ in
            try check(world.lastTopicID != nil, "the topic should have been created")
        }
        registry.step("the topic should have name \"([^\"]*)\"") { world, groups in
            try check(world.lastTopicName == groups[0], "expected topic name \(groups[0]), got \(world.lastTopicName ?? "none")")
        }
        registry.step("the topic should have (\\d+) partitions") { world, groups in
            let expected: UInt32 = try number(groups[0])
            try check(world.lastTopicPartitions == expected, "expected \(expected) partitions, got \(world.lastTopicPartitions.map { "\($0)" } ?? "none")")
        }
        registry.step("I send (\\d+) messages to stream (\\d+), topic (\\d+), partition (\\d+)") { world, groups in
            let count: UInt32 = try number(groups[0])
            var messages = try (0..<count).map { try IggyMessage("test message \($0)", id: MessageID(uuid: UUID())) }
            try await world.requireClient().sendMessages(
                streamID: Identifier(numeric: try number(groups[1])), topicID: Identifier(numeric: try number(groups[2])),
                partitioning: .partition(try number(groups[3])), messages: &messages)
            world.lastSentMessage = messages.last
        }
        registry.step("all messages should be sent successfully") { world, _ in
            try check(world.lastSentMessage != nil, "no messages were sent")
        }
        registry.step("I poll messages from stream (\\d+), topic (\\d+), partition (\\d+) starting from offset (\\d+)") { world, groups in
            world.lastPolled = try await world.requireClient().pollMessages(
                streamID: Identifier(numeric: try number(groups[0])), topicID: Identifier(numeric: try number(groups[1])),
                partitionID: try number(groups[2]), strategy: .offset(try number(groups[3], as: UInt64.self)), count: 100, autoCommit: false)
        }
        registry.step("I should receive (\\d+) messages") { world, groups in
            let expected: Int = try number(groups[0])
            let polled = try lastPolled(world)
            try check(polled.messages.count == expected, "expected \(expected) messages, got \(polled.messages.count)")
        }
        registry.step("the messages should have sequential offsets from (\\d+) to (\\d+)") { world, groups in
            let start: UInt64 = try number(groups[0])
            let end: UInt64 = try number(groups[1])
            let polled = try lastPolled(world)
            for (index, message) in polled.messages.enumerated() {
                let expected = start + UInt64(index)
                try check(message.offset == expected, "message at index \(index) should have offset \(expected), got \(message.offset)")
            }
            try check(polled.messages.last?.offset == end, "the last message should have offset \(end)")
        }
        registry.step("each message should have the expected payload content") { world, _ in
            for (index, message) in try lastPolled(world).messages.enumerated() {
                try check(
                    message.payloadString == "test message \(index)",
                    "message at index \(index) should have payload 'test message \(index)', got '\(message.payloadString)'")
            }
        }
        registry.step("the last polled message should match the last sent message") { world, _ in
            guard let sent = world.lastSentMessage, let polled = try lastPolled(world).messages.last else {
                throw StepFailure("there should be a sent and a polled message")
            }
            try check(polled.id == sent.id, "message ids should match: sent \(sent.id), polled \(polled.id)")
            try check(polled.payload == sent.payload, "message payloads should match: sent \(sent.payloadString), polled \(polled.payloadString)")
        }
    }

    private static func lastPolled(_ world: World) throws -> PolledMessages {
        guard let polled = world.lastPolled else {
            throw StepFailure("no messages were polled in this scenario")
        }
        return polled
    }
}

/// `raw_command.feature`.
enum RawCommandSteps {
    static func register(into registry: inout StepRegistry) {
        registry.step("I send a raw command with code (\\d+) and an empty payload") { world, groups in
            let client = try world.requireClient()
            let code: UInt32 = try number(groups[0])
            await world.captureReply { try await client.sendRawRequest(code: code, payload: []) }
        }
        registry.step("I send a raw command with code (\\d+) and an empty payload to replica (\\d+)") { world, groups in
            let client = try world.requireClient()
            let code: UInt32 = try number(groups[0])
            let replica: UInt8 = try number(groups[1])
            await world.captureReply { try await client.sendRawRequest(code: code, payload: [], replica: replica) }
        }
        registry.step("the raw command should succeed with an empty response") { world, _ in
            try check(world.lastError == nil, "the raw command failed: \(world.lastError.map { "\($0)" } ?? "")")
            try check(world.lastRawResponse?.isEmpty == true, "expected an empty response, got \(world.lastRawResponse?.count ?? 0) bytes")
        }
        registry.step("the raw command should succeed with a non-empty response") { world, _ in
            try check(world.lastError == nil, "the raw command failed: \(world.lastError.map { "\($0)" } ?? "")")
            try check(world.lastRawResponse?.isEmpty == false, "expected a non-empty response")
        }
        registry.step("the raw command should fail with an invalid command error") { world, _ in
            try world.expectFailure(.invalidCommand)
        }
        registry.step("the raw command should fail with an invalid identifier error") { world, _ in
            try world.expectFailure(.invalidIdentifier)
        }
    }
}

/// `session_contract.feature`.
enum SessionContractSteps {
    static func register(into registry: inout StepRegistry) {
        registry.step("I have a cluster node that cannot reach quorum") { world, _ in
            world.serverAddress = try Environment.noQuorumAddress()
        }
        registry.step("I sign out") { world, _ in
            try await world.requireClient().logout()
        }
        registry.step("I request the stream with ID (\\d+)") { world, groups in
            let client = try world.requireClient()
            let streamID = Identifier(numeric: try number(groups[0]))
            await world.capture { _ = try await client.getStream(streamID) }
        }
        registry.step("I sign in to that node as the root user") { world, _ in
            guard let address = world.serverAddress else {
                throw StepFailure("the node address was not read; the steps are out of order")
            }
            let client = try await world.connect(to: address)
            let root = try Environment.rootCredentials()
            await world.capture { _ = try await client.login(username: root.username, password: root.password) }
        }
        registry.step("the call should fail with error code (\\d+)") { world, groups in
            let code: UInt32 = try number(groups[0])
            guard let error = world.lastError as? IggyError else {
                throw StepFailure("expected error code \(code), got \(world.lastError.map { "\($0)" } ?? "success")")
            }
            try check(error.rawCode == code, "expected error code \(code), got \(error)")
        }
    }
}

extension World {
    /// Runs a call whose failure a later step checks.
    func capture(_ call: () async throws -> Void) async {
        lastError = nil
        do {
            try await call()
        } catch {
            lastError = error
        }
    }

    /// Runs a raw request whose reply or failure a later step checks.
    func captureReply(_ call: () async throws -> [UInt8]) async {
        lastRawResponse = nil
        await capture { lastRawResponse = try await call() }
    }

    func expectFailure(_ code: IggyErrorCode) throws {
        guard let error = lastError as? IggyError else {
            throw StepFailure("expected \(code.name), got \(lastError.map { "\($0)" } ?? "success")")
        }
        try check(error.code == code, "expected \(code.name), got \(error)")
    }
}
