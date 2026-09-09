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

/// Step definitions shared by every feature: the background that connects
/// and signs in.
enum CommonSteps {
    static func register(into registry: inout StepRegistry) {
        registry.step("I have a running Iggy server") { world, _ in
            world.serverAddress = try Environment.serverAddress()
        }
        registry.step("I am authenticated as the root user") { world, _ in
            guard let address = world.serverAddress else {
                throw StepFailure("the server address was not read; the background is out of order")
            }
            let client = IggyClient(address: address)
            try await client.connect()
            try await client.ping()
            let credentials = try Environment.rootCredentials()
            _ = try await client.login(username: credentials.username, password: credentials.password)
            world.client = client
        }
    }
}

/// Steps of `basic_messaging.feature` and `stream_crud.feature`, which share
/// the stream and topic vocabulary.
enum StreamSteps {
    static func register(into registry: inout StepRegistry) {
        registry.step("I have no streams in the system") { world, _ in
            let streams = try await world.requireClient().getStreams()
            try check(streams.isEmpty, "the system should have no streams initially, found \(streams.count)")
        }
        registry.step("I create a stream with name \"([^\"]*)\"") { world, groups in
            try await createStream(world, name: groups[0])
        }
        registry.step("a stream with name \"([^\"]*)\" exists") { world, groups in
            try await createStream(world, name: groups[0])
        }
        registry.step("the stream should be created successfully") { world, _ in
            try check(world.lastStreamID != nil, "the stream should have been created")
        }
        registry.step("the stream should have name \"([^\"]*)\"") { world, groups in
            try check(world.lastStreamName == groups[0], "expected stream name \(groups[0]), got \(world.lastStreamName ?? "none")")
            let stream = try await world.requireClient().getStream(try lastStream(world))
            try check(stream?.name == groups[0], "the stream on the server should be named \(groups[0]), got \(stream?.name ?? "none")")
        }
        registry.step("I get the stream by its numeric ID") { world, _ in
            try await getStreamByID(world)
        }
        registry.step("the returned stream should have name \"([^\"]*)\"") { world, groups in
            try check(world.lastStreamName == groups[0], "expected stream name \(groups[0]), got \(world.lastStreamName ?? "none")")
        }
        registry.step("I list all streams") { world, _ in
            let streams = try await world.requireClient().getStreams()
            world.lastStreamWasFound = streams.contains { $0.id == world.lastStreamID }
        }
        registry.step("the stream list should contain the created stream") { world, _ in
            try check(world.lastStreamWasFound, "the stream list should contain the created stream")
        }
        registry.step("I update the stream name to \"([^\"]*)\"") { world, groups in
            try await world.requireClient().updateStream(try lastStream(world), name: groups[0])
        }
        registry.step("getting the stream by its numeric ID should return name \"([^\"]*)\"") { world, groups in
            try await getStreamByID(world)
            try check(world.lastStreamName == groups[0], "expected stream name \(groups[0]), got \(world.lastStreamName ?? "none")")
        }
        registry.step("I delete the stream by its numeric ID") { world, _ in
            try await world.requireClient().deleteStream(try lastStream(world))
        }
        registry.step("getting the stream by its numeric ID should return no stream") { world, _ in
            // The assertion is "not the stream we deleted", not "nothing at
            // this id": the server hands out the lowest free id, so a
            // concurrent create can legitimately occupy it.
            guard let deletedName = world.lastStreamName else {
                throw StepFailure("the stream should have been created")
            }
            let stream = try await world.requireClient().getStream(try lastStream(world))
            try check(stream == nil || stream?.name != deletedName, "the deleted stream should not be returned")
        }
        registry.step("I create a topic with name \"([^\"]*)\" in stream (\\d+) with (\\d+) partitions") { world, groups in
            let streamID = Identifier(numeric: try number(groups[1]))
            let options = TopicCreateOptions(partitionsCount: try number(groups[2]), compressionAlgorithm: CompressionAlgorithm.none, messageExpiry: .never)
            let topic = try await world.requireClient().createTopic(streamID: streamID, name: groups[0], options: options)
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
            let expected = try number(groups[0])
            try check(world.lastTopicPartitions == expected, "expected \(expected) partitions, got \(world.lastTopicPartitions ?? 0)")
        }
    }

    private static func createStream(_ world: World, name: String) async throws {
        let stream = try await world.requireClient().createStream(name: name)
        world.lastStreamID = stream.id
        world.lastStreamName = stream.name
    }

    private static func getStreamByID(_ world: World) async throws {
        let stream = try await world.requireClient().getStream(try lastStream(world))
        world.lastStreamName = stream?.name
    }

    private static func lastStream(_ world: World) throws -> Identifier {
        guard let id = world.lastStreamID else {
            throw StepFailure("no stream was created in this scenario")
        }
        return Identifier(numeric: id)
    }
}

/// Steps of `basic_messaging.feature` that move messages.
enum MessageSteps {
    static func register(into registry: inout StepRegistry) {
        registry.step("I send (\\d+) messages to stream (\\d+), topic (\\d+), partition (\\d+)") { world, groups in
            let count = try number(groups[0])
            var messages: [IggyMessage] = []
            for index in 0..<count {
                messages.append(try IggyMessage("test message \(index)", id: MessageID(uuid: UUID())))
            }
            _ = try await world.requireClient().sendMessages(
                streamID: Identifier(numeric: try number(groups[1])), topicID: Identifier(numeric: try number(groups[2])),
                partitioning: .partition(try number(groups[3])), messages: messages)
            world.lastSentMessage = messages.last
        }
        registry.step("all messages should be sent successfully") { world, _ in
            try check(world.lastSentMessage != nil, "no messages were sent")
        }
        registry.step("I poll messages from stream (\\d+), topic (\\d+), partition (\\d+) starting from offset (\\d+)") { world, groups in
            world.lastPolled = try await world.requireClient().pollMessages(
                streamID: Identifier(numeric: try number(groups[0])), topicID: Identifier(numeric: try number(groups[1])),
                partitionID: try number(groups[2]), consumer: .default, strategy: .offset(UInt64(try number(groups[3]))), count: 100, autoCommit: false)
        }
        registry.step("I should receive (\\d+) messages") { world, groups in
            let expected = try number(groups[0])
            let polled = try lastPolled(world)
            try check(polled.messages.count == Int(expected), "expected \(expected) messages, got \(polled.messages.count)")
        }
        registry.step("the messages should have sequential offsets from (\\d+) to (\\d+)") { world, groups in
            let start = UInt64(try number(groups[0]))
            let end = UInt64(try number(groups[1]))
            let polled = try lastPolled(world)
            for (index, message) in polled.messages.enumerated() {
                try check(
                    message.offset == start + UInt64(index), "message at index \(index) should have offset \(start + UInt64(index)), got \(message.offset)")
            }
            try check(polled.messages.last?.offset == end, "the last message should have offset \(end)")
        }
        registry.step("each message should have the expected payload content") { world, _ in
            for (index, message) in try lastPolled(world).messages.enumerated() {
                try check(
                    message.payloadString == "test message \(index)",
                    "message at offset \(index) should have payload 'test message \(index)', got '\(message.payloadString)'")
            }
        }
        registry.step("the last polled message should match the last sent message") { world, _ in
            guard let sent = world.lastSentMessage, let polled = try lastPolled(world).messages.last else {
                throw StepFailure("there should be a sent and a polled message")
            }
            try check(polled.id == sent.id, "message ids should match: sent \(sent.id), polled \(polled.id)")
            try check(polled.payload == sent.payload, "message payloads should match")
        }
    }

    private static func lastPolled(_ world: World) throws -> PolledMessages {
        guard let polled = world.lastPolled else {
            throw StepFailure("no messages were polled in this scenario")
        }
        return polled
    }
}

/// Steps of `raw_command.feature`.
enum RawCommandSteps {
    static func register(into registry: inout StepRegistry) {
        registry.step("I send a raw command with code (\\d+) and an empty payload") { world, groups in
            world.lastRawResponse = nil
            world.lastRawError = nil
            do {
                world.lastRawResponse = try await world.requireClient().sendRawRequest(code: try number(groups[0]), payload: [])
            } catch {
                world.lastRawError = error
            }
        }
        registry.step("the raw command should succeed with an empty response") { world, _ in
            try check(world.lastRawError == nil, "the raw command failed: \(world.lastRawError.map { "\($0)" } ?? "")")
            try check(world.lastRawResponse?.isEmpty == true, "expected an empty response, got \(world.lastRawResponse?.count ?? 0) bytes")
        }
        registry.step("the raw command should succeed with a non-empty response") { world, _ in
            try check(world.lastRawError == nil, "the raw command failed: \(world.lastRawError.map { "\($0)" } ?? "")")
            try check(world.lastRawResponse?.isEmpty == false, "expected a non-empty response")
        }
        registry.step("the raw command should fail with an invalid command error") { world, _ in
            guard let error = world.lastRawError as? IggyError else {
                throw StepFailure("expected an invalid command error, got \(world.lastRawError.map { "\($0)" } ?? "success")")
            }
            try check(error.code == .invalidCommand, "expected an invalid command error, got \(error)")
        }
    }
}

/// Parses a captured decimal group.
func number(_ text: String) throws -> UInt32 {
    guard let value = UInt32(text) else {
        throw StepFailure("\(text) is not a number")
    }
    return value
}
