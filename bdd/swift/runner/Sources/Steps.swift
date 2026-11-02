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

/// Whether a poll ended, set by the task of the poll.
final class PollEnd: @unchecked Sendable {
    private let lock = NSLock()
    private var endedAt: ContinuousClock.Instant?

    func end() {
        lock.withLock { endedAt = endedAt ?? .now }
    }

    var ended: ContinuousClock.Instant? {
        lock.withLock { endedAt }
    }
}

struct StepFailure: Error, CustomStringConvertible {
    let description: String

    init(_ description: String) {
        self.description = description
    }
}

/// The state of one scenario. Its steps run one after another, never at the
/// same time, so the state needs no lock.
final class World: @unchecked Sendable {
    var client: IggyClient?
    var stream: Identifier?
    var topic: Identifier?
    var poll: Task<PolledMessages, any Error>?
    var pollStarted = ContinuousClock.now
    var pollEnd = PollEnd()
    var reader: PartitionReader?
    var consumer: IggyConsumer?
    /// The operation of a committing poll whose outcome is unknown.
    var lostOperation: UInt128Value?

    func requireClient() throws -> IggyClient {
        guard let client else {
            throw StepFailure("no client is signed in")
        }
        return client
    }

    func requireTopic() throws -> (stream: Identifier, topic: Identifier) {
        guard let stream, let topic else {
            throw StepFailure("no stream and topic were created")
        }
        return (stream, topic)
    }

    func requirePoll() throws -> Task<PolledMessages, any Error> {
        guard let poll else {
            throw StepFailure("no deferred poll was started")
        }
        return poll
    }

    func requireConsumer() throws -> IggyConsumer {
        guard let consumer else {
            throw StepFailure("no consumer is open")
        }
        return consumer
    }

    func tearDown() async {
        poll?.cancel()
        if let reader {
            try? await reader.close()
        }
        await consumer?.shutdown()
        await client?.shutdown()
    }
}

/// How long a started poll gets to reach the server and park there.
private let parkTime: Duration = .milliseconds(500)

struct StepDefinition: Sendable {
    let pattern: String
    let run: @Sendable (World, [String]) async throws -> Void
}

/// Runs `body` and fails when it takes longer than `limit`.
func within<T: Sendable>(_ limit: Duration, _ what: String, _ body: @escaping @Sendable () async throws -> T) async throws -> T {
    try await withThrowingTaskGroup(of: T?.self) { group in
        group.addTask { try await body() }
        group.addTask {
            try await Task.sleep(for: limit)
            return nil
        }
        defer { group.cancelAll() }
        guard let value = try await group.next() ?? nil else {
            throw StepFailure("\(what) took longer than \(limit)")
        }
        return value
    }
}

private func expect(_ condition: Bool, _ message: @autoclosure () -> String) throws {
    if !condition {
        throw StepFailure(message())
    }
}

private func seconds(_ text: String) throws -> Duration {
    guard let value = Int64(text) else {
        throw StepFailure("\(text) is not a number of seconds")
    }
    return .seconds(value)
}

private func number<T: FixedWidthInteger>(_ text: String) throws -> T {
    guard let value = T(text) else {
        throw StepFailure("\(text) is not a number")
    }
    return value
}

private func send(_ world: World, payload: String, to partition: UInt32) async throws {
    let (stream, topic) = try world.requireTopic()
    var messages = [try IggyMessage(payload)]
    try await world.requireClient().sendMessages(streamID: stream, topicID: topic, partitioning: .partition(partition), messages: &messages)
}

private func poll(_ world: World, partition: UInt32, consumer: String, commit: CommitPolicy) async throws -> PolledMessages {
    let (stream, topic) = try world.requireTopic()
    return try await world.requireClient().pollMessages(
        streamID: stream, topicID: topic, partitionID: partition, consumer: .consumer(try Identifier(named: consumer)), strategy: .next,
        options: PollOptions(count: 1, commit: commit))
}

/// The ``IggyError`` that `body` throws, or a failure when it throws
/// anything else or returns, which names what it returned.
private func iggyError(_ what: String, _ body: () async throws -> PolledMessages) async throws -> IggyError {
    let polled: PolledMessages
    do {
        polled = try await body()
    } catch let error as IggyError {
        return error
    } catch {
        throw StepFailure("\(what) failed with \(error)")
    }
    throw StepFailure("\(what) did not fail, it returned \(polled.messages.map(\.payloadString)) from partition \(polled.partitionID)")
}

private func openConsumer(_ world: World, group: String, options: PollOptions) async throws {
    let (stream, topic) = try world.requireTopic()
    let consumer = try world.requireClient().consumerGroup(
        name: group, streamID: stream, topicID: topic, configuration: ConsumerConfiguration(pollOptions: options))
    try await consumer.initialize()
    world.consumer = consumer
}

/// The steps of `bdd/scenarios/deferred_polling.feature` on the Swift SDK.
let stepDefinitions: [StepDefinition] = [
    StepDefinition(pattern: #"I have a running Iggy server"#) { world, _ in
        let address = ProcessInfo.processInfo.environment["IGGY_TCP_ADDRESS"] ?? "127.0.0.1:8090"
        let client = IggyClient(configuration: ClientConfiguration(address: address))
        try await client.connect()
        world.client = client
    },
    StepDefinition(pattern: #"I am authenticated as the root user"#) { world, _ in
        let environment = ProcessInfo.processInfo.environment
        _ = try await world.requireClient().login(
            username: environment["IGGY_ROOT_USERNAME"] ?? "iggy", password: environment["IGGY_ROOT_PASSWORD"] ?? "iggy")
    },
    StepDefinition(pattern: #"a stream named "([^"]+)" with a topic named "([^"]+)" with (\d+) partitions exists"#) { world, values in
        let client = try world.requireClient()
        _ = try await client.createStream(name: values[0])
        let stream = try Identifier(named: values[0])
        _ = try await client.createTopic(streamID: stream, name: values[1], options: TopicCreateOptions(partitionsCount: try number(values[2])))
        world.stream = stream
        world.topic = try Identifier(named: values[1])
    },
    StepDefinition(pattern: #"I send a message with payload "([^"]+)" to partition (\d+)"#) { world, values in
        try await send(world, payload: values[0], to: try number(values[1]))
    },
    StepDefinition(pattern: #"I start a deferred poll of partition (\d+) from offset (\d+) with a max wait of (\d+) seconds?"#) { world, values in
        let (stream, topic) = try world.requireTopic()
        let client = try world.requireClient()
        let partition: UInt32 = try number(values[0])
        let offset: UInt64 = try number(values[1])
        let options = PollOptions(count: 10, maxWait: try seconds(values[2]))
        world.pollStarted = .now
        world.poll = Task {
            try await client.pollMessages(streamID: stream, topicID: topic, partitionID: partition, strategy: .offset(offset), options: options)
        }
    },
    StepDefinition(
        pattern:
            #"I start a deferred poll of partition (\d+) for consumer "([^"]+)" with the on read commit policy, a min count of (\d+) and a max wait of (\d+) seconds?"#
    ) { world, values in
        let (stream, topic) = try world.requireTopic()
        let client = try world.requireClient()
        let partition: UInt32 = try number(values[0])
        let consumer = Consumer.consumer(try Identifier(named: values[1]))
        let options = PollOptions(count: 10, maxWait: try seconds(values[3]), minCount: try number(values[2]), commit: .onRead)
        world.pollStarted = .now
        world.poll = Task {
            try await client.pollMessages(streamID: stream, topicID: topic, partitionID: partition, consumer: consumer, strategy: .next, options: options)
        }
    },
    StepDefinition(
        pattern: #"I start a deferred poll of partition (\d+) for consumer "([^"]+)" with the on read commit policy and a max wait of (\d+) seconds?"#
    ) { world, values in
        let (stream, topic) = try world.requireTopic()
        let client = try world.requireClient()
        let partition: UInt32 = try number(values[0])
        let consumer = Consumer.consumer(try Identifier(named: values[1]))
        let options = PollOptions(count: 10, maxWait: try seconds(values[2]), commit: .onRead)
        // A read that waits for nothing first opens the route and the
        // connection, so the parked poll only writes its frame.
        _ = try await client.pollMessages(
            streamID: stream, topicID: topic, partitionID: partition, consumer: consumer, strategy: .offset(0), options: PollOptions(count: 1, maxWait: .zero))
        let end = PollEnd()
        world.pollEnd = end
        world.pollStarted = .now
        world.poll = Task {
            defer { end.end() }
            return try await client.pollMessages(
                streamID: stream, topicID: topic, partitionID: partition, consumer: consumer, strategy: .next, options: options)
        }
    },
    StepDefinition(pattern: #"the client disconnects before the deferred poll answers"#) { world, _ in
        _ = try world.requirePoll()
        try await Task.sleep(for: parkTime)
        if let ended = world.pollEnd.ended {
            throw StepFailure("the deferred poll ended \(ended - world.pollStarted) after it started, before the disconnect")
        }
        await (try world.requireClient()).disconnect()
    },
    StepDefinition(pattern: #"the deferred poll should fail as uncertain"#) { world, _ in
        let poll = try world.requirePoll()
        world.poll = nil
        let error = try await iggyError("the deferred poll") { try await within(.seconds(5), "the deferred poll") { try await poll.value } }
        let ended = world.pollEnd.ended.map { "\($0 - world.pollStarted)" } ?? "never"
        try expect(error.code == .fetchProgressUncertain, "the deferred poll failed with \(error) \(ended) after it started")
        guard let operation = error.operationID else {
            throw StepFailure("the uncertain poll names no operation: \(error)")
        }
        world.lostOperation = operation
    },
    StepDefinition(pattern: #"the client connects again"#) { world, _ in
        let client = try world.requireClient()
        try await client.connect()
        let environment = ProcessInfo.processInfo.environment
        _ = try await client.login(username: environment["IGGY_ROOT_USERNAME"] ?? "iggy", password: environment["IGGY_ROOT_PASSWORD"] ?? "iggy")
    },
    StepDefinition(
        pattern:
            #"a poll of partition (\d+) for consumer "([^"]+)" with the next strategy and the on read commit policy should fail as blocked by the deferred poll"#
    ) { world, values in
        let partition: UInt32 = try number(values[0])
        let error = try await iggyError("the poll") { try await poll(world, partition: partition, consumer: values[1], commit: .onRead) }
        try expect(error.code == .partitionLaneBlocked, "the poll failed with \(error)")
        try expect(error.operationID == world.lostOperation && error.partitionID == partition, "the poll was blocked by \(error)")
    },
    StepDefinition(pattern: #"I abandon the operation of the deferred poll"#) { world, _ in
        guard let operation = world.lostOperation else {
            throw StepFailure("no deferred poll failed as uncertain")
        }
        try world.requireClient().abandonOperation(operation)
    },
    StepDefinition(pattern: #"I cancel the deferred poll after (\d+) milliseconds"#) { world, values in
        let poll = try world.requirePoll()
        try await Task.sleep(for: .milliseconds(try number(values[0]) as Int64))
        poll.cancel()
        // The server answers a cancelled committing poll once, with nothing
        // stored when the cancellation won.
        let result = await poll.result
        guard case .failure(let error) = result, error is CancellationError else {
            throw StepFailure("the cancelled poll ended with \(result)")
        }
    },
    StepDefinition(pattern: #"I wait (\d+) seconds?"#) { _, values in
        try await Task.sleep(for: try seconds(values[0]))
    },
    StepDefinition(pattern: #"the deferred poll should return the payload "([^"]+)" within (\d+) seconds?"#) { world, values in
        let poll = try world.requirePoll()
        let polled = try await within(try seconds(values[1]), "the deferred poll") { try await poll.value }
        try expect(polled.messages.map(\.payloadString) == [values[0]], "the deferred poll returned \(polled.messages.map(\.payloadString))")
    },
    StepDefinition(pattern: #"the deferred poll should return no messages between (\d+) and (\d+) seconds after it started"#) { world, values in
        let polled = try await world.requirePoll().value
        let elapsed = ContinuousClock.now - world.pollStarted
        try expect(polled.messages.isEmpty, "the deferred poll returned \(polled.messages.map(\.payloadString))")
        try expect(
            elapsed >= (try seconds(values[0])) && elapsed <= (try seconds(values[1])), "the deferred poll returned after \(elapsed)")
    },
    StepDefinition(
        pattern: #"a poll of partition (\d+) for consumer "([^"]+)" with the next strategy and the on read commit policy should return the payload "([^"]+)""#
    ) { world, values in
        let polled = try await poll(world, partition: try number(values[0]), consumer: values[1], commit: .onRead)
        try expect(polled.messages.map(\.payloadString) == [values[2]], "the poll returned \(polled.messages.map(\.payloadString))")
    },
    StepDefinition(pattern: #"a poll of partition (\d+) for consumer "([^"]+)" with the next strategy should return the payload "([^"]+)""#) {
        world, values in
        let polled = try await poll(world, partition: try number(values[0]), consumer: values[1], commit: .manual)
        try expect(polled.messages.map(\.payloadString) == [values[2]], "the poll returned \(polled.messages.map(\.payloadString))")
    },
    StepDefinition(pattern: #"I open a partition reader on partitions ([\d, and]+) from offset (\d+) with a max wait of (\d+) seconds?"#) { world, values in
        let (stream, topic) = try world.requireTopic()
        let partitions = values[0].split(whereSeparator: { !$0.isNumber }).compactMap { UInt32($0) }
        let offset: UInt64 = try number(values[1])
        world.reader = try await world.requireClient().reader(
            streamID: stream, topicID: topic, consumer: .consumer(try Identifier(named: "bdd-reader")),
            configuration: ReaderConfiguration(partitions: partitions, start: .offset(offset), options: .wait(try seconds(values[2]))))
    },
    StepDefinition(pattern: #"the partition reader should return the payload "([^"]+)" from partition (\d+) within (\d+) seconds?"#) {
        world, values in
        guard let reader = world.reader else {
            throw StepFailure("no partition reader is open")
        }
        let batch = try await within(try seconds(values[2]), "the partition reader") { try await reader.next() }
        let partition: UInt32 = try number(values[1])
        try expect(batch?.partitionID == partition, "the reader returned partition \(String(describing: batch?.partitionID))")
        try expect(batch?.messages.map(\.payloadString) == [values[0]], "the reader returned \(String(describing: batch?.messages.map(\.payloadString)))")
    },
    StepDefinition(pattern: #"I open a consumer in group "([^"]+)" on the topic with a max wait of (\d+) seconds?"#) { world, values in
        try await openConsumer(world, group: values[0], options: .wait(try seconds(values[1])))
    },
    StepDefinition(pattern: #"I open a consumer in group "([^"]+)" on the topic that commits after processing"#) { world, values in
        var options = PollOptions.wait(ReaderConfiguration.defaultMaxWait)
        options.commit = .auto(AutoCommit(interval: .seconds(5), after: .processing))
        try await openConsumer(world, group: values[0], options: options)
    },
    StepDefinition(pattern: #"the consumer should return the payload "([^"]+)" within (\d+) seconds?"#) { world, values in
        let consumer = try world.requireConsumer()
        let received = try await within(try seconds(values[1]), "the consumer") { try await consumer.next() }
        try expect(received?.message.payloadString == values[0], "the consumer returned \(String(describing: received?.message.payloadString))")
    },
    StepDefinition(pattern: #"I close the consumer"#) { world, _ in
        await (try world.requireConsumer()).shutdown()
        world.consumer = nil
    },
]
