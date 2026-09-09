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
import Logging

@testable import Iggy

/// An in-memory stand-in for the client: topics are partition logs, offsets
/// and group membership are plain dictionaries, and every call is recorded
/// so a test can assert on what the producer or consumer did.
final class FakeBackend: MessagingBackend, @unchecked Sendable {
    enum Call: Equatable, Sendable {
        case getStream(Identifier)
        case createStream(String)
        case getTopic(Identifier, Identifier)
        case createTopic(Identifier, String, UInt32?)
        case send(Identifier, Identifier, Partitioning, Int)
        case poll(Identifier, Identifier, UInt32?, Consumer, PollingStrategy, UInt32, Bool)
        case getGroup(Identifier, Identifier, Identifier)
        case createGroup(Identifier, Identifier, String)
        case join(Identifier, Identifier, Identifier)
        case leave(Identifier, Identifier, Identifier)
        case storeOffset(Consumer, UInt32?, UInt64)
        case deleteOffset(Consumer, UInt32?)
    }

    struct Topic {
        var id: UInt32
        var name: String
        var partitions: [[IggyMessage]]
    }

    private let state = FakeState()
    private let eventStreams = EventHub()

    var calls: [Call] {
        get async { await state.calls }
    }

    func reset() async {
        await state.resetCalls()
    }

    // MARK: Scripting

    func addStream(_ name: String) async {
        await state.addStream(name)
    }

    func addTopic(stream: String, name: String, partitions: Int) async {
        await state.addTopic(stream: stream, name: name, partitions: partitions)
    }

    func addGroup(stream: String, topic: String, name: String) async {
        await state.addGroup(stream: stream, topic: topic, name: name)
    }

    /// The next `count` sends fail with `error`, after `successes` sends
    /// that go through.
    func failSends(_ count: Int, after successes: Int = 0, with error: IggyError) async {
        await state.failSends(count, after: successes, with: error)
    }

    /// The next `count` polls fail with `error`.
    func failPolls(_ count: Int, with error: IggyError) async {
        await state.failPolls(count, with: error)
    }

    /// The next `count` polls answer with `reply` instead of the log.
    func scriptPolls(_ replies: [PolledMessages]) async {
        await state.scriptPolls(replies)
    }

    /// The next `count` offset stores fail.
    func failStores(_ count: Int, with error: IggyError) async {
        await state.failStores(count, with: error)
    }

    /// Every send waits for `delay` before it is applied.
    func setSendDelay(_ delay: Duration) async {
        await state.setSendDelay(delay)
    }

    func messages(stream: String, topic: String, partition: Int) async -> [IggyMessage] {
        await state.messages(stream: stream, topic: topic, partition: partition)
    }

    func storedOffset(consumer: Consumer, partition: UInt32) async -> UInt64? {
        await state.storedOffset(consumer: consumer, partition: partition)
    }

    func members(stream: String, topic: String, group: String) async -> Int {
        await state.members(stream: stream, topic: topic, group: group)
    }

    func emit(_ event: DiagnosticEvent) async {
        await eventStreams.emit(event)
    }

    var subscriberCount: Int {
        get async { await eventStreams.count }
    }

    // MARK: MessagingBackend

    func getStream(_ streamID: Identifier) async throws -> StreamDetails? {
        try await state.getStream(streamID)
    }

    func createStream(name: String) async throws -> StreamDetails {
        try await state.createStream(name: name)
    }

    func getTopic(streamID: Identifier, topicID: Identifier) async throws -> TopicDetails? {
        try await state.getTopic(streamID: streamID, topicID: topicID)
    }

    func createTopic(streamID: Identifier, name: String, options: TopicCreateOptions) async throws -> TopicDetails {
        try await state.createTopic(streamID: streamID, name: name, options: options)
    }

    func sendMessages(streamID: Identifier, topicID: Identifier, partitioning: Partitioning, messages: [IggyMessage]) async throws -> SendMessagesResponse {
        try await state.sendMessages(streamID: streamID, topicID: topicID, partitioning: partitioning, messages: messages)
    }

    func pollMessages(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32?, consumer: Consumer, count: UInt32, autoCommit: Bool,
        strategyFor: @Sendable (UInt32) -> PollingStrategy
    ) async throws -> PolledMessages {
        try await state.pollMessages(
            streamID: streamID, topicID: topicID, partitionID: partitionID, consumer: consumer, count: count, autoCommit: autoCommit, strategyFor: strategyFor)
    }

    func getConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws -> ConsumerGroupDetails? {
        try await state.getConsumerGroup(streamID: streamID, topicID: topicID, groupID: groupID)
    }

    func createConsumerGroup(streamID: Identifier, topicID: Identifier, name: String) async throws -> ConsumerGroupDetails {
        try await state.createConsumerGroup(streamID: streamID, topicID: topicID, name: name)
    }

    func joinConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws {
        try await state.joinConsumerGroup(streamID: streamID, topicID: topicID, groupID: groupID)
    }

    func leaveConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws {
        try await state.leaveConsumerGroup(streamID: streamID, topicID: topicID, groupID: groupID)
    }

    func storeConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, offset: UInt64) async throws {
        try await state.storeConsumerOffset(consumer: consumer, partitionID: partitionID, offset: offset)
    }

    func deleteConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?) async throws {
        try await state.deleteConsumerOffset(consumer: consumer, partitionID: partitionID)
    }

    var events: AsyncStream<DiagnosticEvent> {
        get async { await eventStreams.subscribe() }
    }
}

actor EventHub {
    private var continuations: [AsyncStream<DiagnosticEvent>.Continuation] = []

    var count: Int { continuations.count }

    func subscribe() -> AsyncStream<DiagnosticEvent> {
        let (stream, continuation) = AsyncStream<DiagnosticEvent>.makeStream()
        continuations.append(continuation)
        return stream
    }

    func emit(_ event: DiagnosticEvent) {
        for continuation in continuations {
            continuation.yield(event)
        }
    }
}

actor FakeState {
    private(set) var calls: [FakeBackend.Call] = []
    private var streams: [String: UInt32] = [:]
    private var topics: [String: [String: FakeBackend.Topic]] = [:]
    private var groups: [String: [String: Int]] = [:]
    private var offsets: [String: UInt64] = [:]
    private var pendingSendFailures: (skip: Int, count: Int, error: IggyError)?
    private var pendingPollFailures: (count: Int, error: IggyError)?
    private var pendingStoreFailures: (count: Int, error: IggyError)?
    private var scriptedPolls: [PolledMessages] = []
    private var sendDelay: Duration?
    private var nextID: UInt32 = 1
    private var balanced = 0

    func resetCalls() {
        calls.removeAll()
    }

    func addStream(_ name: String) {
        streams[name] = nextID
        nextID += 1
        topics[name] = [:]
    }

    func addTopic(stream: String, name: String, partitions: Int) {
        topics[stream, default: [:]][name] = FakeBackend.Topic(id: nextID, name: name, partitions: Array(repeating: [], count: partitions))
        nextID += 1
    }

    func addGroup(stream: String, topic: String, name: String) {
        groups["\(stream)|\(topic)", default: [:]][name] = 0
    }

    func failSends(_ count: Int, after successes: Int, with error: IggyError) {
        pendingSendFailures = (successes, count, error)
    }

    func failPolls(_ count: Int, with error: IggyError) {
        pendingPollFailures = (count, error)
    }

    func failStores(_ count: Int, with error: IggyError) {
        pendingStoreFailures = (count, error)
    }

    func scriptPolls(_ replies: [PolledMessages]) {
        scriptedPolls.append(contentsOf: replies)
    }

    func setSendDelay(_ delay: Duration) {
        sendDelay = delay
    }

    func messages(stream: String, topic: String, partition: Int) -> [IggyMessage] {
        topics[stream]?[topic]?.partitions[partition] ?? []
    }

    func storedOffset(consumer: Consumer, partition: UInt32) -> UInt64? {
        offsets["\(consumer)|\(partition)"]
    }

    func members(stream: String, topic: String, group: String) -> Int {
        groups["\(stream)|\(topic)"]?[group] ?? 0
    }

    private func streamName(_ id: Identifier) -> String? {
        if let name = id.name {
            return streams[name] != nil ? name : nil
        }
        return streams.first { $0.value == id.numericValue }?.key
    }

    private func topic(_ streamID: Identifier, _ topicID: Identifier) -> (stream: String, topic: FakeBackend.Topic)? {
        guard let stream = streamName(streamID), let list = topics[stream] else { return nil }
        if let name = topicID.name {
            return list[name].map { (stream, $0) }
        }
        return list.values.first { $0.id == topicID.numericValue }.map { (stream, $0) }
    }

    func getStream(_ streamID: Identifier) throws -> StreamDetails? {
        calls.append(.getStream(streamID))
        guard let name = streamName(streamID), let id = streams[name] else { return nil }
        return StreamDetails(id: id, createdAt: .zero, name: name, sizeBytes: 0, messagesCount: 0, topicsCount: UInt32(topics[name]?.count ?? 0), topics: [])
    }

    func createStream(name: String) throws -> StreamDetails {
        calls.append(.createStream(name))
        guard streams[name] == nil else {
            throw IggyError(.streamNameAlreadyExists)
        }
        addStream(name)
        return StreamDetails(id: streams[name]!, createdAt: .zero, name: name, sizeBytes: 0, messagesCount: 0, topicsCount: 0, topics: [])
    }

    func getTopic(streamID: Identifier, topicID: Identifier) throws -> TopicDetails? {
        calls.append(.getTopic(streamID, topicID))
        guard let found = topic(streamID, topicID) else { return nil }
        return Self.details(found.topic)
    }

    func createTopic(streamID: Identifier, name: String, options: TopicCreateOptions) throws -> TopicDetails {
        calls.append(.createTopic(streamID, name, options.partitionsCount))
        guard let stream = streamName(streamID) else {
            throw IggyError(.streamIdNotFound)
        }
        guard topics[stream]?[name] == nil else {
            throw IggyError(.topicNameAlreadyExists)
        }
        addTopic(stream: stream, name: name, partitions: Int(options.partitionsCount ?? 1))
        return Self.details(topics[stream]![name]!)
    }

    private static func details(_ topic: FakeBackend.Topic) -> TopicDetails {
        TopicDetails(
            topic: Topic(
                id: topic.id, createdAt: .zero, name: topic.name, sizeBytes: 0, messageExpiry: .never, compressionAlgorithm: .none, maxTopicSize: .unlimited,
                messagesCount: 0, partitionsCount: UInt32(topic.partitions.count)),
            partitions: [])
    }

    func sendMessages(streamID: Identifier, topicID: Identifier, partitioning: Partitioning, messages: [IggyMessage]) async throws -> SendMessagesResponse {
        calls.append(.send(streamID, topicID, partitioning, messages.count))
        if let sendDelay {
            try? await Task.sleep(for: sendDelay)
        }
        if let failure = pendingSendFailures {
            if failure.skip > 0 {
                pendingSendFailures = (failure.skip - 1, failure.count, failure.error)
            } else if failure.count > 0 {
                pendingSendFailures = failure.count == 1 ? nil : (0, failure.count - 1, failure.error)
                throw failure.error
            }
        }
        guard let found = topic(streamID, topicID) else {
            throw IggyError(.topicIdNotFound)
        }
        var entry = found.topic
        let partition: Int
        switch partitioning {
        case .partition(let id): partition = Int(id)
        case .balanced:
            partition = balanced % entry.partitions.count
            balanced += 1
        case .messagesKey(let key): partition = Int(XXH32.hash(key) % UInt32(entry.partitions.count))
        }
        guard partition < entry.partitions.count else {
            throw IggyError(.partitionNotFound)
        }
        let base = UInt64(entry.partitions[partition].count)
        for (index, message) in messages.enumerated() {
            var stored = message
            stored.offset = base + UInt64(index)
            entry.partitions[partition].append(stored)
        }
        topics[found.stream]![entry.name] = entry
        return SendMessagesResponse(confirmations: [
            SendConfirmation(streamID: streams[found.stream]!, topicID: entry.id, partitionID: UInt32(partition), baseOffset: base)
        ])
    }

    func pollMessages(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32?, consumer: Consumer, count: UInt32, autoCommit: Bool,
        strategyFor: @Sendable (UInt32) -> PollingStrategy
    ) throws -> PolledMessages {
        let partition = partitionID ?? 0
        let strategy = strategyFor(partition)
        calls.append(.poll(streamID, topicID, partitionID, consumer, strategy, count, autoCommit))
        if let failure = pendingPollFailures, failure.count > 0 {
            pendingPollFailures = failure.count == 1 ? nil : (failure.count - 1, failure.error)
            throw failure.error
        }
        if !scriptedPolls.isEmpty {
            return scriptedPolls.removeFirst()
        }
        guard let found = topic(streamID, topicID), partition < UInt32(found.topic.partitions.count) else {
            throw IggyError(.topicIdNotFound)
        }
        if consumer.kind == .consumerGroup {
            let group = consumer.id.name ?? ""
            guard (groups["\(found.stream)|\(found.topic.name)"]?[group] ?? 0) > 0 else {
                throw IggyError(.consumerGroupMemberNotFound)
            }
        }
        let log = found.topic.partitions[Int(partition)]
        let start: Int
        switch strategy.kind {
        case .offset: start = Int(strategy.value)
        case .first: start = 0
        case .last: start = max(log.count - Int(count), 0)
        case .next: start = offsets["\(consumer)|\(partition)"].map { Int($0) + 1 } ?? 0
        case .timestamp: start = 0
        }
        let slice = start < log.count ? Array(log[start..<min(start + Int(count), log.count)]) : []
        if autoCommit, let last = slice.last {
            offsets["\(consumer)|\(partition)"] = last.offset
        }
        return PolledMessages(partitionID: partition, currentOffset: log.isEmpty ? 0 : UInt64(log.count - 1), count: UInt32(slice.count), messages: slice)
    }

    func getConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) throws -> ConsumerGroupDetails? {
        calls.append(.getGroup(streamID, topicID, groupID))
        guard let found = topic(streamID, topicID), let name = groupID.name, let members = groups["\(found.stream)|\(found.topic.name)"]?[name] else {
            return nil
        }
        return ConsumerGroupDetails(id: 1, name: name, partitionsCount: UInt32(found.topic.partitions.count), membersCount: UInt32(members), members: [])
    }

    func createConsumerGroup(streamID: Identifier, topicID: Identifier, name: String) throws -> ConsumerGroupDetails {
        calls.append(.createGroup(streamID, topicID, name))
        guard let found = topic(streamID, topicID) else {
            throw IggyError(.topicIdNotFound)
        }
        let key = "\(found.stream)|\(found.topic.name)"
        guard groups[key]?[name] == nil else {
            throw IggyError(.consumerGroupNameAlreadyExists)
        }
        groups[key, default: [:]][name] = 0
        return ConsumerGroupDetails(id: 1, name: name, partitionsCount: UInt32(found.topic.partitions.count), membersCount: 0, members: [])
    }

    func joinConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) throws {
        calls.append(.join(streamID, topicID, groupID))
        guard let found = topic(streamID, topicID), let name = groupID.name, groups["\(found.stream)|\(found.topic.name)"]?[name] != nil else {
            throw IggyError(.consumerGroupIdNotFound)
        }
        groups["\(found.stream)|\(found.topic.name)"]![name]! += 1
    }

    func leaveConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) throws {
        calls.append(.leave(streamID, topicID, groupID))
        guard let found = topic(streamID, topicID), let name = groupID.name, let members = groups["\(found.stream)|\(found.topic.name)"]?[name], members > 0
        else {
            throw IggyError(.consumerGroupMemberNotFound)
        }
        groups["\(found.stream)|\(found.topic.name)"]![name] = members - 1
    }

    func storeConsumerOffset(consumer: Consumer, partitionID: UInt32?, offset: UInt64) throws {
        calls.append(.storeOffset(consumer, partitionID, offset))
        if let failure = pendingStoreFailures, failure.count > 0 {
            pendingStoreFailures = failure.count == 1 ? nil : (failure.count - 1, failure.error)
            throw failure.error
        }
        offsets["\(consumer)|\(partitionID ?? 0)"] = offset
    }

    func deleteConsumerOffset(consumer: Consumer, partitionID: UInt32?) throws {
        calls.append(.deleteOffset(consumer, partitionID))
        guard offsets.removeValue(forKey: "\(consumer)|\(partitionID ?? 0)") != nil else {
            throw IggyError(.consumerOffsetNotFound)
        }
    }
}

extension FakeBackend {
    /// A backend with stream `s`, topic `t` of `partitions` partitions.
    static func prepared(partitions: Int = 1) async -> FakeBackend {
        let backend = FakeBackend()
        await backend.addStream("s")
        await backend.addTopic(stream: "s", name: "t", partitions: partitions)
        return backend
    }
}

let testLogger = Logger(label: "org.apache.iggy.tests")

/// A value behind a lock, for counters shared with `@Sendable` closures.
final class Locked<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value

    init(_ value: Value) {
        self.value = value
    }

    func withLock<T>(_ body: (inout Value) throws -> T) rethrows -> T {
        lock.lock()
        defer { lock.unlock() }
        return try body(&value)
    }
}

func makeMessages(_ count: Int, prefix: String = "m") -> [IggyMessage] {
    (0..<count).map { try! IggyMessage("\(prefix)\($0)") }
}

/// Polls `condition` until it holds or `timeout` passes.
func eventually(timeout: Duration = .seconds(5), _ condition: @Sendable () async -> Bool) async -> Bool {
    let deadline = ContinuousClock.now + timeout
    while ContinuousClock.now < deadline {
        if await condition() {
            return true
        }
        try? await Task.sleep(for: .milliseconds(5))
    }
    return await condition()
}
