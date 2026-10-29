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

/// `stream_options.feature` and `resource_options_patch.feature`, whose
/// stream and topic steps `metadata_changes.feature` shares.
enum ResourceOptionsSteps {
    /// The partition every message of these scenarios goes to.
    static let partitionID: UInt32 = 0
    /// Option keys no read may ever return.
    static let hiddenKeys = ["encryption_key", "encryption_key_wrapped", "critical_keys"]

    static func register(into registry: inout StepRegistry) {
        registerStreams(into: &registry)
        registerMessages(into: &registry)
        registerTopics(into: &registry)
        registerUsers(into: &registry)
    }

    private static func registerStreams(into registry: inout StepRegistry) {
        let keyed: StepHandler = { world, groups in
            let key = Data(repeating: try number(groups[1], as: UInt8.self), count: EncryptionKey.length)
            try await createStream(world, name: groups[0], options: StreamCreateOptions(encryptionKey: key))
            world.streamKey = key
        }
        let keyless: StepHandler = { world, groups in
            world.streamKey = nil
            try await createStream(world, name: groups[0], options: StreamCreateOptions())
        }
        registry.step("I create a stream named \"([^\"]+)\" with an encryption key of 32 bytes valued (\\d+)", keyed)
        registry.step("a stream named \"([^\"]+)\" with an encryption key of 32 bytes valued (\\d+) exists", keyed)
        registry.step("I create a stream named \"([^\"]+)\" without an encryption key", keyless)
        registry.step("a stream named \"([^\"]+)\" exists", keyless)
        registry.step("the stream should report encryption key source \"([^\"]+)\"") { world, groups in
            let stream = try await fetchStream(world)
            try check(
                stream.encryptionKeySource?.rawValue == groups[0],
                "encryption key source is \(stream.encryptionKeySource.map { $0.rawValue } ?? "unreported"), want \(groups[0])")
        }
        registry.step("the stream encryption key fingerprint should match the key") { world, _ in
            let stream = try await fetchStream(world)
            let expected = try EncryptionKey.fingerprint(try streamKey(world))
            try check(
                stream.encryptionKeyFingerprint == expected,
                "encryption key fingerprint is \(stream.encryptionKeyFingerprint ?? "unreported"), want \(expected)")
        }
        registry.step("the stream should report an encryption key fingerprint") { world, _ in
            let fingerprint = try await fetchStream(world).encryptionKeyFingerprint
            try check(fingerprint?.count == 16, "encryption key fingerprint is \(fingerprint ?? "unreported"), want 16 hex characters")
        }
        registry.step("the stream should report at least one KEK fingerprint") { world, _ in
            try check(!(try await fetchStream(world).encryptionKekFingerprints.isEmpty), "the stream reports no KEK fingerprint")
        }
        registry.step("the stream should expose no encryption key material") { world, _ in
            try checkNoKeyMaterial(try await fetchStream(world).options, key: world.streamKey, label: "the stream")
        }
        registry.step("I rename the stream to \"([^\"]+)\"") { world, groups in
            try await world.requireClient().updateStream(try world.requireStream(), options: StreamUpdateOptions(name: groups[0]))
        }
        registry.step("the stream name should be \"([^\"]+)\"") { world, groups in
            let name = try await fetchStream(world).name
            try check(name == groups[0], "stream name is \(name), want \(groups[0])")
        }
        registry.step("I request a reset of the stream option \"([^\"]+)\"") { world, groups in
            let client = try world.requireClient()
            let streamID = try world.requireStream()
            let options = StreamOptionKey(rawValue: groups[0]).map { StreamUpdateOptions(reset: [$0]) } ?? StreamUpdateOptions(rawReset: [groups[0]])
            await world.capture { try await client.updateStream(streamID, options: options) }
        }
        registry.step("the stream update should be rejected") { world, _ in
            try check(world.lastError != nil, "the update must fail")
        }
        registry.step("I describe the stream options") { world, _ in
            world.optionSpecs = try await world.requireClient().describeOptions(scope: .stream)
        }
        registry.step("the stream option \"([^\"]+)\" should be updatable and not secret") { world, groups in
            let flags = try optionFlags(world, groups[0])
            try check(flags.contains(.updatable) && !flags.contains(.secret) && !flags.contains(.readOnly), "\(groups[0]) has flags \(flags.rawValue)")
        }
        registry.step("the stream option \"([^\"]+)\" should be create only and secret") { world, groups in
            let flags = try optionFlags(world, groups[0])
            try check(
                flags.contains(.secret) && !flags.contains(.updatable) && !flags.contains(.resettable) && !flags.contains(.readOnly),
                "\(groups[0]) has flags \(flags.rawValue)")
        }
        registry.step("the stream option \"([^\"]+)\" should be read only") { world, groups in
            let flags = try optionFlags(world, groups[0])
            try check(flags.contains(.readOnly) && !flags.contains(.updatable) && !flags.contains(.secret), "\(groups[0]) has flags \(flags.rawValue)")
        }
    }

    private static func registerMessages(into registry: inout StepRegistry) {
        registry.step("I send a message with payload \"([^\"]+)\" to the topic") { world, groups in
            try await send(world, try IggyMessage(groups[0]))
        }
        registry.step("I send a message with payload \"([^\"]+)\" and header \"([^\"]+)\" set to \"([^\"]+)\" to the topic") { world, groups in
            try await send(world, try IggyMessage(groups[0], userHeaders: [try HeaderKey(groups[1]): try HeaderValue.string(groups[2])]))
        }
        registry.step("polling the topic should return the payload \"([^\"]+)\"") { world, groups in
            let message = try await pollFirst(world)
            try check(message.payloadString == groups[0], "polled payload \(message.payloadString), want \(groups[0])")
        }
        registry.step("polling the topic should return the payload \"([^\"]+)\" and header \"([^\"]+)\" set to \"([^\"]+)\"") { world, groups in
            let message = try await pollFirst(world)
            try check(message.payloadString == groups[0], "polled payload \(message.payloadString), want \(groups[0])")
            let value = message.userHeaders?[try HeaderKey(groups[1])]
            try check(value == (try HeaderValue.string(groups[2])), "polled headers \(message.userHeaders ?? [:]) do not carry \(groups[1])=\(groups[2])")
        }
    }

    private static func registerTopics(into registry: inout StepRegistry) {
        registry.step("I create a topic named \"([^\"]+)\" with (\\d+) partitions? in the stream") { world, groups in
            let options = TopicCreateOptions(partitionsCount: try number(groups[1]), messageExpiry: .never)
            world.lastTopicID = try await world.requireClient().createTopic(streamID: try world.requireStream(), name: groups[0], options: options).id
        }
        registry.step(
            "a topic named \"([^\"]+)\" exists in the stream with message expiry (\\d+) microseconds and max topic size (\\d+) bytes"
        ) { world, groups in
            let options = TopicCreateOptions(
                partitionsCount: 1, messageExpiry: .after(.microseconds(try number(groups[1], as: UInt64.self))),
                maxTopicSize: .bytes(try number(groups[2])))
            world.lastTopicID = try await world.requireClient().createTopic(streamID: try world.requireStream(), name: groups[0], options: options).id
        }
        registry.step("I update the topic message expiry to (\\d+) microseconds without a name") { world, groups in
            try await updateTopic(world, TopicUpdateOptions(messageExpiry: .after(.microseconds(try number(groups[0], as: UInt64.self)))))
        }
        registry.step("I reset the topic option \"([^\"]+)\"") { world, groups in
            try await updateTopic(
                world, TopicOptionKey(rawValue: groups[0]).map { TopicUpdateOptions(reset: [$0]) } ?? TopicUpdateOptions(rawReset: [groups[0]]))
        }
        registry.step("I rename the topic to \"([^\"]+)\"") { world, groups in
            try await updateTopic(world, TopicUpdateOptions(name: groups[0]))
        }
        registry.step("the topic name should be \"([^\"]+)\"") { world, groups in
            let name = try await fetchTopic(world).name
            try check(name == groups[0], "topic name is \(name), want \(groups[0])")
        }
        registry.step("the topic message expiry should be (\\d+) microseconds set explicitly") { world, groups in
            let topic = try await fetchTopic(world)
            let expected: UInt64 = try number(groups[0])
            try check(topic.messageExpiry == .after(.microseconds(expected)), "message expiry is \(topic.messageExpiry), want \(expected) microseconds")
            try check(topic.options[TopicOptionKey.messageExpiry.rawValue]?.explicit == true, "message expiry must be set explicitly")
        }
        registry.step("the topic max topic size should be (\\d+) bytes set explicitly") { world, groups in
            let topic = try await fetchTopic(world)
            let expected: UInt64 = try number(groups[0])
            try check(topic.maxTopicSize == .bytes(expected), "max topic size is \(topic.maxTopicSize), want \(expected) bytes")
            try check(topic.options[TopicOptionKey.maxTopicSize.rawValue]?.explicit == true, "max topic size must be set explicitly")
        }
        registry.step("the topic max topic size should be derived from the server default") { world, _ in
            let option = try await fetchTopic(world).options[TopicOptionKey.maxTopicSize.rawValue]
            try check(option?.explicit == false, "max topic size must be derived from the server default, got \(option.map { "\($0)" } ?? "nothing")")
        }
    }

    private static func registerUsers(into registry: inout StepRegistry) {
        registry.step("a user named \"([^\"]+)\" with password \"([^\"]+)\" exists") { world, groups in
            try await createUser(world, username: groups[0], password: groups[1], permissions: nil)
        }
        registry.step("I update the user username to \"([^\"]+)\" and status to \"([^\"]+)\"") { world, groups in
            let options = UserUpdateOptions(username: groups[0], status: try userStatus(groups[1]))
            try await world.requireClient().updateUser(try world.requireUser(), options: options)
        }
        registry.step("the user should have username \"([^\"]+)\"") { world, groups in
            let username = try await fetchUser(world).username
            try check(username == groups[0], "username is \(username), want \(groups[0])")
        }
        registry.step("the user should have status \"([^\"]+)\"") { world, groups in
            let status = try await fetchUser(world).status
            try check(status == (try userStatus(groups[0])), "user status is \(status), want \(groups[0])")
        }
    }

    static func createStream(_ world: World, name: String, options: StreamCreateOptions) async throws {
        let stream = try await world.requireClient().createStream(name: name, options: options)
        world.lastStreamID = stream.id
        world.lastStreamName = name
        world.createdStreams.append(stream.id)
    }

    static func createUser(_ world: World, username: String, password: String, permissions: Permissions?) async throws {
        let user = try await world.requireClient().createUser(username: username, password: password, permissions: permissions)
        world.userID = user.id
        world.createdUsers.append(user.id)
    }

    static func streamKey(_ world: World) throws -> Data {
        guard let key = world.streamKey else {
            throw StepFailure("the scenario created no keyed stream")
        }
        return key
    }

    /// Fails when `options` returns a hidden key or carries `key` raw, in
    /// base64 or in hex.
    static func checkNoKeyMaterial(_ options: ResourceOptions, key: Data?, label: String) throws {
        for hidden in hiddenKeys {
            try check(options[hidden] == nil, "\(label) returns \(hidden)")
        }
        guard let key else {
            return
        }
        let spellings = [Array(key), Array(key.base64EncodedString().utf8), Array(key.map { String(format: "%02x", $0) }.joined().utf8)]
        for (name, option) in options {
            for spelling in spellings {
                try check(!contains(option.value.bytes, spelling), "\(label) option \(name) carries the stream key")
            }
        }
    }

    private static func contains(_ bytes: [UInt8], _ needle: [UInt8]) -> Bool {
        guard bytes.count >= needle.count else {
            return false
        }
        return (0...(bytes.count - needle.count)).contains { start in bytes[start..<(start + needle.count)].elementsEqual(needle) }
    }

    private static func send(_ world: World, _ message: IggyMessage) async throws {
        var messages = [message]
        try await world.requireClient().sendMessages(
            streamID: try world.requireStream(), topicID: try world.requireTopic(), partitioning: .partition(partitionID), messages: &messages)
    }

    private static func pollFirst(_ world: World) async throws -> IggyMessage {
        let polled = try await world.requireClient().pollMessages(
            streamID: try world.requireStream(), topicID: try world.requireTopic(), partitionID: partitionID, strategy: .offset(0), count: 1,
            autoCommit: false)
        guard let message = polled.messages.first else {
            throw StepFailure("the poll returned no message")
        }
        return message
    }

    private static func updateTopic(_ world: World, _ options: TopicUpdateOptions) async throws {
        try await world.requireClient().updateTopic(streamID: try world.requireStream(), topicID: try world.requireTopic(), options: options)
    }

    private static func optionFlags(_ world: World, _ key: String) throws -> OptionFlags {
        guard let spec = world.optionSpecs.first(where: { $0.key == key }) else {
            throw StepFailure("the catalog does not describe \(key)")
        }
        return spec.flags
    }

    private static func fetchStream(_ world: World) async throws -> StreamDetails {
        guard let stream = try await world.requireClient().getStream(try world.requireStream()) else {
            throw StepFailure("the stream is gone")
        }
        return stream
    }

    private static func fetchTopic(_ world: World) async throws -> TopicDetails {
        guard let topic = try await world.requireClient().getTopic(streamID: try world.requireStream(), topicID: try world.requireTopic()) else {
            throw StepFailure("the topic is gone")
        }
        return topic
    }

    private static func fetchUser(_ world: World) async throws -> UserInfoDetails {
        guard let user = try await world.requireClient().getUser(try world.requireUser()) else {
            throw StepFailure("the user is gone")
        }
        return user
    }

    private static func userStatus(_ name: String) throws -> UserStatus {
        switch name {
        case "active": .active
        case "inactive": .inactive
        default: throw StepFailure("unknown user status \(name)")
        }
    }
}
