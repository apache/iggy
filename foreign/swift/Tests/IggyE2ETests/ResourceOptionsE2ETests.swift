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

/// The shared resource options scenarios. The server must run with
/// encryption enabled (`IGGY_ENCRYPTION_ENABLED=true` and a 32-byte
/// `IGGY_ENCRYPTION_KEY`), so a stream created without a key gets one the
/// server generates.
@Suite("Resource options", requiresServer)
struct ResourceOptionsE2ETests {
    /// Keys no read may ever carry.
    static let neverReturnedKeys: Set<String> = ["encryption_key", "encryption_key_wrapped", "critical_keys"]

    @Test func givenStreamKeyWhenCreatingShouldReadMatchingProjection() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let key = Self.randomKey()
            let name = E2EEnvironment.uniqueName("swift-keyed")
            let created = try await client.createStream(name: name, options: StreamCreateOptions(encryptionKey: key))
            await session.track(stream: try Identifier(named: name))
            let fetched = try #require(try await client.getStream(Identifier(numeric: created.id)))
            let listed = try #require(try await client.getStreams().first { $0.id == created.id })

            let fingerprint = try EncryptionKey.fingerprint(key)
            for stream in [created.projection, fetched.projection, listed.projection] {
                #expect(stream.source == .stream)
                #expect(stream.keyFingerprint == fingerprint)
                #expect(stream.kekFingerprints.count == 1)
                #expect(stream.kekFingerprints.allSatisfy(Self.isFingerprint))
                #expect(stream.kekFingerprints == created.encryptionKekFingerprints)
            }
            for options in [created.options, fetched.options, listed.options] {
                Self.expectNoKeyMaterial(options, key: key)
            }
        }
    }

    @Test func givenKeylessStreamWhenReadingShouldReportServerGeneratedKey() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            var fingerprints: [String] = []
            for _ in 0..<2 {
                let name = E2EEnvironment.uniqueName("swift-keyless")
                let created = try await client.createStream(name: name)
                await session.track(stream: try Identifier(named: name))
                let fetched = try #require(try await client.getStream(Identifier(numeric: created.id)))
                let listed = try #require(try await client.getStreams().first { $0.id == created.id })
                for stream in [created.projection, fetched.projection, listed.projection] {
                    #expect(stream.source == .server)
                    #expect(stream.keyFingerprint.map(Self.isFingerprint) == true)
                    #expect(stream.keyFingerprint == created.encryptionKeyFingerprint)
                    #expect(stream.kekFingerprints.count == 1)
                    #expect(stream.kekFingerprints.allSatisfy(Self.isFingerprint))
                }
                #expect(Self.neverReturnedKeys.isDisjoint(with: fetched.options.keys))
                fingerprints.append(try #require(created.encryptionKeyFingerprint))
            }
            // Every keyless stream gets its own key.
            #expect(Set(fingerprints).count == 2)
        }
    }

    @Test func givenStreamRenameWhenUpdatingShouldPreserveEncryptionProjection() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let key = Self.randomKey()
            let name = E2EEnvironment.uniqueName("swift-rename")
            let created = try await client.createStream(name: name, options: StreamCreateOptions(encryptionKey: key))
            let streamID = Identifier(numeric: created.id)
            let topic = try await client.createTopic(streamID: streamID, name: name, options: TopicCreateOptions(partitionsCount: 1))
            let topicID = Identifier(numeric: topic.id)
            try await client.sendBatch(streamID: streamID, topicID: topicID, partitioning: .partition(0), messages: [IggyMessage("sealed payload")])

            let renamed = E2EEnvironment.uniqueName("swift-renamed")
            await session.track(stream: try Identifier(named: renamed))
            try await client.updateStream(streamID, options: StreamUpdateOptions(name: renamed))
            let fetched = try #require(try await client.getStream(streamID))
            #expect(fetched.id == created.id)
            #expect(fetched.name == renamed)
            #expect(fetched.projection == created.projection)
            #expect(fetched.encryptionKeyFingerprint == (try EncryptionKey.fingerprint(key)))
            let polled = try await client.pollMessages(
                streamID: Identifier(named: renamed), topicID: topicID, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
            #expect(polled.messages.map(\.payloadString) == ["sealed payload"])

            // No stream key is resettable and the projections are read-only,
            // so these never leave the client.
            await expectIggyError(.optionNotResettable) { try await client.updateStream(streamID, options: StreamUpdateOptions(reset: [.name])) }
            await expectIggyError(.optionNotUpdatable) { try await client.updateStream(streamID, options: StreamUpdateOptions(reset: [.encryptionKey])) }
            await expectIggyError(.readOnlyOption) {
                try await client.updateStream(streamID, options: StreamUpdateOptions(reset: [.encryptionKeySource]))
            }
            await expectIggyError(.invalidCommand) { try await client.updateStream(streamID, options: StreamUpdateOptions()) }
            // The raw escape hatch still goes through server admission.
            let unknown = E2EEnvironment.uniqueName("swift-unknown-key")
            await expectReplicatedIggyError(.unsupportedOptionKey) {
                _ = try await client.createStream(name: unknown, options: StreamCreateOptions(raw: ["future_option": "enabled"]))
            }
            #expect(try await client.getStream(Identifier(named: unknown)) == nil)
            #expect(try await client.getStream(streamID)?.projection == created.projection)
        }
    }

    @Test func givenTopicResetWhenUpdatingShouldPreserveUnmentionedValues() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let name = E2EEnvironment.uniqueName("swift-reset")
            let stream = try await client.createStream(name: name)
            let streamID = Identifier(numeric: stream.id)
            await session.track(stream: try Identifier(named: name))
            let created = try await client.createTopic(
                streamID: streamID, name: name,
                options: TopicCreateOptions(
                    compressionAlgorithm: .gzip, messageExpiry: .after(.seconds(3600)), maxTopicSize: .bytes(10_737_418_240), durability: .persisted))
            let topicID = Identifier(numeric: created.id)
            #expect(created.options[TopicOptionKey.maxTopicSize.rawValue] == .explicit(.uint64(10_737_418_240)))

            try await client.updateTopic(
                streamID: streamID, topicID: topicID, options: TopicUpdateOptions(messageExpiry: .after(.seconds(7200)), reset: [.maxTopicSize]))
            let updated = try #require(try await client.getTopic(streamID: streamID, topicID: topicID))
            #expect(updated.name == name)
            #expect(updated.messageExpiry == .after(.seconds(7200)))
            #expect(updated.options[TopicOptionKey.messageExpiry.rawValue] == .explicit(.uint64(7_200_000_000)))
            #expect(updated.maxTopicSize == .unlimited)
            #expect(updated.options[TopicOptionKey.maxTopicSize.rawValue] == .derived(.uint64(UInt64.max)))
            #expect(updated.compressionAlgorithm == .gzip)
            #expect(updated.options[TopicOptionKey.compressionAlgorithm.rawValue] == .explicit("gzip"))
            #expect(updated.options[TopicOptionKey.durability.rawValue] == .explicit("persisted"))
            #expect(updated.options[TopicOptionKey.segmentSize.rawValue] == created.options[TopicOptionKey.segmentSize.rawValue])

            // A rename alone keeps every option and its provenance.
            let renamed = E2EEnvironment.uniqueName("swift-reset-renamed")
            try await client.updateTopic(streamID: streamID, topicID: topicID, options: TopicUpdateOptions(name: renamed))
            let after = try #require(try await client.getTopic(streamID: streamID, topicID: topicID))
            #expect(after.name == renamed)
            #expect(after.options == updated.options)

            try await client.updateTopic(streamID: streamID, topicID: topicID, options: TopicUpdateOptions(reset: [.messageExpiry]))
            let reset = try #require(try await client.getTopic(streamID: streamID, topicID: topicID))
            #expect(reset.messageExpiry == .never)
            #expect(reset.options[TopicOptionKey.messageExpiry.rawValue]?.explicit == false)

            // Create-only keys cannot be reset, and a key cannot be both set and reset.
            await expectIggyError(.optionNotUpdatable) {
                try await client.updateTopic(streamID: streamID, topicID: topicID, options: TopicUpdateOptions(reset: [.compressionAlgorithm]))
            }
            await expectIggyError(.invalidCommand) {
                try await client.updateTopic(
                    streamID: streamID, topicID: topicID, options: TopicUpdateOptions(messageExpiry: .never, reset: [.messageExpiry]))
            }
            #expect(try await client.getTopic(streamID: streamID, topicID: topicID)?.options == reset.options)

            // Raw keys and reset names are the server's to judge.
            await expectReplicatedIggyError(.optionNotUpdatable) {
                try await client.updateTopic(
                    streamID: streamID, topicID: topicID, options: TopicUpdateOptions(reset: [.name], raw: [TopicOptionKey.segmentSize.rawValue: "1 GiB"]))
            }
            await expectReplicatedIggyError(.unsupportedOptionKey) {
                try await client.updateTopic(streamID: streamID, topicID: topicID, options: TopicUpdateOptions(rawReset: ["future_retention"]))
            }
            await expectReplicatedIggyError(.optionNotUpdatable) {
                try await client.updateTopic(
                    streamID: streamID, topicID: topicID, options: TopicUpdateOptions(rawReset: [TopicOptionKey.segmentSize.rawValue]))
            }
            #expect(try await client.getTopic(streamID: streamID, topicID: topicID)?.options == reset.options)

            let fixedName = E2EEnvironment.uniqueName("swift-fixed")
            let fixed = try await client.createTopic(streamID: streamID, name: fixedName, options: TopicCreateOptions(partitionResizePolicy: .fixed))
            #expect(fixed.options[TopicOptionKey.partitionResizePolicy.rawValue] == .explicit("fixed"))
        }
    }

    @Test func givenUserOptionsWhenCreatingAndUpdatingShouldPreserveUserBehavior() async throws {
        try await E2EEnvironment.withSession() { session in
            let client = session.client
            let first = String(E2EEnvironment.uniqueName("swift-opt-a").prefix(50))
            let second = String(E2EEnvironment.uniqueName("swift-opt-b").prefix(50))
            let created = try await client.createUser(username: first, password: "secret-1", options: UserCreateOptions())
            let other = try await client.createUser(username: second, password: "secret-2", options: UserCreateOptions())
            let userID = Identifier(numeric: created.id)
            let otherID = Identifier(numeric: other.id)
            #expect(created.options.isEmpty)

            let renamed = String((first + "-x").suffix(50))
            try await client.updateUser(userID, options: UserUpdateOptions(username: renamed, status: .inactive))
            let updated = try #require(try await client.getUser(userID))
            #expect(updated.username == renamed)
            #expect(updated.status == .inactive)
            #expect(updated.options.isEmpty)
            #expect(try await client.getUsers().first { $0.id == created.id }?.username == renamed)

            // A taken username refuses the whole patch: the status stays.
            await expectReplicatedIggyError(.userAlreadyExists) {
                try await client.updateUser(otherID, options: UserUpdateOptions(username: renamed, status: .inactive))
            }
            let untouched = try #require(try await client.getUser(otherID))
            #expect(untouched.username == second)
            #expect(untouched.status == .active)

            await expectIggyError(.optionNotResettable) { try await client.updateUser(userID, options: UserUpdateOptions(reset: [.status])) }
            await expectIggyError(.invalidCommand) { try await client.updateUser(userID, options: UserUpdateOptions()) }
            await expectIggyError(.invalidUsername) { try await client.updateUser(userID, options: UserUpdateOptions(username: "ab")) }
            await expectReplicatedIggyError(.readOnlyOption) {
                try await client.updateUser(userID, options: UserUpdateOptions(rawReset: ["critical_keys"]))
            }
            try await client.deleteUser(userID)
            try await client.deleteUser(otherID)
        }
    }

    @Test func givenLoggedInUserRenameWhenReconnectingShouldKeepNewUsername() async throws {
        try await E2EEnvironment.withSession() { session in
            let root = session.client
            let username = String(E2EEnvironment.uniqueName("swift-self").prefix(50))
            let permissions = Permissions(global: GlobalPermissions(manageUsers: true, readUsers: true, manageStreams: true, readStreams: true))
            let user = try await root.createUser(username: username, password: "secret-1", permissions: permissions)
            let userID = Identifier(numeric: user.id)

            let client = IggyClient(
                configuration: E2EEnvironment.configuration(autoLogin: .usernamePassword(username: username, password: "secret-1")))
            do {
                try await client.connect()
                let renamed = String((username + "-r").suffix(50))
                try await client.updateUser(userID, options: UserUpdateOptions(username: renamed))
                await client.disconnect()
                try await client.connect()
                #expect(try await client.getMe().userID == user.id)

                // A refused rename keeps the credentials that work.
                await expectReplicatedIggyError(.userAlreadyExists) {
                    try await client.updateUser(userID, options: UserUpdateOptions(username: E2EEnvironment.rootUsername))
                }
                await client.disconnect()
                try await client.connect()
                #expect(try await client.getMe().userID == user.id)

                // Renaming a stream does not touch the credentials.
                let name = E2EEnvironment.uniqueName("swift-self-stream")
                let stream = try await client.createStream(name: name)
                let streamName = E2EEnvironment.uniqueName("swift-self-renamed")
                await session.track(stream: try Identifier(named: streamName))
                try await client.updateStream(Identifier(numeric: stream.id), options: StreamUpdateOptions(name: streamName))
                await client.disconnect()
                try await client.connect()
                #expect(try await client.getMe().userID == user.id)
                #expect(try await root.getUser(userID)?.username == renamed)
            } catch {
                await client.shutdown()
                try? await root.deleteUser(userID)
                throw error
            }
            await client.shutdown()
            try await root.deleteUser(userID)
        }
    }

    static func randomKey() -> Data {
        Data((0..<EncryptionKey.length).map { _ in UInt8.random(in: .min ... .max) })
    }

    static func isFingerprint(_ text: String) -> Bool {
        text.count == 16 && text.allSatisfy { $0.isHexDigit && !$0.isUppercase }
    }

    static func expectNoKeyMaterial(_ options: ResourceOptions, key: Data, sourceLocation: SourceLocation = #_sourceLocation) {
        #expect(neverReturnedKeys.isDisjoint(with: options.keys), sourceLocation: sourceLocation)
        let encoded = [key.base64EncodedString(), key.map { String(format: "%02x", $0) }.joined()]
        for (name, option) in options {
            #expect(option.value.bytes != Array(key), "\(name)", sourceLocation: sourceLocation)
            #expect(!encoded.contains { option.value.description.contains($0) }, "\(name)", sourceLocation: sourceLocation)
        }
    }
}

/// The encryption projection a stream read reports.
struct StreamProjection: Equatable {
    let source: StreamEncryptionSource?
    let keyFingerprint: String?
    let kekFingerprints: [String]
}

extension StreamInfo {
    var projection: StreamProjection {
        StreamProjection(source: encryptionKeySource, keyFingerprint: encryptionKeyFingerprint, kekFingerprints: encryptionKekFingerprints)
    }
}

extension StreamDetails {
    var projection: StreamProjection {
        StreamProjection(source: encryptionKeySource, keyFingerprint: encryptionKeyFingerprint, kekFingerprints: encryptionKekFingerprints)
    }
}
