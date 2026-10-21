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
import Testing

@testable import Iggy

/// The resource options contract every SDK shares, checked against
/// `Fixtures/resource_options.json` from the Rust encoders. The Swift encoder
/// and decoders are independent, so these bytes are the contract.
@Suite("Resource options match the shared fixture", .serialized)
struct ResourceOptionsTests {
    let fixture = ResourceOptionsFixture.shared

    @Test func givenInvalidKeyWhenEncodingShouldRejectBeforeNetworkIo() async throws {
        let key = Data([UInt8](hex: fixture.streamKey.keyHex))
        #expect(key.count == EncryptionKey.length)
        #expect(try EncryptionKey.decode(base64: fixture.streamKey.keyBase64) == key)
        #expect(try StreamCreateOptions(encryptionKey: key).encode().isEmpty == false)

        for invalid in fixture.streamKey.invalidKeys {
            #expect(invalid.error.code == IggyErrorCode.invalidEncryptionKey.rawValue, "\(invalid.name)")
            if let rawHex = invalid.input.rawHex {
                let raw = Data([UInt8](hex: rawHex))
                #expect(throws: IggyError(.invalidEncryptionKey), "\(invalid.name)") { try StreamCreateOptions(encryptionKey: raw).encode() }
                #expect(throws: IggyError(.invalidEncryptionKey), "\(invalid.name)") { try EncryptionKey.fingerprint(raw) }
            }
            if let base64 = invalid.input.base64 {
                #expect(throws: IggyError(.invalidEncryptionKey), "\(invalid.name)") { try EncryptionKey.decode(base64: base64) }
                #expect(throws: IggyError(.invalidEncryptionKey), "\(invalid.name)") { try EncryptionKey.fingerprint(base64: base64) }
            }
        }
        #expect(throws: IggyError(.invalidEncryptionKey)) { try EncryptionKey.decode(base64: "") }
        #expect(throws: IggyError(.invalidEncryptionKey)) { try StreamCreateOptions(encryptionKey: Data()).encode() }

        // Diagnostics the SDK produces never show the key, typed or raw.
        let keyed = StreamCreateOptions(encryptionKey: key, raw: [StreamOptionKey.encryptionKey.rawValue: fixture.streamKey.keyBase64])
        var dumped = ""
        dump(keyed, to: &dumped)
        let short = Data(key.prefix(31))
        var refusal = ""
        do {
            _ = try StreamCreateOptions(encryptionKey: short).encode()
        } catch {
            refusal = "\(error) \(String(reflecting: error))"
        }
        for rendering in [String(describing: keyed), String(reflecting: keyed), dumped, refusal] {
            #expect(rendering.contains("redacted") || rendering == refusal)
            for secret in [fixture.streamKey.keyHex, fixture.streamKey.keyBase64, String(fixture.streamKey.keyHex.prefix(16)), "\(Array(key))"] {
                #expect(!rendering.contains(secret), "\(rendering)")
            }
        }

        // A refused key never reaches the connection.
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = fakeServerClient(server)
        try await client.connect()
        for invalid in fixture.streamKey.invalidKeys {
            guard let rawHex = invalid.input.rawHex else { continue }
            await expectCode(.invalidEncryptionKey) {
                _ = try await client.createStream(name: "keyed", options: StreamCreateOptions(encryptionKey: Data([UInt8](hex: rawHex))))
            }
        }
        #expect(await server.requests.contains { $0.operation == .createStream } == false)
        await client.shutdown()
    }

    @Test func givenSharedVectorsWhenEncodingShouldMatchServerContract() async throws {
        #expect(fixture.schemaVersion == 1)
        let constants = fixture.constants
        #expect(ProtocolVersion.current.packed == constants.protocolVersion.packed)
        #expect(ProtocolVersion.current.description == constants.protocolVersion.text)
        #expect(constants.protocolVersionMin.packed == constants.protocolVersion.packed)

        // Command and operation mirrors.
        for (name, entry) in constants.commandCodes {
            let code = try #require(CommandCode(rawValue: entry.code), "\(name)")
            #expect(code.name.isEmpty == false)
            if let operation = entry.operation {
                #expect(ConsensusOperation.forCode(entry.code).rawValue == operation, "\(name)")
            } else {
                #expect(ConsensusOperation.forCode(entry.code) == .nonReplicated, "\(name)")
            }
        }
        #expect(CommandCode.updateResource.name == "resource.update")
        for (name, code) in constants.retiredCommandCodes {
            #expect(CommandCode(rawValue: code) == nil, "\(name)")
            #expect(ConsensusOperation.forCode(code) == .nonReplicated, "\(name)")
        }
        for (name, entry) in constants.operations {
            let operation = try #require(ConsensusOperation(rawValue: entry.value), "\(name)")
            #expect(operation.isInternal == entry.internal, "\(name)")
            #expect(operation.isMetadata, "\(name)")
            #expect(operation.isResultFramed, "\(name)")
            // A resolved operation answers the generic request it was rewritten from.
            #expect([entry.value, ConsensusOperation.updateResource.rawValue].contains(entry.requestOperation), "\(name)")
        }
        for (name, value) in constants.retiredOperations {
            #expect(ConsensusOperation(rawValue: value) == nil, "\(name)")
        }
        // The rewrap is server-originated: no command code ever maps to it.
        #expect(CommandCode.allCases.allSatisfy { ConsensusOperation.forCode($0.rawValue) != .rewrapStreamKeys })

        for (name, code) in constants.scopeCodes {
            #expect(scope(name)?.rawValue == code, "\(name)")
        }
        for (name, code) in constants.headerKindCodes {
            #expect(HeaderKind(name: name)?.rawValue == code, "\(name)")
        }
        let flags: [String: OptionFlags] = [
            "updatable": .updatable, "resettable": .resettable, "secret": .secret, "read_only": .readOnly, "critical": .critical,
        ]
        for (name, bit) in constants.optionFlags {
            #expect(flags[name]?.rawValue == bit, "\(name)")
        }
        for (name, code) in constants.userStatusCodes {
            #expect(userStatus(name)?.rawValue == code, "\(name)")
        }
        #expect(constants.streamKeyLength == EncryptionKey.length)

        // Typed keys: every described catalog key of a scope, in catalog
        // order, with the catalog's flags. Internal keys have no SDK surface.
        func expectParity<Key: CatalogOptionKey>(_: Key.Type, scope: String) {
            let rows = fixture.catalog.filter { $0.described && $0.scopes.contains(scope) }
            #expect(Key.allCases.map(\.rawValue) == rows.map(\.key), "\(scope)")
            for row in rows {
                #expect(Key(rawValue: row.key)?.flags.rawValue == row.flags, "\(scope) \(row.key)")
            }
            for row in fixture.catalog where row.scopes.contains(scope) && !row.described {
                #expect(Key(rawValue: row.key) == nil, "\(scope) \(row.key)")
            }
        }
        expectParity(StreamOptionKey.self, scope: "stream")
        expectParity(TopicOptionKey.self, scope: "topic")
        expectParity(UserOptionKey.self, scope: "user")

        // DescribeOptions.
        for vector in fixture.describeOptions {
            #expect(Requests.describeOptions(scope: try #require(scope(vector.scope))) == [UInt8](hex: vector.requestHex))
            let specs = try Responses.optionSpecs([UInt8](hex: vector.responseHex)[...])
            #expect(specs.map(\.key) == vector.entries.map(\.key), "\(vector.scope)")
            for (spec, entry) in zip(specs, vector.entries) {
                #expect(spec.kind.description == entry.kind, "\(entry.key)")
                #expect(spec.flags.rawValue == entry.flags, "\(entry.key)")
                if let defaultValue = entry.defaultValue {
                    #expect(text(HeaderValue(uncheckedKind: spec.kind, bytes: spec.defaultValue)) == defaultValue, "\(entry.key)")
                } else {
                    #expect(spec.defaultValue.isEmpty, "\(entry.key)")
                }
            }
        }

        // Fingerprint.
        let key = Data([UInt8](hex: fixture.streamKey.keyHex))
        #expect(try EncryptionKey.fingerprint(key) == fixture.streamKey.fingerprint)
        #expect(try EncryptionKey.fingerprint(base64: fixture.streamKey.keyBase64) == fixture.streamKey.fingerprint)

        // UpdateResource, through the typed update values.
        for vector in fixture.updateResource {
            let body = try updateBody(scope: vector.scope, target: vector.target, input: vector.input)
            #expect(body == [UInt8](hex: vector.bodyHex), "\(vector.name)")
        }
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = fakeServerClient(server)
        try await client.connect()
        for vector in fixture.updateResourceErrors {
            let code = try #require(IggyErrorCode(rawValue: vector.error.code))
            #expect(code.name == vector.error.name, "\(vector.name)")
            // A name with no typed reset member, an internal key among them,
            // travels as a raw reset name and the server refuses it.
            if !rawResetNames(scope: vector.scope, vector.input).isEmpty {
                let body = try updateBody(scope: vector.scope, target: vector.target, input: vector.input)
                #expect(body.isEmpty == false, "\(vector.name)")
                continue
            }
            #expect(throws: IggyError(code), "\(vector.name)") {
                try updateBody(scope: vector.scope, target: vector.target, input: vector.input)
            }
            await expectCode(code) { try await update(client, scope: vector.scope, target: vector.target, input: vector.input) }
        }
        #expect(await server.requests.contains { $0.operation == .updateResource } == false)
        await client.shutdown()

        // Creates.
        for vector in fixture.createStream {
            let key = vector.input.encryptionKey?.rawHex.map { Data([UInt8](hex: $0)) }
            let options = StreamCreateOptions(encryptionKey: key, raw: vector.input.raw ?? [:])
            #expect(try Requests.createStream(name: vector.input.name, options: options.encode()) == [UInt8](hex: vector.bodyHex), "\(vector.name)")
        }
        for vector in fixture.createTopic {
            let input = vector.input
            let options = TopicCreateOptions(
                partitionsCount: input.partitionsCount, compressionAlgorithm: CompressionAlgorithm(name: input.compressionAlgorithm),
                messageExpiry: IggyExpiry(wireMicroseconds: try #require(UInt64(input.messageExpiry))),
                maxTopicSize: MaxTopicSize(wireValue: try #require(UInt64(input.maxTopicSize))), segmentSizeBytes: UInt64(input.segmentSize),
                durability: try #require(Durability(rawValue: input.durability)),
                consumerOffsetDurability: try #require(Durability(rawValue: input.consumerOffsetDurability)))
            let body = try Requests.createTopic(
                streamID: identifier(input.streamID), partitionsCount: input.partitionsCount, name: input.name, options: options.encode())
            #expect(body == [UInt8](hex: vector.bodyHex), "\(vector.name)")
        }
        for vector in fixture.createUser {
            let input = vector.input
            let body = try Requests.createUser(
                username: input.username, password: input.password, status: try #require(userStatus(input.status)), permissions: nil,
                options: UserCreateOptions().encode())
            #expect(body == [UInt8](hex: vector.bodyHex), "\(vector.name)")
        }

        // Responses and the typed accessors.
        let neverReturned = Set(constants.neverReturnedKeys)
        for vector in fixture.streamResponses {
            let stream = try Responses.streamDetails([UInt8](hex: vector.responseHex)[...])
            let expected = vector.expected
            #expect(stream.id == expected.id)
            #expect(stream.createdAt.microseconds == expected.createdAt)
            #expect(stream.name == expected.name)
            #expect(stream.topicsCount == expected.topicsCount)
            #expect(stream.sizeBytes == expected.sizeBytes)
            #expect(stream.messagesCount == expected.messagesCount)
            #expect(fixtureForm(stream.options) == expected.options, "\(vector.name)")
            #expect(stream.encryptionKeySource?.rawValue == expected.encryptionKeySource, "\(vector.name)")
            #expect(stream.encryptionKeyFingerprint == expected.encryptionKeyFingerprint, "\(vector.name)")
            #expect(stream.encryptionKekFingerprints == expected.encryptionKekFingerprints, "\(vector.name)")
            #expect(neverReturned.isDisjoint(with: stream.options.keys), "\(vector.name)")
            let listed = try #require(try Responses.streams([UInt8](hex: vector.responseHex)[...]).first)
            #expect(listed.options == stream.options)
            #expect(listed.encryptionKeySource == stream.encryptionKeySource)
            #expect(listed.encryptionKeyFingerprint == stream.encryptionKeyFingerprint)
            #expect(listed.encryptionKekFingerprints == stream.encryptionKekFingerprints)
        }
        for vector in fixture.topicResponses {
            let topic = try Responses.topicDetails([UInt8](hex: vector.responseHex)[...])
            let expected = vector.expected
            #expect(topic.id == expected.id)
            #expect(topic.createdAt.microseconds == expected.createdAt)
            #expect(topic.name == expected.name)
            #expect(topic.partitionsCount == expected.partitionsCount)
            #expect(topic.sizeBytes == expected.sizeBytes)
            #expect(topic.messagesCount == expected.messagesCount)
            #expect(fixtureForm(topic.options) == expected.options, "\(vector.name)")
            #expect(topic.compressionAlgorithm.description == expected.compressionAlgorithm)
            #expect(try topic.messageExpiry.wireMicroseconds().map(String.init) == expected.messageExpiry)
            #expect(String(topic.maxTopicSize.wireValue) == expected.maxTopicSize)
            #expect(neverReturned.isDisjoint(with: topic.options.keys))
        }
        for vector in fixture.userResponses {
            let user = try Responses.userDetails([UInt8](hex: vector.responseHex)[...])
            let expected = vector.expected
            #expect(user.id == expected.id)
            #expect(user.createdAt.microseconds == expected.createdAt)
            #expect(user.status.description == expected.status)
            #expect(user.username == expected.username)
            #expect(user.permissions == nil)
            #expect(fixtureForm(user.options) == expected.options, "\(vector.name)")
            #expect(neverReturned.isDisjoint(with: user.options.keys))
        }
    }

    /// Every public catalog key has its typed surface: a create field per
    /// create-only or updatable map key, an update field per updatable key,
    /// a reset member per resettable key, and an accessor per read-only key.
    @Test func givenFixtureCatalogWhenBuildingTypedOptionsShouldCoverEveryPublicKey() throws {
        func rows(_ scope: String, _ matches: (ResourceOptionsFixture.CatalogRow) -> Bool) -> Set<String> {
            Set(fixture.catalog.filter { $0.scopes.contains(scope) && matches($0) }.map(\.key))
        }
        func creatable(_ scope: String) -> Set<String> {
            rows(scope) { $0.storage == "map" && ["public", "secret"].contains($0.visibility) && $0.mutability != "read_only" }
        }
        func updatable(_ scope: String) -> Set<String> {
            rows(scope) { $0.described && OptionFlags(rawValue: $0.flags).contains(.updatable) }
        }
        func resettable(_ scope: String) -> Set<String> {
            rows(scope) { $0.described && OptionFlags(rawValue: $0.flags).contains(.resettable) }
        }
        func readOnly(_ scope: String) -> Set<String> {
            rows(scope) { $0.described && OptionFlags(rawValue: $0.flags).contains(.readOnly) }
        }

        let streamCreate = StreamCreateOptions(encryptionKey: Data([UInt8](hex: fixture.streamKey.keyHex)))
        let topicCreate = TopicCreateOptions(
            partitionsCount: 3, compressionAlgorithm: .gzip, messageExpiry: .never, maxTopicSize: .bytes(1 << 30), segmentSizeBytes: 1 << 20,
            durability: .persisted, consumerOffsetDurability: .persisted, partitionResizePolicy: .fixed, messagesRequiredToSave: 10,
            sizeOfMessagesRequiredToSaveBytes: 1 << 10, preallocateSegments: true)
        #expect(unsetFields(streamCreate, except: ["raw"]).isEmpty)
        #expect(unsetFields(topicCreate, except: ["raw"]).isEmpty)
        #expect(Set(try streamCreate.toResourceOptions().keys) == creatable("stream"))
        #expect(Set(try topicCreate.toResourceOptions().keys) == creatable("topic"))
        #expect(creatable("user").isEmpty, "a user create key needs a typed field on UserCreateOptions")

        let streamUpdate = StreamUpdateOptions(name: "renamed")
        let topicUpdate = TopicUpdateOptions(name: "renamed", messageExpiry: .never, maxTopicSize: .bytes(1 << 30))
        let userUpdate = UserUpdateOptions(username: "renamed", status: .inactive)
        let untyped: Set<String> = ["reset", "raw", "rawReset"]
        #expect(unsetFields(streamUpdate, except: untyped).isEmpty)
        #expect(unsetFields(topicUpdate, except: untyped).isEmpty)
        #expect(unsetFields(userUpdate, except: untyped).isEmpty)
        #expect(Set(try streamUpdate.toPatch().set.keys) == updatable("stream"))
        #expect(Set(try topicUpdate.toPatch().set.keys) == updatable("topic"))
        #expect(Set(try userUpdate.toPatch().set.keys) == updatable("user"))

        func resetMembers<Key: CatalogOptionKey>(_: Key.Type) -> Set<String> {
            Set(Key.allCases.filter { $0.flags.contains(.resettable) }.map(\.rawValue))
        }
        #expect(resetMembers(StreamOptionKey.self) == resettable("stream"))
        #expect(resetMembers(TopicOptionKey.self) == resettable("topic"))
        #expect(resetMembers(UserOptionKey.self) == resettable("user"))
        let resets = Set(TopicOptionKey.allCases.filter { $0.flags.contains(.resettable) })
        #expect(Set(try TopicUpdateOptions(reset: resets).toPatch().reset) == resettable("topic"))

        let streamAccessors: [String: (StreamDetails) -> Bool] = [
            StreamOptionKey.encryptionKeySource.rawValue: { $0.encryptionKeySource != nil },
            StreamOptionKey.encryptionKeyFingerprint.rawValue: { $0.encryptionKeyFingerprint != nil },
            StreamOptionKey.encryptionKekFingerprint.rawValue: { !$0.encryptionKekFingerprints.isEmpty },
        ]
        #expect(Set(streamAccessors.keys) == readOnly("stream"))
        #expect(readOnly("topic").isEmpty, "a read-only topic key needs an accessor on Topic and TopicDetails")
        #expect(readOnly("user").isEmpty, "a read-only user key needs an accessor on UserInfo and UserInfoDetails")
        for vector in fixture.streamResponses {
            let stream = try Responses.streamDetails([UInt8](hex: vector.responseHex)[...])
            for (key, reads) in streamAccessors where stream.options[key] != nil {
                #expect(reads(stream), "\(vector.name) \(key)")
            }
        }
    }

    /// A raw key or reset name may be one a newer server knows, so only the
    /// shape of such a patch is checked here. A typed-only patch is refused
    /// with the category the server would return, values last.
    @Test func givenRawKeysWhenPatchingShouldLeaveTheCategoryChecksToTheServer() throws {
        let segment = try TopicUpdateOptions(reset: [.name], raw: [TopicOptionKey.segmentSize.rawValue: "1 GiB"]).toPatch()
        #expect(segment.reset == [TopicOptionKey.name.rawValue])
        #expect(segment.set[TopicOptionKey.segmentSize.rawValue] == .explicit("1 GiB"))
        let source = try StreamUpdateOptions(reset: [.name], raw: [StreamOptionKey.encryptionKeySource.rawValue: "x"]).toPatch()
        #expect(source.reset == [StreamOptionKey.name.rawValue])
        let internalReset = try UserUpdateOptions(rawReset: ["critical_keys"]).toPatch()
        #expect(internalReset.reset == ["critical_keys"] && internalReset.set.isEmpty)
        // Typed and raw reset names travel together in byte order.
        let mixed = try TopicUpdateOptions(reset: [.messageExpiry], rawReset: ["future_retention", "message_expiry"]).toPatch()
        #expect(mixed.reset == ["future_retention", "message_expiry"])

        #expect(throws: IggyError(.invalidCommand)) { try TopicUpdateOptions(raw: ["future": "x"], rawReset: ["future"]).toPatch() }
        #expect(throws: IggyError(.invalidCommand)) { try StreamUpdateOptions(name: "renamed", rawReset: ["name"]).toPatch() }
        #expect(throws: IggyError(.unsupportedOptionKey)) { try UserUpdateOptions(rawReset: [""]).toPatch() }
        #expect(throws: IggyError(.unsupportedOptionKey)) { try UserUpdateOptions(rawReset: [String(repeating: "k", count: 256)]).toPatch() }
        let tooMany = Set((0...OptionsPatch.maxResetKeys).map { "future_\($0)" })
        #expect(throws: IggyError(.unsupportedOptionKey)) { try TopicUpdateOptions(rawReset: tooMany).toPatch() }

        #expect(throws: IggyError(.invalidCommand)) { try UserUpdateOptions(username: "ab", reset: [.username]).toPatch() }
        #expect(throws: IggyError(.optionNotResettable)) { try UserUpdateOptions(username: "ab", reset: [.status]).toPatch() }
        #expect(throws: IggyError(.invalidUsername)) { try UserUpdateOptions(username: "ab").toPatch() }
        #expect(throws: IggyError(.optionNotUpdatable)) { try TopicUpdateOptions(name: "", reset: [.compressionAlgorithm]).toPatch() }
    }

    /// The primary commits an `UpdateResource` under its scope's resolved
    /// operation, so the reply names 72, 73 or 74, never the request's 150.
    @Test func givenResolvedUpdateReplyWhenCorrelatingShouldAcceptOnlyMatchingRequestIdentity() async throws {
        let golden = GoldenFixture.shared
        let header = golden.bytes("vsr.reply.update_topic_resolved")[...]
        #expect(VSRFrame.readReplyOperation(header) == .updateTopicResolved)
        #expect(VSRFrame.hasCommittedResult(header))
        #expect(try VSRFrame.decodeReply(header: header, body: UInt32(0).littleEndianBytes[...]).isEmpty)

        let resolved: [ConsensusOperation] = [.updateStreamResolved, .updateTopicResolved, .updateUserResolved]
        #expect(resolved.map(\.rawValue) == [72, 73, 74])
        #expect(ConsensusOperation.rewrapStreamKeys.rawValue == 75)
        #expect(ConsensusOperation.rewrapStreamKeys.isInternal)
        for operation in resolved {
            #expect(operation.isInternal)
            #expect(operation.isMetadata)
            #expect(operation.isResultFramed)
        }

        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = fakeServerClient(server)
        try await client.connect()
        for operation in resolved {
            // The first reply answers another request id: the client drops the
            // connection and sends the same request again on the resumed
            // session, whose resolved reply it accepts.
            let misrouted = Locked(true)
            await server.respond { request in
                if let reply = FakeVSRServer.sessionReply(request) { return reply }
                guard request.operation == .updateResource else { return FakeVSRServer.defaultReply(request, address: server.address) }
                return .resolved(operation, requestID: misrouted.exchange(false) ? request.requestID + 7 : nil)
            }
            let connections = await server.connectionCount
            try await client.updateStream(1, options: StreamUpdateOptions(name: "renamed"))
            #expect(await server.connectionCount == connections + 1)
            let attempts = await server.requests.filter { $0.operation == .updateResource }.suffix(2)
            #expect(attempts.count == 2 && Set(attempts.map(\.requestID)).count == 1)
            #expect(Set(attempts.map(\.checksum)).count == 1)
            try await client.updateTopic(streamID: 1, topicID: 2, options: TopicUpdateOptions(reset: [.maxTopicSize]))
            try await client.updateUser(9, options: UserUpdateOptions(status: .inactive))
        }
        let updates = await server.requests.filter { $0.operation == .updateResource }
        #expect(updates.count == 4 * resolved.count)
        #expect(
            updates.allSatisfy {
                $0.code == CommandCode.updateResource.rawValue
                    && $0.checksum != 0
                    && $0.checksum != XXH3.hash64($0.payload)
            })
        await client.shutdown()
    }

    /// Entries of a kind this build does not know are skipped, so a resource
    /// a newer server wrote still reads back. A known kind with a malformed
    /// value fails the read instead of being reinterpreted.
    @Test func givenUnknownOptionKindWhenDecodingShouldPreserveSupportedValues() throws {
        let derived =
            optionEntry("encryption_key_fingerprint", kind: HeaderKind.string.rawValue, value: Array("8441e38538cbfa57".utf8))
            + optionEntry("encryption_key_source", kind: HeaderKind.string.rawValue, value: Array("stream".utf8))
            + optionEntry("future_vector", kind: 99, value: [1, 2, 3])
        let explicit = optionEntry("future_option", kind: HeaderKind.string.rawValue, value: Array("enabled".utf8))
        let stream = try Responses.streamDetails(streamResponse(explicit: explicit, derived: derived)[...])
        #expect(stream.options.keys.sorted() == ["encryption_key_fingerprint", "encryption_key_source", "future_option"])
        #expect(stream.options["future_option"] == .explicit("enabled"))
        #expect(stream.encryptionKeySource == .stream)
        #expect(stream.encryptionKeyFingerprint == "8441e38538cbfa57")
        #expect(stream.encryptionKekFingerprints.isEmpty)

        let malformed = optionEntry("future_quota", kind: HeaderKind.uint64.rawValue, value: [1, 2, 3, 4])
        #expect(throws: IggyError(.invalidHeaderValue)) {
            try Responses.streamDetails(streamResponse(explicit: [], derived: derived + malformed)[...])
        }
        let unknownSource = optionEntry("encryption_key_source", kind: HeaderKind.string.rawValue, value: Array("vault".utf8))
        let future = try Responses.streamDetails(streamResponse(explicit: [], derived: unknownSource)[...])
        #expect(future.options["encryption_key_source"] == .derived("vault"))
        #expect(future.encryptionKeySource == nil)
    }

    // MARK: Helpers

    func fakeServerClient(_ server: FakeVSRServer) -> IggyClient {
        var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
        configuration.reconnection = ReconnectionOptions(enabled: true, interval: .milliseconds(20), reestablishAfter: .zero)
        configuration.connectTimeout = .seconds(2)
        configuration.heartbeatInterval = .seconds(60)
        return IggyClient(configuration: configuration, connector: NIOConnector(), logger: testLogger)
    }

    func scope(_ name: String) -> OptionsScope? {
        [OptionsScope.topic, .stream, .user].first { "\($0)" == name }
    }

    func userStatus(_ name: String) -> UserStatus? {
        [UserStatus.active, .inactive].first { $0.description == name }
    }

    func identifier(_ id: ResourceOptionsFixture.TargetID) throws -> Identifier {
        switch id {
        case .numeric(let value): Identifier(numeric: value)
        case .named(let name): try Identifier(named: name)
        }
    }

    func target(_ ids: [String: ResourceOptionsFixture.TargetID], _ name: String) throws -> Identifier {
        try identifier(try #require(ids[name], "missing \(name)"))
    }

    func topicUpdate(_ input: ResourceOptionsFixture.UpdateInput) throws -> TopicUpdateOptions {
        TopicUpdateOptions(
            name: input.name, messageExpiry: try input.messageExpiry.map { IggyExpiry(wireMicroseconds: try #require(UInt64($0))) },
            maxTopicSize: try input.maxTopicSize.map { MaxTopicSize(wireValue: try #require(UInt64($0))) },
            reset: Set((input.reset ?? []).compactMap(TopicOptionKey.init(rawValue:))), rawReset: rawResetNames(TopicOptionKey.self, input))
    }

    func streamUpdate(_ input: ResourceOptionsFixture.UpdateInput) throws -> StreamUpdateOptions {
        StreamUpdateOptions(
            name: input.name, reset: Set((input.reset ?? []).compactMap(StreamOptionKey.init(rawValue:))),
            rawReset: rawResetNames(StreamOptionKey.self, input))
    }

    func userUpdate(_ input: ResourceOptionsFixture.UpdateInput) throws -> UserUpdateOptions {
        UserUpdateOptions(
            username: input.username, status: try input.status.map { try #require(userStatus($0)) },
            reset: Set((input.reset ?? []).compactMap(UserOptionKey.init(rawValue:))), rawReset: rawResetNames(UserOptionKey.self, input))
    }

    /// Reset names of the input this SDK has no typed member for.
    func rawResetNames<Key: CatalogOptionKey>(_: Key.Type, _ input: ResourceOptionsFixture.UpdateInput) -> Set<String> {
        Set((input.reset ?? []).filter { Key(rawValue: $0) == nil })
    }

    func rawResetNames(scope: String, _ input: ResourceOptionsFixture.UpdateInput) -> Set<String> {
        switch scope {
        case "topic": rawResetNames(TopicOptionKey.self, input)
        case "stream": rawResetNames(StreamOptionKey.self, input)
        default: rawResetNames(UserOptionKey.self, input)
        }
    }

    func updateBody(scope: String, target ids: [String: ResourceOptionsFixture.TargetID], input: ResourceOptionsFixture.UpdateInput) throws -> [UInt8] {
        switch scope {
        case "topic":
            try Requests.updateResource(.topic(stream: target(ids, "stream_id"), topic: target(ids, "topic_id")), topicUpdate(input).toPatch())
        case "stream":
            try Requests.updateResource(.stream(target(ids, "stream_id")), streamUpdate(input).toPatch())
        default:
            try Requests.updateResource(.user(target(ids, "user_id")), userUpdate(input).toPatch())
        }
    }

    func update(
        _ client: IggyClient, scope: String, target ids: [String: ResourceOptionsFixture.TargetID], input: ResourceOptionsFixture.UpdateInput
    )
        async throws
    {
        switch scope {
        case "topic":
            try await client.updateTopic(streamID: target(ids, "stream_id"), topicID: target(ids, "topic_id"), options: topicUpdate(input))
        case "stream":
            try await client.updateStream(target(ids, "stream_id"), options: streamUpdate(input))
        default:
            try await client.updateUser(target(ids, "user_id"), options: userUpdate(input))
        }
    }

    /// Stored properties of `value` left nil, so a new typed field cannot
    /// slip past the surface checks unset.
    func unsetFields(_ value: Any, except skipped: Set<String>) -> [String] {
        Mirror(reflecting: value).children.compactMap { child in
            guard let label = child.label, !skipped.contains(label) else { return nil }
            let field = Mirror(reflecting: child.value)
            return field.displayStyle == .optional && field.children.isEmpty ? label : nil
        }
    }

    /// An option value in the fixture's reversible text form.
    func text(_ value: HeaderValue) -> String {
        switch value.kind {
        case .raw: Data(value.bytes).base64EncodedString()
        default: value.description
        }
    }

    func fixtureForm(_ options: ResourceOptions) -> [String: ResourceOptionsFixture.OptionEntry] {
        options.mapValues { ResourceOptionsFixture.OptionEntry(value: text($0.value), kind: $0.value.kind.description, explicit: $0.explicit) }
    }

    func optionEntry(_ key: String, kind: UInt8, value: [UInt8]) -> [UInt8] {
        var writer = ByteWriter()
        writer.write(HeaderKind.string.rawValue)
        writer.write(UInt32(key.utf8.count))
        writer.write(Array(key.utf8))
        writer.write(kind)
        writer.write(UInt32(value.count))
        writer.write(value)
        return writer.bytes
    }

    /// A `GetStream` reply for a stream without topics.
    func streamResponse(explicit: [UInt8], derived: [UInt8]) throws -> [UInt8] {
        var writer = ByteWriter()
        writer.write(UInt32(9))
        writer.write(UInt64(1_710_000_000_000_000))
        writer.write(UInt32(0))
        writer.write(UInt64(0))
        writer.write(UInt64(0))
        try writer.writeName("future-stream")
        writer.write(UInt32(explicit.count))
        writer.write(explicit)
        writer.write(UInt32(derived.count))
        writer.write(derived)
        return writer.bytes
    }
}
