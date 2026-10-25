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

/// Request payload encoders, one per command, mirroring
/// `core/binary_protocol/src/requests`. Every function returns the body that
/// follows the consensus header.
enum Requests {
    // MARK: Streams

    static func createStream(name: String, options: [UInt8] = []) throws -> [UInt8] {
        var writer = ByteWriter()
        try writer.writeName(validatedWireName(name))
        writer.write(options)
        return writer.bytes
    }

    static func identifierOnly(_ id: Identifier) throws -> [UInt8] {
        var writer = ByteWriter(capacity: id.encodedSize)
        try id.encode(into: &writer)
        return writer.bytes
    }

    // MARK: Topics

    static func streamAndTopic(_ streamID: Identifier, _ topicID: Identifier) throws -> [UInt8] {
        var writer = ByteWriter(capacity: streamID.encodedSize + topicID.encodedSize)
        try streamID.encode(into: &writer)
        try topicID.encode(into: &writer)
        return writer.bytes
    }

    static func createTopic(streamID: Identifier, partitionsCount: UInt32, name: String, options: [UInt8]) throws -> [UInt8] {
        var writer = ByteWriter()
        try streamID.encode(into: &writer)
        writer.write(partitionsCount)
        try writer.writeName(validatedWireName(name))
        writer.write(options)
        return writer.bytes
    }

    // MARK: Resource updates

    /// What an `UpdateResource` patches, tagged on the wire with its options
    /// scope.
    enum ResourceTarget {
        case topic(stream: Identifier, topic: Identifier)
        case stream(Identifier)
        case user(Identifier)
    }

    /// `[scope:u8][target ids][reset_count:u8][reset_key:name]*[options block]`
    static func updateResource(_ target: ResourceTarget, _ patch: OptionsPatch) throws -> [UInt8] {
        var writer = ByteWriter()
        switch target {
        case .topic(let stream, let topic):
            writer.write(OptionsScope.topic.rawValue)
            try stream.encode(into: &writer)
            try topic.encode(into: &writer)
        case .stream(let stream):
            writer.write(OptionsScope.stream.rawValue)
            try stream.encode(into: &writer)
        case .user(let user):
            writer.write(OptionsScope.user.rawValue)
            try user.encode(into: &writer)
        }
        writer.write(UInt8(patch.reset.count))
        for key in patch.reset {
            try writer.writeName(key)
        }
        writer.write(try OptionsBlock.encode(patch.set))
        return writer.bytes
    }

    // MARK: Partitions and segments

    static func partitions(streamID: Identifier, topicID: Identifier, partitionsCount: UInt32) throws -> [UInt8] {
        var writer = ByteWriter()
        try streamID.encode(into: &writer)
        try topicID.encode(into: &writer)
        writer.write(partitionsCount)
        return writer.bytes
    }

    static func deleteSegments(streamID: Identifier, topicID: Identifier, partitionID: UInt32, segmentsCount: UInt32) throws -> [UInt8] {
        var writer = ByteWriter()
        try streamID.encode(into: &writer)
        try topicID.encode(into: &writer)
        writer.write(partitionID)
        writer.write(segmentsCount)
        return writer.bytes
    }

    // MARK: Consumer groups

    static func createConsumerGroup(streamID: Identifier, topicID: Identifier, name: String) throws -> [UInt8] {
        var writer = ByteWriter()
        try streamID.encode(into: &writer)
        try topicID.encode(into: &writer)
        try writer.writeName(validatedWireName(name))
        return writer.bytes
    }

    static func consumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) throws -> [UInt8] {
        var writer = ByteWriter()
        try streamID.encode(into: &writer)
        try topicID.encode(into: &writer)
        try groupID.encode(into: &writer)
        return writer.bytes
    }

    // MARK: Consumer offsets

    private static func consumerTarget(
        _ consumer: Consumer, _ streamID: Identifier, _ topicID: Identifier, _ partitionID: UInt32?, into writer: inout ByteWriter
    ) throws {
        try consumer.encode(into: &writer)
        try streamID.encode(into: &writer)
        try topicID.encode(into: &writer)
        if let partitionID {
            writer.write(UInt8(1))
            writer.write(partitionID)
        } else {
            writer.write(UInt8(0))
            writer.write(UInt32(0))
        }
    }

    static func storeConsumerOffset(
        consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, offset: UInt64, ack: AckLevel
    ) throws -> [UInt8] {
        var writer = ByteWriter()
        try consumerTarget(consumer, streamID, topicID, partitionID, into: &writer)
        writer.write(offset)
        writer.write(ack.rawValue)
        return writer.bytes
    }

    static func getConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?) throws -> [UInt8] {
        var writer = ByteWriter()
        try consumerTarget(consumer, streamID, topicID, partitionID, into: &writer)
        return writer.bytes
    }

    static func deleteConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, ack: AckLevel) throws -> [UInt8] {
        var writer = ByteWriter()
        try consumerTarget(consumer, streamID, topicID, partitionID, into: &writer)
        writer.write(ack.rawValue)
        return writer.bytes
    }

    // MARK: Messages

    static func pollMessages(
        consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, strategy: PollingStrategy, count: UInt32, autoCommit: Bool
    ) throws -> [UInt8] {
        var writer = ByteWriter()
        try consumerTarget(consumer, streamID, topicID, partitionID, into: &writer)
        strategy.encode(into: &writer)
        writer.write(count)
        writer.write(autoCommit)
        return writer.bytes
    }

    static func partitionRoute(streamID: Identifier, topicID: Identifier, partitionID: UInt32) throws -> [UInt8] {
        var writer = ByteWriter()
        try streamID.encode(into: &writer)
        try topicID.encode(into: &writer)
        writer.write(partitionID)
        return writer.bytes
    }

    // MARK: Users

    /// Byte length of a bind secret carried by login and bind-session requests.
    static let bindSecretSize = 32

    static let minUsernameLength = 3
    static let maxUsernameLength = 50
    static let minPasswordLength = 3
    static let maxPasswordLength = 100

    /// The bounds every server enforces, applied before encoding so an
    /// oversized value can never desync the length prefix on the wire.
    static func validateUsername(_ username: String) throws {
        let length = username.utf8.count
        guard length >= minUsernameLength, length <= maxUsernameLength else {
            throw IggyError(.invalidUsername, context: "username must be \(minUsernameLength)-\(maxUsernameLength) bytes, got \(length)")
        }
    }

    static func validatePassword(_ password: String) throws {
        let length = password.utf8.count
        guard length >= minPasswordLength, length <= maxPasswordLength else {
            throw IggyError(.invalidPassword, context: "password must be \(minPasswordLength)-\(maxPasswordLength) bytes, got \(length)")
        }
    }

    static func createUser(username: String, password: String, status: UserStatus, permissions: Permissions?, options: [UInt8] = []) throws -> [UInt8] {
        try validateUsername(username)
        try validatePassword(password)
        var writer = ByteWriter()
        try writer.writeName(username)
        try writer.writeName(password)
        writer.write(status.rawValue)
        writePermissions(permissions, into: &writer)
        writer.write(options)
        return writer.bytes
    }

    private static func writePermissions(_ permissions: Permissions?, into writer: inout ByteWriter) {
        if let permissions {
            writer.write(UInt8(1))
            let bytes = permissions.encode()
            writer.write(UInt32(bytes.count))
            writer.write(bytes)
        } else {
            writer.write(UInt8(0))
        }
    }

    /// The user a user-scope ``updateResource(_:_:)`` payload names and the
    /// name it sets, nil when it patches another scope or keeps the name.
    static func renamedUser(_ payload: [UInt8]) throws -> (userID: Identifier, username: String)? {
        guard payload.first == OptionsScope.user.rawValue else {
            return nil
        }
        return try Responses.decode(payload.dropFirst()) { reader in
            let userID = try Identifier.decode(from: &reader)
            for _ in 0..<(try reader.readUInt8()) {
                _ = try reader.readName()
            }
            let options = try OptionsBlock.decode(try reader.readBytes(reader.remaining), explicit: true)
            return options[UserOptionKey.username.rawValue]?.value.stringValue.map { (userID, $0) }
        }
    }

    static func updatePermissions(userID: Identifier, permissions: Permissions?) throws -> [UInt8] {
        var writer = ByteWriter()
        try userID.encode(into: &writer)
        writePermissions(permissions, into: &writer)
        return writer.bytes
    }

    static func changePassword(userID: Identifier, currentPassword: String, newPassword: String) throws -> [UInt8] {
        try validatePassword(currentPassword)
        try validatePassword(newPassword)
        var writer = ByteWriter()
        try userID.encode(into: &writer)
        try writer.writeName(currentPassword)
        try writer.writeName(newPassword)
        return writer.bytes
    }

    /// The user a ``changePassword(userID:currentPassword:newPassword:)``
    /// payload names and its new password.
    static func changedPassword(_ payload: [UInt8]) throws -> (userID: Identifier, newPassword: String) {
        try Responses.decode(payload[...]) { reader in
            let userID = try Identifier.decode(from: &reader)
            _ = try reader.readName()
            return (userID, try reader.readName())
        }
    }

    /// `[protocol_version:u32][sdk_name][sdk_version]`, the prefix of both
    /// login-register request shapes.
    static func versionInfo(into writer: inout ByteWriter) throws {
        writer.write(ProtocolVersion.current.packed)
        try writer.writeName(IggyVersion.sdkName)
        try writer.writeName(IggyVersion.sdkVersion)
    }

    static func loginRegister(username: String, password: String, bindSecret: [UInt8]) throws -> [UInt8] {
        try validateUsername(username)
        try validatePassword(password)
        guard bindSecret.count == bindSecretSize else {
            throw IggyError(.invalidFormat, context: "bind secret must be exactly \(bindSecretSize) bytes")
        }
        var writer = ByteWriter()
        try versionInfo(into: &writer)
        writer.write(bindSecret)
        try writer.writeName(username)
        try writer.writeName(password)
        writer.write(UInt32(0))
        return writer.bytes
    }

    static func validatePersonalAccessToken(_ token: String) throws {
        let length = token.utf8.count
        guard length >= 1, length <= Identifier.maxNameLength else {
            throw IggyError(.invalidFormat, context: "token must be 1-255 bytes")
        }
    }

    static func loginRegisterWithPersonalAccessToken(token: String, bindSecret: [UInt8]) throws -> [UInt8] {
        try validatePersonalAccessToken(token)
        guard bindSecret.count == bindSecretSize else {
            throw IggyError(.invalidFormat, context: "bind secret must be exactly \(bindSecretSize) bytes")
        }
        var writer = ByteWriter()
        try versionInfo(into: &writer)
        writer.write(bindSecret)
        try writer.writeName(token)
        writer.write(UInt32(0))
        return writer.bytes
    }

    /// `[ClientVersionInfo][client_id:u128LE][session:u64LE][metadata_watermark:u64LE][bind_secret:32]`
    static func bindSession(clientID: UInt128Value, session: UInt64, metadataWatermark: UInt64, bindSecret: [UInt8]) throws -> [UInt8] {
        guard bindSecret.count == bindSecretSize else {
            throw IggyError(.invalidFormat, context: "bind secret must be exactly \(bindSecretSize) bytes")
        }
        var writer = ByteWriter()
        try versionInfo(into: &writer)
        writer.write(clientID)
        writer.write(session)
        writer.write(metadataWatermark)
        writer.write(bindSecret)
        return writer.bytes
    }

    // MARK: Personal access tokens

    static func createPersonalAccessToken(name: String, expiry: PersonalAccessTokenExpiry) throws -> [UInt8] {
        var writer = ByteWriter()
        try writer.writeName(validatedWireName(name))
        writer.write(try expiry.wireMicroseconds() ?? 0)
        return writer.bytes
    }

    static func nameOnly(_ name: String) throws -> [UInt8] {
        var writer = ByteWriter()
        try writer.writeName(validatedWireName(name))
        return writer.bytes
    }

    // MARK: System

    static func getClient(clientID: UInt32) -> [UInt8] {
        clientID.littleEndianBytes
    }

    static func getSnapshot(compression: SnapshotCompression, types: [SystemSnapshotType]) throws -> [UInt8] {
        guard types.count <= Int(UInt8.max) else {
            throw IggyError(.invalidCommand, context: "too many snapshot types")
        }
        var writer = ByteWriter()
        writer.write(compression.rawValue)
        writer.write(UInt8(types.count))
        for type in types {
            writer.write(type.rawValue)
        }
        return writer.bytes
    }

    static func describeOptions(scope: OptionsScope) -> [UInt8] {
        [scope.rawValue]
    }

    /// `[start:u8][cursor:44][has_since:u8][since:u64][has_until:u8][until:u64][limit:u32][groups_count:u8][group:u8]*[token_len:u16][token]`.
    /// The cursor is all zero unless the read starts after it, and groups
    /// travel in the caller's order, at most 255 of them as in the Rust encoder.
    static func getMetadataChanges(_ query: MetadataChangesQuery) -> [UInt8] {
        var groups = query.groups
        if groups.count > Int(UInt8.max) {
            var seen = Set<MetadataChangeGroup>()
            groups = groups.filter { seen.insert($0).inserted }
        }
        var token: [UInt8] = []
        if case .current(let pageToken?) = query.start {
            token = pageToken.encoded()
        }
        var writer = ByteWriter(capacity: 1 + MetadataChangeCursor.encodedSize + 9 + 9 + 4 + 1 + groups.count + 2 + token.count)
        writer.write(query.start.code)
        if case .after(let cursor) = query.start {
            cursor.encode(into: &writer)
        } else {
            writer.writeZeros(MetadataChangeCursor.encodedSize)
        }
        for bound in [query.since, query.until] {
            writer.write(bound != nil)
            if let bound {
                writer.write(bound.microseconds)
            }
        }
        writer.write(query.limit)
        writer.write(UInt8(groups.count))
        for group in groups {
            writer.write(group.rawValue)
        }
        writer.write(UInt16(token.count))
        writer.write(token)
        return writer.bytes
    }
}

/// Validates a wire name and returns it, so the length prefix can never
/// desync the frame.
func validatedWireName(_ value: String, what: String = "name") throws -> String {
    let length = value.utf8.count
    guard length >= 1, length <= Identifier.maxNameLength else {
        throw IggyError(.invalidFormat, context: "\(what) must be 1-\(Identifier.maxNameLength) bytes, got \(length)")
    }
    return value
}
