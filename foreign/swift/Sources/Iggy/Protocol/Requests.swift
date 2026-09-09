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
        writer.writeName(try validatedWireName(name))
        writer.write(options)
        return writer.bytes
    }

    static func identifierOnly(_ id: Identifier) -> [UInt8] {
        var writer = ByteWriter(capacity: id.encodedSize)
        id.encode(into: &writer)
        return writer.bytes
    }

    static func updateStream(streamID: Identifier, name: String, options: [UInt8]) throws -> [UInt8] {
        var writer = ByteWriter()
        streamID.encode(into: &writer)
        writer.writeName(try validatedWireName(name))
        writer.write(options)
        return writer.bytes
    }

    // MARK: Topics

    static func streamAndTopic(_ streamID: Identifier, _ topicID: Identifier) -> [UInt8] {
        var writer = ByteWriter(capacity: streamID.encodedSize + topicID.encodedSize)
        streamID.encode(into: &writer)
        topicID.encode(into: &writer)
        return writer.bytes
    }

    static func createTopic(streamID: Identifier, partitionsCount: UInt32, name: String, options: [UInt8]) throws -> [UInt8] {
        var writer = ByteWriter()
        streamID.encode(into: &writer)
        writer.write(partitionsCount)
        writer.writeName(try validatedWireName(name))
        writer.write(options)
        return writer.bytes
    }

    static func updateTopic(streamID: Identifier, topicID: Identifier, name: String, options: [UInt8]) throws -> [UInt8] {
        var writer = ByteWriter()
        streamID.encode(into: &writer)
        topicID.encode(into: &writer)
        writer.writeName(try validatedWireName(name))
        writer.write(options)
        return writer.bytes
    }

    // MARK: Partitions and segments

    static func partitions(streamID: Identifier, topicID: Identifier, partitionsCount: UInt32) -> [UInt8] {
        var writer = ByteWriter()
        streamID.encode(into: &writer)
        topicID.encode(into: &writer)
        writer.write(partitionsCount)
        return writer.bytes
    }

    static func deleteSegments(streamID: Identifier, topicID: Identifier, partitionID: UInt32, segmentsCount: UInt32) -> [UInt8] {
        var writer = ByteWriter()
        streamID.encode(into: &writer)
        topicID.encode(into: &writer)
        writer.write(partitionID)
        writer.write(segmentsCount)
        return writer.bytes
    }

    // MARK: Consumer groups

    static func createConsumerGroup(streamID: Identifier, topicID: Identifier, name: String) throws -> [UInt8] {
        var writer = ByteWriter()
        streamID.encode(into: &writer)
        topicID.encode(into: &writer)
        writer.writeName(try validatedWireName(name))
        return writer.bytes
    }

    static func consumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) -> [UInt8] {
        var writer = ByteWriter()
        streamID.encode(into: &writer)
        topicID.encode(into: &writer)
        groupID.encode(into: &writer)
        return writer.bytes
    }

    // MARK: Consumer offsets

    private static func consumerTarget(
        _ consumer: Consumer, _ streamID: Identifier, _ topicID: Identifier, _ partitionID: UInt32?, into writer: inout ByteWriter
    ) {
        consumer.encode(into: &writer)
        streamID.encode(into: &writer)
        topicID.encode(into: &writer)
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
    ) -> [UInt8] {
        var writer = ByteWriter()
        consumerTarget(consumer, streamID, topicID, partitionID, into: &writer)
        writer.write(offset)
        writer.write(ack.rawValue)
        return writer.bytes
    }

    static func getConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?) -> [UInt8] {
        var writer = ByteWriter()
        consumerTarget(consumer, streamID, topicID, partitionID, into: &writer)
        return writer.bytes
    }

    static func deleteConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, ack: AckLevel) -> [UInt8] {
        var writer = ByteWriter()
        consumerTarget(consumer, streamID, topicID, partitionID, into: &writer)
        writer.write(ack.rawValue)
        return writer.bytes
    }

    // MARK: Messages

    static func pollMessages(
        consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, strategy: PollingStrategy, count: UInt32, autoCommit: Bool
    ) -> [UInt8] {
        var writer = ByteWriter()
        consumerTarget(consumer, streamID, topicID, partitionID, into: &writer)
        strategy.encode(into: &writer)
        writer.write(count)
        writer.write(autoCommit)
        return writer.bytes
    }

    static func flushUnsavedBuffer(streamID: Identifier, topicID: Identifier, partitionID: UInt32, fsync: Bool) -> [UInt8] {
        var writer = ByteWriter()
        streamID.encode(into: &writer)
        topicID.encode(into: &writer)
        writer.write(partitionID)
        writer.write(fsync)
        return writer.bytes
    }

    // MARK: Users

    static let minUsernameLength = 3
    static let maxUsernameLength = 50
    static let minPasswordLength = 3
    static let maxPasswordLength = 100

    /// The bounds every server enforces, applied before encoding so an
    /// oversized value can never desync the length prefix on the wire.
    static func validateUsername(_ username: String) throws {
        let length = username.utf8.count
        guard length >= minUsernameLength, length <= maxUsernameLength else {
            throw IggyError(.invalidUsername)
        }
    }

    static func validatePassword(_ password: String) throws {
        let length = password.utf8.count
        guard length >= minPasswordLength, length <= maxPasswordLength else {
            throw IggyError(.invalidPassword)
        }
    }

    static func createUser(username: String, password: String, status: UserStatus, permissions: Permissions?, options: [UInt8] = []) throws -> [UInt8] {
        try validateUsername(username)
        try validatePassword(password)
        var writer = ByteWriter()
        writer.writeName(username)
        writer.writeName(password)
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

    static func updateUser(userID: Identifier, username: String?, status: UserStatus?, options: [UInt8]) throws -> [UInt8] {
        var writer = ByteWriter()
        userID.encode(into: &writer)
        if let username {
            try validateUsername(username)
            writer.write(UInt8(1))
            writer.writeName(username)
        } else {
            writer.write(UInt8(0))
        }
        if let status {
            writer.write(UInt8(1))
            writer.write(status.rawValue)
        } else {
            writer.write(UInt8(0))
        }
        writer.write(options)
        return writer.bytes
    }

    static func updatePermissions(userID: Identifier, permissions: Permissions?) -> [UInt8] {
        var writer = ByteWriter()
        userID.encode(into: &writer)
        writePermissions(permissions, into: &writer)
        return writer.bytes
    }

    static func changePassword(userID: Identifier, currentPassword: String, newPassword: String) throws -> [UInt8] {
        try validatePassword(currentPassword)
        try validatePassword(newPassword)
        var writer = ByteWriter()
        userID.encode(into: &writer)
        writer.writeName(currentPassword)
        writer.writeName(newPassword)
        return writer.bytes
    }

    /// `[protocol_version:u32][sdk_name][sdk_version]`, the prefix of both
    /// login-register request shapes.
    static func versionInfo(into writer: inout ByteWriter) {
        writer.write(ProtocolVersion.current.packed)
        writer.writeName(IggyVersion.sdkName)
        writer.writeName(IggyVersion.sdkVersion)
    }

    static func loginRegister(username: String, password: String) throws -> [UInt8] {
        try validateUsername(username)
        try validatePassword(password)
        var writer = ByteWriter()
        versionInfo(into: &writer)
        writer.writeName(username)
        writer.writeName(password)
        writer.write(UInt32(0))
        return writer.bytes
    }

    static func loginRegisterWithPersonalAccessToken(token: String) throws -> [UInt8] {
        let length = token.utf8.count
        guard length >= 1, length <= Identifier.maxNameLength else {
            throw IggyError(.invalidFormat, context: "token must be 1-255 bytes")
        }
        var writer = ByteWriter()
        versionInfo(into: &writer)
        writer.writeName(token)
        writer.write(UInt32(0))
        return writer.bytes
    }

    // MARK: Personal access tokens

    static func createPersonalAccessToken(name: String, expiry: PersonalAccessTokenExpiry) throws -> [UInt8] {
        var writer = ByteWriter()
        writer.writeName(try validatedWireName(name))
        writer.write(expiry.microseconds ?? 0)
        return writer.bytes
    }

    static func nameOnly(_ name: String) throws -> [UInt8] {
        var writer = ByteWriter()
        writer.writeName(try validatedWireName(name))
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
}
