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

/// Response body decoders, one per reply shape, mirroring
/// `core/binary_protocol/src/responses` and the domain conversions of
/// `core/common/src/wire_conversions.rs`. Every decoder takes the body that
/// follows the consensus header, with the result section already stripped.
enum Responses {
    /// Sentinel the wire uses for "no authenticated user".
    private static let noUserID = UInt32.max

    /// Runs a decoder, mapping codec failures onto the `invalidFormat` error
    /// the Rust SDK raises for an unreadable body.
    static func decode<T>(_ body: ArraySlice<UInt8>, _ decoder: (inout ByteReader) throws -> T) throws -> T {
        var reader = ByteReader(body)
        do {
            return try decoder(&reader)
        } catch let error as WireError {
            throw error.iggyError
        }
    }

    // MARK: Streams

    static func stream(from reader: inout ByteReader) throws -> Stream {
        let id = try reader.readUInt32()
        let createdAt = try reader.readUInt64()
        let topicsCount = try reader.readUInt32()
        let sizeBytes = try reader.readUInt64()
        let messagesCount = try reader.readUInt64()
        let name = try reader.readName()
        let options = try OptionsBlock.decode(try OptionsBlock.readPrefixed(from: &reader), explicit: true)
        return Stream(
            id: id, createdAt: IggyTimestamp(microseconds: createdAt), name: name, sizeBytes: sizeBytes, messagesCount: messagesCount, topicsCount: topicsCount,
            options: options)
    }

    static func streamDetails(_ body: ArraySlice<UInt8>) throws -> StreamDetails {
        try decode(body) { reader in
            let stream = try stream(from: &reader)
            var topics: [Topic] = []
            for _ in 0..<stream.topicsCount {
                topics.append(try topic(from: &reader))
            }
            return StreamDetails(
                id: stream.id, createdAt: stream.createdAt, name: stream.name, sizeBytes: stream.sizeBytes, messagesCount: stream.messagesCount,
                topicsCount: stream.topicsCount, topics: topics, options: stream.options)
        }
    }

    static func streams(_ body: ArraySlice<UInt8>) throws -> [Stream] {
        try decode(body) { reader in
            var streams: [Stream] = []
            while !reader.isAtEnd {
                streams.append(try stream(from: &reader))
            }
            return streams
        }
    }

    // MARK: Topics

    static func topic(from reader: inout ByteReader) throws -> Topic {
        let id = try reader.readUInt32()
        let createdAt = try reader.readUInt64()
        let partitionsCount = try reader.readUInt32()
        let messageExpiry = try reader.readUInt64()
        let compression = try reader.readUInt8()
        let maxTopicSize = try reader.readUInt64()
        let sizeBytes = try reader.readUInt64()
        let messagesCount = try reader.readUInt64()
        let name = try reader.readName()
        let explicit = try OptionsBlock.readPrefixed(from: &reader)
        let derived = try OptionsBlock.readPrefixed(from: &reader)
        guard let compressionAlgorithm = CompressionAlgorithm(rawValue: compression) else {
            throw IggyError(.invalidCommand, context: "unknown compression algorithm \(compression)")
        }
        // A stored expiry of zero means the topic never expires, unlike the
        // request side where zero is the server default.
        let expiry: IggyExpiry = messageExpiry == 0 ? .never : IggyExpiry(wireMicroseconds: messageExpiry)
        return Topic(
            id: id, createdAt: IggyTimestamp(microseconds: createdAt), name: name, sizeBytes: sizeBytes, messageExpiry: expiry,
            compressionAlgorithm: compressionAlgorithm, maxTopicSize: MaxTopicSize(wireValue: maxTopicSize),
            messagesCount: messagesCount, partitionsCount: partitionsCount,
            options: try OptionsBlock.decodeSplit(explicit: explicit, derived: derived))
    }

    static func partition(from reader: inout ByteReader) throws -> Partition {
        Partition(
            id: try reader.readUInt32(), createdAt: IggyTimestamp(microseconds: try reader.readUInt64()),
            segmentsCount: try reader.readUInt32(), currentOffset: try reader.readUInt64(),
            sizeBytes: try reader.readUInt64(), messagesCount: try reader.readUInt64())
    }

    static func topicDetails(_ body: ArraySlice<UInt8>) throws -> TopicDetails {
        try decode(body) { reader in
            let topic = try topic(from: &reader)
            var partitions: [Partition] = []
            for _ in 0..<topic.partitionsCount {
                partitions.append(try partition(from: &reader))
            }
            partitions.sort { $0.id < $1.id }
            return TopicDetails(topic: topic, partitions: partitions)
        }
    }

    static func topics(_ body: ArraySlice<UInt8>) throws -> [Topic] {
        try decode(body) { reader in
            let count = try reader.readUInt32()
            var topics: [Topic] = []
            for _ in 0..<count {
                topics.append(try topic(from: &reader))
            }
            topics.sort { $0.id < $1.id }
            return topics
        }
    }

    // MARK: Consumer groups

    static func consumerGroup(from reader: inout ByteReader) throws -> ConsumerGroup {
        let id = try reader.readUInt32()
        let partitionsCount = try reader.readUInt32()
        let membersCount = try reader.readUInt32()
        let name = try reader.readName()
        return ConsumerGroup(id: id, name: name, partitionsCount: partitionsCount, membersCount: membersCount)
    }

    static func consumerGroupDetails(_ body: ArraySlice<UInt8>) throws -> ConsumerGroupDetails {
        try decode(body) { reader in
            let group = try consumerGroup(from: &reader)
            var members: [ConsumerGroupMember] = []
            for _ in 0..<group.membersCount {
                let id = try reader.readUInt32()
                let partitionsCount = try reader.readUInt32()
                var partitions: [UInt32] = []
                for _ in 0..<partitionsCount {
                    partitions.append(try reader.readUInt32())
                }
                members.append(ConsumerGroupMember(id: id, partitionsCount: partitionsCount, partitions: partitions))
            }
            return ConsumerGroupDetails(
                id: group.id, name: group.name, partitionsCount: group.partitionsCount, membersCount: group.membersCount, members: members)
        }
    }

    static func consumerGroups(_ body: ArraySlice<UInt8>) throws -> [ConsumerGroup] {
        try decode(body) { reader in
            var groups: [ConsumerGroup] = []
            while !reader.isAtEnd {
                groups.append(try consumerGroup(from: &reader))
            }
            return groups
        }
    }

    static func consumerGroupAssignment(_ body: ArraySlice<UInt8>) throws -> ConsumerGroupAssignment {
        try decode(body) { reader in
            let generation = try reader.readUInt64()
            let count = try reader.readUInt32()
            var partitions: [UInt32] = []
            for _ in 0..<count {
                partitions.append(try reader.readUInt32())
            }
            return ConsumerGroupAssignment(generation: generation, partitions: partitions)
        }
    }

    // MARK: Consumer offsets

    static func consumerOffset(_ body: ArraySlice<UInt8>) throws -> ConsumerOffsetInfo {
        try decode(body) { reader in
            ConsumerOffsetInfo(partitionID: try reader.readUInt32(), currentOffset: try reader.readUInt64(), storedOffset: try reader.readUInt64())
        }
    }

    // MARK: Clients

    private static func transportName(_ code: UInt8) -> String {
        switch code {
        case 1: "TCP"
        case 2: "QUIC"
        case 3: "HTTP"
        case 4: "WebSocket"
        default: "Unknown"
        }
    }

    static func client(from reader: inout ByteReader) throws -> ClientInfo {
        let clientID = try reader.readUInt32()
        let userID = try reader.readUInt32()
        let transport = try reader.readUInt8()
        let address = try reader.readLongString()
        let consumerGroupsCount = try reader.readUInt32()
        return ClientInfo(
            clientID: clientID, userID: userID == noUserID ? nil : userID, address: address, transport: transportName(transport),
            consumerGroupsCount: consumerGroupsCount)
    }

    static func clientDetails(_ body: ArraySlice<UInt8>) throws -> ClientInfoDetails {
        try decode(body) { reader in
            let client = try client(from: &reader)
            var groups: [ConsumerGroupInfo] = []
            for _ in 0..<client.consumerGroupsCount {
                groups.append(ConsumerGroupInfo(streamID: try reader.readUInt32(), topicID: try reader.readUInt32(), groupID: try reader.readUInt32()))
            }
            return ClientInfoDetails(
                clientID: client.clientID, userID: client.userID, address: client.address, transport: client.transport,
                consumerGroupsCount: client.consumerGroupsCount, consumerGroups: groups)
        }
    }

    static func clients(_ body: ArraySlice<UInt8>) throws -> [ClientInfo] {
        try decode(body) { reader in
            var clients: [ClientInfo] = []
            while !reader.isAtEnd {
                clients.append(try client(from: &reader))
            }
            return clients
        }
    }

    // MARK: Users

    static func user(from reader: inout ByteReader) throws -> UserInfo {
        let id = try reader.readUInt32()
        let createdAt = try reader.readUInt64()
        let statusCode = try reader.readUInt8()
        let username = try reader.readName()
        let options = try OptionsBlock.decode(try OptionsBlock.readPrefixed(from: &reader), explicit: true)
        guard let status = UserStatus(rawValue: statusCode) else {
            throw IggyError(.invalidUserStatus, context: "unknown user status \(statusCode)")
        }
        return UserInfo(id: id, createdAt: IggyTimestamp(microseconds: createdAt), status: status, username: username, options: options)
    }

    static func userDetails(_ body: ArraySlice<UInt8>) throws -> UserInfoDetails {
        try decode(body) { reader in
            let user = try user(from: &reader)
            let flag = try reader.readUInt8()
            var permissions: Permissions?
            if flag == 0 {
                try reader.skip(3)
            } else {
                let length = Int(try reader.readUInt32())
                var permissionsReader = ByteReader(try reader.readBytes(length))
                permissions = try Permissions.decode(from: &permissionsReader)
            }
            return UserInfoDetails(
                id: user.id, createdAt: user.createdAt, status: user.status, username: user.username, permissions: permissions, options: user.options)
        }
    }

    static func users(_ body: ArraySlice<UInt8>) throws -> [UserInfo] {
        try decode(body) { reader in
            var users: [UserInfo] = []
            while !reader.isAtEnd {
                users.append(try user(from: &reader))
            }
            return users
        }
    }

    struct LoginRegister {
        let userID: UInt32
        let session: UInt64
        let serverProtocolVersion: ProtocolVersion
        let serverVersion: String
    }

    static func loginRegister(_ body: ArraySlice<UInt8>) throws -> LoginRegister {
        try decode(body) { reader in
            LoginRegister(
                userID: try reader.readUInt32(), session: try reader.readUInt64(),
                serverProtocolVersion: ProtocolVersion(packed: try reader.readUInt32()), serverVersion: try reader.readName())
        }
    }

    static func identity(_ body: ArraySlice<UInt8>) throws -> IdentityInfo {
        try decode(body) { reader in IdentityInfo(userID: try reader.readUInt32()) }
    }

    // MARK: Personal access tokens

    static func rawPersonalAccessToken(_ body: ArraySlice<UInt8>) throws -> RawPersonalAccessToken {
        try decode(body) { reader in RawPersonalAccessToken(token: try reader.readName()) }
    }

    static func personalAccessTokens(_ body: ArraySlice<UInt8>) throws -> [PersonalAccessTokenInfo] {
        try decode(body) { reader in
            var tokens: [PersonalAccessTokenInfo] = []
            while !reader.isAtEnd {
                let name = try reader.readName()
                let expiry = try reader.readUInt64()
                tokens.append(PersonalAccessTokenInfo(name: name, expiresAt: expiry == 0 ? nil : IggyTimestamp(microseconds: expiry)))
            }
            return tokens
        }
    }

    // MARK: System

    static func stats(_ body: ArraySlice<UInt8>) throws -> Stats {
        try decode(body) { reader in
            let processID = try reader.readUInt32()
            let cpuUsage = try reader.readFloat()
            let totalCPUUsage = try reader.readFloat()
            let memoryUsage = try reader.readUInt64()
            let totalMemory = try reader.readUInt64()
            let availableMemory = try reader.readUInt64()
            let runTime = try reader.readUInt64()
            let startTime = try reader.readUInt64()
            let readBytes = try reader.readUInt64()
            let writtenBytes = try reader.readUInt64()
            let messagesSizeBytes = try reader.readUInt64()
            let streamsCount = try reader.readUInt32()
            let topicsCount = try reader.readUInt32()
            let partitionsCount = try reader.readUInt32()
            let segmentsCount = try reader.readUInt32()
            let messagesCount = try reader.readUInt64()
            let clientsCount = try reader.readUInt32()
            let consumerGroupsCount = try reader.readUInt32()
            let hostname = try reader.readLongString()
            let osName = try reader.readLongString()
            let osVersion = try reader.readLongString()
            let kernelVersion = try reader.readLongString()
            let serverVersion = try reader.readLongString()
            let semver = try reader.readUInt32()
            let cacheCount = try reader.readUInt32()
            var cacheMetrics: [CacheMetrics] = []
            for _ in 0..<cacheCount {
                cacheMetrics.append(
                    CacheMetrics(
                        streamID: try reader.readUInt32(), topicID: try reader.readUInt32(), partitionID: try reader.readUInt32(),
                        hits: try reader.readUInt64(), misses: try reader.readUInt64(), hitRatio: try reader.readFloat()))
            }
            let threadsCount = try reader.readUInt32()
            let freeDiskSpace = try reader.readUInt64()
            let totalDiskSpace = try reader.readUInt64()
            return Stats(
                processID: processID, cpuUsage: cpuUsage, totalCPUUsage: totalCPUUsage, memoryUsage: memoryUsage,
                totalMemory: totalMemory, availableMemory: availableMemory, runTime: .microseconds(Int64(clamping: runTime)),
                startTime: IggyTimestamp(microseconds: startTime), readBytes: readBytes, writtenBytes: writtenBytes,
                messagesSizeBytes: messagesSizeBytes, streamsCount: streamsCount, topicsCount: topicsCount,
                partitionsCount: partitionsCount, segmentsCount: segmentsCount, messagesCount: messagesCount,
                clientsCount: clientsCount, consumerGroupsCount: consumerGroupsCount, hostname: hostname, osName: osName,
                osVersion: osVersion, kernelVersion: kernelVersion, iggyServerVersion: serverVersion,
                iggyServerSemver: semver == 0 ? nil : semver, cacheMetrics: cacheMetrics, threadsCount: threadsCount,
                freeDiskSpace: freeDiskSpace, totalDiskSpace: totalDiskSpace)
        }
    }

    static func clusterMetadata(_ body: ArraySlice<UInt8>) throws -> ClusterMetadata {
        try decode(body) { reader in
            let name = try reader.readLongString()
            let count = try reader.readUInt32()
            var nodes: [ClusterNode] = []
            for _ in 0..<count {
                let nodeName = try reader.readLongString()
                let ip = try reader.readLongString()
                let endpoints = TransportEndpoints(
                    tcp: try reader.readUInt16(), quic: try reader.readUInt16(), http: try reader.readUInt16(), websocket: try reader.readUInt16())
                let roleCode = try reader.readUInt8()
                let statusCode = try reader.readUInt8()
                guard let role = ClusterNodeRole(rawValue: roleCode) else {
                    throw IggyError(.invalidCommand, context: "unknown cluster node role \(roleCode)")
                }
                guard let status = ClusterNodeStatus(rawValue: statusCode) else {
                    throw IggyError(.invalidCommand, context: "unknown cluster node status \(statusCode)")
                }
                nodes.append(ClusterNode(name: nodeName, ip: ip, endpoints: endpoints, role: role, status: status))
            }
            return ClusterMetadata(name: name, nodes: nodes)
        }
    }

    static func optionSpecs(_ body: ArraySlice<UInt8>) throws -> [OptionSpec] {
        try decode(body) { reader in
            let count = try reader.readUInt32()
            var specs: [OptionSpec] = []
            for _ in 0..<count {
                let key = try reader.readName()
                let kindCode = try reader.readUInt8()
                let defaultLength = Int(try reader.readUInt32())
                let defaultValue = Array(try reader.readBytes(defaultLength))
                let descriptionLength = Int(try reader.readUInt32())
                let description = String(decoding: try reader.readBytes(descriptionLength), as: UTF8.self)
                guard let kind = HeaderKind(rawValue: kindCode) else {
                    throw IggyError(.invalidHeaderKind, context: "unknown option kind \(kindCode)")
                }
                specs.append(OptionSpec(key: key, kind: kind, defaultValue: defaultValue, description: description))
            }
            return specs
        }
    }

    // MARK: Messages

    /// Confirmations of a committed batch. An empty body means the batch was
    /// accepted with no offsets reported and decodes to an empty list, never
    /// to a zeroed entry a caller could mistake for a commit at offset 0.
    static func sendConfirmations(_ body: ArraySlice<UInt8>) throws -> SendMessagesResponse {
        if body.isEmpty {
            return SendMessagesResponse()
        }
        return try decode(body) { reader in
            let count = try reader.readUInt32()
            var confirmations: [SendConfirmation] = []
            for _ in 0..<count {
                confirmations.append(
                    SendConfirmation(
                        streamID: try reader.readUInt32(), topicID: try reader.readUInt32(), partitionID: try reader.readUInt32(),
                        baseOffset: try reader.readUInt64()))
            }
            guard reader.isAtEnd else {
                throw WireError.validation("send_messages response has \(reader.remaining) trailing bytes")
            }
            return SendMessagesResponse(confirmations: confirmations)
        }
    }

    static func polledMessages(_ body: ArraySlice<UInt8>, verifyChecksums: Bool = false) throws -> PolledMessages {
        try decode(body) { reader in
            let partitionID = try reader.readUInt32()
            let currentOffset = try reader.readUInt64()
            let count = try reader.readUInt32()
            let messages = try Batch.decodeMessages(reader.rest, expectedCount: count, verifyChecksums: verifyChecksums)
            return PolledMessages(partitionID: partitionID, currentOffset: currentOffset, count: count, messages: messages)
        }
    }
}
