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

/// Compression applied to a topic's messages.
public enum CompressionAlgorithm: UInt8, Sendable, Hashable, Codable, CustomStringConvertible {
    case none = 1
    case gzip = 2

    public var description: String {
        switch self {
        case .none: "none"
        case .gzip: "gzip"
        }
    }

    public init?(name: String) {
        switch name.lowercased() {
        case "none": self = .none
        case "gzip": self = .gzip
        default: return nil
        }
    }
}

/// How long messages are retained.
public enum IggyExpiry: Sendable, Hashable, Codable {
    /// Whatever the server is configured with.
    case serverDefault
    /// Messages older than the duration are removed.
    case after(Duration)
    /// Messages never expire.
    case never

    /// The wire value: microseconds, `UInt64.max` for never, nil for the
    /// server default.
    var microseconds: UInt64? {
        switch self {
        case .serverDefault: nil
        case .after(let duration): duration.microseconds
        case .never: UInt64.max
        }
    }

    init(wireMicroseconds value: UInt64) {
        switch value {
        case 0: self = .serverDefault
        case UInt64.max: self = .never
        default: self = .after(.microseconds(Int64(clamping: value)))
        }
    }
}

/// Upper bound on a topic's size.
public enum MaxTopicSize: Sendable, Hashable, Codable {
    case serverDefault
    case bytes(UInt64)
    case unlimited

    var wireValue: UInt64 {
        switch self {
        case .serverDefault: 0
        case .bytes(let bytes): bytes
        case .unlimited: UInt64.max
        }
    }

    init(wireValue value: UInt64) {
        switch value {
        case 0: self = .serverDefault
        case UInt64.max: self = .unlimited
        default: self = .bytes(value)
        }
    }
}

/// A stream as listed.
public struct Stream: Sendable, Hashable, Codable {
    public var id: UInt32
    public var createdAt: IggyTimestamp
    public var name: String
    public var sizeBytes: UInt64
    public var messagesCount: UInt64
    public var topicsCount: UInt32
    public var options: ResourceOptions

    public init(
        id: UInt32, createdAt: IggyTimestamp, name: String, sizeBytes: UInt64, messagesCount: UInt64, topicsCount: UInt32, options: ResourceOptions = [:]
    ) {
        self.id = id
        self.createdAt = createdAt
        self.name = name
        self.sizeBytes = sizeBytes
        self.messagesCount = messagesCount
        self.topicsCount = topicsCount
        self.options = options
    }
}

/// A stream with its topics.
public struct StreamDetails: Sendable, Hashable, Codable {
    public var id: UInt32
    public var createdAt: IggyTimestamp
    public var name: String
    public var sizeBytes: UInt64
    public var messagesCount: UInt64
    public var topicsCount: UInt32
    public var topics: [Topic]
    public var options: ResourceOptions

    public init(
        id: UInt32, createdAt: IggyTimestamp, name: String, sizeBytes: UInt64, messagesCount: UInt64, topicsCount: UInt32, topics: [Topic],
        options: ResourceOptions = [:]
    ) {
        self.id = id
        self.createdAt = createdAt
        self.name = name
        self.sizeBytes = sizeBytes
        self.messagesCount = messagesCount
        self.topicsCount = topicsCount
        self.topics = topics
        self.options = options
    }
}

/// A topic as listed.
public struct Topic: Sendable, Hashable, Codable {
    public var id: UInt32
    public var createdAt: IggyTimestamp
    public var name: String
    public var sizeBytes: UInt64
    public var messageExpiry: IggyExpiry
    public var compressionAlgorithm: CompressionAlgorithm
    public var maxTopicSize: MaxTopicSize
    public var messagesCount: UInt64
    public var partitionsCount: UInt32
    public var options: ResourceOptions

    public init(
        id: UInt32, createdAt: IggyTimestamp, name: String, sizeBytes: UInt64, messageExpiry: IggyExpiry, compressionAlgorithm: CompressionAlgorithm,
        maxTopicSize: MaxTopicSize, messagesCount: UInt64, partitionsCount: UInt32, options: ResourceOptions = [:]
    ) {
        self.id = id
        self.createdAt = createdAt
        self.name = name
        self.sizeBytes = sizeBytes
        self.messageExpiry = messageExpiry
        self.compressionAlgorithm = compressionAlgorithm
        self.maxTopicSize = maxTopicSize
        self.messagesCount = messagesCount
        self.partitionsCount = partitionsCount
        self.options = options
    }
}

/// A topic with its partitions.
public struct TopicDetails: Sendable, Hashable, Codable {
    public var id: UInt32
    public var createdAt: IggyTimestamp
    public var name: String
    public var sizeBytes: UInt64
    public var messageExpiry: IggyExpiry
    public var compressionAlgorithm: CompressionAlgorithm
    public var maxTopicSize: MaxTopicSize
    public var messagesCount: UInt64
    public var partitionsCount: UInt32
    public var partitions: [Partition]
    public var options: ResourceOptions

    public init(topic: Topic, partitions: [Partition]) {
        id = topic.id
        createdAt = topic.createdAt
        name = topic.name
        sizeBytes = topic.sizeBytes
        messageExpiry = topic.messageExpiry
        compressionAlgorithm = topic.compressionAlgorithm
        maxTopicSize = topic.maxTopicSize
        messagesCount = topic.messagesCount
        partitionsCount = topic.partitionsCount
        self.partitions = partitions
        options = topic.options
    }
}

/// One partition of a topic.
public struct Partition: Sendable, Hashable, Codable {
    public var id: UInt32
    public var createdAt: IggyTimestamp
    public var segmentsCount: UInt32
    public var currentOffset: UInt64
    public var sizeBytes: UInt64
    public var messagesCount: UInt64

    public init(id: UInt32, createdAt: IggyTimestamp, segmentsCount: UInt32, currentOffset: UInt64, sizeBytes: UInt64, messagesCount: UInt64) {
        self.id = id
        self.createdAt = createdAt
        self.segmentsCount = segmentsCount
        self.currentOffset = currentOffset
        self.sizeBytes = sizeBytes
        self.messagesCount = messagesCount
    }
}

/// A consumer group as listed.
public struct ConsumerGroup: Sendable, Hashable, Codable {
    public var id: UInt32
    public var name: String
    public var partitionsCount: UInt32
    public var membersCount: UInt32

    public init(id: UInt32, name: String, partitionsCount: UInt32, membersCount: UInt32) {
        self.id = id
        self.name = name
        self.partitionsCount = partitionsCount
        self.membersCount = membersCount
    }
}

/// A consumer group with its members.
public struct ConsumerGroupDetails: Sendable, Hashable, Codable {
    public var id: UInt32
    public var name: String
    public var partitionsCount: UInt32
    public var membersCount: UInt32
    public var members: [ConsumerGroupMember]

    public init(id: UInt32, name: String, partitionsCount: UInt32, membersCount: UInt32, members: [ConsumerGroupMember]) {
        self.id = id
        self.name = name
        self.partitionsCount = partitionsCount
        self.membersCount = membersCount
        self.members = members
    }
}

/// One member of a consumer group and the partitions it owns.
public struct ConsumerGroupMember: Sendable, Hashable, Codable {
    public var id: UInt32
    public var partitionsCount: UInt32
    public var partitions: [UInt32]

    public init(id: UInt32, partitionsCount: UInt32, partitions: [UInt32]) {
        self.id = id
        self.partitionsCount = partitionsCount
        self.partitions = partitions
    }
}

/// The stored offset of a consumer on a partition.
public struct ConsumerOffsetInfo: Sendable, Hashable, Codable {
    public var partitionID: UInt32
    public var currentOffset: UInt64
    public var storedOffset: UInt64

    public init(partitionID: UInt32, currentOffset: UInt64, storedOffset: UInt64) {
        self.partitionID = partitionID
        self.currentOffset = currentOffset
        self.storedOffset = storedOffset
    }
}

/// The partitions a consumer-group member currently owns.
public struct ConsumerGroupAssignment: Sendable, Hashable, Codable {
    public var generation: UInt64
    public var partitions: [UInt32]

    public init(generation: UInt64, partitions: [UInt32]) {
        self.generation = generation
        self.partitions = partitions
    }
}

/// Whether a user can sign in.
public enum UserStatus: UInt8, Sendable, Hashable, Codable, CustomStringConvertible {
    case active = 1
    case inactive = 2

    public var description: String {
        switch self {
        case .active: "active"
        case .inactive: "inactive"
        }
    }
}

/// A user as listed.
public struct UserInfo: Sendable, Hashable, Codable {
    public var id: UInt32
    public var createdAt: IggyTimestamp
    public var status: UserStatus
    public var username: String
    public var options: ResourceOptions

    public init(id: UInt32, createdAt: IggyTimestamp, status: UserStatus, username: String, options: ResourceOptions = [:]) {
        self.id = id
        self.createdAt = createdAt
        self.status = status
        self.username = username
        self.options = options
    }
}

/// A user with its permissions.
public struct UserInfoDetails: Sendable, Hashable, Codable {
    public var id: UInt32
    public var createdAt: IggyTimestamp
    public var status: UserStatus
    public var username: String
    public var permissions: Permissions?
    public var options: ResourceOptions

    public init(id: UInt32, createdAt: IggyTimestamp, status: UserStatus, username: String, permissions: Permissions?, options: ResourceOptions = [:]) {
        self.id = id
        self.createdAt = createdAt
        self.status = status
        self.username = username
        self.permissions = permissions
        self.options = options
    }
}

/// Who a sign-in authenticated as.
public struct IdentityInfo: Sendable, Hashable, Codable {
    public var userID: UInt32

    public init(userID: UInt32) {
        self.userID = userID
    }
}

/// A personal access token as listed. The secret is only shown at creation.
public struct PersonalAccessTokenInfo: Sendable, Hashable, Codable {
    public var name: String
    /// When the token expires, nil when it never does.
    public var expiresAt: IggyTimestamp?

    public init(name: String, expiresAt: IggyTimestamp?) {
        self.name = name
        self.expiresAt = expiresAt
    }
}

/// The secret of a freshly created personal access token.
public struct RawPersonalAccessToken: Sendable, Hashable, Codable {
    public var token: String

    public init(token: String) {
        self.token = token
    }
}

/// When a personal access token stops working.
public typealias PersonalAccessTokenExpiry = IggyExpiry

/// A connected client as listed.
public struct ClientInfo: Sendable, Hashable, Codable {
    public var clientID: UInt32
    /// The signed-in user, nil before sign-in.
    public var userID: UInt32?
    public var address: String
    public var transport: String
    public var consumerGroupsCount: UInt32

    public init(clientID: UInt32, userID: UInt32?, address: String, transport: String, consumerGroupsCount: UInt32) {
        self.clientID = clientID
        self.userID = userID
        self.address = address
        self.transport = transport
        self.consumerGroupsCount = consumerGroupsCount
    }
}

/// A connected client with the consumer groups it joined.
public struct ClientInfoDetails: Sendable, Hashable, Codable {
    public var clientID: UInt32
    public var userID: UInt32?
    public var address: String
    public var transport: String
    public var consumerGroupsCount: UInt32
    public var consumerGroups: [ConsumerGroupInfo]

    public init(clientID: UInt32, userID: UInt32?, address: String, transport: String, consumerGroupsCount: UInt32, consumerGroups: [ConsumerGroupInfo]) {
        self.clientID = clientID
        self.userID = userID
        self.address = address
        self.transport = transport
        self.consumerGroupsCount = consumerGroupsCount
        self.consumerGroups = consumerGroups
    }
}

/// A consumer group a client is a member of.
public struct ConsumerGroupInfo: Sendable, Hashable, Codable {
    public var streamID: UInt32
    public var topicID: UInt32
    public var groupID: UInt32

    public init(streamID: UInt32, topicID: UInt32, groupID: UInt32) {
        self.streamID = streamID
        self.topicID = topicID
        self.groupID = groupID
    }
}

/// Cache hit statistics of one partition.
public struct CacheMetrics: Sendable, Hashable, Codable {
    public var streamID: UInt32
    public var topicID: UInt32
    public var partitionID: UInt32
    public var hits: UInt64
    public var misses: UInt64
    public var hitRatio: Float

    public init(streamID: UInt32, topicID: UInt32, partitionID: UInt32, hits: UInt64, misses: UInt64, hitRatio: Float) {
        self.streamID = streamID
        self.topicID = topicID
        self.partitionID = partitionID
        self.hits = hits
        self.misses = misses
        self.hitRatio = hitRatio
    }
}

/// Server statistics.
public struct Stats: Sendable, Hashable, Codable {
    public var processID: UInt32
    public var cpuUsage: Float
    public var totalCPUUsage: Float
    public var memoryUsage: UInt64
    public var totalMemory: UInt64
    public var availableMemory: UInt64
    public var runTime: Duration
    public var startTime: IggyTimestamp
    public var readBytes: UInt64
    public var writtenBytes: UInt64
    public var messagesSizeBytes: UInt64
    public var streamsCount: UInt32
    public var topicsCount: UInt32
    public var partitionsCount: UInt32
    public var segmentsCount: UInt32
    public var messagesCount: UInt64
    public var clientsCount: UInt32
    public var consumerGroupsCount: UInt32
    public var hostname: String
    public var osName: String
    public var osVersion: String
    public var kernelVersion: String
    public var iggyServerVersion: String
    public var iggyServerSemver: UInt32?
    public var cacheMetrics: [CacheMetrics]
    public var threadsCount: UInt32
    public var freeDiskSpace: UInt64
    public var totalDiskSpace: UInt64
}

/// Role of a node in a cluster.
public enum ClusterNodeRole: UInt8, Sendable, Hashable, Codable {
    case leader = 0
    case follower = 1
}

/// Health of a node in a cluster.
public enum ClusterNodeStatus: UInt8, Sendable, Hashable, Codable {
    case healthy = 0
    case starting = 1
    case stopping = 2
    case unreachable = 3
    case maintenance = 4
}

/// Ports a cluster node listens on; zero when a transport is disabled.
public struct TransportEndpoints: Sendable, Hashable, Codable {
    public var tcp: UInt16
    public var quic: UInt16
    public var http: UInt16
    public var websocket: UInt16

    public init(tcp: UInt16, quic: UInt16, http: UInt16, websocket: UInt16) {
        self.tcp = tcp
        self.quic = quic
        self.http = http
        self.websocket = websocket
    }
}

/// One node of the cluster roster.
public struct ClusterNode: Sendable, Hashable, Codable {
    public var name: String
    public var ip: String
    public var endpoints: TransportEndpoints
    public var role: ClusterNodeRole
    public var status: ClusterNodeStatus

    public init(name: String, ip: String, endpoints: TransportEndpoints, role: ClusterNodeRole, status: ClusterNodeStatus) {
        self.name = name
        self.ip = ip
        self.endpoints = endpoints
        self.role = role
        self.status = status
    }

    /// The `host:port` of the node for the TCP transport, bracketing an IPv6
    /// literal so the spelling can be dialed.
    public var tcpAddress: String {
        if ip.contains(":") && !ip.hasPrefix("[") {
            return "[\(ip)]:\(endpoints.tcp)"
        }
        return "\(ip):\(endpoints.tcp)"
    }
}

/// The cluster roster.
public struct ClusterMetadata: Sendable, Hashable, Codable {
    public var name: String
    public var nodes: [ClusterNode]

    public init(name: String, nodes: [ClusterNode]) {
        self.name = name
        self.nodes = nodes
    }
}

/// What a snapshot captures.
public enum SystemSnapshotType: UInt8, Sendable, Hashable, Codable, CaseIterable {
    case filesystemOverview = 1
    case processList = 2
    case resourceUsage = 3
    case test = 4
    case serverLogs = 5
    case serverConfig = 6
    case all = 100
}

/// How a snapshot archive is compressed.
public enum SnapshotCompression: UInt8, Sendable, Hashable, Codable {
    case stored = 1
    case deflated = 2
    case bzip2 = 3
    case zstd = 4
    case lzma = 5
    case xz = 6
}

/// A snapshot archive.
public struct Snapshot: Sendable, Hashable {
    public var data: [UInt8]

    public init(data: [UInt8]) {
        self.data = data
    }
}

extension Duration {
    var microseconds: UInt64 {
        let (seconds, attoseconds) = components
        if seconds < 0 {
            return 0
        }
        return UInt64(seconds) &* 1_000_000 &+ UInt64(attoseconds / 1_000_000_000_000)
    }

    var milliseconds: Int64 {
        Int64(microseconds / 1000)
    }
}
