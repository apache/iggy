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

/// Consensus operation discriminant carried in every VSR header, a port of
/// `core/binary_protocol/src/consensus/operation.rs`. It selects the
/// replication plane a request travels on and tells the client how to read
/// the reply body.
enum ConsensusOperation: UInt8, Sendable, Hashable {
    case reserved = 0
    case register = 1
    case nonReplicated = 2
    case logout = 3

    case createTopicWithAssignments = 64
    case createPartitionsWithAssignments = 65
    case removeConsumerGroupMember = 66
    case completeConsumerGroupRevocation = 67
    case truncatePartition = 68
    case finalizeSession = 70
    case completeLifecycle = 71
    /// The resolved forms of `updateResource`, one per scope. The primary
    /// commits the generic update under one of them, so its reply carries it.
    case updateStreamResolved = 72
    case updateTopicResolved = 73
    case updateUserResolved = 74
    /// A replicated rewrap of stream keys the server proposes itself.
    case rewrapStreamKeys = 75

    case createStream = 128
    // 129 was updateStream. The server keeps the code retired.
    case deleteStream = 130
    // 131 was purgeStream; the server keeps the code retired.
    case createTopic = 132
    // 133 was updateTopic. The server keeps the code retired.
    case deleteTopic = 134
    // 135 was purgeTopic; the server keeps the code retired.
    case createPartitions = 136
    case deletePartitions = 137
    case deleteSegments = 138
    case createConsumerGroup = 139
    case deleteConsumerGroup = 140
    case createUser = 141
    // 142 was updateUser. The server keeps the code retired.
    case deleteUser = 143
    case changePassword = 144
    case updatePermissions = 145
    case createPersonalAccessToken = 146
    case deletePersonalAccessToken = 147
    case joinConsumerGroup = 148
    case leaveConsumerGroup = 149
    case updateResource = 150

    case sendMessages = 160
    case storeConsumerOffset = 161
    case deleteConsumerOffset = 162
    // 163 is reserved; 164 and 165 are retired offset operations.
    case retireSession = 166
    case installConsumerGroupOwner = 167
    case transitionPartitionHistory = 168
    case retireConsumerGroupOwners = 169

    private static let internalStart = ConsensusOperation.createTopicWithAssignments.rawValue
    private static let metadataStart = ConsensusOperation.createStream.rawValue
    private static let partitionStart = ConsensusOperation.sendMessages.rawValue

    /// Replica-internal operations, never sent by a client: the internal band
    /// plus the fences the server orders on the partition plane.
    var isInternal: Bool {
        switch self {
        case .retireSession, .installConsumerGroupOwner, .transitionPartitionHistory, .retireConsumerGroupOwners:
            true
        default:
            rawValue >= Self.internalStart && rawValue < Self.metadataStart
        }
    }

    /// Operations replicated through the metadata consensus group.
    var isMetadata: Bool {
        if isInternal && !isPartition {
            return true
        }
        switch self {
        case .createStream, .deleteStream, .createTopic, .deleteTopic, .createPartitions,
            .deletePartitions, .createConsumerGroup, .deleteConsumerGroup, .createUser,
            .deleteUser, .changePassword, .updatePermissions, .createPersonalAccessToken,
            .deletePersonalAccessToken, .joinConsumerGroup, .leaveConsumerGroup, .updateResource:
            return true
        default:
            return false
        }
    }

    /// Data-plane operations routed to the shard owning the partition.
    var isPartition: Bool {
        rawValue >= Self.partitionStart
    }

    /// Whether the reply body leads with a committed result section.
    var isResultFramed: Bool {
        isMetadata || self == .storeConsumerOffset || self == .deleteConsumerOffset
    }

    /// The command code a replicated operation maps to.
    static func replicated(for code: UInt32) -> ConsensusOperation? {
        switch CommandCode(rawValue: code) {
        case .createUser: .createUser
        case .deleteUser: .deleteUser
        case .updatePermissions: .updatePermissions
        case .changePassword: .changePassword
        case .createPersonalAccessToken: .createPersonalAccessToken
        case .deletePersonalAccessToken: .deletePersonalAccessToken
        case .sendMessages: .sendMessages
        case .storeConsumerOffset: .storeConsumerOffset
        case .deleteConsumerOffset: .deleteConsumerOffset
        case .createStream: .createStream
        case .deleteStream: .deleteStream
        case .createTopic: .createTopic
        case .deleteTopic: .deleteTopic
        case .createPartitions: .createPartitions
        case .deletePartitions: .deletePartitions
        case .deleteSegments: .deleteSegments
        case .createConsumerGroup: .createConsumerGroup
        case .deleteConsumerGroup: .deleteConsumerGroup
        case .joinConsumerGroup: .joinConsumerGroup
        case .leaveConsumerGroup: .leaveConsumerGroup
        case .updateResource: .updateResource
        default: nil
        }
    }

    /// The header operation for a command code. The two register codes resolve
    /// to `register` even though the dispatch table files them as
    /// non-replicated: the SDK drives the handshake and the server reads the
    /// operation byte. Unknown codes travel as non-replicated so the server
    /// can classify or reject them.
    static func forCode(_ code: UInt32) -> ConsensusOperation {
        if CommandCode.isRegister(code) {
            return .register
        }
        if code == CommandCode.logoutUser.rawValue {
            return .logout
        }
        return replicated(for: code) ?? .nonReplicated
    }
}
