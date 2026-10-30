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

/// Family of metadata change records and the unit of filtering. Raw values
/// are the wire codes. A query without groups selects every group.
public enum MetadataChangeGroup: UInt8, Sendable, Hashable, CaseIterable, CustomStringConvertible {
    case streams = 1
    case topics = 2
    case partitions = 3
    case consumerGroups = 4
    case consumerGroupMembers = 5
    case users = 6
    case personalAccessTokens = 7
    case sessions = 8

    /// The snake_case name the other SDKs and the HTTP API use.
    public var description: String {
        switch self {
        case .streams: "streams"
        case .topics: "topics"
        case .partitions: "partitions"
        case .consumerGroups: "consumer_groups"
        case .consumerGroupMembers: "consumer_group_members"
        case .users: "users"
        case .personalAccessTokens: "personal_access_tokens"
        case .sessions: "sessions"
        }
    }

    public init?(name: String) {
        guard let group = Self.allCases.first(where: { $0.description == name }) else {
            return nil
        }
        self = group
    }

    /// Groups a current state scan of this group must include, so a mirror
    /// can apply the cascades of their deletes.
    public var parents: [MetadataChangeGroup] {
        switch self {
        case .streams, .users: []
        case .topics: [.streams]
        case .partitions, .consumerGroups: [.topics, .streams]
        case .consumerGroupMembers: [.consumerGroups, .partitions, .topics, .streams]
        case .personalAccessTokens, .sessions: [.users]
        }
    }

    /// Bit of the group in a page token's group mask.
    var maskBit: UInt8 { 1 << (rawValue - 1) }
}

/// Kind of a metadata change record. Raw values are the wire codes.
public enum MetadataChangeKind: UInt8, Sendable, Hashable, CaseIterable, CustomStringConvertible {
    case streamCreated = 1
    case streamUpdated = 2
    case streamDeleted = 3
    case topicCreated = 10
    case topicUpdated = 11
    case topicDeleted = 12
    case partitionsChanged = 20
    case consumerGroupCreated = 30
    case consumerGroupDeleted = 31
    case consumerGroupMemberJoined = 40
    case consumerGroupMemberLeft = 41
    case consumerGroupPartitionHandedOver = 42
    case userCreated = 50
    case userUpdated = 51
    case userDeleted = 52
    case userPasswordChanged = 53
    case userPermissionsUpdated = 54
    case personalAccessTokenCreated = 60
    case personalAccessTokenDeleted = 61
    case sessionRegistered = 70
    case sessionClosed = 71

    /// The snake_case name the other SDKs and the HTTP API use.
    public var description: String {
        switch self {
        case .streamCreated: "stream_created"
        case .streamUpdated: "stream_updated"
        case .streamDeleted: "stream_deleted"
        case .topicCreated: "topic_created"
        case .topicUpdated: "topic_updated"
        case .topicDeleted: "topic_deleted"
        case .partitionsChanged: "partitions_changed"
        case .consumerGroupCreated: "consumer_group_created"
        case .consumerGroupDeleted: "consumer_group_deleted"
        case .consumerGroupMemberJoined: "consumer_group_member_joined"
        case .consumerGroupMemberLeft: "consumer_group_member_left"
        case .consumerGroupPartitionHandedOver: "consumer_group_partition_handed_over"
        case .userCreated: "user_created"
        case .userUpdated: "user_updated"
        case .userDeleted: "user_deleted"
        case .userPasswordChanged: "user_password_changed"
        case .userPermissionsUpdated: "user_permissions_updated"
        case .personalAccessTokenCreated: "personal_access_token_created"
        case .personalAccessTokenDeleted: "personal_access_token_deleted"
        case .sessionRegistered: "session_registered"
        case .sessionClosed: "session_closed"
        }
    }

    public init?(name: String) {
        guard let kind = Self.allCases.first(where: { $0.description == name }) else {
            return nil
        }
        self = kind
    }

    /// The group every record of this kind belongs to.
    public var group: MetadataChangeGroup {
        switch self {
        case .streamCreated, .streamUpdated, .streamDeleted: .streams
        case .topicCreated, .topicUpdated, .topicDeleted: .topics
        case .partitionsChanged: .partitions
        case .consumerGroupCreated, .consumerGroupDeleted: .consumerGroups
        case .consumerGroupMemberJoined, .consumerGroupMemberLeft, .consumerGroupPartitionHandedOver: .consumerGroupMembers
        case .userCreated, .userUpdated, .userDeleted, .userPasswordChanged, .userPermissionsUpdated: .users
        case .personalAccessTokenCreated, .personalAccessTokenDeleted: .personalAccessTokens
        case .sessionRegistered, .sessionClosed: .sessions
        }
    }
}

/// Outcome of a metadata change query.
public enum MetadataChangeStatus: UInt8, Sendable, Hashable, CaseIterable, CustomStringConvertible {
    /// Records or items after the requested position.
    case ok = 0
    /// The answering node has not applied the requested position yet. Retry later.
    case behind = 1
    /// The node no longer holds every record after the position. Rebuild from current state.
    case expired = 2
    /// The position belongs to another history. Rebuild from current state.
    case reset = 3

    public var description: String {
        switch self {
        case .ok: "ok"
        case .behind: "behind"
        case .expired: "expired"
        case .reset: "reset"
        }
    }
}

/// Whether an answer pages change records or current state items.
public enum MetadataChangePhase: UInt8, Sendable, Hashable, CaseIterable, CustomStringConvertible {
    case changes = 0
    case current = 1

    public var description: String {
        switch self {
        case .changes: "changes"
        case .current: "current"
        }
    }
}

/// Why a consumer group member was removed.
public enum MetadataMemberLeftReason: UInt8, Sendable, Hashable, CaseIterable, CustomStringConvertible {
    case left = 0
    case sessionEnded = 1

    public var description: String {
        switch self {
        case .left: "left"
        case .sessionEnded: "session_ended"
        }
    }
}

/// Why a personal access token was removed.
public enum MetadataTokenDeletedReason: UInt8, Sendable, Hashable, CaseIterable, CustomStringConvertible {
    case deleted = 0
    case expired = 1

    public var description: String {
        switch self {
        case .deleted: "deleted"
        case .expired: "expired"
        }
    }
}

/// Why a session was closed.
public enum MetadataSessionClosedReason: UInt8, Sendable, Hashable, CaseIterable, CustomStringConvertible {
    case logout = 0
    case expired = 1
    /// The user was deactivated or deleted.
    case revoked = 2

    public var description: String {
        switch self {
        case .logout: "logout"
        case .expired: "expired"
        case .revoked: "revoked"
        }
    }
}

/// Why a user's public permission set changed.
public enum MetadataPermissionsUpdatedReason: UInt8, Sendable, Hashable, CaseIterable, CustomStringConvertible {
    /// An update of the user's permissions replaced the set.
    case updated = 0
    /// A stream or topic delete pruned the user's grants on it.
    case resourceDeleted = 1

    public var description: String {
        switch self {
        case .updated: "updated"
        case .resourceDeleted: "resource_deleted"
        }
    }
}

/// Position of the last metadata change record a caller consumed.
///
/// Persist ``description``, `mc1_` followed by 59 characters of unpadded
/// Base64url, and read it back with ``init(text:)``. ``op`` 0 names the
/// beginning of the history ``logID`` identifies.
public struct MetadataChangeCursor: Sendable, Hashable, CustomStringConvertible {
    public static let textPrefix = "mc1_"
    static let encodedSize = 44
    static let textLength = textPrefix.utf8.count + 59

    /// Prepare checksum of op 1, the root bootstrap of the history.
    public var logID: UInt128Value
    public var op: UInt64
    /// Prepare checksum of ``op``.
    public var checksum: UInt128Value
    /// Position of the record within ``op``.
    public var ordinal: UInt32

    public init(logID: UInt128Value, op: UInt64, checksum: UInt128Value, ordinal: UInt32) {
        self.logID = logID
        self.op = op
        self.checksum = checksum
        self.ordinal = ordinal
    }

    /// The cursor before the first record of `logID`.
    public static func zero(logID: UInt128Value) -> MetadataChangeCursor {
        MetadataChangeCursor(logID: logID, op: 0, checksum: .zero, ordinal: 0)
    }

    public var isZero: Bool { op == 0 }

    /// Op 0 carries no checksum and no ordinal.
    public var isCanonical: Bool { op != 0 || (checksum.isZero && ordinal == 0) }

    /// Parses the `mc1_` text form. Fails with
    /// ``IggyErrorCode/invalidCommand`` for any other text, whose error never
    /// repeats the text.
    public init(text: String) throws {
        guard text.utf8.count == Self.textLength, let bytes = Base64URL.decode(text, prefix: Self.textPrefix), bytes.count == Self.encodedSize else {
            throw IggyError(.invalidCommand, context: "not a metadata change cursor")
        }
        var reader = ByteReader(bytes)
        guard let cursor = try? Self.decode(from: &reader) else {
            throw IggyError(.invalidCommand, context: "not a canonical metadata change cursor")
        }
        self = cursor
    }

    /// The `mc1_` text form.
    public var description: String {
        var writer = ByteWriter(capacity: Self.encodedSize)
        encode(into: &writer)
        return Self.textPrefix + Base64URL.encode(writer.bytes)
    }

    func encode(into writer: inout ByteWriter) {
        writer.write(logID)
        writer.write(op)
        writer.write(checksum)
        writer.write(ordinal)
    }

    static func decode(from reader: inout ByteReader) throws -> MetadataChangeCursor {
        let cursor = MetadataChangeCursor(
            logID: try reader.readUInt128(), op: try reader.readUInt64(), checksum: try reader.readUInt128(), ordinal: try reader.readUInt32())
        guard cursor.isCanonical else {
            throw WireError.validation("a cursor at op 0 carries no checksum and no ordinal")
        }
        return cursor
    }
}

/// Continuation of a current state scan.
///
/// Pass it back unchanged in ``MetadataChangeStart/current(_:)`` with the
/// same groups to read the next page. Its text form starts with `cs1_`.
public struct MetadataCurrentPageToken: Sendable, Hashable, CustomStringConvertible {
    public static let textPrefix = "cs1_"
    /// Longest key: a user id and a personal access token name.
    static let maxKeySize = 4 + 1 + 255
    static let maxEncodedSize = MetadataChangeCursor.encodedSize + 8 + 1 + 1 + 2 + maxKeySize

    /// The position captured before the scan's first page.
    public let anchor: MetadataChangeCursor
    /// The highest head any page of the scan reported so far.
    public let requiredHead: UInt64
    let groupsMask: UInt8
    /// The group of the last item returned.
    public let lastGroup: MetadataChangeGroup
    /// The key of the last item returned, in the page token key layout.
    public let lastKey: [UInt8]

    /// The groups the scan was started with.
    public var groups: Set<MetadataChangeGroup> {
        Set(MetadataChangeGroup.allCases.filter { groupsMask & $0.maskBit != 0 })
    }

    /// Parses the `cs1_` text form. Fails with
    /// ``IggyErrorCode/invalidCommand`` for any other text, whose error never
    /// repeats the text.
    public init(text: String) throws {
        let longest = Self.textPrefix.utf8.count + (Self.maxEncodedSize + 2) / 3 * 4
        guard text.utf8.count <= longest, let bytes = Base64URL.decode(text, prefix: Self.textPrefix) else {
            throw IggyError(.invalidCommand, context: "not a metadata current page token")
        }
        var reader = ByteReader(bytes)
        guard let token = try? Self.decode(from: &reader), reader.isAtEnd else {
            throw IggyError(.invalidCommand, context: "not a valid metadata current page token")
        }
        self = token
    }

    init(anchor: MetadataChangeCursor, requiredHead: UInt64, groupsMask: UInt8, lastGroup: MetadataChangeGroup, lastKey: [UInt8]) {
        self.anchor = anchor
        self.requiredHead = requiredHead
        self.groupsMask = groupsMask
        self.lastGroup = lastGroup
        self.lastKey = lastKey
    }

    /// The `cs1_` text form.
    public var description: String {
        Self.textPrefix + Base64URL.encode(encoded())
    }

    func encoded() -> [UInt8] {
        var writer = ByteWriter(capacity: MetadataChangeCursor.encodedSize + 12 + lastKey.count)
        anchor.encode(into: &writer)
        writer.write(requiredHead)
        writer.write(groupsMask)
        writer.write(lastGroup.rawValue)
        writer.write(UInt16(lastKey.count))
        writer.write(lastKey)
        return writer.bytes
    }

    /// Decodes `[anchor:44][required_head:u64][groups_mask:u8][group:u8][key_len:u16][last_key]`
    /// and checks that the group is selected, the key has that group's width
    /// and the head covers the anchor.
    static func decode(from reader: inout ByteReader) throws -> MetadataCurrentPageToken {
        let anchor = try MetadataChangeCursor.decode(from: &reader)
        let requiredHead = try reader.readUInt64()
        let groupsMask = try reader.readUInt8()
        let groupCode = try reader.readUInt8()
        guard let lastGroup = MetadataChangeGroup(rawValue: groupCode) else {
            throw WireError.validation("page token names an unknown group")
        }
        let keyLength = Int(try reader.readUInt16())
        guard keyLength <= maxKeySize else {
            throw WireError.validation("page token key of \(keyLength) bytes is too long")
        }
        let token = MetadataCurrentPageToken(
            anchor: anchor, requiredHead: requiredHead, groupsMask: groupsMask, lastGroup: lastGroup, lastKey: Array(try reader.readBytes(keyLength)))
        guard groupsMask & lastGroup.maskBit != 0 else {
            throw WireError.validation("page token group is not in its group mask")
        }
        guard requiredHead >= anchor.op else {
            throw WireError.validation("page token head is below its anchor")
        }
        guard token.keyFitsGroup else {
            throw WireError.validation("page token key of \(keyLength) bytes does not fit group \(groupCode)")
        }
        return token
    }

    private var keyFitsGroup: Bool {
        switch lastGroup {
        case .streams, .users:
            return lastKey.count == 4
        case .topics, .partitions, .sessions:
            return lastKey.count == 8
        case .consumerGroups:
            return lastKey.count == 12
        case .consumerGroupMembers:
            return lastKey.count == 16
        case .personalAccessTokens:
            guard lastKey.count > 4 else {
                return false
            }
            var reader = ByteReader(lastKey[4...])
            return (try? reader.readName()) != nil && reader.isAtEnd
        }
    }
}

/// Where a metadata change query starts. The wire code of each case is its
/// position: earliest 0, latest 1, after 2, current 3.
public enum MetadataChangeStart: Sendable, Hashable {
    /// The oldest operation the requested groups still hold completely.
    case earliest
    /// No records, only a cursor at the newest retained record to follow from.
    case latest
    /// The records after a cursor.
    case after(MetadataChangeCursor)
    /// Current state pages: nil starts a scan, a token continues it.
    case current(MetadataCurrentPageToken?)

    var code: UInt8 {
        switch self {
        case .earliest: 0
        case .latest: 1
        case .after: 2
        case .current: 3
        }
    }
}

/// One read of the metadata change log or of the current state.
///
/// Empty ``groups`` selects every group. ``since`` and ``until`` keep records
/// whose prepare timestamp is in the inclusive range and apply to
/// ``MetadataChangeStart/earliest`` and ``MetadataChangeStart/after(_:)`` only.
public struct MetadataChangesQuery: Sendable, Hashable {
    /// Records or items per page when the caller does not say.
    public static let defaultLimit: UInt32 = 100
    /// Most records or items one page returns.
    public static let maxLimit: UInt32 = 1000

    public var start: MetadataChangeStart
    public var groups: [MetadataChangeGroup]
    public var since: IggyTimestamp?
    public var until: IggyTimestamp?
    public var limit: UInt32

    public init(
        start: MetadataChangeStart = .earliest, groups: [MetadataChangeGroup] = [], since: IggyTimestamp? = nil, until: IggyTimestamp? = nil,
        limit: UInt32 = defaultLimit
    ) {
        self.start = start
        self.groups = groups
        self.since = since
        self.until = until
        self.limit = limit
    }

    /// The checks the server applies, run before anything is sent. Fails with
    /// ``IggyErrorCode/invalidCommand`` when the limit is outside 1 to
    /// ``maxLimit``, ``since`` is above ``until``, a time bound accompanies
    /// latest or current, a cursor is not canonical, a current scan misses a
    /// parent group, or a page token was issued for other groups.
    public func validate() throws {
        guard (1...Self.maxLimit).contains(limit) else {
            throw IggyError(.invalidCommand, context: "limit must be 1 to \(Self.maxLimit), got \(limit)")
        }
        if let since, let until, since > until {
            throw IggyError(.invalidCommand, context: "since is above until")
        }
        let selected = groups.isEmpty ? Set(MetadataChangeGroup.allCases) : Set(groups)
        let hasTimeBound = since != nil || until != nil
        switch start {
        case .earliest:
            break
        case .after(let cursor):
            guard cursor.isCanonical else {
                throw IggyError(.invalidCommand, context: "a cursor at op 0 carries no checksum and no ordinal")
            }
        case .latest:
            guard !hasTimeBound else {
                throw IggyError(.invalidCommand, context: "time bounds apply to earliest and after only")
            }
        case .current(let token):
            guard !hasTimeBound else {
                throw IggyError(.invalidCommand, context: "time bounds apply to earliest and after only")
            }
            if let group = MetadataChangeGroup.allCases.first(where: { selected.contains($0) && !$0.parents.allSatisfy(selected.contains) }) {
                throw IggyError(.invalidCommand, context: "a current scan of \(group) needs its parent groups")
            }
            if let token, token.groups != selected {
                throw IggyError(.invalidCommand, context: "the page token was issued for other groups")
            }
        }
    }
}

/// Public options of a resource after a change, as the read routes return
/// them. ``unreadable`` marks stored critical state this build cannot read:
/// a mirror keeps its previous values then.
public enum MetadataOptions: Sendable, Hashable {
    case readable(ResourceOptions)
    case unreadable
}

/// A stream after a create or update, and a current state streams item.
public struct MetadataStreamChange: Sendable, Hashable {
    public var streamID: UInt32
    /// The op that created the stream. A recreated stream with a reused id has another.
    public var streamCreatedOp: UInt64
    public var createdAt: IggyTimestamp
    public var name: String
    public var options: MetadataOptions

    public init(streamID: UInt32, streamCreatedOp: UInt64, createdAt: IggyTimestamp, name: String, options: MetadataOptions) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.createdAt = createdAt
        self.name = name
        self.options = options
    }
}

/// A removed stream. ``requestedOp`` is the op of a deferred delete's request.
public struct MetadataStreamDeleted: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var createdAt: IggyTimestamp
    public var name: String
    public var requestedOp: UInt64?

    public init(streamID: UInt32, streamCreatedOp: UInt64, createdAt: IggyTimestamp, name: String, requestedOp: UInt64?) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.createdAt = createdAt
        self.name = name
        self.requestedOp = requestedOp
    }
}

/// A topic after a create or update.
public struct MetadataTopicChange: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var createdAt: IggyTimestamp
    public var name: String
    public var partitionsCount: UInt32
    public var options: MetadataOptions

    public init(
        streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, createdAt: IggyTimestamp, name: String,
        partitionsCount: UInt32, options: MetadataOptions
    ) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.createdAt = createdAt
        self.name = name
        self.partitionsCount = partitionsCount
        self.options = options
    }
}

/// A current state topics item. Partition counts travel as partitions items.
public struct MetadataTopicItem: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var createdAt: IggyTimestamp
    public var name: String
    public var options: MetadataOptions

    public init(
        streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, createdAt: IggyTimestamp, name: String,
        options: MetadataOptions
    ) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.createdAt = createdAt
        self.name = name
        self.options = options
    }
}

/// A removed topic. ``requestedOp`` is the op of a deferred delete's request.
public struct MetadataTopicDeleted: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var createdAt: IggyTimestamp
    public var name: String
    public var requestedOp: UInt64?

    public init(
        streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, createdAt: IggyTimestamp, name: String,
        requestedOp: UInt64?
    ) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.createdAt = createdAt
        self.name = name
        self.requestedOp = requestedOp
    }
}

/// A partition count change. ``removedPartitionIDs`` lists the deleted ids in
/// ascending order and is empty on create.
public struct MetadataPartitionsChanged: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var partitionsCount: UInt32
    public var removedPartitionIDs: [UInt32]
    public var requestedOp: UInt64?

    public init(
        streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, partitionsCount: UInt32, removedPartitionIDs: [UInt32],
        requestedOp: UInt64?
    ) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.partitionsCount = partitionsCount
        self.removedPartitionIDs = removedPartitionIDs
        self.requestedOp = requestedOp
    }
}

/// A current state partitions item.
public struct MetadataPartitionsItem: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var partitionsCount: UInt32

    public init(streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, partitionsCount: UInt32) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.partitionsCount = partitionsCount
    }
}

/// A created consumer group with the topic's resulting group count.
public struct MetadataConsumerGroupCreated: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var groupID: UInt32
    public var name: String
    public var consumerGroupsCount: UInt32

    public init(
        streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, groupID: UInt32, name: String, consumerGroupsCount: UInt32
    ) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.groupID = groupID
        self.name = name
        self.consumerGroupsCount = consumerGroupsCount
    }
}

/// A removed consumer group with the topic's resulting group count.
public struct MetadataConsumerGroupDeleted: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var groupID: UInt32
    public var name: String
    public var consumerGroupsCount: UInt32
    public var requestedOp: UInt64?

    public init(
        streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, groupID: UInt32, name: String, consumerGroupsCount: UInt32,
        requestedOp: UInt64?
    ) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.groupID = groupID
        self.name = name
        self.consumerGroupsCount = consumerGroupsCount
        self.requestedOp = requestedOp
    }
}

/// A current state consumer groups item.
public struct MetadataConsumerGroupItem: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var groupID: UInt32
    public var name: String

    public init(streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, groupID: UInt32, name: String) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.groupID = groupID
        self.name = name
    }
}

/// A member that joined, with its installed partitions, normally none since
/// a handover installs them.
public struct MetadataMemberJoined: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var groupID: UInt32
    public var memberID: UInt32
    public var clientID: UInt128Value
    public var membersCount: UInt32
    public var partitionIDs: [UInt32]

    public init(
        streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, groupID: UInt32, memberID: UInt32, clientID: UInt128Value,
        membersCount: UInt32, partitionIDs: [UInt32]
    ) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.groupID = groupID
        self.memberID = memberID
        self.clientID = clientID
        self.membersCount = membersCount
        self.partitionIDs = partitionIDs
    }
}

/// A member that left a consumer group.
public struct MetadataMemberLeft: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var groupID: UInt32
    public var memberID: UInt32
    public var clientID: UInt128Value
    public var membersCount: UInt32
    public var reason: MetadataMemberLeftReason

    public init(
        streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, groupID: UInt32, memberID: UInt32, clientID: UInt128Value,
        membersCount: UInt32, reason: MetadataMemberLeftReason
    ) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.groupID = groupID
        self.memberID = memberID
        self.clientID = clientID
        self.membersCount = membersCount
        self.reason = reason
    }
}

/// A completed partition handover. ``memberID`` is nil when the last member left.
public struct MetadataPartitionHandedOver: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var groupID: UInt32
    public var partitionID: UInt32
    public var memberID: UInt32?

    public init(
        streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, groupID: UInt32, partitionID: UInt32, memberID: UInt32?
    ) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.groupID = groupID
        self.partitionID = partitionID
        self.memberID = memberID
    }
}

/// A current state consumer group members item.
public struct MetadataMemberItem: Sendable, Hashable {
    public var streamID: UInt32
    public var streamCreatedOp: UInt64
    public var topicID: UInt32
    public var topicCreatedOp: UInt64
    public var groupID: UInt32
    public var memberID: UInt32
    public var clientID: UInt128Value
    public var partitionIDs: [UInt32]

    public init(
        streamID: UInt32, streamCreatedOp: UInt64, topicID: UInt32, topicCreatedOp: UInt64, groupID: UInt32, memberID: UInt32, clientID: UInt128Value,
        partitionIDs: [UInt32]
    ) {
        self.streamID = streamID
        self.streamCreatedOp = streamCreatedOp
        self.topicID = topicID
        self.topicCreatedOp = topicCreatedOp
        self.groupID = groupID
        self.memberID = memberID
        self.clientID = clientID
        self.partitionIDs = partitionIDs
    }
}

/// A user after a create or update, and a current state users item.
public struct MetadataUserChange: Sendable, Hashable {
    public var userID: UInt32
    /// The user's creation time. A recreated user with a reused id has another.
    public var userCreatedAt: IggyTimestamp
    public var username: String
    public var status: UserStatus
    /// The complete public permission set, nil when the user has none.
    public var permissions: Permissions?
    public var options: MetadataOptions

    public init(userID: UInt32, userCreatedAt: IggyTimestamp, username: String, status: UserStatus, permissions: Permissions?, options: MetadataOptions) {
        self.userID = userID
        self.userCreatedAt = userCreatedAt
        self.username = username
        self.status = status
        self.permissions = permissions
        self.options = options
    }
}

/// A deleted user, or one whose password changed.
public struct MetadataUserIdentity: Sendable, Hashable {
    public var userID: UInt32
    public var userCreatedAt: IggyTimestamp
    public var username: String

    public init(userID: UInt32, userCreatedAt: IggyTimestamp, username: String) {
        self.userID = userID
        self.userCreatedAt = userCreatedAt
        self.username = username
    }
}

/// A user's public permission set after a change.
public struct MetadataUserPermissionsUpdated: Sendable, Hashable {
    public var userID: UInt32
    public var userCreatedAt: IggyTimestamp
    public var username: String
    public var permissions: Permissions?
    public var reason: MetadataPermissionsUpdatedReason

    public init(
        userID: UInt32, userCreatedAt: IggyTimestamp, username: String, permissions: Permissions?, reason: MetadataPermissionsUpdatedReason
    ) {
        self.userID = userID
        self.userCreatedAt = userCreatedAt
        self.username = username
        self.permissions = permissions
        self.reason = reason
    }
}

/// A created personal access token, and a current state personal access
/// tokens item. ``expiry`` is nil for a token that never expires.
public struct MetadataTokenChange: Sendable, Hashable {
    public var userID: UInt32
    public var userCreatedAt: IggyTimestamp
    public var name: String
    public var expiry: IggyTimestamp?

    public init(userID: UInt32, userCreatedAt: IggyTimestamp, name: String, expiry: IggyTimestamp?) {
        self.userID = userID
        self.userCreatedAt = userCreatedAt
        self.name = name
        self.expiry = expiry
    }
}

/// A removed personal access token.
public struct MetadataTokenDeleted: Sendable, Hashable {
    public var userID: UInt32
    public var userCreatedAt: IggyTimestamp
    public var name: String
    public var reason: MetadataTokenDeletedReason

    public init(userID: UInt32, userCreatedAt: IggyTimestamp, name: String, reason: MetadataTokenDeletedReason) {
        self.userID = userID
        self.userCreatedAt = userCreatedAt
        self.name = name
        self.reason = reason
    }
}

/// A registered session, and a current state sessions item.
public struct MetadataSession: Sendable, Hashable {
    public var userID: UInt32
    public var userCreatedAt: IggyTimestamp
    public var clientID: UInt128Value
    public var session: UInt64

    public init(userID: UInt32, userCreatedAt: IggyTimestamp, clientID: UInt128Value, session: UInt64) {
        self.userID = userID
        self.userCreatedAt = userCreatedAt
        self.clientID = clientID
        self.session = session
    }
}

/// A closed session.
public struct MetadataSessionClosed: Sendable, Hashable {
    public var userID: UInt32
    public var userCreatedAt: IggyTimestamp
    public var clientID: UInt128Value
    public var session: UInt64
    public var reason: MetadataSessionClosedReason

    public init(userID: UInt32, userCreatedAt: IggyTimestamp, clientID: UInt128Value, session: UInt64, reason: MetadataSessionClosedReason) {
        self.userID = userID
        self.userCreatedAt = userCreatedAt
        self.clientID = clientID
        self.session = session
        self.reason = reason
    }
}

/// The typed body of a change record.
public enum MetadataChange: Sendable, Hashable {
    case streamCreated(MetadataStreamChange)
    case streamUpdated(MetadataStreamChange)
    case streamDeleted(MetadataStreamDeleted)
    case topicCreated(MetadataTopicChange)
    case topicUpdated(MetadataTopicChange)
    case topicDeleted(MetadataTopicDeleted)
    case partitionsChanged(MetadataPartitionsChanged)
    case consumerGroupCreated(MetadataConsumerGroupCreated)
    case consumerGroupDeleted(MetadataConsumerGroupDeleted)
    case consumerGroupMemberJoined(MetadataMemberJoined)
    case consumerGroupMemberLeft(MetadataMemberLeft)
    case consumerGroupPartitionHandedOver(MetadataPartitionHandedOver)
    case userCreated(MetadataUserChange)
    case userUpdated(MetadataUserChange)
    case userDeleted(MetadataUserIdentity)
    case userPasswordChanged(MetadataUserIdentity)
    case userPermissionsUpdated(MetadataUserPermissionsUpdated)
    case personalAccessTokenCreated(MetadataTokenChange)
    case personalAccessTokenDeleted(MetadataTokenDeleted)
    case sessionRegistered(MetadataSession)
    case sessionClosed(MetadataSessionClosed)
    /// A record of a group or kind this build does not know, kept with its
    /// wire codes and raw body. A mirror stops before advancing its durable
    /// cursor past it.
    case unknown(group: UInt8, kind: UInt8, body: [UInt8])

    /// The kind, nil for a record this build does not know.
    public var kind: MetadataChangeKind? {
        switch self {
        case .streamCreated: .streamCreated
        case .streamUpdated: .streamUpdated
        case .streamDeleted: .streamDeleted
        case .topicCreated: .topicCreated
        case .topicUpdated: .topicUpdated
        case .topicDeleted: .topicDeleted
        case .partitionsChanged: .partitionsChanged
        case .consumerGroupCreated: .consumerGroupCreated
        case .consumerGroupDeleted: .consumerGroupDeleted
        case .consumerGroupMemberJoined: .consumerGroupMemberJoined
        case .consumerGroupMemberLeft: .consumerGroupMemberLeft
        case .consumerGroupPartitionHandedOver: .consumerGroupPartitionHandedOver
        case .userCreated: .userCreated
        case .userUpdated: .userUpdated
        case .userDeleted: .userDeleted
        case .userPasswordChanged: .userPasswordChanged
        case .userPermissionsUpdated: .userPermissionsUpdated
        case .personalAccessTokenCreated: .personalAccessTokenCreated
        case .personalAccessTokenDeleted: .personalAccessTokenDeleted
        case .sessionRegistered: .sessionRegistered
        case .sessionClosed: .sessionClosed
        case .unknown: nil
        }
    }

    /// The group, nil for a record this build does not know.
    public var group: MetadataChangeGroup? { kind?.group }
}

/// One current state item: a resource or session as it is now.
public enum MetadataCurrentItem: Sendable, Hashable {
    case stream(MetadataStreamChange)
    case topic(MetadataTopicItem)
    case partitions(MetadataPartitionsItem)
    case consumerGroup(MetadataConsumerGroupItem)
    case consumerGroupMember(MetadataMemberItem)
    case user(MetadataUserChange)
    case personalAccessToken(MetadataTokenChange)
    case session(MetadataSession)
    /// An item of a group this build does not know, kept with its wire code
    /// and raw body.
    case unknown(group: UInt8, body: [UInt8])

    /// The group, nil for an item this build does not know.
    public var group: MetadataChangeGroup? {
        switch self {
        case .stream: .streams
        case .topic: .topics
        case .partitions: .partitions
        case .consumerGroup: .consumerGroups
        case .consumerGroupMember: .consumerGroupMembers
        case .user: .users
        case .personalAccessToken: .personalAccessTokens
        case .session: .sessions
        case .unknown: nil
        }
    }
}

/// One change record: the committed operation that produced it, its position
/// within that operation, and what is true after it.
public struct MetadataChangeRecord: Sendable, Hashable {
    public var op: UInt64
    public var ordinal: UInt32
    /// Prepare checksum of ``op``.
    public var checksum: UInt128Value
    /// Prepare timestamp of ``op``.
    public var timestamp: IggyTimestamp
    /// The acting user, nil for server actions.
    public var actorUserID: UInt32?
    public var change: MetadataChange

    public init(op: UInt64, ordinal: UInt32, checksum: UInt128Value, timestamp: IggyTimestamp, actorUserID: UInt32?, change: MetadataChange) {
        self.op = op
        self.ordinal = ordinal
        self.checksum = checksum
        self.timestamp = timestamp
        self.actorUserID = actorUserID
        self.change = change
    }
}

/// One page of the metadata change log, or of the current state.
///
/// On ``MetadataChangeStatus/ok`` keep ``next`` and send it back. On
/// ``MetadataChangeStatus/behind`` retry later, on
/// ``MetadataChangeStatus/expired`` or ``MetadataChangeStatus/reset`` rebuild
/// from the current state.
public struct MetadataChanges: Sendable, Hashable {
    public var status: MetadataChangeStatus
    public var phase: MetadataChangePhase
    /// The node's log id, nil before it applied op 1.
    public var logID: UInt128Value?
    /// Lowest whole operation from which the requested groups are complete.
    public var oldestOp: UInt64
    /// The requested groups are complete for every timestamp above this one,
    /// zero when nothing was evicted.
    public var retainedSince: IggyTimestamp
    /// Highest metadata operation the node applied.
    public var headOp: UInt64
    /// The cursor to send back: present on ok, echoed on behind, nil on
    /// expired and reset. The last current state page carries the anchor to
    /// follow the change log from.
    public var next: MetadataChangeCursor?
    /// More records remain after ``next``, or another state page exists.
    public var hasMore: Bool
    public var records: [MetadataChangeRecord]
    public var items: [MetadataCurrentItem]
    /// Continuation of a current state scan, nil once it is complete.
    public var currentPageToken: MetadataCurrentPageToken?

    public init(
        status: MetadataChangeStatus, phase: MetadataChangePhase, logID: UInt128Value?, oldestOp: UInt64, retainedSince: IggyTimestamp, headOp: UInt64,
        next: MetadataChangeCursor?, hasMore: Bool, records: [MetadataChangeRecord], items: [MetadataCurrentItem],
        currentPageToken: MetadataCurrentPageToken?
    ) {
        self.status = status
        self.phase = phase
        self.logID = logID
        self.oldestOp = oldestOp
        self.retainedSince = retainedSince
        self.headOp = headOp
        self.next = next
        self.hasMore = hasMore
        self.records = records
        self.items = items
        self.currentPageToken = currentPageToken
    }
}

// MARK: - Wire codecs

extension MetadataChange {
    /// Decodes a record body by its codes, mirroring `WireMetadataChange::decode`.
    /// A group or kind this build does not know is kept raw. A known kind
    /// under another group, a malformed body, an unknown reason, and trailing
    /// bytes are refused.
    static func decode(group: UInt8, kind: UInt8, body: ArraySlice<UInt8>) throws -> MetadataChange {
        guard let knownGroup = MetadataChangeGroup(rawValue: group), let knownKind = MetadataChangeKind(rawValue: kind) else {
            return .unknown(group: group, kind: kind, body: Array(body))
        }
        guard knownKind.group == knownGroup else {
            throw WireError.validation("change kind \(kind) does not belong to group \(group)")
        }
        var reader = ByteReader(body)
        let change: MetadataChange
        switch knownKind {
        case .streamCreated: change = .streamCreated(try .decode(from: &reader))
        case .streamUpdated: change = .streamUpdated(try .decode(from: &reader))
        case .streamDeleted: change = .streamDeleted(try .decode(from: &reader))
        case .topicCreated: change = .topicCreated(try .decode(from: &reader))
        case .topicUpdated: change = .topicUpdated(try .decode(from: &reader))
        case .topicDeleted: change = .topicDeleted(try .decode(from: &reader))
        case .partitionsChanged: change = .partitionsChanged(try .decode(from: &reader))
        case .consumerGroupCreated: change = .consumerGroupCreated(try .decode(from: &reader))
        case .consumerGroupDeleted: change = .consumerGroupDeleted(try .decode(from: &reader))
        case .consumerGroupMemberJoined: change = .consumerGroupMemberJoined(try .decode(from: &reader))
        case .consumerGroupMemberLeft: change = .consumerGroupMemberLeft(try .decode(from: &reader))
        case .consumerGroupPartitionHandedOver: change = .consumerGroupPartitionHandedOver(try .decode(from: &reader))
        case .userCreated: change = .userCreated(try .decode(from: &reader))
        case .userUpdated: change = .userUpdated(try .decode(from: &reader))
        case .userDeleted: change = .userDeleted(try .decode(from: &reader))
        case .userPasswordChanged: change = .userPasswordChanged(try .decode(from: &reader))
        case .userPermissionsUpdated: change = .userPermissionsUpdated(try .decode(from: &reader))
        case .personalAccessTokenCreated: change = .personalAccessTokenCreated(try .decode(from: &reader))
        case .personalAccessTokenDeleted: change = .personalAccessTokenDeleted(try .decode(from: &reader))
        case .sessionRegistered: change = .sessionRegistered(try .decode(from: &reader))
        case .sessionClosed: change = .sessionClosed(try .decode(from: &reader))
        }
        try reader.requireBodyEnd()
        return change
    }
}

extension MetadataCurrentItem {
    /// Decodes an item body by its group code. A group this build does not
    /// know is kept raw.
    static func decode(group: UInt8, body: ArraySlice<UInt8>) throws -> MetadataCurrentItem {
        guard let knownGroup = MetadataChangeGroup(rawValue: group) else {
            return .unknown(group: group, body: Array(body))
        }
        var reader = ByteReader(body)
        let item: MetadataCurrentItem
        switch knownGroup {
        case .streams: item = .stream(try .decode(from: &reader))
        case .topics: item = .topic(try .decode(from: &reader))
        case .partitions: item = .partitions(try .decode(from: &reader))
        case .consumerGroups: item = .consumerGroup(try .decode(from: &reader))
        case .consumerGroupMembers: item = .consumerGroupMember(try .decode(from: &reader))
        case .users: item = .user(try .decode(from: &reader))
        case .personalAccessTokens: item = .personalAccessToken(try .decode(from: &reader))
        case .sessions: item = .session(try .decode(from: &reader))
        }
        try reader.requireBodyEnd()
        return item
    }
}

extension MetadataStreamChange {
    /// `[stream_id:u32][stream_created_op:u64][created_at:u64][name][options]`
    static func decode(from reader: inout ByteReader) throws -> MetadataStreamChange {
        MetadataStreamChange(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), createdAt: try reader.readTimestamp(), name: try reader.readName(),
            options: try reader.readChangeOptions())
    }
}

extension MetadataStreamDeleted {
    /// `[stream_id:u32][stream_created_op:u64][created_at:u64][name][has:u8][requested_op:u64]`
    static func decode(from reader: inout ByteReader) throws -> MetadataStreamDeleted {
        MetadataStreamDeleted(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), createdAt: try reader.readTimestamp(), name: try reader.readName(),
            requestedOp: try reader.readOptionalUInt64())
    }
}

extension MetadataTopicChange {
    /// `[topic key][created_at:u64][name][partitions_count:u32][options]`
    static func decode(from reader: inout ByteReader) throws -> MetadataTopicChange {
        MetadataTopicChange(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), createdAt: try reader.readTimestamp(), name: try reader.readName(),
            partitionsCount: try reader.readUInt32(), options: try reader.readChangeOptions())
    }
}

extension MetadataTopicItem {
    /// `[topic key][created_at:u64][name][options]`
    static func decode(from reader: inout ByteReader) throws -> MetadataTopicItem {
        MetadataTopicItem(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), createdAt: try reader.readTimestamp(), name: try reader.readName(), options: try reader.readChangeOptions()
        )
    }
}

extension MetadataTopicDeleted {
    /// `[topic key][created_at:u64][name][has:u8][requested_op:u64]`
    static func decode(from reader: inout ByteReader) throws -> MetadataTopicDeleted {
        MetadataTopicDeleted(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), createdAt: try reader.readTimestamp(), name: try reader.readName(),
            requestedOp: try reader.readOptionalUInt64())
    }
}

extension MetadataPartitionsChanged {
    /// `[topic key][partitions_count:u32][count:u32][id:u32]*[has:u8][requested_op:u64]`
    static func decode(from reader: inout ByteReader) throws -> MetadataPartitionsChanged {
        MetadataPartitionsChanged(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), partitionsCount: try reader.readUInt32(), removedPartitionIDs: try reader.readPartitionIDs(),
            requestedOp: try reader.readOptionalUInt64())
    }
}

extension MetadataPartitionsItem {
    /// `[topic key][partitions_count:u32]`
    static func decode(from reader: inout ByteReader) throws -> MetadataPartitionsItem {
        MetadataPartitionsItem(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), partitionsCount: try reader.readUInt32())
    }
}

extension MetadataConsumerGroupCreated {
    /// `[topic key][group_id:u32][name][consumer_groups_count:u32]`
    static func decode(from reader: inout ByteReader) throws -> MetadataConsumerGroupCreated {
        MetadataConsumerGroupCreated(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), groupID: try reader.readUInt32(), name: try reader.readName(), consumerGroupsCount: try reader.readUInt32()
        )
    }
}

extension MetadataConsumerGroupDeleted {
    /// `[topic key][group_id:u32][name][consumer_groups_count:u32][has:u8][requested_op:u64]`
    static func decode(from reader: inout ByteReader) throws -> MetadataConsumerGroupDeleted {
        MetadataConsumerGroupDeleted(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), groupID: try reader.readUInt32(), name: try reader.readName(),
            consumerGroupsCount: try reader.readUInt32(),
            requestedOp: try reader.readOptionalUInt64())
    }
}

extension MetadataConsumerGroupItem {
    /// `[topic key][group_id:u32][name]`
    static func decode(from reader: inout ByteReader) throws -> MetadataConsumerGroupItem {
        MetadataConsumerGroupItem(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), groupID: try reader.readUInt32(), name: try reader.readName())
    }
}

extension MetadataMemberJoined {
    /// `[topic key][group_id:u32][member_id:u32][client_id:u128][members_count:u32][ids]`
    static func decode(from reader: inout ByteReader) throws -> MetadataMemberJoined {
        MetadataMemberJoined(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), groupID: try reader.readUInt32(), memberID: try reader.readUInt32(), clientID: try reader.readUInt128(),
            membersCount: try reader.readUInt32(), partitionIDs: try reader.readPartitionIDs())
    }
}

extension MetadataMemberLeft {
    /// `[topic key][group_id:u32][member_id:u32][client_id:u128][members_count:u32][reason:u8]`
    static func decode(from reader: inout ByteReader) throws -> MetadataMemberLeft {
        MetadataMemberLeft(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), groupID: try reader.readUInt32(), memberID: try reader.readUInt32(), clientID: try reader.readUInt128(),
            membersCount: try reader.readUInt32(), reason: try reader.readReason(MetadataMemberLeftReason.self))
    }
}

extension MetadataPartitionHandedOver {
    /// `[topic key][group_id:u32][partition_id:u32][has:u8][member_id:u32]`
    static func decode(from reader: inout ByteReader) throws -> MetadataPartitionHandedOver {
        MetadataPartitionHandedOver(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), groupID: try reader.readUInt32(), partitionID: try reader.readUInt32(),
            memberID: try reader.readOptionalUInt32())
    }
}

extension MetadataMemberItem {
    /// `[topic key][group_id:u32][member_id:u32][client_id:u128][ids]`
    static func decode(from reader: inout ByteReader) throws -> MetadataMemberItem {
        MetadataMemberItem(
            streamID: try reader.readUInt32(), streamCreatedOp: try reader.readUInt64(), topicID: try reader.readUInt32(),
            topicCreatedOp: try reader.readUInt64(), groupID: try reader.readUInt32(), memberID: try reader.readUInt32(), clientID: try reader.readUInt128(),
            partitionIDs: try reader.readPartitionIDs())
    }
}

extension MetadataUserChange {
    /// `[user_id:u32][user_created_at:u64][username][status:u8][has:u8][permissions][options]`
    static func decode(from reader: inout ByteReader) throws -> MetadataUserChange {
        let userID = try reader.readUInt32()
        let userCreatedAt = try reader.readTimestamp()
        let username = try reader.readName()
        let statusCode = try reader.readUInt8()
        guard let status = UserStatus(rawValue: statusCode) else {
            throw IggyError(.invalidUserStatus, context: "unknown user status \(statusCode)")
        }
        return MetadataUserChange(
            userID: userID, userCreatedAt: userCreatedAt, username: username, status: status, permissions: try reader.readOptionalPermissions(),
            options: try reader.readChangeOptions())
    }
}

extension MetadataUserIdentity {
    /// `[user_id:u32][user_created_at:u64][username]`
    static func decode(from reader: inout ByteReader) throws -> MetadataUserIdentity {
        MetadataUserIdentity(userID: try reader.readUInt32(), userCreatedAt: try reader.readTimestamp(), username: try reader.readName())
    }
}

extension MetadataUserPermissionsUpdated {
    /// `[user_id:u32][user_created_at:u64][username][has:u8][permissions][reason:u8]`
    static func decode(from reader: inout ByteReader) throws -> MetadataUserPermissionsUpdated {
        MetadataUserPermissionsUpdated(
            userID: try reader.readUInt32(), userCreatedAt: try reader.readTimestamp(), username: try reader.readName(),
            permissions: try reader.readOptionalPermissions(), reason: try reader.readReason(MetadataPermissionsUpdatedReason.self))
    }
}

extension MetadataTokenChange {
    /// `[user_id:u32][user_created_at:u64][name][expiry:u64]`, expiry 0 for a token that never expires.
    static func decode(from reader: inout ByteReader) throws -> MetadataTokenChange {
        let userID = try reader.readUInt32()
        let userCreatedAt = try reader.readTimestamp()
        let name = try reader.readName()
        let expiry = try reader.readUInt64()
        return MetadataTokenChange(userID: userID, userCreatedAt: userCreatedAt, name: name, expiry: expiry == 0 ? nil : IggyTimestamp(microseconds: expiry))
    }
}

extension MetadataTokenDeleted {
    /// `[user_id:u32][user_created_at:u64][name][reason:u8]`
    static func decode(from reader: inout ByteReader) throws -> MetadataTokenDeleted {
        MetadataTokenDeleted(
            userID: try reader.readUInt32(), userCreatedAt: try reader.readTimestamp(), name: try reader.readName(),
            reason: try reader.readReason(MetadataTokenDeletedReason.self))
    }
}

extension MetadataSession {
    /// `[user_id:u32][user_created_at:u64][client_id:u128][session:u64]`
    static func decode(from reader: inout ByteReader) throws -> MetadataSession {
        MetadataSession(
            userID: try reader.readUInt32(), userCreatedAt: try reader.readTimestamp(), clientID: try reader.readUInt128(), session: try reader.readUInt64())
    }
}

extension MetadataSessionClosed {
    /// `[user_id:u32][user_created_at:u64][client_id:u128][session:u64][reason:u8]`
    static func decode(from reader: inout ByteReader) throws -> MetadataSessionClosed {
        MetadataSessionClosed(
            userID: try reader.readUInt32(), userCreatedAt: try reader.readTimestamp(), clientID: try reader.readUInt128(), session: try reader.readUInt64(),
            reason: try reader.readReason(MetadataSessionClosedReason.self))
    }
}

extension ByteReader {
    /// A presence byte, refusing anything but 0 and 1 as the Rust codec does.
    mutating func readFlag() throws -> Bool {
        let offset = position
        switch try readUInt8() {
        case 0: return false
        case 1: return true
        case let value: throw WireError.validation("presence byte \(value) at offset \(offset) is not 0 or 1")
        }
    }

    mutating func readOptionalUInt64() throws -> UInt64? {
        guard try readFlag() else {
            return nil
        }
        return try readUInt64()
    }

    mutating func readOptionalUInt32() throws -> UInt32? {
        guard try readFlag() else {
            return nil
        }
        return try readUInt32()
    }

    mutating func readTimestamp() throws -> IggyTimestamp {
        IggyTimestamp(microseconds: try readUInt64())
    }

    /// `[count:u32][id:u32]*`, refusing ids that are not strictly ascending.
    mutating func readPartitionIDs() throws -> [UInt32] {
        let count = try readCount()
        var ids: [UInt32] = []
        ids.reserveCapacity(min(count, remaining / 4))
        for _ in 0..<count {
            let id = try readUInt32()
            if let previous = ids.last, previous >= id {
                throw WireError.validation("partition ids must be strictly ascending")
            }
            ids.append(id)
        }
        return ids
    }

    /// `[count:u32]`, refusing a count a 32-bit `Int` cannot hold instead of trapping.
    mutating func readCount() throws -> Int {
        let offset = position
        let declared = try readUInt32()
        guard let count = Int(exactly: declared) else {
            throw WireError.validation("count \(declared) at offset \(offset) does not fit the platform Int")
        }
        return count
    }

    mutating func readReason<Reason: RawRepresentable>(_: Reason.Type) throws -> Reason where Reason.RawValue == UInt8 {
        let offset = position
        let code = try readUInt8()
        guard let reason = Reason(rawValue: code) else {
            throw WireError.validation("unknown \(Reason.self) code \(code) at offset \(offset)")
        }
        return reason
    }

    mutating func readOptionalPermissions() throws -> Permissions? {
        guard try readFlag() else {
            return nil
        }
        return try Permissions.decode(from: &self)
    }

    /// `[state:u8]`, then for state 0 the explicit and derived blocks as the
    /// read responses carry them, nothing for state 1.
    mutating func readChangeOptions() throws -> MetadataOptions {
        let offset = position
        switch try readUInt8() {
        case 0:
            let explicit = try readPrefixedBytes()
            let derived = try readPrefixedBytes()
            return .readable(try OptionsBlock.decodeSplit(explicit: explicit, derived: derived))
        case 1:
            return .unreadable
        case let state:
            throw WireError.validation("options state \(state) at offset \(offset) is not 0 or 1")
        }
    }

    /// Refuses bytes left after a change body.
    func requireBodyEnd() throws {
        guard isAtEnd else {
            throw WireError.validation("\(remaining) trailing bytes after a change body")
        }
    }
}

/// Unpadded Base64url on top of Foundation. Decoding accepts only the text
/// ``encode(_:)`` writes back, so padding, other alphabets and nonzero
/// trailing bits are refused.
enum Base64URL {
    static func encode(_ bytes: [UInt8]) -> String {
        Data(bytes).base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    /// The bytes after `prefix`, or nil for text that is not canonical.
    static func decode(_ text: String, prefix: String) -> [UInt8]? {
        guard text.utf8.starts(with: prefix.utf8) else {
            return nil
        }
        let encoded = String(decoding: text.utf8.dropFirst(prefix.utf8.count), as: UTF8.self)
        let standard = encoded.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
        let padding = String(repeating: "=", count: (4 - standard.utf8.count % 4) % 4)
        guard let data = Data(base64Encoded: standard + padding), encode([UInt8](data)) == encoded else {
            return nil
        }
        return [UInt8](data)
    }
}
