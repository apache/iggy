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

// The canonical form of a metadata changes answer is the HTTP JSON that the
// fixture core/common/fixtures/metadata_changes.json names "expected". The
// views follow the fixture test of the SDK, MetadataChangesTests.swift: a
// record keeps the fields of its body at its top level, an absent optional
// field is left out, and a 128-bit value is 32 lowercase hex characters.

extension MetadataChanges {
    func canonical() throws -> JSON {
        .object([
            "status": .string(status.description), "phase": .string(phase.description), "log_id": logID.map { .string(hex128($0)) } ?? .null,
            "oldest_op": .number(oldestOp), "retained_since": .number(retainedSince.microseconds), "head_op": .number(headOp),
            "next": next.map { .string($0.description) } ?? .null, "has_more": .bool(hasMore),
            "records": .array(try records.map { try $0.canonical() }), "items": .array(try items.map { try $0.canonical() }),
            "current_page_token": currentPageToken.map { .string($0.description) } ?? .null,
        ])
    }
}

extension MetadataChangeRecord {
    func canonical() throws -> JSON {
        let envelope: [String: JSON?] = [
            "op": .number(op), "ordinal": JSON(ordinal), "checksum": .string(hex128(checksum)), "timestamp": .number(timestamp.microseconds),
            "actor_user_id": actorUserID.map { JSON($0) },
        ]
        // A record of a group or kind that the SDK does not know keeps its
        // codes as text and its body as base64.
        if case .unknown(let group, let kind, let body) = change {
            return view(envelope, ["group": .string(String(group)), "kind": .string(String(kind)), "body": .string(Data(body).base64EncodedString())])
        }
        let kind: [String: JSON?] = change.kind.map { ["group": .string($0.group.description), "kind": .string($0.description)] } ?? [:]
        return view(envelope, kind, try change.fields())
    }
}

extension MetadataCurrentItem {
    func canonical() throws -> JSON {
        switch self {
        case .stream(let body): view(["group": groupName], try body.fields())
        case .topic(let body):
            view(
                ["group": groupName], topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                ["created_at": .number(body.createdAt.microseconds), "name": .string(body.name)], try optionFields(body.options))
        case .partitions(let body):
            view(
                ["group": groupName], topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                ["partitions_count": JSON(body.partitionsCount)])
        case .consumerGroup(let body):
            view(
                ["group": groupName], topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                ["group_id": JSON(body.groupID), "name": .string(body.name)])
        case .consumerGroupMember(let body):
            view(
                ["group": groupName], topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                [
                    "group_id": JSON(body.groupID), "member_id": JSON(body.memberID), "client_id": .string(hex128(body.clientID)),
                    "partition_ids": ids(body.partitionIDs),
                ])
        case .user(let body): view(["group": groupName], try body.fields())
        case .personalAccessToken(let body): view(["group": groupName], body.fields())
        case .session(let body): view(["group": groupName], body.fields())
        case .unknown(let group, let body): view(["group": .string(String(group)), "body": .string(Data(body).base64EncodedString())])
        }
    }

    private var groupName: JSON? {
        group.map { .string($0.description) }
    }
}

extension MetadataChange {
    /// The fields of the body of a known record.
    fileprivate func fields() throws -> [String: JSON?] {
        switch self {
        case .streamCreated(let body), .streamUpdated(let body):
            return try body.fields()
        case .streamDeleted(let body):
            return merged(
                streamKey(body.streamID, body.streamCreatedOp),
                ["created_at": .number(body.createdAt.microseconds), "name": .string(body.name), "requested_op": body.requestedOp.map(JSON.number)])
        case .topicCreated(let body), .topicUpdated(let body):
            return merged(
                topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                ["created_at": .number(body.createdAt.microseconds), "name": .string(body.name), "partitions_count": JSON(body.partitionsCount)],
                try optionFields(body.options))
        case .topicDeleted(let body):
            return merged(
                topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                ["created_at": .number(body.createdAt.microseconds), "name": .string(body.name), "requested_op": body.requestedOp.map(JSON.number)])
        case .partitionsChanged(let body):
            return merged(
                topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                [
                    "partitions_count": JSON(body.partitionsCount), "removed_partition_ids": ids(body.removedPartitionIDs),
                    "requested_op": body.requestedOp.map(JSON.number),
                ])
        case .consumerGroupCreated(let body):
            return merged(
                topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                ["group_id": JSON(body.groupID), "name": .string(body.name), "consumer_groups_count": JSON(body.consumerGroupsCount)])
        case .consumerGroupDeleted(let body):
            return merged(
                topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                [
                    "group_id": JSON(body.groupID), "name": .string(body.name), "consumer_groups_count": JSON(body.consumerGroupsCount),
                    "requested_op": body.requestedOp.map(JSON.number),
                ])
        case .consumerGroupMemberJoined(let body):
            return merged(
                topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                [
                    "group_id": JSON(body.groupID), "member_id": JSON(body.memberID), "client_id": .string(hex128(body.clientID)),
                    "members_count": JSON(body.membersCount), "partition_ids": ids(body.partitionIDs),
                ])
        case .consumerGroupMemberLeft(let body):
            return merged(
                topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                [
                    "group_id": JSON(body.groupID), "member_id": JSON(body.memberID), "client_id": .string(hex128(body.clientID)),
                    "members_count": JSON(body.membersCount), "reason": .string(body.reason.description),
                ])
        case .consumerGroupPartitionHandedOver(let body):
            return merged(
                topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp),
                ["group_id": JSON(body.groupID), "partition_id": JSON(body.partitionID), "member_id": body.memberID.map { JSON($0) }])
        case .userCreated(let body), .userUpdated(let body):
            return try body.fields()
        case .userDeleted(let body), .userPasswordChanged(let body):
            return merged(userKey(body.userID, body.userCreatedAt), ["username": .string(body.username)])
        case .userPermissionsUpdated(let body):
            return merged(
                userKey(body.userID, body.userCreatedAt),
                ["username": .string(body.username), "permissions": permissionsView(body.permissions), "reason": .string(body.reason.description)])
        case .personalAccessTokenCreated(let body):
            return body.fields()
        case .personalAccessTokenDeleted(let body):
            return merged(userKey(body.userID, body.userCreatedAt), ["name": .string(body.name), "reason": .string(body.reason.description)])
        case .sessionRegistered(let body):
            return body.fields()
        case .sessionClosed(let body):
            return merged(
                userKey(body.userID, body.userCreatedAt),
                ["client_id": .string(hex128(body.clientID)), "session": .number(body.session), "reason": .string(body.reason.description)])
        case .unknown:
            return [:]
        }
    }
}

extension MetadataStreamChange {
    fileprivate func fields() throws -> [String: JSON?] {
        merged(
            streamKey(streamID, streamCreatedOp), ["created_at": .number(createdAt.microseconds), "name": .string(name)], try optionFields(options))
    }
}

extension MetadataUserChange {
    fileprivate func fields() throws -> [String: JSON?] {
        merged(
            userKey(userID, userCreatedAt),
            ["username": .string(username), "status": .string(status.description), "permissions": permissionsView(permissions)],
            try optionFields(options))
    }
}

extension MetadataTokenChange {
    fileprivate func fields() -> [String: JSON?] {
        merged(userKey(userID, userCreatedAt), ["name": .string(name), "expiry": expiry.map { .number($0.microseconds) }])
    }
}

extension MetadataSession {
    fileprivate func fields() -> [String: JSON?] {
        merged(userKey(userID, userCreatedAt), ["client_id": .string(hex128(clientID)), "session": .number(session)])
    }
}

/// The object of the parts, without the optional fields that are absent.
private func view(_ parts: [String: JSON?]...) -> JSON {
    .object(join(parts).compactMapValues { $0 })
}

private func merged(_ parts: [String: JSON?]...) -> [String: JSON?] {
    join(parts)
}

private func join(_ parts: [[String: JSON?]]) -> [String: JSON?] {
    parts.reduce(into: [:]) { joined, part in joined.merge(part) { _, new in new } }
}

private func streamKey(_ streamID: UInt32, _ streamCreatedOp: UInt64) -> [String: JSON?] {
    ["stream_id": JSON(streamID), "stream_created_op": .number(streamCreatedOp)]
}

private func topicKey(_ streamID: UInt32, _ streamCreatedOp: UInt64, _ topicID: UInt32, _ topicCreatedOp: UInt64) -> [String: JSON?] {
    merged(streamKey(streamID, streamCreatedOp), ["topic_id": JSON(topicID), "topic_created_op": .number(topicCreatedOp)])
}

private func userKey(_ userID: UInt32, _ userCreatedAt: IggyTimestamp) -> [String: JSON?] {
    ["user_id": JSON(userID), "user_created_at": .number(userCreatedAt.microseconds)]
}

/// Options that the node cannot read are null and flagged, so a mirror keeps
/// the values that it has.
private func optionFields(_ options: MetadataOptions) throws -> [String: JSON?] {
    switch options {
    case .readable(let options): ["options": try options.canonical()]
    case .unreadable: ["options": .null, "options_unreadable": .bool(true)]
    }
}

/// The permissions in the JSON form of a user read, with stream permissions.
private func permissionsView(_ permissions: Permissions?) -> JSON {
    guard let permissions else {
        return .null
    }
    var streams: [String: JSON] = [:]
    for (streamID, stream) in permissions.streams {
        var topics: [String: JSON] = [:]
        for (topicID, topic) in stream.topics {
            topics[String(topicID)] = .object([
                "manage_topic": .bool(topic.manageTopic), "read_topic": .bool(topic.readTopic), "poll_messages": .bool(topic.pollMessages),
                "send_messages": .bool(topic.sendMessages),
            ])
        }
        streams[String(streamID)] = .object([
            "manage_stream": .bool(stream.manageStream), "read_stream": .bool(stream.readStream), "manage_topics": .bool(stream.manageTopics),
            "read_topics": .bool(stream.readTopics), "poll_messages": .bool(stream.pollMessages), "send_messages": .bool(stream.sendMessages),
            "topics": topics.isEmpty ? .null : .object(topics),
        ])
    }
    return .object(["global": permissions.global.canonical(), "streams": streams.isEmpty ? .null : .object(streams)])
}

private func ids(_ ids: [UInt32]) -> JSON {
    .array(ids.map { JSON($0) })
}

/// The high half then the low half, as 32 lowercase hex characters.
private func hex128(_ value: UInt128Value) -> String {
    zeroPadded(String(value.high, radix: 16), to: 16) + zeroPadded(String(value.low, radix: 16), to: 16)
}
