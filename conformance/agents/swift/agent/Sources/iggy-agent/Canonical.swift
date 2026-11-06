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

// The canonical result forms of core/conformance/src/canonical.rs. The
// orchestrator decodes each result into its form and refuses a field that the
// form does not have. Sizes are byte counts and timestamps are microseconds.

extension StreamInfo {
    func canonical() throws -> JSON {
        .object([
            "id": JSON(id), "name": .string(name), "created_at": .number(createdAt.microseconds), "size_bytes": .number(sizeBytes),
            "messages_count": .number(messagesCount), "topics_count": JSON(topicsCount), "options": try options.canonical(),
        ])
    }
}

extension StreamDetails {
    func canonical() throws -> JSON {
        .object([
            "id": JSON(id), "name": .string(name), "created_at": .number(createdAt.microseconds), "size_bytes": .number(sizeBytes),
            "messages_count": .number(messagesCount), "topics_count": JSON(topicsCount), "topics": .array(try topics.map { try $0.canonical() }),
            "options": try options.canonical(),
        ])
    }
}

extension Topic {
    func canonical() throws -> JSON {
        .object([
            "id": JSON(id), "name": .string(name), "created_at": .number(createdAt.microseconds), "size_bytes": .number(sizeBytes),
            "messages_count": .number(messagesCount), "partitions_count": JSON(partitionsCount), "options": try options.canonical(),
        ])
    }
}

extension TopicDetails {
    func canonical() throws -> JSON {
        .object([
            "id": JSON(id), "name": .string(name), "created_at": .number(createdAt.microseconds), "size_bytes": .number(sizeBytes),
            "messages_count": .number(messagesCount), "partitions_count": JSON(partitionsCount),
            "partitions": .array(partitions.map { $0.canonical() }), "options": try options.canonical(),
        ])
    }
}

extension Partition {
    func canonical() -> JSON {
        .object([
            "id": JSON(id), "created_at": .number(createdAt.microseconds), "segments_count": JSON(segmentsCount),
            "current_offset": .number(currentOffset), "size_bytes": .number(sizeBytes), "messages_count": .number(messagesCount),
        ])
    }
}

extension PolledMessages {
    func canonical() throws -> JSON {
        .object([
            "partition_id": JSON(partitionID), "current_offset": .number(currentOffset), "count": JSON(count),
            "messages": .array(try messages.map { try $0.canonical() }),
        ])
    }
}

extension IggyMessage {
    /// The id as decimal text, `payload` only for a payload that is UTF-8,
    /// and `headers` only for a message that has some.
    func canonical() throws -> JSON {
        var fields: [String: JSON] = [
            "offset": .number(offset), "id": .string(id.description), "timestamp": .number(timestamp.microseconds),
            "origin_timestamp": .number(originTimestamp.microseconds), "checksum": .number(checksum), "payload_hex": .string(hex(payload)),
        ]
        if let text = String(validating: payload, as: UTF8.self) {
            fields["payload"] = .string(text)
        }
        if rawUserHeaders != nil {
            guard let userHeaders else {
                throw Fault("result: the SDK cannot decode the user headers of the message at offset \(offset)")
            }
            var headers: [String: JSON] = [:]
            for (key, value) in userHeaders {
                let entry: JSON = .object(["kind": .string(value.kind.description), "value": .string(try value.text())])
                headers[String(decoding: key.bytes, as: UTF8.self)] = entry
            }
            fields["headers"] = .object(headers)
        }
        return .object(fields)
    }
}

extension ConsumerGroupDetails {
    func canonical() -> JSON {
        .object([
            "id": JSON(id), "name": .string(name), "partitions_count": JSON(partitionsCount), "members_count": JSON(membersCount),
            "members": .array(
                members.map { member in
                    .object([
                        "id": JSON(member.id), "partitions_count": JSON(member.partitionsCount),
                        "partitions": .array(member.partitions.map { JSON($0) }),
                    ])
                }),
        ])
    }
}

extension UserInfoDetails {
    /// Global permissions only: stream permissions are not in format 1.
    func canonical() throws -> JSON {
        .object([
            "id": JSON(id), "username": .string(username), "status": .string(status.description), "created_at": .number(createdAt.microseconds),
            "permissions": permissions.map { .object(["global": $0.global.canonical()]) } ?? .null, "options": try options.canonical(),
        ])
    }
}

extension GlobalPermissions {
    func canonical() -> JSON {
        .object([
            "manage_servers": .bool(manageServers), "read_servers": .bool(readServers), "manage_users": .bool(manageUsers),
            "read_users": .bool(readUsers), "manage_streams": .bool(manageStreams), "read_streams": .bool(readStreams),
            "manage_topics": .bool(manageTopics), "read_topics": .bool(readTopics), "poll_messages": .bool(pollMessages),
            "send_messages": .bool(sendMessages),
        ])
    }
}

extension OptionSpec {
    func canonical() -> JSON {
        .object([
            "key": .string(key), "kind": .string(kind.description), "updatable": .bool(flags.contains(.updatable)),
            "resettable": .bool(flags.contains(.resettable)), "secret": .bool(flags.contains(.secret)),
            "read_only": .bool(flags.contains(.readOnly)), "critical": .bool(flags.contains(.critical)),
        ])
    }
}

extension ResourceOptions {
    /// The HTTP JSON form of an option map, which the codec fixtures share:
    /// `{key: {value, kind, explicit}}` with the value as reversible text.
    func canonical() throws -> JSON {
        .object(
            try mapValues { option in
                .object(["value": .string(try option.value.text()), "kind": .string(option.value.kind.description), "explicit": .bool(option.explicit)])
            })
    }
}

extension HeaderValue {
    /// The reversible text of the value, as `resource_options_json` in the
    /// Rust SDK writes it: UTF-8 for strings, true or false, base-10
    /// integers, padded standard base64 for raw bytes, and 0x with the
    /// IEEE-754 bits in fixed-width lowercase hex for floats.
    func text() throws -> String {
        switch kind {
        case .raw:
            return Data(bytes).base64EncodedString()
        case .string:
            guard let text = String(validating: bytes, as: UTF8.self) else {
                throw Fault("a string value that is not UTF-8")
            }
            return text
        case .bool:
            switch bytes {
            case [0]: return "false"
            case [1]: return "true"
            default: throw Fault("a bool value of the bytes \(hex(bytes))")
            }
        case .int8: return String(try integer(Int8.self))
        case .int16: return String(try integer(Int16.self))
        case .int32: return String(try integer(Int32.self))
        case .int64: return String(try integer(Int64.self))
        case .int128: return String(try integer(Int128.self))
        case .uint8: return String(try integer(UInt8.self))
        case .uint16: return String(try integer(UInt16.self))
        case .uint32: return String(try integer(UInt32.self))
        case .uint64: return String(try integer(UInt64.self))
        case .uint128: return String(try integer(UInt128.self))
        case .float32: return "0x" + zeroPadded(String(try integer(UInt32.self), radix: 16), to: 8)
        case .float64: return "0x" + zeroPadded(String(try integer(UInt64.self), radix: 16), to: 16)
        }
    }

    /// Reads the text that ``text()`` writes. A text that is not the
    /// canonical one, such as `+1`, is refused, so a value has one text.
    init(kind: HeaderKind, text: String) throws {
        let bytes: [UInt8]
        switch kind {
        case .raw:
            guard let decoded = Data(base64Encoded: text) else {
                throw Fault("\(text) is not base64")
            }
            bytes = Array(decoded)
        case .string: bytes = Array(text.utf8)
        case .bool: bytes = text == "true" ? [1] : [0]
        case .int8: bytes = try Self.littleEndian(Int8(text), text)
        case .int16: bytes = try Self.littleEndian(Int16(text), text)
        case .int32: bytes = try Self.littleEndian(Int32(text), text)
        case .int64: bytes = try Self.littleEndian(Int64(text), text)
        case .int128: bytes = try Self.littleEndian(Int128(text), text)
        case .uint8: bytes = try Self.littleEndian(UInt8(text), text)
        case .uint16: bytes = try Self.littleEndian(UInt16(text), text)
        case .uint32: bytes = try Self.littleEndian(UInt32(text), text)
        case .uint64: bytes = try Self.littleEndian(UInt64(text), text)
        case .uint128: bytes = try Self.littleEndian(UInt128(text), text)
        case .float32: bytes = try Self.littleEndian(text.hasPrefix("0x") ? UInt32(text.dropFirst(2), radix: 16) : nil, text)
        case .float64: bytes = try Self.littleEndian(text.hasPrefix("0x") ? UInt64(text.dropFirst(2), radix: 16) : nil, text)
        }
        try self.init(kind: kind, bytes: bytes)
        guard try self.text() == text else {
            throw Fault("\(text) is not the text of a \(kind) value")
        }
    }

    private func integer<Integer: FixedWidthInteger>(_: Integer.Type) throws -> Integer {
        guard bytes.count == Integer.bitWidth / 8 else {
            throw Fault("a \(kind) value of \(bytes.count) bytes")
        }
        return Integer(truncatingIfNeeded: bytes.reversed().reduce(UInt128(0)) { $0 << 8 | UInt128($1) })
    }

    private static func littleEndian<Integer: FixedWidthInteger>(_ value: Integer?, _ text: String) throws -> [UInt8] {
        guard let value else {
            throw Fault("\(text) is not a \(Integer.self)")
        }
        return withUnsafeBytes(of: value.littleEndian) { Array($0) }
    }
}
