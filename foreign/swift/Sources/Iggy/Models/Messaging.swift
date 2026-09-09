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

/// Who is polling: a standalone consumer or a member of a consumer group.
public struct Consumer: Sendable, Hashable, Codable, CustomStringConvertible {
    public enum Kind: UInt8, Sendable, Hashable, Codable {
        case consumer = 1
        case consumerGroup = 2
    }

    public var kind: Kind
    public var id: Identifier

    public init(kind: Kind, id: Identifier) {
        self.kind = kind
        self.id = id
    }

    /// A standalone consumer.
    public static func consumer(_ id: Identifier) -> Consumer {
        Consumer(kind: .consumer, id: id)
    }

    /// A member of a consumer group.
    public static func group(_ id: Identifier) -> Consumer {
        Consumer(kind: .consumerGroup, id: id)
    }

    /// The consumer the Rust SDK uses when none is named: consumer id 1.
    public static let `default` = Consumer(kind: .consumer, id: Identifier(numeric: 1))

    public var description: String {
        switch kind {
        case .consumer: "consumer|\(id)"
        case .consumerGroup: "consumer_group|\(id)"
        }
    }

    func encode(into writer: inout ByteWriter) {
        writer.write(kind.rawValue)
        id.encode(into: &writer)
    }

    static func decode(from reader: inout ByteReader) throws -> Consumer {
        let code = try reader.readUInt8()
        guard let kind = Kind(rawValue: code) else {
            throw WireError.invalidDiscriminant(type: "Consumer", value: code)
        }
        return Consumer(kind: kind, id: try Identifier.decode(from: &reader))
    }
}

/// How a batch of messages picks its partition.
///
/// Under the consensus protocol the server only routes explicit partitions,
/// so ``balanced`` and ``messagesKey(_:)`` are resolved on the client before
/// the batch is sent: balanced round-robins over the topic's partitions and a
/// key is hashed with XXH32, the same way every other SDK does it.
public enum Partitioning: Sendable, Hashable, Codable {
    /// Round-robin over the topic's partitions.
    case balanced
    /// A specific partition.
    case partition(UInt32)
    /// Hash of the key selects the partition, so one key always lands on the
    /// same partition. The key is 1 to 255 bytes.
    case messagesKey([UInt8])

    public static func messagesKey(_ key: String) -> Partitioning {
        .messagesKey(Array(key.utf8))
    }

    public static func messagesKey(_ key: UInt32) -> Partitioning {
        .messagesKey(key.littleEndianBytes)
    }

    public static func messagesKey(_ key: UInt64) -> Partitioning {
        .messagesKey(key.littleEndianBytes)
    }

    public static func messagesKey(_ key: UInt128Value) -> Partitioning {
        .messagesKey(key.littleEndianBytes)
    }

    private static let balancedCode: UInt8 = 1
    private static let partitionCode: UInt8 = 2
    private static let messagesKeyCode: UInt8 = 3

    func validate() throws {
        if case .messagesKey(let key) = self, key.isEmpty || key.count > 255 {
            throw IggyError(.invalidKeyValueLength, context: "messages key must be 1-255 bytes, got \(key.count)")
        }
    }

    /// `[kind:1][length:1][value]`.
    func encode(into writer: inout ByteWriter) {
        switch self {
        case .balanced:
            writer.write(Self.balancedCode)
            writer.write(UInt8(0))
        case .partition(let id):
            writer.write(Self.partitionCode)
            writer.write(UInt8(4))
            writer.write(id)
        case .messagesKey(let key):
            writer.write(Self.messagesKeyCode)
            writer.write(UInt8(key.count))
            writer.write(key)
        }
    }

    var encodedSize: Int {
        switch self {
        case .balanced: 2
        case .partition: 6
        case .messagesKey(let key): 2 + key.count
        }
    }

    static func decode(from reader: inout ByteReader) throws -> Partitioning {
        let kind = try reader.readUInt8()
        let length = Int(try reader.readUInt8())
        switch kind {
        case balancedCode:
            guard length == 0 else {
                throw WireError.validation("balanced partitioning must have length 0, got \(length)")
            }
            return .balanced
        case partitionCode:
            guard length == 4 else {
                throw WireError.validation("partition_id partitioning must have length 4, got \(length)")
            }
            return .partition(try reader.readUInt32())
        case messagesKeyCode:
            guard length > 0 else {
                throw WireError.validation("messages_key partitioning cannot have empty key")
            }
            return .messagesKey(Array(try reader.readBytes(length)))
        default:
            throw WireError.invalidDiscriminant(type: "Partitioning", value: kind)
        }
    }
}

/// Where a poll starts reading a partition.
public struct PollingStrategy: Sendable, Hashable, Codable, CustomStringConvertible {
    public enum Kind: UInt8, Sendable, Hashable, Codable {
        case offset = 1
        case timestamp = 2
        case first = 3
        case last = 4
        case next = 5
    }

    public var kind: Kind
    public var value: UInt64

    public init(kind: Kind, value: UInt64) {
        self.kind = kind
        self.value = value
    }

    /// Resume from a specific offset.
    public static func offset(_ offset: UInt64) -> PollingStrategy {
        PollingStrategy(kind: .offset, value: offset)
    }

    /// Resume from the first message at or after a timestamp.
    public static func timestamp(_ timestamp: IggyTimestamp) -> PollingStrategy {
        PollingStrategy(kind: .timestamp, value: timestamp.microseconds)
    }

    /// Start from the beginning of the partition.
    public static let first = PollingStrategy(kind: .first, value: 0)
    /// Start from the end of the partition.
    public static let last = PollingStrategy(kind: .last, value: 0)
    /// Continue after the consumer's stored offset.
    public static let next = PollingStrategy(kind: .next, value: 0)

    public var description: String {
        switch kind {
        case .offset: "offset|\(value)"
        case .timestamp: "timestamp|\(value)"
        case .first: "first|0"
        case .last: "last|0"
        case .next: "next|0"
        }
    }

    /// `[kind:1][value:8]`.
    func encode(into writer: inout ByteWriter) {
        writer.write(kind.rawValue)
        writer.write(value)
    }

    static func decode(from reader: inout ByteReader) throws -> PollingStrategy {
        let code = try reader.readUInt8()
        guard let kind = Kind(rawValue: code) else {
            throw WireError.invalidDiscriminant(type: "PollingStrategy", value: code)
        }
        return PollingStrategy(kind: kind, value: try reader.readUInt64())
    }
}

/// Acknowledgement policy for consumer-offset writes.
public enum AckLevel: UInt8, Sendable, Hashable, Codable {
    /// Local fast path for a single-replica partition.
    case noAck = 0
    /// Reply only after a quorum of replicas committed the write. The default.
    case quorum = 1
}

/// Microseconds since the Unix epoch, the timestamp unit of the whole
/// platform.
public struct IggyTimestamp: Sendable, Hashable, Codable, Comparable, CustomStringConvertible {
    public var microseconds: UInt64

    public init(microseconds: UInt64) {
        self.microseconds = microseconds
    }

    public static var now: IggyTimestamp {
        IggyTimestamp(date: .init())
    }

    public static let zero = IggyTimestamp(microseconds: 0)

    public init(date: Date) {
        let interval = date.timeIntervalSince1970
        microseconds = interval > 0 ? UInt64(interval * 1_000_000) : 0
    }

    public var date: Date {
        Date(timeIntervalSince1970: Double(microseconds) / 1_000_000)
    }

    public static func < (lhs: IggyTimestamp, rhs: IggyTimestamp) -> Bool {
        lhs.microseconds < rhs.microseconds
    }

    public var description: String {
        String(microseconds)
    }
}
