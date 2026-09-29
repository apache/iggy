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
public struct Consumer: Sendable, Hashable, CustomStringConvertible {
    public enum Kind: UInt8, Sendable, Hashable {
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

    /// The consumer every other SDK and the CLI use when none is named:
    /// consumer id 0, so offsets are shared with them.
    public static let `default` = Consumer(kind: .consumer, id: Identifier(numeric: 0))

    public var description: String {
        switch kind {
        case .consumer: "consumer|\(id)"
        case .consumerGroup: "consumer_group|\(id)"
        }
    }

    func encode(into writer: inout ByteWriter) throws {
        writer.write(kind.rawValue)
        try id.encode(into: &writer)
    }
}

/// How a batch of messages picks its partition.
///
/// Under the consensus protocol the server only routes explicit partitions,
/// so ``balanced`` and ``messagesKey(_:)`` are resolved on the client before
/// the batch is sent: balanced round-robins over the topic's partitions and a
/// key is hashed with XXH32, the same way every other SDK does it.
public enum Partitioning: Sendable, Hashable {
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

    /// The width is part of the label because it is part of the key: the
    /// same number as a `uint64` lands on a different partition.
    public static func messagesKey(uint32 key: UInt32) -> Partitioning {
        .messagesKey(key.littleEndianBytes)
    }

    public static func messagesKey(uint64 key: UInt64) -> Partitioning {
        .messagesKey(key.littleEndianBytes)
    }

    public static func messagesKey(uint128 key: UInt128Value) -> Partitioning {
        .messagesKey(key.littleEndianBytes)
    }

    private static let balancedCode: UInt8 = 1
    private static let partitionCode: UInt8 = 2
    private static let messagesKeyCode: UInt8 = 3

    /// A messages key outside 1...255 bytes cannot be framed. The client
    /// checks this before it resolves the key to a partition, and `encode`
    /// checks it again before writing the length byte.
    func validate() throws {
        if case .messagesKey(let key) = self, key.isEmpty || key.count > Int(UInt8.max) {
            throw IggyError(.invalidKeyValueLength, context: "messages key must be 1-255 bytes, got \(key.count)")
        }
    }

    /// `[kind:1][length:1][value]`.
    func encode(into writer: inout ByteWriter) throws {
        try validate()
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
}

/// Where a poll starts reading a partition.
public struct PollingStrategy: Sendable, Hashable, CustomStringConvertible {
    public enum Kind: UInt8, Sendable, Hashable {
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
}

/// Acknowledgement policy for consumer-offset write commands.
public enum AckLevel: UInt8, Sendable, Hashable {
    /// Local fast path for a single-replica partition. Replicated partitions
    /// commit offset writes through VSR before replying, including when this
    /// acknowledgement value is selected. On a single replica, a
    /// directory-sync failure can be reported after the mutation became
    /// visible; its crash durability is then unknown, and retrying a deletion
    /// that already took effect can return `consumerOffsetNotFound`.
    case noAck = 0
    /// Submit through the partition VSR consensus pipeline and respond only
    /// after the write has been committed by a quorum of replicas. The
    /// default for explicit client writes.
    case quorum = 1
}

/// Microseconds since the Unix epoch, the timestamp unit of the whole
/// platform.
public struct IggyTimestamp: Sendable, Hashable, Comparable, CustomStringConvertible {
    public var microseconds: UInt64

    public init(microseconds: UInt64) {
        self.microseconds = microseconds
    }

    public static var now: IggyTimestamp {
        IggyTimestamp(date: .init())
    }

    public static let zero = IggyTimestamp(microseconds: 0)

    /// Dates before the epoch clamp to zero and dates past the `UInt64`
    /// microsecond range clamp to `UInt64.max` instead of trapping.
    public init(date: Date) {
        let interval = date.timeIntervalSince1970 * 1_000_000
        if !(interval > 0) {
            microseconds = 0
        } else if interval >= 0x1p64 {
            microseconds = UInt64.max
        } else {
            microseconds = UInt64(interval)
        }
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
