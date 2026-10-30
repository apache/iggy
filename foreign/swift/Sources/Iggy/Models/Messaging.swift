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
    private static let partitionIDSize: UInt8 = 4

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
            writer.write(Self.partitionIDSize)
            writer.write(id)
        case .messagesKey(let key):
            writer.write(Self.messagesKeyCode)
            writer.write(UInt8(key.count))
            writer.write(key)
        }
    }

    /// `[kind:1][length:1][value]`, held to the lengths the server accepts.
    static func decode(from reader: inout ByteReader) throws -> Partitioning {
        let kind = try reader.readUInt8()
        let length = try reader.readUInt8()
        switch kind {
        case balancedCode where length == 0:
            return .balanced
        case partitionCode where length == partitionIDSize:
            return .partition(try reader.readUInt32())
        case messagesKeyCode where length > 0:
            return .messagesKey(Array(try reader.readBytes(Int(length))))
        default:
            throw WireError.validation("partitioning kind \(kind) cannot carry \(length) bytes")
        }
    }

    var encodedSize: Int {
        switch self {
        case .balanced: 2
        case .partition: 2 + Int(Self.partitionIDSize)
        case .messagesKey(let key): 2 + key.count
        }
    }
}

/// Where a poll starts reading a partition.
///
/// A continuation, from ``PolledMessages/continuation``, also carries the
/// partition and the partition history it was read from: a poll of another
/// partition refuses it, and a poll after the partition was recreated fails
/// with ``IggyErrorCode/historyUnavailable`` instead of reading the offset in
/// another history.
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
    /// The partition incarnation and owner a continuation was read under.
    var context: PartitionContext?
    /// The partition a continuation belongs to.
    var partitionID: UInt32?

    public init(kind: Kind, value: UInt64) {
        self.kind = kind
        self.value = value
    }

    init(kind: Kind, value: UInt64, context: PartitionContext?, partitionID: UInt32? = nil) {
        self.kind = kind
        self.value = value
        self.context = context
        self.partitionID = partitionID
    }

    /// Start at a specific offset.
    public static func offset(_ offset: UInt64) -> PollingStrategy {
        PollingStrategy(kind: .offset, value: offset)
    }

    /// Start at the first message at or after a timestamp.
    public static func timestamp(_ timestamp: IggyTimestamp) -> PollingStrategy {
        PollingStrategy(kind: .timestamp, value: timestamp.microseconds)
    }

    /// A start bound to `partitionID` and the history of `context`.
    static func continuation(partitionID: UInt32, kind: Kind, value: UInt64, context: PartitionContext) -> PollingStrategy {
        PollingStrategy(kind: kind, value: value, context: context, partitionID: partitionID)
    }

    /// Start from the beginning of the partition.
    public static let first = PollingStrategy(kind: .first, value: 0)
    /// Start from the end of the partition.
    public static let last = PollingStrategy(kind: .last, value: 0)
    /// Continue after the consumer's stored offset.
    public static let next = PollingStrategy(kind: .next, value: 0)

    /// Refuses a continuation of another partition.
    func validate(partitionID: UInt32) throws {
        if let bound = self.partitionID, bound != partitionID {
            throw IggyError(.invalidCommand, context: "a continuation of partition \(bound) cannot read partition \(partitionID)")
        }
    }

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

/// The limits of a deferred poll, as `DeferredPollOptions` in the Rust SDK.
///
/// The server answers once `minCount` messages are readable, the next
/// message would pass `maxBytes`, or `maxWait` ran out. At the end of the
/// wait it reads what there is, so a reply can hold fewer messages, or none.
/// `requestTimeout` bounds the whole call: a reconnect and a sync of the
/// group assignment on the way, routing, admission, the wait, the reply and
/// every retry. A request that waits must leave one second between its wait
/// and its timeout for the final read and the reply.
///
/// `maxBytes` covers the whole reply body, the poll metadata and the batch
/// framing included. A first message larger than that comes alone in its
/// own reply.
struct DeferredPollOptions: Sendable, Hashable {
    /// `MAX_DEFERRED_POLL_WAIT_US`, the longest wait the protocol carries.
    static let maxWaitMicroseconds: UInt64 = 600_000_000
    /// `MAX_DEFERRED_POLL_TIMEOUT_US`: the longest wait and 30 seconds more.
    static let maxRequestTimeoutMicroseconds: UInt64 = 630_000_000
    /// `MIN_DEFERRED_POLL_REPLY_MARGIN_US`: the server spends it on the final
    /// read and the reply, so an idle wait answers before the client gives up.
    static let replyMarginMicroseconds: UInt64 = 1_000_000
    /// `MIN_DEFERRED_POLL_MAX_BYTES`, the smallest limit that can return a
    /// message: the poll reply header, one batch header and one message header.
    static let minMaxBytes: UInt32 = 344
    static let maxReplyBytes = UInt32(VSRFrame.maxFrameSize - VSRFrame.headerSize)

    var maxWait: Duration
    var minCount: UInt32
    var maxBytes: UInt32
    var requestTimeout: Duration

    /// One message, a 1 MiB reply, a one-second wait and an eleven-second
    /// request timeout by default.
    init(maxWait: Duration = .seconds(1), minCount: UInt32 = 1, maxBytes: UInt32 = 1_048_576, requestTimeout: Duration = .seconds(11)) {
        self.maxWait = maxWait
        self.minCount = minCount
        self.maxBytes = maxBytes
        self.requestTimeout = requestTimeout
    }

    /// Refuses, before anything is sent, what the server refuses, as
    /// `DeferredPollOptions::validate` in the Rust SDK.
    func validate(count: UInt32) throws {
        guard minCount >= 1, minCount <= count else {
            throw IggyError(.invalidMessagesCount, context: "minCount \(minCount) must be between 1 and the count \(count)")
        }
        try validateBudget()
        guard maxBytes >= Self.minMaxBytes else {
            throw IggyError(.invalidSizeBytes, context: "maxBytes \(maxBytes) must be at least \(Self.minMaxBytes)")
        }
    }

    /// A new fetch leaves time for local setup before its final read and
    /// reply, even without a wait. Check at the wire's microsecond precision.
    func validateFetch(count: UInt32) throws {
        try validate(count: count)
        let (wait, timeout) = try budgetMicroseconds()
        guard timeout > Self.replyMarginMicroseconds, timeout >= wait + Self.replyMarginMicroseconds else {
            throw IggyError(
                .invalidCommand,
                context: "a fetch needs requestTimeout \(requestTimeout) above one second and at least one second past maxWait \(maxWait)")
        }
    }

    func validateBudget() throws {
        let (wait, timeout) = try budgetMicroseconds()
        guard wait <= Self.maxWaitMicroseconds, timeout >= 1, timeout <= Self.maxRequestTimeoutMicroseconds,
            wait == 0 || timeout >= wait + Self.replyMarginMicroseconds
        else {
            throw IggyError(.invalidCommand, context: "maxWait \(maxWait) or requestTimeout \(requestTimeout) is out of range")
        }
    }

    /// The options of an attempt `elapsed` into its call, as
    /// `DeferredPollOptions::remaining` in the Rust SDK. The wait and the
    /// timeout shrink together, so a wait keeps its reply margin. A spent
    /// timeout is refused as not accepted: the attempt never left the client.
    func remaining(after elapsed: Duration) throws -> DeferredPollOptions {
        var options = self
        options.requestTimeout = requestTimeout - elapsed
        guard let timeout = options.requestTimeout.wholeMicroseconds, timeout > 0 else {
            throw IggyError(.transientNotAccepted, context: "the request timeout of \(requestTimeout) ran out before the request was sent")
        }
        options.maxWait = max(.zero, maxWait - elapsed)
        return options
    }

    /// The wait and the timeout in whole microseconds, as the wire carries them.
    func budgetMicroseconds() throws -> (wait: UInt64, timeout: UInt64) {
        guard let wait = maxWait.wholeMicroseconds, let timeout = requestTimeout.wholeMicroseconds else {
            throw IggyError(.invalidCommand, context: "maxWait \(maxWait) and requestTimeout \(requestTimeout) must not be negative")
        }
        return (wait, timeout)
    }

    /// The wait and the timeout as an attempt sends them. Whole microseconds
    /// of a shrunk budget can lose a part of the reply margin, so the wait
    /// gives way, as in `deferred_payload` of the Rust SDK.
    func wireBudget() throws -> (wait: UInt64, timeout: UInt64) {
        let (wait, timeout) = try budgetMicroseconds()
        let room = timeout > Self.replyMarginMicroseconds ? timeout - Self.replyMarginMicroseconds : 0
        return (min(wait, room), timeout)
    }

    /// ``wireBudget()`` of a fetch attempt. A timeout shrunk below the reply
    /// margin is one the server refuses, so the attempt is refused as not
    /// accepted: it never left the client.
    func fetchWireBudget() throws -> (wait: UInt64, timeout: UInt64) {
        let (wait, timeout) = try budgetMicroseconds()
        guard timeout >= Self.replyMarginMicroseconds else {
            throw IggyError(.transientNotAccepted, context: "the request timeout left for the fetch, \(requestTimeout), is below the reply margin")
        }
        return (min(wait, timeout - Self.replyMarginMicroseconds), timeout)
    }

    /// `[wait_us:8][min_count:4][max_bytes:4][request_timeout_us:8]`, which
    /// turns a poll into a deferred one. A reply larger than a frame fails the
    /// connection that reads it, so the server must not fill more.
    func encode(into writer: inout ByteWriter) throws {
        let (wait, timeout) = try wireBudget()
        writer.write(wait)
        writer.write(minCount)
        writer.write(min(maxBytes, Self.maxReplyBytes))
        writer.write(timeout)
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
