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

/// When the offsets of the messages that a read returns are stored.
public enum CommitPolicy: Sendable, Hashable {
    /// Nothing is stored. The application commits checkpoints.
    case manual
    /// The server stores the offset of the last message of each read as part
    /// of the read: one round trip, at most once.
    case onRead
    /// The client stores the latest counted position, at least once with
    /// ``CommitAfter/processing``. A single poll refuses it with
    /// ``IggyErrorCode/invalidConfiguration``, as it has nothing to count. A
    /// ``PartitionReader`` and an ``IggyConsumer`` accept it.
    case auto(AutoCommit)
}

/// When ``CommitPolicy/auto(_:)`` stores. It needs `interval` or `every`. It
/// also stores on close, on shutdown and before a rebalance takes a partition
/// away. A store only moves forward, and a position of an older generation of
/// its partition, from before a seek or a new assignment, is dropped.
public struct AutoCommit: Sendable, Hashable {
    /// Store the latest counted position at this interval, when it moved.
    public var interval: Duration?
    /// Also store it on this event.
    public var every: CommitEvery?
    /// When a message counts.
    public var after: CommitAfter

    public init(interval: Duration? = nil, every: CommitEvery? = nil, after: CommitAfter = .processing) {
        self.interval = interval
        self.every = every
        self.after = after
    }

    /// Every 10,000 messages or 5 seconds, whichever comes first, after
    /// ``CommitAfter/processing``.
    public static let `default` = AutoCommit(interval: .seconds(5), every: .messages(10_000), after: .processing)

    func validate() throws {
        guard interval != nil || every != nil else {
            throw IggyError(.invalidConfiguration, context: "auto commit needs an interval or an event, or it stores only on close")
        }
        if let interval {
            guard interval > .zero, interval.wholeMicroseconds != nil else {
                throw IggyError(.invalidConfiguration, context: "the auto commit interval must be positive, got \(interval)")
            }
        }
        if case .messages(0)? = every {
            throw IggyError(.invalidConfiguration, context: "auto commit every n messages needs n of at least 1")
        }
    }
}

/// The event of ``AutoCommit/every``.
public enum CommitEvery: Sendable, Hashable {
    /// Each counted message, or each counted batch of a ``PartitionReader``.
    case message
    /// This many counted messages of a partition since its last store.
    case messages(UInt32)
    /// The last message of each batch.
    case batch
}

/// When a message counts for ``CommitPolicy/auto(_:)``.
public enum CommitAfter: Sendable, Hashable {
    /// Once the application asks for the next message or batch, returns from
    /// its handler with success, or closes the reader. A handler that throws
    /// leaves its message uncounted.
    case processing
    /// When the message is handed over.
    case delivery
    /// Only when the application marks its checkpoint, so messages processed
    /// concurrently count once they are done.
    case marked
}

/// The limits of one poll of one partition, as `PollOptions` in the Rust SDK.
///
/// The server answers once `minCount` messages are readable, the next message
/// would pass `maxBytes`, or `maxWait` ran out, and then returns what there
/// is, possibly nothing. The request timeout is `maxWait` plus ten seconds.
public struct PollOptions: Sendable, Hashable {
    /// Ten seconds past the wait leave room for routing, the final read and
    /// the reply, as in the Rust SDK.
    static let requestTimeoutMargin: Duration = .seconds(10)

    /// Most messages in one batch. Default 1000.
    public var count: UInt32
    /// Zero answers at once. At most 600 seconds.
    public var maxWait: Duration
    /// At least 1 and at most `count`. Default 1.
    public var minCount: UInt32
    /// The whole reply body, default 1 MiB, at least 344 bytes. A first
    /// message larger than that comes alone in its own reply.
    public var maxBytes: UInt32
    /// Default ``CommitPolicy/manual``.
    public var commit: CommitPolicy

    public init(count: UInt32 = 1000, maxWait: Duration = .zero, minCount: UInt32 = 1, maxBytes: UInt32 = 1_048_576, commit: CommitPolicy = .manual) {
        self.count = count
        self.maxWait = maxWait
        self.minCount = minCount
        self.maxBytes = maxBytes
        self.commit = commit
    }

    /// The defaults with a wait of up to `maxWait` for `minCount` messages.
    public static func wait(_ maxWait: Duration) -> PollOptions {
        PollOptions(maxWait: maxWait)
    }

    /// The limit of the whole request: `maxWait` plus ten seconds.
    public var requestTimeout: Duration {
        maxWait + Self.requestTimeoutMargin
    }

    /// Refuses, before anything is sent, what the server refuses, and
    /// ``CommitPolicy/auto(_:)`` without an interval or an event.
    func validate() throws {
        if case .auto(let auto) = commit {
            try auto.validate()
        }
        try deferred.validate(count: count)
    }

    var commitsOnRead: Bool {
        commit == .onRead
    }

    var autoCommit: AutoCommit? {
        if case .auto(let auto) = commit { auto } else { nil }
    }

    /// The wire limits of these options.
    var deferred: DeferredPollOptions {
        DeferredPollOptions(maxWait: maxWait, minCount: minCount, maxBytes: maxBytes, requestTimeout: requestTimeout)
    }
}

/// The position after one delivered message, to commit. It is bound to its
/// partition and, from a ``PartitionReader``, to the reader and the
/// generation of the partition that delivered it, so a delayed commit cannot
/// land on a recreated or reassigned partition.
public struct Checkpoint: Sendable, Hashable, CustomStringConvertible {
    /// The partition of the delivered message.
    public let partitionID: UInt32
    /// The offset of the delivered message, which a commit stores.
    public let offset: UInt64
    /// The partition incarnation and owner the read ran under.
    let context: PartitionContext
    /// The reader and generation that delivered it, nil for a single poll.
    let stamp: ReaderStamp?

    init(partitionID: UInt32, offset: UInt64, context: PartitionContext, stamp: ReaderStamp? = nil) {
        self.partitionID = partitionID
        self.offset = offset
        self.context = context
        self.stamp = stamp
    }

    public var description: String {
        "\(partitionID)|\(offset)"
    }
}

/// The reader and the generation of a partition that delivered a batch.
struct ReaderStamp: Sendable, Hashable {
    let reader: UInt64
    let generation: UInt64
}

extension PolledMessages {
    /// The position after the last message, to commit the whole batch. Nil
    /// for an empty batch.
    public var checkpoint: Checkpoint? {
        messages.last.map(checkpoint(of:))
    }

    /// The position after `messages[index]`, to commit a processed part of
    /// the batch. Nil for an index out of range.
    public func checkpoint(at index: Int) -> Checkpoint? {
        messages.indices.contains(index) ? checkpoint(of: messages[index]) : nil
    }

    /// Where the next poll of this partition starts: after the last message,
    /// or, for an empty batch, the start of its request, with `first` as
    /// offset zero. It belongs to the partition of the batch, and a poll of
    /// another partition refuses it. It carries the context of the batch, so
    /// a poll after a change of the partition history fails with
    /// ``IggyErrorCode/historyUnavailable``. An empty batch with no context
    /// names no history, so its continuation is the start as it was.
    public var continuation: PollingStrategy {
        if let last = messages.last {
            return .continuation(partitionID: partitionID, kind: .offset, value: last.offset + 1, context: context)
        }
        guard let start else {
            return .next
        }
        guard context != .empty else {
            return start
        }
        if start.kind == .first {
            return .continuation(partitionID: partitionID, kind: .offset, value: 0, context: context)
        }
        return .continuation(partitionID: partitionID, kind: start.kind, value: start.value, context: context)
    }

    private func checkpoint(of message: IggyMessage) -> Checkpoint {
        Checkpoint(partitionID: partitionID, offset: message.offset, context: context, stamp: stamp)
    }
}
