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

/// When a consumer stores its offsets on the server.
public enum AutoCommit: Sendable, Hashable {
    /// Never, not even on ``IggyConsumer/shutdown()``; store with
    /// ``IggyConsumer/storePosition(_:)``.
    case disabled
    /// On every tick, the reading position of every partition read so far.
    case interval(Duration)
    /// On every tick, and as `when` says while messages are handed over.
    case intervalOrWhen(Duration, AutoCommitWhen)
    /// On every tick, and as `after` says once a handler of
    /// ``IggyConsumer/consumeMessages(_:)`` returned.
    case intervalOrAfter(Duration, AutoCommitAfter)
    /// As `when` says while messages are handed over.
    case when(AutoCommitWhen)
    /// As `after` says once a handler of ``IggyConsumer/consumeMessages(_:)``
    /// returned. Reading with ``IggyConsumer/next()`` never triggers it.
    case after(AutoCommitAfter)

    var interval: Duration? {
        switch self {
        case .interval(let interval), .intervalOrWhen(let interval, _), .intervalOrAfter(let interval, _): interval
        case .disabled, .when, .after: nil
        }
    }

    var whenTrigger: AutoCommitWhen? {
        switch self {
        case .intervalOrWhen(_, let when), .when(let when): when
        case .disabled, .interval, .intervalOrAfter, .after: nil
        }
    }

    var afterTrigger: AutoCommitAfter? {
        switch self {
        case .intervalOrAfter(_, let after), .after(let after): after
        case .disabled, .interval, .intervalOrWhen, .when: nil
        }
    }
}

/// A commit triggered while messages are handed over, before the code that
/// reads them has seen them.
public enum AutoCommitWhen: Sendable, Hashable {
    /// With the poll request itself: the server stores the offset of the
    /// last message it returns, before any of the batch is handed over.
    case pollingMessages
    /// Queued when the buffer of the current batch runs empty.
    case consumingAllMessages
    /// Queued just before each message is handed over. Commits queued faster
    /// than they are sent collapse into the latest one per partition.
    case consumingEachMessage
    /// Queued just before a message whose offset divides by `n` is handed
    /// over. It tests the offset, not a count of messages. `n` must not be
    /// zero.
    case consumingEveryNthMessage(UInt32)
}

/// A commit triggered once a handler of ``IggyConsumer/consumeMessages(_:)``
/// returned, whether it succeeded or threw.
public enum AutoCommitAfter: Sendable, Hashable {
    /// After the message whose offset is the partition's newest at poll
    /// time.
    case consumingAllMessages
    /// After each message.
    case consumingEachMessage
    /// After a message whose offset divides by `n`. `n` must not be zero.
    case consumingEveryNthMessage(UInt32)
}

/// Options of an ``IggyConsumer``, with the Rust SDK's defaults.
public struct ConsumerConfiguration: Sendable {
    /// Where reading a partition starts. After the first batch each read
    /// continues after the last batch read from that partition, except a
    /// ``PollingStrategy/next`` poll of a standalone consumer that commits
    /// with ``AutoCommitWhen/pollingMessages``: it starts after the offset
    /// the previous poll stored.
    public var pollingStrategy: PollingStrategy
    /// Messages asked for per poll, and per fetch of a group member, whose
    /// partitions share them.
    public var batchLength: UInt32
    /// The limits of each poll and fetch, which are deferred: the server
    /// answers once `minCount` messages are readable, the reply would pass
    /// `maxBytes`, or `maxWait` ran out. One message, a 1 MiB reply, a
    /// one-second wait and an eleven-second request timeout by default, as
    /// in the Rust SDK. A message larger than `maxBytes` is polled alone,
    /// without that limit. A fetch also needs a request timeout above one
    /// second and at least one second past `maxWait`, in whole microseconds.
    public var pollOptions: DeferredPollOptions
    public var autoCommit: AutoCommit
    /// Joins the group during ``IggyConsumer/initialize()`` and again before
    /// a read once the membership was lost. Off, a read without a membership
    /// fails with ``IggyErrorCode/consumerGroupMemberNotFound``.
    public var autoJoinConsumerGroup: Bool
    /// Creates the group when it is missing before joining it.
    public var createConsumerGroupIfNotExists: Bool
    /// The pause after a failed read or join, before a member that lost its
    /// membership or holds no partitions reads again, and between checks
    /// while the client is signed out. Other reads wait on the server
    /// instead.
    public var pollingRetryInterval: Duration
    /// Lookups of a missing stream or topic during
    /// ``IggyConsumer/initialize()`` before it fails. Nil or zero looks once.
    public var initRetries: UInt32?
    public var initRetryInterval: Duration
    /// Hands over a message again when a poll returns it again, as after a
    /// rebalance or a store that moved back. Off, a message at or below the
    /// last one handed over from the same partition history is skipped.
    public var allowReplay: Bool
    /// How long ``IggyConsumer/shutdown()`` waits for each background commit
    /// task before it stops it.
    public var offsetDrainTimeout: Duration

    public init(
        pollingStrategy: PollingStrategy = .next, batchLength: UInt32 = 1000, pollOptions: DeferredPollOptions = DeferredPollOptions(),
        autoCommit: AutoCommit = .intervalOrWhen(.seconds(1), .pollingMessages), autoJoinConsumerGroup: Bool = true,
        createConsumerGroupIfNotExists: Bool = true, pollingRetryInterval: Duration = .seconds(1), initRetries: UInt32? = nil,
        initRetryInterval: Duration = .seconds(1), allowReplay: Bool = false, offsetDrainTimeout: Duration = .seconds(5)
    ) {
        self.pollingStrategy = pollingStrategy
        self.batchLength = batchLength
        self.pollOptions = pollOptions
        self.autoCommit = autoCommit
        self.autoJoinConsumerGroup = autoJoinConsumerGroup
        self.createConsumerGroupIfNotExists = createConsumerGroupIfNotExists
        self.pollingRetryInterval = pollingRetryInterval
        self.initRetries = initRetries
        self.initRetryInterval = initRetryInterval
        self.allowReplay = allowReplay
        self.offsetDrainTimeout = offsetDrainTimeout
    }

    /// Refuses what the Rust SDK's types cannot hold: a zero batch, a zero
    /// or negative retry or commit interval (`NonZeroIggyDuration`), a zero
    /// `n`, and any duration past `UInt64.max` microseconds, which would
    /// also trap in `Task.sleep`. Poll options that do not fit `batchLength`
    /// fail with the error of their validation, as in the Rust SDK.
    func validate() throws {
        if batchLength == 0 {
            throw IggyError(.invalidConfiguration, context: "batchLength must be at least 1")
        }
        try pollOptions.validate(count: batchLength)
        try Self.validatePositive(pollingRetryInterval, named: "pollingRetryInterval")
        try Self.validatePositive(initRetryInterval, named: "initRetryInterval")
        if let interval = autoCommit.interval {
            try Self.validatePositive(interval, named: "the auto-commit interval")
        }
        try ProducerConfiguration.validate(offsetDrainTimeout, named: "offsetDrainTimeout")
        if case .consumingEveryNthMessage(0)? = autoCommit.whenTrigger {
            throw IggyError(.invalidConfiguration, context: "consumingEveryNthMessage needs n of at least 1")
        }
        if case .consumingEveryNthMessage(0)? = autoCommit.afterTrigger {
            throw IggyError(.invalidConfiguration, context: "consumingEveryNthMessage needs n of at least 1")
        }
    }

    private static func validatePositive(_ duration: Duration, named name: String) throws {
        guard duration > .zero else {
            throw IggyError(.invalidConfiguration, context: "\(name) must be positive, got \(duration)")
        }
        try ProducerConfiguration.validate(duration, named: name)
    }
}

/// One message handed over by an ``IggyConsumer``.
public struct ReceivedMessage: Sendable {
    public var message: IggyMessage
    /// The newest offset the partition had when the batch was polled, the
    /// same for every message of the batch. Compared with the message's
    /// offset it shows how far the consumer lags.
    public var currentOffset: UInt64
    /// The partition the message was read from. A group member reads
    /// several.
    public var partitionID: UInt32
    /// The partition incarnation and owner the poll ran under.
    public var context: PartitionContext

    public init(message: IggyMessage, currentOffset: UInt64, partitionID: UInt32, context: PartitionContext) {
        self.message = message
        self.currentOffset = currentOffset
        self.partitionID = partitionID
        self.context = context
    }

    /// The checkpoint to store for this message with
    /// ``IggyConsumer/storePosition(_:)``. It keeps the context that
    /// delivered the message, so a store after a recreation or a reassignment
    /// is refused instead of landing on the newer partition.
    public var position: ConsumerPosition {
        ConsumerPosition(partitionID: partitionID, offset: message.offset, context: context)
    }
}
