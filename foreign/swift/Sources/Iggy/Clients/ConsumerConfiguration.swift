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

/// When a consumer stores its offset on the server while handing messages over.
public enum AutoCommitWhen: Sendable, Hashable {
    /// The poll request itself commits the whole batch, before any message
    /// of it is handed over.
    case pollingMessages
    /// A commit is queued once the last message of a batch is handed over.
    case consumingAllMessages
    /// A commit is queued for every message handed over.
    case consumingEachMessage
    /// A commit is queued for every message whose offset is a multiple of `n`.
    case consumingEveryNthMessage(UInt32)
}

/// When ``IggyConsumer/consume(_:)`` stores the offset after the handler
/// returned for a message.
public enum AutoCommitAfter: Sendable, Hashable {
    case consumingAllMessages
    case consumingEachMessage
    case consumingEveryNthMessage(UInt32)
}

/// How a consumer stores the offset of what it has read.
public enum AutoCommit: Sendable, Hashable {
    /// Nothing is stored unless ``IggyConsumer/storeOffset(_:partitionID:)`` is called.
    case disabled
    /// Every interval the reading position of every partition read so far is stored.
    case interval(Duration)
    /// Both the interval and the trigger.
    case intervalOrWhen(Duration, AutoCommitWhen)
    /// Both the interval and the trigger; the trigger only fires under ``IggyConsumer/consume(_:)``.
    case intervalOrAfter(Duration, AutoCommitAfter)
    case when(AutoCommitWhen)
    /// Only fires under ``IggyConsumer/consume(_:)``.
    case after(AutoCommitAfter)

    var interval: Duration? {
        switch self {
        case .interval(let interval), .intervalOrWhen(let interval, _), .intervalOrAfter(let interval, _): interval
        case .disabled, .when, .after: nil
        }
    }

    var when: AutoCommitWhen? {
        switch self {
        case .when(let when), .intervalOrWhen(_, let when): when
        case .disabled, .interval, .intervalOrAfter, .after: nil
        }
    }

    var after: AutoCommitAfter? {
        switch self {
        case .after(let after), .intervalOrAfter(_, let after): after
        case .disabled, .interval, .intervalOrWhen, .when: nil
        }
    }
}

/// Options for an ``IggyConsumer``.
public struct ConsumerConfiguration: Sendable {
    /// Where reading a partition starts. From then on every poll continues
    /// after the last message handed over from that partition, except under
    /// `next`, which leaves the continuation to the offset stored on the server.
    public var pollingStrategy: PollingStrategy
    /// Max messages one poll fetches.
    public var batchLength: UInt32
    /// Minimum gap between polls; nil polls again as soon as the buffer runs
    /// empty, which spins against an empty topic.
    public var pollInterval: Duration?
    public var autoCommit: AutoCommit
    /// Joins the group during `initialize()` and again after the membership
    /// was lost, for example after a reconnect.
    public var autoJoinConsumerGroup: Bool
    public var createConsumerGroupIfNotExists: Bool
    /// Wait before the next attempt while a poll is blocked: after a
    /// disconnect, a failed join, or while the member holds no partitions.
    public var pollingRetryInterval: Duration
    /// Retries while the stream or topic is missing during `initialize()`,
    /// for a topic the producer creates dynamically. Nil does not retry.
    public var initRetries: UInt32?
    public var initRetryInterval: Duration
    /// Hands over messages at or below an offset already handed over, and
    /// stores offsets that do not advance.
    public var allowReplay: Bool
    /// How long `shutdown()` waits for the background commit tasks to drain.
    public var offsetDrainTimeout: Duration

    public init(
        pollingStrategy: PollingStrategy = .next, batchLength: UInt32 = 1000, pollInterval: Duration? = nil,
        autoCommit: AutoCommit = .intervalOrWhen(.seconds(1), .pollingMessages), autoJoinConsumerGroup: Bool = true,
        createConsumerGroupIfNotExists: Bool = true, pollingRetryInterval: Duration = .seconds(1), initRetries: UInt32? = nil,
        initRetryInterval: Duration = .seconds(1), allowReplay: Bool = false, offsetDrainTimeout: Duration = .seconds(5)
    ) {
        self.pollingStrategy = pollingStrategy
        self.batchLength = batchLength
        self.pollInterval = pollInterval
        self.autoCommit = autoCommit
        self.autoJoinConsumerGroup = autoJoinConsumerGroup
        self.createConsumerGroupIfNotExists = createConsumerGroupIfNotExists
        self.pollingRetryInterval = pollingRetryInterval
        self.initRetries = initRetries
        self.initRetryInterval = initRetryInterval
        self.allowReplay = allowReplay
        self.offsetDrainTimeout = offsetDrainTimeout
    }
}

/// A message handed over by an ``IggyConsumer``.
public struct ReceivedMessage: Sendable, Hashable {
    public var message: IggyMessage
    /// The newest offset the partition had when the batch was polled, the
    /// same for every message of the batch.
    public var currentOffset: UInt64
    /// The partition the message was read from; varies for a group member.
    public var partitionID: UInt32

    public init(message: IggyMessage, currentOffset: UInt64, partitionID: UInt32) {
        self.message = message
        self.currentOffset = currentOffset
        self.partitionID = partitionID
    }
}
