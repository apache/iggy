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

/// How a producer hands messages to the server.
public enum SendMode: Sendable {
    /// Writes from the calling task and returns the confirmations.
    case direct(DirectSendOptions)
    /// Queues the batch and writes it later from a background worker.
    case background(BackgroundSendOptions)

    public static let direct = SendMode.direct(DirectSendOptions())
    public static let background = SendMode.background(BackgroundSendOptions())
}

/// Options for ``SendMode/direct(_:)``.
///
/// A direct send splits a batch into requests of at most `batchLength`
/// messages, awaits them one after another and returns their confirmations.
/// Nothing is buffered between calls.
public struct DirectSendOptions: Sendable, Hashable {
    /// Max messages in one request; a larger send is split. Zero means one
    /// million.
    public var batchLength: UInt32
    /// Minimum gap between sequential sends, waited out before the call.
    public var lingerTime: Duration

    public init(batchLength: UInt32 = 1000, lingerTime: Duration = .zero) {
        self.batchLength = batchLength
        self.lingerTime = lingerTime
    }
}

/// What a background send does when the byte budget is exhausted.
public enum BackpressureMode: Sendable, Hashable {
    /// Waits, without a timeout, until enough capacity is released.
    case block
    /// Waits for the duration, then fails with
    /// ``IggyErrorCode/backgroundSendTimeout``.
    case blockWithTimeout(Duration)
    /// Fails at once with ``IggyErrorCode/backgroundSendBufferOverflow``.
    case failImmediately
}

/// Picks the worker a background batch is queued on.
public enum ShardingStrategy: Sendable {
    /// Every batch for one stream and topic goes to the same worker, so the
    /// order it was dispatched in is kept.
    case ordered
    /// Round-robin over the workers. A single topic can use every worker,
    /// at the price of ordering.
    case balanced
    /// A custom pick; the result must be below the worker count.
    case custom(@Sendable (_ workerCount: Int, _ streamID: Identifier, _ topicID: Identifier, _ messages: [IggyMessage]) -> Int)
}

/// Everything known about a background write that did not return a usable
/// confirmation. Handed to ``BackgroundSendOptions/errorHandler``.
public struct ProducerSendFailure: Sendable {
    /// The error that ended the send. No further automatic retry follows.
    public var cause: IggyError
    public var streamID: Identifier
    public var topicID: Identifier
    /// The per-send override, or nil when the producer's partitioning was used.
    public var partitioning: Partitioning?
    /// The unconfirmed tail of the send. A request can commit before its
    /// reply is lost, so resending is at-least-once.
    public var messages: [IggyMessage]
    /// Confirmations of the chunks written before the failure.
    public var committed: [SendConfirmation]
}

/// Options for ``SendMode/background(_:)``.
///
/// A background producer returns from a send once the batch is queued on one
/// of `workerCount` workers. A worker flushes its buffer when it holds
/// `batchSize` bytes or `batchLength` sends, or once `lingerTime` has passed
/// since the first send entered the buffer. `maxBufferSize` bounds the bytes
/// queued or in flight across the whole producer and `backpressure` decides
/// what a send does when that budget is full. Write failures are reported to
/// `errorHandler`, since no caller waits for them.
///
/// Zero disables the `batchSize` and `batchLength` triggers, flushes at once
/// for `lingerTime`, and lifts the `maxBufferSize` and `maxInFlight` bounds.
public struct BackgroundSendOptions: Sendable {
    public var workerCount: Int
    public var sharding: ShardingStrategy
    public var batchSize: Int
    public var batchLength: Int
    public var lingerTime: Duration
    public var maxBufferSize: Int
    public var backpressure: BackpressureMode
    public var maxInFlight: Int
    /// Runs on its own task, one failure at a time. The default logs and drops
    /// the messages.
    public var errorHandler: (@Sendable (ProducerSendFailure) async -> Void)?

    public init(
        workerCount: Int = 1, sharding: ShardingStrategy = .ordered, batchSize: Int = 1024 * 1024, batchLength: Int = 1000,
        lingerTime: Duration = .milliseconds(1), maxBufferSize: Int = 32 * 1024 * 1024, backpressure: BackpressureMode = .block,
        maxInFlight: Int = 1, errorHandler: (@Sendable (ProducerSendFailure) async -> Void)? = nil
    ) {
        self.workerCount = workerCount
        self.sharding = sharding
        self.batchSize = batchSize
        self.batchLength = batchLength
        self.lingerTime = lingerTime
        self.maxBufferSize = maxBufferSize
        self.backpressure = backpressure
        self.maxInFlight = maxInFlight
        self.errorHandler = errorHandler
    }
}

/// Options for an ``IggyProducer``.
public struct ProducerConfiguration: Sendable {
    /// Partitioning used when a send passes none. Balanced by default.
    public var partitioning: Partitioning?
    /// Creates the stream during `initialize()` when it is missing.
    public var createStreamIfNotExists: Bool
    /// Creates the topic during `initialize()` when it is missing, with the
    /// three topic settings below.
    public var createTopicIfNotExists: Bool
    public var topicPartitionsCount: UInt32
    public var topicMessageExpiry: IggyExpiry
    public var topicMaxSize: MaxTopicSize
    /// Retries after a failed write or while the client is disconnected.
    /// Nil disables retrying.
    public var sendRetries: UInt32?
    /// Wait between retries; nil retries at once.
    public var sendRetryInterval: Duration?
    public var mode: SendMode

    public init(
        partitioning: Partitioning? = nil, createStreamIfNotExists: Bool = true, createTopicIfNotExists: Bool = true,
        topicPartitionsCount: UInt32 = 1, topicMessageExpiry: IggyExpiry = .serverDefault, topicMaxSize: MaxTopicSize = .serverDefault,
        sendRetries: UInt32? = 3, sendRetryInterval: Duration? = .seconds(1), mode: SendMode = .direct
    ) {
        self.partitioning = partitioning
        self.createStreamIfNotExists = createStreamIfNotExists
        self.createTopicIfNotExists = createTopicIfNotExists
        self.topicPartitionsCount = topicPartitionsCount
        self.topicMessageExpiry = topicMessageExpiry
        self.topicMaxSize = topicMaxSize
        self.sendRetries = sendRetries
        self.sendRetryInterval = sendRetryInterval
        self.mode = mode
    }
}

/// A direct send that failed part way through. `committed` holds the
/// confirmations of the requests written before the failure and `failed` the
/// unconfirmed tail. Resending the tail is at-least-once: a request can
/// commit before its reply is lost.
public struct ProducerSendError: Error, Sendable, CustomStringConvertible {
    public var cause: IggyError
    public var failed: [IggyMessage]
    public var committed: [SendConfirmation]
    public var streamID: Identifier
    public var topicID: Identifier

    public var description: String {
        "producer send to \(streamID)|\(topicID) failed after \(committed.count) committed chunks with \(failed.count) unconfirmed messages: \(cause)"
    }
}
