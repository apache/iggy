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

/// Computes the partition of a batch on the client, ahead of any
/// partitioning. It sees the batch as it is sent. A thrown error fails the
/// send before anything is written.
public typealias Partitioner = @Sendable (_ streamID: Identifier, _ topicID: Identifier, _ messages: [IggyMessage]) throws -> UInt32

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
/// messages and awaits them one after another, so a failure in the middle
/// leaves the confirmations of the prefix. Nothing is buffered between calls.
public struct DirectSendOptions: Sendable, Hashable {
    /// Max messages in one request; a larger send is split. Zero means a
    /// million.
    public var batchLength: UInt32
    /// Minimum gap between sequential send calls, waited out before a call.
    /// It does not space out the requests of one call and does not order
    /// concurrent callers.
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
    /// Every batch for one stream and topic goes to the same worker, which
    /// writes its queue in order.
    case ordered
    /// Round-robin over the workers. A single topic can use every worker, at
    /// the price of its order.
    case balanced
    /// A custom pick; the result must be below the worker count.
    case custom(@Sendable (_ workerCount: Int, _ messages: [IggyMessage], _ streamID: Identifier, _ topicID: Identifier) -> Int)
}

/// A background write that ended without a usable confirmation, handed to
/// ``BackgroundSendOptions/errorHandler`` once no automatic retry follows.
public struct ProducerSendFailure: Sendable {
    /// Why the write stopped.
    public var cause: IggyError
    public var streamID: Identifier
    public var topicID: Identifier
    /// The per-send override, or nil when the producer's own partitioning
    /// applied.
    public var partitioning: Partitioning?
    /// The unconfirmed tail. A request can commit before its reply is lost,
    /// so resending it can append the messages twice.
    public var messages: [IggyMessage]
    /// Confirmations of the requests written before the failure.
    public var committed: [SendConfirmation]
}

/// Options for ``SendMode/background(_:)``.
///
/// A background send returns once the batch is queued on one of
/// `workerCount` workers. A worker flushes its buffer when it holds
/// `batchLength` sends or `batchSize` bytes, or `lingerTime` after the first
/// of them was buffered, merging adjacent sends to one destination into one
/// request. `maxBufferSize` bounds the bytes queued or in flight across the
/// producer and `backpressure` decides what a send does when it is used up.
/// `maxInFlight` bounds concurrent writes across workers; each worker stays
/// sequential. Write failures go to `errorHandler`, since no caller waits for
/// them.
///
/// Zero disables the `batchLength` and `batchSize` triggers, flushes at once
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
    /// Runs on its own task, one failure at a time. The default logs the
    /// failure and drops its messages.
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

/// Options of an ``IggyProducer``, with the Rust SDK's defaults.
public struct ProducerConfiguration: Sendable {
    /// Partitioning used when a send passes none. Balanced when nil.
    public var partitioning: Partitioning?
    /// Takes precedence over any partitioning when set.
    public var partitioner: Partitioner?
    /// Creates the stream during ``IggyProducer/initialize()`` when it is
    /// missing and named.
    public var createStreamIfNotExists: Bool
    /// Creates the topic during ``IggyProducer/initialize()`` when it is
    /// missing and named, with the four topic settings below.
    public var createTopicIfNotExists: Bool
    public var topicPartitionsCount: UInt32
    public var topicMessageExpiry: IggyExpiry
    public var topicMaxSize: MaxTopicSize
    public var topicDurability: Durability
    /// Retries of a request the server refused to admit or that found no
    /// connection, and checks while the client is signed out. Nil or zero
    /// sends once.
    public var sendRetries: UInt32?
    /// The first retry is immediate; later ones are at most this far apart.
    /// Nil retries back to back.
    public var sendRetryInterval: Duration?
    public var mode: SendMode

    public init(
        partitioning: Partitioning? = nil, partitioner: Partitioner? = nil, createStreamIfNotExists: Bool = true, createTopicIfNotExists: Bool = true,
        topicPartitionsCount: UInt32 = 1, topicMessageExpiry: IggyExpiry = .serverDefault, topicMaxSize: MaxTopicSize = .serverDefault,
        topicDurability: Durability = .replicated, sendRetries: UInt32? = 3, sendRetryInterval: Duration? = .seconds(1), mode: SendMode = .direct
    ) {
        self.partitioning = partitioning
        self.partitioner = partitioner
        self.createStreamIfNotExists = createStreamIfNotExists
        self.createTopicIfNotExists = createTopicIfNotExists
        self.topicPartitionsCount = topicPartitionsCount
        self.topicMessageExpiry = topicMessageExpiry
        self.topicMaxSize = topicMaxSize
        self.topicDurability = topicDurability
        self.sendRetries = sendRetries
        self.sendRetryInterval = sendRetryInterval
        self.mode = mode
    }
}

/// A direct send that failed. `committed` holds the confirmations of the
/// requests written before the failure and `failed` the unconfirmed tail,
/// with the ids the producer minted for it. Check `cause` before resending
/// the tail: a request can commit before its reply is lost, so a resend can
/// append it twice.
public struct ProducerSendError: Error, Sendable, CustomStringConvertible {
    public var cause: IggyError
    public var failed: [IggyMessage]
    public var committed: [SendConfirmation]
    public var streamID: Identifier
    public var topicID: Identifier

    public var description: String {
        "producer send to stream \(streamID), topic \(topicID) failed with \(failed.count) unconfirmed messages after \(committed.count) confirmed requests: \(cause)"
    }
}
