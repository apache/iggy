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

/// Options of an ``IggyConsumer``, with the Rust SDK's defaults.
public struct ConsumerConfiguration: Sendable {
    /// Where reading a partition without a position starts.
    public var pollingStrategy: PollingStrategy
    /// The limits of each read and the commit policy: a five-second wait,
    /// 1000 messages, 1 MiB and ``CommitPolicy/manual`` by default. With
    /// ``CommitPolicy/auto(_:)``, ``IggyConsumer/next()`` counts a message
    /// under ``CommitAfter/processing`` once the next one is asked for, and
    /// ``IggyConsumer/consumeMessages(_:)`` once its handler returned.
    public var pollOptions: PollOptions
    /// Joins the group during ``IggyConsumer/initialize()`` and again before
    /// a read once the membership was lost. Off, a read without a membership
    /// fails with ``IggyErrorCode/consumerGroupMemberNotFound``.
    public var autoJoinConsumerGroup: Bool
    /// Creates the group when it is missing before joining it.
    public var createConsumerGroupIfNotExists: Bool
    /// The pause after a failed read before its partitions are read again.
    public var pollingRetryInterval: Duration
    /// Lookups of a missing stream or topic during
    /// ``IggyConsumer/initialize()`` before it fails. Nil or zero looks once.
    public var initRetries: UInt32?
    public var initRetryInterval: Duration
    /// Hands over a message again when a read returns it again, as after a
    /// rebalance. Off, a message at or below the last one handed over from
    /// the same partition history is skipped, unless a seek moved back.
    public var allowReplay: Bool
    /// How long ``IggyConsumer/shutdown()`` waits for the reader to store
    /// its offsets and leave the group before it returns.
    public var offsetDrainTimeout: Duration

    public init(
        pollingStrategy: PollingStrategy = .next, pollOptions: PollOptions = .wait(ReaderConfiguration.defaultMaxWait),
        autoJoinConsumerGroup: Bool = true, createConsumerGroupIfNotExists: Bool = true, pollingRetryInterval: Duration = .seconds(1),
        initRetries: UInt32? = nil, initRetryInterval: Duration = .seconds(1), allowReplay: Bool = false, offsetDrainTimeout: Duration = .seconds(5)
    ) {
        self.pollingStrategy = pollingStrategy
        self.pollOptions = pollOptions
        self.autoJoinConsumerGroup = autoJoinConsumerGroup
        self.createConsumerGroupIfNotExists = createConsumerGroupIfNotExists
        self.pollingRetryInterval = pollingRetryInterval
        self.initRetries = initRetries
        self.initRetryInterval = initRetryInterval
        self.allowReplay = allowReplay
        self.offsetDrainTimeout = offsetDrainTimeout
    }

    /// Refuses what the Rust SDK's types cannot hold: a zero or negative
    /// interval and any duration past `UInt64.max` microseconds, which would
    /// also trap in `Task.sleep`, and poll options the server refuses.
    func validate() throws {
        try pollOptions.validate()
        try Self.validatePositive(pollingRetryInterval, named: "pollingRetryInterval")
        try Self.validatePositive(initRetryInterval, named: "initRetryInterval")
        try ProducerConfiguration.validate(offsetDrainTimeout, named: "offsetDrainTimeout")
    }

    func reader(partitions: [UInt32]?) -> ReaderConfiguration {
        ReaderConfiguration(
            partitions: partitions, start: pollingStrategy, options: pollOptions, retryInterval: pollingRetryInterval,
            autoJoinConsumerGroup: autoJoinConsumerGroup, createConsumerGroupIfNotExists: createConsumerGroupIfNotExists)
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
    /// The newest offset the partition had when the batch was read, the same
    /// for every message of the batch. Compared with the message's offset it
    /// shows how far the consumer lags.
    public var currentOffset: UInt64
    /// The partition the message was read from.
    public var partitionID: UInt32
    /// The position after this message, to commit with
    /// ``IggyConsumer/commit(_:)`` or to count with ``IggyConsumer/mark(_:)``.
    public let checkpoint: Checkpoint
}
