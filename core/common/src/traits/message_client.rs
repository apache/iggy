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

use crate::{
    Consumer, ConsumerKind, Identifier, IggyError, IggyMessage, Partitioning, PolledMessages,
    PollingStrategy, SendMessagesResponse,
};
use async_trait::async_trait;

/// This trait defines the methods to interact with the messaging module.
#[async_trait]
pub trait MessageClient {
    /// Poll given amount of messages using the specified consumer and strategy from the specified stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    ///
    /// Polling a consumer group the client is not (or no longer) a member of fails with `ConsumerGroupMemberNotFound` rather than returning an empty batch, so the caller can rejoin.
    /// A member that holds no partitions gets an empty batch whose `partition_id` is [`NO_ASSIGNED_PARTITION`](crate::NO_ASSIGNED_PARTITION).
    #[allow(clippy::too_many_arguments)]
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError>;

    /// [`poll_messages`](Self::poll_messages) whose strategy is chosen once the partition is
    /// known. A consumer-group poll without a partition picks one of the member's assigned
    /// partitions first and then asks `strategy_for` for it, so a caller can continue every
    /// partition from its own position. Any other poll has its partition up front and asks
    /// `strategy_for` for that one, or for `0` when none was given, which is the partition the
    /// server reads then.
    ///
    /// The default implementation is for transports that cannot pick a partition client-side:
    /// a consumer-group poll without a partition fails with `FeatureUnavailable`.
    #[allow(clippy::too_many_arguments)]
    async fn poll_messages_with_strategy_for(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy_for: &(dyn Fn(u32) -> PollingStrategy + Send + Sync),
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError> {
        if consumer.kind == ConsumerKind::ConsumerGroup && partition_id.is_none() {
            return Err(IggyError::FeatureUnavailable);
        }
        let strategy = strategy_for(partition_id.unwrap_or(0));
        self.poll_messages(
            stream_id,
            topic_id,
            partition_id,
            consumer,
            &strategy,
            count,
            auto_commit,
        )
        .await
    }

    /// Send messages using specified partitioning strategy to the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to send the messages.
    ///
    /// Returns the per-partition commit confirmations, which may be empty: the
    /// legacy server reports none, and a server that does report them can still
    /// commit a batch it has no offsets to describe. Callers must handle an
    /// empty list rather than assume a confirmation per send.
    ///
    /// A reported `base_offset` is where the batch's first message landed, with
    /// two limits. Delivery is at-least-once, so an earlier retry may already
    /// have committed the same batch at a lower offset and the value never
    /// implies uniqueness. A batch is confirmed once it is committed in memory,
    /// not once it is fsynced, so a crash-restart can stamp a later batch with
    /// an offset a client has already recorded.
    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError>;

    /// Force flush of the `unsaved_messages` buffer to disk, optionally fsyncing the data.
    #[allow(clippy::too_many_arguments)]
    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError>;
}
