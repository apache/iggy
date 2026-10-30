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
    Consumer, DecodeError, Identifier, IggyError, IggyMessage, Partitioning, PollOptions,
    PolledMessages, PollingStrategy, SendMessagesResponse,
};
use async_trait::async_trait;

/// This trait defines the methods to interact with the messaging module.
#[async_trait]
pub trait MessageClient {
    /// Read up to `options.count` messages of one partition, from `strategy`.
    ///
    /// Authentication is required, and the permission to poll the messages.
    ///
    /// The server waits up to `options.max_wait` until the partition holds `options.min_count`
    /// messages, and a zero wait answers at once. `options.max_bytes` bounds the reply, and a
    /// first message larger than it comes alone in the reply. The request timeout is `max_wait`
    /// plus 10 s. Invalid options fail before anything is sent, with the errors of
    /// [`PollOptions::validate`], and [`CommitPolicy::Auto`](crate::CommitPolicy::Auto) with
    /// [`IggyError::InvalidConfiguration`]: one call has nothing to count, so it takes
    /// [`CommitPolicy::Manual`](crate::CommitPolicy::Manual) or
    /// [`CommitPolicy::OnRead`](crate::CommitPolicy::OnRead).
    ///
    /// One call has one deadline for admission, route refreshes and retries. If the deadline
    /// passes before the request leaves the client, the call returns
    /// [`IggyError::TransientNotAccepted`], and a retry is safe.
    ///
    /// # Continuations and checkpoints
    ///
    /// To read on, pass [`PolledMessages::continuation`] as the next `strategy`. A continuation
    /// belongs to the partition of its batch, so a poll of another partition refuses it with
    /// [`IggyError::InvalidCommand`]. Serde drops that partition, so a deserialized continuation
    /// fits every partition. It carries the partition history of its batch. After
    /// another client deletes and recreates the partition, a poll from it keeps getting
    /// [`IggyError::HistoryUnavailable`] (87), or `ConsumerGroupPartitionNotOwned` (5009) for a
    /// consumer group. A strategy from [`PollingStrategy::offset`] or the other constructors
    /// has no history, so binary clients use the history of their cached route. Such a poll
    /// can fail once with the same errors, and the next call routes again and takes the new
    /// history.
    ///
    /// To commit, pass [`PolledMessages::checkpoint`] to
    /// [`store_consumer_position`](crate::ConsumerOffsetClient::store_consumer_position).
    /// A [`ConsumerKind::ExternalGroup`](crate::ConsumerKind::ExternalGroup) consumer only holds
    /// offsets, so polling with it returns [`IggyError::FeatureUnavailable`].
    ///
    /// # Server commits and lost replies
    ///
    /// Without [`CommitPolicy::OnRead`](crate::CommitPolicy::OnRead), the server
    /// stores no offset. If a [`PollingStrategy::next`] request left the client and its reply
    /// does not arrive, the call returns [`IggyError::TransientNotCommitted`]. The server stored
    /// nothing, so a retry is safe.
    ///
    /// With `OnRead`, the server stores the offset of the last message of a reply as part of the
    /// reply. A new offset key can be refused with `TooManyConsumerOffsets` at the configured
    /// limit of the partition, and that poll returns no messages. If an `OnRead` request left the
    /// client and its reply does not arrive or reports an unknown outcome, the call returns
    /// [`IggyError::FetchProgressUncertain`]. The partition stays blocked until
    /// [`abandon_operation`](crate::Client::abandon_operation), because the server can still
    /// store the offset after the client gives up. A dropped `OnRead` call that already sent
    /// its request gets the same block until the server answers its cancel, over TCP and
    /// WebSocket, which proves the outcome and lifts the block. The next call on the partition
    /// meanwhile returns [`IggyError::PartitionLaneBlocked`] with the operation to abandon. An
    /// `OnRead` poll is never sent again after an unknown outcome.
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        options: PollOptions,
    ) -> Result<PolledMessages, IggyError>;

    /// [`Self::poll_messages`], keeping the record of a reply that does not decode.
    #[doc(hidden)]
    async fn poll_messages_raw(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        options: PollOptions,
    ) -> Result<PolledMessages, DecodeError> {
        self.poll_messages(
            stream_id,
            topic_id,
            partition_id,
            consumer,
            strategy,
            options,
        )
        .await
        .map_err(DecodeError::without_record)
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
    /// implies uniqueness. Confirmation follows VSR quorum commit. Persisted
    /// message durability also requires recoverable stable-storage copies on
    /// the quorum.
    ///
    /// Binary clients refresh the partition context once after a definitive
    /// `HistoryUnavailable` (87) refusal. A refusal after an uncertain send
    /// keeps its original context and returns `OperationUncertain(op)`.
    /// `Balanced` and `MessagesKey` sends select a new partition once after a
    /// definitive `PartitionNotFound` refusal.
    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError>;
}
