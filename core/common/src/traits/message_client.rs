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
    BatchProducer, Consumer, DecodeError, Identifier, IggyError, IggyMessage, Partitioning,
    PollOptions, PolledMessages, PollingStrategy, SendMessagesResponse,
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
    /// With `OnRead`, the server stores the offset of the last message of a read as part of the
    /// read, in one round trip. While the primary stays up a message is delivered at most once.
    /// A failover before the store replicates can deliver the last batch again. A new offset key
    /// can be refused with `TooManyConsumerOffsets` at the configured limit of the partition,
    /// and that poll returns no messages.
    ///
    /// An `OnRead` call whose outcome is unknown fails with
    /// [`IggyError::FetchProgressUncertain`] naming its operation, never with
    /// `TransientNotCommitted`: its request left the client and no reply proves what the server
    /// stored, also when the client disconnects while the call waits. Every later poll of that
    /// partition with `OnRead` or from `Next`, and every offset store or delete of it, on the
    /// same client then fails with [`IggyError::PartitionLaneBlocked`] naming that operation,
    /// until [`abandon_operation`](crate::Client::abandon_operation) or a reply proves the
    /// outcome, because the server can still store the offset after the client gives up. The
    /// block survives a reconnect and a new session of the same client, and it names the
    /// partition by its incarnation, so another spelling of its stream or topic meets it too. An
    /// `OnRead` call dropped after its request left keeps its reply slot and sends a cancel over
    /// TCP, QUIC and WebSocket, and its block lasts until that one reply proves the outcome. An
    /// `OnRead` poll is never sent again after an unknown outcome.
    ///
    /// HTTP has no cancel and no connection to close, so a dropped call keeps its block until
    /// `abandon_operation`, and `disconnect` leaves a call in flight to its reply. An HTTP block
    /// covers the stream and the topic as the lost call spelled them, and any spelling for a
    /// call that carries a context of the same partition incarnation.
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

    /// [`Self::send_messages`] for one batch of an idempotent producer, to
    /// the partition `partition_id`. The producer counts its sequence per
    /// partition, so the caller names the partition.
    ///
    /// The partition keeps the last batches of each producer. A batch it
    /// already holds is not written again: the reply names the offset of its
    /// first write. Only binary clients send a producer, and HTTP returns
    /// [`IggyError::FeatureUnavailable`].
    ///
    /// A client with an encryptor encrypts `messages` in place. To resend a
    /// batch, pass its plaintext messages again.
    async fn send_producer_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        producer: BatchProducer,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        let _ = (stream_id, topic_id, partition_id, producer, messages);
        Err(IggyError::FeatureUnavailable)
    }

    /// A producer id that no other client of the cluster gets. It is built
    /// from this client's session number, the op of its `Register`, so no
    /// request goes to the server. The ids of a session stay unique after the
    /// session ends, so a producer can store its id and use it again.
    ///
    /// Fails with [`IggyError::ProducerIdsExhausted`] after 2^20 ids from one
    /// session, and with [`IggyError::Unauthenticated`] before a sign-in.
    /// HTTP returns [`IggyError::FeatureUnavailable`].
    async fn next_producer_id(&self) -> Result<u64, IggyError> {
        Err(IggyError::FeatureUnavailable)
    }
}
