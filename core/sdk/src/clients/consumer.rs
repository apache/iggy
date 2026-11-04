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

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::reader::membership::Membership;
use crate::clients::reader::progress::{Progress, same_incarnation_and_owner};
use crate::clients::reader::{Hold, PartitionReader, ReadError};
use crate::clients::reader_builder::ReaderBuilder;
use futures::Stream;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    Checkpoint, CommitAfter, CommitPolicy, Consumer, ConsumerKind, EncryptorKind, IdKind,
    Identifier, IggyDuration, IggyError, IggyMessage, NonZeroIggyDuration, PollOptions,
    PolledMessages, PollingKind, PollingStrategy, ReaderStamp,
};
use iggy_common::{StreamClient, TopicClient};
use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::task::{Context, Poll};
use std::vec;
use tokio::time;
use tracing::{error, info, warn};

const ORDERING: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;

/// A cheap, cloneable view of the state shared with an [`IggyConsumer`].
///
/// Consuming borrows the consumer as `&mut` for the whole run, so reading its getters or
/// committing an offset concurrently means sharing it behind a lock and then waiting on
/// that lock. This view carries the same shared state and needs neither.
///
/// Every getter is an independent load rather than part of one snapshot, so the partition
/// ID can already have moved on by the time an offset is read for it.
#[derive(Clone)]
pub struct IggyConsumerState {
    /// The record of the reader of the consumer, so all commits of a consumer share one path.
    progress: Arc<Progress>,
    consumer: Arc<Consumer>,
    stream_id: Arc<Identifier>,
    topic_id: Arc<Identifier>,
    is_consumer_group: bool,
    commit: CommitPolicy,
    current_partition_id: Arc<AtomicU32>,
}

impl Debug for IggyConsumerState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IggyConsumerState")
            .field("consumer", &self.consumer)
            .field("stream_id", &self.stream_id)
            .field("topic_id", &self.topic_id)
            .field("is_consumer_group", &self.is_consumer_group)
            .field("commit", &self.commit)
            .field("current_partition_id", &self.partition_id())
            .finish_non_exhaustive()
    }
}

impl IggyConsumerState {
    fn new(
        client: IggyRwLock<ClientWrapper>,
        consumer: Arc<Consumer>,
        stream_id: Arc<Identifier>,
        topic_id: Arc<Identifier>,
        commit: CommitPolicy,
    ) -> Self {
        let progress = Arc::new(Progress::new(
            client,
            Arc::clone(&consumer),
            Arc::clone(&stream_id),
            Arc::clone(&topic_id),
            commit,
        ));
        Self {
            progress,
            is_consumer_group: consumer.kind == ConsumerKind::ConsumerGroup,
            consumer,
            stream_id,
            topic_id,
            commit,
            current_partition_id: Arc::new(AtomicU32::new(0)),
        }
    }

    /// The commit policy of the [`PollOptions`] of the consumer.
    pub fn commit_policy(&self) -> CommitPolicy {
        self.commit
    }

    /// Returns the current partition ID of the consumer.
    pub fn partition_id(&self) -> u32 {
        self.current_partition_id.load(ORDERING)
    }

    /// Returns the last offset handed over for this partition, or `None` until
    /// a message from the partition has been consumed.
    /// To get the current partition ID use `partition_id()`
    pub fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        self.progress
            .last_delivered(partition_id)
            .map(|checkpoint| checkpoint.offset())
    }

    /// Returns the last offset successfully stored for this partition, or
    /// `None` until this consumer has stored one.
    /// To get the current partition ID use `partition_id()`
    pub fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        self.progress
            .last_committed(partition_id)
            .map(|checkpoint| checkpoint.offset())
    }

    /// Stores an offset either for the current partition or the provided partition ID, see
    /// [`IggyConsumer::store_offset`].
    pub async fn store_offset(
        &self,
        offset: u64,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        let partition_id = partition_id.unwrap_or_else(|| self.partition_id());
        let stored = match self.progress.last_delivered(partition_id) {
            Some(last) => {
                let stored = Checkpoint::new(partition_id, offset, last.context());
                self.progress.store(stored).await?;
                stored
            }
            // Nothing was consumed from this partition, so there is no captured context to keep.
            None => {
                self.progress.store_offset(partition_id, offset).await?;
                Checkpoint::new(partition_id, offset, PartitionContext::default())
            }
        };
        // The batches that the reader holds and its close would undo the store.
        if self.commit == CommitPolicy::OnRead {
            self.progress.read_after(stored);
        }
        Ok(())
    }

    /// Deletes the stored offset either for the current partition or the provided partition ID,
    /// see [`IggyConsumer::delete_offset`].
    pub async fn delete_offset(&self, mut partition_id: Option<u32>) -> Result<(), IggyError> {
        // `None` is only resolved server-side for consumer groups. For a standalone consumer
        // explicitly assign the current partition_id.
        if partition_id.is_none() && !self.is_consumer_group {
            partition_id = Some(self.partition_id());
        }
        self.progress.delete_offset(partition_id).await
    }

    /// Stores the offset of a checkpoint that the consumer yielded, see [`IggyConsumer::commit`].
    pub async fn commit(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        if self.commit == CommitPolicy::OnRead {
            return Err(IggyError::InvalidCommand);
        }
        self.progress.commit(checkpoint).await
    }

    /// Counts a checkpoint that the consumer yielded, see [`IggyConsumer::mark`].
    pub fn mark(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        self.progress.mark(checkpoint)
    }
}

/// Reads a topic as a stream of messages through one [`PartitionReader`], with its commit
/// policy, group create, init retries and a replay filter.
///
/// Call [`Self::init`] before reading and [`Self::shutdown`] before dropping the consumer.
/// A standalone consumer reads the partitions of
/// [`partitions`](crate::prelude::IggyConsumerBuilder::partitions). A group member reads its
/// assignment: the reader joins the group, joins again after a session loss, and leaves the group
/// in [`shutdown`](Self::shutdown). [`PartitionReader`] describes the routing, the prefetch
/// budget, the errors and the guarantees. The batch that the consumer yields from stays in the
/// budget until the consumer yields its last message.
///
/// # Positions and commits
///
/// [`polling_strategy`](crate::prelude::IggyConsumerBuilder::polling_strategy) is the start of
/// a partition without a position. `Next` starts after the stored offset, or at zero when no
/// offset exists. A partition that a rebalance leaves with its group member continues after the
/// last message that the consumer yielded. Messages already yielded are filtered unless replay is
/// enabled, or a [`seek`](Self::seek) of their partition asked for them again. Delivery and
/// durable processing progress are separate.
///
/// The [`CommitPolicy`] of [`poll_options`](crate::prelude::IggyConsumerBuilder::poll_options)
/// decides who stores offsets:
///
/// - `Manual`, the default, stores nothing. Commit [`ReceivedMessage::checkpoint`] with
///   [`commit`](Self::commit) after processing, in partition order.
/// - `OnRead` has the server store the offset of each batch that it reads, so the reader reads
///   only when the application asks for a message and nothing is queued, and then fills its
///   prefetch budget: one fetch per primary that may wait for messages, and the partitions with
///   messages left behind spread over the other slots.
///   [`shutdown`](Self::shutdown) moves the offset back to the last message that the consumer
///   yielded.
/// - `Auto` stores the latest counted message of each partition at the interval or the event of
///   the policy, before a rebalance takes a partition away, and at shutdown. It never stores
///   the end of a prefetched batch. With [`CommitAfter::Processing`], a message that the stream
///   yields counts when the stream is polled again or shuts down, and one that
///   [`consume_messages`](crate::prelude::IggyConsumerMessageExt::consume_messages) hands to its
///   handler counts once the handler succeeds. With [`CommitAfter::Delivery`] it counts when it
///   is handed over, and with [`CommitAfter::Marked`] only [`mark`](Self::mark) counts.
///
/// ```rust,no_run
/// use futures_util::StreamExt;
/// use iggy::prelude::*;
///
/// # async fn handle(message: &IggyMessage) -> Result<(), IggyError> { Ok(()) }
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
/// let mut consumer = client
///     .consumer("reader", "stream", "topic")?
///     .partitions(0..8)
///     .build();
/// consumer.init().await?;
/// while let Some(received) = consumer.next().await {
///     let received = match received {
///         Ok(received) => received,
///         Err(error) => {
///             eprintln!("Failed to read a message: {error}");
///             continue;
///         }
///     };
///     // Leaving a failed message uncommitted is what brings it back on the next run.
///     if handle(&received.message).await.is_err() {
///         break;
///     }
///     consumer.commit(received.checkpoint()).await?;
/// }
/// consumer.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Encryption
///
/// A message that does not decrypt comes out as a [`ReadError`] with the continuation after it in
/// [`skip`](ReadError::skip). The reader reads the message again until [`seek`](Self::seek)
/// moves past it. `OnRead` with an encryptor is refused, because the server would store the
/// offset of a batch before it decrypts.
pub struct IggyConsumer {
    client: IggyRwLock<ClientWrapper>,
    consumer_name: String,
    consumer: Arc<Consumer>,
    stream_id: Arc<Identifier>,
    topic_id: Arc<Identifier>,
    partitions: Option<Vec<u32>>,
    polling_strategy: PollingStrategy,
    poll_options: PollOptions,
    prefetch_bytes: u32,
    prefetch_messages: Option<u32>,
    /// The membership of a group consumer. It outlives a failed `init()`, so the next attempt
    /// resolves a create or a join of an unknown outcome instead of finding its lane blocked.
    membership: Option<Arc<Membership>>,
    encryptor: Option<Arc<EncryptorKind>>,
    polling_retry_interval: NonZeroIggyDuration,
    init_retries: Option<u32>,
    init_retry_interval: NonZeroIggyDuration,
    allow_replay: bool,
    offset_drain_timeout: IggyDuration,
    state: IggyConsumerState,
    reader: Option<PartitionReader>,
    current: Option<CurrentBatch>,
    initialized: bool,
    uninitialized_poll_warned: bool,
    shutdown: AtomicBool,
    /// Under `CommitAfter::Processing`: the message that the stream yielded last, with whether
    /// it ends its batch. It counts when the stream is polled again or shuts down.
    uncounted: Option<(Checkpoint, bool)>,
}

/// The rest of the batch that the consumer yields one message at a time.
struct CurrentBatch {
    messages: vec::IntoIter<IggyMessage>,
    partition_id: u32,
    context: PartitionContext,
    stamp: Option<ReaderStamp>,
    current_offset: u64,
    hold: Hold,
    /// The replay filter skips the messages up to this offset, which the application already
    /// took in the history of the batch. Offsets rise within a batch, so one read before its
    /// first message covers the whole batch.
    delivered_offset: Option<u64>,
}

impl CurrentBatch {
    /// `delivered` is the last message that the application took from the partition, `None`
    /// to hand over every message again.
    fn new(batch: PolledMessages, hold: Hold, delivered: Option<Checkpoint>) -> Self {
        let context = batch.context();
        Self {
            partition_id: batch.partition_id,
            context,
            stamp: batch.stamp(),
            current_offset: batch.current_offset,
            messages: batch.messages.into_iter(),
            hold,
            delivered_offset: delivered
                .filter(|delivered| same_incarnation_and_owner(delivered.context(), context))
                .map(|delivered| delivered.offset()),
        }
    }
}

impl IggyConsumer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
        consumer_name: String,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partitions: Option<Vec<u32>>,
        polling_strategy: PollingStrategy,
        poll_options: PollOptions,
        prefetch_bytes: u32,
        prefetch_messages: Option<u32>,
        create_consumer_group_if_not_exists: bool,
        encryptor: Option<Arc<EncryptorKind>>,
        polling_retry_interval: NonZeroIggyDuration,
        init_retries: Option<u32>,
        init_retry_interval: NonZeroIggyDuration,
        allow_replay: bool,
        offset_drain_timeout: IggyDuration,
    ) -> Self {
        let consumer = Arc::new(consumer);
        let stream_id = Arc::new(stream_id);
        let topic_id = Arc::new(topic_id);
        let state = IggyConsumerState::new(
            client.clone(),
            Arc::clone(&consumer),
            Arc::clone(&stream_id),
            Arc::clone(&topic_id),
            poll_options.commit,
        );
        let membership = (consumer.kind == ConsumerKind::ConsumerGroup).then(|| {
            Arc::new(Membership::new(
                client.clone(),
                Arc::clone(&stream_id),
                Arc::clone(&topic_id),
                consumer.id.clone(),
                create_consumer_group_if_not_exists,
            ))
        });
        Self {
            client,
            consumer_name,
            consumer,
            stream_id,
            topic_id,
            partitions,
            polling_strategy,
            poll_options,
            prefetch_bytes,
            prefetch_messages,
            membership,
            encryptor,
            polling_retry_interval,
            init_retries,
            init_retry_interval,
            allow_replay,
            offset_drain_timeout,
            state,
            reader: None,
            current: None,
            initialized: false,
            uninitialized_poll_warned: false,
            shutdown: AtomicBool::new(false),
            uncounted: None,
        }
    }

    /// Whether the consumer resumes after its stored offsets but never stores one itself.
    pub(crate) fn relies_on_manual_commits(&self) -> bool {
        self.poll_options.commit == CommitPolicy::Manual
            && self.polling_strategy.kind == PollingKind::Next
    }

    /// When a message counts under `CommitPolicy::Auto`.
    pub(crate) fn counts_after(&self) -> Option<CommitAfter> {
        match self.poll_options.commit {
            CommitPolicy::Auto(auto) => Some(auto.after),
            CommitPolicy::Manual | CommitPolicy::OnRead => None,
        }
    }

    /// Counts the message at `checkpoint` under `CommitPolicy::Auto`.
    pub(crate) fn count(&self, checkpoint: Checkpoint, ends_batch: bool) {
        self.state.progress.count(checkpoint, 1, ends_batch);
    }

    /// Whether the client is up or comes back without the caller.
    pub(crate) async fn client_can_recover(&self) -> bool {
        self.client.read().await.can_recover().await
    }

    /// Returns the name of the consumer.
    ///
    /// For a consumer group this is also the name of the group.
    pub fn name(&self) -> &str {
        &self.consumer_name
    }

    /// Returns the identifier of the topic this consumer reads from.
    pub fn topic(&self) -> &Identifier {
        &self.topic_id
    }

    /// Returns the identifier of the stream this consumer reads from.
    pub fn stream(&self) -> &Identifier {
        &self.stream_id
    }

    /// Returns the partition of the batch that the consumer yields from, or `0` before the
    /// first one.
    ///
    /// The value changes over time when the consumer reads several partitions. To commit for the
    /// partition a message came from, pass [`ReceivedMessage::checkpoint`] to
    /// [`commit()`](Self::commit) instead.
    pub fn partition_id(&self) -> u32 {
        self.state.partition_id()
    }

    /// Returns a view of the consumer state that can be read without exclusive access.
    pub fn state(&self) -> IggyConsumerState {
        self.state.clone()
    }

    /// Stores an offset on the server, marking every message up to and including it as consumed.
    /// It is the raw counterpart to [`commit()`](Self::commit): it always sends, also an offset
    /// below the stored one, and it needs no checkpoint.
    ///
    /// Pass `None` as `partition_id` to use [`partition_id()`](Self::partition_id), which can
    /// already point at another partition. Prefer passing [`ReceivedMessage::partition_id`].
    ///
    /// The offset is stored under the context of the latest message consumed from that
    /// partition, or without a context when nothing was consumed from it. An offset taken from a
    /// message of an older incarnation or owner is therefore stored under the newer context. A
    /// refusal of the captured context, such as [`IggyError::HistoryUnavailable`] after the
    /// partition was recreated, repeats until a message is consumed under the new context.
    ///
    /// With [`CommitPolicy::OnRead`], a stored offset also moves the reading position: the
    /// consumer drops the messages of the partition that it holds and reads on from the message
    /// after `offset`, as a [`seek`](Self::seek) there does, and the offset counts as the last
    /// consumed message, so [`shutdown()`](Self::shutdown) keeps it. A skip past a message is
    /// therefore never delivered again.
    ///
    /// On binary transports, a retained operation blocking this partition is resolved before
    /// the requested store. Recovery and the new store share one operation deadline. The store
    /// goes on after a proved commit or committed rejection of that operation, and the client keeps
    /// the proved outcome for the caller that owns the operation, see
    /// [`Client::resolve_operation`](crate::prelude::Client::resolve_operation).
    ///
    /// # Errors
    ///
    /// [`IggyError::OperationUncertain`] means this store may have committed and remains retained.
    /// [`IggyError::LaneBlocked`] means the earlier operation is still unresolved and this store
    /// was not sent. Other failures include connection, permission, history and ownership errors.
    pub async fn store_offset(
        &self,
        offset: u64,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        self.state.store_offset(offset, partition_id).await
    }

    /// Stores the offset of a checkpoint that this consumer yielded, which covers every earlier
    /// message of its partition. It keeps the partition history that delivered the message, so a
    /// commit after a recreation or an ownership change is refused instead of landing on the
    /// newer partition.
    ///
    /// A checkpoint of an older generation of its partition, after a [`seek`](Self::seek) or a
    /// new assignment, or at or below the last commit of its generation, stores nothing.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidCommand`] with [`CommitPolicy::OnRead`], and for a checkpoint that
    /// this consumer did not yield. Otherwise the recovery and error contract of
    /// [`Self::store_offset`].
    pub async fn commit(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        self.state.commit(checkpoint).await
    }

    /// Counts a checkpoint that this consumer yielded, under [`CommitPolicy::Auto`]. It stores
    /// nothing now: the policy stores the latest counted position of the partition. With
    /// [`CommitAfter::Marked`] only marks count, so messages processed concurrently count once
    /// they are done. [`IggyConsumerState::mark`] does the same from another task.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidCommand`] without `CommitPolicy::Auto`, and for a checkpoint that
    /// this consumer did not yield.
    pub fn mark(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        self.state.mark(checkpoint)
    }

    /// Returns the offset of the last message this consumer handed over for the given partition,
    /// or `None` until a message from that partition has been handed over.
    ///
    /// This is the local reading position, which can be ahead of what has been stored on the
    /// server.
    pub fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        self.state.get_last_consumed_offset(partition_id)
    }

    /// Deletes the offset stored on the server, so that the next consumer polling with
    /// [`PollingStrategy::next()`] starts at the first message.
    ///
    /// `None` as `partition_id` means [`partition_id()`](Self::partition_id) for a standalone
    /// consumer. A binary consumer group requires an explicit partition. Starting a delete
    /// clears this consumer's stored-offset record, while retaining its consumed position, so
    /// a commit policy or [`shutdown()`](Self::shutdown) can store that position again. When
    /// starting over, call it after [`shutdown()`](Self::shutdown).
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::ConsumerOffsetNotFound`] when nothing is stored for that partition, or
    /// any other error the server raised. Uses the recovery and uncertainty contract of
    /// [`Self::store_offset`].
    pub async fn delete_offset(&self, partition_id: Option<u32>) -> Result<(), IggyError> {
        self.state.delete_offset(partition_id).await
    }

    /// Returns the offset this consumer last stored on the server for the given partition, or
    /// `None` until this consumer observes a successful commit for the partition.
    ///
    /// The value is this consumer's own record of what it committed, kept in memory rather than
    /// read back from the server. With [`CommitPolicy::OnRead`], it is the end of the last
    /// batch that the consumer took from its reader.
    pub fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        self.state.get_last_stored_offset(partition_id)
    }

    /// Sets the next read position of `partition`, as [`PartitionReader::seek`] does, and drops
    /// the rest of the batch of `partition` that the consumer yields from. With
    /// [`CommitPolicy::OnRead`], a seek to an offset above 0 first stores the offset before it,
    /// so a restart resumes at the new position. Every other seek stores nothing.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidConfiguration`] before [`init()`](Self::init) and after
    /// [`shutdown()`](Self::shutdown). Otherwise the errors of [`PartitionReader::seek`].
    pub async fn seek(&mut self, partition: u32, start: PollingStrategy) -> Result<(), IggyError> {
        let Some(reader) = self.reader.as_mut() else {
            return Err(IggyError::InvalidConfiguration);
        };
        reader.seek(partition, start).await?;
        if self
            .current
            .as_ref()
            .is_some_and(|current| current.partition_id == partition)
        {
            self.current = None;
        }
        Ok(())
    }

    /// Initializes the consumer and makes it ready to poll messages.
    ///
    /// This must be called before the consumer can start polling messages. Calling it again on an
    /// initialized consumer does nothing and returns immediately.
    ///
    /// Initialization ensures that:
    /// - the configuration can run, checked before anything is sent.
    /// - the consumer's `stream_id` and `topic_id` exist on the server.
    ///   It retries for a number of `init_retries` (defaults to `None`, which is treated as no
    ///   retry) with `init_retry_interval` (defaults to one
    ///   second) time in between retries. Both can be set together through
    ///   [`IggyConsumerBuilder::init_retries`](crate::prelude::IggyConsumerBuilder::init_retries).
    /// - the [`PartitionReader`] of the consumer starts. A group member creates its group if it
    ///   does not exist yet and
    ///   [`create_consumer_group_if_not_exists`](crate::prelude::IggyConsumerBuilder::create_consumer_group_if_not_exists)
    ///   is on, and joins it.
    /// - under [`CommitPolicy::Auto`], the reader runs the task that stores the counted
    ///   positions, see [Positions and commits](IggyConsumer#positions-and-commits).
    ///
    /// # Errors
    ///
    /// - The errors of [`ReaderBuilder::build`]: the refusals of the configuration, and of a
    ///   consumer group over HTTP. They are checked before anything is sent, except the
    ///   transport check of a group and the join.
    /// - [`IggyError::StreamNameNotFound`], [`IggyError::StreamIdNotFound`] or [`IggyError::TopicNameNotFound`],
    ///   [`IggyError::TopicIdNotFound`] when the stream or the topic still does not exist once the retries are
    ///   exhausted.
    /// - [`IggyError::ConsumerGroupNameNotFound`] when the consumer group does not exist
    ///   and its auto creation is disabled.
    /// - Any error returned by the server while looking up the stream or the topic, or
    ///   while creating or joining the consumer group. Such an error ends initialization
    ///   immediately instead of consuming a retry.
    /// - [`IggyError::OperationUncertain`] for a create or a join of the group with an unknown
    ///   outcome. Call `init()` again: it resolves that operation before it joins.
    pub async fn init(&mut self) -> Result<(), IggyError> {
        if self.initialized {
            return Ok(());
        }
        self.validate()?;

        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let consumer_name = &self.consumer_name;

        info!(
            "Initializing consumer: {consumer_name} for stream: {stream_id}, topic: {topic_id}..."
        );

        {
            let mut retries = 0;
            let init_retries = self.init_retries.unwrap_or_default();
            let interval = self.init_retry_interval;

            let mut timer = time::interval(interval.get_duration());
            timer.tick().await;

            let client = self.client.read().await;
            let mut stream_exists = client.get_stream(&stream_id).await?.is_some();
            let mut topic_exists = client.get_topic(&stream_id, &topic_id).await?.is_some();

            // Absent streams or topics are not necessarily permanent failures.
            // It may happen that get_stream/ get_topic races the initial setup of the stream/ topic.
            // Retry for init_retries times, while waiting interval between retries.
            loop {
                if stream_exists && topic_exists {
                    info!(
                        "Stream: {stream_id} and topic: {topic_id} were found. Initializing consumer...",
                    );
                    break;
                }

                if retries >= init_retries {
                    break;
                }

                retries += 1;
                if !stream_exists {
                    warn!(
                        "Stream: {stream_id} does not exist. Retrying ({retries}/{init_retries}) in {interval}...",
                    );
                    timer.tick().await;
                    stream_exists = client.get_stream(&stream_id).await?.is_some();
                }

                if !stream_exists {
                    continue;
                }

                topic_exists = client.get_topic(&stream_id, &topic_id).await?.is_some();
                if topic_exists {
                    break;
                }

                warn!(
                    "Topic: {topic_id} does not exist in stream: {stream_id}. Retrying ({retries}/{init_retries}) in {interval}...",
                );
                timer.tick().await;
            }

            if !stream_exists {
                error!("Stream: {stream_id} was not found.");
                return Err(match stream_id.kind {
                    IdKind::String => IggyError::StreamNameNotFound(stream_id.get_string_value()?),
                    IdKind::Numeric => {
                        IggyError::StreamIdNotFound(Identifier::from_identifier(&stream_id))
                    }
                });
            }

            if !topic_exists {
                error!("Topic: {topic_id} was not found in stream: {stream_id}.");
                return Err(match topic_id.kind {
                    IdKind::String => IggyError::TopicNameNotFound(
                        topic_id.get_string_value()?,
                        stream_id.to_string(),
                    ),
                    IdKind::Numeric => IggyError::TopicIdNotFound(
                        Identifier::from_identifier(&topic_id),
                        Identifier::from_identifier(&stream_id),
                    ),
                });
            }
        }

        self.reader = Some(self.reader_builder().build().await?);
        self.initialized = true;
        info!(
            "Consumer: {} has been initialized for stream: {}, topic: {}.",
            self.consumer_name, self.stream_id, self.topic_id
        );
        Ok(())
    }

    /// The checks that need no I/O, so a configuration that cannot run fails before the lookups.
    fn validate(&self) -> Result<(), IggyError> {
        self.reader_builder().into_parts()?;
        Ok(())
    }

    fn reader_builder(&self) -> ReaderBuilder {
        let mut builder = ReaderBuilder::new(
            self.client.clone(),
            Identifier::clone(&self.stream_id),
            Identifier::clone(&self.topic_id),
            Consumer::clone(&self.consumer),
            self.encryptor.clone(),
        )
        .start(self.polling_strategy)
        .options(self.poll_options)
        .prefetch_bytes(self.prefetch_bytes)
        .progress(Arc::clone(&self.state.progress))
        .retry_interval(self.polling_retry_interval.get_duration());
        if let Some(membership) = &self.membership {
            builder = builder.membership(Arc::clone(membership));
        }
        if let Some(partitions) = &self.partitions {
            builder = builder.partitions(partitions.iter().copied());
        }
        if let Some(prefetch_messages) = self.prefetch_messages {
            builder = builder.prefetch_messages(prefetch_messages);
        }
        builder
    }

    /// The next message, with whether it ends its batch. It counts nothing: the stream and
    /// `consume_messages` count each message as their commit policy says.
    pub(crate) fn poll_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(ReceivedMessage, bool), ReadError>>> {
        if self.shutdown.load(ORDERING) {
            return Poll::Ready(None);
        }
        if !self.initialized {
            // A loop that continues on errors would otherwise spin without a trace.
            if !self.uninitialized_poll_warned {
                self.uninitialized_poll_warned = true;
                warn!(
                    "Consumer: {} was polled before init(). Every poll fails until init() runs.",
                    self.consumer_name
                );
            }
            return Poll::Ready(Some(Err(ReadError::new(
                None,
                IggyError::InvalidConfiguration,
            ))));
        }
        loop {
            if let Some((received, ends_batch)) = self.yield_current() {
                return Poll::Ready(Some(Ok((received, ends_batch))));
            }
            let Some(reader) = self.reader.as_mut() else {
                return Poll::Ready(None);
            };
            match reader.poll_held(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Ok((batch, hold))) => {
                    self.state
                        .current_partition_id
                        .store(batch.partition_id, ORDERING);
                    let delivered = if self.allow_replay {
                        None
                    } else {
                        self.state.progress.replay_floor(batch.partition_id)
                    };
                    self.current = Some(CurrentBatch::new(batch, hold, delivered));
                }
            }
        }
    }

    /// The next message of the current batch past the replay filter, recorded as delivered. The
    /// batch leaves the budget with its last message.
    fn yield_current(&mut self) -> Option<(ReceivedMessage, bool)> {
        let current = self.current.as_mut()?;
        // The server stored the offset of an `OnRead` batch, so only a seek drops it, which
        // the hold also tells.
        if !current.hold.is_current() {
            self.current = None;
            return None;
        }
        let yielded = current.messages.find_map(|message| {
            let offset = message.header.offset;
            let replayed = current
                .delivered_offset
                .is_some_and(|delivered| offset <= delivered);
            (!replayed).then(|| ReceivedMessage {
                message,
                current_offset: current.current_offset,
                partition_id: current.partition_id,
                checkpoint: Checkpoint::stamped(
                    current.partition_id,
                    offset,
                    current.context,
                    current.stamp,
                ),
            })
        });
        let ends_batch = current.messages.as_slice().is_empty();
        if ends_batch {
            self.current = None;
        }
        let received = yielded?;
        if let Some(reader) = &self.reader {
            reader.delivered(&received.checkpoint);
        }
        Some((received, ends_batch))
    }
}

/// A single message handed over by an [`IggyConsumer`].
#[derive(Debug)]
pub struct ReceivedMessage {
    /// The message itself, with its payload already decrypted when the client uses an encryptor.
    ///
    /// Commit [`Self::checkpoint`] with [`IggyConsumer::commit`] to retain the incarnation and
    /// owner even if another poll observes a newer context.
    pub message: IggyMessage,
    /// The offset of the newest message in the partition at the time it was polled.
    ///
    /// Comparing it with `message.header.offset` shows how far this consumer lags behind the end
    /// of the partition. It is a snapshot taken per request, so it does not change while the
    /// messages of that request are handed over.
    pub current_offset: u64,
    /// The partition this message was read from.
    ///
    /// For a consumer that reads several partitions this varies between messages.
    pub partition_id: u32,
    checkpoint: Checkpoint,
}

impl ReceivedMessage {
    /// The position after this message, to commit it and every earlier message of its partition
    /// with [`IggyConsumer::commit`]. It keeps the partition incarnation and owner that delivered
    /// the message, so a commit after a recreation or an ownership change is refused instead of
    /// landing on the newer partition.
    #[must_use]
    pub const fn checkpoint(&self) -> Checkpoint {
        self.checkpoint
    }

    /// Creates a received message from a message, the partition head at poll time, the
    /// partition it was read from and the context of its partition history. No consumer yielded
    /// it, so [`IggyConsumer::commit`] refuses its checkpoint.
    pub fn new(
        message: IggyMessage,
        current_offset: u64,
        partition_id: u32,
        context: PartitionContext,
    ) -> Self {
        let checkpoint = Checkpoint::new(partition_id, message.header.offset, context);
        Self {
            message,
            current_offset,
            partition_id,
            checkpoint,
        }
    }
}

/// Yields messages one at a time, from the current batch first and from the reader once it is
/// empty.
///
/// See [`IggyConsumer`] for how the reader reads, and [`PartitionReader`] for where reading
/// continues after an error.
impl Stream for IggyConsumer {
    type Item = Result<ReceivedMessage, ReadError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let consumer = self.get_mut();
        consumer.count_uncounted();
        let polled = consumer.poll_message(cx);
        if let Poll::Ready(Some(Ok((received, ends_batch)))) = &polled {
            match consumer.counts_after() {
                Some(CommitAfter::Delivery) => consumer.count(received.checkpoint, *ends_batch),
                Some(CommitAfter::Processing) => {
                    consumer.uncounted = Some((received.checkpoint, *ends_batch));
                }
                Some(CommitAfter::Marked) | None => {}
            }
        }
        polled.map_ok(|(received, _)| received)
    }
}

impl IggyConsumer {
    /// Shuts the consumer down. Await it before dropping the consumer: a dropped consumer
    /// stops its reader without storing or rewinding anything and stays in its group until the
    /// session ends.
    ///
    /// - Under [`CommitPolicy::Auto`], the message that the stream yielded last counts under
    ///   [`CommitAfter::Processing`], and the latest counted position of each partition is
    ///   stored. The stores wait at most
    ///   [`offset_drain_timeout`](crate::prelude::IggyConsumerBuilder::offset_drain_timeout),
    ///   and a failure is logged.
    /// - The reader closes with [`PartitionReader::close`]: with [`CommitPolicy::OnRead`] it
    ///   moves the offset back to the last message that the consumer yielded, and a group
    ///   member leaves its group, so the server gives its partitions to the remaining members
    ///   at once instead of after the connection times out. The close cancels each `OnRead`
    ///   read in flight and takes its one reply, which tells whether the server stored, so a
    ///   read that waits on the server ends at once. Over HTTP, which has no cancel, it waits
    ///   until the deadline of that read.
    ///
    /// # Errors
    ///
    /// The errors of [`PartitionReader::close`]: the first failed store of the `OnRead` close,
    /// or else `PartitionLaneBlocked` for a partition that another call on the client left with
    /// an unknown outcome. Explicitly abandon that operation on the client before starting a
    /// replacement consumer. Group-leave failures are logged.
    pub async fn shutdown(&mut self) -> Result<(), IggyError> {
        if self.shutdown.swap(true, ORDERING) {
            return Ok(());
        }
        self.current = None;
        info!("Shutting down consumer: {}...", self.consumer_name);
        self.count_uncounted();

        let progress = &self.state.progress;
        let unstored = progress.unstored_delivered(None);
        if self.relies_on_manual_commits()
            && !unstored.is_empty()
            && unstored
                .iter()
                .all(|delivered| progress.last_committed(delivered.partition_id()).is_none())
        {
            warn!(
                "Consumer: {} delivered messages but stored no offset, so the next run reads them again. Commit checkpoints after processing, or set CommitPolicy::Auto in the poll options.",
                self.consumer_name
            );
        }

        let Some(mut reader) = self.reader.take() else {
            return Ok(());
        };
        // The stores go out while the member still owns its partitions, before the close leaves
        // the group.
        match time::timeout(self.offset_drain_timeout.get_duration(), reader.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(
                "Failed to store the counted offsets of consumer: {}. {error}",
                self.consumer_name
            ),
            Err(_) => warn!(
                "Timed out storing the counted offsets of consumer: {} after {}.",
                self.consumer_name, self.offset_drain_timeout
            ),
        }
        let result = reader.close().await;
        info!("Consumer: {} has been shut down.", self.consumer_name);
        result
    }

    /// Under `CommitAfter::Processing`, the message that the stream yielded last counts once the
    /// stream is polled again or shuts down.
    fn count_uncounted(&mut self) {
        if let Some((checkpoint, ends_batch)) = self.uncounted.take() {
            self.count(checkpoint, ends_batch);
        }
    }
}

#[cfg(test)]
mod tests;
