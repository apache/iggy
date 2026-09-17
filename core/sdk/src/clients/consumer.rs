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
use dashmap::DashMap;
use futures::Stream;
use futures_util::StreamExt;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{Client, ConsumerGroupClient, ConsumerOffsetClient, StreamClient, TopicClient};
use iggy_common::{
    Consumer, ConsumerKind, DeferredPollOptions, DiagnosticEvent, EncryptorKind, IdKind,
    Identifier, IggyDuration, IggyError, IggyMessage, NonZeroIggyDuration, PollingStrategy,
};
use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
use std::task::{Context, Poll};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, info, trace, warn};

const ORDERING: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;

/// The auto-commit configuration for storing the offset on the server.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum AutoCommit {
    /// The auto-commit is disabled and the offset must be stored manually by the consumer.
    Disabled,
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval.
    Interval(NonZeroIggyDuration),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval or depending on the mode when consuming the messages.
    IntervalOrWhen(NonZeroIggyDuration, AutoCommitWhen),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval or depending on the mode after consuming the messages.
    ///
    /// **This will only work with the `IggyConsumerMessageExt` trait when using `consume_messages()`.**
    IntervalOrAfter(NonZeroIggyDuration, AutoCommitAfter),
    /// The auto-commit is enabled and the offset is stored on the server depending on the mode when consuming the messages.
    When(AutoCommitWhen),
    /// The auto-commit is enabled and the offset is stored on the server depending on the mode after consuming the messages.
    ///
    /// **This will only work with the `IggyConsumerMessageExt` trait when using `consume_messages()`.**
    After(AutoCommitAfter),
}

/// The auto-commit mode for storing the offset on the server.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum AutoCommitWhen {
    /// The offset is stored on the server when the messages are received.
    PollingMessages,
    /// The offset is stored on the server when all the messages are consumed.
    ConsumingAllMessages,
    /// The offset is stored on the server when consuming each message.
    ConsumingEachMessage,
    /// The offset is stored on the server when consuming every Nth message.
    ConsumingEveryNthMessage(u32),
}

/// The auto-commit mode for storing the offset on the server **after** receiving the messages.
///
/// **This will only work with the `IggyConsumerMessageExt` trait when using `consume_messages()`.**
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum AutoCommitAfter {
    /// The offset is stored on the server after all the messages are consumed.
    ConsumingAllMessages,
    /// The offset is stored on the server after consuming each message.
    ConsumingEachMessage,
    /// The offset is stored on the server after consuming every Nth message.
    ConsumingEveryNthMessage(u32),
}

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
    client: IggyRwLock<ClientWrapper>,
    consumer: Arc<Consumer>,
    stream_id: Arc<Identifier>,
    topic_id: Arc<Identifier>,
    is_consumer_group: bool,
    allow_replay: bool,
    current_partition_id: Arc<AtomicU32>,
    last_consumed_offsets: Arc<DashMap<u32, AtomicU64>>,
    last_stored_offsets: Arc<DashMap<u32, AtomicU64>>,
}

impl Debug for IggyConsumerState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IggyConsumerState")
            .field("consumer", &self.consumer)
            .field("stream_id", &self.stream_id)
            .field("topic_id", &self.topic_id)
            .field("is_consumer_group", &self.is_consumer_group)
            .field("allow_replay", &self.allow_replay)
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
        is_consumer_group: bool,
        allow_replay: bool,
    ) -> Self {
        Self {
            client,
            consumer,
            stream_id,
            topic_id,
            is_consumer_group,
            allow_replay,
            current_partition_id: Arc::new(AtomicU32::new(0)),
            last_consumed_offsets: Arc::new(DashMap::new()),
            last_stored_offsets: Arc::new(DashMap::new()),
        }
    }

    /// Returns the current partition ID of the consumer.
    pub fn partition_id(&self) -> u32 {
        self.current_partition_id.load(ORDERING)
    }

    /// Retrieves the last consumed offset for the specified partition ID, or `None` while
    /// the partition is still untracked. Polling seeds an entry the first time it sees a
    /// partition, so `Some(0)` also covers "seen, nothing consumed yet".
    /// To get the current partition ID use `partition_id()`
    pub fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        let offset = self.last_consumed_offsets.get(&partition_id)?;
        Some(offset.load(ORDERING))
    }

    /// Retrieves the last stored offset (on the server) for the specified partition ID, or
    /// `None` while the partition is still untracked. Storing seeds an entry the first time
    /// it sees a partition, so `Some(0)` also covers "seen, nothing stored yet".
    /// To get the current partition ID use `partition_id()`
    pub fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        let offset = self.last_stored_offsets.get(&partition_id)?;
        Some(offset.load(ORDERING))
    }

    /// Stores the consumer offset on the server either for the current partition or the provided partition ID.
    pub async fn store_offset(
        &self,
        offset: u64,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        let partition_id = partition_id.unwrap_or_else(|| self.partition_id());
        self.store_consumer_offset(partition_id, offset, self.allow_replay)
            .await
    }

    /// Deletes the consumer offset on the server either for the current partition or the provided partition ID.
    pub async fn delete_offset(&self, mut partition_id: Option<u32>) -> Result<(), IggyError> {
        // `None` is only resolved server-side for consumer groups. For a standalone consumer
        // explicitly assign the current partition_id.
        if partition_id.is_none() && !self.is_consumer_group {
            partition_id = Some(self.partition_id());
        }
        let client = self.client.read().await;
        client
            .delete_consumer_offset(
                &self.consumer,
                &self.stream_id,
                &self.topic_id,
                partition_id,
            )
            .await
    }

    async fn store_consumer_offset(
        &self,
        partition_id: u32,
        offset: u64,
        allow_replay: bool,
    ) -> Result<(), IggyError> {
        let consumer = &self.consumer;
        let stream_id = &self.stream_id;
        let topic_id = &self.topic_id;
        trace!(
            "Storing offset: {offset} for consumer: {consumer}, partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}..."
        );
        let stored_offset;
        if let Some(offset_entry) = self.last_stored_offsets.get(&partition_id) {
            stored_offset = offset_entry.load(ORDERING);
        } else {
            stored_offset = 0;
            self.last_stored_offsets
                .insert(partition_id, AtomicU64::new(0));
        }

        if !allow_replay && (offset <= stored_offset && offset >= 1) {
            trace!(
                "Offset: {offset} is less than or equal to the last stored offset: {stored_offset} for consumer: {consumer}, partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}. Skipping storing the offset."
            );
            return Ok(());
        }

        let client = self.client.read().await;
        if let Err(error) = client
            .store_consumer_offset(consumer, stream_id, topic_id, Some(partition_id), offset)
            .await
        {
            error!(
                "Failed to store offset: {offset} for consumer: {consumer}, partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}. {error}"
            );
            return Err(error);
        }
        trace!(
            "Stored offset: {offset} for consumer: {consumer}, partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}."
        );
        if let Some(last_offset_entry) = self.last_stored_offsets.get(&partition_id) {
            last_offset_entry.store(offset, ORDERING);
        } else {
            self.last_stored_offsets
                .insert(partition_id, AtomicU64::new(offset));
        }
        Ok(())
    }

    /// Snapshots the last consumed offset of every tracked partition. Collecting up front
    /// releases the map guards, which must not be held across the store round trip.
    fn last_consumed_offsets(&self) -> Vec<(u32, u64)> {
        self.last_consumed_offsets
            .iter()
            .map(|entry| (*entry.key(), entry.load(ORDERING)))
            .collect()
    }
}

/// Reads a topic as a stream of messages, using long polling and bounded prefetch.
///
/// Call [`Self::init`] before reading and [`Self::shutdown`] before dropping the consumer.
/// A standalone consumer reads one partition. A group member reads its assignment on
/// independent data connections, while control traffic and assignment refresh continue
/// even when the application stops reading. At most 16 partition fetches run concurrently.
///
/// A poll returns when at least `min_count` messages are available (one by default),
/// its byte limit is reached, or `max_wait` expires. `batch_length` is the maximum
/// message count. The separate request timeout covers routing, waiting and reading.
/// There is no normal polling interval; errors and reconnects retain retry backoff.
///
/// # Positions and commits
///
/// The initial polling strategy is resolved separately for each partition. `Next`
/// starts after the stored offset, or at zero when no offset exists. Subsequent
/// fetches use explicit offsets, so a lost reply can be retried without advancing
/// past unseen messages. Reassignment discards prefetched data and resolves the
/// starting position again. Messages already delivered are filtered unless replay
/// is enabled. Delivery and durable processing progress are separate.
///
/// The default is [`AutoCommit::Disabled`]. Store offsets only after successfully
/// processing messages, in partition order. Fetching ahead never commits under this
/// default. Opting into auto-commit can acknowledge messages before processing;
/// `PollingMessages` also acknowledges batches still in the prefetch buffer.
///
/// ```rust,no_run
/// use futures_util::StreamExt;
/// use iggy::prelude::*;
///
/// # async fn handle(message: &IggyMessage) -> Result<(), IggyError> { Ok(()) }
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
/// let mut consumer = client.consumer("reader", "stream", "topic", 0)?.build();
/// consumer.init().await?;
/// while let Some(received) = consumer.next().await {
///     let received = received?;
///     handle(&received.message).await?;
///     consumer.store_offset(received.message.header.offset, Some(received.partition_id)).await?;
/// }
/// consumer.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Buffer bounds
///
/// Both builders reserve a full possible batch in bytes and messages before sending
/// each request. The reservation covers in-flight, completed and buffered replies
/// until the batch is drained or discarded. The byte budget measures encoded poll
/// replies; decoded message metadata is additionally bounded by the message budget.
/// Messages retained by application code are outside these SDK buffer limits.
/// Assignment changes invalidate buffered generations before the next delivery.
///
/// Encryption failures are returned without advancing the fetch position. Polling
/// auto-commit with an encryptor is rejected because the server would commit before
/// decryption. Other auto-commit policies retain their explicit semantics, including
/// interval commits of delivered messages and `AutoCommitAfter` commits after a
/// handler returns either success or failure.
pub struct IggyConsumer {
    poll_options: DeferredPollOptions,
    prefetch_bytes: u32,
    prefetch_messages: u32,
    deferred_polls: Option<deferred::DeferredPolls>,
    fetch_generation: Arc<AtomicU64>,
    fetch_notify: Arc<Notify>,
    buffered_generation: u64,
    buffered_assignment: Option<(u64, u64)>,
    assignment_state: Option<(Arc<iggy_common::ConsumerGroupClientState>, String)>,
    buffered_capacity: Option<deferred::FetchCapacity>,
    initialized: bool,
    shutdown: Arc<AtomicBool>,
    can_poll: Arc<AtomicBool>,
    client: IggyRwLock<ClientWrapper>,
    consumer_name: String,
    consumer: Arc<Consumer>,
    is_consumer_group: bool,
    joined_consumer_group: Arc<AtomicBool>,
    stream_id: Arc<Identifier>,
    topic_id: Arc<Identifier>,
    partition_id: Option<u32>,
    polling_strategy: PollingStrategy,
    batch_length: u32,
    auto_commit: AutoCommit,
    auto_commit_after_polling: bool,
    auto_join_consumer_group: bool,
    create_consumer_group_if_not_exists: bool,
    state: IggyConsumerState,
    current_offsets: Arc<DashMap<u32, AtomicU64>>,
    buffered_messages: VecDeque<IggyMessage>,
    encryptor: Option<Arc<EncryptorKind>>,
    /// The latest offset each message trigger asked to commit, per partition. The store task
    /// drains it, so a burst of triggers costs one round trip per partition instead of one each.
    pending_commits: Arc<DashMap<u32, u64>>,
    store_offset_notify: Arc<Notify>,
    store_offset_task: Option<JoinHandle<()>>,
    background_commit_task: Option<JoinHandle<()>>,
    background_commit_notify: Arc<Notify>,
    events_task: Option<JoinHandle<()>>,
    store_offset_after_each_message: bool,
    store_offset_after_all_messages: bool,
    store_after_every_nth_message: u64,
    reconnection_retry_interval: NonZeroIggyDuration,
    init_retries: Option<u32>,
    init_retry_interval: NonZeroIggyDuration,
    allow_replay: bool,
    offset_drain_timeout: IggyDuration,
}

impl IggyConsumer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
        consumer_name: String,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        poll_options: DeferredPollOptions,
        prefetch_bytes: u32,
        prefetch_messages: u32,
        polling_strategy: PollingStrategy,
        batch_length: u32,
        auto_commit: AutoCommit,
        auto_join_consumer_group: bool,
        create_consumer_group_if_not_exists: bool,
        encryptor: Option<Arc<EncryptorKind>>,
        reconnection_retry_interval: NonZeroIggyDuration,
        init_retries: Option<u32>,
        init_retry_interval: NonZeroIggyDuration,
        allow_replay: bool,
        offset_drain_timeout: IggyDuration,
    ) -> Self {
        let is_consumer_group = consumer.kind == ConsumerKind::ConsumerGroup;
        let partition_id = if is_consumer_group && partition_id.is_some() {
            warn!(
                "Consumer group member: {consumer_name} ignores the partition set on the builder and reads the server's assignment"
            );
            None
        } else {
            partition_id
        };
        let consumer = Arc::new(consumer);
        let stream_id = Arc::new(stream_id);
        let topic_id = Arc::new(topic_id);
        let state = IggyConsumerState::new(
            client.clone(),
            consumer.clone(),
            stream_id.clone(),
            topic_id.clone(),
            is_consumer_group,
            allow_replay,
        );
        Self {
            poll_options,
            prefetch_bytes,
            prefetch_messages,
            deferred_polls: None,
            fetch_generation: Arc::new(AtomicU64::new(0)),
            fetch_notify: Arc::new(Notify::new()),
            buffered_generation: 0,
            buffered_assignment: None,
            assignment_state: None,
            buffered_capacity: None,
            initialized: false,
            shutdown: Arc::new(AtomicBool::new(false)),
            is_consumer_group,
            joined_consumer_group: Arc::new(AtomicBool::new(false)),
            can_poll: Arc::new(AtomicBool::new(true)),
            client,
            consumer_name,
            consumer,
            stream_id,
            topic_id,
            partition_id,
            polling_strategy,
            state,
            current_offsets: Arc::new(DashMap::new()),
            batch_length,
            auto_commit,
            auto_commit_after_polling: matches!(
                auto_commit,
                AutoCommit::When(AutoCommitWhen::PollingMessages)
                    | AutoCommit::IntervalOrWhen(_, AutoCommitWhen::PollingMessages)
            ),
            auto_join_consumer_group,
            create_consumer_group_if_not_exists,
            buffered_messages: VecDeque::new(),
            encryptor,
            pending_commits: Arc::new(DashMap::new()),
            store_offset_notify: Arc::new(Notify::new()),
            store_offset_task: None,
            background_commit_task: None,
            background_commit_notify: Arc::new(Notify::new()),
            events_task: None,
            store_offset_after_each_message: matches!(
                auto_commit,
                AutoCommit::When(AutoCommitWhen::ConsumingEachMessage)
                    | AutoCommit::IntervalOrWhen(_, AutoCommitWhen::ConsumingEachMessage)
            ),
            store_offset_after_all_messages: matches!(
                auto_commit,
                AutoCommit::When(AutoCommitWhen::ConsumingAllMessages)
                    | AutoCommit::IntervalOrWhen(_, AutoCommitWhen::ConsumingAllMessages)
            ),
            store_after_every_nth_message: match auto_commit {
                AutoCommit::When(AutoCommitWhen::ConsumingEveryNthMessage(n))
                | AutoCommit::IntervalOrWhen(_, AutoCommitWhen::ConsumingEveryNthMessage(n)) => {
                    n as u64
                }
                _ => 0,
            },
            reconnection_retry_interval,
            init_retries,
            init_retry_interval,
            allow_replay,
            offset_drain_timeout,
        }
    }

    pub(crate) fn auto_commit(&self) -> AutoCommit {
        self.auto_commit
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

    /// Returns the partition the most recent poll response with messages came from, or `0` before
    /// the first one.
    ///
    /// For a consumer group the value changes over time, as the server hands different partitions
    /// to this member. To commit for the partition a message came from, pass
    /// [`ReceivedMessage::partition_id`] to [`store_offset()`](Self::store_offset) instead.
    pub fn partition_id(&self) -> u32 {
        self.state.partition_id()
    }

    /// Returns a view of the consumer state that can be read without exclusive access.
    pub fn state(&self) -> IggyConsumerState {
        self.state.clone()
    }

    /// Stores an offset on the server, marking every message up to and including it as consumed.
    ///
    /// This is the manual counterpart to [`AutoCommit`] and is meant for
    /// [`AutoCommit::Disabled`].
    ///
    /// Pass `None` as `partition_id` to use [`partition_id()`](Self::partition_id), which for a
    /// consumer group can already point at another partition. Prefer passing
    /// [`ReceivedMessage::partition_id`].
    ///
    /// An offset that is not ahead of the last one this consumer stored for that partition is
    /// skipped and `Ok(())` is returned without a request, unless the consumer was built with
    /// [`allow_replay`](crate::prelude::IggyConsumerBuilder::allow_replay). Offset `0` is always
    /// sent, yet [`PollingStrategy::next()`] then resumes at offset `1`, so it does not rewind to
    /// the start. To start over from the first message, delete the stored offset with
    /// [`delete_offset()`](Self::delete_offset) after [`shutdown()`](Self::shutdown) and build a new
    /// consumer. A new consumer built with [`PollingStrategy::offset()`] re-reads from any point.
    ///
    /// # Errors
    ///
    /// Returns any error the server raised while storing the offset, for example
    /// [`IggyError::Disconnected`] or a permission error. The offset is then not stored and the
    /// call can be retried.
    pub async fn store_offset(
        &self,
        offset: u64,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        self.state.store_offset(offset, partition_id).await
    }

    /// Returns the offset of the last message this consumer handed over for the given partition,
    /// or `None` while it has not polled that partition yet. The first poll of a partition seeds
    /// the entry with `0`, so `Some(0)` also covers "polled, nothing handed over yet".
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
    /// consumer. A consumer group passes `None` through to the server. This consumer's own records
    /// are untouched, so its auto-commit or [`shutdown()`](Self::shutdown) can store the offset
    /// again. When starting over, call it after [`shutdown()`](Self::shutdown).
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::ConsumerOffsetNotFound`] when nothing is stored for that partition, or
    /// any other error the server raised, for example [`IggyError::Disconnected`].
    pub async fn delete_offset(&self, partition_id: Option<u32>) -> Result<(), IggyError> {
        self.state.delete_offset(partition_id).await
    }

    /// Returns the offset this consumer last stored on the server for the given partition, or
    /// `None` until this consumer observes a successful commit for the partition.
    ///
    /// The value is this consumer's own record of what it committed, kept in memory rather than
    /// read back from the server.
    /// With auto-commit-on-poll, the server can be ahead by prefetched batches.
    pub fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        self.state.get_last_stored_offset(partition_id)
    }

    /// Initializes the consumer and makes it ready to poll messages.
    ///
    /// This must be called before the consumer can start polling messages. Calling it again on an
    /// initialized consumer does nothing and returns immediately.
    ///
    /// Initialization ensures that:
    /// - the consumer's `stream_id` and `topic_id` exist on the server.
    ///   It retries for a number of `init_retries` (defaults to `None`, which is treated as no
    ///   retry) with `init_retry_interval` (defaults to one
    ///   second) time in between retries. Both can be set together through
    ///   [`IggyConsumerBuilder::init_retries`](crate::prelude::IggyConsumerBuilder::init_retries).
    /// - the consumer subscribes to connection lifecycle events ([`DiagnosticEvent`]) in order to
    ///   update its state, should it receive a shutdown, connected, disconnected, log in or log out event.
    /// - if the consumer belongs to a group and `auto_join_consumer_group` is enabled, the group is
    ///   initialized if it does not exist yet, and the consumer joins that group.
    /// - the tasks that store the offset on the server are spawned.
    ///
    /// # Lifecycle events
    ///
    /// Calling init spawns a background task that listens for lifecycle changes ([`DiagnosticEvent`]s) of the
    /// client connection. It runs until [`shutdown()`](Self::shutdown) or until the client shuts
    /// down.
    /// - [`DiagnosticEvent::Connected`]: a fresh connection has not joined anything yet.
    ///   Polling resumes immediately only for a consumer that is not a group member.
    /// - [`DiagnosticEvent::SignedIn`]: re-enables polling. A group member whose membership is
    ///   gone rejoins its group on the next poll, before the request goes out. A failed rejoin is
    ///   yielded as a poll error and tried again on the poll after.
    /// - [`DiagnosticEvent::Disconnected`] and [`DiagnosticEvent::SignedOut`] disable polling and
    ///   forget the group membership.
    /// - [`DiagnosticEvent::Shutdown`] disables polling and terminates the background task listening
    ///   for lifecycle changes. It does not flush in-flight commits; that only happens when
    ///   [`shutdown()`](Self::shutdown) itself is called.
    ///
    /// # Storing offsets
    ///
    /// When the consumer commits is decided by
    /// [`auto_commit()`](crate::prelude::IggyConsumerBuilder::auto_commit), see
    /// [Positions and commits](IggyConsumer#positions-and-commits). `init()` spawns the
    /// tasks behind it:
    /// - An interval task, only for the variants that carry an interval ([`AutoCommit::Interval`],
    ///   [`AutoCommit::IntervalOrWhen`], [`AutoCommit::IntervalOrAfter`]). Every tick it stores the
    ///   reading position of every partition read so far.
    /// - An offset store task, always. It sends the commits queued by the [`AutoCommitWhen`] and
    ///   [`AutoCommitAfter`] triggers, keeping only the latest queued offset per partition, and
    ///   stays idle under [`AutoCommit::Disabled`].
    ///
    /// Both skip an offset that is not ahead of this consumer's own record of what it stored
    /// ([`get_last_stored_offset()`](Self::get_last_stored_offset)). Only offset `0` is always sent.
    /// Combining interval commits with auto-commit-on-poll can move the server offset
    /// back to the delivered position while later prefetched batches are already committed.
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidConfiguration`] when the consumer has an encryptor and
    ///   [`auto_commit()`](crate::prelude::IggyConsumerBuilder::auto_commit) is
    ///   [`AutoCommitWhen::PollingMessages`], checked before anything is sent because
    ///   the server would commit before decryption succeeds.
    /// - [`IggyError::StreamNameNotFound`] or [`IggyError::TopicNameNotFound`] when the
    ///   stream or the topic still does not exist once the retries are exhausted.
    /// - [`IggyError::ConsumerGroupNameNotFound`] when the consumer group does not exist
    ///   and its auto creation is disabled.
    /// - Any error returned by the server while looking up the stream or the topic, or
    ///   while creating or joining the consumer group. Such an error ends initialization
    ///   immediately instead of consuming a retry.
    pub async fn init(&mut self) -> Result<(), IggyError> {
        self.poll_options.validate(self.batch_length)?;
        if self.prefetch_slots() == 0 {
            return Err(IggyError::InvalidConfiguration);
        }
        if self.initialized {
            return Ok(());
        }

        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let consumer_name = &self.consumer_name;

        if self.encryptor.is_some() && self.auto_commit_after_polling {
            error!(
                "Consumer: {consumer_name} has an encryptor and auto-commit on polling. That commits a batch before it is decrypted, so a batch that fails to decrypt would be lost. Pick another auto-commit setting."
            );
            return Err(IggyError::InvalidConfiguration);
        }

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
                return Err(IggyError::StreamNameNotFound(
                    self.stream_id.get_string_value().unwrap_or_default(),
                ));
            };

            if !topic_exists {
                error!("Topic: {topic_id} was not found in stream: {stream_id}.");
                return Err(IggyError::TopicNameNotFound(
                    self.topic_id.get_string_value().unwrap_or_default(),
                    self.stream_id.get_string_value().unwrap_or_default(),
                ));
            }
        }

        // A retried init() after a failed join must not leave the earlier task behind.
        if let Some(previous) = self.events_task.replace(self.subscribe_events().await) {
            previous.abort();
        }
        self.init_consumer_group().await?;
        if self.is_consumer_group {
            self.assignment_state = Some((
                self.client.read().await.deferred_group_state().await?,
                format!("{}|{}|{}", self.stream_id, self.topic_id, self.consumer.id),
            ));
        }

        match self.auto_commit {
            AutoCommit::Interval(interval)
            | AutoCommit::IntervalOrWhen(interval, _)
            | AutoCommit::IntervalOrAfter(interval, _) => {
                self.background_commit_task = Some(self.store_offsets_in_background(interval));
            }
            _ => {}
        }

        self.store_offset_task = Some(self.store_pending_commits_in_background());

        self.initialized = true;
        self.deferred_polls = Some(deferred::DeferredPolls::start(self));
        info!(
            "Consumer: {consumer_name} has been initialized for stream: {}, topic: {}.",
            self.stream_id, self.topic_id
        );
        Ok(())
    }

    fn store_offsets_in_background(&self, interval: NonZeroIggyDuration) -> JoinHandle<()> {
        let state = self.state.clone();
        let shutdown = self.shutdown.clone();
        let notify = self.background_commit_notify.clone();
        tokio::spawn(async move {
            loop {
                // Wait for the task until either the interval has passed or
                // the task is explicitly notified, which happens when shutdown() is called.
                tokio::select! {
                    _ = time::sleep(interval.get_duration()) => {}
                    _ = notify.notified() => {}
                }

                // Checked before storing: `shutdown()` runs its own final flush as a
                // group member and then leaves, so a store past this point would hit
                // a group we've since left. After a bare `Drop` nothing flushes.
                if shutdown.load(ORDERING) {
                    trace!("Shutdown signal received, stopping background offset storage");
                    break;
                }
                for (partition_id, consumed_offset) in state.last_consumed_offsets() {
                    _ = state
                        .store_consumer_offset(partition_id, consumed_offset, false)
                        .await;
                }
            }
        })
    }

    /// Sends the commits queued by the message triggers of `poll_next` and `consume_messages`.
    /// The interval task and the poll request's own `auto_commit` flag are the other commit paths.
    fn store_pending_commits_in_background(&self) -> JoinHandle<()> {
        let state = self.state.clone();
        let pending_commits = self.pending_commits.clone();
        let shutdown = self.shutdown.clone();
        let notify = self.store_offset_notify.clone();
        tokio::spawn(async move {
            loop {
                notify.notified().await;
                // Keys first, so no map guard is held across a round trip. An offset queued
                // meanwhile stays in the map and the permit its trigger leaves wakes the next turn.
                let partitions: Vec<u32> =
                    pending_commits.iter().map(|entry| *entry.key()).collect();
                for partition_id in partitions {
                    let Some((_, offset)) = pending_commits.remove(&partition_id) else {
                        continue;
                    };
                    _ = state
                        .store_consumer_offset(partition_id, offset, false)
                        .await;
                }
                if shutdown.load(ORDERING) && pending_commits.is_empty() {
                    break;
                }
            }
        })
    }

    /// Queues a commit for the store task. A later offset for the same partition replaces a
    /// queued one that has not been sent yet.
    pub(crate) fn send_store_offset(&self, partition_id: u32, offset: u64) {
        if !self.initialized || self.shutdown.load(ORDERING) {
            error!(
                "Offset: {offset} for partition ID: {partition_id} was not queued for storing, consumer: {} is not initialized or has been shut down.",
                self.consumer_name
            );
            return;
        }
        self.pending_commits.insert(partition_id, offset);
        self.store_offset_notify.notify_one();
    }

    async fn init_consumer_group(&self) -> Result<(), IggyError> {
        if !self.is_consumer_group {
            return Ok(());
        }

        if !self.auto_join_consumer_group {
            warn!("Auto join consumer group is disabled");
            return Ok(());
        }
        tracing::debug!(
            "Initializing consumer group for stream ID: {}, topic ID: {}, consumer ID: {}",
            self.stream_id,
            self.topic_id,
            self.consumer
        );

        Self::initialize_consumer_group(
            self.client.clone(),
            self.create_consumer_group_if_not_exists,
            self.stream_id.clone(),
            self.topic_id.clone(),
            self.consumer.clone(),
            &self.consumer_name,
            self.joined_consumer_group.clone(),
        )
        .await
    }

    /// Keeps the polling flags in step with the connection. Joining the group again after a
    /// reconnect is left to the poll path, which retries it and reports a failure as a poll error.
    async fn subscribe_events(&self) -> JoinHandle<()> {
        trace!("Subscribing to diagnostic events");
        let mut receiver;
        {
            let client = self.client.read().await;
            receiver = client.subscribe_events().await;
        }

        let is_consumer_group = self.is_consumer_group;
        let can_poll = self.can_poll.clone();
        let joined_consumer_group = self.joined_consumer_group.clone();
        let generation = Arc::clone(&self.fetch_generation);
        let notify = Arc::clone(&self.fetch_notify);

        tokio::spawn(async move {
            while let Some(event) = receiver.next().await {
                trace!("Received diagnostic event: {event}");
                match event {
                    DiagnosticEvent::Shutdown => {
                        warn!("Consumer has been shutdown");
                        joined_consumer_group.store(false, ORDERING);
                        can_poll.store(false, ORDERING);
                        generation.fetch_add(1, ORDERING);
                        notify.notify_one();
                        break;
                    }
                    DiagnosticEvent::Connected => {
                        trace!("Connected to the server");
                        joined_consumer_group.store(false, ORDERING);
                        if !is_consumer_group {
                            can_poll.store(true, ORDERING);
                        }
                    }
                    DiagnosticEvent::Disconnected => {
                        joined_consumer_group.store(false, ORDERING);
                        can_poll.store(false, ORDERING);
                        warn!("Disconnected from the server");
                    }
                    DiagnosticEvent::SignedIn => {
                        can_poll.store(true, ORDERING);
                    }
                    DiagnosticEvent::SignedOut => {
                        joined_consumer_group.store(false, ORDERING);
                        can_poll.store(false, ORDERING);
                    }
                }
                generation.fetch_add(1, ORDERING);
                notify.notify_one();
            }
        })
    }

    async fn initialize_consumer_group(
        client: IggyRwLock<ClientWrapper>,
        create_consumer_group_if_not_exists: bool,
        stream_id: Arc<Identifier>,
        topic_id: Arc<Identifier>,
        consumer: Arc<Consumer>,
        consumer_name: &str,
        joined_consumer_group: Arc<AtomicBool>,
    ) -> Result<(), IggyError> {
        if joined_consumer_group.load(ORDERING) {
            return Ok(());
        }

        let client = client.read().await;
        let (name, _id) = match consumer.id.kind {
            IdKind::Numeric => (consumer_name.to_owned(), Some(consumer.id.get_u32_value()?)),
            IdKind::String => (consumer.id.get_string_value()?, None),
        };

        let consumer_group_id = name.to_owned().try_into()?;
        trace!(
            "Validating consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}"
        );
        if client
            .get_consumer_group(&stream_id, &topic_id, &consumer_group_id)
            .await?
            .is_none()
        {
            if !create_consumer_group_if_not_exists {
                error!("Consumer group does not exist and auto-creation is disabled.");
                let topic_identifier = Identifier::from_identifier(&topic_id);
                return Err(IggyError::ConsumerGroupNameNotFound(
                    name.to_owned(),
                    topic_identifier,
                ));
            }

            info!(
                "Creating consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}"
            );
            match client
                .create_consumer_group(&stream_id, &topic_id, &name)
                .await
            {
                Ok(_) => {}
                Err(IggyError::ConsumerGroupNameAlreadyExists(_, _)) => {}
                Err(error) => {
                    error!(
                        "Failed to create consumer group {consumer_group_id} for topic: {topic_id}, stream: {stream_id}: {error}"
                    );
                    return Err(error);
                }
            }
        }

        info!(
            "Joining consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}",
        );
        if let Err(error) = client
            .join_consumer_group(&stream_id, &topic_id, &consumer_group_id)
            .await
        {
            joined_consumer_group.store(false, ORDERING);
            error!(
                "Failed to join consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}: {error}"
            );
            return Err(error);
        }

        joined_consumer_group.store(true, ORDERING);
        info!(
            "Joined consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}"
        );
        Ok(())
    }

    fn prefetch_slots(&self) -> usize {
        if self.batch_length == 0 || self.poll_options.max_bytes == 0 {
            return 0;
        }
        (self.prefetch_messages / self.batch_length)
            .min(self.prefetch_bytes / self.poll_options.max_bytes)
            .min(crate::poll_routing::MAX_DEFERRED_CONNECTIONS as u32) as usize
    }
}

/// A single message handed over by an [`IggyConsumer`].
pub struct ReceivedMessage {
    /// The message itself, with its payload already decrypted when the client uses an encryptor.
    ///
    /// Its own offset is `message.header.offset`, which is the value to pass to
    /// [`IggyConsumer::store_offset`] when committing by hand.
    pub message: IggyMessage,
    /// The offset of the newest message in the partition at the time it was polled.
    ///
    /// Comparing it with `message.header.offset` shows how far this consumer lags behind the end
    /// of the partition. It is a snapshot taken per request, so it does not change while the
    /// buffered messages of that request are handed over.
    pub current_offset: u64,
    /// The partition this message was read from.
    ///
    /// For a consumer group this varies between messages, since the server hands different
    /// partitions to the same member.
    pub partition_id: u32,
}

impl ReceivedMessage {
    /// Creates a received message from a message, the partition head at poll time and the
    /// partition it was read from.
    pub fn new(message: IggyMessage, current_offset: u64, partition_id: u32) -> Self {
        Self {
            message,
            current_offset,
            partition_id,
        }
    }
}

/// Yields messages one at a time, from the buffer first and from a fresh poll once it is empty.
///
/// See [How messages are read](IggyConsumer#how-messages-are-read) for errors, `None` and polling.
impl Stream for IggyConsumer {
    type Item = Result<ReceivedMessage, IggyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.shutdown.load(ORDERING) {
            return Poll::Ready(None);
        }
        if !self.initialized {
            return Poll::Ready(Some(Err(IggyError::InvalidConfiguration)));
        }
        loop {
            let assignment_valid = self.assignment_state.as_ref().is_none_or(|(state, key)| {
                self.buffered_assignment.is_some_and(|assignment| {
                    state.is_current_assignment(key, assignment, self.state.partition_id())
                })
            });
            if self.buffered_generation != self.fetch_generation.load(ORDERING) || !assignment_valid
            {
                self.buffered_messages.clear();
                self.buffered_capacity = None;
            }
            let partition_id = self.state.partition_id();
            while let Some(message) = self.buffered_messages.pop_front() {
                let offset = message.header.offset;
                if !self.allow_replay
                    && self
                        .state
                        .get_last_consumed_offset(partition_id)
                        .is_some_and(|consumed| offset <= consumed)
                {
                    continue;
                }
                self.state
                    .last_consumed_offsets
                    .entry(partition_id)
                    .or_insert_with(|| AtomicU64::new(offset))
                    .store(offset, ORDERING);
                let last = self.buffered_messages.is_empty();
                if self.store_offset_after_each_message
                    || (self.store_after_every_nth_message > 0
                        && offset % self.store_after_every_nth_message == 0)
                    || (self.store_offset_after_all_messages && last)
                {
                    self.send_store_offset(partition_id, offset);
                }
                if last {
                    self.buffered_capacity = None;
                }
                let current_offset = self
                    .current_offsets
                    .get(&partition_id)
                    .map_or(0, |offset| offset.load(ORDERING));
                return Poll::Ready(Some(Ok(ReceivedMessage::new(
                    message,
                    current_offset,
                    partition_id,
                ))));
            }
            self.buffered_capacity = None;
            if self.deferred_polls.is_none() {
                self.deferred_polls = Some(deferred::DeferredPolls::start(&self));
            }
            let received = self
                .deferred_polls
                .as_mut()
                .expect("fetch worker initialized")
                .poll_recv(cx);
            match received {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.deferred_polls = None;
                    return Poll::Ready(Some(Err(IggyError::ClientShutdown)));
                }
                Poll::Ready(Some(batch)) => {
                    if batch.generation != self.fetch_generation.load(ORDERING) {
                        continue;
                    }
                    match batch.result {
                        Err(error) => return Poll::Ready(Some(Err(error))),
                        Ok(polled) => {
                            if polled.messages.is_empty() {
                                continue;
                            }
                            self.state
                                .current_partition_id
                                .store(polled.partition_id, ORDERING);
                            self.current_offsets
                                .entry(polled.partition_id)
                                .or_insert_with(|| AtomicU64::new(polled.current_offset))
                                .store(polled.current_offset, ORDERING);
                            if self.auto_commit_after_polling
                                && let Some(last) = polled.messages.last()
                            {
                                self.state
                                    .last_stored_offsets
                                    .entry(polled.partition_id)
                                    .or_insert_with(|| AtomicU64::new(last.header.offset))
                                    .store(last.header.offset, ORDERING);
                            }
                            self.buffered_messages = polled.messages.into();
                            self.buffered_generation = batch.generation;
                            self.buffered_assignment = batch.assignment;
                            self.buffered_capacity = Some(batch.capacity);
                        }
                    }
                }
            }
        }
    }
}

impl IggyConsumer {
    /// Shuts the consumer down.
    ///
    /// Specifically, run shutdown and await before dropping the consumer to
    /// - finish storing the offsets that are currently in-flight.
    ///   The interval task and the offset store task (see [`init()`](Self::init)) can both have
    ///   commits in flight. The consumer waits for `offset_drain_timeout` on each in turn before
    ///   forcing it to abort.
    /// - commit the reading position of every partition where it is ahead of this consumer's own
    ///   record of what it stored, unless [`auto_commit()`] is [`AutoCommit::Disabled`]. Under
    ///   explicit auto-commit-on-poll the poll already committed the whole batch, so this
    ///   store moves the server offset back to the last message handed over, and the next run
    ///   resumes right after it instead of after the last batch fetched.
    /// - leave the consumer group, if this consumer is a group member. This lets the server give its partitions to
    ///   the remaining members immediately instead of waiting for the connection to time out.
    /// - stop the task watching the connection lifecycle.
    ///
    /// [`auto_commit()`]: crate::prelude::IggyConsumerBuilder::auto_commit
    ///
    /// # Errors
    ///
    /// Returns `Ok(())` even when the final commits or the group leave failed, since those
    /// failures are logged and do not leave anything for the caller to undo. The
    /// [`Result`] is part of the signature for forward compatibility.
    pub async fn shutdown(&mut self) -> Result<(), IggyError> {
        // Swap so background tasks see that the consumer got shut down.
        if self.shutdown.swap(true, ORDERING) {
            return Ok(());
        }

        self.deferred_polls = None;
        self.buffered_messages.clear();
        self.buffered_capacity = None;

        info!("Shutting down consumer: {}...", self.consumer_name);

        // Drain the background commit tasks while still a group member, before
        // leaving below. Otherwise a store they send afterward hits a group
        // we've already left.
        self.background_commit_notify.notify_one();

        // A background_commit_task exists, if auto_commit is configured with an interval option.
        if let Some(mut task) = self.background_commit_task.take()
            && time::timeout(self.offset_drain_timeout.get_duration(), &mut task)
                .await
                .is_err()
        {
            // Still running past the bound: abort it rather than leaving it
            // detached, so it can't send a stale store after we leave below.
            task.abort();
            warn!(
                "Timed out waiting for the background offset-commit task to stop for consumer: {}, aborted",
                self.consumer_name
            );
        }

        // Wakes the store task, which sends what is still queued and exits on the shutdown flag.
        self.store_offset_notify.notify_one();
        if let Some(mut task) = self.store_offset_task.take()
            && time::timeout(self.offset_drain_timeout.get_duration(), &mut task)
                .await
                .is_err()
        {
            task.abort();
            warn!(
                "Timed out draining pending consumer offset stores for consumer: {}, aborted",
                self.consumer_name
            );
        }

        if self.auto_commit != AutoCommit::Disabled {
            for (partition_id, consumed_offset) in self.state.last_consumed_offsets() {
                let stored_offset = self.state.get_last_stored_offset(partition_id).unwrap_or(0);
                if consumed_offset > stored_offset {
                    trace!(
                        "Flushing final offset: {consumed_offset} for partition: {partition_id}, stream: {}, topic: {}",
                        self.stream_id, self.topic_id
                    );
                    let _ = self
                        .state
                        .store_consumer_offset(partition_id, consumed_offset, self.allow_replay)
                        .await;
                }
            }
        }

        if self.is_consumer_group && self.joined_consumer_group.load(ORDERING) {
            let group_id = self.consumer.id.clone();
            trace!(
                "Leaving consumer group: {group_id} for stream: {}, topic: {}",
                self.stream_id, self.topic_id
            );

            let client = self.client.read().await;
            // Cleared either way: this consumer is torn down regardless of
            // whether the broker confirmed the leave.
            self.joined_consumer_group.store(false, ORDERING);
            if let Err(error) = client
                .leave_consumer_group(&self.stream_id, &self.topic_id, &group_id)
                .await
            {
                // Expected on clean teardown after an explicit leave (member
                // not found) or when the group was deleted underneath the
                // consumer, so this is debug, not a warning.
                debug!(
                    "Failed to leave consumer group: {group_id} for stream: {}, topic: {}. {error}",
                    self.stream_id, self.topic_id
                );
            }
        }

        if let Some(task) = self.events_task.take() {
            task.abort();
        }

        info!("Consumer: {} has been shut down.", self.consumer_name);
        Ok(())
    }
}

/// Stops the background tasks. Commits already queued still go out, nothing else is flushed and
/// the consumer group is not left. Await [`IggyConsumer::shutdown`] first, see
/// [Shutting down](IggyConsumer#shutting-down).
impl Drop for IggyConsumer {
    fn drop(&mut self) {
        self.shutdown.store(true, ORDERING);
        self.background_commit_notify.notify_one();
        self.store_offset_notify.notify_one();
        if let Some(task) = self.events_task.take() {
            task.abort();
        }
        trace!(
            "Consumer {} has been dropped, shutdown signal sent",
            self.consumer_name
        );
    }
}

pub(crate) mod deferred;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_wrappers::client_wrapper::ClientWrapper;
    use crate::clients::consumer_builder::IggyConsumerBuilder;
    use crate::tcp::tcp_client::TcpClient;
    use iggy_common::Aes256GcmEncryptor;
    use iggy_common::locking::IggyRwLockFn;
    use std::str::FromStr;
    use std::task::Waker;
    use std::time::Duration;
    use tokio::time::timeout;

    const POLL_RETRY_INTERVAL: Duration = Duration::from_millis(10);
    const POLL_TIMEOUT: Duration = Duration::from_secs(2);

    fn builder_for(consumer: Consumer) -> IggyConsumerBuilder {
        IggyConsumerBuilder::new(
            IggyRwLock::new(ClientWrapper::Tcp(TcpClient::default())),
            "consumer".to_owned(),
            consumer,
            Identifier::numeric(1).unwrap(),
            Identifier::numeric(1).unwrap(),
            None,
            None,
        )
    }

    fn builder() -> IggyConsumerBuilder {
        builder_for(Consumer::new(Identifier::numeric(1).unwrap()))
    }

    async fn assert_stream_terminates_after_shutdown(consumer: Consumer) {
        let mut consumer = builder_for(consumer)
            .partition(Some(1))
            .batch_length(1)
            .auto_commit(AutoCommit::Disabled)
            .build();
        consumer.initialized = true;
        consumer.buffered_messages.extend([
            IggyMessage::from_str("a").unwrap(),
            IggyMessage::from_str("b").unwrap(),
        ]);
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(
            Pin::new(&mut consumer).poll_next(&mut context),
            Poll::Ready(Some(Ok(_)))
        ));

        consumer.shutdown().await.unwrap();

        assert!(consumer.buffered_messages.is_empty());
        assert!(matches!(
            Pin::new(&mut consumer).poll_next(&mut context),
            Poll::Ready(None)
        ));
    }

    #[tokio::test]
    async fn standalone_consumer_should_stop_yielding_messages_after_shutdown() {
        assert_stream_terminates_after_shutdown(Consumer::new(Identifier::numeric(1).unwrap()))
            .await;
    }

    #[tokio::test]
    async fn consumer_group_should_stop_yielding_messages_after_shutdown() {
        assert_stream_terminates_after_shutdown(Consumer::group(Identifier::numeric(1).unwrap()))
            .await;
    }

    #[tokio::test]
    async fn consumer_group_should_not_create_fetch_worker_after_shutdown() {
        let mut consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .auto_commit(AutoCommit::Disabled)
            .build();
        let mut context = Context::from_waker(Waker::noop());

        consumer.shutdown().await.unwrap();

        assert!(matches!(
            Pin::new(&mut consumer).poll_next(&mut context),
            Poll::Ready(None)
        ));
        assert!(consumer.deferred_polls.is_none());
    }

    #[test]
    fn group_member_should_ignore_the_partition_set_on_the_builder() {
        let consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .build();

        assert_eq!(consumer.partition_id, None);
    }

    #[test]
    fn standalone_consumer_should_keep_the_partition_set_on_the_builder() {
        let consumer = builder().partition(Some(1)).build();

        assert_eq!(consumer.partition_id, Some(1));
    }

    fn message_at(offset: u64) -> IggyMessage {
        let mut message = IggyMessage::from_str("payload").unwrap();
        message.header.offset = offset;
        message
    }

    /// Hands over `messages` as one buffered batch read from `partition_id`.
    fn hand_over_batch(consumer: &mut IggyConsumer, partition_id: u32, messages: Vec<IggyMessage>) {
        consumer.initialized = true;
        consumer
            .state
            .current_partition_id
            .store(partition_id, ORDERING);
        consumer.buffered_messages = VecDeque::from(messages);
        let mut context = Context::from_waker(Waker::noop());
        while !consumer.buffered_messages.is_empty() {
            assert!(matches!(
                Pin::new(&mut *consumer).poll_next(&mut context),
                Poll::Ready(Some(Ok(_)))
            ));
        }
    }

    #[test]
    fn group_member_tracks_delivered_positions_separately_per_partition() {
        let mut consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .polling_strategy(PollingStrategy::first())
            .auto_commit(AutoCommit::Disabled)
            .build();

        hand_over_batch(&mut consumer, 3, vec![message_at(10), message_at(11)]);
        assert_eq!(consumer.get_last_consumed_offset(3), Some(11));
        assert_eq!(consumer.polling_strategy, PollingStrategy::first());

        hand_over_batch(&mut consumer, 4, vec![message_at(7)]);
        assert_eq!(consumer.get_last_consumed_offset(4), Some(7));
        assert_eq!(consumer.get_last_consumed_offset(3), Some(11));
    }

    #[test]
    fn default_consumer_does_not_commit_delivered_messages() {
        let mut consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .auto_commit(AutoCommit::Disabled)
            .build();

        hand_over_batch(&mut consumer, 3, vec![message_at(10), message_at(11)]);

        assert_eq!(consumer.auto_commit, AutoCommit::Disabled);
        assert!(consumer.pending_commits.is_empty());
        assert_eq!(consumer.get_last_stored_offset(3), None);
    }

    /// Polls once as a group member on a client that is not connected. The outcome must be an
    /// error, never an endless wait for a join.
    async fn poll_once_as_group_member(
        builder: IggyConsumerBuilder,
    ) -> Option<Result<ReceivedMessage, IggyError>> {
        let mut consumer = builder
            .polling_retry_interval(NonZeroIggyDuration::new(POLL_RETRY_INTERVAL).unwrap())
            .build();
        consumer.initialized = true;
        timeout(POLL_TIMEOUT, consumer.next())
            .await
            .expect("a group member must poll or report an error instead of waiting for a join")
    }

    #[tokio::test]
    async fn group_member_without_auto_join_should_poll_instead_of_waiting_for_the_join() {
        let builder = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .do_not_auto_join_consumer_group();

        assert!(matches!(
            poll_once_as_group_member(builder).await,
            Some(Err(_))
        ));
    }

    #[tokio::test]
    async fn group_member_should_report_a_failed_join_as_a_poll_error() {
        let builder = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .auto_join_consumer_group();

        assert!(matches!(
            poll_once_as_group_member(builder).await,
            Some(Err(_))
        ));
    }

    #[tokio::test]
    async fn init_should_reject_an_encryptor_with_auto_commit_on_polling() {
        let encryptor = Arc::new(EncryptorKind::Aes256Gcm(
            Aes256GcmEncryptor::new(&[1; 32]).unwrap(),
        ));
        for auto_commit in [
            AutoCommit::When(AutoCommitWhen::PollingMessages),
            AutoCommit::IntervalOrWhen(
                NonZeroIggyDuration::ONE_SECOND,
                AutoCommitWhen::PollingMessages,
            ),
        ] {
            let mut consumer = builder()
                .encryptor(encryptor.clone())
                .auto_commit(auto_commit)
                .build();

            assert!(
                matches!(consumer.init().await, Err(IggyError::InvalidConfiguration)),
                "{auto_commit:?} must be rejected with an encryptor"
            );
        }

        let mut consumer = builder()
            .encryptor(encryptor)
            .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingEachMessage))
            .build();

        assert!(!matches!(
            consumer.init().await,
            Err(IggyError::InvalidConfiguration)
        ));
    }

    #[test]
    fn send_store_offset_should_keep_the_latest_offset_per_partition() {
        let mut consumer = builder().build();
        consumer.initialized = true;

        consumer.send_store_offset(1, 5);
        consumer.send_store_offset(1, 7);
        consumer.send_store_offset(2, 3);

        let mut queued: Vec<(u32, u64)> = consumer
            .pending_commits
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();
        queued.sort_unstable();
        assert_eq!(queued, vec![(1, 7), (2, 3)]);
    }

    #[tokio::test]
    async fn should_accept_every_auto_commit_mode() {
        for auto_commit in [
            AutoCommit::Disabled,
            AutoCommit::Interval(NonZeroIggyDuration::ONE_SECOND),
            AutoCommit::When(AutoCommitWhen::PollingMessages),
            AutoCommit::After(AutoCommitAfter::ConsumingAllMessages),
        ] {
            let mut consumer = builder().auto_commit(auto_commit).build();

            let error = consumer.init().await.err();

            assert!(
                !matches!(error, Some(IggyError::InvalidConfiguration)),
                "{auto_commit:?} must be accepted"
            );
        }
    }
}
