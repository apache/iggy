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
use crate::driver::{operation_deadline, with_deadline};
use crate::links::OPERATION_TIMEOUT;
use dashmap::DashMap;
use futures::Stream;
use futures_util::StreamExt;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_common::ConsumerPosition;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    AssignmentVersion, Consumer, ConsumerGroupClientState, ConsumerKind, DeferredPollOptions,
    DiagnosticEvent, EncryptorKind, IdKind, Identifier, IggyDuration, IggyError, IggyMessage,
    NonZeroIggyDuration, PolledMessages, PollingKind, PollingStrategy, group_cache_key,
};
use iggy_common::{Client, ConsumerGroupClient, ConsumerOffsetClient, StreamClient, TopicClient};
use std::collections::{BTreeSet, VecDeque};
use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
use std::task::{Context, Poll};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::Instant;
use tracing::{debug, error, info, trace, warn};

const ORDERING: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;

#[derive(Clone, Copy)]
enum OffsetMutation {
    Store { partition_id: u32, offset: u64 },
    StorePosition(ConsumerPosition),
    Delete { partition_id: Option<u32> },
}

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
    last_consumed_offsets: Arc<DashMap<u32, ConsumerPosition>>,
    last_stored_offsets: Arc<DashMap<u32, ConsumerPosition>>,
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

    /// Returns the last offset handed over for this partition, or `None` until
    /// a message from the partition has been consumed.
    /// To get the current partition ID use `partition_id()`
    pub fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        let offset = self.last_consumed_offsets.get(&partition_id)?;
        Some(offset.offset)
    }

    /// Returns the last offset successfully stored for this partition, or
    /// `None` until this consumer has stored one.
    /// To get the current partition ID use `partition_id()`
    pub fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        let offset = self.last_stored_offsets.get(&partition_id)?;
        Some(offset.offset)
    }

    /// Stores the consumer offset on the server either for the current partition or the provided partition ID.
    ///
    /// It commits under the context of the latest message consumed from that partition, see
    /// [`IggyConsumer::store_offset`].
    pub async fn store_offset(
        &self,
        offset: u64,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        let partition_id = partition_id.unwrap_or_else(|| self.partition_id());
        let position = self
            .last_consumed_offsets
            .get(&partition_id)
            .map(|last| ConsumerPosition { offset, ..*last });
        if let Some(position) = position {
            return self.store_position(position).await;
        }
        // Nothing was consumed from this partition, so there is no captured context to keep.
        let position = ConsumerPosition {
            partition_id,
            offset,
            context: PartitionContext::default(),
        };
        self.mutate_offset(OffsetMutation::Store {
            partition_id,
            offset,
        })
        .await?;
        self.last_stored_offsets.insert(partition_id, position);
        Ok(())
    }

    /// Deletes the consumer offset on the server either for the current partition or the provided partition ID.
    pub async fn delete_offset(&self, mut partition_id: Option<u32>) -> Result<(), IggyError> {
        // `None` is only resolved server-side for consumer groups. For a standalone consumer
        // explicitly assign the current partition_id.
        if partition_id.is_none() && !self.is_consumer_group {
            partition_id = Some(self.partition_id());
        }
        // Cancellation can hide a committed delete, so a later store must reach the server.
        if let Some(partition_id) = partition_id {
            self.last_stored_offsets.remove(&partition_id);
        } else {
            self.last_stored_offsets.clear();
        }
        self.mutate_offset(OffsetMutation::Delete { partition_id })
            .await
    }

    /// Store the exact position delivered to the application, even after another poll or rebalance.
    pub async fn store_position(&self, position: ConsumerPosition) -> Result<(), IggyError> {
        self.store_consumer_position(position, self.allow_replay)
            .await
    }

    async fn store_consumer_position(
        &self,
        position: ConsumerPosition,
        allow_replay: bool,
    ) -> Result<(), IggyError> {
        if !allow_replay && self.stored_offset_covers(position) {
            return Ok(());
        }
        self.mutate_offset(OffsetMutation::StorePosition(position))
            .await?;
        self.last_stored_offsets
            .insert(position.partition_id, position);
        Ok(())
    }

    async fn mutate_offset(&self, mutation: OffsetMutation) -> Result<(), IggyError> {
        super::with_client!(self, client => {
            let deadline = operation_deadline();
            match self.send_offset_mutation(&client, mutation).await {
                Err(IggyError::LaneBlocked(op)) => {
                    if matches!(
                        client.resolve_operation(op, deadline.into_std()).await,
                        Err(IggyError::OperationUncertain(_))
                    ) {
                        return Err(IggyError::LaneBlocked(op));
                    }
                    // The blocker has its own outcome. Re-entering the lane either
                    // admits this new mutation or reports that it remains blocked.
                    self.send_offset_mutation(&client, mutation).await
                }
                result => result,
            }
        })
    }

    async fn send_offset_mutation(
        &self,
        client: &ClientWrapper,
        mutation: OffsetMutation,
    ) -> Result<(), IggyError> {
        match mutation {
            OffsetMutation::Store {
                partition_id,
                offset,
            } => {
                client
                    .store_consumer_offset(
                        &self.consumer,
                        &self.stream_id,
                        &self.topic_id,
                        Some(partition_id),
                        offset,
                    )
                    .await
            }
            OffsetMutation::StorePosition(position) => {
                client
                    .store_consumer_position(
                        &self.consumer,
                        &self.stream_id,
                        &self.topic_id,
                        position,
                    )
                    .await
            }
            OffsetMutation::Delete { partition_id } => {
                client
                    .delete_consumer_offset(
                        &self.consumer,
                        &self.stream_id,
                        &self.topic_id,
                        partition_id,
                    )
                    .await
            }
        }
    }

    /// An uncaptured scalar store cannot prove coverage in any partition history.
    fn stored_offset_covers(&self, position: ConsumerPosition) -> bool {
        self.last_stored_offsets
            .get(&position.partition_id)
            .is_some_and(|stored| {
                stored.context.incarnation != 0
                    && position.context.incarnation != 0
                    && same_incarnation_and_owner(stored.context, position.context)
                    && position.offset <= stored.offset
            })
    }

    /// The commit tasks have no caller to hand a failure to, so it is logged here. Replay governs
    /// reading only, so they never move a stored offset back.
    async fn store_in_background(&self, position: ConsumerPosition) {
        if let Err(error) = self.store_consumer_position(position, false).await {
            error!(
                "Failed to store offset: {} for consumer: {}, partition ID: {}, topic: {}, stream: {}. {error}",
                position.offset,
                self.consumer,
                position.partition_id,
                self.topic_id,
                self.stream_id
            );
        }
    }

    fn last_consumed_positions(&self) -> Vec<ConsumerPosition> {
        self.last_consumed_offsets
            .iter()
            .map(|entry| *entry.value())
            .collect()
    }

    /// Stores the delivered position of every partition where it differs from the record.
    ///
    /// Only for auto-commit-on-poll. The poll commits each whole batch, so the record holds
    /// the batch end while the application can still be behind it, and the store must bypass
    /// the skip rule to move the server offset back. `assignment` limits a group member to
    /// the partitions it still owns; each store also carries the captured partition context.
    async fn store_delivered_offsets(
        &self,
        assignment: Option<&(Arc<ConsumerGroupClientState>, String)>,
    ) {
        let owned = assignment.map(|(groups, key)| groups.assignment_snapshot(key).1);
        for position in self.unstored_delivered_offsets(owned.as_deref()) {
            if let Err(error) = self.store_consumer_position(position, true).await {
                error!("Failed to store delivered position: {position:?}. {error}");
            }
        }
    }

    /// The server committed `undelivered`, but the application never took it.
    /// Recording each commit lets [`Self::store_delivered_offsets`] move the
    /// offset back to the last delivered message. Without a delivered message,
    /// a `Next` consumer stores the offset before the first undelivered one,
    /// because its next run resumes after the stored offset.
    async fn rewind_undelivered_offsets(
        &self,
        undelivered: Vec<PolledMessages>,
        assignment: Option<&(Arc<ConsumerGroupClientState>, String)>,
        resumes_next: bool,
    ) {
        let owned = assignment.map(|(groups, key)| groups.assignment_snapshot(key).1);
        let mut rewound = BTreeSet::new();
        for polled in undelivered {
            let partition_id = polled.partition_id;
            let (Some(first), Some(last)) = (polled.messages.first(), polled.messages.last())
            else {
                continue;
            };
            if owned
                .as_ref()
                .is_some_and(|owned| !owned.contains(&partition_id))
            {
                continue;
            }
            if self.last_consumed_offsets.contains_key(&partition_id) {
                self.last_stored_offsets.insert(
                    partition_id,
                    ConsumerPosition {
                        partition_id,
                        offset: last.header.offset,
                        context: polled.context,
                    },
                );
                continue;
            }
            // Batches arrive in offset order, so the first one of a partition
            // holds the oldest undelivered message.
            if !resumes_next || !rewound.insert(partition_id) {
                continue;
            }
            let result = match first.header.offset.checked_sub(1) {
                Some(offset) => {
                    let position = ConsumerPosition {
                        partition_id,
                        offset,
                        context: polled.context,
                    };
                    self.store_consumer_position(position, true).await
                }
                None => {
                    self.last_stored_offsets.remove(&partition_id);
                    self.mutate_offset(OffsetMutation::Delete {
                        partition_id: Some(partition_id),
                    })
                    .await
                }
            };
            if let Err(error) = result {
                error!(
                    "Failed to rewind the offset before undelivered messages in partition: {partition_id}. {error}"
                );
            }
        }
    }

    fn unstored_delivered_offsets(&self, owned: Option<&[u32]>) -> Vec<ConsumerPosition> {
        self.last_consumed_positions()
            .into_iter()
            .filter(|position| {
                owned.is_none_or(|owned| owned.contains(&position.partition_id))
                    && self
                        .last_stored_offsets
                        .get(&position.partition_id)
                        .is_none_or(|stored| *stored != *position)
            })
            .collect()
    }
}

/// Offsets are comparable only within one partition incarnation and owner. The metadata frontier
/// is left out: another group's ownership change can advance it between two polls of the same
/// partition.
fn same_incarnation_and_owner(left: PartitionContext, right: PartitionContext) -> bool {
    left.incarnation == right.incarnation && left.owner_generation == right.owner_generation
}

/// Reads a topic as a stream of messages, using long polling and bounded prefetch.
///
/// Call [`Self::init`] before reading and [`Self::shutdown`] before dropping the consumer.
/// A standalone consumer reads one partition. A group member reads its assignment, and its
/// assignment refresh continues even when the application stops reading. Fetches share the
/// client's multiplexed connections with control traffic. At most 16 fetches and readiness
/// waits run concurrently per consumer, within the client's limit of 64 deferred requests.
///
/// A poll returns when at least `min_count` messages are available (one by default),
/// its byte limit is reached, or `max_wait` expires. `batch_length` is the maximum
/// message count. The separate request timeout covers routing, waiting and reading.
/// There is no normal polling interval; errors and reconnects retain retry backoff.
/// A poll that is still refused when its request timeout ends, for example by a
/// server at its deferred poll capacity, reaches the application as an error, and
/// the consumer polls that partition again after
/// [`polling_retry_interval`](crate::prelude::IggyConsumerBuilder::polling_retry_interval).
///
/// # Positions and commits
///
/// The initial polling strategy is resolved separately for each partition. `Next`
/// starts after the stored offset, or at zero when no offset exists. Subsequent
/// fetches use explicit offsets, so a lost reply can be retried without advancing
/// past unseen messages. Reassignment discards prefetched data. A partition that a
/// rebalance leaves with its group member continues after the last message that the
/// application took. The member resolves the starting position again for a partition
/// that it gets back from another member. After the client starts a new session, the
/// member resolves it again for every partition. Messages already delivered are
/// filtered unless replay is enabled. Delivery and durable processing progress are
/// separate.
///
/// The default is [`AutoCommit::Disabled`]. Store offsets only after successfully
/// processing messages, in partition order. Fetching ahead never commits under this
/// default. Opting into auto-commit can acknowledge messages before processing.
///
/// Under [`AutoCommitWhen::PollingMessages`] the server commits every batch that it
/// reads, so the consumer does not fetch ahead. It fetches one batch when the
/// application asks for a message and the buffer is empty. A group member first waits
/// until one of its partitions holds a message, with one readiness wait per partition
/// primary. A batch that the server committed reaches the application even after a
/// reassignment. After its first batch, a partition continues from an explicit offset,
/// so the consumer retries a lost reply from the same offset. If the consumer shuts down
/// first, [`shutdown`](Self::shutdown) moves the offset back over the messages that the
/// application did not take, also for a fetch in flight. A lost reply to the first `Next`
/// poll of a partition can still skip its batch. So can a lost reply whose partition gets
/// a new owner before the retry. A new session of the client gives every partition a new
/// owner.
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
///     consumer.store_position(received.position()).await?;
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
/// # Encryption
///
/// Encryption failures are returned without advancing the fetch position. Storing the
/// offset of a message that never decrypts, which the log names with its partition,
/// moves the fetch position past it. Polling
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
    fetch_demand: Arc<AtomicBool>,
    buffered_generation: u64,
    buffered_assignment: Option<AssignmentVersion>,
    /// The group state changes at which the buffered batch was still current.
    checked_assignment: Option<u64>,
    assignment_state: Option<(Arc<ConsumerGroupClientState>, String)>,
    buffered_capacity: Option<deferred::FetchCapacity>,
    initialized: bool,
    uninitialized_poll_warned: bool,
    shutdown: Arc<AtomicBool>,
    can_poll: Arc<AtomicBool>,
    client: IggyRwLock<ClientWrapper>,
    consumer_name: String,
    consumer: Arc<Consumer>,
    is_consumer_group: bool,
    joined_consumer_group: Arc<AtomicBool>,
    uncertain_consumer_group_operation: Arc<Mutex<Option<u64>>>,
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
    buffered_messages: VecDeque<IggyMessage>,
    buffered_context: PartitionContext,
    buffered_current_offset: u64,
    encryptor: Option<Arc<EncryptorKind>>,
    /// The latest offset each message trigger asked to commit, per partition. The store task
    /// drains it, so a burst of triggers costs one round trip per partition instead of one each.
    pending_commits: Arc<DashMap<u32, ConsumerPosition>>,
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
            fetch_demand: Arc::new(AtomicBool::new(false)),
            buffered_generation: 0,
            buffered_assignment: None,
            checked_assignment: None,
            assignment_state: None,
            buffered_capacity: None,
            initialized: false,
            uninitialized_poll_warned: false,
            shutdown: Arc::new(AtomicBool::new(false)),
            is_consumer_group,
            joined_consumer_group: Arc::new(AtomicBool::new(false)),
            uncertain_consumer_group_operation: Arc::new(Mutex::new(None)),
            can_poll: Arc::new(AtomicBool::new(true)),
            client,
            consumer_name,
            consumer,
            stream_id,
            topic_id,
            partition_id,
            polling_strategy,
            state,
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
            buffered_context: PartitionContext::default(),
            buffered_current_offset: 0,
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

    /// Whether the consumer resumes after its stored offsets but never stores one itself.
    pub(crate) fn relies_on_manual_commits(&self) -> bool {
        self.auto_commit == AutoCommit::Disabled && self.polling_strategy.kind == PollingKind::Next
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

    /// Returns the partition the most recent poll response with messages came from, or `0` before
    /// the first one.
    ///
    /// For a consumer group the value changes over time, as the server hands different partitions
    /// to this member. To commit for the partition a message came from, pass
    /// [`ReceivedMessage::position`] to [`store_position()`](Self::store_position) instead.
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
    /// An offset that is not ahead of the last one stored in the same captured context is
    /// skipped and `Ok(())` is returned without a request, unless the consumer was built with
    /// [`allow_replay`](crate::prelude::IggyConsumerBuilder::allow_replay). A store without a
    /// captured context is always sent and cannot suppress a later captured position.
    ///
    /// The offset is committed under the context of the latest message consumed from that
    /// partition, or without a captured context when nothing was consumed from it. An offset taken
    /// from a message of an older incarnation or owner is therefore committed under the newer
    /// context. To commit under the exact context of a message, pass [`ReceivedMessage::position`]
    /// to [`store_position()`](Self::store_position). A refusal of the captured context, such as
    /// [`IggyError::HistoryUnavailable`] after the partition was recreated, repeats until a message
    /// is consumed under the new context.
    ///
    /// To start over from the first message, delete the stored offset with
    /// [`delete_offset()`](Self::delete_offset) after [`shutdown()`](Self::shutdown) and build a new
    /// consumer. A new consumer built with [`PollingStrategy::offset()`] re-reads from any point.
    ///
    /// On binary transports, a retained operation blocking this partition is resolved before
    /// the requested store. Recovery and the new store share one operation deadline. The store
    /// goes on after a proved commit or committed rejection of that operation, and the client keeps
    /// the proved outcome for the caller that owns the operation, see
    /// [`Client::resolve_operation`].
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

    /// Commit a message's captured partition incarnation and owner using
    /// [`ReceivedMessage::position`]. A position from a deleted incarnation or a previous owner is
    /// refused without updating replacement progress.
    ///
    /// # Errors
    /// Uses the recovery and error contract of [`Self::store_offset`].
    pub async fn store_position(&self, position: ConsumerPosition) -> Result<(), IggyError> {
        self.state.store_position(position).await
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
    /// auto-commit or [`shutdown()`](Self::shutdown) can store that position again. When starting
    /// over, call it after [`shutdown()`](Self::shutdown).
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
    /// read back from the server.
    /// With auto-commit-on-poll, the server can be ahead by the batch that is on its way to
    /// this consumer.
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
    /// down. After every other event, polling is enabled only while the client is signed in at
    /// that moment, because an event can arrive after one that reports a later state. A client
    /// that only the caller can bring back is polled anyway, so each poll fails with the reason.
    /// - [`DiagnosticEvent::Connected`], [`DiagnosticEvent::Disconnected`] and
    ///   [`DiagnosticEvent::SignedOut`] forget the group membership. A group member whose
    ///   membership is gone rejoins its group on the next poll, before the request goes out. A
    ///   failed rejoin is yielded as a poll error and tried again on the poll after.
    /// - [`DiagnosticEvent::Shutdown`] terminates the background task listening for lifecycle
    ///   changes, and every later poll fails with [`IggyError::ClientShutdown`]. It does not flush
    ///   in-flight commits; that only happens when [`shutdown()`](Self::shutdown) itself is called.
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
    /// ([`get_last_stored_offset()`](Self::get_last_stored_offset)) in the same captured context.
    /// An earlier scalar store without a captured context cannot suppress these commits.
    /// Combined with auto-commit-on-poll, the interval task instead stores the delivered position
    /// wherever it differs from that record. This moves the server offset back to the delivered
    /// position while the rest of the fetched batch is already committed. A group member does this
    /// only for the partitions of its current assignment.
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidConfiguration`] when the consumer has an encryptor and
    ///   [`auto_commit()`](crate::prelude::IggyConsumerBuilder::auto_commit) is
    ///   [`AutoCommitWhen::PollingMessages`], checked before anything is sent. See
    ///   [Encryption](IggyConsumer#encryption).
    /// - [`IggyError::InvalidConfiguration`] when the prefetch limits cannot hold one reply, and
    ///   the error of [`DeferredPollOptions::validate`] when the poll options do not fit
    ///   `batch_length`. Both are logged with the values that failed.
    /// - [`IggyError::StreamNameNotFound`], [`IggyError::StreamIdNotFound`] or [`IggyError::TopicNameNotFound`],
    ///   [`IggyError::TopicIdNotFound`] when the stream or the topic still does not exist once the retries are
    ///   exhausted.
    /// - [`IggyError::ConsumerGroupNameNotFound`] when the consumer group does not exist
    ///   and its auto creation is disabled.
    /// - [`IggyError::FeatureUnavailable`] for a consumer group over HTTP, before the join.
    /// - Any error returned by the server while looking up the stream or the topic, or
    ///   while creating or joining the consumer group. Such an error ends initialization
    ///   immediately instead of consuming a retry.
    pub async fn init(&mut self) -> Result<(), IggyError> {
        if let Err(error) = self.poll_options.validate(self.batch_length) {
            error!(
                "Consumer: {} has poll options: {:?} that do not fit batch length: {}. {error}",
                self.consumer_name, self.poll_options, self.batch_length
            );
            return Err(error);
        }
        if self.prefetch_slots() == 0 {
            error!(
                "Consumer: {} cannot reserve one reply. Prefetch messages: {} must cover batch length: {}, and prefetch bytes: {} must cover poll max bytes: {}.",
                self.consumer_name,
                self.prefetch_messages,
                self.batch_length,
                self.prefetch_bytes,
                self.poll_options.max_bytes
            );
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

        // A retried init() after a failed join must not leave the earlier task behind.
        if let Some(previous) = self.events_task.replace(self.subscribe_events().await) {
            previous.abort();
        }
        // A transport without group state must fail before the join, or the
        // member would hold partitions that it never reads.
        let group_state = if self.is_consumer_group {
            Some(self.client.read().await.deferred_group_state().await?)
        } else {
            None
        };
        self.init_consumer_group().await?;
        self.assignment_state = group_state.map(|state| {
            (
                state,
                group_cache_key(&self.stream_id, &self.topic_id, &self.consumer.id),
            )
        });

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
        let commit_on_poll = self.auto_commit_after_polling;
        let assignment = self.assignment_state.clone();
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
                if commit_on_poll {
                    state.store_delivered_offsets(assignment.as_ref()).await;
                    continue;
                }
                for position in state.last_consumed_positions() {
                    state.store_in_background(position).await;
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
                    let Some((_, position)) = pending_commits.remove(&partition_id) else {
                        continue;
                    };
                    state.store_in_background(position).await;
                }
                if shutdown.load(ORDERING) && pending_commits.is_empty() {
                    break;
                }
            }
        })
    }

    /// Queues a commit for the store task. A later offset for the same partition replaces a
    /// queued one that has not been sent yet.
    pub(crate) fn send_store_offset(&self, position: ConsumerPosition) {
        if !self.initialized || self.shutdown.load(ORDERING) {
            error!(
                ?position,
                consumer = self.consumer_name,
                "Offset was not queued because the consumer is not running"
            );
            return;
        }
        self.pending_commits.insert(position.partition_id, position);
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
            &self.consumer_name,
            self.joined_consumer_group.clone(),
            self.uncertain_consumer_group_operation.clone(),
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

        let client = self.client.clone();
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
                    }
                    DiagnosticEvent::Disconnected => {
                        joined_consumer_group.store(false, ORDERING);
                        warn!("Disconnected from the server");
                    }
                    DiagnosticEvent::SignedIn => {}
                    DiagnosticEvent::SignedOut => {
                        joined_consumer_group.store(false, ORDERING);
                    }
                }
                // Links publish from several tasks, so an event can arrive after one that
                // reports a later state. The state of the client decides instead.
                let authenticated = client.read().await.is_authenticated().await;
                can_poll.store(authenticated, ORDERING);
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
        consumer_name: &str,
        joined_consumer_group: Arc<AtomicBool>,
        uncertain_consumer_group_operation: Arc<Mutex<Option<u64>>>,
    ) -> Result<(), IggyError> {
        if joined_consumer_group.load(ORDERING) {
            return Ok(());
        }

        let mut uncertain_operation = uncertain_consumer_group_operation.lock().await;
        let client = client.read().await;
        if let Some(op) = *uncertain_operation {
            let result = client
                .resolve_operation(op, operation_deadline().into_std())
                .await;
            if matches!(result, Err(IggyError::OperationUncertain(_))) {
                return result;
            }
            *uncertain_operation = None;
            match result {
                Ok(())
                | Err(IggyError::OperationNotRetained(_))
                | Err(IggyError::ConsumerGroupNameAlreadyExists(_, _)) => {}
                Err(error) => return Err(error),
            }
        }
        // A proved join can belong to an earlier session, so establish membership again.
        let consumer_group_id = Identifier::named(consumer_name)?;
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
                    consumer_name.to_owned(),
                    topic_identifier,
                ));
            }

            info!(
                "Creating consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}"
            );
            match client
                .create_consumer_group(&stream_id, &topic_id, consumer_name)
                .await
            {
                Ok(_) => {}
                Err(IggyError::ConsumerGroupNameAlreadyExists(_, _)) => {}
                Err(error) => {
                    if let IggyError::OperationUncertain(op) = &error {
                        *uncertain_operation = Some(*op);
                    }
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
            if let IggyError::OperationUncertain(op) = &error {
                *uncertain_operation = Some(*op);
            }
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
            .min(crate::driver::MAX_DEFERRED_REQUESTS_PER_CONSUMER as u32) as usize
    }

    /// Locks the group state only after it changed since the buffered batch
    /// was last found current.
    fn buffered_assignment_is_current(&mut self) -> bool {
        let Some((state, key)) = &self.assignment_state else {
            return true;
        };
        let changes = state.assignment_changes();
        if self.checked_assignment == Some(changes) {
            return true;
        }
        let current = self.buffered_assignment.is_some_and(|assignment| {
            state.is_current_assignment(key, assignment, self.state.partition_id())
        });
        if current {
            self.checked_assignment = Some(changes);
        }
        current
    }
}

/// A single message handed over by an [`IggyConsumer`].
#[derive(Debug)]
pub struct ReceivedMessage {
    /// Partition incarnation and owner captured by the poll. Retain it for delayed manual commits.
    pub context: PartitionContext,
    /// The message itself, with its payload already decrypted when the client uses an encryptor.
    ///
    /// Commit [`Self::position`] with [`IggyConsumer::store_position`] to retain
    /// the incarnation and owner even if another poll observes a newer context.
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
    /// The position to commit for this message with [`IggyConsumer::store_position`]. It keeps the
    /// partition incarnation and owner that delivered the message, so a commit after a recreation
    /// or an ownership change is refused instead of landing on the newer partition.
    #[must_use]
    pub const fn position(&self) -> ConsumerPosition {
        ConsumerPosition {
            partition_id: self.partition_id,
            offset: self.message.header.offset,
            context: self.context,
        }
    }

    /// Creates a received message from a message, the partition head at poll time and the
    /// partition it was read from.
    pub fn new(
        message: IggyMessage,
        current_offset: u64,
        partition_id: u32,
        context: PartitionContext,
    ) -> Self {
        Self {
            context,
            message,
            current_offset,
            partition_id,
        }
    }
}

/// Yields messages one at a time, from the buffer first and from a fresh poll once it is empty.
///
/// See [`IggyConsumer`] for how a poll waits, and [Positions and commits](IggyConsumer#positions-and-commits)
/// for where reading continues after an error.
impl Stream for IggyConsumer {
    type Item = Result<ReceivedMessage, IggyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
            return Poll::Ready(Some(Err(IggyError::InvalidConfiguration)));
        }
        loop {
            // The server committed a batch of a committing fetch, so it stays.
            if !self.auto_commit_after_polling
                && !self.buffered_messages.is_empty()
                && (self.buffered_generation != self.fetch_generation.load(ORDERING)
                    || !self.buffered_assignment_is_current())
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
                        .last_consumed_offsets
                        .get(&partition_id)
                        .is_some_and(|consumed| {
                            same_incarnation_and_owner(consumed.context, self.buffered_context)
                                && offset <= consumed.offset
                        })
                {
                    continue;
                }
                self.state.last_consumed_offsets.insert(
                    partition_id,
                    ConsumerPosition {
                        partition_id,
                        offset,
                        context: self.buffered_context,
                    },
                );
                let last = self.buffered_messages.is_empty();
                if self.store_offset_after_each_message
                    || (self.store_after_every_nth_message > 0
                        && offset % self.store_after_every_nth_message == 0)
                    || (self.store_offset_after_all_messages && last)
                {
                    self.send_store_offset(ConsumerPosition {
                        partition_id,
                        offset: message.header.offset,
                        context: self.buffered_context,
                    });
                }
                if last {
                    self.buffered_capacity = None;
                }
                return Poll::Ready(Some(Ok(ReceivedMessage::new(
                    message,
                    self.buffered_current_offset,
                    partition_id,
                    self.buffered_context,
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
                Poll::Pending => {
                    if self.auto_commit_after_polling && !self.fetch_demand.swap(true, ORDERING) {
                        self.fetch_notify.notify_one();
                    }
                    return Poll::Pending;
                }
                Poll::Ready(None) => {
                    self.deferred_polls = None;
                    return Poll::Ready(Some(Err(IggyError::ClientShutdown)));
                }
                Poll::Ready(Some(batch)) => {
                    if batch.generation != self.fetch_generation.load(ORDERING)
                        && (batch.result.is_err() || !self.auto_commit_after_polling)
                    {
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
                            if self.auto_commit_after_polling
                                && let Some(last) = polled.messages.last()
                            {
                                self.state.last_stored_offsets.insert(
                                    polled.partition_id,
                                    ConsumerPosition {
                                        partition_id: polled.partition_id,
                                        offset: last.header.offset,
                                        context: polled.context,
                                    },
                                );
                            }
                            self.buffered_context = polled.context;
                            self.buffered_current_offset = polled.current_offset;
                            self.buffered_messages = polled.messages.into();
                            self.buffered_generation = batch.generation;
                            self.buffered_assignment = batch.assignment;
                            self.checked_assignment = None;
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
    ///   forcing it to abort. Under auto-commit-on-poll, a fetch in flight first gets the same
    ///   time to hand its batch over. The store below then moves the offset back over it.
    /// - commit the reading position of every partition where it is ahead of this consumer's own
    ///   record of what it stored, unless [`auto_commit()`] is [`AutoCommit::Disabled`]. Under
    ///   auto-commit-on-poll, the poll admitted progress for the whole batch. This
    ///   store requests the last message handed over, so the next run resumes after it when the
    ///   store succeeds. On binary transports, final stores and any blocker recovery share one
    ///   operation deadline. HTTP retains its configured request timeout.
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

        let grace = self
            .auto_commit_after_polling
            .then(|| self.offset_drain_timeout.get_duration());
        let undelivered = match self.deferred_polls.take() {
            Some(polls) => polls.stop(grace).await,
            None => Vec::new(),
        };
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

        if self.relies_on_manual_commits()
            && self.state.last_stored_offsets.is_empty()
            && !self.state.last_consumed_offsets.is_empty()
        {
            warn!(
                "Consumer: {} delivered messages but stored no offset, so the next run reads them again. Store offsets after processing, or enable auto-commit.",
                self.consumer_name
            );
        }

        if self.auto_commit != AutoCommit::Disabled {
            // Replay governs reading only: this flush must never move a stored offset back.
            let allow_replay = false;
            with_deadline(Instant::now() + OPERATION_TIMEOUT, async {
                if self.auto_commit_after_polling {
                    let assignment = self.assignment_state.as_ref();
                    self.state
                        .rewind_undelivered_offsets(
                            undelivered,
                            assignment,
                            self.polling_strategy.kind == PollingKind::Next,
                        )
                        .await;
                    self.state.store_delivered_offsets(assignment).await;
                    return;
                }
                for position in self.state.last_consumed_positions() {
                    if let Err(error) = self
                        .state
                        .store_consumer_position(position, allow_replay)
                        .await
                    {
                        warn!(?position, %error, "Final consumer checkpoint was not acknowledged");
                    }
                }
            })
            .await;
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
/// the consumer group is not left. Await [`IggyConsumer::shutdown`] first.
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
    use crate::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
    use crate::links::ReconnectPolicy;
    use crate::links::fake_cluster::{
        Answer, FakeCluster, TEST_BUDGET, connected, connected_with, endpoint, eventually, policy,
    };
    use crate::tcp::tcp_client::TcpClient;
    use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BATCH_MESSAGE_HEADER_SIZE, BatchHeader};
    use iggy_binary_protocol::codes::{
        CREATE_CONSUMER_GROUP_CODE, DELETE_CONSUMER_OFFSET_CODE, GET_CONSUMER_GROUP_CODE,
        JOIN_CONSUMER_GROUP_CODE, LOGIN_REGISTER_CODE, POLL_MESSAGES_DEFERRED_CODE,
        STORE_CONSUMER_OFFSET_CODE, SYNC_CONSUMER_GROUP_CODE,
    };
    use iggy_binary_protocol::consensus::EvictionReason;
    use iggy_binary_protocol::requests::consumer_offsets::{
        DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
    };
    use iggy_binary_protocol::responses::consumer_groups::{
        ConsumerGroupDetailsResponse, ConsumerGroupResponse, SyncConsumerGroupResponse,
    };
    use iggy_binary_protocol::responses::messages::{
        PartitionRouteResponse, PollMessagesResponseHeader,
    };
    use iggy_binary_protocol::{PartitionContext, WireDecode, WireEncode, WireName};
    use iggy_common::locking::IggyRwLockFn;
    use iggy_common::{
        Aes256GcmEncryptor, BinaryTransport, TcpClientReconnectionConfig, UserClient,
    };
    use std::iter::repeat_n;
    use std::str::FromStr;
    use std::task::Waker;
    use std::time::Duration;
    use tokio::time::sleep;
    use tokio::time::timeout;
    use tracing::field::Field;
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Level, Metadata, Subscriber};

    const POLL_RETRY_INTERVAL: Duration = Duration::from_millis(10);
    const POLL_TIMEOUT: Duration = Duration::from_secs(2);

    struct RejectMessages;

    impl MessageConsumer for RejectMessages {
        async fn consume(&self, _message: ReceivedMessage) -> Result<(), IggyError> {
            panic!("a blocked poll must not deliver a message")
        }
    }

    /// Records the offset of each message. With a `shutdown` sender, it stops the consumption
    /// after the first one.
    struct RecordMessages {
        offsets: std::sync::Mutex<Vec<u64>>,
        shutdown: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl MessageConsumer for RecordMessages {
        async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError> {
            self.offsets
                .lock()
                .unwrap()
                .push(message.message.header.offset);
            if let Some(shutdown) = self.shutdown.lock().unwrap().take() {
                shutdown.send(()).unwrap();
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_blocked_poll_when_consuming_callbacks_should_return_the_retained_operation() {
        const OPERATION: u64 = 42;
        let mut consumer = builder().auto_commit(AutoCommit::Disabled).build();
        consumer.initialized = true;
        consumer.deferred_polls = Some(deferred::DeferredPolls::with_error(
            &consumer,
            IggyError::LaneBlocked(OPERATION),
        ));
        let (_shutdown, receiver) = tokio::sync::oneshot::channel();

        let result = timeout(
            POLL_TIMEOUT,
            consumer.consume_messages(&RejectMessages, receiver),
        )
        .await;

        assert_eq!(result, Ok(Err(IggyError::LaneBlocked(OPERATION))));
    }

    pub(super) fn builder_for(consumer: Consumer) -> IggyConsumerBuilder {
        builder_on(TcpClient::default(), consumer)
    }

    fn builder_on(client: TcpClient, consumer: Consumer) -> IggyConsumerBuilder {
        IggyConsumerBuilder::new(
            IggyRwLock::new(ClientWrapper::Tcp(client)),
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

    pub(super) fn message_at(offset: u64) -> IggyMessage {
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

    #[tokio::test]
    async fn group_member_should_drop_the_rest_of_its_batch_once_the_assignment_changes() {
        let mut consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .auto_commit(AutoCommit::Disabled)
            .build();
        let state = Arc::new(ConsumerGroupClientState::new());
        let key = "1|1|1".to_owned();
        let identifier = Identifier::numeric(1).unwrap();
        state.register_group(
            key.clone(),
            identifier.clone(),
            identifier.clone(),
            identifier,
        );
        state.set_assignment(key.clone(), 1, vec![3]);
        consumer.assignment_state = Some((Arc::clone(&state), key.clone()));
        consumer.buffered_assignment = Some(AssignmentVersion {
            session: state.session_generation(),
            generation: 1,
        });
        consumer.initialized = true;
        consumer.state.current_partition_id.store(3, ORDERING);
        consumer.buffered_messages =
            VecDeque::from(vec![message_at(10), message_at(11), message_at(12)]);
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..2 {
            assert!(matches!(
                Pin::new(&mut consumer).poll_next(&mut context),
                Poll::Ready(Some(Ok(_)))
            ));
        }

        state.set_assignment(key, 2, vec![3]);
        assert!(Pin::new(&mut consumer).poll_next(&mut context).is_pending());
        assert!(consumer.buffered_messages.is_empty());
        assert_eq!(consumer.get_last_consumed_offset(3), Some(11));
    }

    #[tokio::test]
    async fn given_a_committed_batch_when_the_generation_or_assignment_changes_should_hand_over_the_rest()
     {
        let mut consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
            .build();
        let state = Arc::new(ConsumerGroupClientState::new());
        let key = "1|1|1".to_owned();
        let identifier = Identifier::numeric(1).unwrap();
        state.register_group(
            key.clone(),
            identifier.clone(),
            identifier.clone(),
            identifier,
        );
        state.set_assignment(key.clone(), 1, vec![3]);
        consumer.assignment_state = Some((Arc::clone(&state), key.clone()));
        consumer.buffered_assignment = Some(AssignmentVersion {
            session: state.session_generation(),
            generation: 1,
        });
        consumer.initialized = true;
        consumer.state.current_partition_id.store(3, ORDERING);
        consumer.buffered_messages =
            VecDeque::from(vec![message_at(10), message_at(11), message_at(12)]);
        assert_eq!(poll_offset(&mut consumer), Poll::Ready(Some(Ok(10))));

        consumer.fetch_generation.fetch_add(1, ORDERING);
        assert_eq!(
            poll_offset(&mut consumer),
            Poll::Ready(Some(Ok(11))),
            "the server committed the batch, so a new fetch generation keeps it"
        );
        state.set_assignment(key, 2, vec![3]);
        assert_eq!(
            poll_offset(&mut consumer),
            Poll::Ready(Some(Ok(12))),
            "the server committed the batch, so a new assignment keeps it"
        );
    }

    #[test]
    fn given_a_reread_up_to_the_delivered_offset_when_polled_should_hand_over_only_newer_messages()
    {
        const DELIVERED: u64 = 11;
        let mut consumer = builder_for(Consumer::default())
            .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
            .build();
        buffer_reread(&mut consumer, DELIVERED);

        assert_eq!(
            poll_offset(&mut consumer),
            Poll::Ready(Some(Ok(DELIVERED + 1))),
            "the application already took the messages up to {DELIVERED}"
        );
    }

    #[test]
    fn given_replay_when_a_reread_up_to_the_delivered_offset_is_polled_should_hand_it_over_again() {
        const DELIVERED: u64 = 11;
        let mut consumer = builder_for(Consumer::default())
            .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
            .allow_replay()
            .build();
        buffer_reread(&mut consumer, DELIVERED);

        for offset in DELIVERED - 1..=DELIVERED + 1 {
            assert_eq!(
                poll_offset(&mut consumer),
                Poll::Ready(Some(Ok(offset))),
                "replay hands over every message that the batch read"
            );
        }
    }

    /// Buffers a batch that reads the messages around `delivered` again, in the
    /// partition history in which the application took `delivered`.
    fn buffer_reread(consumer: &mut IggyConsumer, delivered: u64) {
        let position = position_at(delivered, 1);
        consumer.initialized = true;
        consumer
            .state
            .current_partition_id
            .store(position.partition_id, ORDERING);
        consumer
            .state
            .last_consumed_offsets
            .insert(position.partition_id, position);
        consumer.buffered_context = position.context;
        consumer.buffered_messages = VecDeque::from(vec![
            message_at(delivered - 1),
            message_at(delivered),
            message_at(delivered + 1),
        ]);
    }

    /// The offset of the message that one poll of `consumer` hands over.
    fn poll_offset(consumer: &mut IggyConsumer) -> Poll<Option<Result<u64, IggyError>>> {
        Pin::new(consumer)
            .poll_next(&mut Context::from_waker(Waker::noop()))
            .map(|polled| {
                polled.map(|received| received.map(|received| received.message.header.offset))
            })
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

    struct Served(Arc<FakeCluster>);

    impl Served {
        fn stored_offsets(&self) -> Vec<u64> {
            self.0
                .received(STORE_CONSUMER_OFFSET_CODE)
                .iter()
                .map(|request| {
                    StoreConsumerOffsetRequest::decode_from(&request.body)
                        .unwrap()
                        .offset
                })
                .collect()
        }

        fn deleted_partitions(&self) -> Vec<u32> {
            self.0
                .received(DELETE_CONSUMER_OFFSET_CODE)
                .iter()
                .map(|request| {
                    DeleteConsumerOffsetRequest::decode_from(&request.body)
                        .unwrap()
                        .partition_id
                        .unwrap()
                })
                .collect()
        }
    }

    async fn connect_to_test_server(
        partitions: Vec<u32>,
        poll_refusal: IggyError,
    ) -> (TcpClient, Served) {
        let cluster = FakeCluster::new(1);
        *cluster.route.lock().unwrap() = Some(PartitionRouteResponse {
            incarnation: 1,
            metadata_op: 1,
            view: 0,
            partitions_count: partitions.iter().copied().max().unwrap_or_default() + 1,
            primary_replica: 0,
        });
        cluster.script(
            &endpoint(0),
            SYNC_CONSUMER_GROUP_CODE,
            [Answer::Body(
                SyncConsumerGroupResponse {
                    generation: 1,
                    partitions,
                }
                .to_bytes()
                .to_vec(),
            )],
        );
        cluster.script(
            &endpoint(0),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Refuse(poll_refusal)],
        );
        let links = connected(&cluster, 1).await;
        (TcpClient::from_links(links), Served(cluster))
    }

    /// A position delivered at `offset` by a poll that saw the metadata frontier `metadata_op`.
    fn position_at(offset: u64, metadata_op: u64) -> ConsumerPosition {
        ConsumerPosition {
            partition_id: 1,
            offset,
            context: PartitionContext {
                incarnation: 1,
                metadata_op,
                ..PartitionContext::default()
            },
        }
    }

    #[tokio::test]
    async fn store_position_should_skip_an_older_position_from_an_earlier_poll() {
        let consumer = builder().partition(Some(1)).build();
        let stored = position_at(19, 2);
        consumer.state.last_stored_offsets.insert(1, stored);

        // The client is not connected, so only a store skipped before any request succeeds.
        consumer.store_position(position_at(9, 1)).await.unwrap();

        assert_eq!(consumer.get_last_stored_offset(1), Some(stored.offset));
    }

    #[tokio::test]
    async fn shutdown_should_not_move_a_higher_stored_offset_back_under_replay() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let mut consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .allow_replay()
            .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingAllMessages))
            .build();
        let consumed = position_at(9, 1);
        let stored = position_at(19, 1);
        consumer.state.last_consumed_offsets.insert(1, consumed);
        consumer.state.last_stored_offsets.insert(1, stored);

        consumer.shutdown().await.unwrap();

        assert!(served.stored_offsets().is_empty());
        assert_eq!(consumer.get_last_stored_offset(1), Some(stored.offset));
    }

    #[tokio::test]
    async fn store_offset_without_a_consumed_position_should_record_the_stored_offset() {
        const OFFSET: u64 = 7;
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();

        consumer.store_offset(OFFSET, Some(1)).await.unwrap();

        assert_eq!(served.stored_offsets(), [OFFSET]);
        assert_eq!(consumer.get_last_stored_offset(1), Some(OFFSET));
    }

    /// The offset the first store leaves on the server.
    const STORED_OFFSET: u64 = 100;
    /// An offset behind [`STORED_OFFSET`] that a second store asks for.
    const LOWER_OFFSET: u64 = 10;

    /// A position delivered at `offset` under a real partition incarnation. Metadata revisions
    /// start at 1, so no real partition has the incarnation 0 of an uncaptured context.
    fn captured_position_at(offset: u64) -> ConsumerPosition {
        ConsumerPosition {
            partition_id: 1,
            offset,
            context: PartitionContext {
                incarnation: 1,
                owner_generation: 0,
                metadata_op: 1,
            },
        }
    }

    #[tokio::test]
    async fn store_offset_without_a_consumed_position_should_send_every_offset() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();

        consumer.store_offset(STORED_OFFSET, Some(1)).await.unwrap();
        consumer.store_offset(LOWER_OFFSET, Some(1)).await.unwrap();

        assert_eq!(served.stored_offsets(), [STORED_OFFSET, LOWER_OFFSET]);
        assert_eq!(consumer.get_last_stored_offset(1), Some(LOWER_OFFSET));
    }

    #[tokio::test]
    async fn store_offset_without_a_consumed_position_should_store_a_lower_offset_under_replay() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .allow_replay()
            .build();

        consumer.store_offset(STORED_OFFSET, Some(1)).await.unwrap();
        consumer.store_offset(LOWER_OFFSET, Some(1)).await.unwrap();

        assert_eq!(served.stored_offsets(), [STORED_OFFSET, LOWER_OFFSET]);
        assert_eq!(consumer.get_last_stored_offset(1), Some(LOWER_OFFSET));
    }

    #[tokio::test]
    async fn store_position_should_send_after_an_uncaptured_scalar_store() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();

        consumer.store_offset(STORED_OFFSET, Some(1)).await.unwrap();
        consumer
            .store_position(captured_position_at(LOWER_OFFSET))
            .await
            .unwrap();

        assert_eq!(served.stored_offsets(), [STORED_OFFSET, LOWER_OFFSET]);
        assert_eq!(consumer.get_last_stored_offset(1), Some(LOWER_OFFSET));
    }

    /// A recreated partition starts again at 0, and the offset stored without a consumed position
    /// can belong to its earlier incarnation, which the record cannot tell apart.
    #[tokio::test]
    async fn store_position_should_send_offset_zero_after_one_stored_without_a_consumed_position() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();

        consumer.store_offset(STORED_OFFSET, Some(1)).await.unwrap();
        consumer
            .store_position(captured_position_at(0))
            .await
            .unwrap();

        assert_eq!(served.stored_offsets(), [STORED_OFFSET, 0]);
        assert_eq!(consumer.get_last_stored_offset(1), Some(0));
    }

    #[tokio::test]
    async fn store_offset_without_a_consumed_position_should_not_compare_a_captured_store() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();

        consumer
            .store_position(captured_position_at(STORED_OFFSET))
            .await
            .unwrap();
        consumer.store_offset(LOWER_OFFSET, Some(1)).await.unwrap();

        assert_eq!(served.stored_offsets(), [STORED_OFFSET, LOWER_OFFSET]);
        assert_eq!(consumer.get_last_stored_offset(1), Some(LOWER_OFFSET));
    }

    #[tokio::test]
    async fn shutdown_should_store_the_captured_position_after_an_uncaptured_scalar_store() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let mut consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingAllMessages))
            .build();
        consumer.store_offset(STORED_OFFSET, Some(1)).await.unwrap();
        consumer
            .state
            .last_consumed_offsets
            .insert(1, captured_position_at(LOWER_OFFSET));

        consumer.shutdown().await.unwrap();

        assert_eq!(served.stored_offsets(), [STORED_OFFSET, LOWER_OFFSET]);
        assert_eq!(consumer.get_last_stored_offset(1), Some(LOWER_OFFSET));
    }

    #[tokio::test]
    async fn given_a_deleted_offset_when_storing_the_same_position_should_send_again() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();
        let position = captured_position_at(STORED_OFFSET);
        consumer.store_position(position).await.unwrap();

        consumer
            .delete_offset(Some(position.partition_id))
            .await
            .unwrap();

        assert_eq!(consumer.get_last_stored_offset(position.partition_id), None);
        consumer.store_position(position).await.unwrap();
        assert_eq!(served.deleted_partitions(), [position.partition_id]);
        assert_eq!(served.stored_offsets(), [STORED_OFFSET, STORED_OFFSET]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_unconfirmed_delete_when_storing_the_same_position_should_resolve_and_store() {
        for cancel in [false, true] {
            let (client, served) =
                connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
            let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
                .partition(Some(1))
                .auto_commit(AutoCommit::Disabled)
                .build();
            let position = captured_position_at(STORED_OFFSET);
            consumer.store_position(position).await.unwrap();
            served.0.script(
                &endpoint(0),
                DELETE_CONSUMER_OFFSET_CODE,
                [Answer::Silence, Answer::Commit(2)],
            );

            if cancel {
                assert!(
                    tokio::time::timeout(POLL_RETRY_INTERVAL, consumer.delete_offset(Some(1)))
                        .await
                        .is_err()
                );
            } else {
                assert!(matches!(
                    consumer.delete_offset(Some(1)).await,
                    Err(IggyError::OperationUncertain(_))
                ));
            }
            assert_eq!(served.0.count(DELETE_CONSUMER_OFFSET_CODE), 1);
            consumer.store_position(position).await.unwrap();

            let deletes = served.0.received(DELETE_CONSUMER_OFFSET_CODE);
            assert_eq!(deletes.len(), 2);
            assert_eq!(deletes[0].header.request, deletes[1].header.request);
            assert_eq!(served.stored_offsets(), [STORED_OFFSET, STORED_OFFSET]);
        }
    }

    #[tokio::test]
    async fn given_a_rejected_delete_when_storing_the_same_position_should_refresh_coverage() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();
        let position = captured_position_at(STORED_OFFSET);
        consumer.store_position(position).await.unwrap();
        served.0.script(
            &endpoint(0),
            DELETE_CONSUMER_OFFSET_CODE,
            [Answer::CommitRejected(2, IggyError::InvalidCommand)],
        );

        assert_eq!(
            consumer.delete_offset(Some(1)).await,
            Err(IggyError::InvalidCommand)
        );
        consumer.store_position(position).await.unwrap();

        assert_eq!(consumer.get_last_stored_offset(1), Some(STORED_OFFSET));
        assert_eq!(served.stored_offsets(), [STORED_OFFSET, STORED_OFFSET]);
    }

    #[tokio::test]
    async fn given_a_stored_position_when_its_owner_or_incarnation_changes_should_store_the_lower_offset()
     {
        for context in [
            PartitionContext {
                owner_generation: 2,
                ..captured_position_at(LOWER_OFFSET).context
            },
            PartitionContext {
                incarnation: 2,
                ..captured_position_at(LOWER_OFFSET).context
            },
        ] {
            let (client, served) =
                connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
            let consumer = builder_on(client, Consumer::group(Identifier::numeric(1).unwrap()))
                .partition(Some(1))
                .auto_commit(AutoCommit::Disabled)
                .build();
            consumer
                .store_position(captured_position_at(STORED_OFFSET))
                .await
                .unwrap();

            consumer
                .store_position(ConsumerPosition {
                    context,
                    ..captured_position_at(LOWER_OFFSET)
                })
                .await
                .unwrap();

            assert_eq!(served.stored_offsets(), [STORED_OFFSET, LOWER_OFFSET]);
            let stores = served.0.received(STORE_CONSUMER_OFFSET_CODE);
            assert_eq!(stores[1].header.partition_incarnation, context.incarnation);
            assert_eq!(stores[1].header.owner_generation, context.owner_generation);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_store_when_the_blocker_is_rejected_should_store_the_new_position() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();
        served.0.script(
            &endpoint(0),
            STORE_CONSUMER_OFFSET_CODE,
            [
                Answer::Silence,
                Answer::CommitRejected(1, IggyError::HistoryUnavailable),
                Answer::Commit(2),
            ],
        );
        let first = consumer
            .store_position(captured_position_at(LOWER_OFFSET))
            .await;
        let old = served.0.received(STORE_CONSUMER_OFFSET_CODE)[0]
            .header
            .request;
        assert_eq!(first, Err(IggyError::OperationUncertain(old)));

        consumer
            .store_position(captured_position_at(STORED_OFFSET))
            .await
            .unwrap();

        let resolved = consumer
            .client
            .read()
            .await
            .resolve_operation(old, (Instant::now() + OPERATION_TIMEOUT).into_std())
            .await;
        assert_eq!(resolved, Err(IggyError::HistoryUnavailable));
        let stores = served.0.received(STORE_CONSUMER_OFFSET_CODE);
        assert_eq!(
            served.stored_offsets(),
            [LOWER_OFFSET, LOWER_OFFSET, STORED_OFFSET]
        );
        assert_eq!(stores[0].header.request, stores[1].header.request);
        assert_eq!(stores[0].body, stores[1].body);
        assert_ne!(stores[1].header.request, stores[2].header.request);
        assert_eq!(consumer.get_last_stored_offset(1), Some(STORED_OFFSET));
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_store_when_deleting_should_resolve_before_sending_the_delete() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();
        served.0.script(
            &endpoint(0),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence, Answer::Commit(1)],
        );
        assert!(matches!(
            consumer
                .store_position(captured_position_at(LOWER_OFFSET))
                .await,
            Err(IggyError::OperationUncertain(_))
        ));

        consumer.delete_offset(Some(1)).await.unwrap();

        let mutations: Vec<_> = served
            .0
            .codes(&endpoint(0))
            .into_iter()
            .filter(|code| {
                matches!(
                    *code,
                    STORE_CONSUMER_OFFSET_CODE | DELETE_CONSUMER_OFFSET_CODE
                )
            })
            .collect();
        assert_eq!(
            mutations,
            [
                STORE_CONSUMER_OFFSET_CODE,
                STORE_CONSUMER_OFFSET_CODE,
                DELETE_CONSUMER_OFFSET_CODE
            ]
        );
        assert_eq!(served.deleted_partitions(), [1]);
        assert_eq!(consumer.get_last_stored_offset(1), None);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_still_uncertain_store_when_storing_again_should_leave_the_new_position_unsent()
    {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();
        served.0.script(
            &endpoint(0),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence, Answer::Silence],
        );
        assert!(matches!(
            consumer
                .store_position(captured_position_at(LOWER_OFFSET))
                .await,
            Err(IggyError::OperationUncertain(_))
        ));
        let old = served.0.received(STORE_CONSUMER_OFFSET_CODE)[0]
            .header
            .request;
        let started = Instant::now();

        let result = consumer
            .store_position(captured_position_at(STORED_OFFSET))
            .await;

        assert_eq!(result, Err(IggyError::LaneBlocked(old)));
        assert!(started.elapsed() <= OPERATION_TIMEOUT);
        assert_eq!(served.stored_offsets(), [LOWER_OFFSET, LOWER_OFFSET]);
        assert_eq!(consumer.get_last_stored_offset(1), None);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_outer_deadline_past_the_default_budget_when_a_store_is_unanswered_should_end_at_the_outer_deadline()
     {
        const OUTER_DEADLINE: Duration = OPERATION_TIMEOUT.saturating_mul(2);
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(1))
            .auto_commit(AutoCommit::Disabled)
            .build();
        served
            .0
            .script(&endpoint(0), STORE_CONSUMER_OFFSET_CODE, [Answer::Silence]);
        let started = Instant::now();

        let result = with_deadline(
            started + OUTER_DEADLINE,
            consumer.store_position(captured_position_at(STORED_OFFSET)),
        )
        .await;

        let op = served.0.received(STORE_CONSUMER_OFFSET_CODE)[0]
            .header
            .request;
        assert_eq!(result, Err(IggyError::OperationUncertain(op)));
        assert_eq!(started.elapsed(), OUTER_DEADLINE);
    }

    #[tokio::test(start_paused = true)]
    async fn given_multiple_final_positions_when_shutdown_times_out_should_share_one_deadline() {
        let (client, served) =
            connect_to_test_server(vec![1, 2], IggyError::HistoryUnavailable).await;
        let mut consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingAllMessages))
            .build();
        for partition_id in [1, 2] {
            consumer.state.last_consumed_offsets.insert(
                partition_id,
                ConsumerPosition {
                    partition_id,
                    ..captured_position_at(LOWER_OFFSET)
                },
            );
        }
        served.0.script(
            &endpoint(0),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence, Answer::Silence],
        );
        let started = Instant::now();

        consumer.shutdown().await.unwrap();

        assert!(started.elapsed() <= OPERATION_TIMEOUT);
        assert_eq!(served.stored_offsets(), [LOWER_OFFSET]);
        assert_eq!(consumer.get_last_stored_offset(1), None);
        assert_eq!(consumer.get_last_stored_offset(2), None);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_inflight_background_store_when_shutdown_aborts_it_should_resolve_and_flush() {
        let (client, served) = connect_to_test_server(vec![1], IggyError::HistoryUnavailable).await;
        let mut consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .offset_drain_timeout(IggyDuration::new(POLL_RETRY_INTERVAL))
            .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingAllMessages))
            .build();
        consumer
            .state
            .last_consumed_offsets
            .insert(1, captured_position_at(LOWER_OFFSET));
        served.0.script(
            &endpoint(0),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence, Answer::Commit(1), Answer::Commit(2)],
        );
        consumer.background_commit_task =
            Some(consumer.store_offsets_in_background(
                NonZeroIggyDuration::new(POLL_RETRY_INTERVAL).unwrap(),
            ));
        eventually("background store reached the primary", || {
            served.0.count(STORE_CONSUMER_OFFSET_CODE) == 1
        })
        .await;

        consumer.shutdown().await.unwrap();

        let stores = served.0.received(STORE_CONSUMER_OFFSET_CODE);
        assert_eq!(stores.len(), 3);
        assert_eq!(stores[0].header.request, stores[1].header.request);
        assert_ne!(stores[1].header.request, stores[2].header.request);
        assert_eq!(consumer.get_last_stored_offset(1), Some(LOWER_OFFSET));
    }

    /// Records the events of one level emitted on the test thread. The current-thread runtime
    /// of `#[tokio::test]` polls the tasks it spawns on that thread, so their events count too.
    #[derive(Clone)]
    struct LevelReports {
        level: Level,
        events: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl Subscriber for LevelReports {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            *metadata.level() == self.level
        }

        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _: &Id, _: &Record<'_>) {}

        fn record_follows_from(&self, _: &Id, _: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut fields = Vec::new();
            event.record(&mut |field: &Field, value: &dyn Debug| {
                fields.push(format!("{field}: {value:?}"));
            });
            self.events.lock().unwrap().push(fields.join(", "));
        }

        fn enter(&self, _: &Id) {}

        fn exit(&self, _: &Id) {}
    }

    impl LevelReports {
        fn errors() -> Self {
            Self {
                level: Level::ERROR,
                events: Arc::default(),
            }
        }

        fn warnings() -> Self {
            Self {
                level: Level::WARN,
                events: Arc::default(),
            }
        }

        /// Waits for the first report, or returns none after [`POLL_TIMEOUT`].
        async fn wait(&self) -> Vec<String> {
            let first = async {
                loop {
                    let reports = self.events.lock().unwrap().clone();
                    if !reports.is_empty() {
                        return reports;
                    }
                    sleep(POLL_RETRY_INTERVAL).await;
                }
            };
            timeout(POLL_TIMEOUT, first).await.unwrap_or_default()
        }
    }

    #[tokio::test]
    async fn interval_commit_should_report_a_failed_store() {
        let recorder = LevelReports::errors();
        let _subscriber = tracing::subscriber::set_default(recorder.clone());
        let consumer = builder().partition(Some(1)).build();
        consumer
            .state
            .last_consumed_offsets
            .insert(1, position_at(9, 1));

        // The client is not connected, so every store fails.
        let task = consumer
            .store_offsets_in_background(NonZeroIggyDuration::new(POLL_RETRY_INTERVAL).unwrap());
        let reports = recorder.wait().await;
        task.abort();

        assert!(
            !reports.is_empty(),
            "a failed interval commit must be reported"
        );
        let failure = IggyError::Disconnected.to_string();
        assert!(
            reports.iter().all(|report| report.contains(&failure)),
            "{reports:?}"
        );
    }

    #[tokio::test]
    async fn queued_commit_should_report_a_failed_store() {
        let recorder = LevelReports::errors();
        let _subscriber = tracing::subscriber::set_default(recorder.clone());
        let mut consumer = builder().partition(Some(1)).build();
        consumer.initialized = true;
        let task = consumer.store_pending_commits_in_background();

        // The client is not connected, so the store fails.
        consumer.send_store_offset(position_at(9, 1));
        let reports = recorder.wait().await;
        task.abort();

        assert_eq!(
            reports.len(),
            1,
            "a failed queued commit must be reported once"
        );
        assert!(
            reports[0].contains(&IggyError::Disconnected.to_string()),
            "{reports:?}"
        );
    }

    #[test]
    fn commit_on_poll_rewind_should_skip_revoked_and_already_stored_partitions() {
        let consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap())).build();
        for (partition_id, delivered, record) in [(1, 2, 9), (2, 5, 9), (3, 9, 9)] {
            consumer.state.last_consumed_offsets.insert(
                partition_id,
                ConsumerPosition {
                    partition_id,
                    ..position_at(delivered, 1)
                },
            );
            consumer.state.last_stored_offsets.insert(
                partition_id,
                ConsumerPosition {
                    partition_id,
                    ..position_at(record, 1)
                },
            );
        }

        assert_eq!(
            consumer.state.unstored_delivered_offsets(Some(&[1, 3])),
            [position_at(2, 1)]
        );
        let mut standalone = consumer.state.unstored_delivered_offsets(None);
        standalone.sort_unstable_by_key(|position| position.partition_id);
        assert_eq!(
            standalone,
            [
                position_at(2, 1),
                ConsumerPosition {
                    partition_id: 2,
                    ..position_at(5, 1)
                }
            ]
        );
    }

    #[tokio::test]
    async fn commit_on_poll_shutdown_should_rewind_over_a_queued_batch() {
        let consumer = builder_for(Consumer::default()).build();
        let delivered = position_at(4, 1);
        consumer.state.last_consumed_offsets.insert(1, delivered);
        consumer.state.last_stored_offsets.insert(1, delivered);
        let queued = PolledMessages {
            partition_id: 1,
            context: delivered.context,
            current_offset: 6,
            count: 2,
            messages: vec![message_at(5), message_at(6)],
        };

        consumer
            .state
            .rewind_undelivered_offsets(vec![queued], None, true)
            .await;
        assert_eq!(
            consumer.state.unstored_delivered_offsets(None),
            [delivered],
            "the server committed offset 6, so the delivered offset 4 is stored again"
        );
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
    fn default_prefetch_should_fit_a_batch_longer_than_the_default() {
        assert_eq!(builder().batch_length(20_000).build().prefetch_slots(), 1);
        assert_eq!(
            builder()
                .batch_length(20_000)
                .prefetch_messages(16_000)
                .build()
                .prefetch_slots(),
            0,
            "an explicit limit stays as configured"
        );
    }

    #[test]
    fn send_store_offset_should_keep_the_latest_offset_per_partition() {
        let mut consumer = builder().build();
        consumer.initialized = true;

        consumer.send_store_offset(ConsumerPosition {
            partition_id: 1,
            offset: 5,
            context: PartitionContext::default(),
        });
        consumer.send_store_offset(ConsumerPosition {
            partition_id: 1,
            offset: 7,
            context: PartitionContext::default(),
        });
        consumer.send_store_offset(ConsumerPosition {
            partition_id: 2,
            offset: 3,
            context: PartitionContext::default(),
        });

        let mut queued: Vec<(u32, u64)> = consumer
            .pending_commits
            .iter()
            .map(|entry| (*entry.key(), entry.value().offset))
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

    const POLLED_PARTITION: u32 = 1;
    const PARTITION_PRIMARY: u8 = 1;

    /// A signed-in client of three ready replicas. Replica 0 is the metadata
    /// primary, and `PARTITION_PRIMARY` leads every partition.
    async fn connect_to_three_replicas() -> (TcpClient, Arc<FakeCluster>) {
        connect_to_three_replicas_with(policy()).await
    }

    async fn connect_to_three_replicas_with(
        policy: ReconnectPolicy,
    ) -> (TcpClient, Arc<FakeCluster>) {
        let cluster = FakeCluster::new(3);
        *cluster.route.lock().unwrap() = Some(PartitionRouteResponse {
            incarnation: 1,
            metadata_op: 1,
            view: 0,
            partitions_count: POLLED_PARTITION + 1,
            primary_replica: PARTITION_PRIMARY,
        });
        let links = connected_with(&cluster, 3, policy).await;
        (TcpClient::from_links(links), cluster)
    }

    /// Takes every replica of `cluster` down, which closes the links to them.
    fn take_every_replica_down(cluster: &FakeCluster) {
        let endpoints: Vec<String> = cluster
            .nodes
            .lock()
            .unwrap()
            .iter()
            .map(|node| endpoint(node.replica_id))
            .collect();
        for endpoint in endpoints {
            cluster.down.lock().unwrap().insert(endpoint.clone());
            cluster.drop_connections(&endpoint);
        }
    }

    /// A consumer of `POLLED_PARTITION` whose polling follows the client
    /// events, as after `init`.
    async fn polling_consumer(client: TcpClient, consumer: Consumer) -> IggyConsumer {
        let mut consumer = builder_on(client, consumer)
            .partition(Some(POLLED_PARTITION))
            .polling_strategy(PollingStrategy::first())
            .auto_commit(AutoCommit::Disabled)
            .polling_retry_interval(NonZeroIggyDuration::new(POLL_RETRY_INTERVAL).unwrap())
            .build();
        consumer.initialized = true;
        consumer.events_task = Some(consumer.subscribe_events().await);
        consumer
    }

    /// Publishes `events` in order on the client of `consumer`.
    async fn publish(consumer: &IggyConsumer, events: &[DiagnosticEvent]) {
        let client = consumer.client.read().await;
        let ClientWrapper::Tcp(client) = &*client else {
            panic!("a test consumer runs on a TCP client");
        };
        for event in events {
            client.publish_event(*event).await;
        }
    }

    /// A poll reply of `POLLED_PARTITION` with `messages_count` messages in `batches`.
    fn poll_answer(current_offset: u64, messages_count: u32, batches: &[u8]) -> Answer {
        let header = PollMessagesResponseHeader {
            partition_id: POLLED_PARTITION,
            current_offset,
            messages_count,
            context: PartitionContext::default(),
        };
        let mut body = header.to_bytes().to_vec();
        body.extend_from_slice(batches);
        Answer::CommitBody(1, body)
    }

    fn available_poll() -> Answer {
        poll_of_one_message(0)
    }

    /// A poll reply with one empty message at `offset`. A zeroed frame is such a message, and the
    /// client does not check its checksum.
    fn poll_of_one_message(offset: u64) -> Answer {
        let mut batch = vec![0; BATCH_HEADER_SIZE + BATCH_MESSAGE_HEADER_SIZE];
        let mut header = BatchHeader::new(0, 0, batch.len() as u64, 1);
        header.base_offset = offset;
        header.encode_into(&mut batch);
        poll_answer(offset, 1, &batch)
    }

    fn existing_consumer_group() -> Answer {
        Answer::Body(
            ConsumerGroupDetailsResponse {
                group: ConsumerGroupResponse {
                    id: 1,
                    partitions_count: POLLED_PARTITION + 1,
                    members_count: 0,
                    name: WireName::new("consumer").unwrap(),
                },
                members: Vec::new(),
            }
            .to_bytes()
            .to_vec(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_group_mutation_when_polling_again_should_resolve_and_join() {
        for mutation in [CREATE_CONSUMER_GROUP_CODE, JOIN_CONSUMER_GROUP_CODE] {
            let (client, cluster) = connect_to_three_replicas().await;
            let mut consumer =
                polling_consumer(client, Consumer::group(Identifier::numeric(1).unwrap())).await;
            let first_group = if mutation == CREATE_CONSUMER_GROUP_CODE {
                Answer::Body(Vec::new())
            } else {
                existing_consumer_group()
            };
            cluster.script(
                &endpoint(0),
                GET_CONSUMER_GROUP_CODE,
                [first_group, existing_consumer_group()],
            );
            cluster.script(&endpoint(0), mutation, [Answer::Silence, Answer::Commit(1)]);
            cluster.script(
                &endpoint(0),
                SYNC_CONSUMER_GROUP_CODE,
                [Answer::Body(
                    SyncConsumerGroupResponse {
                        generation: 1,
                        partitions: vec![POLLED_PARTITION],
                    }
                    .to_bytes()
                    .to_vec(),
                )],
            );
            cluster.script(
                &endpoint(PARTITION_PRIMARY),
                POLL_MESSAGES_DEFERRED_CODE,
                [available_poll()],
            );

            let first = poll_once(&mut consumer).await;
            let op = cluster.received(mutation)[0].header.request;
            assert_eq!(first.unwrap_err(), IggyError::OperationUncertain(op));

            let polled = poll_once(&mut consumer).await;
            assert!(polled.is_ok(), "mutation {mutation}: {polled:?}");

            let requests = cluster.received(mutation);
            assert_eq!(requests[0].header.request, requests[1].header.request);
            assert_eq!(requests[0].body, requests[1].body);
            assert!((1..=2).contains(&cluster.count(POLL_MESSAGES_DEFERRED_CODE)));
            assert!(consumer.joined_consumer_group.load(ORDERING));
            assert_eq!(
                consumer
                    .client
                    .read()
                    .await
                    .resolve_operation(op, (Instant::now() + OPERATION_TIMEOUT).into_std())
                    .await,
                Ok(())
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_join_when_resolution_is_uncertain_should_keep_the_operation() {
        let (client, cluster) = connect_to_three_replicas().await;
        let consumer =
            polling_consumer(client, Consumer::group(Identifier::numeric(1).unwrap())).await;
        cluster.script(
            &endpoint(0),
            GET_CONSUMER_GROUP_CODE,
            repeat_n(existing_consumer_group(), 2),
        );
        cluster.script(
            &endpoint(0),
            JOIN_CONSUMER_GROUP_CODE,
            [Answer::Silence, Answer::Silence, Answer::Commit(1)],
        );
        let first = consumer.init_consumer_group().await;
        let op = cluster.received(JOIN_CONSUMER_GROUP_CODE)[0].header.request;
        assert_eq!(first, Err(IggyError::OperationUncertain(op)));

        assert_eq!(
            consumer.init_consumer_group().await,
            Err(IggyError::OperationUncertain(op))
        );
        assert_eq!(cluster.count(GET_CONSUMER_GROUP_CODE), 1);
        assert!(!consumer.joined_consumer_group.load(ORDERING));

        consumer.init_consumer_group().await.unwrap();
        let joins = cluster.received(JOIN_CONSUMER_GROUP_CODE);
        assert_eq!(joins.len(), 4);
        assert_eq!(joins[1].header.request, op);
        assert_eq!(joins[2].header.request, op);
        assert_ne!(joins[3].header.request, op);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_join_when_resolution_is_rejected_should_report_and_allow_a_new_join()
     {
        let (client, cluster) = connect_to_three_replicas().await;
        let consumer =
            polling_consumer(client, Consumer::group(Identifier::numeric(1).unwrap())).await;
        cluster.script(
            &endpoint(0),
            GET_CONSUMER_GROUP_CODE,
            repeat_n(existing_consumer_group(), 2),
        );
        cluster.script(
            &endpoint(0),
            JOIN_CONSUMER_GROUP_CODE,
            [
                Answer::Silence,
                Answer::CommitRejected(1, IggyError::Unauthorized),
            ],
        );
        assert!(matches!(
            consumer.init_consumer_group().await,
            Err(IggyError::OperationUncertain(_))
        ));

        assert_eq!(
            consumer.init_consumer_group().await,
            Err(IggyError::Unauthorized)
        );
        assert!(!consumer.joined_consumer_group.load(ORDERING));

        consumer.init_consumer_group().await.unwrap();
        assert_eq!(cluster.count(JOIN_CONSUMER_GROUP_CODE), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn given_another_callers_uncertain_join_when_initializing_a_group_should_leave_it_retained()
     {
        let (client, cluster) = connect_to_three_replicas().await;
        cluster.script(&endpoint(0), JOIN_CONSUMER_GROUP_CODE, [Answer::Silence]);
        let first = client
            .join_consumer_group(
                &Identifier::numeric(1).unwrap(),
                &Identifier::numeric(1).unwrap(),
                &Identifier::numeric(2).unwrap(),
            )
            .await;
        let op = cluster.received(JOIN_CONSUMER_GROUP_CODE)[0].header.request;
        assert_eq!(first, Err(IggyError::OperationUncertain(op)));
        let consumer =
            polling_consumer(client, Consumer::group(Identifier::numeric(1).unwrap())).await;
        cluster.script(
            &endpoint(0),
            GET_CONSUMER_GROUP_CODE,
            [existing_consumer_group()],
        );

        assert_eq!(
            consumer.init_consumer_group().await,
            Err(IggyError::LaneBlocked(op))
        );

        assert_eq!(cluster.count(JOIN_CONSUMER_GROUP_CODE), 1);
        assert!(!consumer.joined_consumer_group.load(ORDERING));
        consumer
            .client
            .read()
            .await
            .resolve_operation(op, (Instant::now() + OPERATION_TIMEOUT).into_std())
            .await
            .unwrap();
        assert_eq!(cluster.count(JOIN_CONSUMER_GROUP_CODE), 2);
    }

    /// Polls one poll after another, as the consumer stream does, until one
    /// succeeds.
    async fn poll_once(consumer: &mut IggyConsumer) -> Result<ReceivedMessage, IggyError> {
        consumer.next().await.ok_or(IggyError::ClientShutdown)?
    }

    async fn poll_until_success(consumer: &mut IggyConsumer) -> Option<ReceivedMessage> {
        let mut last_error = None;
        let polled = async {
            loop {
                match poll_once(consumer).await {
                    Ok(polled) => return polled,
                    Err(error) => last_error = Some(error),
                }
            }
        };
        let result = timeout(TEST_BUDGET, polled).await.ok();
        if result.is_none() {
            eprintln!(
                "last poll error: {last_error:?}; joined: {}, can_poll: {}",
                consumer.joined_consumer_group.load(ORDERING),
                consumer.can_poll.load(ORDERING)
            );
        }
        result
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_live_session_when_a_replica_refuses_a_poll_with_stale_client_should_poll_again_without_a_new_sign_in()
     {
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer =
            polling_consumer(client, Consumer::new(Identifier::numeric(1).unwrap())).await;
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Detached, available_poll()],
        );

        let refused = poll_once(&mut consumer).await;

        assert!(
            matches!(refused, Err(IggyError::StaleClient)),
            "{refused:?}"
        );
        assert!(poll_until_success(&mut consumer).await.is_some());
        assert!((2..=3).contains(&cluster.count(POLL_MESSAGES_DEFERRED_CODE)));
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lost_route_link_when_a_poll_outlasts_it_should_poll_again_once_the_link_is_back()
     {
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer =
            polling_consumer(client, Consumer::new(Identifier::numeric(1).unwrap())).await;
        let primary = endpoint(PARTITION_PRIMARY);
        cluster.down.lock().unwrap().insert(primary.clone());
        cluster.drop_connections(&primary);

        let lost = timeout(POLL_TIMEOUT, poll_once(&mut consumer)).await;
        assert!(
            lost.is_err(),
            "non-admission retries inside the consumer: {lost:?}"
        );
        cluster.down.lock().unwrap().clear();
        cluster.script(&primary, POLL_MESSAGES_DEFERRED_CODE, [available_poll()]);
        assert!(poll_until_success(&mut consumer).await.is_some());
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_delayed_poll_error_when_sign_in_has_completed_should_keep_polling_enabled() {
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer =
            polling_consumer(client, Consumer::new(Identifier::numeric(1).unwrap())).await;
        *cluster.session.lock().unwrap() = None;
        cluster
            .held_registers
            .lock()
            .unwrap()
            .insert("iggy".to_owned());
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Evict(EvictionReason::NoSession), available_poll()],
        );
        let can_poll = Arc::clone(&consumer.can_poll);
        let mut poll = Box::pin(poll_once(&mut consumer));
        timeout(TEST_BUDGET, async {
            while cluster.count(POLL_MESSAGES_DEFERRED_CODE) == 0 {
                assert!(
                    poll.as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        eventually("the failed poll triggers sign-in", || {
            cluster.count(LOGIN_REGISTER_CODE) == 2
        })
        .await;
        eventually("the consumer processes SignedOut", || {
            !can_poll.load(ORDERING)
        })
        .await;
        cluster.held_registers.lock().unwrap().clear();
        eventually("the consumer processes SignedIn", || {
            can_poll.load(ORDERING)
        })
        .await;

        assert!(timeout(POLL_TIMEOUT, poll).await.unwrap().is_ok());
        assert!(consumer.can_poll.load(ORDERING));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_poll_lost_to_a_disconnect_when_consuming_callbacks_should_deliver_the_next_poll()
     {
        const OFFSET: u64 = 5;
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer =
            polling_consumer(client, Consumer::new(Identifier::numeric(1).unwrap())).await;
        consumer.deferred_polls = Some(deferred::DeferredPolls::with_error(
            &consumer,
            IggyError::Disconnected,
        ));
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [poll_of_one_message(OFFSET)],
        );
        let (shutdown, receiver) = tokio::sync::oneshot::channel();
        let handler = RecordMessages {
            offsets: std::sync::Mutex::default(),
            shutdown: std::sync::Mutex::new(Some(shutdown)),
        };

        let result = timeout(POLL_TIMEOUT, consumer.consume_messages(&handler, receiver)).await;

        assert_eq!(result, Ok(Ok(())));
        assert_eq!(*handler.offsets.lock().unwrap(), [OFFSET]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lost_commit_reply_when_shutdown_comes_before_the_retry_should_store_the_delivered_offset_again()
     {
        const DELIVERED: u64 = 4;
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer = committing_consumer(client).await;
        record_delivered(&consumer, DELIVERED);
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [
                Answer::Refuse(IggyError::HistoryUnavailable),
                Answer::Refuse(IggyError::TransientNotCommitted),
            ],
        );

        let refused = poll_once(&mut consumer).await;
        assert!(
            matches!(refused, Err(IggyError::HistoryUnavailable)),
            "{refused:?}"
        );
        assert_eq!(
            consumer.get_last_stored_offset(POLLED_PARTITION),
            Some(DELIVERED),
            "a refused poll commits nothing"
        );
        let lost = poll_once(&mut consumer).await;
        assert!(
            matches!(lost, Err(IggyError::TransientNotCommitted)),
            "{lost:?}"
        );
        consumer.shutdown().await.unwrap();

        assert_eq!(
            Served(cluster).stored_offsets(),
            [DELIVERED],
            "the server can have committed the batch of the lost reply"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_committing_fetch_in_flight_when_shutdown_starts_should_rewind_over_its_batch()
    {
        const FIRST: u64 = 5;
        const REPLY_DELAY: Duration = Duration::from_millis(100);
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer = committing_consumer(client).await;
        cluster.hold(POLL_MESSAGES_DEFERRED_CODE);
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [poll_of_one_message(FIRST)],
        );
        start_fetch(&mut consumer, &cluster).await;
        let started = Instant::now();

        let (shut_down, ()) = tokio::join!(consumer.shutdown(), async {
            sleep(REPLY_DELAY).await;
            cluster.release(POLL_MESSAGES_DEFERRED_CODE);
        });

        shut_down.unwrap();
        assert!(
            started.elapsed() < consumer.offset_drain_timeout.get_duration(),
            "the stop ends with the fetch, not with the drain timeout"
        );
        assert_eq!(
            Served(cluster).stored_offsets(),
            [FIRST - 1],
            "the server committed a batch that the application never got"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_committing_fetch_that_outlasts_the_drain_timeout_when_shutdown_aborts_it_should_store_the_delivered_offset_again()
     {
        const DELIVERED: u64 = 4;
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer = committing_consumer(client).await;
        assert!(
            consumer.offset_drain_timeout.get_duration()
                < consumer.poll_options.request_timeout.get_duration(),
            "the drain timeout must end before the poll does"
        );
        record_delivered(&consumer, DELIVERED);
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Silence],
        );
        start_fetch(&mut consumer, &cluster).await;

        consumer.shutdown().await.unwrap();

        assert_eq!(
            Served(cluster).stored_offsets(),
            [DELIVERED],
            "the server can commit the batch of the aborted poll"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn given_a_shutdown_task_on_the_worker_thread_when_it_aborts_a_committing_fetch_should_store_after_the_fetch_drops()
     {
        const DELIVERED: u64 = 4;
        const DRAIN_TIMEOUT: Duration = Duration::from_millis(50);
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer = committing_consumer(client).await;
        consumer.offset_drain_timeout = IggyDuration::from(DRAIN_TIMEOUT);
        record_delivered(&consumer, DELIVERED);
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Silence],
        );
        start_fetch(&mut consumer, &cluster).await;

        // The only worker thread runs the woken shutdown task before the
        // aborted fetch task, which drops its future later.
        let shut_down = tokio::spawn(async move { consumer.shutdown().await }).await;

        assert!(matches!(shut_down, Ok(Ok(()))), "{shut_down:?}");
        assert_eq!(
            Served(cluster).stored_offsets(),
            [DELIVERED],
            "the server can commit the batch of the aborted poll"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_committing_fetch_in_flight_when_the_generation_changes_should_hand_over_its_batch()
     {
        const FIRST: u64 = 5;
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer = committing_consumer(client).await;
        cluster.hold(POLL_MESSAGES_DEFERRED_CODE);
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [poll_of_one_message(FIRST)],
        );
        start_fetch(&mut consumer, &cluster).await;

        // The worker reads the generation only at the top of its loop, so it
        // hands this batch over with the generation that the fetch started in.
        consumer.fetch_generation.fetch_add(1, ORDERING);
        cluster.release(POLL_MESSAGES_DEFERRED_CODE);
        let polled = timeout(TEST_BUDGET, poll_once(&mut consumer)).await;

        assert!(
            matches!(&polled, Ok(Ok(received)) if received.message.header.offset == FIRST),
            "the server committed this batch, so a new fetch generation must not drop it: {polled:?}"
        );
    }

    /// Records `offset` of `POLLED_PARTITION` as the last delivered and stored message.
    fn record_delivered(consumer: &IggyConsumer, offset: u64) {
        let delivered = position_at(offset, 1);
        consumer
            .state
            .last_consumed_offsets
            .insert(POLLED_PARTITION, delivered);
        consumer
            .state
            .last_stored_offsets
            .insert(POLLED_PARTITION, delivered);
    }

    /// A consumer of `POLLED_PARTITION` whose every poll commits its batch.
    async fn committing_consumer(client: TcpClient) -> IggyConsumer {
        let mut consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(POLLED_PARTITION))
            .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
            .polling_retry_interval(NonZeroIggyDuration::new(POLL_RETRY_INTERVAL).unwrap())
            .build();
        consumer.initialized = true;
        consumer.events_task = Some(consumer.subscribe_events().await);
        consumer
    }

    /// Polls `consumer` until its fetch reaches `cluster`, then leaves that fetch in flight.
    async fn start_fetch(consumer: &mut IggyConsumer, cluster: &FakeCluster) {
        let mut poll = Box::pin(poll_once(consumer));
        timeout(TEST_BUDGET, async {
            while cluster.count(POLL_MESSAGES_DEFERRED_CODE) == 0 {
                assert!(
                    poll.as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_signed_out_client_when_polls_fail_with_unauthenticated_should_back_off_between_attempts()
     {
        let (client, cluster) = connect_to_three_replicas().await;
        client.logout_user().await.unwrap();
        // No lifecycle task, as for a sign-out that `init` did not see yet.
        let mut consumer = builder_on(client, Consumer::new(Identifier::numeric(1).unwrap()))
            .partition(Some(POLLED_PARTITION))
            .auto_commit(AutoCommit::Disabled)
            .polling_retry_interval(NonZeroIggyDuration::new(POLL_RETRY_INTERVAL).unwrap())
            .build();
        consumer.initialized = true;
        assert!(matches!(
            poll_once(&mut consumer).await,
            Err(IggyError::Unauthenticated)
        ));
        let started = Instant::now();

        let polled = poll_once(&mut consumer).await;

        assert!(
            matches!(polled, Err(IggyError::Unauthenticated)),
            "{polled:?}"
        );
        assert!(started.elapsed() >= POLL_RETRY_INTERVAL);
        assert_eq!(cluster.count(POLL_MESSAGES_DEFERRED_CODE), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_signed_in_client_when_a_late_disconnected_event_arrives_should_keep_polling() {
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer =
            polling_consumer(client, Consumer::new(Identifier::numeric(1).unwrap())).await;
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [available_poll()],
        );

        publish(
            &consumer,
            &[DiagnosticEvent::SignedIn, DiagnosticEvent::Disconnected],
        )
        .await;
        sleep(POLL_RETRY_INTERVAL).await;
        let polled = timeout(POLL_TIMEOUT, poll_once(&mut consumer)).await;

        assert!(matches!(polled, Ok(Ok(_))), "{polled:?}");
        assert!((1..=2).contains(&cluster.count(POLL_MESSAGES_DEFERRED_CODE)));
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_evicted_poll_when_the_client_signs_in_again_should_wait_for_signed_in() {
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer =
            polling_consumer(client, Consumer::new(Identifier::numeric(1).unwrap())).await;
        *cluster.session.lock().unwrap() = None;
        cluster
            .held_registers
            .lock()
            .unwrap()
            .insert("iggy".to_owned());
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Evict(EvictionReason::NoSession), available_poll()],
        );

        let evicted = timeout(POLL_TIMEOUT, poll_once(&mut consumer)).await;
        assert!(
            evicted.is_err(),
            "polling waits for the new session: {evicted:?}"
        );
        eventually("the client signs in again", || {
            cluster.count(LOGIN_REGISTER_CODE) == 2
        })
        .await;
        let parked = timeout(POLL_TIMEOUT, poll_once(&mut consumer)).await;
        assert!(parked.is_err(), "{parked:?}");
        assert_eq!(cluster.count(POLL_MESSAGES_DEFERRED_CODE), 1);
        cluster.held_registers.lock().unwrap().clear();
        assert!(poll_until_success(&mut consumer).await.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_group_member_when_a_poll_finds_its_session_ended_should_sign_in_and_join_again()
     {
        let (client, cluster) = connect_to_three_replicas().await;
        let mut consumer =
            polling_consumer(client, Consumer::group(Identifier::numeric(1).unwrap())).await;
        let group = ConsumerGroupDetailsResponse {
            group: ConsumerGroupResponse {
                id: 1,
                partitions_count: POLLED_PARTITION + 1,
                members_count: 0,
                name: WireName::new("consumer").unwrap(),
            },
            members: Vec::new(),
        };
        let assignment = SyncConsumerGroupResponse {
            generation: 1,
            partitions: vec![POLLED_PARTITION],
        };
        cluster.script(
            &endpoint(0),
            GET_CONSUMER_GROUP_CODE,
            repeat_n(Answer::Body(group.to_bytes().to_vec()), 4),
        );
        cluster.script(
            &endpoint(0),
            SYNC_CONSUMER_GROUP_CODE,
            repeat_n(Answer::Body(assignment.to_bytes().to_vec()), 4),
        );
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Refuse(IggyError::StaleClient), available_poll()],
        );

        let refused = poll_once(&mut consumer).await;

        assert!(
            matches!(refused, Err(IggyError::StaleClient)),
            "{refused:?}"
        );
        assert!(poll_until_success(&mut consumer).await.is_some());
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 2);
        assert!((2..=3).contains(&cluster.count(JOIN_CONSUMER_GROUP_CODE)));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_last_replica_link_that_gives_up_when_a_keepalive_restarts_the_links_should_pause_polling_and_keep_consuming_callbacks()
     {
        const OFFSET: u64 = 5;
        let one_retry = TcpClientReconnectionConfig {
            max_retries: Some(1),
            ..TcpClientReconnectionConfig::default()
        };
        let (client, cluster) =
            connect_to_three_replicas_with(ReconnectPolicy::from(&one_retry)).await;
        let mut consumer =
            polling_consumer(client, Consumer::new(Identifier::numeric(1).unwrap())).await;
        take_every_replica_down(&cluster);
        eventually("polling pauses once the last replica link gives up", || {
            !consumer.can_poll.load(ORDERING)
        })
        .await;
        cluster.down.lock().unwrap().clear();
        cluster.script(
            &endpoint(PARTITION_PRIMARY),
            POLL_MESSAGES_DEFERRED_CODE,
            [poll_of_one_message(OFFSET)],
        );
        let (shutdown, receiver) = tokio::sync::oneshot::channel();
        let handler = RecordMessages {
            offsets: std::sync::Mutex::default(),
            shutdown: std::sync::Mutex::new(Some(shutdown)),
        };
        let client = consumer.client.clone();
        let heartbeat = async {
            sleep(POLL_TIMEOUT).await;
            client.read().await.keepalive().await
        };

        let (consumed, keepalive) = tokio::join!(
            timeout(TEST_BUDGET, consumer.consume_messages(&handler, receiver)),
            heartbeat
        );

        assert_eq!(keepalive, Ok(true));
        assert_eq!(consumed, Ok(Ok(())));
        assert_eq!(*handler.offsets.lock().unwrap(), [OFFSET]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_reconnection_disabled_when_every_replica_link_is_lost_should_end_consuming_callbacks()
     {
        let disabled = TcpClientReconnectionConfig {
            enabled: false,
            ..TcpClientReconnectionConfig::default()
        };
        let (client, cluster) =
            connect_to_three_replicas_with(ReconnectPolicy::from(&disabled)).await;
        let mut consumer =
            polling_consumer(client, Consumer::new(Identifier::numeric(1).unwrap())).await;
        take_every_replica_down(&cluster);
        eventually("polling pauses once the client is disconnected", || {
            !consumer.can_poll.load(ORDERING)
        })
        .await;
        let (_shutdown, receiver) = tokio::sync::oneshot::channel();

        let result = timeout(
            POLL_TIMEOUT,
            consumer.consume_messages(&RejectMessages, receiver),
        )
        .await;

        assert_eq!(result, Ok(Err(IggyError::Disconnected)));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_shut_down_client_when_consuming_callbacks_should_end_with_client_shutdown() {
        let (client, _cluster) = connect_to_three_replicas().await;
        let mut consumer =
            polling_consumer(client, Consumer::new(Identifier::numeric(1).unwrap())).await;
        consumer.client.read().await.shutdown().await.unwrap();
        eventually("polling stops once the client shuts down", || {
            !consumer.can_poll.load(ORDERING)
        })
        .await;
        let (_shutdown, receiver) = tokio::sync::oneshot::channel();

        let result = timeout(
            POLL_TIMEOUT,
            consumer.consume_messages(&RejectMessages, receiver),
        )
        .await;

        assert_eq!(result, Ok(Err(IggyError::ClientShutdown)));
    }

    #[tokio::test]
    async fn given_an_uninitialized_consumer_when_consuming_callbacks_should_end_at_once() {
        let mut consumer = builder_for(Consumer::default()).build();
        let (_shutdown, receiver) = tokio::sync::oneshot::channel();

        let result = timeout(
            POLL_TIMEOUT,
            consumer.consume_messages(&RejectMessages, receiver),
        )
        .await;

        assert_eq!(result, Ok(Err(IggyError::InvalidConfiguration)));
    }

    #[tokio::test]
    async fn given_disabled_auto_commit_and_next_when_consuming_callbacks_should_warn_once() {
        let warnings = consume_three_messages(AutoCommit::Disabled, PollingStrategy::next()).await;

        assert_eq!(
            warnings.len(),
            1,
            "a run warns once, not per message: {warnings:?}"
        );
        assert!(warnings[0].contains("never stores offsets"), "{warnings:?}");
    }

    #[tokio::test]
    async fn given_auto_commit_or_a_fixed_start_when_consuming_callbacks_should_not_warn() {
        for (auto_commit, polling_strategy) in [
            (
                AutoCommit::When(AutoCommitWhen::PollingMessages),
                PollingStrategy::next(),
            ),
            (AutoCommit::Disabled, PollingStrategy::first()),
        ] {
            let warnings = consume_three_messages(auto_commit, polling_strategy).await;

            assert!(
                warnings.is_empty(),
                "{auto_commit:?} with {polling_strategy:?}: {warnings:?}"
            );
        }
    }

    /// Runs `consume_messages` over three buffered messages, ends it with a blocked poll, and
    /// returns the warnings that the run logged.
    async fn consume_three_messages(
        auto_commit: AutoCommit,
        polling_strategy: PollingStrategy,
    ) -> Vec<String> {
        const OPERATION: u64 = 42;
        let mut consumer = builder()
            .partition(Some(1))
            .auto_commit(auto_commit)
            .polling_strategy(polling_strategy)
            .build();
        consumer.initialized = true;
        consumer.buffered_messages =
            VecDeque::from(vec![message_at(1), message_at(2), message_at(3)]);
        consumer.deferred_polls = Some(deferred::DeferredPolls::with_error(
            &consumer,
            IggyError::LaneBlocked(OPERATION),
        ));
        let handler = RecordMessages {
            offsets: std::sync::Mutex::default(),
            shutdown: std::sync::Mutex::new(None),
        };
        let (_shutdown, receiver) = tokio::sync::oneshot::channel();
        let recorder = LevelReports::warnings();
        let _subscriber = tracing::subscriber::set_default(recorder.clone());

        let result = timeout(POLL_TIMEOUT, consumer.consume_messages(&handler, receiver)).await;

        assert_eq!(result, Ok(Err(IggyError::LaneBlocked(OPERATION))));
        assert_eq!(*handler.offsets.lock().unwrap(), [1, 2, 3]);
        recorder.events.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn given_an_uninitialized_consumer_when_polled_should_fail_each_poll_and_warn_once() {
        let mut consumer = builder_for(Consumer::default()).build();
        let recorder = LevelReports::warnings();
        let _subscriber = tracing::subscriber::set_default(recorder.clone());

        let first = poll_once(&mut consumer).await;
        let second = poll_once(&mut consumer).await;

        for polled in [first, second] {
            assert!(
                matches!(polled, Err(IggyError::InvalidConfiguration)),
                "every poll before init fails: {polled:?}"
            );
        }
        let warnings = recorder.events.lock().unwrap().clone();
        assert_eq!(
            warnings.len(),
            1,
            "only the first poll before init warns: {warnings:?}"
        );
        assert!(
            warnings[0].contains("was polled before init()"),
            "{warnings:?}"
        );
    }
}
