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
use bytes::Bytes;
use dashmap::DashMap;
use futures::Stream;
use futures_util::{FutureExt, StreamExt};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_common::ConsumerPosition;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    Client, ConsumerGroupClient, ConsumerOffsetClient, MessageClient, StreamClient, TopicClient,
};
use iggy_common::{
    Consumer, ConsumerKind, DiagnosticEvent, EncryptorKind, IdKind, Identifier, IggyDuration,
    IggyError, IggyMessage, IggyTimestamp, NO_ASSIGNED_PARTITION, NonZeroIggyDuration,
    PolledMessages, PollingKind, PollingStrategy,
};
use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};

const ORDERING: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;
type PollMessagesFuture = Pin<Box<dyn Future<Output = Result<PolledMessages, IggyError>> + Send>>;

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
        self.client
            .read()
            .await
            .store_consumer_offset(
                &self.consumer,
                &self.stream_id,
                &self.topic_id,
                Some(partition_id),
                offset,
            )
            .await?;
        // Nothing was consumed from this partition, so there is no captured context to keep.
        self.last_stored_offsets.insert(
            partition_id,
            ConsumerPosition {
                partition_id,
                offset,
                context: PartitionContext::default(),
            },
        );
        Ok(())
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
        if !allow_replay
            && self
                .last_stored_offsets
                .get(&position.partition_id)
                .is_some_and(|stored| {
                    same_incarnation_and_owner(stored.context, position.context)
                        && position.offset <= stored.offset
                })
        {
            return Ok(());
        }
        self.client
            .read()
            .await
            .store_consumer_position(&self.consumer, &self.stream_id, &self.topic_id, position)
            .await?;
        self.last_stored_offsets
            .insert(position.partition_id, position);
        Ok(())
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
}

/// Offsets are comparable only within one partition incarnation and owner. The metadata frontier
/// is left out: another group's ownership change can advance it between two polls of the same
/// partition.
fn same_incarnation_and_owner(left: PartitionContext, right: PartitionContext) -> bool {
    left.incarnation == right.incarnation && left.owner_generation == right.owner_generation
}

// SAFETY: IggyConsumer is Sync because:
// 1. The only non-Sync field is `poll_future: Option<PollMessagesFuture>`
// 2. `poll_future` is only accessed through `poll_next()` which requires `Pin<&mut Self>`
//    (exclusive mutable access), so concurrent access to `poll_future` is impossible
// 3. All other fields are inherently Sync (Arc<AtomicX>, Arc<DashMap>, etc.) or
//    only accessed through `&mut self` methods
// 4. All `&self` methods only access Sync-safe fields
unsafe impl Sync for IggyConsumer {}

/// Reads messages from the partitions of one topic and yields them one at a time.
///
/// A topic is split into partitions, and a partition is an ordered log that producers append to.
/// Every message sits at an *offset*, its position in that log. Reading is therefore always the
/// same three decisions: which partition to read, where in it to start, and how to keep track of
/// how far you got so the next run can continue there.
///
/// `IggyConsumer` handles all three. It fetches batches of messages from the server, keeps them in
/// an in-memory buffer, decrypts them when it has an encryptor, and records how far it has read.
/// **It implements [`Stream`], so consuming is a loop over [`StreamExt::next`].**
///
/// You can use a consumer as a worker draining a topic, a reader that replays a
/// partition from a chosen point, and a pool of consumers sharing a workload through a consumer
/// group.
///
/// # Creating a consumer
///
/// Easiest way is to use the [`IggyClient`] with a configured connection. Then:
/// - [`IggyClient::consumer()`] builds a standalone consumer, bound to the one partition passed
///   in.
/// - [`IggyClient::consumer_group()`] builds a member of a consumer group. The server gives every
///   partition to exactly one member, so several consumers using the same group name split the
///   topic between them and share one set of offsets.
///
/// Note, building never talks to the server. [`init()`](Self::init) must be awaited once before the
/// first message is read.
///
/// # Examples
///
/// A standalone consumer reading partition 1 with the defaults:
///
/// ```rust,no_run
/// use futures_util::StreamExt;
/// use iggy::prelude::*;
///
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
///
/// let mut consumer = client
///     .consumer("my-consumer", "my-stream", "my-topic", 1)?
///     .batch_length(100)
///     .poll_interval(IggyDuration::new_from_secs(1))
///     .build();
/// consumer.init().await?;
///
/// while let Some(received) = consumer.next().await {
///     match received {
///         Ok(received) => println!("Offset: {}", received.message.header.offset),
///         Err(error) => eprintln!("Failed to read a message: {error}"),
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// A group member that queues a commit for every message just before handing it over, and shuts
/// down cleanly:
///
/// ```rust,no_run
/// use futures_util::StreamExt;
/// use iggy::prelude::*;
///
/// # async fn handle(message: &IggyMessage) {}
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
///
/// let mut consumer = client
///     .consumer_group("order-workers", "my-stream", "my-topic")?
///     .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingEachMessage))
///     .polling_strategy(PollingStrategy::next())
///     .build();
/// consumer.init().await?;
///
/// let mut consumed = 0;
/// while let Some(received) = consumer.next().await {
///     match received {
///         Ok(received) => {
///             handle(&received.message).await;
///             consumed += 1;
///         }
///         Err(error) => eprintln!("Failed to read a message: {error}"),
///     }
///     if consumed == 100 {
///         break;
///     }
/// }
///
/// consumer.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// Committing by hand, so that a message the handler could not process comes back on the next
/// run. Every commit is one round trip, and no auto-commit setting substitutes, since each of them
/// also commits a message whose handler failed:
///
/// ```rust,no_run
/// use futures_util::StreamExt;
/// use iggy::prelude::*;
///
/// # async fn handle(message: &IggyMessage) -> Result<(), IggyError> { Ok(()) }
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
///
/// let mut consumer = client
///     .consumer("my-consumer", "my-stream", "my-topic", 1)?
///     .auto_commit(AutoCommit::Disabled)
///     .polling_strategy(PollingStrategy::next())
///     .build();
/// consumer.init().await?;
///
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
///
/// consumer.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Which partitions are read
///
/// A **standalone consumer** reads exactly one partition, the one passed to
/// [`IggyClient::consumer()`]. Covering a whole topic with several partitions
/// means running one consumer per partition and dividing the work yourself.
///
/// A **consumer group member** does not choose. The server hands every partition of the topic to
/// exactly one member, so consumers sharing a group name split the topic between them without
/// coordinating. [`ReceivedMessage::partition_id`] tells where a message came from.
///
/// What to know when working with consumer groups:
/// - With [`auto_join_consumer_group()`] (the default) a member joins during [`init()`](Self::init),
///   creating the group first if [`create_consumer_group_if_not_exists()`] is set (the default).
///   It rejoins on its own after a reconnect and whenever the server reports that its membership
///   is gone.
/// - Such a member does not poll until it is in the group. A join that fails is yielded as
///   `Some(Err(..))` after [`polling_retry_interval()`], and the next call tries again.
/// - Partitions are redistributed whenever members join or leave, so a member reads different
///   partitions over time and messages from several partitions interleave in its stream.
/// - More members than partitions leaves the surplus members without partitions. Such a member
///   keeps asking the server for an assignment, parking for [`polling_retry_interval()`] between
///   attempts. The partition count of the topic is the ceiling on how far one group can be
///   scaled out.
/// - The group shares one set of stored offsets, kept under the group name. Thus,
///   a partition taken over by another member continues where the previous one
///   committed.
///
/// # How messages are read
///
/// Reading is done by polling. One request fetches up to [`batch_length()`] messages. The consumer
/// passes the first one to the caller and buffers the rest. The next request is sent once that buffer
/// is empty.
///
/// [`poll_interval()`] sets the smallest gap between two requests, measured from one send to the
/// next. Without it the next request goes out as soon as the previous one is answered, which is
/// the fastest option but keeps a busy loop running against an idle topic.
///
/// [`polling_strategy()`] decides **where** in the partition reading begins:
///
/// | Strategy | Starts at |
/// | --- | --- |
/// | [`PollingStrategy::next()`] (default) | the message after the offset stored on the server, or the first message when nothing is stored yet |
/// | [`PollingStrategy::first()`] | the oldest message in the partition |
/// | [`PollingStrategy::last()`] | the end of the partition (returns up to [`batch_length()`] of the most recent messages) |
/// | [`PollingStrategy::offset()`] | a custom offset |
/// | [`PollingStrategy::timestamp()`] | the first message at or after a given point in time |
///
/// Only [`PollingStrategy::next()`] consults the offset stored on the server.
/// Use this if you want to resume where a previous run stopped. The other four are the starting
/// point for the first request to each partition. From then on the consumer asks that partition
/// for whatever follows the last message it handed over from it, and it keeps that position per
/// partition. A partition that moves to another member and back therefore continues from this
/// consumer's own position, not from where the other member got to, so a rebalance can repeat
/// messages under these strategies, which ignore the group's stored offsets by definition.
///
/// [`StreamExt::next`] yields `None` once [`shutdown()`](Self::shutdown) has been called, and never
/// otherwise: not when the topic is empty and not while the client is disconnected. A request that
/// comes back empty is not an error and not the end of the stream, it just means nothing new has
/// arrived yet.
///
/// A failed poll request waits [`polling_retry_interval()`] before yielding `Some(Err(..))` and leaves
/// the consumer usable for the next call. This delay also applies to terminal server errors and when
/// the ordinary poll interval is disabled. Connection and authentication failures are yielded at once
/// and pause polling until the client has reconnected and signed in again, which the consumer handles
/// automatically; the next call parks for [`polling_retry_interval()`] while polling is paused. Hence,
/// deciding when to give up on repeated errors is up to you.
///
/// After another client deletes and recreates a partition, one poll of it can fail with
/// [`IggyError::HistoryUnavailable`], or [`IggyError::ConsumerGroupPartitionNotOwned`] for a group
/// member, because the request still carries the context of the deleted partition. That context is
/// dropped with the failed request, so the next poll reads the new partition. Commits of positions
/// read before the change keep their context and are refused the same way.
///
/// With [`AutoCommitWhen::PollingMessages`], a missing response can leave the server's
/// cursor ahead of messages this consumer received. Continuing with
/// [`PollingStrategy::next()`] can skip those messages. See the
/// [poll recovery contract](MessageClient::poll_messages) for recovery from explicit
/// checkpoints for each partition.
///
/// For a boilerplate implementation of such a loop Iggy provides [`IggyConsumerMessageExt::consume_messages`].
///
/// # Tracking what has been read
///
/// An offset is the index tracking what has been already read from a partition by the consumer.
/// Managing the offset has implications on where consumers resume reading messages.
///
/// There are two positions (offsets) tracked in two different places:
/// - The **reading position** is held by the consumer, one per partition, and is the offset of the
///   last message handed over
///   ([`get_last_consumed_offset()`](Self::get_last_consumed_offset)). It dies with the process.
/// - The **stored offset** is committed to the server under the consumer or group name.
///   Its restart guarantees depend on the topic's consumer-offset durability policy.
///
/// Committing matters because [`PollingStrategy::next()`] resumes from the stored offset. A
/// consumer that never commits starts over from the same place on every run. Within a run it
/// stalls instead: the server serves the same messages again on every request, messages at or
/// below the reading position are dropped (see [Guarantees](#guarantees)), so the stream goes
/// quiet once the reading position is [`batch_length()`] or more ahead of the stored offset. Under
/// [`PollingStrategy::next()`], keep commits within [`batch_length()`] of the reading position.
///
/// [`auto_commit()`] decides when the consumer commits by itself:
///
/// | Setting | Commits |
/// | --- | --- |
/// | [`AutoCommit::Disabled`] | never, not even on [`shutdown()`](Self::shutdown). Commit with [`store_position()`](Self::store_position) |
/// | [`AutoCommit::Interval`] | on every tick, the reading position of every partition read so far |
/// | [`AutoCommitWhen::PollingMessages`] | sends the commit with the poll request itself, before your code sees the batch |
/// | [`AutoCommitWhen::ConsumingEachMessage`] | queued just before every message is handed over to the calling code. Commits queued faster than they are sent collapse into the latest one per partition |
/// | [`AutoCommitWhen::ConsumingEveryNthMessage`] | queued just before a message whose offset divides by `n` is handed over |
/// | [`AutoCommitWhen::ConsumingAllMessages`] | queued when the buffer of the current batch runs empty |
/// | [`AutoCommitAfter`] variants | once the handler returned, `Ok` or `Err`, and only under [`IggyConsumerMessageExt::consume_messages`], see below |
///
/// [`AutoCommit::IntervalOrWhen`] and [`AutoCommit::IntervalOrAfter`] combine an interval with a
/// message trigger. The default is [`AutoCommit::IntervalOrWhen`] with one second and
/// [`AutoCommitWhen::PollingMessages`].
/// Important implications of these settings:
/// - [`AutoCommitWhen::PollingMessages`] marks a batch as consumed while it is being delivered,
///   before your code has seen any of it. For a crash-safe option configure with [`AutoCommit::Disabled`]
///   and manually store the position of each handled message with [`Self::store_position()`].
/// - [`AutoCommitWhen::ConsumingEveryNthMessage`] tests the offset of a message, not a counter of
///   messages this process handled, so it commits at every `n`-th offset of the partition. With
///   `n = 0` the trigger never fires, so without an interval only [`shutdown()`](Self::shutdown)
///   commits.
/// - [`AutoCommitAfter::ConsumingAllMessages`] fires for the message whose offset equals the
///   partition head seen by the poll ([`ReceivedMessage::current_offset`]), not when the buffer
///   runs empty, so a consumer that lags behind commits nothing until it has caught up. Every
///   [`AutoCommitAfter`] variant commits after a handler that returned `Err` as well.
///
/// ## Guarantees
///
/// - **Each message is handed over once per consumer.** Messages whose offset is not greater than
///   the reading position of their partition are dropped before they reach the stream. Re-reading
///   a partition, or seeing a failed message again within the same consumer, needs
///   [`allow_replay()`], which turns that filter off. A new consumer starts with an empty filter.
/// - **Delivering at-least-once.** If you cannot tolerate missing any messages, use [`AutoCommit::Disabled`]
///   and store the position using [`Self::store_position()`] after handling a message. Every other
///   setting except the plain [`AutoCommit::After`] variants can commit a message before your
///   handler is done with it, so a crash in the handler loses it. [`AutoCommit::IntervalOrAfter`]
///   still commits on its interval tick, and [`shutdown()`](Self::shutdown) commits the reading
///   position under every setting but [`AutoCommit::Disabled`], a failed message included.
///
/// # Options and defaults
///
/// Everything is configured on the [`IggyConsumerBuilder`] before [`build()`] and is fixed
/// afterwards.
///
/// | Option | Default | Controls |
/// | --- | --- | --- |
/// | [`stream()`], [`topic()`], [`partition()`] | the values passed to the entry point | what is read. [`partition()`] is for standalone consumers, a group member ignores it with a warning and reads the server's assignment |
/// | [`batch_length()`] | 1000 | messages fetched per request |
/// | [`poll_interval()`] | none | smallest gap between two requests |
/// | [`polling_strategy()`] | [`PollingStrategy::next()`] | where reading each partition starts |
/// | [`auto_commit()`] | [`AutoCommit::IntervalOrWhen`], one second, [`AutoCommitWhen::PollingMessages`] | when offsets are committed |
/// | [`allow_replay()`] | off | whether a message can be handed over again |
/// | [`auto_join_consumer_group()`] | on | joining the group during [`init()`](Self::init) and again whenever the membership is lost. With [`do_not_auto_join_consumer_group()`] joining is up to the caller, and a poll without a membership fails with [`IggyError::ConsumerGroupMemberNotFound`] |
/// | [`create_consumer_group_if_not_exists()`] | on | creating the group when it is missing |
/// | [`polling_retry_interval()`] | one second | delay before yielding poll errors other than connection and authentication failures, and between attempts while polling is blocked or the member holds no partitions |
/// | [`init_retries()`] | none, one second apart | retries when the stream or topic is missing at [`init()`](Self::init) |
/// | [`offset_drain_timeout()`] | five seconds | how long [`shutdown()`](Self::shutdown) waits for pending commits |
/// | [`encryptor()`] | inherited from the client | decrypting payloads and user headers, see [Encryption](#encryption) |
///
/// The switches have inverse setters as well, such as [`without_poll_interval()`],
/// [`without_encryptor()`], [`do_not_auto_join_consumer_group()`] and
/// [`do_not_create_consumer_group_if_not_exists()`].
///
/// # Encryption
///
/// A consumer with an encryptor, inherited from the [`IggyClient`] or set with [`encryptor()`],
/// decrypts payloads and user headers before a message is yielded. That only works if the producer
/// encrypted them with the same key, which an [`IggyProducer`] and an `IggyConsumer` from the same
/// client share unless one of them overrides it on its builder. Without an encryptor the consumer
/// yields payloads as stored, encrypted or not.
///
/// A message that cannot be decrypted is yielded as an `Err` and the whole batch is dropped. The
/// next request fetches the same batch and fails the same way until
/// [`store_offset()`](Self::store_offset) moves the offset past it. Under
/// [`AutoCommitWhen::PollingMessages`] the server would have committed the batch with the poll and
/// skipped it for good, so [`init()`](Self::init) rejects that setting, the default included, with
/// [`IggyError::InvalidConfiguration`] when the consumer has an encryptor.
///
/// # Concurrency
///
/// `IggyConsumer` is `Send` and `Sync` but not `Clone`. Driving the stream
/// ([`StreamExt::next`]) and [`shutdown()`](Self::shutdown) take `&mut self`, so one task owns and
/// drives a consumer end to end. To read offsets or commit from another task, take an
/// [`IggyConsumerState`] via [`state()`](Self::state): it is a cheap clone of the shared
/// bookkeeping and needs no lock.
///
/// Besides the stream, a consumer runs background tasks: one watching the connection lifecycle,
/// one sending queued commits, and an interval commit task for the [`AutoCommit`] variants that
/// carry an interval. See [`init()`](Self::init).
///
/// # Shutting down
///
/// Call [`shutdown()`](Self::shutdown) once done consuming. It drains the commit tasks, commits
/// the reading position of every partition unless [`auto_commit()`] is [`AutoCommit::Disabled`],
/// leaves the consumer group and stops the background tasks. Dropping an `IggyConsumer` instead
/// skips the final commit and the group leave, so the server reassigns the member's partitions
/// only once the connection is gone. Commits already queued are still sent and the background
/// tasks still stop.
///
/// [`IggyClient`]: crate::prelude::IggyClient
/// [`IggyClient::consumer()`]: crate::prelude::IggyClient::consumer
/// [`IggyClient::consumer_group()`]: crate::prelude::IggyClient::consumer_group
/// [`IggyProducer`]: crate::prelude::IggyProducer
/// [`IggyConsumerBuilder`]: crate::prelude::IggyConsumerBuilder
/// [`IggyConsumerMessageExt::consume_messages`]: crate::prelude::IggyConsumerMessageExt::consume_messages
/// [`allow_replay()`]: crate::prelude::IggyConsumerBuilder::allow_replay
/// [`auto_commit()`]: crate::prelude::IggyConsumerBuilder::auto_commit
/// [`auto_join_consumer_group()`]: crate::prelude::IggyConsumerBuilder::auto_join_consumer_group
/// [`batch_length()`]: crate::prelude::IggyConsumerBuilder::batch_length
/// [`build()`]: crate::prelude::IggyConsumerBuilder::build
/// [`create_consumer_group_if_not_exists()`]: crate::prelude::IggyConsumerBuilder::create_consumer_group_if_not_exists
/// [`do_not_auto_join_consumer_group()`]: crate::prelude::IggyConsumerBuilder::do_not_auto_join_consumer_group
/// [`do_not_create_consumer_group_if_not_exists()`]: crate::prelude::IggyConsumerBuilder::do_not_create_consumer_group_if_not_exists
/// [`encryptor()`]: crate::prelude::IggyConsumerBuilder::encryptor
/// [`init_retries()`]: crate::prelude::IggyConsumerBuilder::init_retries
/// [`offset_drain_timeout()`]: crate::prelude::IggyConsumerBuilder::offset_drain_timeout
/// [`partition()`]: crate::prelude::IggyConsumerBuilder::partition
/// [`poll_interval()`]: crate::prelude::IggyConsumerBuilder::poll_interval
/// [`polling_retry_interval()`]: crate::prelude::IggyConsumerBuilder::polling_retry_interval
/// [`polling_strategy()`]: crate::prelude::IggyConsumerBuilder::polling_strategy
/// [`stream()`]: crate::prelude::IggyConsumerBuilder::stream
/// [`topic()`]: crate::prelude::IggyConsumerBuilder::topic
/// [`without_encryptor()`]: crate::prelude::IggyConsumerBuilder::without_encryptor
/// [`without_poll_interval()`]: crate::prelude::IggyConsumerBuilder::without_poll_interval
///
/// A server-side auto-commit poll can fail with `TooManyConsumerOffsets` when
/// its consumer needs a new offset key at the partition's configured limit.
/// The rejected poll returns no messages. Other auto-commit modes store the
/// same key through a client request. Background and shutdown stores log a
/// capacity failure but do not yield it through this stream. Only
/// [`AutoCommit::Disabled`] avoids automatic key allocation. Existing keys
/// remain usable.
pub struct IggyConsumer {
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
    /// The next offset to ask each partition for. Empty under [`PollingStrategy::next()`], which
    /// leaves the continuation to the offset stored on the server.
    next_offsets: Arc<DashMap<u32, ConsumerPosition>>,
    poll_interval_micros: u64,
    batch_length: u32,
    auto_commit: AutoCommit,
    auto_commit_after_polling: bool,
    auto_join_consumer_group: bool,
    create_consumer_group_if_not_exists: bool,
    state: IggyConsumerState,
    poll_future: Option<PollMessagesFuture>,
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
    last_polled_at: Arc<AtomicU64>,
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
        polling_interval: Option<IggyDuration>,
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
            next_offsets: Arc::new(DashMap::new()),
            poll_interval_micros: polling_interval.map_or(0, |interval| interval.as_micros()),
            state,
            poll_future: None,
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
            last_polled_at: Arc::new(AtomicU64::new(0)),
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
    /// [`allow_replay`](crate::prelude::IggyConsumerBuilder::allow_replay).
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

    /// Commit a message's captured partition incarnation and owner using
    /// [`ReceivedMessage::position`]. A position from a deleted incarnation or a previous owner is
    /// refused without updating replacement progress.
    ///
    /// # Errors
    /// Returns the server's storage, permission, history or ownership error.
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
    /// `None` until this consumer has successfully stored an offset for that partition.
    ///
    /// The value is this consumer's own record of what it committed, kept in memory rather than
    /// read back from the server.
    /// Under auto-commit-on-poll (the default) this can trail the server by up to one batch.
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
    /// [Tracking what has been read](IggyConsumer#tracking-what-has-been-read). `init()` spawns the
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
    /// Under auto-commit-on-poll (the default) that record trails the server by one batch, so every
    /// tick re-sends the reading position and the server, which takes an explicit store as is,
    /// moves its offset back to it until the next poll.
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidConfiguration`] when the consumer has an encryptor and
    ///   [`auto_commit()`](crate::prelude::IggyConsumerBuilder::auto_commit) is
    ///   [`AutoCommitWhen::PollingMessages`], checked before anything is sent. See
    ///   [Encryption](IggyConsumer#encryption).
    /// - [`IggyError::StreamNameNotFound`], [`IggyError::StreamIdNotFound`] or [`IggyError::TopicNameNotFound`],
    ///   [`IggyError::TopicIdNotFound`] when the stream or the topic still does not exist once the retries are
    ///   exhausted.
    /// - [`IggyError::ConsumerGroupNameNotFound`] when the consumer group does not exist
    ///   and its auto creation is disabled.
    /// - Any error returned by the server while looking up the stream or the topic, or
    ///   while creating or joining the consumer group. Such an error ends initialization
    ///   immediately instead of consuming a retry.
    pub async fn init(&mut self) -> Result<(), IggyError> {
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
        self.init_consumer_group().await?;

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
                    _ = sleep(interval.get_duration()) => {}
                    _ = notify.notified() => {}
                }

                // Checked before storing: `shutdown()` runs its own final flush as a
                // group member and then leaves, so a store past this point would hit
                // a group we've since left. After a bare `Drop` nothing flushes.
                if shutdown.load(ORDERING) {
                    trace!("Shutdown signal received, stopping background offset storage");
                    break;
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

        tokio::spawn(async move {
            while let Some(event) = receiver.next().await {
                trace!("Received diagnostic event: {event}");
                match event {
                    DiagnosticEvent::Shutdown => {
                        warn!("Consumer has been shutdown");
                        joined_consumer_group.store(false, ORDERING);
                        can_poll.store(false, ORDERING);
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
            }
        })
    }

    fn create_poll_messages_future(
        &self,
    ) -> impl Future<Output = Result<PolledMessages, IggyError>> + use<> {
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let partition_id = self.partition_id;
        let consumer = self.consumer.clone();
        let polling_strategy = self.polling_strategy;
        let next_offsets = self.next_offsets.clone();
        let client = self.client.clone();
        let count = self.batch_length;
        let auto_commit_after_polling = self.auto_commit_after_polling;
        let interval = self.poll_interval_micros;
        let last_polled_at = self.last_polled_at.clone();
        let can_poll = self.can_poll.clone();
        let retry_interval = self.reconnection_retry_interval;
        let last_consumed_offset = self.state.last_consumed_offsets.clone();
        let allow_replay = self.allow_replay;
        let is_consumer_group = self.is_consumer_group;
        let auto_join_consumer_group = self.auto_join_consumer_group;
        let create_consumer_group_if_not_exists = self.create_consumer_group_if_not_exists;
        let joined_consumer_group = self.joined_consumer_group.clone();
        let consumer_name = self.consumer_name.clone();

        async move {
            if interval > 0 {
                Self::wait_before_polling(interval, last_polled_at.load(ORDERING)).await;
            }

            while !can_poll.load(ORDERING) {
                trace!("Cannot poll yet, waiting {retry_interval}...");
                sleep(retry_interval.get_duration()).await;
            }

            // A member that joins on its own is in the group before it polls. One built with
            // `do_not_auto_join_consumer_group()` polls right away and gets a missing
            // membership reported as a poll error.
            if is_consumer_group
                && auto_join_consumer_group
                && !joined_consumer_group.load(ORDERING)
                && let Err(error) = Self::initialize_consumer_group(
                    client.clone(),
                    create_consumer_group_if_not_exists,
                    stream_id.clone(),
                    topic_id.clone(),
                    &consumer_name,
                    joined_consumer_group.clone(),
                )
                .await
            {
                error!(
                    "Failed to join consumer group: {consumer_name} for stream: {stream_id}, topic: {topic_id}. {error}"
                );
                sleep(retry_interval.get_duration()).await;
                return Err(error);
            }

            trace!("Sending poll messages request");
            last_polled_at.store(IggyTimestamp::now().into(), ORDERING);
            // A refusal does not name its partition, and it always answers the last request.
            let polled_partition = AtomicU32::new(NO_ASSIGNED_PARTITION);
            // The map guard is dropped inside `map_or`, and the only writer is `poll_next`,
            // which runs after this future has returned, so the lookup cannot block.
            let strategy_for = |partition: u32| {
                polled_partition.store(partition, ORDERING);
                next_offsets
                    .get(&partition)
                    .map_or(polling_strategy, |position| {
                        PollingStrategy::offset(position.offset).with_context(position.context)
                    })
            };
            let polled_messages = client
                .read()
                .await
                .poll_messages_with_strategy_for(
                    &stream_id,
                    &topic_id,
                    partition_id,
                    &consumer,
                    &strategy_for,
                    count,
                    auto_commit_after_polling,
                )
                .await;

            if let Ok(polled) = &polled_messages
                && polled.partition_id == NO_ASSIGNED_PARTITION
            {
                next_offsets.clear();
                trace!(
                    "No partition assigned to consumer: {consumer_name}, waiting {retry_interval}..."
                );
                sleep(retry_interval.get_duration()).await;
            }

            if let Ok(mut polled_messages) = polled_messages {
                if polled_messages.messages.is_empty() {
                    return Ok(polled_messages);
                }

                let partition_id = polled_messages.partition_id;
                let consumed = last_consumed_offset
                    .get(&partition_id)
                    .filter(|position| {
                        same_incarnation_and_owner(position.context, polled_messages.context)
                    })
                    .map(|position| position.offset);
                if !allow_replay && let Some(consumed) = consumed {
                    polled_messages
                        .messages
                        .retain(|message| message.header.offset > consumed);
                    polled_messages.count = polled_messages.messages.len() as u32;
                }

                trace!(
                    "Last consumed offset: {consumed:?}, current offset: {}, in partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}, consumer: {consumer}",
                    polled_messages.current_offset
                );
                return Ok(polled_messages);
            }

            let error = polled_messages.unwrap_err();
            if matches!(
                error,
                IggyError::HistoryUnavailable | IggyError::ConsumerGroupPartitionNotOwned(..)
            ) {
                next_offsets.remove(&polled_partition.load(ORDERING));
            }
            error!("Failed to poll messages: {error}");

            if is_consumer_group
                && auto_join_consumer_group
                && matches!(&error, IggyError::ConsumerGroupMemberNotFound(..))
            {
                info!(
                    "Consumer group membership was revoked for consumer: {consumer_name}, stream: {stream_id}, topic: {topic_id}. Rejoining on the next poll..."
                );
                joined_consumer_group.store(false, ORDERING);
                return Ok(PolledMessages::empty());
            }

            // Connection and auth errors: disable polling until the event task
            // re-enables it after reconnection and rejoin complete. Yielded at
            // once: the next poll already parks on `can_poll`, so a retry sleep
            // here would only delay the caller's view of an error it does not
            // act on.
            if matches!(
                error,
                IggyError::Disconnected | IggyError::Unauthenticated | IggyError::StaleClient
            ) {
                can_poll.store(false, ORDERING);
                if is_consumer_group {
                    joined_consumer_group.store(false, ORDERING);
                }
                return Err(error);
            }
            trace!("Retrying to poll messages in {retry_interval}...");
            sleep(retry_interval.get_duration()).await;
            Err(error)
        }
    }

    async fn wait_before_polling(interval: u64, last_sent_at: u64) {
        if interval == 0 {
            return;
        }

        let now: u64 = IggyTimestamp::now().into();
        if now < last_sent_at {
            warn!(
                "Returned monotonic time went backwards, now < last_sent_at: ({now} < {last_sent_at})"
            );
            sleep(Duration::from_micros(interval)).await;
            return;
        }

        let elapsed = now - last_sent_at;
        if elapsed >= interval {
            trace!("No need to wait before polling messages. {now} - {last_sent_at} = {elapsed}");
            return;
        }

        let remaining = interval - elapsed;
        trace!(
            "Waiting for {remaining} microseconds before polling messages... {interval} - {elapsed} = {remaining}"
        );
        sleep(Duration::from_micros(remaining)).await;
    }

    async fn initialize_consumer_group(
        client: IggyRwLock<ClientWrapper>,
        create_consumer_group_if_not_exists: bool,
        stream_id: Arc<Identifier>,
        topic_id: Arc<Identifier>,
        consumer_name: &str,
        joined_consumer_group: Arc<AtomicBool>,
    ) -> Result<(), IggyError> {
        if joined_consumer_group.load(ORDERING) {
            return Ok(());
        }

        let client = client.read().await;
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
}

/// A single message handed over by an [`IggyConsumer`].
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
/// See [How messages are read](IggyConsumer#how-messages-are-read) for errors, `None` and polling.
impl Stream for IggyConsumer {
    type Item = Result<ReceivedMessage, IggyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.shutdown.load(ORDERING) {
            return Poll::Ready(None);
        }

        let partition_id = self.state.partition_id();
        if let Some(message) = self.buffered_messages.pop_front() {
            {
                self.state.last_consumed_offsets.insert(
                    partition_id,
                    ConsumerPosition {
                        partition_id,
                        offset: message.header.offset,
                        context: self.buffered_context,
                    },
                );

                if (self.store_after_every_nth_message > 0
                    && message.header.offset % self.store_after_every_nth_message == 0)
                    || self.store_offset_after_each_message
                {
                    self.send_store_offset(ConsumerPosition {
                        partition_id,
                        offset: message.header.offset,
                        context: self.buffered_context,
                    });
                }
            }

            // Popping above may have left the buffer empty, so the next turn polls the server.
            // `polling_strategy` is only where reading a partition starts; from then on each
            // poll continues after the last message handed over from that partition.
            if self.buffered_messages.is_empty() {
                if self.polling_strategy.kind != PollingKind::Next {
                    self.next_offsets.insert(
                        partition_id,
                        ConsumerPosition {
                            partition_id,
                            offset: message.header.offset + 1,
                            context: self.buffered_context,
                        },
                    );
                }

                if self.store_offset_after_all_messages {
                    self.send_store_offset(ConsumerPosition {
                        partition_id,
                        offset: message.header.offset,
                        context: self.buffered_context,
                    });
                }
            }

            // Not the position of this message but the newest offset the partition had when the
            // batch was polled. So every message of a batch reports the same value.
            return Poll::Ready(Some(Ok(ReceivedMessage::new(
                message,
                self.buffered_current_offset,
                partition_id,
                self.buffered_context,
            ))));
        }

        // A used (and therefore invalid) future was dropped, thus create a fresh one.
        if self.poll_future.is_none() {
            let future = self.create_poll_messages_future();
            self.poll_future = Some(Box::pin(future));
        }

        while let Some(future) = self.poll_future.as_mut() {
            match future.poll_unpin(cx) {
                Poll::Ready(Ok(polled_messages)) => {
                    let PolledMessages {
                        partition_id,
                        current_offset,
                        context,
                        messages,
                        ..
                    } = polled_messages;
                    let mut messages = VecDeque::from(messages);
                    let Some(mut first) = messages.pop_front() else {
                        self.poll_future = Some(Box::pin(self.create_poll_messages_future()));
                        continue;
                    };

                    // Only a response that carries messages names a partition; an empty one can
                    // carry a sentinel instead of a real id.
                    self.state
                        .current_partition_id
                        .store(partition_id, ORDERING);

                    if let Some(ref encryptor) = self.encryptor {
                        for message in std::iter::once(&mut first).chain(messages.iter_mut()) {
                            let offset = message.header.offset;
                            let payload = encryptor.decrypt(&message.payload);
                            if let Err(error) = payload {
                                self.poll_future = None;
                                error!(
                                    "Failed to decrypt the message payload at offset: {offset}, partition ID: {partition_id}",
                                );
                                return Poll::Ready(Some(Err(error)));
                            }

                            let payload = payload.unwrap();
                            message.payload = Bytes::from(payload);
                            message.header.payload_length = message.payload.len() as u32;

                            if let Some(ref user_headers) = message.user_headers {
                                let decrypted_headers = encryptor.decrypt(user_headers);
                                if let Err(error) = decrypted_headers {
                                    self.poll_future = None;
                                    error!(
                                        "Failed to decrypt the message user headers at offset: {offset}, partition ID: {partition_id}",
                                    );
                                    return Poll::Ready(Some(Err(error)));
                                }
                                let decrypted_headers = decrypted_headers.unwrap();
                                message.header.user_headers_length = decrypted_headers.len() as u32;
                                message.user_headers = Some(Bytes::from(decrypted_headers));
                            }
                        }
                    }

                    // A poll is only sent once the buffer has run empty, so nothing is overwritten.
                    self.buffered_messages = messages;
                    self.buffered_context = context;
                    self.buffered_current_offset = current_offset;

                    if self.polling_strategy.kind != PollingKind::Next {
                        self.next_offsets.insert(
                            partition_id,
                            ConsumerPosition {
                                partition_id,
                                offset: first.header.offset + 1,
                                context,
                            },
                        );
                    }

                    self.state.last_consumed_offsets.insert(
                        partition_id,
                        ConsumerPosition {
                            partition_id,
                            offset: first.header.offset,
                            context,
                        },
                    );

                    if (self.store_after_every_nth_message > 0
                        && first.header.offset % self.store_after_every_nth_message == 0)
                        || self.store_offset_after_each_message
                        || (self.store_offset_after_all_messages
                            && self.buffered_messages.is_empty())
                    {
                        self.send_store_offset(ConsumerPosition {
                            partition_id,
                            offset: first.header.offset,
                            context,
                        });
                    }

                    // Drop future since it is [invalid after being ready](https://doc.rust-lang.org/std/future/trait.Future.html#panics)
                    self.poll_future = None;
                    return Poll::Ready(Some(Ok(ReceivedMessage::new(
                        first,
                        current_offset,
                        partition_id,
                        context,
                    ))));
                }
                Poll::Ready(Err(err)) => {
                    self.poll_future = None;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Pending
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
    ///   auto-commit-on-poll (the default) the poll already committed the whole batch, so this
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
            // Replay governs reading only: this flush must never move a stored offset back.
            let allow_replay = false;
            for position in self.state.last_consumed_positions() {
                if let Err(error) = self
                    .state
                    .store_consumer_position(position, allow_replay)
                    .await
                {
                    warn!(?position, %error, "Final consumer checkpoint was not acknowledged");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_wrappers::client_wrapper::ClientWrapper;
    use crate::clients::consumer_builder::IggyConsumerBuilder;
    use crate::tcp::tcp_client::TcpClient;
    use iggy_binary_protocol::codes::{
        GET_CLUSTER_METADATA_CODE, GET_CONSUMER_OFFSET_ROUTING_CODE, GET_POLL_ROUTING_CODE,
        POLL_MESSAGES_ON_PRIMARY_CODE, SYNC_CONSUMER_GROUP_CODE,
    };
    use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
    use iggy_binary_protocol::requests::messages::PollMessagesRequest;
    use iggy_binary_protocol::requests::system::SessionIdentity;
    use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
    use iggy_binary_protocol::responses::messages::PollRoutingResponse;
    use iggy_binary_protocol::responses::system::get_cluster_metadata::{
        ClusterMetadataResponse, ClusterNodeResponse,
    };
    use iggy_binary_protocol::{
        Command, HEADER_SIZE, Operation, ReplyHeader, RequestHeader, STATUS_OK, WireDecode,
        WireEncode,
    };
    use iggy_common::locking::IggyRwLockFn;
    use iggy_common::{
        Aes256GcmEncryptor, BinaryTransport, ClientState, TcpClientConfig, VsrSessionControl,
    };
    use std::str::FromStr;
    use std::task::Waker;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tracing::field::Field;
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Level, Metadata, Subscriber};

    const POLL_RETRY_INTERVAL: Duration = Duration::from_millis(10);
    const POLL_TIMEOUT: Duration = Duration::from_secs(2);

    fn builder_for(consumer: Consumer) -> IggyConsumerBuilder {
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

        assert_eq!(consumer.buffered_messages.len(), 1);
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
    async fn consumer_group_should_not_create_poll_future_after_shutdown() {
        let mut consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .auto_commit(AutoCommit::Disabled)
            .build();
        let mut context = Context::from_waker(Waker::noop());

        consumer.shutdown().await.unwrap();

        assert!(matches!(
            Pin::new(&mut consumer).poll_next(&mut context),
            Poll::Ready(None)
        ));
        assert!(consumer.poll_future.is_none());
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

    fn next_offset(consumer: &IggyConsumer, partition_id: u32) -> Option<u64> {
        consumer
            .next_offsets
            .get(&partition_id)
            .map(|position| position.offset)
    }

    #[test]
    fn group_member_should_continue_each_partition_after_its_last_message() {
        let mut consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .polling_strategy(PollingStrategy::first())
            .auto_commit(AutoCommit::Disabled)
            .build();

        hand_over_batch(&mut consumer, 3, vec![message_at(10), message_at(11)]);
        assert_eq!(next_offset(&consumer, 3), Some(12));
        assert_eq!(consumer.polling_strategy, PollingStrategy::first());

        hand_over_batch(&mut consumer, 4, vec![message_at(7)]);
        assert_eq!(next_offset(&consumer, 4), Some(8));
        assert_eq!(next_offset(&consumer, 3), Some(12));
    }

    #[test]
    fn next_strategy_should_leave_the_continuation_to_the_server() {
        let mut consumer = builder_for(Consumer::group(Identifier::numeric(1).unwrap()))
            .auto_commit(AutoCommit::Disabled)
            .build();

        hand_over_batch(&mut consumer, 3, vec![message_at(10), message_at(11)]);

        assert!(consumer.next_offsets.is_empty());
    }

    /// The requests a server from [`connect_to_test_server`] served.
    #[derive(Debug, Default)]
    struct Served {
        polled_partitions: Vec<u32>,
        stored_offsets: Vec<u64>,
    }

    /// Connects a signed-in client to a single-node server whose group assigns `partitions`. The
    /// server refuses every poll with `poll_refusal` and accepts every offset store.
    async fn connect_to_test_server(
        partitions: Vec<u32>,
        poll_refusal: IggyError,
    ) -> (TcpClient, Arc<std::sync::Mutex<Served>>) {
        /// A result section without entries, which accepts a replicated request.
        const ACCEPTED: [u8; size_of::<u32>()] = STATUS_OK.to_le_bytes();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let node = ClusterNodeResponse {
            name: "node".to_owned(),
            ip: address.ip().to_string(),
            tcp_port: address.port(),
            quic_port: 0,
            http_port: 0,
            websocket_port: 0,
            role: 1,
            status: 1,
        };
        let served = Arc::new(std::sync::Mutex::new(Served::default()));
        let server_served = Arc::clone(&served);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0; HEADER_SIZE];
            while stream.read_exact(&mut header).await.is_ok() {
                let request: RequestHeader =
                    bytemuck::checked::try_pod_read_unaligned(&header).unwrap();
                let mut body = vec![0; request.size as usize - HEADER_SIZE];
                stream.read_exact(&mut body).await.unwrap();
                let mut status = STATUS_OK;
                let reply = match (request.operation, u32::from_le_bytes(request.reserved)) {
                    (Operation::StoreConsumerOffset, _) => {
                        let store = StoreConsumerOffsetRequest::decode_from(&body).unwrap();
                        server_served
                            .lock()
                            .unwrap()
                            .stored_offsets
                            .push(store.offset);
                        Bytes::from_static(&ACCEPTED)
                    }
                    (_, GET_CLUSTER_METADATA_CODE) => ClusterMetadataResponse {
                        name: "cluster".to_owned(),
                        nodes: vec![node.clone()],
                    }
                    .to_bytes(),
                    (_, SYNC_CONSUMER_GROUP_CODE) => SyncConsumerGroupResponse {
                        generation: 1,
                        partitions: partitions.clone(),
                    }
                    .to_bytes(),
                    (_, GET_POLL_ROUTING_CODE | GET_CONSUMER_OFFSET_ROUTING_CODE) => {
                        PollRoutingResponse {
                            consumer_session: SessionIdentity {
                                client_id: 1,
                                session: 1,
                                metadata_watermark: 0,
                            },
                            context: PartitionContext::default(),
                            primary: node.clone(),
                        }
                        .to_bytes()
                    }
                    (_, POLL_MESSAGES_ON_PRIMARY_CODE) => {
                        let poll = PollMessagesRequest::decode_from(&body).unwrap();
                        server_served
                            .lock()
                            .unwrap()
                            .polled_partitions
                            .push(poll.partition_id.unwrap());
                        status = poll_refusal.as_code();
                        Bytes::new()
                    }
                    (operation, code) => panic!("unexpected {operation:?} request, code: {code}"),
                };
                let reply_header = ReplyHeader {
                    command: Command::Reply,
                    operation: request.operation,
                    client: request.client,
                    request: request.request,
                    size: u32::try_from(HEADER_SIZE + reply.len()).unwrap(),
                    status,
                    ..Default::default()
                };
                stream
                    .write_all(bytemuck::bytes_of(&reply_header))
                    .await
                    .unwrap();
                stream.write_all(&reply).await.unwrap();
            }
        });
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: address.to_string(),
            ..Default::default()
        }))
        .unwrap();
        Client::connect(&client).await.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        client.set_state(ClientState::Authenticated).await;
        (client, served)
    }

    #[tokio::test]
    async fn group_member_should_drop_only_the_continuation_of_the_refused_partition() {
        const REFUSED_PARTITION: u32 = 3;
        const RETAINED_PARTITION: u32 = 4;
        for refusal in [
            IggyError::HistoryUnavailable,
            IggyError::ConsumerGroupPartitionNotOwned(0, 0),
        ] {
            let (client, served) = connect_to_test_server(
                vec![REFUSED_PARTITION, RETAINED_PARTITION],
                refusal.clone(),
            )
            .await;
            let mut consumer = builder_on(client, Consumer::group(Identifier::numeric(1).unwrap()))
                .polling_strategy(PollingStrategy::first())
                .do_not_auto_join_consumer_group()
                .auto_commit(AutoCommit::Disabled)
                .polling_retry_interval(NonZeroIggyDuration::new(POLL_RETRY_INTERVAL).unwrap())
                .build();
            hand_over_batch(&mut consumer, REFUSED_PARTITION, vec![message_at(10)]);
            hand_over_batch(&mut consumer, RETAINED_PARTITION, vec![message_at(20)]);

            let polled = timeout(POLL_TIMEOUT, consumer.next()).await.unwrap();

            assert!(
                matches!(&polled, Some(Err(error)) if *error == refusal),
                "{refusal}"
            );
            assert_eq!(
                served.lock().unwrap().polled_partitions,
                [REFUSED_PARTITION]
            );
            assert_eq!(next_offset(&consumer, REFUSED_PARTITION), None, "{refusal}");
            assert_eq!(
                next_offset(&consumer, RETAINED_PARTITION),
                Some(21),
                "{refusal}: a refusal of another partition must keep this continuation"
            );
        }
    }

    /// A position delivered at `offset` by a poll that saw the metadata frontier `metadata_op`.
    fn position_at(offset: u64, metadata_op: u64) -> ConsumerPosition {
        ConsumerPosition {
            partition_id: 1,
            offset,
            context: PartitionContext {
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
            .build();
        let consumed = position_at(9, 1);
        let stored = position_at(19, 1);
        consumer.state.last_consumed_offsets.insert(1, consumed);
        consumer.state.last_stored_offsets.insert(1, stored);

        consumer.shutdown().await.unwrap();

        assert!(served.lock().unwrap().stored_offsets.is_empty());
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

        assert_eq!(served.lock().unwrap().stored_offsets, [OFFSET]);
        assert_eq!(consumer.get_last_stored_offset(1), Some(OFFSET));
    }

    /// Records the error events emitted on the test thread. The current-thread runtime of
    /// `#[tokio::test]` polls the tasks it spawns on that thread, so their events count too.
    #[derive(Clone, Default)]
    struct ErrorReports(Arc<std::sync::Mutex<Vec<String>>>);

    impl Subscriber for ErrorReports {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            *metadata.level() == Level::ERROR
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
            self.0.lock().unwrap().push(fields.join(", "));
        }

        fn enter(&self, _: &Id) {}

        fn exit(&self, _: &Id) {}
    }

    impl ErrorReports {
        /// Waits for the first report, or returns none after [`POLL_TIMEOUT`].
        async fn wait(&self) -> Vec<String> {
            let first = async {
                loop {
                    let reports = self.0.lock().unwrap().clone();
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
        let recorder = ErrorReports::default();
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
        let recorder = ErrorReports::default();
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

    /// Polls once as a group member on a client that is not connected. The outcome must be an
    /// error, never an endless wait for a join.
    async fn poll_once_as_group_member(
        builder: IggyConsumerBuilder,
    ) -> Option<Result<ReceivedMessage, IggyError>> {
        let mut consumer = builder
            .polling_retry_interval(NonZeroIggyDuration::new(POLL_RETRY_INTERVAL).unwrap())
            .build();
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
}
