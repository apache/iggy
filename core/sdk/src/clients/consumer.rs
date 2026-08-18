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
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    Client, ConsumerGroupClient, ConsumerOffsetClient, MessageClient, StreamClient, TopicClient,
};
use iggy_common::{
    Consumer, ConsumerKind, DiagnosticEvent, EncryptorKind, IdKind, Identifier, IggyDuration,
    IggyError, IggyMessage, IggyTimestamp, PolledMessages, PollingKind, PollingStrategy,
};
use std::collections::VecDeque;
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
    Interval(IggyDuration),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval or depending on the mode when consuming the messages.
    IntervalOrWhen(IggyDuration, AutoCommitWhen),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval or depending on the mode after consuming the messages.
    ///
    /// **This will only work with the `IggyConsumerMessageExt` trait when using `consume_messages()`.**
    IntervalOrAfter(IggyDuration, AutoCommitAfter),
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
/// an in-memory buffer, decrypts them when the client is configured with an encryptor, and records
/// how far it has read. **It implements [`Stream`], so consuming is a loop over [`StreamExt::next`].**
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
/// A group member that commits every message right after it is handed over, and shuts down
/// cleanly:
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
///     handle(&received?.message).await;
///     consumed += 1;
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
/// Committing by hand, so that a message the handler could not process comes back:
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
///     // Without this, messages already handed over once are never handed over again.
///     .allow_replay()
///     .build();
/// consumer.init().await?;
///
/// while let Some(received) = consumer.next().await {
///     let received = received?;
///     if handle(&received.message).await.is_ok() {
///         consumer
///             .store_offset(received.message.header.offset, Some(received.partition_id))
///             .await?;
///     }
/// }
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
/// coordinating. [`partition_id()`](Self::partition_id) reports where the last message came from.
///
/// What to know when working with consumer groups:
/// - A member joins during [`init()`](Self::init), creating the group first if
///   [`create_consumer_group_if_not_exists()`] is set (the default). It rejoins on its own after a
///   reconnect and whenever the server reports that its membership is gone.
/// - Until the join has succeeded the consumer does not poll. It waits for
///   [`polling_retry_interval()`] and tries again.
/// - Partitions are redistributed whenever members join or leave, so a member reads different
///   partitions over time and messages from several partitions interleave in its stream.
/// - More members than partitions leaves the surplus members idle. The partition count of the
///   topic is the ceiling on how far one group can be scaled out.
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
/// [`poll_interval()`] sets a timeout between two requests should the buffer be empty.
/// Without it the next request goes out as soon as the previous one is
/// answered, which is the fastest option but keeps a busy loop running against an idle topic.
///
/// [`polling_strategy()`] decides **where** in the partition reading begins:
///
/// | Strategy | Starts at |
/// | --- | --- |
/// | [`PollingStrategy::next()`] (default) | the message after the offset that is stored on the server |
/// | [`PollingStrategy::first()`] | the oldest message in the partition |
/// | [`PollingStrategy::last()`] | the end of the partition (returns up to [`batch_length()`] of the most recent messages) |
/// | [`PollingStrategy::offset()`] | a custom offset |
/// | [`PollingStrategy::timestamp()`] | the first message at or after a given point in time |
///
/// Only [`PollingStrategy::next()`] consults the offset stored on the server.
/// Use this if you want to resume where a previous run stopped. The other four are starting points for the first request only.
/// From the second request onwards, the consumer asks for whatever follows it.
///
/// Note, when polling, [`StreamExt::next`] never returns `None`, not when the topic is empty
/// and not while the client is disconnected. A `while let Some(..)` loop only ends when the
/// loop body breaks out of it. A request that comes back empty is not an error and not the end of
/// the stream, it just means nothing new has arrived yet.
///
/// A failed request is yielded as `Some(Err(..))` and leaves the consumer usable, while the next call
/// retries. Connection and authentication failures pause polling until the client has reconnected
/// and signed in again, which the consumer handles automatically. Hence, deciding when to give up on
/// repeated errors is up to you.
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
/// - The **stored offset** lives on the server under the consumer name, or the group name for a
///   group. This offset survives restarts. Writing it is called *storing* or *committing* an offset.
///
/// Committing matters because [`PollingStrategy::next()`] resumes from the stored offset. A
/// consumer that never commits keeps starting over from the same place.
/// Note, *committing* is a request of its own, not a side effect of reading and therefore controllable.
///
/// [`auto_commit()`] decides when the consumer commits by itself:
///
/// | Setting | Commits |
/// | --- | --- |
/// | [`AutoCommit::Disabled`] | never on its own, decide manually with [`store_offset()`](Self::store_offset) or on [`shutdown()`](Self::shutdown) |
/// | [`AutoCommit::Interval`] | on every tick, for every partition read so far |
/// | [`AutoCommitWhen::PollingMessages`] | with the poll request itself, before your code sees the batch |
/// | [`AutoCommitWhen::ConsumingEachMessage`] | after every message was handed over to the calling code |
/// | [`AutoCommitWhen::ConsumingEveryNthMessage`] | when the offset of a message handed over divides by `n` |
/// | [`AutoCommitWhen::ConsumingAllMessages`] | when the buffer of the current batch runs empty |
/// | [`AutoCommitAfter`] variants | as their [`AutoCommitWhen`] counterparts, but once the handler returned, and only under [`IggyConsumerMessageExt::consume_messages`] |
///
/// [`AutoCommit::IntervalOrWhen`] and [`AutoCommit::IntervalOrAfter`] combine an interval with a
/// message trigger. The default is [`AutoCommit::IntervalOrWhen`] with one second and
/// [`AutoCommitWhen::PollingMessages`].
/// Important implications of these defaults:
/// - [`AutoCommitWhen::PollingMessages`] marks a batch as consumed while it is being delivered,
///   before your code has seen any of it. If a crash must not skip messages, commit after handling
///   with [`AutoCommitWhen::ConsumingEachMessage`] instead.
/// - [`AutoCommitWhen::ConsumingEveryNthMessage`] tests the offset of a message, not a counter of
///   messages this process handled, so it commits at every `n`-th offset of the partition.
///
/// ## Guarantees
///
/// - **Each message is handed over once per consumer.** Messages whose offset is not greater than
///   the reading position of their partition are dropped before they reach the stream. Re-reading
///   a partition, or letting a message come back because your handler failed, needs
///   [`allow_replay()`], which turns that filter off.
/// - **Delivery is at-least-once.** A crash between handling a message and committing its offset
///   replays that message on the next run, so handlers have to tolerate seeing one twice. No
///   setting makes this exactly-once.
///
/// # Options and defaults
///
/// Everything is configured on the [`IggyConsumerBuilder`] before [`build()`] and is fixed
/// afterwards.
///
/// | Option | Default | Controls |
/// | --- | --- | --- |
/// | [`stream()`], [`topic()`], [`partition()`] | the values passed to the entry point | what is read |
/// | [`batch_length()`] | 1000 | messages fetched per request |
/// | [`poll_interval()`] | none | smallest gap between two requests |
/// | [`polling_strategy()`] | [`PollingStrategy::next()`] | where reading starts |
/// | [`auto_commit()`] | [`AutoCommit::IntervalOrWhen`], one second, [`AutoCommitWhen::PollingMessages`] | when offsets are committed |
/// | [`allow_replay()`] | off | whether a message can be handed over again |
/// | [`auto_join_consumer_group()`] | on | joining the group during [`init()`](Self::init) |
/// | [`create_consumer_group_if_not_exists()`] | on | creating the group when it is missing |
/// | [`polling_retry_interval()`] | one second | wait between attempts while polling is blocked |
/// | [`init_retries()`] | none, one second apart | retries when the stream or topic is missing at [`init()`](Self::init) |
/// | [`offset_drain_timeout()`] | five seconds | how long [`shutdown()`](Self::shutdown) waits for pending commits |
/// | [`encryptor()`] | inherited from the client | decrypting payloads and user headers |
///
/// The switches have inverse setters as well, such as [`without_poll_interval()`],
/// [`without_encryptor()`], [`do_not_auto_join_consumer_group()`] and
/// [`do_not_create_consumer_group_if_not_exists()`].
///
/// # Encryption
///
/// When the [`IggyClient`] was created with an encryptor, payloads and user headers are decrypted
/// before a message is yielded, which only works if the producer encrypted them with a matching
/// key. This is guaranteed if you spawned both, the [`IggyProducer`] and the [`IggyConsumer`] from the same [`IggyClient`].
/// A message that cannot be decrypted is yielded as an error and the rest of its batch is
/// discarded.
///
/// # Concurrency
///
/// A consumer hands you a stream to poll messages, but also spawns background tasks
/// for watching connection lifecycle changes and committing offsets. Refer to [`init()`](Self::init)
/// docs for more details.
///
/// `IggyConsumer` is `Send` and `Sync` but not `Clone`. Driving the stream
/// ([`StreamExt::next`]) and [`shutdown()`](Self::shutdown) take exclusively (`&mut self`)
/// Hence, one task owns and drives a given consumer end to end.
///
/// The following methods take `&self` and are safe to call from any other task holding a
/// `&IggyConsumer`: [`store_offset()`](Self::store_offset),
/// [`delete_offset()`](Self::delete_offset),
/// [`get_last_consumed_offset()`](Self::get_last_consumed_offset),
/// [`get_last_stored_offset()`](Self::get_last_stored_offset),
/// [`partition_id()`](Self::partition_id), [`name()`](Self::name), [`stream()`](Self::stream) and
/// [`topic()`](Self::topic).
///
/// # Shutting down
///
/// Call [`shutdown()`](Self::shutdown) once done consuming. It stops concurrent background tasks gracefully.
/// Note, that just dropping a `IggyConsumer` loses everything that is currently in-flight.
/// Read the docs of [`shutdown()`](Self::shutdown) for details.
///
/// [`IggyClient`]: crate::clients::client::IggyClient
/// [`IggyClient::consumer()`]: crate::clients::client::IggyClient::consumer
/// [`IggyClient::consumer_group()`]: crate::clients::client::IggyClient::consumer_group
/// [`IggyProducer`]: crate::clients::producer::IggyProducer
/// [`IggyConsumerBuilder`]: crate::clients::consumer_builder::IggyConsumerBuilder
/// [`IggyConsumerMessageExt::consume_messages`]: crate::consumer_ext::IggyConsumerMessageExt::consume_messages
/// [`allow_replay()`]: crate::clients::consumer_builder::IggyConsumerBuilder::allow_replay
/// [`auto_commit()`]: crate::clients::consumer_builder::IggyConsumerBuilder::auto_commit
/// [`auto_join_consumer_group()`]: crate::clients::consumer_builder::IggyConsumerBuilder::auto_join_consumer_group
/// [`batch_length()`]: crate::clients::consumer_builder::IggyConsumerBuilder::batch_length
/// [`build()`]: crate::clients::consumer_builder::IggyConsumerBuilder::build
/// [`create_consumer_group_if_not_exists()`]: crate::clients::consumer_builder::IggyConsumerBuilder::create_consumer_group_if_not_exists
/// [`do_not_auto_join_consumer_group()`]: crate::clients::consumer_builder::IggyConsumerBuilder::do_not_auto_join_consumer_group
/// [`do_not_create_consumer_group_if_not_exists()`]: crate::clients::consumer_builder::IggyConsumerBuilder::do_not_create_consumer_group_if_not_exists
/// [`encryptor()`]: crate::clients::consumer_builder::IggyConsumerBuilder::encryptor
/// [`init_retries()`]: crate::clients::consumer_builder::IggyConsumerBuilder::init_retries
/// [`offset_drain_timeout()`]: crate::clients::consumer_builder::IggyConsumerBuilder::offset_drain_timeout
/// [`partition()`]: crate::clients::consumer_builder::IggyConsumerBuilder::partition
/// [`poll_interval()`]: crate::clients::consumer_builder::IggyConsumerBuilder::poll_interval
/// [`polling_retry_interval()`]: crate::clients::consumer_builder::IggyConsumerBuilder::polling_retry_interval
/// [`polling_strategy()`]: crate::clients::consumer_builder::IggyConsumerBuilder::polling_strategy
/// [`stream()`]: crate::clients::consumer_builder::IggyConsumerBuilder::stream
/// [`topic()`]: crate::clients::consumer_builder::IggyConsumerBuilder::topic
/// [`without_encryptor()`]: crate::clients::consumer_builder::IggyConsumerBuilder::without_encryptor
/// [`without_poll_interval()`]: crate::clients::consumer_builder::IggyConsumerBuilder::without_poll_interval
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
    poll_interval_micros: u64,
    batch_length: u32,
    auto_commit: AutoCommit,
    auto_commit_after_polling: bool,
    auto_join_consumer_group: bool,
    create_consumer_group_if_not_exists: bool,
    last_stored_offsets: Arc<DashMap<u32, AtomicU64>>,
    last_consumed_offsets: Arc<DashMap<u32, AtomicU64>>,
    current_offsets: Arc<DashMap<u32, AtomicU64>>,
    poll_future: Option<PollMessagesFuture>,
    buffered_messages: VecDeque<IggyMessage>,
    encryptor: Option<Arc<EncryptorKind>>,
    store_offset_sender: flume::Sender<(u32, u64)>,
    store_offset_task: Option<JoinHandle<()>>,
    background_commit_task: Option<JoinHandle<()>>,
    background_commit_notify: Arc<Notify>,
    store_offset_after_each_message: bool,
    store_offset_after_all_messages: bool,
    store_after_every_nth_message: u64,
    last_polled_at: Arc<AtomicU64>,
    current_partition_id: Arc<AtomicU32>,
    reconnection_retry_interval: IggyDuration,
    init_retries: Option<u32>,
    init_retry_interval: IggyDuration,
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
        reconnection_retry_interval: IggyDuration,
        init_retries: Option<u32>,
        init_retry_interval: IggyDuration,
        allow_replay: bool,
        offset_drain_timeout: IggyDuration,
    ) -> Self {
        let (store_offset_sender, _) = flume::unbounded();
        Self {
            initialized: false,
            shutdown: Arc::new(AtomicBool::new(false)),
            is_consumer_group: consumer.kind == ConsumerKind::ConsumerGroup,
            joined_consumer_group: Arc::new(AtomicBool::new(false)),
            can_poll: Arc::new(AtomicBool::new(true)),
            client,
            consumer_name,
            consumer: Arc::new(consumer),
            stream_id: Arc::new(stream_id),
            topic_id: Arc::new(topic_id),
            partition_id,
            polling_strategy,
            poll_interval_micros: polling_interval.map_or(0, |interval| interval.as_micros()),
            last_stored_offsets: Arc::new(DashMap::new()),
            last_consumed_offsets: Arc::new(DashMap::new()),
            current_offsets: Arc::new(DashMap::new()),
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
            encryptor,
            store_offset_sender,
            store_offset_task: None,
            background_commit_task: None,
            background_commit_notify: Arc::new(Notify::new()),
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
            current_partition_id: Arc::new(AtomicU32::new(0)),
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

    /// Returns the partition the last message came from.
    ///
    /// This is `0` until the first message has been read, because a partition is only known once
    /// the server has answered. For a consumer group the value changes over time, as the server
    /// can hand different partitions to this member.
    pub fn partition_id(&self) -> u32 {
        self.current_partition_id.load(ORDERING)
    }

    /// Stores an offset on the server, marking every message up to and including it as consumed.
    ///
    /// This is the manual counterpart to [`AutoCommit`] and is meant for
    /// [`AutoCommit::Disabled`].
    ///
    /// Pass `None` as `partition_id` to use the partition of the most recent batch polled.
    ///
    /// An offset that is not ahead of the last one this consumer stored for that partition is
    /// skipped and `Ok(())` is returned without a request.
    /// If you to re-read messages again, e.g. want to move an offset backwards configure the consumer
    /// with [`allow_replay`](crate::clients::consumer_builder::IggyConsumerBuilder::allow_replay).
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
        let partition_id = if let Some(partition_id) = partition_id {
            partition_id
        } else {
            self.current_partition_id.load(ORDERING)
        };
        Self::store_consumer_offset(
            &self.client,
            &self.consumer,
            &self.stream_id,
            &self.topic_id,
            partition_id,
            offset,
            &self.last_stored_offsets,
            self.allow_replay,
        )
        .await
    }

    /// Returns the offset of the last message this consumer handed over for the given partition,
    /// or `None` if it has not read from that partition yet.
    ///
    /// This is the local reading position, which can be ahead of what has been stored on the
    /// server.
    pub fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        let offset = self.last_consumed_offsets.get(&partition_id)?;
        Some(offset.load(ORDERING))
    }

    /// Deletes the stored offset on the server, so the next run starts from the beginning of the
    /// partition.
    ///
    /// # Errors
    ///
    /// Returns any error the server raised while deleting the offset, for example
    /// [`IggyError::Disconnected`] or a permission error.
    pub async fn delete_offset(&self, mut partition_id: Option<u32>) -> Result<(), IggyError> {
        // `None` is only resolved server-side for consumer groups. For a standalone consumer
        // explicitly assign the current partition_id.
        if partition_id.is_none() && !self.is_consumer_group {
            partition_id = Some(self.current_partition_id.load(ORDERING));
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

    /// Returns the offset this consumer last stored on the server for the given partition, or
    /// `None` if it has not stored one yet.
    ///
    /// The value is this consumer's own record of what it committed, kept in memory rather than
    /// read back from the server.
    pub fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        let offset = self.last_stored_offsets.get(&partition_id)?;
        Some(offset.load(ORDERING))
    }

    /// Initializes the consumer and makes it ready to poll messages.
    ///
    /// This must be called before the consumer can start polling messages. Calling it again on an
    /// initialized consumer does nothing and returns immediately.
    ///
    /// Initialization ensures that:
    /// - the consumers `stream_id` and `topic_id` exist on the server.
    ///   It retries for a number of `init_retries` (defaults to `None`, which is treated as no
    ///   retry) with `init_retry_interval` (defaults to one
    ///   second) time in between retries. Both can be set together through
    ///   [`IggyConsumerBuilder::init_retries`](crate::clients::consumer_builder::IggyConsumerBuilder::init_retries).
    /// - the consumer subscribes to connection lifecycle events ([`DiagnosticEvent`]) in order to
    ///   update its state, should it receive a shutdown, connected, disconnected, log in or log out event.
    /// - if the consumer belongs to a group and `auto_join_consumer_group` is enabled, the group is
    ///   initialized if it does not exist yet, and the consumer joins that group.
    /// - the tasks that store the offset on the server are spawned.
    ///
    /// # Lifecycle events
    ///
    /// Calling init spawns a background tasks that listens for lifecycle changes ([`DiagnosticEvent`]s) of the
    /// client connection.
    /// - [`DiagnosticEvent::Connected`]: a fresh connection has not joined anything yet.
    ///   Polling resumes immediately only for a consumer that is not a group member.
    /// - [`DiagnosticEvent::SignedIn`]: re-enables polling. A group member signing in after a
    ///   reconnect rejoins its group first and only polls once that succeeded. A failed rejoin is
    ///   logged and leaves polling disabled until the next event.
    /// - [`DiagnosticEvent::Disconnected`] and [`DiagnosticEvent::SignedOut`] disables polling.
    /// - [`DiagnosticEvent::Shutdown`] disables polling and terminates the background task listening
    ///   for lifecycle changes. It does not flush in-flight commits; that only happens when
    ///   [`shutdown()`](Self::shutdown) itself is called.
    ///
    /// # Storing offsets
    ///
    /// An offset is the position of a message within a partition, and storing one tells the server
    /// how many this consumer (or its consumer group) has consumed already.
    /// When this offset is stored at the server is configured in `auto_commit`, which defaults to
    /// [`AutoCommit::IntervalOrWhen`] equal to 1s and [`AutoCommitWhen::PollingMessages`].
    /// - An interval background task is only spawned for the variants that carry an interval
    ///   ([`AutoCommit::Interval`], [`AutoCommit::IntervalOrWhen`], [`AutoCommit::IntervalOrAfter`]).
    /// - The offset store task is spawned in any case. It can be configured with [`AutoCommitWhen::ConsumingEachMessage`],
    ///   [`AutoCommitWhen::ConsumingEveryNthMessage`], [`AutoCommitWhen::ConsumingAllMessages`] and
    ///   their [`AutoCommitAfter`] counterparts. Under [`AutoCommit::Disabled`] nothing is
    ///   ever sent and the task stays idle.
    ///
    /// A variant such as [`AutoCommit::IntervalOrWhen`] runs both together. The message count
    /// trigger stores as messages are consumed, the interval stores what the trigger has not
    /// covered yet. There is no double-work, since an offset that is not ahead of the one last stored
    /// for that partition is skipped instead of sent.
    /// Unless `allow_replay` is enabled an offset that is not past the last stored offset on the server
    /// will not be committed.
    ///
    /// The [`AutoCommitAfter`] variants only take effect when consuming through
    /// [`IggyConsumerMessageExt::consume_messages`](crate::consumer_ext::IggyConsumerMessageExt::consume_messages).
    ///
    /// # Errors
    ///
    /// - [`IggyError::StreamNameNotFound`] or [`IggyError::TopicNameNotFound`] when the
    ///   stream or the topic still does not exist once the retries are exhausted.
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
            // Retry for init_retires times, while waiting interval between retries.
            loop {
                // immediate happy path
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

            // Unhappy-path after hitting retry limit while stream is still missing.
            if !stream_exists {
                error!("Stream: {stream_id} was not found.");
                return Err(IggyError::StreamNameNotFound(
                    self.stream_id.get_string_value().unwrap_or_default(),
                ));
            };

            // Unhappy-path after hitting retry limit. Stream exists but topic is missing.
            if !topic_exists {
                error!("Topic: {topic_id} was not found in stream: {stream_id}.");
                return Err(IggyError::TopicNameNotFound(
                    self.topic_id.get_string_value().unwrap_or_default(),
                    self.stream_id.get_string_value().unwrap_or_default(),
                ));
            }
        }

        // Spawn background task to track status changes in the connection lifecycle
        // (connected, shutdown, disconnect, sign in, sign out)
        self.subscribe_events().await;
        // No-op if either is_consumer_group or auto_join_consumer_group is false
        self.init_consumer_group().await?;

        // Storing the offset on the server is configured with `AutoCommit`.
        // If a the configuration defines an time interval at which the offset should be stored
        // the corresponding process is spawned.
        match self.auto_commit {
            AutoCommit::Interval(interval)
            | AutoCommit::IntervalOrWhen(interval, _)
            | AutoCommit::IntervalOrAfter(interval, _) => {
                self.background_commit_task = Some(self.store_offsets_in_background(interval));
            }
            _ => {}
        }

        let client = self.client.clone();
        let consumer = self.consumer.clone();
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let last_stored_offsets = self.last_stored_offsets.clone();
        let (store_offset_sender, store_offset_receiver) = flume::unbounded();
        self.store_offset_sender = store_offset_sender;

        // The IggyClients `poll_next` implementation sends store offset requests down to this receiver.
        // This is the second path over which offsets are stored on the server on a message base, compared to
        // the duration based config above. While the interval based path can be configured, this task always runs.
        self.store_offset_task = Some(tokio::spawn(async move {
            while let Ok((partition_id, offset)) = store_offset_receiver.recv_async().await {
                trace!(
                    "Received offset to store: {offset}, partition ID: {partition_id}, stream: {stream_id}, topic: {topic_id}"
                );
                _ = Self::store_consumer_offset(
                    &client,
                    &consumer,
                    &stream_id,
                    &topic_id,
                    partition_id,
                    offset,
                    &last_stored_offsets,
                    false,
                )
                .await
            }
        }));

        self.initialized = true;
        info!(
            "Consumer: {consumer_name} has been initialized for stream: {}, topic: {}.",
            self.stream_id, self.topic_id
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn store_consumer_offset(
        client: &IggyRwLock<ClientWrapper>,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        offset: u64,
        last_stored_offsets: &DashMap<u32, AtomicU64>,
        allow_replay: bool,
    ) -> Result<(), IggyError> {
        trace!(
            "Storing offset: {offset} for consumer: {consumer}, partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}..."
        );
        let stored_offset;
        if let Some(offset_entry) = last_stored_offsets.get(&partition_id) {
            stored_offset = offset_entry.load(ORDERING);
        } else {
            stored_offset = 0;
            last_stored_offsets.insert(partition_id, AtomicU64::new(0));
        }

        if !allow_replay && (offset <= stored_offset && offset >= 1) {
            trace!(
                "Offset: {offset} is less than or equal to the last stored offset: {stored_offset} for consumer: {consumer}, partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}. Skipping storing the offset."
            );
            return Ok(());
        }

        let client = client.read().await;
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
        if let Some(last_offset_entry) = last_stored_offsets.get(&partition_id) {
            last_offset_entry.store(offset, ORDERING);
        } else {
            last_stored_offsets.insert(partition_id, AtomicU64::new(offset));
        }
        Ok(())
    }

    fn store_offsets_in_background(&self, interval: IggyDuration) -> JoinHandle<()> {
        let client = self.client.clone();
        let consumer = self.consumer.clone();
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let last_consumed_offsets = self.last_consumed_offsets.clone();
        let last_stored_offsets = self.last_stored_offsets.clone();
        let shutdown = self.shutdown.clone();
        let notify = self.background_commit_notify.clone();
        tokio::spawn(async move {
            loop {
                // Wait the task until either the interval has passed or
                // the task is explicitly notified, which happens when shutdown() is called.
                tokio::select! {
                    _ = sleep(interval.get_duration()) => {}
                    _ = notify.notified() => {}
                }

                // On consumer shutdown the final commit is owned by the shutdown() method,
                // so skip here.
                if shutdown.load(ORDERING) {
                    trace!("Shutdown signal received, stopping background offset storage");
                    break;
                }

                for entry in last_consumed_offsets.iter() {
                    let partition_id = *entry.key();
                    let consumed_offset = entry.load(ORDERING);
                    _ = Self::store_consumer_offset(
                        &client,
                        &consumer,
                        &stream_id,
                        &topic_id,
                        partition_id,
                        consumed_offset,
                        &last_stored_offsets,
                        false,
                    )
                    .await;
                }
            }
        })
    }

    pub(crate) fn send_store_offset(&self, partition_id: u32, offset: u64) {
        if let Err(error) = self.store_offset_sender.send((partition_id, offset)) {
            error!(
                "Failed to send offset to store: {error}, please verify if `init()` on IggyConsumer object has been called."
            );
        }
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

    async fn subscribe_events(&self) {
        trace!("Subscribing to diagnostic events");
        let mut receiver;
        {
            let client = self.client.read().await;
            receiver = client.subscribe_events().await;
        }

        let is_consumer_group = self.is_consumer_group;
        let can_join_consumer_group = is_consumer_group && self.auto_join_consumer_group;
        let client = self.client.clone();
        let create_consumer_group_if_not_exists = self.create_consumer_group_if_not_exists;
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let consumer = self.consumer.clone();
        let consumer_name = self.consumer_name.clone();
        let can_poll = self.can_poll.clone();
        let joined_consumer_group = self.joined_consumer_group.clone();
        let mut reconnected = false;
        let mut disconnected = false;

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
                        if disconnected {
                            reconnected = true;
                            disconnected = false;
                        }
                    }
                    DiagnosticEvent::Disconnected => {
                        disconnected = true;
                        reconnected = false;
                        joined_consumer_group.store(false, ORDERING);
                        can_poll.store(false, ORDERING);
                        warn!("Disconnected from the server");
                    }
                    DiagnosticEvent::SignedIn => {
                        if !is_consumer_group {
                            can_poll.store(true, ORDERING);
                            continue;
                        }

                        if !can_join_consumer_group {
                            can_poll.store(true, ORDERING);
                            trace!("Auto join consumer group is disabled");
                            continue;
                        }

                        if !reconnected {
                            can_poll.store(true, ORDERING);
                            continue;
                        }

                        if joined_consumer_group.load(ORDERING) {
                            can_poll.store(true, ORDERING);
                            continue;
                        }

                        info!(
                            "Rejoining consumer group: {consumer_name} for stream: {stream_id}, topic: {topic_id}..."
                        );
                        if let Err(error) = Self::initialize_consumer_group(
                            client.clone(),
                            create_consumer_group_if_not_exists,
                            stream_id.clone(),
                            topic_id.clone(),
                            consumer.clone(),
                            &consumer_name,
                            joined_consumer_group.clone(),
                        )
                        .await
                        {
                            error!(
                                "Failed to join consumer group: {consumer_name} for stream: {stream_id}, topic: {topic_id}. {error}"
                            );
                            continue;
                        }
                        info!(
                            "Rejoined consumer group: {consumer_name} for stream: {stream_id}, topic: {topic_id}"
                        );
                        can_poll.store(true, ORDERING);
                    }
                    DiagnosticEvent::SignedOut => {
                        joined_consumer_group.store(false, ORDERING);
                        can_poll.store(false, ORDERING);
                    }
                }
            }
        });
    }

    fn create_poll_messages_future(
        &self,
    ) -> impl Future<Output = Result<PolledMessages, IggyError>> + use<> {
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let partition_id = self.partition_id;
        let consumer = self.consumer.clone();
        let polling_strategy = self.polling_strategy;
        let client = self.client.clone();
        let count = self.batch_length;
        let auto_commit_after_polling = self.auto_commit_after_polling;
        let auto_commit_enabled = self.auto_commit != AutoCommit::Disabled;
        let interval = self.poll_interval_micros;
        let last_polled_at = self.last_polled_at.clone();
        let can_poll = self.can_poll.clone();
        let retry_interval = self.reconnection_retry_interval;
        let last_stored_offset = self.last_stored_offsets.clone();
        let last_consumed_offset = self.last_consumed_offsets.clone();
        let allow_replay = self.allow_replay;
        let is_consumer_group = self.is_consumer_group;
        let auto_join_consumer_group = self.auto_join_consumer_group;
        let create_consumer_group_if_not_exists = self.create_consumer_group_if_not_exists;
        let joined_consumer_group = self.joined_consumer_group.clone();

        async move {
            if interval > 0 {
                Self::wait_before_polling(interval, last_polled_at.load(ORDERING)).await;
            }

            while !can_poll.load(ORDERING)
                || (is_consumer_group && !joined_consumer_group.load(ORDERING))
            {
                trace!(
                    "Cannot poll yet (can_poll={}, joined_cg={}), waiting {retry_interval}...",
                    can_poll.load(ORDERING),
                    joined_consumer_group.load(ORDERING)
                );
                sleep(retry_interval.get_duration()).await;
            }

            trace!("Sending poll messages request");
            last_polled_at.store(IggyTimestamp::now().into(), ORDERING);
            let polled_messages = client
                .read()
                .await
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    partition_id,
                    &consumer,
                    &polling_strategy,
                    count,
                    auto_commit_after_polling,
                )
                .await;

            if let Ok(mut polled_messages) = polled_messages {
                if polled_messages.messages.is_empty() {
                    return Ok(polled_messages);
                }

                let partition_id = polled_messages.partition_id;
                let consumed_offset;
                let has_consumed_offset;
                if let Some(offset_entry) = last_consumed_offset.get(&partition_id) {
                    has_consumed_offset = true;
                    consumed_offset = offset_entry.load(ORDERING);
                } else {
                    consumed_offset = 0;
                    has_consumed_offset = false;
                    last_consumed_offset.insert(partition_id, AtomicU64::new(0));
                }

                if !allow_replay && has_consumed_offset {
                    polled_messages
                        .messages
                        .retain(|message| message.header.offset > consumed_offset);
                    if polled_messages.messages.is_empty() {
                        return Ok(PolledMessages::empty());
                    }
                }

                let stored_offset;
                if let Some(stored_offset_entry) = last_stored_offset.get(&partition_id) {
                    if auto_commit_after_polling {
                        stored_offset_entry.store(consumed_offset, ORDERING);
                        stored_offset = consumed_offset;
                    } else {
                        stored_offset = stored_offset_entry.load(ORDERING);
                    }
                } else {
                    if auto_commit_after_polling {
                        stored_offset = consumed_offset;
                    } else {
                        stored_offset = 0;
                    }
                    last_stored_offset.insert(partition_id, AtomicU64::new(stored_offset));
                }

                trace!(
                    "Last consumed offset: {consumed_offset}, current offset: {}, stored offset: {stored_offset}, in partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}, consumer: {consumer}",
                    polled_messages.current_offset
                );

                if !allow_replay
                    && (has_consumed_offset && polled_messages.current_offset == consumed_offset)
                {
                    trace!(
                        "No new messages to consume in partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}, consumer: {consumer}"
                    );
                    if auto_commit_enabled && stored_offset < consumed_offset {
                        trace!(
                            "Auto-committing the offset: {consumed_offset} in partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}, consumer: {consumer}"
                        );
                        client
                            .read()
                            .await
                            .store_consumer_offset(
                                &consumer,
                                &stream_id,
                                &topic_id,
                                Some(partition_id),
                                consumed_offset,
                            )
                            .await?;
                        if let Some(stored_offset_entry) = last_stored_offset.get(&partition_id) {
                            stored_offset_entry.store(consumed_offset, ORDERING);
                        } else {
                            last_stored_offset
                                .insert(partition_id, AtomicU64::new(consumed_offset));
                        }
                    }

                    return Ok(PolledMessages {
                        messages: vec![],
                        current_offset: polled_messages.current_offset,
                        partition_id,
                        count: 0,
                    });
                }

                return Ok(polled_messages);
            }

            let error = polled_messages.unwrap_err();
            error!("Failed to poll messages: {error}");

            if is_consumer_group
                && auto_join_consumer_group
                && matches!(&error, IggyError::ConsumerGroupMemberNotFound(..))
            {
                joined_consumer_group.store(false, ORDERING);
                let consumer_name = consumer.id.as_string();
                info!(
                    "Consumer group membership was revoked for consumer: {consumer_name}, stream: {stream_id}, topic: {topic_id}. Rejoining..."
                );
                if let Err(error) = Self::initialize_consumer_group(
                    client,
                    create_consumer_group_if_not_exists,
                    stream_id,
                    topic_id,
                    consumer,
                    &consumer_name,
                    joined_consumer_group.clone(),
                )
                .await
                {
                    // Allow the next poll to retry rejoining
                    joined_consumer_group.store(true, ORDERING);
                    return Err(error);
                }
                return Ok(PolledMessages::empty());
            }

            // Handle connection/auth errors - disable polling until event task re-enables
            // it after reconnection and rejoin complete
            if matches!(
                error,
                IggyError::Disconnected | IggyError::Unauthenticated | IggyError::StaleClient
            ) {
                can_poll.store(false, ORDERING);
                if is_consumer_group {
                    joined_consumer_group.store(false, ORDERING);
                }
                trace!("Retrying to poll messages in {retry_interval}...");
                sleep(retry_interval.get_duration()).await;
            }
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
    /// Creates a received message from a message and the partition it was read from.
    pub fn new(message: IggyMessage, current_offset: u64, partition_id: u32) -> Self {
        Self {
            message,
            current_offset,
            partition_id,
        }
    }
}

/// Yields messages one at a time.
///
/// Tries the buffer first, before a new batch is fetched from the server and stored in the buffer.
///
/// The stream never yields `None`. So a `while let Some(..)` loop over it runs
/// until the loop body breaks out. Errors are yielded as items and do not end the stream, polling
/// again retries. See the [type documentation](IggyConsumer#polling) for the details of polling.
impl Stream for IggyConsumer {
    type Item = Result<ReceivedMessage, IggyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let partition_id = self.current_partition_id.load(ORDERING);
        // First handle messages that are currently buffered.
        if let Some(message) = self.buffered_messages.pop_front() {
            {
                // Since a consumer can be standalone or a member of a consumer group, in which case
                // it can be reassigned to another partition, either update the offset of a partition
                // the consumer already worked with or add a new record, if it got reassigned.
                if let Some(last_consumed_offset_entry) =
                    self.last_consumed_offsets.get(&partition_id)
                {
                    last_consumed_offset_entry.store(message.header.offset, ORDERING);
                } else {
                    self.last_consumed_offsets
                        .insert(partition_id, AtomicU64::new(message.header.offset));
                }

                if (self.store_after_every_nth_message > 0
                    && message.header.offset % self.store_after_every_nth_message == 0)
                    || self.store_offset_after_each_message
                {
                    self.send_store_offset(partition_id, message.header.offset);
                }
            }

            // Popping above may have left the buffer empty.
            // The next turn will therefore poll messages from the server.
            // With `PollingStrategy` the user defines the starting point where to poll from.
            // After that, each poll must read the next sequential offset. Hence, strategy is
            // set to `PollingKind::Offset` and the next offset to read from is the last consumed message + 1.
            if self.buffered_messages.is_empty() {
                if self.polling_strategy.kind != PollingKind::Next {
                    self.polling_strategy = PollingStrategy::offset(message.header.offset + 1);
                }

                if self.store_offset_after_all_messages {
                    self.send_store_offset(partition_id, message.header.offset);
                }
            }

            // Not the position of this message but the newest offset the partition had when the
            // batch was polled. So every message of a batch reports the same value.
            let current_offset;
            if let Some(current_offset_entry) = self.current_offsets.get(&partition_id) {
                current_offset = current_offset_entry.load(ORDERING);
            } else {
                current_offset = 0;
            }

            return Poll::Ready(Some(Ok(ReceivedMessage::new(
                message,
                current_offset,
                partition_id,
            ))));
        }

        // If the buffer is empty, messages are polled from the server, which itself is async.
        // A previous used (and therefore invalid) future was dropped, thus create a fresh one.
        if self.poll_future.is_none() {
            let future = self.create_poll_messages_future();
            self.poll_future = Some(Box::pin(future));
        }

        while let Some(future) = self.poll_future.as_mut() {
            match future.poll_unpin(cx) {
                Poll::Ready(Ok(mut polled_messages)) => {
                    let partition_id = polled_messages.partition_id;
                    self.current_partition_id.store(partition_id, ORDERING);
                    if polled_messages.messages.is_empty() {
                        self.poll_future = Some(Box::pin(self.create_poll_messages_future()));
                    } else {
                        if let Some(ref encryptor) = self.encryptor {
                            for message in &mut polled_messages.messages {
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
                                    message.header.user_headers_length =
                                        decrypted_headers.len() as u32;
                                    message.user_headers = Some(Bytes::from(decrypted_headers));
                                }
                            }
                        }

                        if let Some(current_offset_entry) = self.current_offsets.get(&partition_id)
                        {
                            current_offset_entry.store(polled_messages.current_offset, ORDERING);
                        } else {
                            self.current_offsets.insert(
                                partition_id,
                                AtomicU64::new(polled_messages.current_offset),
                            );
                        }

                        // Return the first message and move the rest into the buffer.
                        let message = polled_messages.messages.remove(0);
                        self.buffered_messages.extend(polled_messages.messages);

                        if self.polling_strategy.kind != PollingKind::Next {
                            self.polling_strategy =
                                PollingStrategy::offset(message.header.offset + 1);
                        }

                        if let Some(last_consumed_offset_entry) =
                            self.last_consumed_offsets.get(&partition_id)
                        {
                            last_consumed_offset_entry.store(message.header.offset, ORDERING);
                        } else {
                            self.last_consumed_offsets
                                .insert(partition_id, AtomicU64::new(message.header.offset));
                        }

                        if (self.store_after_every_nth_message > 0
                            && message.header.offset % self.store_after_every_nth_message == 0)
                            || self.store_offset_after_each_message
                            || (self.store_offset_after_all_messages
                                && self.buffered_messages.is_empty())
                        {
                            self.send_store_offset(
                                polled_messages.partition_id,
                                message.header.offset,
                            );
                        }

                        // Drop future since it is [invalid after being ready](https://doc.rust-lang.org/std/future/trait.Future.html#panics)
                        self.poll_future = None;
                        return Poll::Ready(Some(Ok(ReceivedMessage::new(
                            message,
                            polled_messages.current_offset,
                            polled_messages.partition_id,
                        ))));
                    }
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
    ///   There are two background tasks that can have commits in flight. The interval-based one
    ///   (only spawned for [`AutoCommit`] variants that carry an interval) and the one driven by
    ///   [`AutoCommitWhen`]/[`AutoCommitAfter`] (always spawned). The consumer waits for
    ///   `offset_drain_timeout` on each in turn before forcing it to abort.
    ///   Any offset that is not stored until then will be lost.
    /// - commit every offset from partitions where the consumed offset is ahead of the stored one.
    ///   Note, this happens even under [`AutoCommit::Disabled`].
    /// - leave the consumer group, if this consumer is a group member. This lets the server give its partitions to
    ///   the remaining members immediately instead of waiting for the connection to time out.
    ///
    /// # Errors
    ///
    /// Returns `Ok(())` even when the final commits or the group leave failed, since those
    /// failures are logged and do not leave anything for the caller to undo. The
    /// [`Result`] is part of the signature for forward compatibility.
    pub async fn shutdown(&mut self) -> Result<(), IggyError> {
        // Immediately return, if the consumer is already shut down.
        // Otherwise, swap so background tasks see that the consumer got shut down.
        if self.shutdown.swap(true, ORDERING) {
            return Ok(());
        }

        info!("Shutting down consumer: {}...", self.consumer_name);

        // Wake the task responsible for storing the offsets (spawned in store_offset_in_background())
        self.background_commit_notify.notify_one();

        // A background_commit_task exists, if auto_commit is configured with an interval option.
        // If it exists, the task may be waiting or currently perform the interval based store offset operation.
        // In case it is currently working, wait until drain timeout has passed and then force
        // the task to abort.
        if let Some(mut task) = self.background_commit_task.take()
            && time::timeout(self.offset_drain_timeout.get_duration(), &mut task)
                .await
                .is_err()
        {
            task.abort();
            warn!(
                "Timed out waiting for the background offset-commit task to stop for consumer: {}, aborted",
                self.consumer_name
            );
        }

        // Drop the sending end of the store offset task to end the `recv_async()` loop in `send_store_offset().
        // Offsets in queue will still be committed. This prevents loading additional offsets into a channel
        // that is not read anymore.
        // Replace with a new (hanging) channel, since `store_offset_sender` is not optional.
        let (closed_sender, _) = flume::bounded(0);
        drop(std::mem::replace(
            &mut self.store_offset_sender,
            closed_sender,
        ));

        // This task never sleeps, so no need to notify.
        // If the task is working, wait until drain timeout has passed
        // and then force to abort.
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

        // For a standalone consumer `last_consumed_offsets` has one key-value pair,
        // while a consumer assigned to a group may have polled different partitions
        // and keeps therefore track of multiple offsets.
        // Store the latest offset for each partition, if it the consumed offset
        // is larger than the last stored offset.
        for entry in self.last_consumed_offsets.iter() {
            let partition_id = *entry.key();
            let consumed_offset = entry.load(ORDERING);

            let stored_offset = self
                .last_stored_offsets
                .get(&partition_id)
                .map(|e| e.load(ORDERING))
                .unwrap_or(0);

            if consumed_offset > stored_offset {
                trace!(
                    "Flushing final offset: {consumed_offset} for partition: {partition_id}, stream: {}, topic: {}",
                    self.stream_id, self.topic_id
                );
                let _ = Self::store_consumer_offset(
                    &self.client,
                    &self.consumer,
                    &self.stream_id,
                    &self.topic_id,
                    partition_id,
                    consumed_offset,
                    &self.last_stored_offsets,
                    self.allow_replay,
                )
                .await;
            }
        }

        if self.is_consumer_group && self.joined_consumer_group.load(ORDERING) {
            let group_id = self.consumer.id.clone();
            trace!(
                "Leaving consumer group: {group_id} for stream: {}, topic: {}",
                self.stream_id, self.topic_id
            );

            let client = self.client.read().await;
            // Update consumer state to not being part of a consumer group.
            self.joined_consumer_group.store(false, ORDERING);
            // Let the server know that the consumer left its group.
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

        info!("Consumer: {} has been shut down.", self.consumer_name);
        Ok(())
    }
}

/// Stops the background tasks, nothing more.
///
/// Dropping cannot await, so it neither commits pending offsets nor leaves the consumer group.
/// Await [`IggyConsumer::shutdown`] to finish background tasks.
impl Drop for IggyConsumer {
    fn drop(&mut self) {
        self.shutdown.store(true, ORDERING);
        self.background_commit_notify.notify_one();
        trace!(
            "Consumer {} has been dropped, shutdown signal sent",
            self.consumer_name
        );
    }
}
