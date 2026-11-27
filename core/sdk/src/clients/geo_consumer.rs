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
use crate::clients::consumer::{IggyConsumer, IggyConsumerState, ReceivedMessage};
use crate::clients::consumer_builder::IggyConsumerBuilder;
use crate::clients::reader::ReadError;
use crate::session_owner::SessionOwner;
use futures::future::{BoxFuture, Shared};
use futures::stream::FuturesUnordered;
use futures::task::{ArcWake, AtomicWaker, waker};
use futures::{FutureExt, Stream, StreamExt};
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    Checkpoint, IdKind, Identifier, IggyError, IggyMessage, NamedPageQuery, PageQuery,
    PollingStrategy, StreamClient, TopicClient, TopicType,
};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::time::{Instant, Sleep, sleep_until};
use tracing::{debug, error, info, warn};

/// How often the consumer lists the streams for shadow topics unless its builder sets another
/// interval.
pub(crate) const DEFAULT_DISCOVERY_INTERVAL: Duration = Duration::from_secs(5);

/// A shadow stream is named `__shadow_{origin}_{stream}`. Its name is the only link that a
/// client sees from a shadow stream to the stream it shadows.
const SHADOW_STREAM_PREFIX: &str = "__shadow_";

/// A shadow that keeps refusing is tried again after at most `2^4` discovery intervals.
const MAX_REFUSAL_BACKOFF_STEPS: u32 = 4;

/// The topics of a geo topic that an [`IggyGeoConsumer`] reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum GeoSources {
    /// The topic in this cluster and each of its shadow topics.
    #[default]
    Both,
    /// Only the topic in this cluster: the messages written in this datacenter.
    LocalOnly,
    /// Only the shadow topics: the messages written in the other datacenters.
    ShadowsOnly,
}

impl GeoSources {
    const fn reads_local(self) -> bool {
        matches!(self, Self::Both | Self::LocalOnly)
    }

    const fn reads_shadows(self) -> bool {
        matches!(self, Self::Both | Self::ShadowsOnly)
    }
}

/// Where a message of an [`IggyGeoConsumer`] was written: the topic in this cluster, or the
/// shadow topic that serves the log of another datacenter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeoOrigin {
    /// `None` for the topic in this cluster.
    name: Option<Arc<str>>,
}

impl GeoOrigin {
    pub(crate) const fn local() -> Self {
        Self { name: None }
    }

    pub(crate) fn shadow(origin: &str) -> Self {
        Self {
            name: Some(Arc::from(origin)),
        }
    }

    /// The origin of the datacenter that wrote the message: the `shadow_origin` of its shadow
    /// stream. `None` for the topic in this cluster, because a client cannot read the origin of
    /// the cluster it is connected to.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Whether the message comes from the topic in this cluster.
    pub const fn is_local(&self) -> bool {
        self.name.is_none()
    }

    /// Whether the message comes from a shadow topic.
    pub const fn is_shadow(&self) -> bool {
        self.name.is_some()
    }
}

impl Display for GeoOrigin {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.name().unwrap_or("local"))
    }
}

/// A message handed over by an [`IggyGeoConsumer`]: the fields of a [`ReceivedMessage`] and the
/// origin of the message.
#[derive(Debug)]
pub struct GeoReceivedMessage {
    /// The message itself, with its payload already decrypted when the client uses an encryptor.
    pub message: IggyMessage,
    /// The offset of the newest message in the partition at the time it was polled.
    pub current_offset: u64,
    /// The partition this message was read from, in the topic of its origin.
    pub partition_id: u32,
    /// Where the message was written. Offsets and order hold within one origin and partition.
    pub origin: GeoOrigin,
    checkpoint: Checkpoint,
}

impl GeoReceivedMessage {
    fn new(received: ReceivedMessage, origin: GeoOrigin) -> Self {
        let checkpoint = received.checkpoint();
        Self {
            message: received.message,
            current_offset: received.current_offset,
            partition_id: received.partition_id,
            origin,
            checkpoint,
        }
    }

    /// The position after this message, to commit it and every earlier message of its partition
    /// in its log with [`IggyGeoConsumer::commit`].
    #[must_use]
    pub const fn checkpoint(&self) -> Checkpoint {
        self.checkpoint
    }
}

/// An error of one topic of an [`IggyGeoConsumer`], or of its search for shadow topics. The
/// consumer stays usable after it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GeoReadError {
    /// The origin of the topic of the error. `None` for an error of the consumer itself, such
    /// as a listing of the streams that failed.
    pub origin: Option<GeoOrigin>,
    /// The stream of the topic: the stream of the consumer or a shadow stream. `None` when
    /// `origin` is `None`.
    pub stream: Option<String>,
    /// The error, with its partition in the topic of `origin`.
    pub error: ReadError,
}

impl GeoReadError {
    fn of_log(log: &Log, error: ReadError) -> Self {
        Self {
            origin: Some(log.origin.clone()),
            stream: Some(log.stream.clone()),
            error,
        }
    }

    fn of_consumer(error: IggyError) -> Self {
        Self {
            origin: None,
            stream: None,
            error: ReadError::new(None, error),
        }
    }
}

impl Display for GeoReadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match (&self.origin, &self.stream) {
            (Some(origin), Some(stream)) => {
                write!(f, "stream {stream} of origin {origin}: {}", self.error)
            }
            _ => Display::fmt(&self.error, f),
        }
    }
}

impl Error for GeoReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

impl From<GeoReadError> for IggyError {
    fn from(error: GeoReadError) -> Self {
        error.error.error
    }
}

/// Reads a geo topic and its shadow topics as one stream of messages, through one consumer
/// group name.
///
/// A geo topic exists in each datacenter under the same stream and topic name. In this cluster,
/// the messages written in another datacenter are in a read-only shadow topic of the same name,
/// in the shadow stream of that datacenter. This consumer reads the topic in this cluster and
/// each shadow of it through an [`IggyConsumer`] of the same group name, which joins a group of
/// that name on each topic. Build one with
/// [`IggyConsumerBuilder::with_shadows`](crate::prelude::IggyConsumerBuilder::with_shadows).
///
/// - Each topic is a separate log, with its own group, offsets and partition assignment. Each
///   log commits as the commit policy says, and [`commit`](Self::commit) stores a checkpoint in
///   the log that yielded it.
/// - Each message names its log in [`GeoReceivedMessage::origin`]. Order holds within one origin
///   and partition only. Shadow topics serve their messages seconds after the topic in their
///   own datacenter does.
/// - The consumer takes turns between the logs that have messages, so no log holds back the
///   others. Each shadow topic is read from the moment its consumer joins its group, and
///   [`init`](Self::init) waits for no join. The joins share the requests of the client, so a
///   slow join can delay the other joins, but not the logs that already joined.
/// - Every [`discovery_interval`](crate::prelude::IggyGeoConsumerBuilder::discovery_interval),
///   while the application reads, it lists the streams: it starts reading each new shadow
///   topic, and stops reading each one that is gone, such as the shadow of a retired origin.
///   The list holds only the streams that the user may read. The geo consumers of one client
///   share a listing that is younger than half their interval.
/// - An error of one log comes as a [`GeoReadError`] with its origin, and the other logs read
///   on. A shadow topic that the consumer cannot open is reported once and tried again later,
///   and so is a failed shutdown of the consumer of a shadow topic that is gone.
///
/// Call [`Self::init`] before reading and [`Self::shutdown`] before dropping the consumer. A
/// call of either that the caller drops part way keeps its work in the consumer, and the next
/// call goes on from there.
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
///     .consumer_group("billing", "orders", "payments")?
///     .with_shadows()
///     .build();
/// consumer.init().await?;
/// let mut outcome = Ok(());
/// while let Some(received) = consumer.next().await {
///     let received = match received {
///         Ok(received) => received,
///         Err(error) => {
///             eprintln!("Failed to read a message: {error}");
///             continue;
///         }
///     };
///     if handle(&received.message).await.is_err() {
///         break;
///     }
///     if let Err(error) = consumer.commit(received.checkpoint()).await {
///         outcome = Err(error);
///         break;
///     }
/// }
/// consumer.shutdown().await?;
/// outcome
/// # }
/// ```
pub struct IggyGeoConsumer {
    name: String,
    stream: Identifier,
    topic: Identifier,
    sources: GeoSources,
    discovery_interval: Duration,
    catalog: Arc<dyn LogCatalog>,
    phase: Phase,
    /// The names that the shadow streams and topics derive from, known after `init` and
    /// looked up again by each search.
    names: Option<Arc<TopicNames>>,
    logs: Vec<Log>,
    /// The logs to poll, in turn order. The waker of each log adds it.
    ready: Arc<ReadyLogs>,
    next_key: u64,
    /// The key of the log that yielded the last message.
    last_yielded: Option<u64>,
    search: Option<BoxFuture<'static, Result<Listing, IggyError>>>,
    next_search: Option<Pin<Box<Sleep>>>,
    /// The consumers of shadow topics that are joining their groups.
    joins: FuturesUnordered<BoxFuture<'static, (ShadowStream, Joined)>>,
    /// The shadow streams of `joins`, by name.
    joining: BTreeSet<String>,
    /// Shadow streams that refused to open, by name.
    refused: HashMap<String, Refused>,
    /// Shadow streams whose consumer reported its stream or topic missing, with that error, by
    /// name. A check after the next search drops each one that is gone, and reports the others.
    missing: HashMap<String, GeoReadError>,
    /// The checks of `missing` streams, whether each still holds the topic.
    checks: FuturesUnordered<BoxFuture<'static, (String, bool)>>,
    /// The shadow streams of `checks`, by name.
    checking: BTreeSet<String>,
    listing_failed: bool,
    errors: VecDeque<GeoReadError>,
    /// The shutdowns of the consumers of shadows that are gone.
    closing: FuturesUnordered<BoxFuture<'static, Closed>>,
    /// The shadow streams of `closing`, by name. None is joined again before its old consumer
    /// has left its group.
    removing: BTreeSet<String>,
    uninitialized_poll_warned: bool,
}

impl IggyGeoConsumer {
    pub(crate) fn new(
        name: String,
        stream: Identifier,
        topic: Identifier,
        sources: GeoSources,
        discovery_interval: Duration,
        catalog: Arc<dyn LogCatalog>,
    ) -> Self {
        Self {
            name,
            stream,
            topic,
            sources,
            discovery_interval,
            catalog,
            phase: Phase::New,
            names: None,
            logs: Vec::new(),
            ready: Arc::default(),
            next_key: 0,
            last_yielded: None,
            search: None,
            next_search: None,
            joins: FuturesUnordered::new(),
            joining: BTreeSet::new(),
            refused: HashMap::new(),
            missing: HashMap::new(),
            checks: FuturesUnordered::new(),
            checking: BTreeSet::new(),
            listing_failed: false,
            errors: VecDeque::new(),
            closing: FuturesUnordered::new(),
            removing: BTreeSet::new(),
            uninitialized_poll_warned: false,
        }
    }

    /// Returns the name of the consumer group.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the identifier of the stream of the topic in this cluster.
    pub fn stream(&self) -> &Identifier {
        &self.stream
    }

    /// Returns the identifier of the topic in this cluster.
    pub fn topic(&self) -> &Identifier {
        &self.topic
    }

    /// Initializes the consumer and makes it ready to poll messages. Calling it again on an
    /// initialized consumer does nothing.
    ///
    /// It checks that the transport can join a consumer group. When it reads the topic in this
    /// cluster, it looks up the stream and the topic as [`IggyConsumer::init`] does, checks that
    /// the topic is a geo topic in a stream that is not a shadow stream, and only then joins its
    /// group. It lists the streams and starts a consumer for each shadow topic. It does not
    /// wait for those to join their groups: each shadow topic is read from the moment its
    /// consumer joins. A shadow topic that fails to open does not fail the call: its error
    /// comes from the stream as a [`GeoReadError`], and the consumer tries it again later.
    ///
    /// A call that the caller drops part way keeps what it did in the consumer, and the next
    /// call goes on from there, with the same consumer of the topic in this cluster.
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidConfiguration`] for a consumer that is not a consumer group, for a
    ///   discovery interval too long to schedule, and once [`shutdown()`](Self::shutdown) has
    ///   started.
    /// - The refusals of the configuration of [`IggyConsumer::init`], before anything is sent,
    ///   for each value of [`GeoSources`].
    /// - [`IggyError::FeatureUnavailable`] on HTTP, which cannot join a consumer group.
    /// - When it reads the topic in this cluster: [`IggyError::InvalidConfiguration`] for a
    ///   topic that is not a geo topic and for a shadow stream, and the errors of
    ///   [`IggyConsumer::init`].
    /// - The errors of a lookup of the stream or the topic, when the builder named either by
    ///   its numeric id: the consumer needs their names to find the shadow topics.
    pub async fn init(&mut self) -> Result<(), IggyError> {
        loop {
            match &mut self.phase {
                Phase::Ready => return Ok(()),
                Phase::Closing(_) | Phase::Closed => return Err(IggyError::InvalidConfiguration),
                Phase::New => {
                    self.validate()?;
                    info!(
                        "Initializing geo consumer: {} for stream: {}, topic: {}, sources: {:?}...",
                        self.name, self.stream, self.topic, self.sources
                    );
                    self.phase =
                        Phase::Starting(start(Arc::clone(&self.catalog), self.sources).boxed());
                }
                Phase::Starting(starting) => {
                    let started = starting.await;
                    self.phase = Phase::New;
                    self.adopt(started?);
                }
                Phase::Listing => {
                    poll_fn(|cx| self.poll_first_search(cx)).await;
                    self.phase = Phase::Ready;
                    info!(
                        "Geo consumer: {} has been initialized. It reads {} topics, and {} more are joining.",
                        self.name,
                        self.logs.len(),
                        self.joining.len()
                    );
                }
            }
        }
    }

    /// The checks that need no I/O.
    fn validate(&self) -> Result<(), IggyError> {
        let longest_delay = self
            .discovery_interval
            .saturating_mul(1 << MAX_REFUSAL_BACKOFF_STEPS);
        if Instant::now().checked_add(longest_delay).is_none() {
            error!(
                "Geo consumer: {} has the discovery interval: {:?}, too long to schedule.",
                self.name, self.discovery_interval
            );
            return Err(IggyError::InvalidConfiguration);
        }
        self.catalog.validate()
    }

    /// Takes the consumer of the topic in this cluster and the names that `start` found, and
    /// starts the first search for shadow topics.
    fn adopt(&mut self, started: Started) {
        let names = Arc::new(started.names);
        if let Some(local) = started.local {
            self.add_log(GeoOrigin::local(), names.stream.clone(), local);
        }
        if self.sources.reads_shadows() {
            self.search = Some(self.start_search());
        }
        self.names = Some(names);
        self.phase = Phase::Listing;
    }

    /// Ends once the first search has listed the streams, with the joins that end at once
    /// applied. It waits for no join.
    fn poll_first_search(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.poll_search(cx);
        if self.search.is_some() {
            return Poll::Pending;
        }
        self.poll_joins(cx);
        Poll::Ready(())
    }

    /// Stores the offset of a checkpoint that this consumer yielded, in the log that yielded
    /// it, as [`IggyConsumer::commit`] does. The future does not borrow the consumer, so a
    /// commit can run while the consumer reads on.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidCommand`] for a checkpoint that no log of this consumer yielded,
    /// also one of a shadow topic that is gone. Otherwise the errors of
    /// [`IggyConsumer::commit`].
    pub fn commit(
        &self,
        checkpoint: Checkpoint,
    ) -> impl Future<Output = Result<(), IggyError>> + Send + use<> {
        let committer = self.log_of(checkpoint).map(|log| log.consumer.committer());
        async move { committer?.commit(checkpoint).await }
    }

    /// Counts a checkpoint that this consumer yielded, in the log that yielded it, as
    /// [`IggyConsumer::mark`] does.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidCommand`] for a checkpoint that no log of this consumer yielded.
    /// Otherwise the errors of [`IggyConsumer::mark`].
    pub fn mark(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        self.log_of(checkpoint)?.consumer.mark(checkpoint)
    }

    /// Sets the next read position of `partition` in the log of `origin`, as
    /// [`IggyConsumer::seek`] does. Take `origin` from the [`GeoReadError`] that stopped the
    /// partition.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidConfiguration`] before [`init()`](Self::init) and after
    /// [`shutdown()`](Self::shutdown), and [`IggyError::InvalidCommand`] when the consumer reads
    /// no log of `origin`. Otherwise the errors of [`IggyConsumer::seek`].
    pub async fn seek(
        &mut self,
        origin: &GeoOrigin,
        partition: u32,
        start: PollingStrategy,
    ) -> Result<(), IggyError> {
        if !matches!(self.phase, Phase::Ready) {
            return Err(IggyError::InvalidConfiguration);
        }
        let log = self
            .logs
            .iter_mut()
            .find(|log| log.origin == *origin)
            .ok_or(IggyError::InvalidCommand)?;
        log.consumer.seek(partition, start).await
    }

    /// Shuts down the consumer of each log, as [`IggyConsumer::shutdown`] does: each one stores
    /// what its commit policy says and leaves its group. Await it before dropping the consumer.
    /// The consumers of shadow topics that are still joining or still shutting down, and that
    /// of the topic in this cluster when [`init()`](Self::init) stopped part way, finish too.
    /// The consumers that joined shut down at once, without waiting for them.
    ///
    /// A call that the caller drops part way keeps the rest of the work in the consumer: the
    /// next call finishes it and returns its result, and a call after that returns `Ok`.
    ///
    /// # Errors
    ///
    /// The first error of [`IggyConsumer::shutdown`], that of the topic in this cluster first.
    /// Every log shuts down whatever the others return.
    pub async fn shutdown(&mut self) -> Result<(), IggyError> {
        loop {
            match &mut self.phase {
                Phase::Closed => return Ok(()),
                Phase::Closing(closing) => {
                    let result = closing.await;
                    self.phase = Phase::Closed;
                    info!("Geo consumer: {} has been shut down.", self.name);
                    return result;
                }
                Phase::New | Phase::Starting(_) | Phase::Listing | Phase::Ready => {
                    info!("Shutting down geo consumer: {}...", self.name);
                    self.phase = Phase::Closing(self.take_for_shutdown());
                }
            }
        }
    }

    /// Moves every consumer, and every one still opening or closing, into the work of a
    /// shutdown, which owns them so that a dropped `shutdown()` call loses none.
    fn take_for_shutdown(&mut self) -> BoxFuture<'static, Result<(), IggyError>> {
        let starting = match std::mem::replace(&mut self.phase, Phase::Closed) {
            Phase::Starting(starting) => Some(starting),
            _ => None,
        };
        // Searches and checks only list and look up, so they hold no consumer.
        self.search = None;
        self.next_search = None;
        self.checks = FuturesUnordered::new();
        self.checking.clear();
        self.last_yielded = None;
        self.joining.clear();
        self.removing.clear();
        close(
            self.name.clone(),
            starting,
            std::mem::take(&mut self.joins),
            std::mem::take(&mut self.closing),
            std::mem::take(&mut self.logs),
        )
        .boxed()
    }

    fn log_of(&self, checkpoint: Checkpoint) -> Result<&Log, IggyError> {
        let reader = checkpoint.stamp().map(|stamp| stamp.reader);
        self.logs
            .iter()
            .find(|log| Some(log.reader) == reader)
            .ok_or(IggyError::InvalidCommand)
    }

    /// Adds a log, ready for its first poll.
    fn add_log(&mut self, origin: GeoOrigin, stream: String, consumer: Box<dyn LogConsumer>) {
        let key = self.next_key;
        self.next_key += 1;
        let wake = Arc::new(LogWake {
            key,
            queued: AtomicBool::new(false),
            ready: Arc::clone(&self.ready),
        });
        wake.enqueue();
        self.logs.push(Log {
            key,
            origin,
            stream,
            reader: consumer.reader_id(),
            waker: waker(Arc::clone(&wake)),
            wake,
            consumer,
        });
    }

    /// Under `CommitAfter::Processing`, the message that the consumer yielded last counts once
    /// the consumer is polled again, also when its log does not get the first turn.
    fn count_uncounted(&mut self) {
        if let Some(key) = self.last_yielded.take()
            && let Some(log) = self.logs.iter_mut().find(|log| log.key == key)
        {
            log.consumer.count_uncounted();
        }
    }

    fn remove_log(&mut self, index: usize) -> Log {
        let log = self.logs.remove(index);
        if self.last_yielded == Some(log.key) {
            self.last_yielded = None;
        }
        log
    }

    /// Applies each shutdown of the consumer of a gone shadow that ended, and reports a failed
    /// one: its offsets may not have been stored, and its group may still hold the member.
    fn poll_closing(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some(closed)) = self.closing.poll_next_unpin(cx) {
            self.removing.remove(&closed.stream);
            if let Err(error) = closed.result {
                warn!(
                    "Geo consumer: {} failed to shut down the consumer of the gone shadow stream: {} of origin: {}. {error}",
                    self.name, closed.stream, closed.origin
                );
                self.errors.push_back(GeoReadError {
                    origin: Some(closed.origin),
                    stream: Some(closed.stream),
                    error: ReadError::new(None, error),
                });
            }
        }
    }

    /// Starts the search for shadow topics when it is due, and applies its result when it
    /// ends. At most one search runs at a time.
    fn poll_search(&mut self, cx: &mut Context<'_>) {
        if !self.sources.reads_shadows() {
            return;
        }
        if self.search.is_none() {
            let due = self
                .next_search
                .as_mut()
                .is_none_or(|next| next.as_mut().poll(cx).is_ready());
            if !due {
                return;
            }
            self.search = Some(self.start_search());
        }
        let Some(search) = self.search.as_mut() else {
            return;
        };
        let Poll::Ready(found) = search.poll_unpin(cx) else {
            return;
        };
        self.search = None;
        self.apply_search(found);
        if let Some(next) = self.next_search.as_mut()
            && next.as_mut().poll(cx).is_ready()
        {
            cx.waker().wake_by_ref();
        }
    }

    fn start_search(&self) -> BoxFuture<'static, Result<Listing, IggyError>> {
        let now = Instant::now();
        let open = self
            .logs
            .iter()
            .filter(|log| log.origin.is_shadow())
            .map(|log| log.stream.clone())
            .collect();
        let skipped = self
            .refused
            .iter()
            .filter(|(_, refused)| refused.retry_at > now)
            .map(|(stream, _)| stream.clone())
            .chain(self.joining.iter().cloned())
            .chain(self.removing.iter().cloned())
            .collect();
        search(Arc::clone(&self.catalog), open, skipped).boxed()
    }

    fn apply_search(&mut self, found: Result<Listing, IggyError>) {
        let now = Instant::now();
        match found {
            Err(error) => {
                if !self.listing_failed {
                    warn!(
                        "Geo consumer: {} cannot list the streams to find the shadow topics. {error}",
                        self.name
                    );
                    self.errors.push_back(GeoReadError::of_consumer(error));
                }
                self.listing_failed = true;
            }
            Ok(listing) => {
                self.listing_failed = false;
                self.refused
                    .retain(|stream, _| listing.listed.contains(stream));
                for stream in listing.gone {
                    self.missing.remove(&stream);
                    self.drop_shadow(&stream);
                }
                let names = self.refresh_names(listing.names);
                for stream in self.missing.keys() {
                    if self.checking.insert(stream.clone()) {
                        self.checks.push(
                            check(
                                Arc::clone(&self.catalog),
                                stream.clone(),
                                names.topic.clone(),
                            )
                            .boxed(),
                        );
                    }
                }
                for shadow in listing.new {
                    self.joining.insert(shadow.name.clone());
                    self.joins
                        .push(join(Arc::clone(&self.catalog), shadow, names.topic.clone()).boxed());
                }
            }
        }
        let next = deadline(now, self.discovery_interval);
        match self.next_search.as_mut() {
            Some(sleep) => sleep.as_mut().reset(next),
            None => self.next_search = Some(Box::pin(sleep_until(next))),
        }
    }

    /// Keeps the names that a search found, which change when the builder named the stream or
    /// the topic by its numeric id and it was renamed.
    fn refresh_names(&mut self, names: TopicNames) -> Arc<TopicNames> {
        if let Some(current) = &self.names
            && **current == names
        {
            return Arc::clone(current);
        }
        info!(
            "Geo consumer: {} finds the stream: {} and the topic: {} under new names.",
            self.name, names.stream, names.topic
        );
        for log in self.logs.iter_mut().filter(|log| log.origin.is_local()) {
            log.stream.clone_from(&names.stream);
        }
        let names = Arc::new(names);
        self.names = Some(Arc::clone(&names));
        names
    }

    /// Applies each join that ended, so a shadow topic is read as soon as its consumer joins.
    fn poll_joins(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some((shadow, joined))) = self.joins.poll_next_unpin(cx) {
            self.joining.remove(&shadow.name);
            match joined {
                Joined::Opened(consumer) => {
                    self.refused.remove(&shadow.name);
                    info!(
                        "Geo consumer: {} reads the shadow stream: {} of origin: {}.",
                        self.name, shadow.name, shadow.origin
                    );
                    self.add_log(GeoOrigin::shadow(&shadow.origin), shadow.name, consumer);
                }
                Joined::Refused(error) => self.refuse(shadow, error),
                Joined::Absent => {}
            }
        }
    }

    /// Applies each check of a missing shadow that ended: drops the shadow when it is gone,
    /// and reports the error of its consumer when it is not.
    fn poll_checks(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some((stream, holds_topic))) = self.checks.poll_next_unpin(cx) {
            self.checking.remove(&stream);
            let Some(error) = self.missing.remove(&stream) else {
                continue;
            };
            if holds_topic {
                self.errors.push_back(error);
            } else {
                self.drop_shadow(&stream);
            }
        }
    }

    /// Reports the first refusal of `shadow`, and tries it again after a backoff that grows
    /// with each refusal.
    fn refuse(&mut self, shadow: ShadowStream, error: IggyError) {
        let now = Instant::now();
        let refused = self.refused.entry(shadow.name.clone()).or_insert(Refused {
            attempts: 0,
            retry_at: now,
        });
        if refused.attempts == 0 {
            warn!(
                "Geo consumer: {} cannot read the shadow stream: {} of origin: {}. {error}",
                self.name, shadow.name, shadow.origin
            );
            self.errors.push_back(GeoReadError {
                origin: Some(GeoOrigin::shadow(&shadow.origin)),
                stream: Some(shadow.name),
                error: ReadError::new(None, error),
            });
        }
        refused.attempts = refused.attempts.saturating_add(1);
        let backoff = 1 << (refused.attempts - 1).min(MAX_REFUSAL_BACKOFF_STEPS);
        refused.retry_at = deadline(now, self.discovery_interval.saturating_mul(backoff));
    }

    /// Stops reading the shadow stream `stream`, whose stream or topic is gone.
    fn drop_shadow(&mut self, stream: &str) {
        let Some(index) = self
            .logs
            .iter()
            .position(|log| log.origin.is_shadow() && log.stream == stream)
        else {
            return;
        };
        let log = self.remove_log(index);
        info!(
            "Geo consumer: {} stops reading the shadow stream: {} of origin: {}, which no longer holds the topic.",
            self.name, log.stream, log.origin
        );
        self.removing.insert(log.stream.clone());
        self.closing
            .push(shut_down(log.origin, log.stream, log.consumer).boxed());
    }

    /// Polls the logs that woke, in turn order, and returns the first message or error. A log
    /// that yields goes to the end of the turn order.
    fn poll_logs(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<GeoReceivedMessage, GeoReadError>>> {
        self.ready.task.register(cx.waker());
        let mut retried = Vec::new();
        let mut deferred = Vec::new();
        let polled = loop {
            let Some(key) = self.ready.pop() else {
                break Poll::Pending;
            };
            let Some(index) = self.logs.iter().position(|log| log.key == key) else {
                continue;
            };
            let log = &mut self.logs[index];
            log.wake.queued.store(false, Ordering::SeqCst);
            let mut log_cx = Context::from_waker(&log.waker);
            match log.consumer.poll_next_unpin(&mut log_cx) {
                Poll::Pending => {}
                Poll::Ready(Some(Ok(received))) => {
                    // A ready poll leaves no wake behind.
                    log.wake.enqueue();
                    self.last_yielded = Some(key);
                    let received = GeoReceivedMessage::new(received, log.origin.clone());
                    break Poll::Ready(Some(Ok(received)));
                }
                Poll::Ready(Some(Err(error))) => {
                    let error = GeoReadError::of_log(log, error);
                    // A node that did not apply a new shadow yet answers not found too, so a
                    // check after the next search decides whether the shadow is gone. The log
                    // is polled once more at once, and then left for the next poll, so it
                    // cannot hold the task.
                    if log.origin.is_shadow() && names_missing_log(&error.error.error) {
                        debug!(
                            "Geo consumer: {} checks the shadow stream: {} after its next search. {error}",
                            self.name, log.stream
                        );
                        self.missing.insert(log.stream.clone(), error);
                        if retried.contains(&key) {
                            deferred.push(key);
                        } else {
                            retried.push(key);
                            log.wake.enqueue_first();
                        }
                        continue;
                    }
                    log.wake.enqueue();
                    break Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    let log = self.remove_log(index);
                    debug!(
                        "Geo consumer: {} stops reading the stream: {}, whose consumer ended.",
                        self.name, log.stream
                    );
                }
            }
        };
        if !deferred.is_empty() {
            for log in self.logs.iter().filter(|log| deferred.contains(&log.key)) {
                log.wake.enqueue();
            }
            cx.waker().wake_by_ref();
        }
        polled
    }
}

/// Yields the messages of every log, one at a time, taking turns between the logs.
///
/// Errors of the search for shadow topics come first. See [`IggyGeoConsumer`].
impl Stream for IggyGeoConsumer {
    type Item = Result<GeoReceivedMessage, GeoReadError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let consumer = self.get_mut();
        match consumer.phase {
            Phase::Ready => {}
            Phase::Closing(_) | Phase::Closed => return Poll::Ready(None),
            Phase::New | Phase::Starting(_) | Phase::Listing => {
                // A loop that continues on errors would otherwise spin without a trace.
                if !consumer.uninitialized_poll_warned {
                    consumer.uninitialized_poll_warned = true;
                    warn!(
                        "Geo consumer: {} was polled before init(). Every poll fails until init() runs.",
                        consumer.name
                    );
                }
                return Poll::Ready(Some(Err(GeoReadError::of_consumer(
                    IggyError::InvalidConfiguration,
                ))));
            }
        }
        consumer.count_uncounted();
        consumer.poll_search(cx);
        consumer.poll_joins(cx);
        consumer.poll_checks(cx);
        consumer.poll_closing(cx);
        if let Some(error) = consumer.errors.pop_front() {
            return Poll::Ready(Some(Err(error)));
        }
        consumer.poll_logs(cx)
    }
}

/// One topic that the geo consumer reads, through its own consumer.
struct Log {
    /// Names the log in the turn order.
    key: u64,
    origin: GeoOrigin,
    /// The name of the stream of the topic: the stream of the consumer, or a shadow stream.
    stream: String,
    reader: u64,
    wake: Arc<LogWake>,
    /// The waker of `wake`, which the consumer is polled with.
    waker: Waker,
    consumer: Box<dyn LogConsumer>,
}

/// The logs whose consumer woke, in turn order, and the task of the geo consumer.
#[derive(Default)]
struct ReadyLogs {
    keys: Mutex<VecDeque<u64>>,
    task: AtomicWaker,
}

impl ReadyLogs {
    fn keys(&self) -> MutexGuard<'_, VecDeque<u64>> {
        self.keys.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn pop(&self) -> Option<u64> {
        self.keys().pop_front()
    }
}

/// Wakes the geo consumer for one log, and queues the log for its turn.
struct LogWake {
    key: u64,
    /// Whether the log is in the queue, so it is there at most once.
    queued: AtomicBool,
    ready: Arc<ReadyLogs>,
}

impl LogWake {
    /// Puts the log at the end of the turn order, unless it is queued already.
    fn enqueue(&self) {
        if !self.queued.swap(true, Ordering::SeqCst) {
            self.ready.keys().push_back(self.key);
        }
    }

    /// Puts the log at the start of the turn order, unless it is queued already.
    fn enqueue_first(&self) {
        if !self.queued.swap(true, Ordering::SeqCst) {
            self.ready.keys().push_front(self.key);
        }
    }
}

impl ArcWake for LogWake {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.enqueue();
        arc_self.ready.task.wake();
    }
}

struct Refused {
    attempts: u32,
    retry_at: Instant,
}

/// Where a geo consumer is between `new` and the end of `shutdown`. A step that waits keeps its
/// work here, so a call that the caller drops leaves it for the next call to finish.
enum Phase {
    New,
    /// Checking the transport, looking up the names and opening the consumer of the topic in
    /// this cluster.
    Starting(BoxFuture<'static, Result<Started, IggyError>>),
    /// The first search for shadow topics lists the streams, in `search`.
    Listing,
    Ready,
    Closing(BoxFuture<'static, Result<(), IggyError>>),
    Closed,
}

/// What [`start`] opened and looked up.
struct Started {
    local: Option<Box<dyn LogConsumer>>,
    names: TopicNames,
}

/// How the join of one shadow topic ended.
enum Joined {
    Opened(Box<dyn LogConsumer>),
    Refused(IggyError),
    /// The shadow stream does not hold the topic, or is gone.
    Absent,
}

/// How the shutdown of the consumer of one log ended.
struct Closed {
    origin: GeoOrigin,
    stream: String,
    result: Result<(), IggyError>,
}

/// The names of the stream and the topic of the consumer, which its shadow topics share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicNames {
    pub(crate) stream: String,
    pub(crate) topic: String,
}

/// The stream and the topic in this cluster, as [`LogCatalog::find_local`] found them.
#[derive(Debug, Clone)]
pub(crate) struct LocalTopic {
    pub(crate) names: TopicNames,
    /// The `shadow_origin` of the stream, set when it is a shadow stream.
    pub(crate) shadow_origin: Option<String>,
    pub(crate) is_geo: bool,
}

/// A stream that the server marks as a shadow stream, with the origin that it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShadowStream {
    pub(crate) name: String,
    pub(crate) origin: String,
}

/// What one search for shadow topics found.
struct Listing {
    /// The names of the stream and the topic at the time of the search.
    names: TopicNames,
    /// Every listed shadow stream of the stream.
    listed: BTreeSet<String>,
    /// Shadow streams to join.
    new: Vec<ShadowStream>,
    /// Shadow streams read so far that are no longer listed.
    gone: Vec<String>,
}

/// The consumer of one topic of a geo consumer: an [`IggyConsumer`], or a fake in the tests.
pub(crate) trait LogConsumer:
    Stream<Item = Result<ReceivedMessage, ReadError>> + Send + Unpin
{
    /// The id of the reader whose stamp the checkpoints of this consumer carry.
    fn reader_id(&self) -> u64;
    fn committer(&self) -> Committer;
    fn mark(&self, checkpoint: Checkpoint) -> Result<(), IggyError>;
    fn seek(
        &mut self,
        partition: u32,
        start: PollingStrategy,
    ) -> BoxFuture<'_, Result<(), IggyError>>;
    fn count_uncounted(&mut self);
    fn shutdown(&mut self) -> BoxFuture<'_, Result<(), IggyError>>;
}

/// What a commit of one log needs, taken from its consumer so the commit does not borrow it.
pub(crate) enum Committer {
    Consumer(IggyConsumerState),
    #[cfg(test)]
    Fake(Arc<dyn Fn(Checkpoint) -> Result<(), IggyError> + Send + Sync>),
}

impl Committer {
    async fn commit(self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        match self {
            Self::Consumer(state) => state.commit(checkpoint).await,
            #[cfg(test)]
            Self::Fake(commit) => commit(checkpoint),
        }
    }
}

/// Where a geo consumer finds and opens its topics: the client, or a fake in the tests.
pub(crate) trait LogCatalog: Send + Sync {
    /// Refuses a configuration that cannot run, before anything is sent.
    fn validate(&self) -> Result<(), IggyError>;
    /// Refuses a transport that cannot join a consumer group.
    fn check_transport(&self) -> BoxFuture<'static, Result<(), IggyError>>;
    /// Looks up the stream and the topic in this cluster, with the init retries of the
    /// builder.
    fn find_local(&self) -> BoxFuture<'static, Result<LocalTopic, IggyError>>;
    /// Opens the consumer of the topic in this cluster, which `find_local` found.
    fn open_local(&self) -> BoxFuture<'static, Result<Box<dyn LogConsumer>, IggyError>>;
    fn names(&self) -> BoxFuture<'static, Result<TopicNames, IggyError>>;
    /// Every shadow stream in the list of the streams that the user may read.
    fn shadow_streams(&self) -> BoxFuture<'static, Result<Arc<[ShadowStream]>, IggyError>>;
    fn holds_topic(&self, stream: &str, topic: &str)
    -> BoxFuture<'static, Result<bool, IggyError>>;
    /// Opens the consumer of the topic in a shadow stream, which `holds_topic` found.
    fn open_shadow(
        &self,
        stream: &str,
        topic: &str,
    ) -> BoxFuture<'static, Result<Box<dyn LogConsumer>, IggyError>>;
}

impl LogConsumer for IggyConsumer {
    fn reader_id(&self) -> u64 {
        IggyConsumer::reader_id(self)
    }

    fn committer(&self) -> Committer {
        Committer::Consumer(self.state())
    }

    fn mark(&self, checkpoint: Checkpoint) -> Result<(), IggyError> {
        IggyConsumer::mark(self, checkpoint)
    }

    fn seek(
        &mut self,
        partition: u32,
        start: PollingStrategy,
    ) -> BoxFuture<'_, Result<(), IggyError>> {
        IggyConsumer::seek(self, partition, start).boxed()
    }

    fn count_uncounted(&mut self) {
        IggyConsumer::count_uncounted(self);
    }

    fn shutdown(&mut self) -> BoxFuture<'_, Result<(), IggyError>> {
        IggyConsumer::shutdown(self).boxed()
    }
}

/// Finds and opens the topics of a geo consumer through its client, with the options of the
/// consumer builder for each one.
pub(crate) struct ClientCatalog {
    consumer: IggyConsumerBuilder,
    /// How old a listing that another geo consumer of the client started may be to serve this
    /// one.
    listing_max_age: Duration,
    /// The cache of the latest search, which keeps it for the other geo consumers of the
    /// client until the next search.
    listings: Arc<Mutex<Option<Arc<ListingCache>>>>,
}

impl ClientCatalog {
    pub(crate) fn new(consumer: IggyConsumerBuilder, discovery_interval: Duration) -> Self {
        Self {
            consumer,
            listing_max_age: discovery_interval / 2,
            listings: Arc::default(),
        }
    }
}

impl LogCatalog for ClientCatalog {
    fn validate(&self) -> Result<(), IggyError> {
        if !self.consumer.is_group() {
            return Err(IggyError::InvalidConfiguration);
        }
        self.consumer.clone().build().validate()
    }

    fn check_transport(&self) -> BoxFuture<'static, Result<(), IggyError>> {
        let client = self.consumer.client();
        async move {
            client.read().await.deferred_group_state().await?;
            Ok(())
        }
        .boxed()
    }

    fn find_local(&self) -> BoxFuture<'static, Result<LocalTopic, IggyError>> {
        let consumer = self.consumer.clone().build();
        async move {
            let (stream, topic) = consumer.find().await?;
            Ok(LocalTopic {
                shadow_origin: stream.shadow_origin(),
                is_geo: topic.resolved_options().topic_type == TopicType::Geo,
                names: TopicNames {
                    stream: stream.name,
                    topic: topic.name,
                },
            })
        }
        .boxed()
    }

    fn open_local(&self) -> BoxFuture<'static, Result<Box<dyn LogConsumer>, IggyError>> {
        let mut consumer = self.consumer.clone().build();
        async move {
            consumer.init_found().await?;
            Ok(Box::new(consumer) as Box<dyn LogConsumer>)
        }
        .boxed()
    }

    fn names(&self) -> BoxFuture<'static, Result<TopicNames, IggyError>> {
        let client = self.consumer.client();
        let stream = self.consumer.stream_id().clone();
        let topic = self.consumer.topic_id().clone();
        async move {
            let stream_name = match stream.kind {
                IdKind::String => stream.get_string_value()?,
                IdKind::Numeric => {
                    client
                        .read()
                        .await
                        .get_stream(&stream)
                        .await?
                        .ok_or_else(|| {
                            IggyError::StreamIdNotFound(Identifier::from_identifier(&stream))
                        })?
                        .name
                }
            };
            let topic_name = match topic.kind {
                IdKind::String => topic.get_string_value()?,
                IdKind::Numeric => {
                    client
                        .read()
                        .await
                        .get_topic(&stream, &topic)
                        .await?
                        .ok_or_else(|| {
                            IggyError::TopicIdNotFound(
                                Identifier::from_identifier(&topic),
                                Identifier::from_identifier(&stream),
                            )
                        })?
                        .name
                }
            };
            Ok(TopicNames {
                stream: stream_name,
                topic: topic_name,
            })
        }
        .boxed()
    }

    fn shadow_streams(&self) -> BoxFuture<'static, Result<Arc<[ShadowStream]>, IggyError>> {
        let client = self.consumer.client();
        let listings = Arc::clone(&self.listings);
        let max_age = self.listing_max_age;
        async move {
            // Looked up on each search, because a login as another user takes another cache.
            let cache = ListingCache::of(&client).await;
            *listings.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&cache));
            cache.shadow_streams(max_age).await
        }
        .boxed()
    }

    fn holds_topic(
        &self,
        stream: &str,
        topic: &str,
    ) -> BoxFuture<'static, Result<bool, IggyError>> {
        let client = self.consumer.client();
        let (stream, topic) = (stream.to_owned(), topic.to_owned());
        async move {
            let topic = client
                .read()
                .await
                .get_topic(&Identifier::named(&stream)?, &Identifier::named(&topic)?)
                .await?;
            Ok(topic.is_some())
        }
        .boxed()
    }

    fn open_shadow(
        &self,
        stream: &str,
        topic: &str,
    ) -> BoxFuture<'static, Result<Box<dyn LogConsumer>, IggyError>> {
        let consumer = self.consumer.clone();
        let (stream, topic) = (stream.to_owned(), topic.to_owned());
        async move {
            let mut consumer = consumer
                .stream(Identifier::named(&stream)?)
                .topic(Identifier::named(&topic)?)
                .build();
            consumer.init_found().await?;
            Ok(Box::new(consumer) as Box<dyn LogConsumer>)
        }
        .boxed()
    }
}

type SharedListing = Shared<BoxFuture<'static, Result<Arc<[ShadowStream]>, IggyError>>>;

/// The listing cache of each signed-in session of each client.
static LISTING_CACHES: Mutex<Vec<ListingKey>> = Mutex::new(Vec::new());

/// The session that a listing cache serves. The `Weak` keeps the allocation of the session
/// state, so no other client takes its address while the key is held.
struct ListingKey {
    session: Weak<SessionOwner>,
    /// The lineage of the sign-in, which rises at each login and logout, so a listing of one
    /// user never serves another.
    lineage: u64,
    cache: Weak<ListingCache>,
}

/// The latest listing of the shadow streams through one signed-in session of one client, which
/// each geo consumer of the client reuses while it is young enough.
struct ListingCache {
    client: IggyRwLock<ClientWrapper>,
    /// The latest listing, with the time it started.
    latest: Mutex<Option<(Instant, SharedListing)>>,
}

impl ListingCache {
    /// The cache of the current sign-in of `client`. A client without a session, on HTTP, gets
    /// a cache of its own.
    async fn of(client: &IggyRwLock<ClientWrapper>) -> Arc<Self> {
        match client.read().await.session_lineage().await {
            Some((session, lineage)) => Self::shared(&session, lineage, client),
            None => Arc::new(Self::new(client)),
        }
    }

    /// The cache of `lineage` of `session`, which the geo consumers of each clone of the
    /// client share. Keys whose session or cache is gone are dropped.
    fn shared(
        session: &Arc<SessionOwner>,
        lineage: u64,
        client: &IggyRwLock<ClientWrapper>,
    ) -> Arc<Self> {
        let mut keys = LISTING_CACHES
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        keys.retain(|key| key.session.strong_count() > 0 && key.cache.strong_count() > 0);
        let session_key = Arc::downgrade(session);
        let found = keys
            .iter()
            .filter(|key| key.lineage == lineage && Weak::ptr_eq(&key.session, &session_key))
            .find_map(|key| key.cache.upgrade());
        if let Some(cache) = found {
            return cache;
        }
        let cache = Arc::new(Self::new(client));
        keys.push(ListingKey {
            session: session_key,
            lineage,
            cache: Arc::downgrade(&cache),
        });
        cache
    }

    fn new(client: &IggyRwLock<ClientWrapper>) -> Self {
        Self {
            client: client.clone(),
            latest: Mutex::new(None),
        }
    }

    fn shadow_streams(&self, max_age: Duration) -> SharedListing {
        self.listing(max_age, || list_shadow_streams(self.client.clone()).boxed())
    }

    /// The listing that started less than `max_age` ago, or a new one from `list`.
    fn listing(
        &self,
        max_age: Duration,
        list: impl FnOnce() -> BoxFuture<'static, Result<Vec<ShadowStream>, IggyError>>,
    ) -> SharedListing {
        let now = Instant::now();
        let mut latest = self.latest.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((started, listing)) = latest.as_ref()
            && now.saturating_duration_since(*started) < max_age
        {
            return listing.clone();
        }
        let listing = list().map(|listed| listed.map(Arc::from)).boxed().shared();
        *latest = Some((now, listing.clone()));
        listing
    }
}

/// Lists every stream whose name starts as a shadow stream's does, and keeps the ones that the
/// server marks as shadow streams.
async fn list_shadow_streams(
    client: IggyRwLock<ClientWrapper>,
) -> Result<Vec<ShadowStream>, IggyError> {
    let client = client.read().await;
    let mut query = NamedPageQuery {
        page: PageQuery::whole_list(),
        name: Some(SHADOW_STREAM_PREFIX.to_owned()),
    };
    let mut shadows = Vec::new();
    loop {
        let page = client.get_streams_page(&query).await?;
        let next = query.next(&page)?;
        shadows.extend(page.items.into_iter().filter_map(|stream| {
            Some(ShadowStream {
                origin: stream.shadow_origin()?,
                name: stream.name,
            })
        }));
        match next {
            Some(next) => query = next,
            None => return Ok(shadows),
        }
    }
}

/// Checks the transport and looks up the names. When `sources` reads the topic in this
/// cluster, it also checks that topic and opens its consumer, so a failure leaves no consumer
/// open.
async fn start(catalog: Arc<dyn LogCatalog>, sources: GeoSources) -> Result<Started, IggyError> {
    catalog.check_transport().await?;
    if !sources.reads_local() {
        let names = catalog.names().await?;
        return Ok(Started { local: None, names });
    }
    let found = catalog.find_local().await?;
    if let Some(origin) = &found.shadow_origin {
        error!(
            "Stream: {} is the shadow stream of origin: {origin}. A geo consumer reads the stream of the topic in this cluster, and finds its shadows itself.",
            found.names.stream
        );
        return Err(IggyError::InvalidConfiguration);
    }
    if !found.is_geo {
        error!(
            "Topic: {} of stream: {} is not a geo topic, so it has no shadow topics.",
            found.names.topic, found.names.stream
        );
        return Err(IggyError::InvalidConfiguration);
    }
    let local = catalog.open_local().await?;
    Ok(Started {
        local: Some(local),
        names: found.names,
    })
}

/// Shuts down every consumer, those still opening or closing too. The consumers that are open
/// shut down at once, without waiting for the others.
async fn close(
    name: String,
    starting: Option<BoxFuture<'static, Result<Started, IggyError>>>,
    joins: FuturesUnordered<BoxFuture<'static, (ShadowStream, Joined)>>,
    closing: FuturesUnordered<BoxFuture<'static, Closed>>,
    logs: Vec<Log>,
) -> Result<(), IggyError> {
    let mut shutdowns: FuturesUnordered<BoxFuture<'static, Option<Closed>>> =
        FuturesUnordered::new();
    for log in logs {
        shutdowns.push(
            shut_down(log.origin, log.stream, log.consumer)
                .map(Some)
                .boxed(),
        );
    }
    if let Some(starting) = starting {
        shutdowns.push(
            async move {
                let Ok(Started {
                    local: Some(local),
                    names,
                }) = starting.await
                else {
                    return None;
                };
                Some(shut_down(GeoOrigin::local(), names.stream, local).await)
            }
            .boxed(),
        );
    }
    for join in joins {
        shutdowns.push(
            async move {
                let (shadow, Joined::Opened(consumer)) = join.await else {
                    return None;
                };
                Some(shut_down(GeoOrigin::shadow(&shadow.origin), shadow.name, consumer).await)
            }
            .boxed(),
        );
    }
    for removal in closing {
        shutdowns.push(removal.map(Some).boxed());
    }
    let mut local_failure = None;
    let mut failure = None;
    while let Some(closed) = shutdowns.next().await {
        let Some(Closed {
            origin,
            stream,
            result: Err(error),
        }) = closed
        else {
            continue;
        };
        warn!(
            "Failed to shut down the consumer of stream: {stream} of origin: {origin} of geo consumer: {name}. {error}"
        );
        if origin.is_local() {
            local_failure.get_or_insert(error);
        } else {
            failure.get_or_insert(error);
        }
    }
    local_failure.or(failure).map_or(Ok(()), Err)
}

async fn shut_down(
    origin: GeoOrigin,
    stream: String,
    mut consumer: Box<dyn LogConsumer>,
) -> Closed {
    let result = consumer.shutdown().await;
    Closed {
        origin,
        stream,
        result,
    }
}

/// Looks up the names, lists the streams, and sorts the shadow streams of the topic. `open`
/// names the shadow streams read now, and `skipped` those not to join now: joining, closing,
/// or refused and not due again.
async fn search(
    catalog: Arc<dyn LogCatalog>,
    open: BTreeSet<String>,
    skipped: BTreeSet<String>,
) -> Result<Listing, IggyError> {
    let names = catalog.names().await?;
    let listed = catalog.shadow_streams().await?;
    let shadows: Vec<ShadowStream> = listed
        .iter()
        .filter(|shadow| shadow.name == shadow_stream_name(&shadow.origin, &names.stream))
        .cloned()
        .collect();
    let gone = open
        .iter()
        .filter(|stream| !shadows.iter().any(|shadow| shadow.name == **stream))
        .cloned()
        .collect();
    let listed = shadows.iter().map(|shadow| shadow.name.clone()).collect();
    let new = shadows
        .into_iter()
        .filter(|shadow| !open.contains(&shadow.name) && !skipped.contains(&shadow.name))
        .collect();
    Ok(Listing {
        names,
        listed,
        new,
        gone,
    })
}

/// Whether the shadow stream `stream` still holds `topic`. An error other than a missing
/// stream or topic counts as holding it, so the error of its consumer is reported.
async fn check(catalog: Arc<dyn LogCatalog>, stream: String, topic: String) -> (String, bool) {
    let holds_topic = match catalog.holds_topic(&stream, &topic).await {
        Ok(holds_topic) => holds_topic,
        Err(error) => !names_missing_log(&error),
    };
    (stream, holds_topic)
}

/// Opens the consumer of the topic in `shadow`, which joins its group there, when the stream
/// holds the topic.
async fn join(
    catalog: Arc<dyn LogCatalog>,
    shadow: ShadowStream,
    topic: String,
) -> (ShadowStream, Joined) {
    let joined = match catalog.holds_topic(&shadow.name, &topic).await {
        Ok(true) => match catalog.open_shadow(&shadow.name, &topic).await {
            Ok(consumer) => Joined::Opened(consumer),
            Err(error) if names_missing_log(&error) => Joined::Absent,
            Err(error) => Joined::Refused(error),
        },
        Ok(false) => Joined::Absent,
        Err(error) if names_missing_log(&error) => Joined::Absent,
        Err(error) => Joined::Refused(error),
    };
    (shadow, joined)
}

/// `now` plus `delay`. `init` refuses an interval whose longest delay overflows, so the
/// fallback is never taken.
fn deadline(now: Instant, delay: Duration) -> Instant {
    now.checked_add(delay).unwrap_or(now)
}

fn shadow_stream_name(origin: &str, stream: &str) -> String {
    format!("{SHADOW_STREAM_PREFIX}{origin}_{stream}")
}

/// Whether `error` says that the stream or the topic of a log does not exist.
fn names_missing_log(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::StreamIdNotFound(..)
            | IggyError::StreamNameNotFound(..)
            | IggyError::TopicIdNotFound(..)
            | IggyError::TopicNameNotFound(..)
    )
}

#[cfg(test)]
mod tests;
