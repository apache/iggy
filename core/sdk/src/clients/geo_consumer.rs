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
use crate::clients::consumer::{IggyConsumer, ReceivedMessage};
use crate::clients::consumer_builder::IggyConsumerBuilder;
use crate::clients::reader::ReadError;
use futures::future::{BoxFuture, join_all};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, Stream, StreamExt};
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    Checkpoint, IdKind, Identifier, IggyError, IggyMessage, NamedPageQuery, NonZeroIggyDuration,
    PageQuery, PollingStrategy, StreamClient, TopicClient,
};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Sleep, sleep_until};
use tracing::{debug, info, warn};

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
/// - The consumer takes turns between the logs, so no log holds back the others. Each shadow
///   topic is read from the moment its consumer joins its group, so a slow join holds back
///   neither the other topics nor [`init`](Self::init).
/// - Every [`discovery_interval`](crate::prelude::IggyGeoConsumerBuilder::discovery_interval),
///   while the application reads, it lists the streams: it starts reading each new shadow
///   topic, and stops reading each one that is gone, such as the shadow of a retired origin.
///   The list holds only the streams that the user may read.
/// - An error of one log comes as a [`GeoReadError`] with its origin, and the other logs read
///   on. A shadow topic that the consumer cannot open is reported once and tried again later.
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
///     consumer.commit(received.checkpoint()).await?;
/// }
/// consumer.shutdown().await?;
/// # Ok(())
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
    /// The names that the shadow streams and topics derive from, known after `init`.
    names: Option<Arc<TopicNames>>,
    logs: Vec<Log>,
    /// The log polled first, so each one gets a turn.
    turn: usize,
    /// The index in `logs` of the log that yielded the last message. Kept right by
    /// [`Self::remove_log`].
    last_yielded: Option<usize>,
    search: Option<BoxFuture<'static, Search>>,
    next_search: Option<Pin<Box<Sleep>>>,
    /// The consumers of shadow topics that are joining their groups.
    joins: FuturesUnordered<BoxFuture<'static, (ShadowStream, Joined)>>,
    /// The shadow streams of `joins`, by name.
    joining: BTreeSet<String>,
    /// Shadow streams that refused to open, by name.
    refused: HashMap<String, Refused>,
    /// Shadow streams whose consumer reported its stream or topic missing, with that error, by
    /// name. The next search drops each one that is gone, and reports the others.
    missing: HashMap<String, GeoReadError>,
    listing_failed: bool,
    errors: VecDeque<GeoReadError>,
    /// The shutdowns of the consumers of shadows that are gone.
    closing: FuturesUnordered<BoxFuture<'static, ()>>,
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
            turn: 0,
            last_yielded: None,
            search: None,
            next_search: None,
            joins: FuturesUnordered::new(),
            joining: BTreeSet::new(),
            refused: HashMap::new(),
            missing: HashMap::new(),
            listing_failed: false,
            errors: VecDeque::new(),
            closing: FuturesUnordered::new(),
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
    /// It initializes the consumer of the topic in this cluster as [`IggyConsumer::init`] does,
    /// lists the streams, and starts a consumer for each shadow topic. It does not wait for
    /// those to join their groups: each shadow topic is read from the moment its consumer
    /// joins. A shadow topic that fails to open does not fail the call: its error comes from
    /// the stream as a [`GeoReadError`], and the consumer tries it again later.
    ///
    /// A call that the caller drops part way keeps what it did in the consumer, and the next
    /// call goes on from there, with the same consumer of the topic in this cluster.
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidConfiguration`] for a consumer that is not a consumer group, and
    ///   once [`shutdown()`](Self::shutdown) has started.
    /// - The refusals of the configuration of [`IggyConsumer::init`], before anything is sent,
    ///   for each value of [`GeoSources`].
    /// - The errors of [`IggyConsumer::init`] for the topic in this cluster.
    /// - The errors of a lookup of the stream or the topic, when the builder named either by
    ///   its numeric id: the consumer needs their names to find the shadow topics.
    pub async fn init(&mut self) -> Result<(), IggyError> {
        loop {
            match &mut self.phase {
                Phase::Ready => return Ok(()),
                Phase::Closing(_) | Phase::Closed => return Err(IggyError::InvalidConfiguration),
                Phase::New => {
                    self.catalog.validate()?;
                    info!(
                        "Initializing geo consumer: {} for stream: {}, topic: {}, sources: {:?}...",
                        self.name, self.stream, self.topic, self.sources
                    );
                    self.phase = Phase::Starting(
                        start(
                            Arc::clone(&self.catalog),
                            self.sources.reads_local(),
                            self.name.clone(),
                            self.stream.clone(),
                        )
                        .boxed(),
                    );
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

    /// Takes the consumer of the topic in this cluster and the names that `start` found, and
    /// starts the first search for shadow topics.
    fn adopt(&mut self, started: Started) {
        let names = Arc::new(started.names);
        if let Some(local) = started.local {
            self.logs
                .push(Log::new(GeoOrigin::local(), names.stream.clone(), local));
        }
        if self.sources.reads_shadows() {
            self.search = Some(self.start_search(Arc::clone(&names)));
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
        let commit = self
            .log_of(checkpoint)
            .map(|log| log.consumer.commit(checkpoint));
        async move { commit?.await }
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
    /// The consumers of shadow topics that are still joining, and that of the topic in this
    /// cluster when [`init()`](Self::init) stopped part way, finish first and shut down too.
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

    /// Moves every consumer, and every one still opening, into the work of a shutdown, which
    /// owns them so that a dropped `shutdown()` call loses none.
    fn take_for_shutdown(&mut self) -> BoxFuture<'static, Result<(), IggyError>> {
        let starting = match std::mem::replace(&mut self.phase, Phase::Closed) {
            Phase::Starting(starting) => Some(starting),
            _ => None,
        };
        // A search only lists and looks up, so it holds no consumer.
        self.search = None;
        self.next_search = None;
        self.last_yielded = None;
        self.joining.clear();
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

    /// Under `CommitAfter::Processing`, the message that the consumer yielded last counts once
    /// the consumer is polled again, also when its log does not get the first turn.
    fn count_uncounted(&mut self) {
        if let Some(index) = self.last_yielded.take()
            && let Some(log) = self.logs.get_mut(index)
        {
            log.consumer.count_uncounted();
        }
    }

    /// Removes the log at `index`, and moves the indexes into `logs` that follow it.
    fn remove_log(&mut self, index: usize) -> Log {
        let log = self.logs.remove(index);
        if index < self.turn {
            self.turn -= 1;
        }
        self.last_yielded = match self.last_yielded {
            Some(last) if last == index => None,
            Some(last) if last > index => Some(last - 1),
            last => last,
        };
        log
    }

    fn poll_closing(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some(())) = self.closing.poll_next_unpin(cx) {}
    }

    /// Starts the search for shadow topics when it is due, and applies its result when it
    /// ends. At most one search runs at a time.
    fn poll_search(&mut self, cx: &mut Context<'_>) {
        if !self.sources.reads_shadows() {
            return;
        }
        if self.search.is_none() {
            let Some(names) = self.names.clone() else {
                return;
            };
            let due = self
                .next_search
                .as_mut()
                .is_none_or(|next| next.as_mut().poll(cx).is_ready());
            if !due {
                return;
            }
            self.search = Some(self.start_search(names));
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

    fn start_search(&self, names: Arc<TopicNames>) -> BoxFuture<'static, Search> {
        let now = Instant::now();
        let open = self
            .logs
            .iter()
            .filter(|log| log.origin.is_shadow())
            .map(|log| log.stream.clone())
            .collect();
        let missing = self.missing.keys().cloned().collect();
        let skipped = self
            .refused
            .iter()
            .filter(|(_, refused)| refused.retry_at > now)
            .map(|(stream, _)| stream.clone())
            .chain(self.joining.iter().cloned())
            .collect();
        search(Arc::clone(&self.catalog), names, open, missing, skipped).boxed()
    }

    fn apply_search(&mut self, found: Search) {
        let now = Instant::now();
        match found.listing {
            None => self.listing_failed = false,
            Some(error) => {
                if !self.listing_failed {
                    warn!(
                        "Geo consumer: {} cannot list the streams to find the shadow topics. {error}",
                        self.name
                    );
                    self.errors.push_back(GeoReadError::of_consumer(error));
                }
                self.listing_failed = true;
            }
        }
        for stream in found.gone {
            self.missing.remove(&stream);
            self.drop_shadow(&stream);
        }
        for stream in found.present {
            if let Some(error) = self.missing.remove(&stream) {
                self.errors.push_back(error);
            }
        }
        if let Some(names) = &self.names {
            for shadow in found.new {
                self.joining.insert(shadow.name.clone());
                self.joins
                    .push(join(Arc::clone(&self.catalog), shadow, names.topic.clone()).boxed());
            }
        }
        let next = now + self.discovery_interval;
        match self.next_search.as_mut() {
            Some(sleep) => sleep.as_mut().reset(next),
            None => self.next_search = Some(Box::pin(sleep_until(next))),
        }
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
                    self.logs.push(Log::new(
                        GeoOrigin::shadow(&shadow.origin),
                        shadow.name,
                        consumer,
                    ));
                }
                Joined::Refused(error) => self.refuse(shadow, error),
                Joined::Absent => {}
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
        refused.retry_at = now + self.discovery_interval.saturating_mul(backoff);
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
        let mut log = self.remove_log(index);
        info!(
            "Geo consumer: {} stops reading the shadow stream: {} of origin: {}, which no longer holds the topic.",
            self.name, log.stream, log.origin
        );
        let name = self.name.clone();
        self.closing.push(
            async move {
                if let Err(error) = log.consumer.shutdown().await {
                    debug!(
                        "Geo consumer: {name} shut down the consumer of the gone shadow stream: {}. {error}",
                        log.stream
                    );
                }
            }
            .boxed(),
        );
    }

    /// Polls each log once, from the one whose turn it is, and returns the first message or
    /// error.
    fn poll_logs(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<GeoReceivedMessage, GeoReadError>>> {
        let mut unpolled = self.logs.len();
        let mut index = self.turn;
        let mut polled_again = false;
        while unpolled > 0 {
            index %= self.logs.len();
            let log = &mut self.logs[index];
            match log.consumer.poll_next_unpin(cx) {
                Poll::Pending => {
                    unpolled -= 1;
                    index += 1;
                    polled_again = false;
                }
                Poll::Ready(Some(Ok(received))) => {
                    let received = GeoReceivedMessage::new(received, log.origin.clone());
                    self.last_yielded = Some(index);
                    self.turn = index + 1;
                    return Poll::Ready(Some(Ok(received)));
                }
                Poll::Ready(Some(Err(error))) => {
                    let error = GeoReadError::of_log(log, error);
                    // A node that did not apply a new shadow yet answers not found too, so the
                    // next search decides whether the shadow is gone. The same log is polled
                    // again, because a ready poll left no wake behind.
                    if log.origin.is_shadow() && names_missing_log(&error.error.error) {
                        debug!(
                            "Geo consumer: {} checks the shadow stream: {} at its next search. {error}",
                            self.name, log.stream
                        );
                        self.missing.insert(log.stream.clone(), error);
                        if polled_again {
                            // A log that fails again without waiting is left for the next
                            // poll, so it cannot hold the task.
                            cx.waker().wake_by_ref();
                            unpolled -= 1;
                            index += 1;
                            polled_again = false;
                        } else {
                            polled_again = true;
                        }
                        continue;
                    }
                    self.turn = index + 1;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    let log = self.remove_log(index);
                    debug!(
                        "Geo consumer: {} stops reading the stream: {}, whose consumer ended.",
                        self.name, log.stream
                    );
                    unpolled -= 1;
                    polled_again = false;
                }
            }
        }
        Poll::Pending
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
        consumer.poll_closing(cx);
        if let Some(error) = consumer.errors.pop_front() {
            return Poll::Ready(Some(Err(error)));
        }
        consumer.poll_logs(cx)
    }
}

/// One topic that the geo consumer reads, through its own consumer.
struct Log {
    origin: GeoOrigin,
    /// The name of the stream of the topic: the stream of the consumer, or a shadow stream.
    stream: String,
    reader: u64,
    consumer: Box<dyn LogConsumer>,
}

impl Log {
    fn new(origin: GeoOrigin, stream: String, consumer: Box<dyn LogConsumer>) -> Self {
        Self {
            origin,
            stream,
            reader: consumer.reader_id(),
            consumer,
        }
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
    /// Opening the consumer of the topic in this cluster and looking up the names.
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

/// The names of the stream and the topic of the consumer, which its shadow topics share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicNames {
    pub(crate) stream: String,
    pub(crate) topic: String,
}

/// A stream that the server marks as a shadow stream, with the origin that it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShadowStream {
    pub(crate) name: String,
    pub(crate) origin: String,
}

/// What one search for shadow topics found.
#[derive(Default)]
struct Search {
    /// Set when the listing of the streams failed, and then nothing else was looked at.
    listing: Option<IggyError>,
    /// Shadow streams of the topic to join.
    new: Vec<ShadowStream>,
    /// Shadow streams read so far whose stream or topic is gone.
    gone: Vec<String>,
    /// Shadow streams reported missing that still hold the topic.
    present: Vec<String>,
}

/// The consumer of one topic of a geo consumer: an [`IggyConsumer`], or a fake in the tests.
pub(crate) trait LogConsumer:
    Stream<Item = Result<ReceivedMessage, ReadError>> + Send + Unpin
{
    /// The id of the reader whose stamp the checkpoints of this consumer carry.
    fn reader_id(&self) -> u64;
    fn commit(&self, checkpoint: Checkpoint) -> BoxFuture<'static, Result<(), IggyError>>;
    fn mark(&self, checkpoint: Checkpoint) -> Result<(), IggyError>;
    fn seek(
        &mut self,
        partition: u32,
        start: PollingStrategy,
    ) -> BoxFuture<'_, Result<(), IggyError>>;
    fn count_uncounted(&mut self);
    fn shutdown(&mut self) -> BoxFuture<'_, Result<(), IggyError>>;
}

/// Where a geo consumer finds and opens its topics: the client, or a fake in the tests.
pub(crate) trait LogCatalog: Send + Sync {
    /// Refuses a configuration that cannot run, before anything is sent.
    fn validate(&self) -> Result<(), IggyError>;
    fn open_local(&self) -> BoxFuture<'static, Result<Box<dyn LogConsumer>, IggyError>>;
    fn names(&self) -> BoxFuture<'static, Result<TopicNames, IggyError>>;
    /// Every shadow stream in the list of the streams that the user may read.
    fn shadow_streams(&self) -> BoxFuture<'static, Result<Vec<ShadowStream>, IggyError>>;
    fn holds_topic(&self, stream: &str, topic: &str)
    -> BoxFuture<'static, Result<bool, IggyError>>;
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

    fn commit(&self, checkpoint: Checkpoint) -> BoxFuture<'static, Result<(), IggyError>> {
        let state = self.state();
        async move { state.commit(checkpoint).await }.boxed()
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
    client: IggyRwLock<ClientWrapper>,
    consumer: IggyConsumerBuilder,
}

impl ClientCatalog {
    pub(crate) fn new(consumer: IggyConsumerBuilder) -> Self {
        Self {
            client: consumer.client(),
            consumer,
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

    fn open_local(&self) -> BoxFuture<'static, Result<Box<dyn LogConsumer>, IggyError>> {
        let mut consumer = self.consumer.clone().build();
        async move {
            consumer.init().await?;
            Ok(Box::new(consumer) as Box<dyn LogConsumer>)
        }
        .boxed()
    }

    fn names(&self) -> BoxFuture<'static, Result<TopicNames, IggyError>> {
        let client = self.client.clone();
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

    fn shadow_streams(&self) -> BoxFuture<'static, Result<Vec<ShadowStream>, IggyError>> {
        let client = self.client.clone();
        async move {
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
        .boxed()
    }

    fn holds_topic(
        &self,
        stream: &str,
        topic: &str,
    ) -> BoxFuture<'static, Result<bool, IggyError>> {
        let client = self.client.clone();
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
            // The search already found the topic, so a missing one is gone, not late.
            let mut consumer = consumer
                .stream(Identifier::named(&stream)?)
                .topic(Identifier::named(&topic)?)
                .init_retries(0, NonZeroIggyDuration::ONE_SECOND)
                .build();
            consumer.init().await?;
            Ok(Box::new(consumer) as Box<dyn LogConsumer>)
        }
        .boxed()
    }
}

/// Opens the consumer of the topic in this cluster when `reads_local` is set, and looks up the
/// names of the stream and the topic. A failure leaves no consumer open.
async fn start(
    catalog: Arc<dyn LogCatalog>,
    reads_local: bool,
    name: String,
    stream: Identifier,
) -> Result<Started, IggyError> {
    let local = if reads_local {
        Some(catalog.open_local().await?)
    } else {
        None
    };
    match catalog.names().await {
        Ok(names) => Ok(Started { local, names }),
        Err(error) => {
            if let Some(mut local) = local
                && let Err(shutdown_error) = local.shutdown().await
            {
                warn!(
                    "Failed to shut down the consumer of stream: {stream} of geo consumer: {name}. {shutdown_error}"
                );
            }
            Err(error)
        }
    }
}

/// Waits for the consumers that are still opening, and shuts down every consumer.
async fn close(
    name: String,
    starting: Option<BoxFuture<'static, Result<Started, IggyError>>>,
    mut joins: FuturesUnordered<BoxFuture<'static, (ShadowStream, Joined)>>,
    mut closing: FuturesUnordered<BoxFuture<'static, ()>>,
    mut logs: Vec<Log>,
) -> Result<(), IggyError> {
    if let Some(starting) = starting
        && let Ok(Started {
            local: Some(local),
            names,
        }) = starting.await
    {
        logs.insert(0, Log::new(GeoOrigin::local(), names.stream, local));
    }
    while let Some((shadow, joined)) = joins.next().await {
        if let Joined::Opened(consumer) = joined {
            logs.push(Log::new(
                GeoOrigin::shadow(&shadow.origin),
                shadow.name,
                consumer,
            ));
        }
    }
    while closing.next().await.is_some() {}
    let shutdowns = logs.into_iter().map(|mut log| async move {
        let result = log.consumer.shutdown().await;
        (log, result)
    });
    let mut failure = None;
    for (log, result) in join_all(shutdowns).await {
        if let Err(error) = result {
            warn!(
                "Failed to shut down the consumer of stream: {} of origin: {} of geo consumer: {name}. {error}",
                log.stream, log.origin
            );
            failure.get_or_insert(error);
        }
    }
    failure.map_or(Ok(()), Err)
}

/// Lists the streams and returns each shadow topic of `names` to join. `open` names the shadow
/// streams read now, `missing` those whose consumer reported them missing, and `skipped` those
/// joining now or refused and not due again.
async fn search(
    catalog: Arc<dyn LogCatalog>,
    names: Arc<TopicNames>,
    open: BTreeSet<String>,
    missing: BTreeSet<String>,
    skipped: BTreeSet<String>,
) -> Search {
    let mut found = Search::default();
    let listed = match catalog.shadow_streams().await {
        Ok(listed) => listed,
        Err(error) => {
            found.listing = Some(error);
            return found;
        }
    };
    let shadows: Vec<ShadowStream> = listed
        .into_iter()
        .filter(|shadow| shadow.name == shadow_stream_name(&shadow.origin, &names.stream))
        .collect();
    for stream in &open {
        if !shadows.iter().any(|shadow| shadow.name == *stream) {
            found.gone.push(stream.clone());
        }
    }
    let listed_missing: Vec<&String> = missing
        .iter()
        .filter(|stream| !found.gone.contains(stream))
        .collect();
    for stream in listed_missing {
        match catalog.holds_topic(stream, &names.topic).await {
            Ok(false) => found.gone.push(stream.clone()),
            Err(error) if names_missing_log(&error) => found.gone.push(stream.clone()),
            Ok(true) | Err(_) => found.present.push(stream.clone()),
        }
    }
    found.new = shadows
        .into_iter()
        .filter(|shadow| !open.contains(&shadow.name) && !skipped.contains(&shadow.name))
        .collect();
    found
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
