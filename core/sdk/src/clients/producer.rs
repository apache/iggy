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

use super::ORDERING;
use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::MAX_BATCH_LENGTH;
use crate::clients::producer_builder::SendMode;
use crate::clients::producer_config::DirectConfig;
use crate::clients::producer_dispatcher::ProducerDispatcher;
use bytes::Bytes;
use futures_util::StreamExt;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{Client, MessageClient, StreamClient, TopicClient, TopicCreateOptions};
use iggy_common::{
    DiagnosticEvent, EncryptorKind, IdKind, Identifier, IggyError, IggyExpiry, IggyMessage,
    IggyTimestamp, MaxTopicSize, NonZeroIggyDuration, Partitioner, Partitioning,
    SendMessagesConfirmationResponse, SendMessagesResponse,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;
use tokio::time::{Interval, sleep};
use tracing::{error, info, trace, warn};

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait ProducerCoreBackend: Send + Sync + 'static {
    /// Sends `msgs`, returning the confirmations of every chunk the send was
    /// split into, concatenated in chunk order.
    fn send_internal(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> impl Future<Output = Result<SendMessagesResponse, IggyError>> + Send;
}

/// Reply for a send that produced no confirmation: nothing was sent, or the
/// send is still queued on a background dispatcher.
pub(crate) fn no_confirmations() -> SendMessagesResponse {
    SendMessagesResponse {
        confirmations: Vec::new(),
    }
}

/// True when `error` can only have been raised after the server committed the
/// batch. Resending then turns one durable write into as many copies as the
/// retry budget allows, on a plane that keeps no reply cache to collapse them.
///
/// Both kinds are raised while decoding the HTTP reply body, which is reached
/// only once the status check has accepted a 2xx, so the batch landed and just
/// its confirmation is unreadable. The binary path degrades an unreadable body
/// to an empty confirmation list and never raises either kind.
///
/// Membership is scoped to the send path and must stay conservative. An error
/// meaning the request never arrived, or that the server rejected it before
/// committing, has to keep retrying.
fn implies_committed_send(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::InvalidBytesResponse | IggyError::InvalidJsonResponse
    )
}

pub struct ProducerCore {
    initialized: AtomicBool,
    can_send: Arc<AtomicBool>,
    client: Arc<IggyRwLock<ClientWrapper>>,
    stream_id: Arc<Identifier>,
    stream_name: String,
    topic_id: Arc<Identifier>,
    topic_name: String,
    partitioning: Option<Arc<Partitioning>>,
    encryptor: Option<Arc<EncryptorKind>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    topic_partitions_count: u32,
    topic_message_expiry: IggyExpiry,
    topic_max_size: MaxTopicSize,
    default_partitioning: Arc<Partitioning>,
    last_sent_at: Arc<AtomicU64>,
    send_retries_count: Option<u32>,
    send_retries_interval: Option<NonZeroIggyDuration>,
    direct_config: Option<DirectConfig>,
}

impl ProducerCore {
    pub async fn init(&self) -> Result<(), IggyError> {
        if self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }

        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        info!("Initializing producer for stream: {stream_id} and topic: {topic_id}...");
        self.subscribe_events().await;
        let client = self.client.clone();
        let client = client.read().await;
        if client.get_stream(&stream_id).await?.is_none() {
            if !self.create_stream_if_not_exists {
                error!("Stream does not exist and auto-creation is disabled.");
                return Err(IggyError::StreamNameNotFound(self.stream_name.clone()));
            }

            let (name, _id) = match stream_id.kind {
                IdKind::Numeric => (
                    self.stream_name.to_owned(),
                    Some(self.stream_id.get_u32_value()?),
                ),
                IdKind::String => (self.stream_id.get_string_value()?, None),
            };
            info!("Creating stream: {name}");
            client.create_stream(&name).await?;
        }

        if client.get_topic(&stream_id, &topic_id).await?.is_none() {
            if !self.create_topic_if_not_exists {
                error!("Topic does not exist and auto-creation is disabled.");
                return Err(IggyError::TopicNameNotFound(
                    self.topic_name.clone(),
                    self.stream_name.clone(),
                ));
            }

            let (name, _id) = match self.topic_id.kind {
                IdKind::Numeric => (
                    self.topic_name.to_owned(),
                    Some(self.topic_id.get_u32_value()?),
                ),
                IdKind::String => (self.topic_id.get_string_value()?, None),
            };
            info!("Creating topic: {name} for stream: {}", self.stream_name);
            client
                .create_topic(
                    &self.stream_id,
                    &self.topic_name,
                    &TopicCreateOptions {
                        partitions_count: Some(self.topic_partitions_count),
                        message_expiry: (self.topic_message_expiry != IggyExpiry::ServerDefault)
                            .then_some(self.topic_message_expiry),
                        max_topic_size: (self.topic_max_size != MaxTopicSize::ServerDefault)
                            .then_some(self.topic_max_size),
                        ..TopicCreateOptions::default()
                    },
                )
                .await?;
        }

        let _ = self
            .initialized
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
        info!("Producer has been initialized for stream: {stream_id} and topic: {topic_id}.");
        Ok(())
    }

    async fn subscribe_events(&self) {
        trace!("Subscribing to diagnostic events");
        let mut receiver;
        {
            let client = self.client.read().await;
            receiver = client.subscribe_events().await;
        }

        let can_send = self.can_send.clone();

        tokio::spawn(async move {
            while let Some(event) = receiver.next().await {
                trace!("Received diagnostic event: {event}");
                match event {
                    DiagnosticEvent::Shutdown => {
                        can_send.store(false, ORDERING);
                        warn!("Client has been shutdown");
                    }
                    DiagnosticEvent::Connected => {
                        can_send.store(false, ORDERING);
                        trace!("Connected to the server");
                    }
                    DiagnosticEvent::Disconnected => {
                        can_send.store(false, ORDERING);
                        warn!("Disconnected from the server");
                    }
                    DiagnosticEvent::SignedIn => {
                        can_send.store(true, ORDERING);
                    }
                    DiagnosticEvent::SignedOut => {
                        can_send.store(false, ORDERING);
                    }
                }
            }
        });
    }

    async fn try_send_messages(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        partitioning: &Arc<Partitioning>,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        let client = self.client.read().await;

        let Some(max_retries) = self.send_retries_count else {
            return client
                .send_messages(stream, topic, partitioning, messages)
                .await;
        };

        if max_retries == 0 {
            return client
                .send_messages(stream, topic, partitioning, messages)
                .await;
        }

        self.wait_until_connected(max_retries, stream, topic)
            .await?;
        self.send_with_retries(&client, max_retries, stream, topic, partitioning, messages)
            .await
    }

    async fn wait_until_connected(
        &self,
        max_retries: u32,
        stream: &Identifier,
        topic: &Identifier,
    ) -> Result<(), IggyError> {
        let mut retries = 0;
        let mut timer: Option<Interval> = None;

        while !self.can_send.load(ORDERING) {
            retries += 1;
            if retries > max_retries {
                error!(
                    "Failed to send messages to topic: {topic}, stream: {stream} \
                     after {max_retries} retries. Client is disconnected."
                );
                return Err(IggyError::CannotSendMessagesDueToClientDisconnection);
            }

            error!(
                "Trying to send messages to topic: {topic}, stream: {stream} \
                 but the client is disconnected. Retrying {retries}/{max_retries}..."
            );

            if let Some(interval) = self.send_retries_interval {
                let timer =
                    timer.get_or_insert_with(|| tokio::time::interval(interval.get_duration()));
                trace!(
                    "Waiting for the next retry to send messages to topic: {topic}, \
                     stream: {stream} for disconnected client..."
                );
                timer.tick().await;
            }
        }
        Ok(())
    }

    async fn send_with_retries(
        &self,
        client: &ClientWrapper,
        max_retries: u32,
        stream: &Identifier,
        topic: &Identifier,
        partitioning: &Arc<Partitioning>,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        let mut retries = 0;
        let mut timer: Option<Interval> = None;

        loop {
            match client
                .send_messages(stream, topic, partitioning, messages)
                .await
            {
                // Only the attempt that finally succeeds yields a confirmation;
                // failed attempts have none to report.
                Ok(confirmation) => return Ok(confirmation),
                Err(error) => {
                    if implies_committed_send(&error) {
                        error!(
                            "Not retrying a send to topic: {topic}, stream: {stream}: the batch \
                             committed and only its confirmation could not be read. {error}."
                        );
                        return Err(error);
                    }

                    retries += 1;
                    if retries > max_retries {
                        error!(
                            "Failed to send messages to topic: {topic}, stream: {stream} \
                             after {max_retries} retries. {error}."
                        );
                        return Err(error);
                    }

                    error!(
                        "Failed to send messages to topic: {topic}, stream: {stream}. \
                         {error} Retrying {retries}/{max_retries}..."
                    );

                    if let Some(interval) = self.send_retries_interval {
                        let timer = timer
                            .get_or_insert_with(|| tokio::time::interval(interval.get_duration()));
                        trace!(
                            "Waiting for the next retry to send messages to topic: {topic}, \
                             stream: {stream}..."
                        );
                        timer.tick().await;
                    }
                }
            }
        }
    }

    fn encrypt_messages(&self, messages: &mut [IggyMessage]) -> Result<(), IggyError> {
        if let Some(encryptor) = &self.encryptor {
            for message in messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
                message.header.payload_length = message.payload.len() as u32;

                if let Some(ref user_headers) = message.user_headers {
                    let encrypted_headers = encryptor.encrypt(user_headers)?;
                    message.header.user_headers_length = encrypted_headers.len() as u32;
                    message.user_headers = Some(Bytes::from(encrypted_headers));
                }
            }
        }
        Ok(())
    }

    fn get_partitioning(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        messages: &[IggyMessage],
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<Arc<Partitioning>, IggyError> {
        if let Some(partitioner) = &self.partitioner {
            trace!("Calculating partition id using custom partitioner.");
            let partition_id = partitioner.calculate_partition_id(stream, topic, messages)?;
            Ok(Arc::new(Partitioning::partition_id(partition_id)))
        } else {
            trace!("Using the provided partitioning.");
            Ok(partitioning.unwrap_or_else(|| {
                self.partitioning
                    .clone()
                    .unwrap_or_else(|| self.default_partitioning.clone())
            }))
        }
    }

    async fn wait_before_sending(interval: u64, last_sent_at: u64) {
        if interval == 0 {
            return;
        }

        let now: u64 = IggyTimestamp::now().into();
        let elapsed = now - last_sent_at;
        if elapsed >= interval {
            trace!("No need to wait before sending messages. {now} - {last_sent_at} = {elapsed}");
            return;
        }

        let remaining = interval - elapsed;
        trace!(
            "Waiting for {remaining} microseconds before sending messages... {interval} - {elapsed} = {remaining}"
        );
        sleep(Duration::from_micros(remaining)).await;
    }

    fn make_failed_error(
        &self,
        cause: IggyError,
        failed: Vec<IggyMessage>,
        committed: Vec<SendMessagesConfirmationResponse>,
    ) -> IggyError {
        IggyError::ProducerSendFailed {
            cause: Box::new(cause),
            failed: Arc::new(failed),
            committed: Arc::new(committed),
            stream_name: self.stream_name.clone(),
            topic_name: self.topic_name.clone(),
        }
    }
}

impl ProducerCoreBackend for ProducerCore {
    async fn send_internal(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        mut msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<SendMessagesResponse, IggyError> {
        if msgs.is_empty() {
            return Ok(no_confirmations());
        }

        if let Err(err) = self.encrypt_messages(&mut msgs) {
            return Err(self.make_failed_error(err, msgs, Vec::new()));
        }

        let part = match self.get_partitioning(stream, topic, &msgs, partitioning.clone()) {
            Ok(p) => p,
            Err(err) => {
                return Err(self.make_failed_error(err, msgs, Vec::new()));
            }
        };

        match &self.direct_config {
            Some(cfg) => {
                let linger_time_micros = cfg.linger_time.as_micros();
                if linger_time_micros > 0 {
                    Self::wait_before_sending(linger_time_micros, self.last_sent_at.load(ORDERING))
                        .await;
                }

                let max = if cfg.batch_length == 0 {
                    MAX_BATCH_LENGTH
                } else {
                    cfg.batch_length as usize
                };
                let mut index = 0;
                let mut confirmations = Vec::with_capacity(msgs.len().div_ceil(max));
                while index < msgs.len() {
                    let end = (index + max).min(msgs.len());
                    let chunk = &mut msgs[index..end];

                    match self.try_send_messages(stream, topic, &part, chunk).await {
                        Ok(response) => confirmations.extend(response.confirmations),
                        Err(err) => {
                            let failed_tail = msgs.split_off(index);
                            return Err(self.make_failed_error(err, failed_tail, confirmations));
                        }
                    }
                    self.last_sent_at
                        .store(IggyTimestamp::now().into(), ORDERING);
                    index = end;
                }
                Ok(SendMessagesResponse { confirmations })
            }
            // background send on
            _ => {
                let response = self
                    .try_send_messages(stream, topic, &part, &mut msgs)
                    .await
                    .map_err(|err| self.make_failed_error(err, msgs, Vec::new()))?;
                self.last_sent_at
                    .store(IggyTimestamp::now().into(), ORDERING);
                Ok(response)
            }
        }
    }
}

unsafe impl Send for IggyProducer {}
unsafe impl Sync for IggyProducer {}

/// Appends messages to one topic of one stream.
///
/// A topic is split into partitions, and a partition is an ordered log that producers append to.
/// `IggyProducer` lets you configure [where](#where-messages-land-and-ordering) and
/// [how](#how-messages-are-sent) messages are sent.
///
/// # Creating a producer
///
/// The easiest way to create a producer is through an [`IggyClient`] with a configured connection.
/// [`IggyClient::producer()`] returns an [`IggyProducerBuilder`] that uses that client's connection.
///
/// Building never talks to the server. [`init()`](Self::init) must be awaited before the first send.
///
/// # Examples
///
/// A producer with the defaults, sending one batch and reading the confirmations:
///
/// ```rust,no_run
/// use iggy::prelude::*;
/// use std::str::FromStr;
///
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
///
/// let producer = client.producer("my-stream", "my-topic")?.build();
/// producer.init().await?;
///
/// let messages = vec![IggyMessage::from_str("hello")?, IggyMessage::from_str("world")?];
/// let response = producer.send(messages).await?;
/// for confirmation in &response.confirmations {
///     println!(
///         "Partition: {}, base offset: {}",
///         confirmation.partition_id, confirmation.base_offset
///     );
/// }
/// # Ok(())
/// # }
/// ```
///
/// A `background` producer, which queues a batch and sends it later, and shuts down cleanly:
///
/// ```rust,no_run
/// use iggy::prelude::*;
/// use std::str::FromStr;
///
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
///
/// let producer = client
///     .producer("my-stream", "my-topic")?
///     .background(
///         BackgroundConfig::builder()
///             .linger_time(IggyDuration::new_from_secs(1))
///             .batch_size(64 * 1024)
///             .build(),
///     )
///     .build();
/// producer.init().await?;
///
/// // Returns once the batch is queued. The send itself happens on a background worker.
/// producer.send_one(IggyMessage::from_str("hello")?).await?;
///
/// // Without graceful shutdown, buffered messages have no completion guarantee and may be lost.
/// producer.shutdown().await;
/// # Ok(())
/// # }
/// ```
///
/// Keying messages to a partition and separating confirmed chunks from the unconfirmed tail:
///
/// ```rust,no_run
/// use iggy::prelude::*;
/// use std::str::FromStr;
///
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
///
/// let producer = client
///     .producer("my-stream", "my-topic")?
///     // Every message of this producer goes to the partition the server derives from the key.
///     .partitioning(Partitioning::messages_key_str("my-key")?)
///     .send_retries(Some(5), Some(NonZeroIggyDuration::ONE_SECOND))
///     .build();
/// producer.init().await?;
///
/// let messages = vec![IggyMessage::from_str("hello")?];
/// if let Err(IggyError::ProducerSendFailed { cause, failed, committed, .. }) =
///     producer.send(messages).await
/// {
///     // `committed` holds confirmations for earlier chunks, `failed` the unconfirmed tail. See
///     // "Retrying and what a failure means" before resending `failed`.
///     eprintln!("{} messages have no usable confirmation: {cause}", failed.len());
///     eprintln!("{} chunk(s) were confirmed before the failure", committed.len());
/// }
/// # Ok(())
/// # }
/// ```
///
/// # How messages are sent
///
/// There are two options: [`direct()`] and [`background()`]. You will pick one of the two modes,
/// and a producer stays in that mode for its lifetime.
///
/// A **direct** producer (the default) sends from the calling task. [`send()`](Self::send) awaits
/// the server and returns its [confirmations](#confirmations). A batch longer than
/// [`DirectConfig::batch_length`] is split into that many messages per request, and the requests are
/// awaited one after another, so a failure in the middle leaves confirmations for the successful
/// prefix. [`DirectConfig::linger_time`] requests a minimum gap between sequential send calls. It
/// does not space out chunks within one call and does not serialize concurrent callers.
///
/// A **background** producer hands the batch to a [`ProducerDispatcher`] and returns after queueing
/// it without waiting for the write. A worker may already have started or completed the write by
/// then, but [`send()`](Self::send) reports no write result and returns no confirmations. The
/// dispatcher runs [`BackgroundConfig::num_shards`] workers, each buffering the batches routed to it
/// and flushing them when one of three limits is hit: [`BackgroundConfig::batch_length`] queued
/// sends, [`BackgroundConfig::batch_size`] bytes, or the next
/// [`BackgroundConfig::linger_time`] deadline. Adjacent buffered sends that share a stream, topic,
/// and partitioning are merged into one request.
///
/// The sharding strategy decides how background dispatch affects message order:
/// - [`BackgroundConfig::sharding`] routes a batch to a worker. The default [`OrderedSharding`] picks
///   it from the stream and the topic, so everything going to one topic stays on one worker and keeps
///   its order. [`BalancedSharding`] spreads batches round-robin and gives up ordering, which can
///   improve throughput when multiple shards are allowed to write concurrently.
/// - [`BackgroundConfig::max_in_flight`] bounds concurrent writes across workers. A worker itself
///   remains sequential for every value and awaits retries before starting its next request. Raising
///   this setting does not break the per-topic order provided by [`OrderedSharding`], but it allows
///   strategies that spread one topic across workers to write those shards concurrently.
///
/// Since a background send is queued rather than written, the dispatcher charges queued and
/// in-flight sends against [`BackgroundConfig::max_buffer_size`] over all workers. The default is
/// bounded, while a value of zero disables the byte budget.
/// [`BackgroundConfig::failure_mode`] decides what a send does when that budget is exhausted. You can block
/// until it frees up (the default), block with a timeout and fail with
/// [`IggyError::BackgroundSendTimeout`], or fail right away with
/// [`IggyError::BackgroundSendBufferOverflow`]. A single send larger than the whole budget always
/// fails with [`IggyError::BackgroundSendBufferOverflow`], whatever the mode.
///
/// In contrast to the direct mode, which awaits the confirmations from the server, a background send
/// has no caller left to return to. Write failures are reported to
/// [`BackgroundConfig::error_callback`] instead. It receives the cause, the unconfirmed tail, and
/// confirmations returned for earlier chunks, see
/// [Retrying and what a failure means](#retrying-and-what-a-failure-means). The default callback
/// logs the context and drops it. A custom [`ErrorCallback`] can retain or persist failed sends
/// according to the application's at-least-once policy.
///
/// # Where messages land and ordering
///
/// The partition is the unit of order in Iggy. Inside one partition, messages receive increasing
/// offsets in the server's append order. Between partitions there is no global order, so a consumer
/// reading several partitions can observe their messages interleaved.
///
/// Two messages therefore stay in order only if both of these hold:
/// 1. they are appended to the **same partition**, and
/// 2. their requests reach the server **one after another**, rather than at the same time.
///
/// Point 1 is what the partitioning strategy and point 2 is what the send mode and the client's own
/// concurrency decide.
///
/// | Setting | Order | What happens |
/// | --- | --- | --- |
/// | [`Partitioning::balanced()`], the default | not guaranteed with multiple partitions | the server chooses a partition per request, so consecutive sends and chunks may land in different logs |
/// | [`Partitioning::partition_id()`], [`Partitioning::messages_key()`] with a stable key, [`partitioner()`] returning a stable id | same partition | every batch is routed to the same log, satisfying the first ordering requirement |
/// | one task, awaiting each [`send()`](Self::send) before the next | sequential | requests reach a partition in call order |
/// | several tasks sharing the producer, or overlapping sends | not guaranteed | requests race, so call order does not determine append order |
/// | `direct` producer | sequential within one call | the calling task writes that call's chunks one after another |
/// | `background` producer with [`OrderedSharding`], the default | sequential per stream/topic pair | one pair is bound to one worker, which writes its queue in order |
/// | `background` producer with [`BalancedSharding`] and multiple shards | not guaranteed | consecutive batches can reach different workers, so a later batch may be written first |
/// | [`BackgroundConfig::max_in_flight`] | depends on sharding | it controls concurrency between workers, while each worker remains sequential |
///
/// In short, per-partition order survives if you name the partition and let one sequential writer
/// write to it, for example keyed or fixed partitioning from one task, or a background producer with
/// ordered sharding. [`Partitioning::messages_key()`] maps the same key consistently to a partition
/// for a fixed topic layout. Different keys can share a partition, so this does not create a
/// separate physical log per key.
///
/// One thing this does not protect against is a message appearing twice. Delivery is at-least-once,
/// so a retried batch can be appended a second time at a higher offset, see
/// [Retrying and what a failure means](#retrying-and-what-a-failure-means).
///
/// # Confirmations
///
/// A successful direct send returns [`SendMessagesResponse`], normally holding one
/// [`SendMessagesConfirmationResponse`] per chunk. Each confirmation records a partition and the
/// `base_offset` assigned to the first message in that chunk. A legacy server may return no
/// confirmation payload, and a background producer always returns an empty confirmation list.
///
/// A confirmation means the server committed the batch in memory. It does not mean the batch was
/// fsynced. After a crash and restart, a later batch can receive an offset that a client recorded
/// before the crash. Delivery is also at least once, so a retry can commit the same messages at
/// another offset.
///
/// # Retrying and what a failure means
///
/// A retry is the same request sent again unchanged, meaning a producer never rewrites, splits, or
/// reorders a batch to retry a send. [`send_retries()`] sets how many times it may try and how
/// later retries are paced. The default allows three retries and configures a one-second interval.
/// The first retry is immediate. Later retries wait for the next tick of an interval timer, so they
/// are at most one interval apart, and an attempt that outlasts the interval is followed by the next
/// one right away. Passing `None` as the interval retries back-to-back without any delay. The retry
/// policy applies to both direct and background producers.
///
/// A request can fail after the server has already appended it, so the first request of an
/// unconfirmed tail may already be in the partition. Sending the tail again can therefore write the
/// same messages twice, leaving the batch in the partition at multiple offsets. A consumer that
/// cannot accept duplicates must recognize them itself.
///
/// What is retried, and what is not:
///
/// | Situation | Retried | What the caller ends up with |
/// | --- | --- | --- |
/// | the client is not signed in, so nothing may be sent yet | yes | the send goes ahead as soon as the client is signed in, or fails with [`IggyError::CannotSendMessagesDueToClientDisconnection`] once the retry budget is spent |
/// | the request failed without an indication that it committed | yes, the identical request is sent again | the confirmation of the attempt that finally succeeds, or the last error once the retry budget is spent |
/// | the batch committed, but its confirmation could not be read ([`IggyError::InvalidBytesResponse`] or [`IggyError::InvalidJsonResponse`], raised on the HTTP transport only) | no | the write did happen and retrying would duplicate it on purpose |
/// | encrypting or partitioning the batch failed | no | that cause, with the whole batch returned as unconfirmed because nothing was sent, although earlier messages may already have been encrypted in place |
/// | [`send_retries()`] passed `None` or `0` | no | the outcome of the single attempt, which skips the sign-in check above and fails with the transport error when the client is disconnected |
///
/// To be precise: the budget is spent per request. Every chunk of a split is a request that gets its own retry budget.
/// Also, waiting for the client to sign in is counted separately from retrying the write itself.
///
/// Where a failure surfaces is the main difference between the two modes. Either way it names the
/// same three pieces: the `cause`, the unconfirmed tail, and confirmations returned for earlier
/// chunks.
///
/// - A **direct** send returns them to the caller as [`IggyError::ProducerSendFailed`]. `committed`
///   contains confirmations returned for earlier chunks, while `failed` is the unconfirmed tail.
///   Inspect `cause` before resending it. Encryption mutates messages before sending or
///   partitioning, so `failed` contains encrypted messages when an encryptor is configured. Passing
///   them back to the same producer would encrypt them again.
/// - A **background** send returned to its caller long before the write, so they go to
///   [`BackgroundConfig::error_callback`] as an
///   [`ErrorCtx`](crate::clients::producer_error_callback::ErrorCtx) instead, once no further
///   automatic retry will be attempted. The default callback logs the failure and drops the context.
///   Implement [`ErrorCallback`] when failed sends must be retained.
///
/// # Options and defaults
///
/// Everything is configured on the [`IggyProducerBuilder`] before [`build()`] and is fixed
/// afterwards.
///
/// | Option | Default | Controls |
/// | --- | --- | --- |
/// | [`stream()`], [`topic()`] | the values passed to [`IggyClient::producer()`] | where messages are appended |
/// | [`direct()`] / [`background()`] | [`direct()`] with the [`DirectConfig`] defaults, 1000 messages per request and no linger time | whether a send waits for the write |
/// | [`partitioning()`] | [`Partitioning::balanced()`] | which partition a batch lands in |
/// | [`partitioner()`] | none | computing the partition on the client instead |
/// | [`send_retries()`] | three retries, one-second interval after the immediate first retry | retrying a failed request |
/// | [`create_stream_if_not_exists()`] | on | creating the stream during [`init()`](Self::init) |
/// | [`create_topic_if_not_exists()`] | on, one partition, server defaults for expiry and max size | creating the topic during [`init()`](Self::init) |
/// | [`encryptor()`] | inherited from the client | encrypting payloads and user headers |
///
/// There are inverse setters as well, such as [`without_partitioning()`],
/// [`without_partitioner()`], [`without_encryptor()`], [`do_not_create_stream_if_not_exists()`] and
/// [`do_not_create_topic_if_not_exists()`].
///
/// # Encryption
///
/// When the [`IggyClient`] was created with an encryptor, a producer built from that client inherits
/// it and encrypts payloads and user headers before a batch leaves the producer. A consumer must use
/// a matching key to decrypt them. Producers and consumers built from the same client inherit the
/// same encryptor unless either builder overrides or clears it. An encryption failure fails the
/// whole send before any request leaves the producer. Encryption runs before a custom
/// [`partitioner()`], so that partitioner observes the encrypted payload and user headers.
///
/// # Concurrency
///
/// `IggyProducer` is `Send` and `Sync` but not `Clone`, and every send method takes `&self`. An
/// `Arc<IggyProducer>` can therefore be shared across tasks without further wrapping. A background
/// dispatcher routes those calls into worker queues and preserves order only within each worker.
///
/// Concurrent direct sends are independent requests, so nothing orders them against each other. The
/// chunk order described above holds within one call to [`send()`](Self::send) only.
///
/// # Shutting down
///
/// Call [`shutdown()`](Self::shutdown) when production is complete. It takes the producer by value,
/// drains a background producer's queues, flushes its remaining buffers, and waits for the worker
/// and error tasks. Dropping a background producer provides no completion guarantee and can lose
/// buffered messages. A direct producer has nothing buffered, so shutdown is a no-op.
///
/// [`IggyClient`]: crate::clients::client::IggyClient
/// [`IggyClient::producer()`]: crate::clients::client::IggyClient::producer
/// [`IggyProducerBuilder`]: crate::clients::producer_builder::IggyProducerBuilder
/// [`BackgroundConfig`]: crate::clients::producer_config::BackgroundConfig
/// [`BackgroundConfig::batch_length`]: crate::clients::producer_config::BackgroundConfig::batch_length
/// [`BackgroundConfig::batch_size`]: crate::clients::producer_config::BackgroundConfig::batch_size
/// [`BackgroundConfig::error_callback`]: crate::clients::producer_config::BackgroundConfig::error_callback
/// [`BackgroundConfig::failure_mode`]: crate::clients::producer_config::BackgroundConfig::failure_mode
/// [`BackgroundConfig::linger_time`]: crate::clients::producer_config::BackgroundConfig::linger_time
/// [`BackgroundConfig::max_buffer_size`]: crate::clients::producer_config::BackgroundConfig::max_buffer_size
/// [`BackgroundConfig::max_in_flight`]: crate::clients::producer_config::BackgroundConfig::max_in_flight
/// [`BackgroundConfig::num_shards`]: crate::clients::producer_config::BackgroundConfig::num_shards
/// [`BackgroundConfig::sharding`]: crate::clients::producer_config::BackgroundConfig::sharding
/// [`BalancedSharding`]: crate::clients::producer_sharding::BalancedSharding
/// [`ErrorCallback`]: crate::clients::producer_error_callback::ErrorCallback
/// [`OrderedSharding`]: crate::clients::producer_sharding::OrderedSharding
/// [`background()`]: crate::clients::producer_builder::IggyProducerBuilder::background
/// [`build()`]: crate::clients::producer_builder::IggyProducerBuilder::build
/// [`create_stream_if_not_exists()`]: crate::clients::producer_builder::IggyProducerBuilder::create_stream_if_not_exists
/// [`create_topic_if_not_exists()`]: crate::clients::producer_builder::IggyProducerBuilder::create_topic_if_not_exists
/// [`direct()`]: crate::clients::producer_builder::IggyProducerBuilder::direct
/// [`do_not_create_stream_if_not_exists()`]: crate::clients::producer_builder::IggyProducerBuilder::do_not_create_stream_if_not_exists
/// [`do_not_create_topic_if_not_exists()`]: crate::clients::producer_builder::IggyProducerBuilder::do_not_create_topic_if_not_exists
/// [`encryptor()`]: crate::clients::producer_builder::IggyProducerBuilder::encryptor
/// [`partitioner()`]: crate::clients::producer_builder::IggyProducerBuilder::partitioner
/// [`partitioning()`]: crate::clients::producer_builder::IggyProducerBuilder::partitioning
/// [`send_retries()`]: crate::clients::producer_builder::IggyProducerBuilder::send_retries
/// [`stream()`]: crate::clients::producer_builder::IggyProducerBuilder::stream
/// [`topic()`]: crate::clients::producer_builder::IggyProducerBuilder::topic
/// [`without_encryptor()`]: crate::clients::producer_builder::IggyProducerBuilder::without_encryptor
/// [`without_partitioner()`]: crate::clients::producer_builder::IggyProducerBuilder::without_partitioner
/// [`without_partitioning()`]: crate::clients::producer_builder::IggyProducerBuilder::without_partitioning
pub struct IggyProducer {
    core: Arc<ProducerCore>,
    dispatcher: Option<ProducerDispatcher>,
}

impl IggyProducer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
        stream: Identifier,
        stream_name: String,
        topic: Identifier,
        topic_name: String,
        partitioning: Option<Partitioning>,
        encryptor: Option<Arc<EncryptorKind>>,
        partitioner: Option<Arc<dyn Partitioner>>,
        create_stream_if_not_exists: bool,
        create_topic_if_not_exists: bool,
        topic_partitions_count: u32,
        topic_message_expiry: IggyExpiry,
        topic_max_size: MaxTopicSize,
        send_retries_count: Option<u32>,
        send_retries_interval: Option<NonZeroIggyDuration>,
        mode: SendMode,
    ) -> Self {
        let core = Arc::new(ProducerCore {
            initialized: AtomicBool::new(false),
            client: Arc::new(client),
            can_send: Arc::new(AtomicBool::new(true)),
            stream_id: Arc::new(stream),
            stream_name,
            topic_id: Arc::new(topic),
            topic_name,
            partitioning: partitioning.map(Arc::new),
            encryptor,
            partitioner,
            create_stream_if_not_exists,
            create_topic_if_not_exists,
            topic_partitions_count,
            topic_message_expiry,
            topic_max_size,
            default_partitioning: Arc::new(Partitioning::balanced()),
            last_sent_at: Arc::new(AtomicU64::new(0)),
            send_retries_count,
            send_retries_interval,
            direct_config: match mode {
                SendMode::Direct(ref cfg) => Some(cfg.clone()),
                _ => None,
            },
        });
        let dispatcher = match mode {
            SendMode::Background(cfg) => Some(ProducerDispatcher::new(core.clone(), cfg)),
            _ => None,
        };

        Self { core, dispatcher }
    }

    /// Returns the identifier of the stream this producer appends to.
    pub fn stream(&self) -> &Identifier {
        &self.core.stream_id
    }

    /// Returns the identifier of the topic this producer appends to.
    pub fn topic(&self) -> &Identifier {
        &self.core.topic_id
    }

    /// Initializes the producer and makes it ready to send messages.
    ///
    /// This must be called before the first send. Calling it again after successful initialization
    /// does nothing and returns immediately.
    ///
    /// Initialization ensures that:
    /// - The producer subscribes to client [`DiagnosticEvent`] values and tracks whether sending is
    ///   currently allowed. The gate starts open and follows the events observed after this call:
    ///   connect, disconnect, sign-out, and shutdown close it, and only a sign-in reopens it. It is
    ///   checked only when
    ///   [`send_retries()`](crate::clients::producer_builder::IggyProducerBuilder::send_retries)
    ///   allows at least one retry.
    /// - the stream exists, creating it when `create_stream_if_not_exists` is set (the default).
    /// - the topic exists, creating it when `create_topic_if_not_exists` is set (the default), with
    ///   the partitions count, message expiry and max size passed to
    ///   [`IggyProducerBuilder::create_topic_if_not_exists`](crate::clients::producer_builder::IggyProducerBuilder::create_topic_if_not_exists).
    ///   These are the only topic options controlled by producer initialization. All other settings
    ///   come from [`TopicCreateOptions::default`].
    ///
    /// # Errors
    ///
    /// - [`IggyError::StreamNameNotFound`] or [`IggyError::TopicNameNotFound`] when the stream or
    ///   the topic does not exist and its auto creation is disabled.
    /// - Any other error the server raised while looking up or creating the stream or the topic.
    pub async fn init(&self) -> Result<(), IggyError> {
        self.core.init().await
    }

    /// Sends `messages` to the stream and topic this producer was built for, with the partitioning it
    /// was built with.
    ///
    /// What a returned `Ok` tells you depends on the send mode:
    ///
    /// | | `direct` producer | `background` producer |
    /// | --- | --- | --- |
    /// | the call returns | once the server has answered every request the batch was split into | once the batch is queued on a worker |
    /// | `Ok` means | the server returned success for every request | the messages were accepted into a worker queue, nothing more |
    /// | confirmations | normally one per request, in order, but legacy servers may return none | always empty |
    /// | a write that fails | comes back as [`IggyError::ProducerSendFailed`] | goes to [`error_callback`] later |
    ///
    /// An empty `messages` vector is a no-op in both modes and returns an empty confirmation list.
    ///
    /// # Confirmations
    ///
    /// A [`SendMessagesConfirmationResponse`] names the partition a chunk of the batch landed in and
    /// the `base_offset` its first message was given.
    /// An offset is a position, not an identity. Delivery is at-least-once, so an earlier retry may
    /// have committed the same messages at a lower offset, see
    /// [Retrying and what a failure means](IggyProducer#retrying-and-what-a-failure-means).
    /// A confirmation reports an in-memory commit, not an fsync. A crash and restart can therefore
    /// lose an acknowledged batch and later reuse an offset the client already observed.
    ///
    /// # How long the call takes
    ///
    /// Both modes can wait:
    ///
    /// - A `direct` send first waits out whatever is left of [`DirectConfig::linger_time`] since the
    ///   previous send, then awaits its requests one after another, so it returns no earlier than the
    ///   last one is written.
    /// - A `background` send waits when the dispatcher has no room for the batch, which
    ///   [`failure_mode`] configures. The default [`BackpressureMode::Block`] waits for as long as it
    ///   takes. It can wait on the queue of the worker it was routed to as well, which holds 256
    ///   queued sends.
    ///
    /// # Errors
    ///
    /// A `direct` producer wraps a send failure in [`IggyError::ProducerSendFailed`]. The cause can be
    /// [`IggyError::CannotSendMessagesDueToClientDisconnection`] after the readiness retry budget is
    /// exhausted, an encryption or partitioning failure raised before anything left the producer,
    /// a server or transport error after write retries, or an unreadable confirmation for a request
    /// that may already have committed.
    ///
    /// A `background` producer only reports what queueing the batch ran into:
    /// [`IggyError::ProducerClosed`] after [`shutdown()`](Self::shutdown),
    /// [`IggyError::BackgroundSendBufferOverflow`] or [`IggyError::BackgroundSendTimeout`] under
    /// back pressure, and [`IggyError::BackgroundSendError`] when a worker is gone. A batch larger
    /// than [`max_buffer_size`] always fails with [`IggyError::BackgroundSendBufferOverflow`],
    /// however idle the producer is. Failures of the write itself reach [`error_callback`] instead,
    /// which drops the messages unless it is implemented to keep them.
    ///
    /// [`BackpressureMode::Block`]: crate::clients::producer_config::BackpressureMode::Block
    /// [`error_callback`]: crate::clients::producer_config::BackgroundConfig::error_callback
    /// [`failure_mode`]: crate::clients::producer_config::BackgroundConfig::failure_mode
    /// [`max_buffer_size`]: crate::clients::producer_config::BackgroundConfig::max_buffer_size
    pub async fn send(
        &self,
        messages: Vec<IggyMessage>,
    ) -> Result<SendMessagesResponse, IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(no_confirmations());
        }

        let stream_id = self.core.stream_id.clone();
        let topic_id = self.core.topic_id.clone();

        match &self.dispatcher {
            Some(disp) => disp
                .dispatch(messages, stream_id, topic_id, None)
                .await
                .map(|()| no_confirmations()),
            None => {
                self.core
                    .send_internal(&stream_id, &topic_id, messages, None)
                    .await
            }
        }
    }

    /// Sends one message.
    ///
    /// This has the same mode-dependent confirmation, backpressure, retry, and error semantics as
    /// [`IggyProducer::send`].
    pub async fn send_one(&self, message: IggyMessage) -> Result<SendMessagesResponse, IggyError> {
        self.send(vec![message]).await
    }

    /// Sends `messages` to the partition `partitioning` selects, overriding the partitioning this
    /// producer was built with for this call only. `None` falls back to that configured
    /// partitioning.
    ///
    /// A [`partitioner()`](crate::clients::producer_builder::IggyProducerBuilder::partitioner) still
    /// wins over the argument, since it computes the partition from the messages themselves.
    pub async fn send_with_partitioning(
        &self,
        messages: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<SendMessagesResponse, IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(no_confirmations());
        }

        let stream_id = self.core.stream_id.clone();
        let topic_id = self.core.topic_id.clone();

        match &self.dispatcher {
            Some(disp) => disp
                .dispatch(messages, stream_id, topic_id, partitioning)
                .await
                .map(|()| no_confirmations()),
            None => {
                self.core
                    .send_internal(&stream_id, &topic_id, messages, partitioning)
                    .await
            }
        }
    }

    /// Sends `messages` to any stream and topic, not only the pair this producer was built for.
    ///
    /// The target has to exist, since [`init()`](Self::init) only creates the producer's own stream
    /// and topic. Everything else stays in force: encryption, partitioning, retries, and for a
    /// `background` producer the routing to a worker [`Shard`].
    ///
    /// [`Shard`]: crate::clients::producer_sharding::Shard
    pub async fn send_to(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        messages: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<SendMessagesResponse, IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(no_confirmations());
        }

        match &self.dispatcher {
            Some(disp) => disp
                .dispatch(messages, stream, topic, partitioning)
                .await
                .map(|()| no_confirmations()),
            None => {
                self.core
                    .send_internal(&stream, &topic, messages, partitioning)
                    .await
            }
        }
    }

    /// Shuts the producer down.
    ///
    /// For a background producer, this drains the dispatcher queues, flushes the remaining shard
    /// buffers, waits for writes and error callbacks to finish, and then returns. Stop every sender
    /// first: a send racing the shutdown can be queued after the drain and is lost without an error.
    /// Dropping a background producer instead provides no such guarantee and can lose buffered
    /// messages.
    ///
    /// A direct producer has nothing buffered, so calling `shutdown()` is a no-op.
    pub async fn shutdown(self) {
        if let Some(dispatcher) = self.dispatcher {
            dispatcher.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::implies_committed_send;
    use iggy_common::IggyError;

    #[test]
    fn test_unreadable_confirmation_of_a_committed_batch_stops_retrying() {
        assert!(implies_committed_send(&IggyError::InvalidBytesResponse));
        assert!(implies_committed_send(&IggyError::InvalidJsonResponse));
    }

    #[test]
    fn test_errors_reachable_before_a_commit_keep_retrying() {
        for error in [
            IggyError::Disconnected,
            IggyError::EmptyResponse,
            IggyError::Unauthenticated,
            IggyError::Unauthorized,
            IggyError::CannotSendMessagesDueToClientDisconnection,
            IggyError::HttpResponseError(500, String::new()),
            IggyError::ResourceNotFound(String::new()),
        ] {
            assert!(
                !implies_committed_send(&error),
                "{error} does not prove a commit, so it must stay retryable"
            );
        }
    }
}
