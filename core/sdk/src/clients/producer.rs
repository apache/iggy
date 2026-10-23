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
use crate::driver::operation_deadline;
use bytes::Bytes;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{BinarySendPartitioning, Client, StreamClient, TopicClient, TopicCreateOptions};
use iggy_common::{
    Durability, EncryptorKind, IdKind, Identifier, IggyError, IggyExpiry, IggyMessage,
    IggyTimestamp, MaxTopicSize, Partitioner, Partitioning, SendMessagesConfirmationResponse,
    SendMessagesResponse, with_binary_lineage,
};
use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::time::{sleep, timeout_at};
use tracing::{error, info, trace};

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

pub struct ProducerCore {
    initialized: AtomicBool,
    client: IggyRwLock<ClientWrapper>,
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
    topic_durability: Durability,
    default_partitioning: Arc<Partitioning>,
    last_sent_at: Arc<AtomicU64>,
    uncertain_operations: Mutex<HashSet<u64>>,
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
        let client = self.client.read().await;
        with_binary_lineage(client.operation_lineage().await, async {
            if client.get_stream(&stream_id).await?.is_none() {
                let name = match stream_id.kind {
                    IdKind::String => stream_id.get_string_value()?,
                    IdKind::Numeric => {
                        error!(
                            "Stream: {stream_id} does not exist and cannot be created from a numeric identifier."
                        );
                        return Err(IggyError::StreamIdNotFound(Identifier::from_identifier(
                            &stream_id,
                        )));
                    }
                };

                if !self.create_stream_if_not_exists {
                    error!("Stream does not exist and auto-creation is disabled.");
                    return Err(IggyError::StreamNameNotFound(name));
                }

                info!("Creating stream: {name}");
                client.create_stream(&name).await?;
            }

            if client.get_topic(&stream_id, &topic_id).await?.is_none() {
                let name = match topic_id.kind {
                    IdKind::String => topic_id.get_string_value()?,
                    IdKind::Numeric => {
                        error!(
                            "Topic: {topic_id} does not exist and cannot be created from a numeric identifier."
                        );
                        return Err(IggyError::TopicIdNotFound(
                            Identifier::from_identifier(&topic_id),
                            Identifier::from_identifier(&stream_id),
                        ));
                    }
                };

                if !self.create_topic_if_not_exists {
                    error!("Topic does not exist and auto-creation is disabled.");
                    return Err(IggyError::TopicNameNotFound(name, stream_id.to_string()));
                }

                info!("Creating topic: {name} for stream: {stream_id}");
                client
                    .create_topic(
                        &stream_id,
                        &name,
                        &TopicCreateOptions {
                            partitions_count: Some(self.topic_partitions_count),
                            message_expiry: (self.topic_message_expiry != IggyExpiry::ServerDefault)
                                .then_some(self.topic_message_expiry),
                            max_topic_size: (self.topic_max_size != MaxTopicSize::ServerDefault)
                                .then_some(self.topic_max_size),
                            durability: self.topic_durability,
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
        })
        .await
    }

    async fn try_send_messages(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        partitioning: &Arc<Partitioning>,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        super::with_client!(self, client => {
            let deadline = operation_deadline();
            timeout_at(deadline, self.prune_uncertain_operations(&client))
                .await
                .map_err(|_| IggyError::TransientNotAccepted)?;
            let mut partitioning = BinarySendPartitioning::new(partitioning);
            let mut result = client
                .send_messages_with_partitioning(stream, topic, &mut partitioning, messages)
                .await;
            let blocker = match &result {
                Err(IggyError::LaneBlocked(op)) if self.uncertain_operations().contains(op) => {
                    Some(*op)
                }
                _ => None,
            };
            if let Some(op) = blocker {
                if matches!(
                    client.resolve_operation(op, deadline.into_std()).await,
                    Err(IggyError::OperationUncertain(_))
                ) {
                    return Err(IggyError::LaneBlocked(op));
                }
                // This call's chunk has not been sent. The earlier chunk is
                // resolved separately and is never submitted as a fresh request.
                result = client
                    .send_messages_with_partitioning(stream, topic, &mut partitioning, messages)
                    .await;
            }
            if let Err(IggyError::OperationUncertain(op)) = &result {
                self.uncertain_operations().insert(*op);
            }
            result
        })
    }

    fn uncertain_operations(&self) -> MutexGuard<'_, HashSet<u64>> {
        self.uncertain_operations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn prune_uncertain_operations(&self, client: &ClientWrapper) {
        let operations: Vec<_> = self.uncertain_operations().iter().copied().collect();
        for op in operations {
            if !client.retains_operation(op).await {
                self.uncertain_operations().remove(&op);
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

    /// `sent` is the number of messages at the head of `failed` that the failed request carried.
    fn make_failed_error(
        &self,
        cause: IggyError,
        failed: Vec<IggyMessage>,
        sent: usize,
        committed: Vec<SendMessagesConfirmationResponse>,
    ) -> IggyError {
        let uncertain = if matches!(
            cause,
            IggyError::OperationUncertain(_) | IggyError::TransientNotCommitted
        ) {
            sent
        } else {
            0
        };
        IggyError::ProducerSendFailed {
            cause: Box::new(cause),
            failed: Arc::new(failed),
            uncertain,
            committed: Arc::new(committed),
            stream_name: self.stream_name.as_str().into(),
            topic_name: self.topic_name.as_str().into(),
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
        let lineage = self.client.read().await.operation_lineage().await;
        with_binary_lineage(lineage, async {
            if msgs.is_empty() {
                return Ok(no_confirmations());
            }

            if let Err(err) = self.encrypt_messages(&mut msgs) {
                return Err(self.make_failed_error(err, msgs, 0, Vec::new()));
            }

            let part = match self.get_partitioning(stream, topic, &msgs, partitioning.clone()) {
                Ok(p) => p,
                Err(err) => {
                    return Err(self.make_failed_error(err, msgs, 0, Vec::new()));
                }
            };

            match &self.direct_config {
                Some(cfg) => {
                    let linger_time_micros = cfg.linger_time.as_micros();
                    if linger_time_micros > 0 {
                        Self::wait_before_sending(
                            linger_time_micros,
                            self.last_sent_at.load(ORDERING),
                        )
                        .await;
                    }

                    let max = NonZeroU32::new(cfg.batch_length)
                        .unwrap_or(MAX_BATCH_LENGTH)
                        .get() as usize;
                    let mut index = 0;
                    let mut confirmations = Vec::with_capacity(msgs.len().div_ceil(max));
                    while index < msgs.len() {
                        let end = (index + max).min(msgs.len());
                        let chunk = &mut msgs[index..end];

                        match self.try_send_messages(stream, topic, &part, chunk).await {
                            Ok(response) => confirmations.extend(response.confirmations),
                            Err(err) => {
                                let failed_tail = msgs.split_off(index);
                                return Err(self.make_failed_error(
                                    err,
                                    failed_tail,
                                    end - index,
                                    confirmations,
                                ));
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
                        .map_err(|err| {
                            let sent = msgs.len();
                            self.make_failed_error(err, msgs, sent, Vec::new())
                        })?;
                    self.last_sent_at
                        .store(IggyTimestamp::now().into(), ORDERING);
                    Ok(response)
                }
            }
        })
        .await
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
/// let producer = client.producer("my-stream", "my-topic")?
///     .topic_durability(Durability::Persisted)
///     .build();
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
///     .topic_durability(Durability::Persisted)
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
///     .topic_durability(Durability::Persisted)
///     // Every message of this producer goes to the partition the server derives from the key.
///     .partitioning(Partitioning::messages_key_str("my-key")?)
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
/// sends, [`BackgroundConfig::batch_size`] bytes, or [`BackgroundConfig::linger_time`] since the
/// first of them was buffered. Adjacent buffered sends that share a stream, topic, and partitioning
/// are merged into one request.
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
/// | [`Partitioning::balanced()`], the default | not guaranteed with multiple partitions | the binary SDK or HTTP server chooses a partition per request, so consecutive sends and chunks may land in different logs |
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
/// Application resends use new identities and can append the same messages twice, see
/// [Retrying and what a failure means](#retrying-and-what-a-failure-means).
///
/// # Confirmations
///
/// A successful direct send returns [`SendMessagesResponse`], normally holding one
/// [`SendMessagesConfirmationResponse`] per chunk. Each confirmation records a partition and the
/// `base_offset` assigned to the first message in that chunk. The list can be empty when the
/// server supplies no offsets, and a background producer always returns an empty list.
///
/// Confirmed sends follow the topic's durability policy. Persisted durability also
/// provides recoverable storage and crash-safe retry receipts on a quorum.
///
/// # Retrying and what a failure means
///
/// Each binary chunk uses one operation deadline. Transport retries preserve its
/// request identity, body and partition context. The producer does not create a
/// new identity to retry a chunk that might already have committed.
///
/// A chunk returned with [`IggyError::OperationUncertain`] stays retained by the
/// client and blocks its partition lane. A later producer call blocked by one
/// of this producer's uncertain chunks resolves that earlier operation first,
/// then sends the new call's chunk within the same deadline. A blocker owned by
/// another producer remains [`IggyError::LaneBlocked`] for the caller to handle.
/// An [`IggyConsumer`](crate::clients::consumer::IggyConsumer) on the same client
/// resolves any blocker, including an uncertain chunk owned by this producer.
///
/// `OperationUncertain(op)` identifies the first chunk of `failed`, its first
/// `uncertain` messages; it may already have committed. `LaneBlocked(op)` means
/// none of `failed` was sent. HTTP retains no operation, so its unknown outcome is
/// [`IggyError::TransientNotCommitted`]: the first `uncertain` messages of `failed`
/// may already have committed, and only a read of the partition tells.
/// Resolving the blocker never resubmits the earlier failed chunk as a new write.
/// [`Client::resolve_operation`] tells the caller that got `OperationUncertain(op)`
/// the outcome of that chunk, also after a later producer or consumer call
/// resolved `op` first. That call only frees the lane, and the client keeps the
/// proved outcome for its owner. A chunk carries no committed rejection, so
/// `Ok(())` is its only proved outcome, and then only the messages after the
/// first `uncertain` still need a send.
/// A session reset or explicit abandonment can destroy the retained result
/// before a replay proves it.
/// [`IggyError::OperationNotRetained`] does not prove the earlier chunk failed,
/// so resubmitting its payload as a new call can duplicate messages.
///
/// A committed chunk whose confirmation cannot be decoded succeeds with no
/// offset information. Missing confirmation or an aged-out request does not
/// prove that no messages were appended.
///
/// HTTP has no retained-operation recovery. Its configured transport retries
/// may resend a failed POST under a new identity. An ambiguous HTTP failure can
/// therefore leave duplicate messages; the producer adds no further retry.
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
/// | [`topic_durability()`](crate::clients::producer_builder::IggyProducerBuilder::topic_durability) | `Durability::Replicated` | crash-safe retry receipts require `Durability::Persisted` |
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
        topic_durability: Durability,
        mode: SendMode,
    ) -> Self {
        let core = Arc::new(ProducerCore {
            initialized: AtomicBool::new(false),
            client,
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
            topic_durability,
            default_partitioning: Arc::new(Partitioning::balanced()),
            last_sent_at: Arc::new(AtomicU64::new(0)),
            uncertain_operations: Mutex::default(),
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
    /// - the stream exists, creating it when `create_stream_if_not_exists` is set (the default).
    /// - the topic exists, creating it when `create_topic_if_not_exists` is set (the default), with
    ///   the partitions count, message expiry and max size passed to
    ///   [`IggyProducerBuilder::create_topic_if_not_exists`](crate::clients::producer_builder::IggyProducerBuilder::create_topic_if_not_exists).
    ///   Message durability comes from
    ///   [`IggyProducerBuilder::topic_durability`](crate::clients::producer_builder::IggyProducerBuilder::topic_durability).
    ///   `Durability::Persisted` also provides crash-safe retry receipts. Other settings come from
    ///   [`TopicCreateOptions::default`].
    ///
    /// # Errors
    ///
    /// - [`IggyError::StreamNameNotFound`], [`IggyError::StreamIdNotFound`] or [`IggyError::TopicNameNotFound`],
    ///   [`IggyError::TopicIdNotFound`] when the stream or the topic still does not exist once the retries are
    ///   exhausted.
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
    /// A binary send keeps the caller sign-in it started with across every chunk. Background
    /// sends capture it before waiting for buffer capacity. A later caller sign-in, sign-out, or
    /// disconnect fails unsent work with [`IggyError::Unauthenticated`], through the error callback
    /// for queued batches. Automatic session recovery keeps that work eligible to send. A request
    /// that may already have applied keeps its uncertain outcome.
    ///
    /// # Confirmations
    ///
    /// A [`SendMessagesConfirmationResponse`] names the partition a chunk of the batch landed in and
    /// the `base_offset` its first message was given.
    /// An offset is a position, not an identity. An application resend may
    /// commit the same messages at another offset, see
    /// [Retrying and what a failure means](IggyProducer#retrying-and-what-a-failure-means).
    /// Confirmed sends follow the topic's durability policy; Persisted also waits
    /// for recoverable storage on a quorum.
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
    /// an encryption or partitioning failure before sending, a server rejection,
    /// or [`IggyError::OperationUncertain`] when a binary chunk may have committed.
    /// [`IggyError::LaneBlocked`] identifies an earlier retained operation and
    /// means this call's failed chunk has not been sent.
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
        self.send_with_partitioning(messages, None).await
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
        self.send_to(
            self.core.stream_id.clone(),
            self.core.topic_id.clone(),
            messages,
            partitioning,
        )
        .await
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

        let lineage = self.core.client.read().await.operation_lineage().await;
        with_binary_lineage(lineage, async {
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
        })
        .await
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
    use super::IggyProducer;
    use crate::client_wrappers::client_wrapper::ClientWrapper;
    use crate::clients::producer_builder::IggyProducerBuilder;
    use crate::clients::producer_config::{BackgroundConfig, DirectConfig};
    use crate::clients::producer_error_callback::{ErrorCallback, ErrorCtx};
    use crate::clients::producer_sharding::ShardMessage;
    use crate::driver::with_deadline;
    use crate::http::http_client::HttpClient;
    use crate::links::OPERATION_TIMEOUT;
    use crate::links::fake_cluster::{
        Answer, FakeCluster, connected, endpoint, eventually, topic_body,
    };
    use crate::tcp::tcp_client::TcpClient;
    use bytes::Bytes;
    use iggy_binary_protocol::codes::{
        CREATE_STREAM_CODE, GET_STREAM_CODE, GET_TOPIC_CODE, LOGIN_REGISTER_CODE,
        SEND_MESSAGES_CODE,
    };
    use iggy_binary_protocol::requests::messages::SendMessagesHeader;
    use iggy_binary_protocol::responses::messages::{
        PartitionRouteResponse, SendMessagesConfirmationResponse,
        SendMessagesResponse as WireSendMessagesResponse,
    };
    use iggy_binary_protocol::responses::streams::StreamResponse;
    use iggy_binary_protocol::{WireDecode, WireEncode, WireName, WireOptions, WirePartitioning};
    use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
    use iggy_common::{
        Client, HttpClientConfig, Identifier, IggyDuration, IggyError, IggyMessage, Partitioning,
        Sizeable, UserClient,
    };
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::Instant;

    async fn connected_producer() -> (Arc<FakeCluster>, IggyProducer) {
        let cluster = FakeCluster::new(1);
        *cluster.route.lock().unwrap() = Some(PartitionRouteResponse {
            incarnation: 7,
            metadata_op: 11,
            view: 0,
            partitions_count: 1,
            primary_replica: 0,
        });
        let client = TcpClient::from_links(connected(&cluster, 1).await);
        let producer = producer_on(IggyRwLock::new(ClientWrapper::Tcp(client)));
        (cluster, producer)
    }

    fn producer_on(client: IggyRwLock<ClientWrapper>) -> IggyProducer {
        producer_batching(client, 1)
    }

    fn producer_batching(client: IggyRwLock<ClientWrapper>, batch_length: u32) -> IggyProducer {
        IggyProducerBuilder::new(
            client,
            Identifier::numeric(1).unwrap(),
            "stream".to_owned(),
            Identifier::numeric(1).unwrap(),
            "topic".to_owned(),
            None,
            None,
        )
        .partitioning(Partitioning::partition_id(0))
        .direct(DirectConfig::builder().batch_length(batch_length).build())
        .build()
    }

    fn message(payload: &'static [u8]) -> IggyMessage {
        IggyMessage::builder()
            .payload(Bytes::from_static(payload))
            .build()
            .unwrap()
    }

    fn confirmation(base_offset: u64) -> Answer {
        confirmation_on(0, base_offset)
    }

    fn confirmation_on(partition_id: u32, base_offset: u64) -> Answer {
        let body = WireSendMessagesResponse {
            confirmations: vec![SendMessagesConfirmationResponse {
                stream_id: 1,
                topic_id: 1,
                partition_id,
                base_offset,
            }],
        }
        .to_bytes()
        .to_vec();
        Answer::CommitBody(base_offset + 1, body)
    }

    /// Resolves `op` the way an application does, through the producer's client.
    async fn resolve(producer: &IggyProducer, op: u64) -> Result<(), IggyError> {
        producer
            .core
            .client
            .read()
            .await
            .resolve_operation(op, (Instant::now() + OPERATION_TIMEOUT).into_std())
            .await
    }

    #[derive(Debug)]
    struct FailedBatches(flume::Sender<ErrorCtx>);

    impl ErrorCallback for FailedBatches {
        fn call(&self, ctx: ErrorCtx) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
            let sender = self.0.clone();
            Box::pin(async move {
                sender.send(ctx).unwrap();
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_confirmed_chunk_when_signing_in_should_end_the_unsent_tail_lineage() {
        let (cluster, producer) = connected_producer().await;
        let producer = Arc::new(producer);
        cluster.script(
            &endpoint(0),
            SEND_MESSAGES_CODE,
            [confirmation(0), confirmation(1)],
        );
        cluster.hold(SEND_MESSAGES_CODE);
        let pending = tokio::spawn({
            let producer = Arc::clone(&producer);
            async move {
                producer
                    .send(vec![message(b"first"), message(b"tail")])
                    .await
            }
        });
        eventually("the first chunk arrives", || {
            cluster.count(SEND_MESSAGES_CODE) == 1
        })
        .await;
        producer
            .core
            .client
            .read()
            .await
            .login_user("iggy", "iggy")
            .await
            .unwrap();
        cluster.release(SEND_MESSAGES_CODE);

        let Err(IggyError::ProducerSendFailed {
            cause,
            failed,
            uncertain,
            committed,
            ..
        }) = pending.await.unwrap()
        else {
            panic!("the old operation must return its unsent tail");
        };
        assert_eq!(*cause, IggyError::Unauthenticated);
        assert_eq!(uncertain, 0);
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].payload, Bytes::from_static(b"tail"));
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].base_offset, 0);
        assert_eq!(cluster.count(SEND_MESSAGES_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_initialization_when_signing_in_should_end_the_creation_lineage() {
        let (cluster, mut producer) = connected_producer().await;
        Arc::get_mut(&mut producer.core).unwrap().stream_id =
            Arc::new(Identifier::named("stream").unwrap());
        cluster.script(
            &endpoint(0),
            CREATE_STREAM_CODE,
            [Answer::CommitBody(
                1,
                StreamResponse {
                    id: 1,
                    created_at: 0,
                    topics_count: 0,
                    size_bytes: 0,
                    messages_count: 0,
                    name: WireName::new("stream").unwrap(),
                    options: WireOptions::empty(),
                }
                .to_bytes()
                .to_vec(),
            )],
        );
        cluster.script(&endpoint(0), GET_TOPIC_CODE, [Answer::Body(topic_body(1))]);
        cluster.hold(GET_STREAM_CODE);
        let producer = Arc::new(producer);
        let pending = tokio::spawn({
            let producer = Arc::clone(&producer);
            async move { producer.init().await }
        });
        eventually("the stream lookup arrives", || {
            cluster.count(GET_STREAM_CODE) == 1
        })
        .await;
        producer
            .core
            .client
            .read()
            .await
            .login_user("iggy", "iggy")
            .await
            .unwrap();
        cluster.release(GET_STREAM_CODE);

        assert_eq!(
            pending.await.unwrap().unwrap_err(),
            IggyError::Unauthenticated
        );
        assert_eq!(cluster.count(CREATE_STREAM_CODE), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_queued_batches_when_signing_in_should_never_merge_different_lineages() {
        let (cluster, mut producer) = connected_producer().await;
        Arc::get_mut(&mut producer.core).unwrap().direct_config = None;
        let (failures, received) = flume::unbounded();
        producer.dispatcher = Some(super::ProducerDispatcher::new(
            Arc::clone(&producer.core),
            BackgroundConfig::builder()
                .batch_length(2)
                .batch_size(0)
                .linger_time(IggyDuration::new(Duration::from_secs(60)))
                .error_callback(Arc::new(Box::new(FailedBatches(failures))))
                .build(),
        ));
        cluster.script(&endpoint(0), SEND_MESSAGES_CODE, [confirmation(0)]);
        producer.send(vec![message(b"old")]).await.unwrap();
        producer
            .core
            .client
            .read()
            .await
            .login_user("iggy", "iggy")
            .await
            .unwrap();
        producer.send(vec![message(b"new")]).await.unwrap();
        producer.shutdown().await;

        let failure = received.try_recv().expect("the old queued batch must fail");
        assert_eq!(*failure.cause, IggyError::Unauthenticated);
        assert_eq!(failure.messages.len(), 1);
        assert_eq!(failure.messages[0].payload, Bytes::from_static(b"old"));
        assert!(received.try_recv().is_err());
        let sends = cluster.received(SEND_MESSAGES_CODE);
        assert_eq!(sends.len(), 1);
        let (size, body) = sends[0].body.split_first_chunk().unwrap();
        assert_eq!(
            SendMessagesHeader::decode_from(&body[..u32::from_le_bytes(*size) as usize])
                .unwrap()
                .messages_count,
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_backpressure_when_signing_in_should_keep_the_waiting_batch_lineage() {
        let (cluster, mut producer) = connected_producer().await;
        Arc::get_mut(&mut producer.core).unwrap().direct_config = None;
        let batch = ShardMessage {
            stream: Arc::clone(&producer.core.stream_id),
            topic: Arc::clone(&producer.core.topic_id),
            messages: vec![message(b"old")],
            partitioning: None,
        };
        let (failures, received) = flume::unbounded();
        producer.dispatcher = Some(super::ProducerDispatcher::new(
            Arc::clone(&producer.core),
            BackgroundConfig::builder()
                .batch_length(1)
                .max_buffer_size(batch.get_size_bytes())
                .error_callback(Arc::new(Box::new(FailedBatches(failures))))
                .build(),
        ));
        cluster.script(
            &endpoint(0),
            SEND_MESSAGES_CODE,
            [confirmation(0), confirmation(1)],
        );
        cluster.hold(SEND_MESSAGES_CODE);
        producer.send(vec![message(b"old")]).await.unwrap();
        eventually("the charged batch arrives", || {
            cluster.count(SEND_MESSAGES_CODE) == 1
        })
        .await;
        let waiting = producer.send(vec![message(b"old")]);
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        producer
            .core
            .client
            .read()
            .await
            .login_user("iggy", "iggy")
            .await
            .unwrap();
        cluster.release(SEND_MESSAGES_CODE);
        waiting.await.unwrap();
        let failure = tokio::time::timeout(
            crate::links::fake_cluster::TEST_BUDGET,
            received.recv_async(),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(*failure.cause, IggyError::Unauthenticated);
        assert_eq!(failure.messages.len(), 1);
        assert_eq!(cluster.count(SEND_MESSAGES_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_queued_batches_when_recovering_should_preserve_their_lineage() {
        let (cluster, mut producer) = connected_producer().await;
        Arc::get_mut(&mut producer.core).unwrap().direct_config = None;
        let (failures, received) = flume::unbounded();
        producer.dispatcher = Some(super::ProducerDispatcher::new(
            Arc::clone(&producer.core),
            BackgroundConfig::builder()
                .batch_length(2)
                .batch_size(0)
                .linger_time(IggyDuration::new(crate::links::fake_cluster::TEST_BUDGET))
                .error_callback(Arc::new(Box::new(FailedBatches(failures))))
                .build(),
        ));
        producer.send(vec![message(b"old")]).await.unwrap();
        *cluster.session.lock().unwrap() = None;
        cluster.drop_connections(&endpoint(0));
        eventually("automatic sign-in restores the session", || {
            cluster.count(LOGIN_REGISTER_CODE) == 2
        })
        .await;
        producer.send(vec![message(b"new")]).await.unwrap();
        producer.shutdown().await;

        assert!(received.try_recv().is_err());
        let sends = cluster.received(SEND_MESSAGES_CODE);
        assert_eq!(sends.len(), 1);
        let (size, body) = sends[0].body.split_first_chunk().unwrap();
        assert_eq!(
            SendMessagesHeader::decode_from(&body[..u32::from_le_bytes(*size) as usize])
                .unwrap()
                .messages_count,
            2
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_unanswered_background_batch_when_signing_in_should_keep_its_lineage_uncertain()
     {
        let (cluster, mut producer) = connected_producer().await;
        Arc::get_mut(&mut producer.core).unwrap().direct_config = None;
        let (failures, received) = flume::unbounded();
        producer.dispatcher = Some(super::ProducerDispatcher::new(
            Arc::clone(&producer.core),
            BackgroundConfig::builder()
                .batch_length(1)
                .error_callback(Arc::new(Box::new(FailedBatches(failures))))
                .build(),
        ));
        cluster.script(&endpoint(0), SEND_MESSAGES_CODE, [Answer::Silence]);
        producer.send(vec![message(b"old")]).await.unwrap();
        eventually("the batch may have applied", || {
            cluster.count(SEND_MESSAGES_CODE) == 1
        })
        .await;
        producer
            .core
            .client
            .read()
            .await
            .login_user("iggy", "iggy")
            .await
            .unwrap();
        producer.shutdown().await;

        let failure = received.try_recv().unwrap();
        assert!(
            matches!(*failure.cause, IggyError::OperationUncertain(_)),
            "{failure:?}"
        );
        assert_eq!(failure.messages.len(), 1);
        assert_eq!(cluster.count(SEND_MESSAGES_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_missing_chosen_partition_when_sending_should_refresh_and_reselect_once() {
        for succeeds in [true, false] {
            let (cluster, producer) = connected_producer().await;
            cluster
                .route
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .partitions_count = 2;
            cluster.script(
                &endpoint(0),
                GET_TOPIC_CODE,
                [Answer::Body(topic_body(1)), Answer::Body(topic_body(2))],
            );
            let missing = IggyError::PartitionNotFound(
                0,
                Identifier::numeric(1).unwrap(),
                Identifier::numeric(1).unwrap(),
            );
            cluster.script(
                &endpoint(0),
                SEND_MESSAGES_CODE,
                [
                    Answer::Refuse(missing.clone()),
                    if succeeds {
                        confirmation_on(1, 0)
                    } else {
                        Answer::Refuse(missing)
                    },
                ],
            );

            let result = producer
                .send_with_partitioning(
                    vec![message(b"new")],
                    Some(Arc::new(Partitioning::balanced())),
                )
                .await;

            assert_eq!(result.is_ok(), succeeds, "{result:?}");
            if let Err(IggyError::ProducerSendFailed { cause, .. }) = result {
                assert!(matches!(*cause, IggyError::PartitionNotFound(..)));
            }
            assert_eq!(cluster.count(GET_TOPIC_CODE), 2);
            let sends = cluster.received(SEND_MESSAGES_CODE);
            assert_eq!(sends.len(), 2);
            assert_eq!(
                sent_partition(&sends[0].body),
                WirePartitioning::PartitionId(0)
            );
            assert_eq!(
                sent_partition(&sends[1].body),
                WirePartitioning::PartitionId(1)
            );
            assert_ne!(sends[0].header.request, sends[1].header.request);
        }
    }

    fn sent_partition(body: &[u8]) -> WirePartitioning {
        let (metadata_length, body) = body.split_first_chunk().unwrap();
        SendMessagesHeader::decode_from(&body[..u32::from_le_bytes(*metadata_length) as usize])
            .unwrap()
            .partitioning
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_balanced_send_blocked_by_its_producer_when_resolving_should_keep_the_chosen_partition()
     {
        let (cluster, producer) = connected_producer().await;
        cluster
            .route
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .partitions_count = 2;
        cluster.script(
            &endpoint(0),
            SEND_MESSAGES_CODE,
            [
                Answer::Silence,
                confirmation(0),
                confirmation(1),
                confirmation_on(1, 0),
            ],
        );
        assert!(producer.send(vec![message(b"old")]).await.is_err());
        cluster.script(&endpoint(0), GET_TOPIC_CODE, [Answer::Body(topic_body(2))]);

        for payload in [b"new".as_slice(), b"next".as_slice()] {
            producer
                .send_with_partitioning(
                    vec![message(payload)],
                    Some(Arc::new(Partitioning::balanced())),
                )
                .await
                .unwrap();
        }

        let sends = cluster.received(SEND_MESSAGES_CODE);
        let partitions: Vec<_> = sends
            .iter()
            .map(|sent| sent_partition(&sent.body))
            .collect();
        assert_eq!(
            partitions,
            [
                WirePartitioning::PartitionId(0),
                WirePartitioning::PartitionId(0),
                WirePartitioning::PartitionId(0),
                WirePartitioning::PartitionId(1)
            ]
        );
        assert_eq!(sends[0].header.request, sends[1].header.request);
        assert_ne!(sends[1].header.request, sends[2].header.request);
    }

    #[tokio::test(start_paused = true)]
    async fn test_aged_out_request_stops_producer_retries_without_minting_another_request() {
        let (cluster, producer) = connected_producer().await;
        cluster.script(
            &endpoint(0),
            SEND_MESSAGES_CODE,
            [Answer::Refuse(IggyError::RequestTooOld)],
        );

        let Err(IggyError::ProducerSendFailed {
            cause,
            failed,
            uncertain,
            ..
        }) = producer.send(vec![message(b"old")]).await
        else {
            panic!("the producer must return the unconfirmed chunk");
        };

        assert_eq!(*cause, IggyError::RequestTooOld);
        assert_eq!(failed.len(), 1);
        assert_eq!(uncertain, 0);
        assert_eq!(cluster.count(SEND_MESSAGES_CODE), 1);
    }

    /// HTTP reports a send that may have committed as `TransientNotCommitted`.
    #[tokio::test(start_paused = true)]
    async fn given_a_chunk_with_an_unknown_outcome_when_it_fails_should_count_it_as_uncertain() {
        let (_cluster, producer) = connected_producer().await;

        let IggyError::ProducerSendFailed { uncertain, .. } = producer.core.make_failed_error(
            IggyError::TransientNotCommitted,
            vec![message(b"sent"), message(b"sent"), message(b"unsent")],
            2,
            Vec::new(),
        ) else {
            panic!("the producer must return the failed messages");
        };

        assert_eq!(uncertain, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_uncertain_tail_chunk_when_a_send_fails_should_count_only_that_chunk_as_uncertain()
     {
        const BATCH_LENGTH: u32 = 3;
        let (cluster, producer) = connected_producer().await;
        let producer = producer_batching(producer.core.client.clone(), BATCH_LENGTH);
        cluster.script(
            &endpoint(0),
            SEND_MESSAGES_CODE,
            [confirmation(0), Answer::Silence],
        );

        let Err(IggyError::ProducerSendFailed {
            cause,
            failed,
            uncertain,
            committed,
            stream_name,
            topic_name,
        }) = producer
            .send(vec![
                message(b"1"),
                message(b"2"),
                message(b"3"),
                message(b"4"),
                message(b"5"),
            ])
            .await
        else {
            panic!("the producer must return the uncertain tail chunk");
        };

        assert!(
            matches!(*cause, IggyError::OperationUncertain(_)),
            "{cause:?}"
        );
        assert_eq!(failed.len(), 2);
        assert_eq!(uncertain, 2);
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].base_offset, 0);
        assert_eq!(stream_name.as_ref(), "stream");
        assert_eq!(topic_name.as_ref(), "topic");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_partial_send_when_a_new_call_recovers_should_replay_only_the_uncertain_chunk()
    {
        let (cluster, producer) = connected_producer().await;
        cluster.script(
            &endpoint(0),
            SEND_MESSAGES_CODE,
            [
                confirmation(0),
                Answer::Silence,
                confirmation(1),
                confirmation(2),
            ],
        );
        let Err(IggyError::ProducerSendFailed {
            cause,
            failed,
            uncertain,
            committed,
            ..
        }) = producer
            .send(vec![
                message(b"first"),
                message(b"uncertain"),
                message(b"unsent"),
            ])
            .await
        else {
            panic!("the producer must preserve the failed tail and prior confirmation");
        };
        let op = cluster.received(SEND_MESSAGES_CODE)[1].header.request;
        assert_eq!(*cause, IggyError::OperationUncertain(op));
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].base_offset, 0);
        assert_eq!(failed.len(), 2);
        assert_eq!(uncertain, 1);
        assert_eq!(failed[0].payload, Bytes::from_static(b"uncertain"));
        assert_eq!(failed[1].payload, Bytes::from_static(b"unsent"));

        let response = producer.send(vec![message(b"later")]).await.unwrap();

        assert_eq!(resolve(&producer, op).await, Ok(()));
        let sends = cluster.received(SEND_MESSAGES_CODE);
        assert_eq!(sends.len(), 4);
        assert_eq!(sends[1].header.request, sends[2].header.request);
        assert_eq!(sends[1].body, sends[2].body);
        assert_ne!(sends[2].header.request, sends[3].header.request);
        assert_ne!(sends[2].body, sends[3].body);
        assert_eq!(response.confirmations.len(), 1);
        assert_eq!(response.confirmations[0].base_offset, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_new_call_and_the_owner_resolving_one_chunk_when_they_race_should_replay_once_and_share_the_outcome()
     {
        let (cluster, producer) = connected_producer().await;
        cluster.script(
            &endpoint(0),
            SEND_MESSAGES_CODE,
            [Answer::Silence, confirmation(0), confirmation(1)],
        );
        assert!(producer.send(vec![message(b"old")]).await.is_err());
        let op = cluster.received(SEND_MESSAGES_CODE)[0].header.request;

        // The new call is polled first, so its replay holds the lane while
        // the owner's resolve waits for it.
        let (sent, resolved) =
            tokio::join!(producer.send(vec![message(b"new")]), resolve(&producer, op));

        sent.unwrap();
        assert_eq!(resolved, Ok(()));
        let sends = cluster.received(SEND_MESSAGES_CODE);
        assert_eq!(sends.len(), 3);
        assert_eq!(sends[0].header.request, sends[1].header.request);
        assert_ne!(sends[1].header.request, sends[2].header.request);
        assert_eq!(resolve(&producer, op).await, Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn given_another_producers_blocker_when_sending_should_return_it_without_resolving() {
        let (cluster, producer) = connected_producer().await;
        let other = producer_on(producer.core.client.clone());
        cluster.script(&endpoint(0), SEND_MESSAGES_CODE, [Answer::Silence]);
        assert!(producer.send(vec![message(b"old")]).await.is_err());
        let op = cluster.received(SEND_MESSAGES_CODE)[0].header.request;

        let Err(IggyError::ProducerSendFailed { cause, .. }) =
            other.send(vec![message(b"new")]).await
        else {
            panic!("another producer's blocker must be returned");
        };

        assert_eq!(*cause, IggyError::LaneBlocked(op));
        assert_eq!(cluster.count(SEND_MESSAGES_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_stopped_producer_when_rebuilt_should_recover_its_previous_operation() {
        for abort_send in [false, true] {
            let (cluster, producer) = connected_producer().await;
            let client = producer.core.client.clone();
            cluster.script(
                &endpoint(0),
                SEND_MESSAGES_CODE,
                [
                    Answer::Silence,
                    confirmation(0),
                    confirmation(1),
                    confirmation(2),
                ],
            );
            let sending = tokio::spawn(async move { producer.send(vec![message(b"old")]).await });
            eventually("the stopped producer's frame should leave", || {
                cluster.count(SEND_MESSAGES_CODE) == 1
            })
            .await;
            let op = cluster.received(SEND_MESSAGES_CODE)[0].header.request;
            if abort_send {
                sending.abort();
                assert!(sending.await.unwrap_err().is_cancelled());
            } else {
                let Err(IggyError::ProducerSendFailed { cause, .. }) = sending.await.unwrap()
                else {
                    panic!("the unanswered send should stay uncertain");
                };
                assert_eq!(*cause, IggyError::OperationUncertain(op));
            }

            let restarted = producer_on(client);
            let Err(IggyError::ProducerSendFailed { cause, failed, .. }) = restarted
                .send(vec![message(b"new-first"), message(b"new-last")])
                .await
            else {
                panic!("the restarted producer must preserve its predecessor's blocker");
            };
            assert_eq!(*cause, IggyError::LaneBlocked(op));
            assert_eq!(cluster.count(SEND_MESSAGES_CODE), 1);

            resolve(&restarted, op).await.unwrap();
            let response = restarted
                .send(Arc::try_unwrap(failed).unwrap())
                .await
                .unwrap();
            assert_eq!(response.confirmations.len(), 2);
            let sends = cluster.received(SEND_MESSAGES_CODE);
            assert_eq!(sends.len(), 4);
            assert_eq!(sends[0].header.request, sends[1].header.request);
            assert_eq!(sends[0].body, sends[1].body);
            assert_ne!(sends[1].header.request, sends[2].header.request);
            assert_ne!(sends[2].header.request, sends[3].header.request);
            assert_eq!(resolve(&restarted, op).await, Ok(()));
            assert_eq!(cluster.count(SEND_MESSAGES_CODE), 4);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_still_uncertain_owned_chunk_when_sending_again_should_preserve_its_blocker() {
        let (cluster, producer) = connected_producer().await;
        cluster.script(
            &endpoint(0),
            SEND_MESSAGES_CODE,
            [Answer::Silence, Answer::Silence],
        );
        assert!(producer.send(vec![message(b"old")]).await.is_err());
        let op = cluster.received(SEND_MESSAGES_CODE)[0].header.request;
        let started = Instant::now();

        let Err(IggyError::ProducerSendFailed { cause, .. }) =
            producer.send(vec![message(b"new")]).await
        else {
            panic!("the new chunk must remain unsent");
        };

        assert_eq!(*cause, IggyError::LaneBlocked(op));
        assert!(started.elapsed() <= OPERATION_TIMEOUT);
        let sends = cluster.received(SEND_MESSAGES_CODE);
        assert_eq!(sends.len(), 2);
        assert_eq!(sends[0].body, sends[1].body);
        assert!(producer.core.uncertain_operations().contains(&op));
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_outer_deadline_past_the_default_budget_when_a_send_is_unanswered_should_end_at_the_outer_deadline()
     {
        const OUTER_DEADLINE: Duration = OPERATION_TIMEOUT.saturating_mul(2);
        let (cluster, producer) = connected_producer().await;
        cluster.script(&endpoint(0), SEND_MESSAGES_CODE, [Answer::Silence]);
        let started = Instant::now();

        let result = with_deadline(
            started + OUTER_DEADLINE,
            producer.send(vec![message(b"unanswered")]),
        )
        .await;

        let Err(IggyError::ProducerSendFailed { cause, .. }) = result else {
            panic!("an unanswered send must fail");
        };
        let op = cluster.received(SEND_MESSAGES_CODE)[0].header.request;
        assert_eq!(*cause, IggyError::OperationUncertain(op));
        assert_eq!(started.elapsed(), OUTER_DEADLINE);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_no_longer_retained_chunk_when_sending_new_data_should_never_resubmit_the_old_chunk()
     {
        for reset_session in [false, true] {
            let (cluster, producer) = connected_producer().await;
            cluster.script(
                &endpoint(0),
                SEND_MESSAGES_CODE,
                [Answer::Silence, confirmation(8)],
            );
            assert!(producer.send(vec![message(b"old")]).await.is_err());
            let op = cluster.received(SEND_MESSAGES_CODE)[0].header.request;
            let client = producer.core.client.read().await;
            if reset_session {
                client.disconnect().await.unwrap();
                client.connect().await.unwrap();
            } else {
                client.abandon_operation(op).await.unwrap();
            }
            assert_eq!(
                client
                    .resolve_operation(op, (Instant::now() + OPERATION_TIMEOUT).into_std())
                    .await,
                Err(IggyError::OperationNotRetained(op))
            );
            drop(client);

            producer.send(vec![message(b"new")]).await.unwrap();

            let sends = cluster.received(SEND_MESSAGES_CODE);
            assert_eq!(sends.len(), 2);
            assert_ne!(sends[0].header.request, sends[1].header.request);
            assert_ne!(sends[0].body, sends[1].body);
            assert_eq!(
                sends[0].header.session != sends[1].header.session,
                reset_session
            );
            assert!(producer.core.uncertain_operations().is_empty());
        }
    }

    #[tokio::test]
    async fn test_aged_out_request_over_http_stops_producer_retries() {
        const SEND_REQUEST_LINE: &str = "POST /streams/1/topics/1/messages ";
        const TOPIC_REQUEST_LINE: &str = "GET /streams/1/topics/1 ";
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_url = format!("http://{}", listener.local_addr().unwrap());
        let request_lines = Arc::new(Mutex::new(Vec::new()));
        let server_request_lines = Arc::clone(&request_lines);
        let server = tokio::spawn(async move {
            let too_old = format!(
                r#"{{"id":{},"code":"request_too_old","reason":"Request too old","field":null}}"#,
                IggyError::RequestTooOld.as_code()
            );
            // The send to an explicit partition first reads its context from the topic details.
            let topic = crate::http::scripted_server::topic_details_json(&[(
                0,
                iggy_common::PartitionContext {
                    incarnation: 7,
                    owner_generation: 0,
                    metadata_op: 11,
                },
            )]);
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request_line = read_http_request(&mut stream).await;
                let (status, body) = if request_line.starts_with(TOPIC_REQUEST_LINE) {
                    ("200 OK", &topic)
                } else {
                    ("400 Bad Request", &too_old)
                };
                server_request_lines.lock().unwrap().push(request_line);
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let client = HttpClient::create(Arc::new(HttpClientConfig {
            api_url,
            jwt: Some("token".to_owned()),
            ..Default::default()
        }))
        .unwrap();
        assert_send_stops_on_request_too_old(ClientWrapper::Http(client)).await;
        server.abort();
        let request_lines = request_lines.lock().unwrap();
        assert_eq!(
            request_lines
                .iter()
                .filter(|line| line.starts_with(SEND_REQUEST_LINE))
                .count(),
            1,
            "unknown outcome must not be sent again: {request_lines:?}"
        );
        assert_eq!(request_lines.len(), 2, "{request_lines:?}");
        assert!(request_lines[0].starts_with(TOPIC_REQUEST_LINE));
    }

    /// Sends one message through a producer and asserts the
    /// send fails at once with `RequestTooOld`. Shuts the client down
    /// afterwards so a peer serving its binary connection sees it close.
    async fn assert_send_stops_on_request_too_old(client: ClientWrapper) {
        const SEND_TIMEOUT: Duration = Duration::from_secs(1);
        let producer = IggyProducerBuilder::new(
            IggyRwLock::new(client),
            Identifier::numeric(1).unwrap(),
            "stream".to_owned(),
            Identifier::numeric(1).unwrap(),
            "topic".to_owned(),
            None,
            None,
        )
        .partitioning(Partitioning::partition_id(0))
        .direct(DirectConfig::builder().build())
        .build();
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"unknown-outcome"))
            .build()
            .unwrap();
        let result = tokio::time::timeout(SEND_TIMEOUT, producer.send(vec![message]))
            .await
            .unwrap();
        let Err(IggyError::ProducerSendFailed { cause, failed, .. }) = result else {
            panic!("the producer must return an unconfirmed batch");
        };
        assert!(
            matches!(*cause, IggyError::RequestTooOld),
            "unexpected producer error: {cause:?}"
        );
        assert_eq!(failed.len(), 1);
        Client::shutdown(&*producer.core.client.read().await)
            .await
            .unwrap();
    }

    /// Reads one HTTP/1.1 request whose body has a `content-length` and returns
    /// its request line.
    async fn read_http_request(stream: &mut TcpStream) -> String {
        const HEAD_END: &[u8] = b"\r\n\r\n";
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "the client closed the connection mid-request");
            request.extend_from_slice(&chunk[..read]);
            let Some(head_len) = request
                .windows(HEAD_END.len())
                .position(|window| window == HEAD_END)
            else {
                continue;
            };
            let head = String::from_utf8_lossy(&request[..head_len]).into_owned();
            let body_len = head
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map_or(0, |(_, value)| value.trim().parse::<usize>().unwrap());
            if request.len() >= head_len + HEAD_END.len() + body_len {
                return head.lines().next().unwrap_or_default().to_owned();
            }
        }
    }
}
