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

//! At-least-once sink delivery: reads store no offset, the plugin accepts messages in
//! `consume`, and an offset is stored only after a flush proved the messages up to it
//! delivered. A failure hands the unflushed messages over again instead of skipping them.

use crate::SinkApi;
use crate::configs::connectors::{DeliveryGuarantee, InvalidMessages, SinkDeliveryConfig};
use crate::error::RuntimeError;
use crate::metrics::{Metrics, SinkLabels};
use crate::sink::{BatchFailure, BatchPolicy, SinkMessages, SinkTask};
use dlopen2::wrapper::Container;
use futures::StreamExt;
use iggy::prelude::{
    Checkpoint, IggyError, IggyMessage, NonZeroIggyDuration, PollingStrategy, ReadError,
    ReceivedMessage,
};
use iggy_connector_sdk::retry::retry_backoff;
use iggy_connector_sdk::sink::{
    ConsumeCallback, DELIVERY_CONTRACT_VERSION, FlushCallback, SinkCallStatus,
};
use iggy_connector_sdk::transforms::Transform;
use iggy_connector_sdk::{FlushPartition, StreamDecoder, TopicMetadata};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Instant, sleep, sleep_until};
use tracing::{error, info, warn};

/// The at-least-once settings of a sink whose plugin accepted the delivery contract.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Delivery {
    flush: FlushCallback,
    retry_max_attempts: Option<u32>,
    retry_initial_backoff: Duration,
    retry_max_backoff: Duration,
    flush_interval: Option<Duration>,
    flush_max_messages: usize,
    invalid_messages: InvalidMessages,
}

/// Enables at-least-once delivery on the plugin instance `plugin_id` when `config` asks for
/// it. A plugin that lacks the delivery symbols or refuses the contract fails the sink.
pub(crate) fn enable(
    container: &Container<SinkApi>,
    plugin_id: u32,
    key: &str,
    config: &SinkDeliveryConfig,
) -> Result<Option<Delivery>, RuntimeError> {
    if config.guarantee == DeliveryGuarantee::AtMostOnce {
        return Ok(None);
    }
    let options = DeliveryOptions::try_from(config)?;
    let (Some(enable), Some(flush)) = (
        container.iggy_sink_enable_at_least_once,
        container.iggy_sink_flush,
    ) else {
        return Err(RuntimeError::AtLeastOnceUnsupported(
            key.to_owned(),
            "its plugin was built with an SDK that predates the delivery contract".to_owned(),
        ));
    };
    if !SinkCallStatus::from(enable(plugin_id, DELIVERY_CONTRACT_VERSION)).is_success() {
        return Err(RuntimeError::AtLeastOnceUnsupported(
            key.to_owned(),
            "its plugin refused it for this configuration, and its log gives the reason".to_owned(),
        ));
    }
    Ok(Some(options.with_flush(flush)))
}

/// The validated `[delivery]` settings, before the plugin supplied its flush.
struct DeliveryOptions {
    retry_max_attempts: Option<u32>,
    retry_initial_backoff: Duration,
    retry_max_backoff: Duration,
    flush_interval: Option<Duration>,
    flush_max_messages: usize,
    invalid_messages: InvalidMessages,
}

impl DeliveryOptions {
    fn with_flush(self, flush: FlushCallback) -> Delivery {
        Delivery {
            flush,
            retry_max_attempts: self.retry_max_attempts,
            retry_initial_backoff: self.retry_initial_backoff,
            retry_max_backoff: self.retry_max_backoff,
            flush_interval: self.flush_interval,
            flush_max_messages: self.flush_max_messages,
            invalid_messages: self.invalid_messages,
        }
    }
}

impl TryFrom<&SinkDeliveryConfig> for DeliveryOptions {
    type Error = RuntimeError;

    fn try_from(config: &SinkDeliveryConfig) -> Result<Self, Self::Error> {
        if config.retry_max_attempts == Some(0) {
            return Err(RuntimeError::InvalidConfiguration(
                "delivery.retry_max_attempts must be at least 1. Leave it unset to retry until the destination recovers".to_owned(),
            ));
        }
        if config.flush_max_messages == 0 {
            return Err(RuntimeError::InvalidConfiguration(
                "delivery.flush_max_messages must be at least 1".to_owned(),
            ));
        }
        Ok(Self {
            retry_max_attempts: config.retry_max_attempts,
            retry_initial_backoff: non_zero_duration(
                "delivery.retry_initial_backoff",
                &config.retry_initial_backoff,
            )?,
            retry_max_backoff: non_zero_duration(
                "delivery.retry_max_backoff",
                &config.retry_max_backoff,
            )?,
            flush_interval: config
                .flush_interval
                .as_deref()
                .map(|interval| non_zero_duration("delivery.flush_interval", interval))
                .transpose()?,
            flush_max_messages: config.flush_max_messages as usize,
            invalid_messages: config.invalid_messages,
        })
    }
}

/// A zero wait would turn a retry or a flush timer into a busy loop.
fn non_zero_duration(name: &str, value: &str) -> Result<Duration, RuntimeError> {
    NonZeroIggyDuration::from_str(value)
        .map(|duration| duration.get_duration())
        .map_err(|error| RuntimeError::InvalidConfiguration(format!("Invalid {name}: {error}")))
}

/// Delivers the messages of one consume task at least once, until shutdown or the end of the
/// reads. It returns an error when a message cannot be delivered, leaving the stored offset of
/// its partition below the message.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn deliver_messages(
    plugin_id: u32,
    decoder: Arc<dyn StreamDecoder>,
    batch_size: u32,
    consume: ConsumeCallback,
    delivery: Delivery,
    transforms: Vec<Arc<dyn Transform>>,
    topic_metadata: TopicMetadata,
    messages: &mut impl SinkMessages,
    verbose: bool,
    benchmark: bool,
    plugin_key: &str,
    metrics: &Arc<Metrics>,
    labels: &SinkLabels,
    shutdown_rx: watch::Receiver<()>,
) -> Result<(), RuntimeError> {
    info!("Started delivering messages at least once for sink connector with ID: {plugin_id}");
    let topic_meta = postcard::to_allocvec(&topic_metadata).map_err(|error| {
        error!(
            "Failed to serialize topic metadata for sink connector with ID: {plugin_id}. {error}"
        );
        RuntimeError::FailedToSerializeTopicMetadata
    })?;
    let mut deliverer = Deliverer {
        task: SinkTask {
            plugin_id,
            plugin_key,
            consume,
            decoder: &decoder,
            transforms: &transforms,
            topic_metadata: &topic_metadata,
            verbose,
            benchmark,
            metrics,
            labels,
        },
        delivery,
        topic_meta,
        messages,
        shutdown_rx,
        batch_size: batch_size as usize,
        batch: None,
        next: None,
        partitions: BTreeMap::new(),
        failures: 0,
        flush_due: None,
        stopping: false,
    };
    let result = deliverer.run().await;
    info!("Stopped delivering messages for sink connector with ID: {plugin_id}");
    result
}

/// Progress of a partition between what the plugin accepted and what the server stored.
#[derive(Debug, Default)]
struct PartitionProgress {
    /// The first message accepted since the last flush, where a failed delivery resumes.
    first_unflushed: Option<u64>,
    /// The last message accepted since the last flush.
    last_unflushed: Option<Checkpoint>,
    /// Messages accepted since the last flush.
    unflushed_messages: usize,
    /// The last message a flush proved, while storing its offset has not succeeded.
    unstored: Option<Checkpoint>,
}

/// Messages of one partition read since the last batch was sent.
struct Batch {
    partition_id: u32,
    current_offset: u64,
    first_offset: u64,
    last: Checkpoint,
    messages: Vec<IggyMessage>,
}

enum Event {
    Message(Box<ReceivedMessage>),
    FlushDue,
    ReadError(ReadError),
    End,
}

struct Deliverer<'a, M> {
    task: SinkTask<'a>,
    delivery: Delivery,
    topic_meta: Vec<u8>,
    messages: &'a mut M,
    shutdown_rx: watch::Receiver<()>,
    batch_size: usize,
    batch: Option<Batch>,
    /// A message of another partition than the batch, read before the batch was sent.
    next: Option<ReceivedMessage>,
    partitions: BTreeMap<u32, PartitionProgress>,
    /// Transient failures since the last successful flush.
    failures: u32,
    flush_due: Option<Instant>,
    stopping: bool,
}

impl<M: SinkMessages> Deliverer<'_, M> {
    async fn run(&mut self) -> Result<(), RuntimeError> {
        loop {
            // Set by a shutdown that arrived during a retry backoff.
            if self.stopping {
                self.finish().await;
                return Ok(());
            }
            let message = match self.next.take() {
                Some(message) => Some(message),
                None => match self.next_event().await {
                    Event::Message(message) => Some(*message),
                    Event::FlushDue => {
                        self.flush().await?;
                        continue;
                    }
                    Event::ReadError(error) => {
                        self.task
                            .metrics
                            .inc_errors_with_labels(&self.task.labels.counter);
                        self.handle_read_error(error).await?;
                        continue;
                    }
                    Event::End => None,
                },
            };
            let ended = message.is_none();
            let ready = message.is_none_or(|message| self.add(message));
            if ready && let Some(batch) = self.batch.take() {
                self.send(batch).await?;
            }
            if ended {
                self.stopping = true;
            }
        }
    }

    async fn next_event(&mut self) -> Event {
        tokio::select! {
            _ = self.shutdown_rx.changed() => {
                info!(
                    "Sink connector with ID: {} received shutdown signal",
                    self.task.plugin_id
                );
                Event::End
            }
            () = flush_timer(self.flush_due) => Event::FlushDue,
            read = self.messages.next() => match read {
                Some(Ok(message)) => Event::Message(Box::new(message)),
                Some(Err(error)) => Event::ReadError(error),
                None => Event::End,
            },
        }
    }

    /// Adds `message` to the batch and tells whether the batch is ready to send. The plugin
    /// learns one partition per call, so a message of another partition sends the batch and
    /// waits for the next one.
    fn add(&mut self, message: ReceivedMessage) -> bool {
        if self
            .batch
            .as_ref()
            .is_some_and(|batch| batch.partition_id != message.partition_id)
        {
            self.next = Some(message);
            return true;
        }
        let offset = message.message.header.offset;
        let current_offset = message.current_offset;
        let last = message.checkpoint();
        let capacity = self.batch_size;
        let batch = self.batch.get_or_insert_with(|| Batch {
            partition_id: message.partition_id,
            current_offset,
            first_offset: offset,
            last,
            messages: Vec::with_capacity(capacity),
        });
        batch.current_offset = current_offset;
        batch.last = last;
        batch.messages.push(message.message);
        current_offset == offset || batch.messages.len() >= self.batch_size
    }

    async fn send(&mut self, batch: Batch) -> Result<(), RuntimeError> {
        let Batch {
            partition_id,
            current_offset,
            first_offset,
            last,
            messages,
        } = batch;
        let messages_count = messages.len();
        let timing = self
            .task
            .send_batch(
                partition_id,
                current_offset,
                messages,
                BatchPolicy::AtLeastOnce(self.delivery.invalid_messages),
            )
            .await?;
        if let Some(failure) = timing.failure {
            // The plugin may have dropped what it held, so the unflushed messages go again.
            let resume = self
                .partitions
                .get(&partition_id)
                .and_then(|progress| progress.first_unflushed)
                .unwrap_or(first_offset);
            return self.recover(failure, &[(partition_id, resume)]).await;
        }
        let progress = self.partitions.entry(partition_id).or_default();
        progress.first_unflushed.get_or_insert(first_offset);
        progress.last_unflushed = Some(last);
        progress.unflushed_messages += messages_count;
        let full = progress.unflushed_messages >= self.delivery.flush_max_messages;
        match self.delivery.flush_interval {
            Some(interval) if !full => {
                self.flush_due
                    .get_or_insert_with(|| Instant::now() + interval);
                Ok(())
            }
            _ => self.flush().await,
        }
    }

    /// Flushes every partition with accepted messages, then stores what the flush proved.
    async fn flush(&mut self) -> Result<(), RuntimeError> {
        self.flush_due = None;
        let partitions = self.unflushed();
        if !partitions.is_empty() {
            let status = self.call_flush(&partitions)?;
            if !status.is_success() {
                self.task
                    .metrics
                    .inc_errors_with_labels(&self.task.labels.counter);
                let resume = self.unflushed_starts();
                let offset = resume.first().map_or(0, |&(_, offset)| offset);
                let failure = BatchFailure {
                    status,
                    offset,
                    reason: format!(
                        "The sink failed to flush {} partitions with status: {}",
                        partitions.len(),
                        status.code()
                    ),
                };
                return self.recover(failure, &resume).await;
            }
            self.failures = 0;
            for progress in self.partitions.values_mut() {
                if let Some(last) = progress.last_unflushed.take() {
                    progress.first_unflushed = None;
                    progress.unflushed_messages = 0;
                    progress.unstored = Some(last);
                }
            }
        }
        self.store().await;
        Ok(())
    }

    /// Stores the offsets that flushes proved. A failed store is retried, since storing a later
    /// offset covers it as well.
    async fn store(&mut self) {
        let plugin_id = self.task.plugin_id;
        let mut failed = false;
        for (partition_id, progress) in &mut self.partitions {
            let Some(checkpoint) = progress.unstored else {
                continue;
            };
            match self.messages.commit(checkpoint).await {
                Ok(()) => progress.unstored = None,
                Err(error) => {
                    failed = true;
                    warn!(
                        "Failed to store offset: {} of partition: {partition_id} for sink connector with ID: {plugin_id}, which is retried. {error}",
                        checkpoint.offset()
                    );
                }
            }
        }
        if failed {
            let retry = Instant::now() + self.delivery.retry_initial_backoff;
            self.flush_due = Some(self.flush_due.map_or(retry, |due| due.min(retry)));
        }
    }

    /// Retries a transient failure from `resume`, one start offset per affected partition,
    /// after a backoff. A permanent failure or the last allowed attempt stops the delivery.
    async fn recover(
        &mut self,
        failure: BatchFailure,
        resume: &[(u32, u64)],
    ) -> Result<(), RuntimeError> {
        let partition_id = resume.first().map_or(0, |&(partition_id, _)| partition_id);
        let reason = format!("{} at offset: {}", failure.reason, failure.offset);
        if failure.status == SinkCallStatus::PermanentFailure {
            return Err(RuntimeError::DeliveryStopped {
                partition_id,
                reason,
            });
        }
        self.failures += 1;
        if self
            .delivery
            .retry_max_attempts
            .is_some_and(|max_attempts| self.failures >= max_attempts)
        {
            return Err(RuntimeError::DeliveryStopped {
                partition_id,
                reason: format!("{reason}, after {} attempts", self.failures),
            });
        }
        let backoff = retry_backoff(
            self.delivery.retry_initial_backoff,
            self.failures,
            self.delivery.retry_max_backoff,
        );
        warn!(
            "Delivering again from offsets: {resume:?} (partition, offset) for sink connector with ID: {} in {backoff:?}, attempt: {}. {reason}",
            self.task.plugin_id,
            self.failures + 1
        );
        // Stored before the seeks, which would make these checkpoints stale.
        self.store().await;
        tokio::select! {
            () = sleep(backoff) => {}
            _ = self.shutdown_rx.changed() => self.stopping = true,
        }
        for &(partition_id, offset) in resume {
            self.rewind(partition_id, PollingStrategy::offset(offset))
                .await?;
        }
        Ok(())
    }

    /// Reads `partition_id` again from `start`, forgetting what was accepted from it since its
    /// last flush. A seek that fails would let the reads continue past undelivered messages,
    /// so it stops the delivery.
    async fn rewind(
        &mut self,
        partition_id: u32,
        start: PollingStrategy,
    ) -> Result<(), RuntimeError> {
        self.partitions.remove(&partition_id);
        if self
            .batch
            .as_ref()
            .is_some_and(|batch| batch.partition_id == partition_id)
        {
            self.batch = None;
        }
        if self
            .next
            .as_ref()
            .is_some_and(|message| message.partition_id == partition_id)
        {
            self.next = None;
        }
        match self.messages.seek(partition_id, start).await {
            Ok(()) => Ok(()),
            // A rebalance moved the partition, and its new owner resumes at the stored offset.
            Err(IggyError::InvalidCommand) => {
                info!(
                    "Partition: {partition_id} is no longer read by sink connector with ID: {}, its new owner resumes at the stored offset.",
                    self.task.plugin_id
                );
                Ok(())
            }
            Err(error) => Err(RuntimeError::DeliveryStopped {
                partition_id,
                reason: format!("Failed to read the partition again from {start:?}. {error}"),
            }),
        }
    }

    /// Handles a partition that the reader stopped. Unlike at most once, a message that does
    /// not decode or a partition with a new history stops the delivery instead of being
    /// skipped, unless the delivery skips invalid messages.
    async fn handle_read_error(&mut self, error: ReadError) -> Result<(), RuntimeError> {
        let plugin_id = self.task.plugin_id;
        let TopicMetadata { stream, topic } = self.task.topic_metadata;
        match (error.partition_id, &error.error, error.skip) {
            (
                Some(partition_id),
                IggyError::FetchProgressUncertain(operation)
                | IggyError::PartitionLaneBlocked(_, operation),
                _,
            ) => {
                warn!(
                    "Abandoning operation: {operation} with an unknown outcome, which blocks partition: {partition_id} of sink connector with ID: {plugin_id} from stream: {stream}, topic: {topic}. The partition resumes after its stored offset. {}",
                    error.error
                );
                match self.messages.abandon_operation(*operation).await {
                    Ok(()) | Err(IggyError::OperationNotRetained(_)) => {}
                    Err(abandon_error) => warn!(
                        "Failed to abandon operation: {operation} of sink connector with ID: {plugin_id}: {abandon_error}"
                    ),
                }
                self.rewind(partition_id, PollingStrategy::next()).await
            }
            (Some(partition_id), _, Some(skip)) => {
                let at = error
                    .offset
                    .map(|offset| format!(" at offset: {offset}"))
                    .unwrap_or_default();
                if self.delivery.invalid_messages == InvalidMessages::Stop {
                    return Err(RuntimeError::DeliveryStopped {
                        partition_id,
                        reason: format!("A message{at} does not decode. {}", error.error),
                    });
                }
                warn!(
                    "Skipping a message{at} that does not decode in partition: {partition_id} of sink connector with ID: {plugin_id} from stream: {stream}, topic: {topic}. {}",
                    error.error
                );
                // A failed seek leaves the reader on the message, so it stops again.
                if let Err(seek_error) = self.messages.seek(partition_id, skip).await {
                    error!(
                        "Failed to skip a message of partition: {partition_id} of sink connector with ID: {plugin_id}: {seek_error}"
                    );
                }
                Ok(())
            }
            (Some(partition_id), IggyError::HistoryUnavailable, None) => {
                Err(RuntimeError::DeliveryStopped {
                    partition_id,
                    reason: format!(
                        "The partition has a new history, so messages after its stored offset may be gone. Move the stored offset to resume. {}",
                        error.error
                    ),
                })
            }
            (Some(partition_id), _, None) if error.is_lasting_refusal() => {
                warn!(
                    "Partition: {partition_id} of sink connector with ID: {plugin_id} from stream: {stream}, topic: {topic} was stopped by the reader after refusals in a row. It resumes after its stored offset. {}",
                    error.error
                );
                self.rewind(partition_id, PollingStrategy::next()).await
            }
            _ => {
                error!(
                    "Failed to receive message for sink connector with ID: {plugin_id} from stream: {stream}, topic: {topic}. {}",
                    error.error
                );
                Ok(())
            }
        }
    }

    /// Flushes once before stopping, without retrying. What the flush does not prove stays
    /// above the stored offsets, so the next start delivers it again.
    async fn finish(&mut self) {
        let partitions = self.unflushed();
        if !partitions.is_empty() {
            match self.call_flush(&partitions) {
                Ok(status) if status.is_success() => {
                    for progress in self.partitions.values_mut() {
                        if let Some(last) = progress.last_unflushed.take() {
                            progress.unstored = Some(last);
                        }
                    }
                }
                Ok(status) => error!(
                    "Failed to flush sink connector with ID: {} before stopping, status: {}. The next start delivers again from the stored offsets.",
                    self.task.plugin_id,
                    status.code()
                ),
                Err(error) => error!(
                    "Failed to flush sink connector with ID: {} before stopping. {error}",
                    self.task.plugin_id
                ),
            }
        }
        self.store().await;
    }

    fn unflushed(&self) -> Vec<FlushPartition> {
        self.partitions
            .iter()
            .filter_map(|(&partition_id, progress)| {
                progress.last_unflushed.map(|last| FlushPartition {
                    partition_id,
                    offset: last.offset(),
                })
            })
            .collect()
    }

    fn unflushed_starts(&self) -> Vec<(u32, u64)> {
        self.partitions
            .iter()
            .filter_map(|(&partition_id, progress)| {
                progress
                    .first_unflushed
                    .map(|offset| (partition_id, offset))
            })
            .collect()
    }

    fn call_flush(&self, partitions: &[FlushPartition]) -> Result<SinkCallStatus, RuntimeError> {
        let partitions = postcard::to_allocvec(partitions).map_err(|error| {
            error!(
                "Failed to serialize the partitions of a flush for sink connector with ID: {}. {error}",
                self.task.plugin_id
            );
            RuntimeError::FailedToSerializeFlushPartitions
        })?;
        let status = (self.delivery.flush)(
            self.task.plugin_id,
            self.topic_meta.as_ptr(),
            self.topic_meta.len(),
            partitions.as_ptr(),
            partitions.len(),
        );
        Ok(SinkCallStatus::from(status))
    }
}

async fn flush_timer(due: Option<Instant>) {
    match due {
        Some(due) => sleep_until(due).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::connectors::SinkDeliveryConfig;
    use dashmap::DashMap;
    use futures::Stream;
    use iggy::prelude::{IggyMessageHeader, PartitionContext, PollingKind};
    use iggy_connector_sdk::transforms::TransformType;
    use iggy_connector_sdk::{DecodedMessage, Error, RawMessages, Schema};
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::LazyLock;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::task::{Context, Poll};

    static TEST_PLUGIN_ID: AtomicU32 = AtomicU32::new(u32::MAX / 4);

    /// Statuses the stub plugin returns for its next calls, keyed by plugin id. A call after
    /// they ran out succeeds.
    static CONSUME_STATUSES: LazyLock<DashMap<u32, VecDeque<i32>>> = LazyLock::new(DashMap::new);
    static FLUSH_STATUSES: LazyLock<DashMap<u32, VecDeque<i32>>> = LazyLock::new(DashMap::new);
    /// The offsets that one consume call carried, with the status it returned.
    type ConsumeCall = (Vec<u64>, i32);

    static CONSUMED: LazyLock<DashMap<u32, Vec<ConsumeCall>>> = LazyLock::new(DashMap::new);
    static FLUSHED: LazyLock<DashMap<u32, Vec<Vec<FlushPartition>>>> = LazyLock::new(DashMap::new);

    fn next_status(statuses: &DashMap<u32, VecDeque<i32>>, plugin_id: u32) -> i32 {
        statuses
            .get_mut(&plugin_id)
            .and_then(|mut statuses| statuses.pop_front())
            .unwrap_or(0)
    }

    extern "C" fn scripted_consume(
        plugin_id: u32,
        _topic_meta_ptr: *const u8,
        _topic_meta_len: usize,
        _messages_meta_ptr: *const u8,
        _messages_meta_len: usize,
        messages_ptr: *const u8,
        messages_len: usize,
    ) -> i32 {
        let messages = unsafe { std::slice::from_raw_parts(messages_ptr, messages_len) };
        let raw = postcard::from_bytes::<RawMessages>(messages).expect("failed to deserialize");
        let status = next_status(&CONSUME_STATUSES, plugin_id);
        CONSUMED.entry(plugin_id).or_default().push((
            raw.messages.iter().map(|message| message.offset).collect(),
            status,
        ));
        status
    }

    extern "C" fn scripted_flush(
        plugin_id: u32,
        _topic_meta_ptr: *const u8,
        _topic_meta_len: usize,
        partitions_ptr: *const u8,
        partitions_len: usize,
    ) -> i32 {
        let partitions = unsafe { std::slice::from_raw_parts(partitions_ptr, partitions_len) };
        let partitions = postcard::from_bytes::<Vec<FlushPartition>>(partitions)
            .expect("failed to deserialize the flushed partitions");
        FLUSHED.entry(plugin_id).or_default().push(partitions);
        next_status(&FLUSH_STATUSES, plugin_id)
    }

    /// A topic whose partitions hold offsets from 0, read one partition after another. A seek
    /// moves the read position, so a message read again after a failure shows up twice.
    struct FakeLog {
        partitions: BTreeMap<u32, Vec<Vec<u8>>>,
        positions: BTreeMap<u32, usize>,
        errors: VecDeque<ReadError>,
        /// Reads wait instead of ending once every partition was read.
        endless: bool,
        /// Partitions that a rebalance moves to another member when they are sought.
        moved_on_seek: Vec<u32>,
        commit_failures: Vec<IggyError>,
        seeks: Vec<(u32, PollingStrategy)>,
        commits: Vec<(u32, u64)>,
    }

    impl FakeLog {
        fn new(partitions: &[(u32, Vec<&[u8]>)]) -> Self {
            Self {
                partitions: partitions
                    .iter()
                    .map(|(partition_id, payloads)| {
                        (
                            *partition_id,
                            payloads.iter().map(|payload| payload.to_vec()).collect(),
                        )
                    })
                    .collect(),
                positions: BTreeMap::new(),
                errors: VecDeque::new(),
                endless: false,
                moved_on_seek: Vec::new(),
                commit_failures: Vec::new(),
                seeks: Vec::new(),
                commits: Vec::new(),
            }
        }

        /// `count` valid JSON messages for each listed partition.
        fn json(partitions: &[(u32, usize)]) -> Self {
            let payloads: Vec<(u32, Vec<&[u8]>)> = partitions
                .iter()
                .map(|&(partition_id, count)| (partition_id, vec![&br#"{"id":1}"#[..]; count]))
                .collect();
            Self::new(&payloads)
        }

        fn stored(&self, partition_id: u32) -> Option<u64> {
            self.commits
                .iter()
                .rev()
                .find(|(partition, _)| *partition == partition_id)
                .map(|&(_, offset)| offset)
        }
    }

    impl Stream for FakeLog {
        type Item = Result<ReceivedMessage, ReadError>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(error) = self.errors.pop_front() {
                return Poll::Ready(Some(Err(error)));
            }
            let log = &mut *self;
            for (&partition_id, payloads) in &log.partitions {
                let position = log.positions.entry(partition_id).or_default();
                let Some(payload) = payloads.get(*position) else {
                    continue;
                };
                let offset = *position as u64;
                *position += 1;
                let head = payloads.len() as u64 - 1;
                return Poll::Ready(Some(Ok(ReceivedMessage::new(
                    message(offset, payload),
                    head,
                    partition_id,
                    PartitionContext::default(),
                ))));
            }
            if log.endless {
                Poll::Pending
            } else {
                Poll::Ready(None)
            }
        }
    }

    impl SinkMessages for FakeLog {
        async fn abandon_operation(&mut self, _operation: u64) -> Result<(), IggyError> {
            Ok(())
        }

        async fn seek(
            &mut self,
            partition_id: u32,
            start: PollingStrategy,
        ) -> Result<(), IggyError> {
            self.seeks.push((partition_id, start));
            if self.moved_on_seek.contains(&partition_id) {
                self.partitions.remove(&partition_id);
                return Err(IggyError::InvalidCommand);
            }
            let position = match start.kind {
                PollingKind::Offset => start.value,
                _ => self.stored(partition_id).map_or(0, |stored| stored + 1),
            };
            self.positions.insert(partition_id, position as usize);
            Ok(())
        }

        async fn commit(&mut self, checkpoint: Checkpoint) -> Result<(), IggyError> {
            if !self.commit_failures.is_empty() {
                return Err(self.commit_failures.remove(0));
            }
            self.commits
                .push((checkpoint.partition_id(), checkpoint.offset()));
            Ok(())
        }
    }

    fn message(offset: u64, payload: &[u8]) -> IggyMessage {
        IggyMessage {
            header: IggyMessageHeader {
                checksum: 0,
                id: u128::from(offset) + 1,
                offset,
                timestamp: 0,
                origin_timestamp: 0,
                user_headers_length: 0,
                payload_length: payload.len() as u32,
                reserved: 0,
            },
            payload: payload.to_vec().into(),
            user_headers: None,
        }
    }

    /// Drops every message, standing in for a filter that matches the whole batch.
    struct DroppingTransform;

    impl Transform for DroppingTransform {
        fn r#type(&self) -> TransformType {
            TransformType::FilterFields
        }

        fn transform(
            &self,
            _metadata: &TopicMetadata,
            _message: DecodedMessage,
        ) -> Result<Option<DecodedMessage>, Error> {
            Ok(None)
        }
    }

    /// Fails the message at `offset`, standing in for a transform that cannot handle it.
    struct FailingTransform {
        offset: u64,
    }

    impl Transform for FailingTransform {
        fn r#type(&self) -> TransformType {
            TransformType::UpdateFields
        }

        fn transform(
            &self,
            _metadata: &TopicMetadata,
            message: DecodedMessage,
        ) -> Result<Option<DecodedMessage>, Error> {
            if message.offset == Some(self.offset) {
                return Err(Error::InvalidRecordValue("cannot transform".to_owned()));
            }
            Ok(Some(message))
        }
    }

    /// Clears the checksum of the message at `offset`, which then fails to serialize for the
    /// plugin.
    struct ClearingTransform {
        offset: u64,
    }

    impl Transform for ClearingTransform {
        fn r#type(&self) -> TransformType {
            TransformType::DeleteFields
        }

        fn transform(
            &self,
            _metadata: &TopicMetadata,
            mut message: DecodedMessage,
        ) -> Result<Option<DecodedMessage>, Error> {
            if message.offset == Some(self.offset) {
                message.checksum = None;
            }
            Ok(Some(message))
        }
    }

    fn test_delivery() -> Delivery {
        Delivery {
            flush: scripted_flush,
            retry_max_attempts: None,
            retry_initial_backoff: Duration::from_millis(1),
            retry_max_backoff: Duration::from_millis(1),
            flush_interval: None,
            flush_max_messages: 10_000,
            invalid_messages: InvalidMessages::Stop,
        }
    }

    /// A plugin id with the given scripted statuses.
    fn plugin(consume_statuses: &[i32], flush_statuses: &[i32]) -> u32 {
        let plugin_id = TEST_PLUGIN_ID.fetch_add(1, Ordering::Relaxed);
        CONSUME_STATUSES.insert(plugin_id, consume_statuses.iter().copied().collect());
        FLUSH_STATUSES.insert(plugin_id, flush_statuses.iter().copied().collect());
        plugin_id
    }

    async fn deliver(
        plugin_id: u32,
        log: &mut FakeLog,
        delivery: Delivery,
        transforms: Vec<Arc<dyn Transform>>,
        batch_size: u32,
        shutdown_rx: watch::Receiver<()>,
    ) -> Result<(), RuntimeError> {
        deliver_messages(
            plugin_id,
            Schema::Json.decoder(),
            batch_size,
            scripted_consume,
            delivery,
            transforms,
            TopicMetadata {
                stream: "test_stream".to_owned(),
                topic: "test_topic".to_owned(),
            },
            log,
            false,
            false,
            "test_sink",
            &Arc::new(Metrics::init()),
            &SinkLabels::new("test_sink"),
            shutdown_rx,
        )
        .await
    }

    /// Runs the delivery until the log is read, with no shutdown.
    async fn deliver_all(
        plugin_id: u32,
        log: &mut FakeLog,
        delivery: Delivery,
    ) -> Result<(), RuntimeError> {
        deliver_with(plugin_id, log, delivery, Vec::new()).await
    }

    /// Runs the delivery through `transforms` until the log is read.
    async fn deliver_with(
        plugin_id: u32,
        log: &mut FakeLog,
        delivery: Delivery,
        transforms: Vec<Arc<dyn Transform>>,
    ) -> Result<(), RuntimeError> {
        let (_shutdown, shutdown_rx) = watch::channel(());
        deliver(
            plugin_id,
            log,
            delivery,
            transforms,
            BATCH_SIZE,
            shutdown_rx,
        )
        .await
    }

    /// Larger than any log here, so a batch ends at its partition head.
    const BATCH_SIZE: u32 = 16;

    fn consumed(plugin_id: u32) -> Vec<ConsumeCall> {
        CONSUMED
            .remove(&plugin_id)
            .map(|(_, calls)| calls)
            .unwrap_or_default()
    }

    fn flushed(plugin_id: u32) -> Vec<Vec<FlushPartition>> {
        FLUSHED
            .remove(&plugin_id)
            .map(|(_, calls)| calls)
            .unwrap_or_default()
    }

    fn stopped_partition(result: Result<(), RuntimeError>) -> u32 {
        stopped(result).0
    }

    fn stopped(result: Result<(), RuntimeError>) -> (u32, String) {
        match result {
            Err(RuntimeError::DeliveryStopped {
                partition_id,
                reason,
            }) => (partition_id, reason),
            other => panic!("expected the delivery to stop, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn given_accepted_batches_when_flushed_should_store_the_last_offset_of_each_partition() {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3), (1, 4)]);

        deliver_all(plugin_id, &mut log, test_delivery())
            .await
            .expect("the delivery should succeed");

        assert_eq!(log.commits, vec![(0, 2), (1, 3)]);
        assert_eq!(
            consumed(plugin_id),
            vec![(vec![0, 1, 2], 0), (vec![0, 1, 2, 3], 0)]
        );
        assert!(log.seeks.is_empty());
    }

    #[tokio::test]
    async fn given_a_transient_consume_failure_when_retried_should_deliver_the_batch_again() {
        let plugin_id = plugin(&[1], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);

        deliver_all(plugin_id, &mut log, test_delivery())
            .await
            .expect("the retry should deliver the batch");

        assert_eq!(log.seeks, vec![(0, PollingStrategy::offset(0))]);
        assert_eq!(
            consumed(plugin_id),
            vec![(vec![0, 1, 2], 1), (vec![0, 1, 2], 0)]
        );
        assert_eq!(log.commits, vec![(0, 2)]);
    }

    #[tokio::test]
    async fn given_a_permanent_consume_failure_when_delivered_should_stop_without_storing() {
        let plugin_id = plugin(&[2], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);

        let result = deliver_all(plugin_id, &mut log, test_delivery()).await;

        assert_eq!(stopped_partition(result), 0);
        assert!(log.commits.is_empty());
        assert!(log.seeks.is_empty(), "a permanent failure is not retried");
    }

    #[tokio::test]
    async fn given_transient_failures_past_the_attempt_limit_when_delivered_should_stop() {
        let plugin_id = plugin(&[1, 1, 1], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        let delivery = Delivery {
            retry_max_attempts: Some(2),
            ..test_delivery()
        };

        let result = deliver_all(plugin_id, &mut log, delivery).await;

        assert_eq!(stopped_partition(result), 0);
        assert_eq!(consumed(plugin_id).len(), 2, "two attempts in total");
        assert!(log.commits.is_empty());
    }

    #[tokio::test]
    async fn given_a_failed_flush_when_retried_should_deliver_the_unflushed_messages_again() {
        let plugin_id = plugin(&[], &[1]);
        let mut log = FakeLog::json(&[(0, 3)]);

        deliver_all(plugin_id, &mut log, test_delivery())
            .await
            .expect("the retry should deliver the batch");

        assert_eq!(log.seeks, vec![(0, PollingStrategy::offset(0))]);
        assert_eq!(consumed(plugin_id).len(), 2);
        assert_eq!(flushed(plugin_id).len(), 2);
        assert_eq!(
            log.commits,
            vec![(0, 2)],
            "stored only after the flush passed"
        );
    }

    #[tokio::test]
    async fn given_a_failure_in_one_partition_when_another_was_flushed_should_keep_its_offset() {
        // Partition 0 is read first and flushed before partition 1 fails for good.
        let plugin_id = plugin(&[0, 2], &[]);
        let mut log = FakeLog::json(&[(0, 2), (1, 3)]);

        let result = deliver_all(plugin_id, &mut log, test_delivery()).await;

        assert_eq!(stopped_partition(result), 1);
        assert_eq!(log.commits, vec![(0, 1)]);
    }

    #[tokio::test]
    async fn given_a_store_that_fails_when_delivering_should_store_the_offset_later() {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        log.commit_failures = vec![IggyError::OperationUncertain(7)];

        deliver_all(plugin_id, &mut log, test_delivery())
            .await
            .expect("the delivery should succeed");

        assert_eq!(log.commits, vec![(0, 2)]);
        assert_eq!(
            consumed(plugin_id).len(),
            1,
            "a failed store sends nothing again"
        );
    }

    #[tokio::test]
    async fn given_a_batch_that_a_transform_filtered_when_delivered_should_store_past_it() {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        let (_shutdown, shutdown_rx) = watch::channel(());

        deliver(
            plugin_id,
            &mut log,
            test_delivery(),
            vec![Arc::new(DroppingTransform)],
            BATCH_SIZE,
            shutdown_rx,
        )
        .await
        .expect("the delivery should succeed");

        assert_eq!(consumed(plugin_id), vec![(Vec::new(), 0)]);
        assert_eq!(log.commits, vec![(0, 2)]);
    }

    #[tokio::test]
    async fn given_a_message_that_does_not_decode_when_invalid_messages_stop_should_stop_before_it()
    {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::new(&[(0, vec![br#"{"id":1}"#, b"{not json", br#"{"id":3}"#])]);

        let result = deliver_all(plugin_id, &mut log, test_delivery()).await;

        assert_eq!(stopped_partition(result), 0);
        assert!(
            consumed(plugin_id).is_empty(),
            "nothing of the batch reaches the sink"
        );
        assert!(log.commits.is_empty());
    }

    #[tokio::test]
    async fn given_a_message_that_does_not_decode_when_invalid_messages_skip_should_store_past_it()
    {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::new(&[(0, vec![br#"{"id":1}"#, b"{not json", br#"{"id":3}"#])]);
        let delivery = Delivery {
            invalid_messages: InvalidMessages::Skip,
            ..test_delivery()
        };

        deliver_all(plugin_id, &mut log, delivery)
            .await
            .expect("the delivery should skip the message");

        assert_eq!(consumed(plugin_id), vec![(vec![0, 2], 0)]);
        assert_eq!(log.commits, vec![(0, 2)]);
    }

    #[tokio::test]
    async fn given_a_partition_with_a_new_history_when_read_should_stop_instead_of_skipping() {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        log.errors.push_back(ReadError {
            partition_id: Some(0),
            error: IggyError::HistoryUnavailable,
            skip: None,
            offset: None,
            record: None,
        });

        let result = deliver_all(plugin_id, &mut log, test_delivery()).await;

        assert_eq!(stopped_partition(result), 0);
        assert!(log.seeks.is_empty(), "no seek to the next message");
    }

    #[tokio::test]
    async fn given_accepted_messages_when_shutdown_arrives_should_flush_and_store_them() {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        log.endless = true;
        let delivery = Delivery {
            // Longer than the test, so only the stop flushes.
            flush_interval: Some(Duration::from_secs(3600)),
            ..test_delivery()
        };
        let (shutdown, shutdown_rx) = watch::channel(());
        // The log is read at once, then the reads wait, so the stop finds the batch accepted.
        let stop = tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            shutdown
                .send(())
                .expect("the delivery should hold a receiver");
        });

        deliver(
            plugin_id,
            &mut log,
            delivery,
            Vec::new(),
            BATCH_SIZE,
            shutdown_rx,
        )
        .await
        .expect("the delivery should stop cleanly");
        stop.await.expect("the stop task should finish");

        assert_eq!(
            flushed(plugin_id),
            vec![vec![FlushPartition {
                partition_id: 0,
                offset: 2
            }]]
        );
        assert_eq!(log.commits, vec![(0, 2)]);
    }

    #[tokio::test]
    async fn given_a_flush_that_fails_at_shutdown_when_stopping_should_store_nothing() {
        let plugin_id = plugin(&[], &[1]);
        let mut log = FakeLog::json(&[(0, 3)]);
        let delivery = Delivery {
            flush_interval: Some(Duration::from_secs(3600)),
            ..test_delivery()
        };

        deliver_all(plugin_id, &mut log, delivery)
            .await
            .expect("a failed final flush still stops cleanly");

        assert_eq!(flushed(plugin_id).len(), 1);
        assert!(log.commits.is_empty());
    }

    #[tokio::test]
    async fn given_a_flush_interval_when_a_partition_reaches_the_message_cap_should_flush_at_once()
    {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 6)]);
        let delivery = Delivery {
            flush_interval: Some(Duration::from_secs(3600)),
            flush_max_messages: 4,
            ..test_delivery()
        };
        let (_shutdown, shutdown_rx) = watch::channel(());

        // Batches of two: the cap flushes after the second, the end of the log after the third.
        deliver(plugin_id, &mut log, delivery, Vec::new(), 2, shutdown_rx)
            .await
            .expect("the delivery should succeed");

        assert_eq!(
            flushed(plugin_id),
            vec![
                vec![FlushPartition {
                    partition_id: 0,
                    offset: 3
                }],
                vec![FlushPartition {
                    partition_id: 0,
                    offset: 5
                }],
            ]
        );
        assert_eq!(log.commits, vec![(0, 3), (0, 5)]);
    }

    #[tokio::test]
    async fn given_a_transform_failure_when_invalid_messages_stop_should_stop_before_the_message() {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);

        let result = deliver_with(
            plugin_id,
            &mut log,
            test_delivery(),
            vec![Arc::new(FailingTransform { offset: 1 })],
        )
        .await;

        let (partition_id, reason) = stopped(result);
        assert_eq!(partition_id, 0);
        assert!(reason.contains("offset: 1"), "{reason}");
        assert!(
            consumed(plugin_id).is_empty(),
            "nothing of the batch reaches the sink"
        );
        assert!(log.commits.is_empty());
    }

    #[tokio::test]
    async fn given_a_transform_failure_when_invalid_messages_skip_should_store_past_the_message() {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        let delivery = Delivery {
            invalid_messages: InvalidMessages::Skip,
            ..test_delivery()
        };

        deliver_with(
            plugin_id,
            &mut log,
            delivery,
            vec![Arc::new(FailingTransform { offset: 1 })],
        )
        .await
        .expect("the delivery should skip the message");

        assert_eq!(consumed(plugin_id), vec![(vec![0, 2], 0)]);
        assert_eq!(log.commits, vec![(0, 2)]);
    }

    #[tokio::test]
    async fn given_a_serialization_failure_when_invalid_messages_stop_should_stop_before_the_message()
     {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);

        let result = deliver_with(
            plugin_id,
            &mut log,
            test_delivery(),
            vec![Arc::new(ClearingTransform { offset: 1 })],
        )
        .await;

        let (partition_id, reason) = stopped(result);
        assert_eq!(partition_id, 0);
        assert!(reason.contains("offset: 1"), "{reason}");
        assert!(
            consumed(plugin_id).is_empty(),
            "nothing of the batch reaches the sink"
        );
        assert!(log.commits.is_empty());
    }

    #[tokio::test]
    async fn given_a_serialization_failure_when_invalid_messages_skip_should_store_past_the_message()
     {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        let delivery = Delivery {
            invalid_messages: InvalidMessages::Skip,
            ..test_delivery()
        };

        deliver_with(
            plugin_id,
            &mut log,
            delivery,
            vec![Arc::new(ClearingTransform { offset: 1 })],
        )
        .await
        .expect("the delivery should skip the message");

        assert_eq!(consumed(plugin_id), vec![(vec![0, 2], 0)]);
        assert_eq!(log.commits, vec![(0, 2)]);
    }

    #[tokio::test]
    async fn given_a_flush_refused_because_its_instance_closed_when_delivered_should_store_nothing()
    {
        // `-1` is what a plugin returns for an instance that is no longer open.
        let plugin_id = plugin(&[], &[-1]);
        let mut log = FakeLog::json(&[(0, 3)]);

        let result = deliver_all(plugin_id, &mut log, test_delivery()).await;

        assert_eq!(stopped_partition(result), 0);
        assert!(
            log.commits.is_empty(),
            "a stale completion stores no offset"
        );
        assert!(log.seeks.is_empty(), "a closed instance is not retried");
    }

    #[tokio::test]
    async fn given_a_partition_moved_to_another_member_when_retrying_should_store_nothing_for_it() {
        // Partition 0 is delivered, then partition 1 fails and moves away before its retry.
        let plugin_id = plugin(&[0, 1], &[]);
        let mut log = FakeLog::json(&[(0, 2), (1, 3)]);
        log.moved_on_seek = vec![1];

        deliver_all(plugin_id, &mut log, test_delivery())
            .await
            .expect("a moved partition does not stop the delivery");

        assert_eq!(log.seeks, vec![(1, PollingStrategy::offset(0))]);
        assert_eq!(
            log.commits,
            vec![(0, 1)],
            "nothing is stored for the moved partition"
        );
    }

    #[test]
    fn given_delivery_settings_when_validated_should_refuse_zero_attempts_and_zero_waits() {
        let valid = SinkDeliveryConfig {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            ..SinkDeliveryConfig::default()
        };
        assert!(DeliveryOptions::try_from(&valid).is_ok());

        for invalid in [
            SinkDeliveryConfig {
                retry_max_attempts: Some(0),
                ..valid.clone()
            },
            SinkDeliveryConfig {
                retry_initial_backoff: "0s".to_owned(),
                ..valid.clone()
            },
            SinkDeliveryConfig {
                flush_interval: Some("0s".to_owned()),
                ..valid.clone()
            },
            SinkDeliveryConfig {
                flush_max_messages: 0,
                ..valid.clone()
            },
        ] {
            assert!(
                matches!(
                    DeliveryOptions::try_from(&invalid),
                    Err(RuntimeError::InvalidConfiguration(_))
                ),
                "{invalid:?} must be refused"
            );
        }
    }
}
