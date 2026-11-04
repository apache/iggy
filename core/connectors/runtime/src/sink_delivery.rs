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
use crate::sink::{BatchPolicy, SinkMessages, SinkOffsets, SinkTask, StoppedRead, stopped_read};
use dlopen2::wrapper::Container;
use futures::StreamExt;
use iggy::prelude::{
    Checkpoint, IggyError, IggyMessage, NonZeroIggyDuration, PollingKind, PollingStrategy,
    ReadError, ReceivedMessage,
};
use iggy_connector_sdk::retry::retry_backoff;
use iggy_connector_sdk::sink::{DELIVERY_CONTRACT_VERSION, FlushCallback, SinkCallStatus};
use iggy_connector_sdk::{FlushPartition, TopicMetadata};
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{Instant, Sleep, sleep, sleep_until};
use tracing::{error, info, warn};

/// The at-least-once settings of a sink whose plugin accepted the delivery contract.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Delivery {
    flush: FlushCallback,
    options: DeliveryOptions,
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
            "its plugin refused it, and its log gives the reason".to_owned(),
        ));
    }
    Ok(Some(Delivery { flush, options }))
}

/// The validated `[delivery]` settings.
#[derive(Debug, Clone, Copy)]
struct DeliveryOptions {
    retry_max_attempts: Option<NonZeroU32>,
    retry_initial_backoff: Duration,
    retry_max_backoff: Duration,
    flush_interval: Option<Duration>,
    flush_max_messages: usize,
    invalid_messages: InvalidMessages,
}

impl TryFrom<&SinkDeliveryConfig> for DeliveryOptions {
    type Error = RuntimeError;

    fn try_from(config: &SinkDeliveryConfig) -> Result<Self, Self::Error> {
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
            flush_max_messages: config.flush_max_messages.get() as usize,
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
pub(crate) async fn deliver_messages<M: SinkMessages>(
    task: &SinkTask,
    delivery: Delivery,
    messages: &mut M,
    shutdown_rx: watch::Receiver<()>,
) -> Result<(), RuntimeError> {
    let plugin_id = task.plugin_id;
    info!("Started delivering messages at least once for sink connector with ID: {plugin_id}");
    let offsets = messages.offsets();
    let mut deliverer = Deliverer {
        task,
        delivery,
        messages,
        offsets,
        shutdown_rx,
        batch: None,
        next: None,
        partitions: BTreeMap::new(),
        retries: BTreeMap::new(),
        flush_failures: 0,
        flush_due: None,
        flush_timer: Box::pin(sleep_until(Instant::now())),
        storing: None,
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
    first_unflushed: Option<Checkpoint>,
    /// The last message accepted since the last flush.
    last_unflushed: Option<Checkpoint>,
    /// Messages accepted since the last flush.
    unflushed_messages: usize,
    /// The last message a flush proved, while storing its offset has not succeeded.
    unstored: Option<Checkpoint>,
}

/// Transient failures in a row of the consume calls of one partition.
#[derive(Debug, Clone, Copy)]
struct ConsumeRetry {
    /// The first message that the last failed call left undelivered. A call that delivers it
    /// ends the retries.
    offset: u64,
    attempts: u32,
}

/// Messages of one partition read since the last batch was sent.
struct Batch {
    partition_id: u32,
    current_offset: u64,
    first: Checkpoint,
    last: Checkpoint,
    messages: Vec<IggyMessage>,
}

/// The checkpoints that one store sent, each with its result.
type StoreOutcome = Vec<(Checkpoint, Result<(), IggyError>)>;

enum Event {
    Message(ReceivedMessage),
    FlushDue,
    Stored(Result<StoreOutcome, JoinError>),
    ReadError(ReadError),
    End,
}

struct Deliverer<'a, M: SinkMessages> {
    task: &'a SinkTask,
    delivery: Delivery,
    messages: &'a mut M,
    offsets: M::Offsets,
    shutdown_rx: watch::Receiver<()>,
    batch: Option<Batch>,
    /// A message of another partition than the batch, read before the batch was sent.
    next: Option<ReceivedMessage>,
    partitions: BTreeMap<u32, PartitionProgress>,
    /// Per partition, the consume calls in a row that failed transiently.
    retries: BTreeMap<u32, ConsumeRetry>,
    /// Flushes in a row that failed transiently.
    flush_failures: u32,
    /// When `flush_timer` runs out, or `None` while no flush waits.
    flush_due: Option<Instant>,
    flush_timer: Pin<Box<Sleep>>,
    /// The store in flight. A later store covers an earlier one, so one at a time is enough.
    storing: Option<JoinHandle<StoreOutcome>>,
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
                    Event::Message(message) => Some(message),
                    Event::FlushDue => {
                        self.flush().await?;
                        continue;
                    }
                    Event::Stored(outcome) => {
                        self.stored(outcome);
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
            () = &mut self.flush_timer, if self.flush_due.is_some() => Event::FlushDue,
            outcome = store_finished(&mut self.storing) => Event::Stored(outcome),
            read = self.messages.next() => match read {
                Some(Ok(message)) => Event::Message(message),
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
        let checkpoint = message.checkpoint();
        let capacity = self.task.batch_size;
        let batch = self.batch.get_or_insert_with(|| Batch {
            partition_id: message.partition_id,
            current_offset,
            first: checkpoint,
            last: checkpoint,
            messages: Vec::with_capacity(capacity),
        });
        batch.current_offset = current_offset;
        batch.last = checkpoint;
        batch.messages.push(message.message);
        current_offset == offset || batch.messages.len() >= self.task.batch_size
    }

    async fn send(&mut self, batch: Batch) -> Result<(), RuntimeError> {
        let Batch {
            partition_id,
            current_offset,
            first,
            last,
            messages,
        } = batch;
        let messages_count = messages.len();
        let policy = BatchPolicy::AtLeastOnce(self.delivery.options.invalid_messages);
        let timing = match self
            .task
            .send_batch(partition_id, current_offset, messages, policy)
            .await
        {
            Ok(timing) => timing,
            Err(error) => return Err(self.stop(vec![partition_id], error.to_string()).await),
        };
        if let Some(failure) = timing.failure {
            // The plugin may have dropped what it held, so the unflushed messages go again.
            let resume = self
                .partitions
                .get(&partition_id)
                .and_then(|progress| progress.first_unflushed)
                .unwrap_or(first);
            let retry = self.retries.entry(partition_id).or_insert(ConsumeRetry {
                offset: failure.offset,
                attempts: 0,
            });
            retry.offset = failure.offset;
            retry.attempts += 1;
            let attempts = retry.attempts;
            let reason = format!("{} at offset: {}", failure.reason, failure.offset);
            return self
                .recover(
                    failure.status,
                    reason,
                    vec![(partition_id, resume)],
                    attempts,
                )
                .await;
        }
        if self
            .retries
            .get(&partition_id)
            .is_some_and(|retry| last.offset() >= retry.offset)
        {
            self.retries.remove(&partition_id);
        }
        let progress = self.partitions.entry(partition_id).or_default();
        progress.first_unflushed.get_or_insert(first);
        progress.last_unflushed = Some(last);
        progress.unflushed_messages += messages_count;
        let full = progress.unflushed_messages >= self.delivery.options.flush_max_messages;
        match self.delivery.options.flush_interval {
            Some(interval) if !full => {
                self.flush_by(Instant::now() + interval);
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
            let status = self.call_flush(&partitions).await;
            if !status.is_success() {
                self.task
                    .metrics
                    .inc_errors_with_labels(&self.task.labels.counter);
                self.flush_failures += 1;
                let resume = self.unflushed_starts();
                let starts: Vec<(u32, u64)> = resume
                    .iter()
                    .map(|&(partition_id, first)| (partition_id, first.offset()))
                    .collect();
                let reason = format!(
                    "The sink failed to flush the messages from offsets: {starts:?} (partition, offset) with status: {}",
                    status.code()
                );
                return self
                    .recover(status, reason, resume, self.flush_failures)
                    .await;
            }
            self.flushed();
        }
        self.store();
        Ok(())
    }

    /// Moves what a successful flush proved to the offsets waiting to be stored.
    fn flushed(&mut self) {
        self.flush_failures = 0;
        for progress in self.partitions.values_mut() {
            if let Some(last) = progress.last_unflushed.take() {
                progress.first_unflushed = None;
                progress.unflushed_messages = 0;
                progress.unstored = Some(last);
            }
        }
    }

    /// Starts storing the offsets that flushes proved. A store in flight starts the next one
    /// when it ends.
    fn store(&mut self) {
        if self.storing.is_some() {
            return;
        }
        let checkpoints: Vec<Checkpoint> = self
            .partitions
            .values()
            .filter_map(|progress| progress.unstored)
            .collect();
        if checkpoints.is_empty() {
            return;
        }
        let offsets = self.offsets.clone();
        self.storing = Some(tokio::spawn(async move {
            let mut outcome = Vec::with_capacity(checkpoints.len());
            for checkpoint in checkpoints {
                outcome.push((checkpoint, offsets.store(checkpoint).await));
            }
            outcome
        }));
    }

    /// Records what a store did. A failed store is retried, since storing a later offset covers
    /// it as well.
    fn stored(&mut self, outcome: Result<StoreOutcome, JoinError>) {
        let plugin_id = self.task.plugin_id;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                error!(
                    "Failed to store offsets for sink connector with ID: {plugin_id}, which is retried. {error}"
                );
                self.retry_store();
                return;
            }
        };
        let mut failed = false;
        for (checkpoint, result) in outcome {
            let partition_id = checkpoint.partition_id();
            match result {
                Ok(()) => {
                    if let Some(progress) = self.partitions.get_mut(&partition_id)
                        && progress.unstored == Some(checkpoint)
                    {
                        progress.unstored = None;
                    }
                }
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
            self.retry_store();
        } else {
            self.store();
        }
    }

    fn retry_store(&mut self) {
        self.flush_by(Instant::now() + self.delivery.options.retry_initial_backoff);
    }

    /// Waits for the store in flight, then stores what it left, so that a seek or a stop does
    /// not leave a proven offset unsent.
    async fn store_now(&mut self) {
        if let Some(storing) = self.storing.take() {
            let outcome = storing.await;
            self.stored(outcome);
        }
        self.store();
        if let Some(storing) = self.storing.take() {
            let outcome = storing.await;
            self.stored(outcome);
        }
    }

    /// Makes the next flush run by `due` at the latest.
    fn flush_by(&mut self, due: Instant) {
        if self.flush_due.is_some_and(|scheduled| scheduled <= due) {
            return;
        }
        self.flush_due = Some(due);
        self.flush_timer.as_mut().reset(due);
    }

    /// Retries a transient failure, which the calls that hand over `resume` had for `attempts`
    /// times in a row, after a backoff. `resume` holds the first message to hand over again of
    /// each partition. A permanent failure or the last allowed attempt stops the delivery.
    async fn recover(
        &mut self,
        status: SinkCallStatus,
        reason: String,
        resume: Vec<(u32, Checkpoint)>,
        attempts: u32,
    ) -> Result<(), RuntimeError> {
        let partitions: Vec<u32> = resume
            .iter()
            .map(|&(partition_id, _)| partition_id)
            .collect();
        // The plugin may have dropped what it held for these partitions, so no flush may
        // prove it before they are read again.
        for &partition_id in &partitions {
            self.forget(partition_id);
        }
        if status == SinkCallStatus::PermanentFailure {
            return Err(self.stop(partitions, reason).await);
        }
        if self
            .delivery
            .options
            .retry_max_attempts
            .is_some_and(|max_attempts| attempts >= max_attempts.get())
        {
            let reason = format!("{reason}, after {attempts} attempts");
            return Err(self.stop(partitions, reason).await);
        }
        let backoff = retry_backoff(
            self.delivery.options.retry_initial_backoff,
            attempts,
            self.delivery.options.retry_max_backoff,
        );
        let starts: Vec<(u32, u64)> = resume
            .iter()
            .map(|&(partition_id, first)| (partition_id, first.offset()))
            .collect();
        warn!(
            "Delivering again from offsets: {starts:?} (partition, offset) for sink connector with ID: {} in {backoff:?}, attempt: {}. {reason}",
            self.task.plugin_id,
            attempts + 1
        );
        // Stored before the seeks, which would make these checkpoints stale.
        self.store_now().await;
        tokio::select! {
            () = sleep(backoff) => {}
            _ = self.shutdown_rx.changed() => self.stopping = true,
        }
        for (partition_id, first) in resume {
            // The history of the first message, so that a partition recreated meanwhile is
            // refused instead of read from the same offset in its new history.
            let start = PollingStrategy::continuation(
                partition_id,
                PollingKind::Offset,
                first.offset(),
                first.context(),
            );
            self.rewind(partition_id, start).await?;
        }
        Ok(())
    }

    /// Forgets what the plugin accepted from `partition_id` since its last flush, keeping the
    /// offset that a flush proved.
    fn forget(&mut self, partition_id: u32) {
        if let Some(progress) = self.partitions.get_mut(&partition_id) {
            progress.first_unflushed = None;
            progress.last_unflushed = None;
            progress.unflushed_messages = 0;
        }
    }

    /// Stops the delivery at `partitions`. The other partitions flush once and every proven
    /// offset is stored first, so a restart delivers again only what the stop left in doubt.
    async fn stop(&mut self, partitions: Vec<u32>, reason: String) -> RuntimeError {
        for &partition_id in &partitions {
            self.forget(partition_id);
        }
        self.finish().await;
        let TopicMetadata { stream, topic } = &self.task.topic_metadata;
        RuntimeError::DeliveryStopped {
            stream: stream.clone(),
            topic: topic.clone(),
            partitions,
            reason,
        }
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
                self.retries.remove(&partition_id);
                info!(
                    "Partition: {partition_id} is no longer read by sink connector with ID: {}, its new owner resumes at the stored offset.",
                    self.task.plugin_id
                );
                Ok(())
            }
            Err(error) => {
                let reason = format!("Failed to read the partition again from {start:?}. {error}");
                Err(self.stop(vec![partition_id], reason).await)
            }
        }
    }

    /// Handles a partition that the reader stopped. Unlike at most once, a message that does
    /// not decode or a partition with a new history stops the delivery instead of being
    /// skipped, unless the delivery skips invalid messages.
    async fn handle_read_error(&mut self, error: ReadError) -> Result<(), RuntimeError> {
        let task = self.task;
        let plugin_id = task.plugin_id;
        let TopicMetadata { stream, topic } = &task.topic_metadata;
        match stopped_read(self.messages, &error, plugin_id, &task.topic_metadata).await {
            Some(StoppedRead::Abandoned(partition_id) | StoppedRead::Refused(partition_id)) => {
                self.rewind(partition_id, PollingStrategy::next()).await
            }
            Some(StoppedRead::Undecodable { partition_id, skip }) => {
                let at = error
                    .offset
                    .map(|offset| format!(" at offset: {offset}"))
                    .unwrap_or_default();
                if self.delivery.options.invalid_messages == InvalidMessages::Stop {
                    let reason = format!("A message{at} does not decode. {}", error.error);
                    return Err(self.stop(vec![partition_id], reason).await);
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
            Some(StoppedRead::NewHistory(partition_id)) => {
                let reason = format!(
                    "The partition has a new history, so messages after its stored offset may be gone. Move the stored offset to resume. {}",
                    error.error
                );
                Err(self.stop(vec![partition_id], reason).await)
            }
            None => Ok(()),
        }
    }

    /// Flushes once before stopping, without retrying, and stores every proven offset. What
    /// the flush does not prove stays above the stored offsets, so the next start delivers it
    /// again.
    async fn finish(&mut self) {
        let partitions = self.unflushed();
        if !partitions.is_empty() {
            let status = self.call_flush(&partitions).await;
            if status.is_success() {
                self.flushed();
            } else {
                error!(
                    "Failed to flush sink connector with ID: {} before stopping, status: {}. The next start delivers again from the stored offsets.",
                    self.task.plugin_id,
                    status.code()
                );
            }
        }
        self.store_now().await;
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

    fn unflushed_starts(&self) -> Vec<(u32, Checkpoint)> {
        self.partitions
            .iter()
            .filter_map(|(&partition_id, progress)| {
                progress.first_unflushed.map(|first| (partition_id, first))
            })
            .collect()
    }

    /// Calls the plugin's flush on a blocking thread, since a sink that buffers writes its
    /// buffers there, which would hold up an async worker for the whole write.
    async fn call_flush(&self, partitions: &[FlushPartition]) -> SinkCallStatus {
        let plugin_id = self.task.plugin_id;
        let partitions = match postcard::to_allocvec(partitions) {
            Ok(partitions) => partitions,
            Err(error) => {
                error!(
                    "Failed to serialize the partitions of a flush for sink connector with ID: {plugin_id}. {error}"
                );
                return SinkCallStatus::PermanentFailure;
            }
        };
        let flush = self.delivery.flush;
        let topic_meta = self.task.topic_meta.clone();
        match tokio::task::spawn_blocking(move || {
            flush(
                plugin_id,
                topic_meta.as_ptr(),
                topic_meta.len(),
                partitions.as_ptr(),
                partitions.len(),
            )
        })
        .await
        {
            Ok(status) => SinkCallStatus::from(status),
            Err(error) => {
                error!("The flush of sink connector with ID: {plugin_id} did not finish. {error}");
                SinkCallStatus::PermanentFailure
            }
        }
    }
}

/// Waits for the store in flight, or forever without one.
async fn store_finished(
    storing: &mut Option<JoinHandle<StoreOutcome>>,
) -> Result<StoreOutcome, JoinError> {
    let Some(handle) = storing.as_mut() else {
        return std::future::pending().await;
    };
    let outcome = handle.await;
    *storing = None;
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::connectors::SinkDeliveryConfig;
    use crate::metrics::{Metrics, SinkLabels};
    use crate::sink::serialize_topic_metadata;
    use dashmap::DashMap;
    use futures::Stream;
    use iggy::prelude::{IggyMessageHeader, PartitionContext};
    use iggy_connector_sdk::transforms::{Transform, TransformType};
    use iggy_connector_sdk::{DecodedMessage, Error, RawMessages, Schema};
    use std::collections::VecDeque;
    use std::future::Future;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, LazyLock, Mutex};
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
        /// Partitions whose seeks fail.
        failing_seeks: Vec<u32>,
        /// The partition history that every message is read under.
        context: PartitionContext,
        seeks: Vec<(u32, PollingStrategy)>,
        offsets: FakeOffsets,
    }

    /// Records the stored offsets, which a store from another task reaches through a clone.
    #[derive(Clone, Default)]
    struct FakeOffsets {
        commits: Arc<Mutex<Vec<(u32, u64)>>>,
        /// Failures the next stores return, in order. A store after they ran out succeeds.
        failures: Arc<Mutex<VecDeque<IggyError>>>,
    }

    impl SinkOffsets for FakeOffsets {
        fn store(
            &self,
            checkpoint: Checkpoint,
        ) -> impl Future<Output = Result<(), IggyError>> + Send {
            let failure = self
                .failures
                .lock()
                .expect("the failures were poisoned")
                .pop_front();
            let result = match failure {
                Some(error) => Err(error),
                None => {
                    self.commits
                        .lock()
                        .expect("the commits were poisoned")
                        .push((checkpoint.partition_id(), checkpoint.offset()));
                    Ok(())
                }
            };
            std::future::ready(result)
        }
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
                failing_seeks: Vec::new(),
                context: PartitionContext::default(),
                seeks: Vec::new(),
                offsets: FakeOffsets::default(),
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

        /// The stored offsets, in the order of their stores.
        fn commits(&self) -> Vec<(u32, u64)> {
            self.offsets
                .commits
                .lock()
                .expect("the commits were poisoned")
                .clone()
        }

        fn fail_stores(&self, failures: impl IntoIterator<Item = IggyError>) {
            self.offsets
                .failures
                .lock()
                .expect("the failures were poisoned")
                .extend(failures);
        }

        fn stored(&self, partition_id: u32) -> Option<u64> {
            self.commits()
                .into_iter()
                .rev()
                .find(|&(partition, _)| partition == partition_id)
                .map(|(_, offset)| offset)
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
                    log.context,
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
        type Offsets = FakeOffsets;

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
            if self.failing_seeks.contains(&partition_id) {
                return Err(IggyError::Disconnected);
            }
            let position = match start.kind {
                PollingKind::Offset => start.value,
                _ => self.stored(partition_id).map_or(0, |stored| stored + 1),
            };
            self.positions.insert(partition_id, position as usize);
            Ok(())
        }

        fn offsets(&self) -> FakeOffsets {
            self.offsets.clone()
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

    fn test_options() -> DeliveryOptions {
        DeliveryOptions {
            retry_max_attempts: None,
            retry_initial_backoff: Duration::from_millis(1),
            retry_max_backoff: Duration::from_millis(1),
            flush_interval: None,
            flush_max_messages: 10_000,
            invalid_messages: InvalidMessages::Stop,
        }
    }

    fn test_delivery() -> Delivery {
        delivery(test_options())
    }

    fn delivery(options: DeliveryOptions) -> Delivery {
        Delivery {
            flush: scripted_flush,
            options,
        }
    }

    /// Where a retry reads `partition_id` again from `offset`, under the history of the log.
    fn resume_at(partition_id: u32, offset: u64, context: PartitionContext) -> PollingStrategy {
        PollingStrategy::continuation(partition_id, PollingKind::Offset, offset, context)
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
        let topic_metadata = TopicMetadata {
            stream: "test_stream".to_owned(),
            topic: "test_topic".to_owned(),
        };
        let task = SinkTask {
            plugin_id,
            plugin_key: "test_sink".to_owned(),
            consume: scripted_consume,
            decoder: Schema::Json.decoder(),
            batch_size: batch_size as usize,
            transforms,
            topic_meta: serialize_topic_metadata(plugin_id, &topic_metadata)
                .expect("the topic metadata should serialize"),
            topic_metadata,
            verbose: false,
            benchmark: false,
            metrics: Arc::new(Metrics::init()),
            labels: Arc::new(SinkLabels::new("test_sink")),
        };
        deliver_messages(&task, delivery, log, shutdown_rx).await
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

    fn stopped_partitions(result: Result<(), RuntimeError>) -> Vec<u32> {
        stopped(result).0
    }

    fn stopped(result: Result<(), RuntimeError>) -> (Vec<u32>, String) {
        match result {
            Err(RuntimeError::DeliveryStopped {
                stream,
                topic,
                partitions,
                reason,
            }) => {
                assert_eq!(
                    (stream.as_str(), topic.as_str()),
                    ("test_stream", "test_topic")
                );
                (partitions, reason)
            }
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

        assert_eq!(log.commits(), vec![(0, 2), (1, 3)]);
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

        assert_eq!(
            log.seeks,
            vec![(0, resume_at(0, 0, PartitionContext::default()))]
        );
        assert_eq!(
            consumed(plugin_id),
            vec![(vec![0, 1, 2], 1), (vec![0, 1, 2], 0)]
        );
        assert_eq!(log.commits(), vec![(0, 2)]);
    }

    #[tokio::test]
    async fn given_a_permanent_consume_failure_when_delivered_should_stop_without_storing() {
        let plugin_id = plugin(&[2], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);

        let result = deliver_all(plugin_id, &mut log, test_delivery()).await;

        assert_eq!(stopped_partitions(result), [0]);
        assert!(log.commits().is_empty());
        assert!(log.seeks.is_empty(), "a permanent failure is not retried");
    }

    #[tokio::test]
    async fn given_transient_failures_past_the_attempt_limit_when_delivered_should_stop() {
        let plugin_id = plugin(&[1, 1, 1], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        let delivery = delivery(DeliveryOptions {
            retry_max_attempts: NonZeroU32::new(2),
            ..test_options()
        });

        let result = deliver_all(plugin_id, &mut log, delivery).await;

        assert_eq!(stopped_partitions(result), [0]);
        assert_eq!(consumed(plugin_id).len(), 2, "two attempts in total");
        assert!(log.commits().is_empty());
    }

    #[tokio::test]
    async fn given_a_failed_flush_when_retried_should_deliver_the_unflushed_messages_again() {
        let plugin_id = plugin(&[], &[1]);
        let mut log = FakeLog::json(&[(0, 3)]);

        deliver_all(plugin_id, &mut log, test_delivery())
            .await
            .expect("the retry should deliver the batch");

        assert_eq!(
            log.seeks,
            vec![(0, resume_at(0, 0, PartitionContext::default()))]
        );
        assert_eq!(consumed(plugin_id).len(), 2);
        assert_eq!(flushed(plugin_id).len(), 2);
        assert_eq!(
            log.commits(),
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

        assert_eq!(stopped_partitions(result), [1]);
        assert_eq!(log.commits(), vec![(0, 1)]);
    }

    #[tokio::test]
    async fn given_a_store_that_fails_when_delivering_should_store_the_offset_later() {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        log.fail_stores([IggyError::OperationUncertain(7)]);

        deliver_all(plugin_id, &mut log, test_delivery())
            .await
            .expect("the delivery should succeed");

        assert_eq!(log.commits(), vec![(0, 2)]);
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
        assert_eq!(log.commits(), vec![(0, 2)]);
    }

    #[tokio::test]
    async fn given_a_message_that_does_not_decode_when_invalid_messages_stop_should_stop_before_it()
    {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::new(&[(0, vec![br#"{"id":1}"#, b"{not json", br#"{"id":3}"#])]);

        let result = deliver_all(plugin_id, &mut log, test_delivery()).await;

        assert_eq!(stopped_partitions(result), [0]);
        assert!(
            consumed(plugin_id).is_empty(),
            "nothing of the batch reaches the sink"
        );
        assert!(log.commits().is_empty());
    }

    #[tokio::test]
    async fn given_a_message_that_does_not_decode_when_invalid_messages_skip_should_store_past_it()
    {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::new(&[(0, vec![br#"{"id":1}"#, b"{not json", br#"{"id":3}"#])]);
        let delivery = delivery(DeliveryOptions {
            invalid_messages: InvalidMessages::Skip,
            ..test_options()
        });

        deliver_all(plugin_id, &mut log, delivery)
            .await
            .expect("the delivery should skip the message");

        assert_eq!(consumed(plugin_id), vec![(vec![0, 2], 0)]);
        assert_eq!(log.commits(), vec![(0, 2)]);
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

        assert_eq!(stopped_partitions(result), [0]);
        assert!(log.seeks.is_empty(), "no seek to the next message");
    }

    #[tokio::test]
    async fn given_accepted_messages_when_shutdown_arrives_should_flush_and_store_them() {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        log.endless = true;
        let delivery = delivery(DeliveryOptions {
            // Longer than the test, so only the stop flushes.
            flush_interval: Some(Duration::from_secs(3600)),
            ..test_options()
        });
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
        assert_eq!(log.commits(), vec![(0, 2)]);
    }

    #[tokio::test]
    async fn given_a_flush_that_fails_at_shutdown_when_stopping_should_store_nothing() {
        let plugin_id = plugin(&[], &[1]);
        let mut log = FakeLog::json(&[(0, 3)]);
        let delivery = delivery(DeliveryOptions {
            flush_interval: Some(Duration::from_secs(3600)),
            ..test_options()
        });

        deliver_all(plugin_id, &mut log, delivery)
            .await
            .expect("a failed final flush still stops cleanly");

        assert_eq!(flushed(plugin_id).len(), 1);
        assert!(log.commits().is_empty());
    }

    #[tokio::test]
    async fn given_a_flush_interval_when_a_partition_reaches_the_message_cap_should_flush_at_once()
    {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 6)]);
        let delivery = delivery(DeliveryOptions {
            flush_interval: Some(Duration::from_secs(3600)),
            flush_max_messages: 4,
            ..test_options()
        });
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
        assert_eq!(log.commits(), vec![(0, 3), (0, 5)]);
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

        let (partitions, reason) = stopped(result);
        assert_eq!(partitions, [0]);
        assert!(reason.contains("offset: 1"), "{reason}");
        assert!(
            consumed(plugin_id).is_empty(),
            "nothing of the batch reaches the sink"
        );
        assert!(log.commits().is_empty());
    }

    #[tokio::test]
    async fn given_a_transform_failure_when_invalid_messages_skip_should_store_past_the_message() {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        let delivery = delivery(DeliveryOptions {
            invalid_messages: InvalidMessages::Skip,
            ..test_options()
        });

        deliver_with(
            plugin_id,
            &mut log,
            delivery,
            vec![Arc::new(FailingTransform { offset: 1 })],
        )
        .await
        .expect("the delivery should skip the message");

        assert_eq!(consumed(plugin_id), vec![(vec![0, 2], 0)]);
        assert_eq!(log.commits(), vec![(0, 2)]);
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

        let (partitions, reason) = stopped(result);
        assert_eq!(partitions, [0]);
        assert!(reason.contains("offset: 1"), "{reason}");
        assert!(
            consumed(plugin_id).is_empty(),
            "nothing of the batch reaches the sink"
        );
        assert!(log.commits().is_empty());
    }

    #[tokio::test]
    async fn given_a_serialization_failure_when_invalid_messages_skip_should_store_past_the_message()
     {
        let plugin_id = plugin(&[], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        let delivery = delivery(DeliveryOptions {
            invalid_messages: InvalidMessages::Skip,
            ..test_options()
        });

        deliver_with(
            plugin_id,
            &mut log,
            delivery,
            vec![Arc::new(ClearingTransform { offset: 1 })],
        )
        .await
        .expect("the delivery should skip the message");

        assert_eq!(consumed(plugin_id), vec![(vec![0, 2], 0)]);
        assert_eq!(log.commits(), vec![(0, 2)]);
    }

    #[tokio::test]
    async fn given_a_flush_refused_because_its_instance_closed_when_delivered_should_store_nothing()
    {
        // `-1` is what a plugin returns for an instance that is no longer open.
        let plugin_id = plugin(&[], &[-1]);
        let mut log = FakeLog::json(&[(0, 3)]);

        let result = deliver_all(plugin_id, &mut log, test_delivery()).await;

        assert_eq!(stopped_partitions(result), [0]);
        assert!(
            log.commits().is_empty(),
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

        assert_eq!(
            log.seeks,
            vec![(1, resume_at(1, 0, PartitionContext::default()))]
        );
        assert_eq!(
            log.commits(),
            vec![(0, 1)],
            "nothing is stored for the moved partition"
        );
    }

    #[tokio::test]
    async fn given_a_transient_failure_when_reading_again_should_keep_the_partition_history() {
        let plugin_id = plugin(&[1], &[]);
        let mut log = FakeLog::json(&[(0, 3)]);
        log.context = PartitionContext {
            incarnation: 7,
            owner_generation: 3,
            metadata_op: 11,
        };

        deliver_all(plugin_id, &mut log, test_delivery())
            .await
            .expect("the retry should deliver the batch");

        assert_eq!(log.seeks, vec![(0, resume_at(0, 0, log.context))]);
    }

    #[tokio::test]
    async fn given_one_transient_failure_in_each_of_two_partitions_when_each_recovers_should_not_stop()
     {
        // Partition 0 fails once and recovers, then partition 1 does the same, with no flush
        // in between.
        let plugin_id = plugin(&[1, 0, 1, 0], &[]);
        let mut log = FakeLog::json(&[(0, 2), (1, 2)]);
        let delivery = delivery(DeliveryOptions {
            retry_max_attempts: NonZeroU32::new(2),
            flush_interval: Some(Duration::from_secs(3600)),
            ..test_options()
        });

        deliver_all(plugin_id, &mut log, delivery)
            .await
            .expect("each failed call recovered within its attempts");

        assert_eq!(log.commits(), vec![(0, 1), (1, 1)]);
    }

    #[tokio::test]
    async fn given_a_partition_that_fails_again_after_delivering_the_failed_message_should_count_anew()
     {
        // The batch of offsets 0 and 1 fails and goes again. The batch of offsets 2 and 3 then
        // fails, and its retry hands over offsets 0 and 1 again before offsets 2 and 3.
        let plugin_id = plugin(&[1, 0, 1, 0, 0], &[]);
        let mut log = FakeLog::json(&[(0, 4)]);
        let delivery = delivery(DeliveryOptions {
            retry_max_attempts: NonZeroU32::new(2),
            flush_interval: Some(Duration::from_secs(3600)),
            ..test_options()
        });
        let (_shutdown, shutdown_rx) = watch::channel(());

        deliver(plugin_id, &mut log, delivery, Vec::new(), 2, shutdown_rx)
            .await
            .expect("each failed call recovered within its attempts");

        assert_eq!(log.commits(), vec![(0, 3)]);
    }

    #[tokio::test]
    async fn given_a_batch_that_keeps_failing_behind_accepted_messages_when_retried_should_stop() {
        // Offsets 0 and 1 are accepted, then offsets 2 and 3 fail. Each retry hands over
        // offsets 0 and 1 again, which succeed without delivering the failed message.
        let plugin_id = plugin(&[0, 1, 0, 1], &[]);
        let mut log = FakeLog::json(&[(0, 4)]);
        let delivery = delivery(DeliveryOptions {
            retry_max_attempts: NonZeroU32::new(2),
            flush_interval: Some(Duration::from_secs(3600)),
            ..test_options()
        });
        let (_shutdown, shutdown_rx) = watch::channel(());

        let result = deliver(plugin_id, &mut log, delivery, Vec::new(), 2, shutdown_rx).await;

        assert_eq!(stopped_partitions(result), [0]);
        assert_eq!(
            consumed(plugin_id).len(),
            4,
            "two attempts of offsets 2 and 3"
        );
        assert!(log.commits().is_empty());
    }

    #[tokio::test]
    async fn given_a_failed_flush_when_a_seek_of_its_retry_fails_should_not_flush_its_partitions_again()
     {
        let plugin_id = plugin(&[], &[1]);
        // Partition 1 reaches the cap and flushes both partitions, and the flush fails.
        let mut log = FakeLog::json(&[(0, 2), (1, 3)]);
        log.failing_seeks = vec![0];
        let delivery = delivery(DeliveryOptions {
            flush_interval: Some(Duration::from_secs(3600)),
            flush_max_messages: 3,
            ..test_options()
        });

        let result = deliver_all(plugin_id, &mut log, delivery).await;

        assert_eq!(stopped_partitions(result), [0]);
        assert_eq!(
            flushed(plugin_id).len(),
            1,
            "the failed flush may have dropped partition 1 too"
        );
        assert!(log.commits().is_empty());
    }

    #[tokio::test]
    async fn given_a_permanent_failure_when_another_partition_holds_accepted_messages_should_flush_and_store_it()
     {
        let plugin_id = plugin(&[0, 2], &[]);
        let mut log = FakeLog::json(&[(0, 2), (1, 3)]);
        let delivery = delivery(DeliveryOptions {
            flush_interval: Some(Duration::from_secs(3600)),
            ..test_options()
        });

        let result = deliver_all(plugin_id, &mut log, delivery).await;

        assert_eq!(stopped_partitions(result), [1]);
        assert_eq!(
            flushed(plugin_id),
            vec![vec![FlushPartition {
                partition_id: 0,
                offset: 1
            }]],
            "only the partition that did not fail flushes"
        );
        assert_eq!(log.commits(), vec![(0, 1)]);
    }

    #[tokio::test]
    async fn given_a_flush_that_fails_for_good_when_it_covered_two_partitions_should_name_both() {
        let plugin_id = plugin(&[], &[2]);
        // Partition 1 reaches the cap and flushes both partitions.
        let mut log = FakeLog::json(&[(0, 2), (1, 3)]);
        let delivery = delivery(DeliveryOptions {
            flush_interval: Some(Duration::from_secs(3600)),
            flush_max_messages: 3,
            ..test_options()
        });

        let result = deliver_all(plugin_id, &mut log, delivery).await;

        let (partitions, reason) = stopped(result);
        assert_eq!(partitions, [0, 1]);
        assert!(reason.contains("[(0, 0), (1, 0)]"), "{reason}");
        assert!(log.commits().is_empty());
    }

    #[test]
    fn given_delivery_settings_when_validated_should_refuse_zero_waits() {
        let valid = SinkDeliveryConfig {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            ..SinkDeliveryConfig::default()
        };
        assert!(DeliveryOptions::try_from(&valid).is_ok());

        for invalid in [
            SinkDeliveryConfig {
                retry_initial_backoff: "0s".to_owned(),
                ..valid.clone()
            },
            SinkDeliveryConfig {
                flush_interval: Some("0s".to_owned()),
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
