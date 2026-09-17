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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    ConsumerOffsetClient, IggyError, MessageClient, PolledMessages, PollingKind, PollingStrategy,
    TopicClient,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};

use super::{IggyConsumer, ORDERING};
use crate::poll_routing::MAX_DEFERRED_CONNECTIONS;

const ASSIGNMENT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const DEFAULT_PREFETCH_BYTES: u32 = 16 * 1024 * 1024;
pub(crate) const DEFAULT_PREFETCH_MESSAGES: u32 = 16_000;

/// A reservation covers the whole possible reply, including work still in flight.
/// Keep it until the batch is drained: message slices can share its backing allocation.
pub(super) struct FetchCapacity {
    permit: Option<OwnedSemaphorePermit>,
    wake: Arc<Notify>,
}

impl Drop for FetchCapacity {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.wake.notify_one();
    }
}

pub(super) struct PrefetchedBatch {
    pub generation: u64,
    pub assignment: Option<(u64, u64)>,
    pub result: Result<PolledMessages, IggyError>,
    pub capacity: FetchCapacity,
}

pub(super) struct DeferredPolls {
    receiver: mpsc::Receiver<PrefetchedBatch>,
    task: JoinHandle<()>,
}

impl DeferredPolls {
    pub(super) fn start(consumer: &IggyConsumer) -> Self {
        let slots = consumer.prefetch_slots();
        let (sender, receiver) = mpsc::channel(slots);
        let worker = Worker::new(consumer, sender);
        Self {
            receiver,
            task: tokio::spawn(worker.run()),
        }
    }

    pub(super) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<PrefetchedBatch>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for DeferredPolls {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct CompletedPoll {
    partition: u32,
    strategy: Option<PollingStrategy>,
    result: Result<PolledMessages, IggyError>,
    capacity: FetchCapacity,
}

struct Worker {
    client: iggy_common::locking::IggyRwLock<crate::client_wrappers::client_wrapper::ClientWrapper>,
    stream: Arc<iggy_common::Identifier>,
    topic: Arc<iggy_common::Identifier>,
    consumer: Arc<iggy_common::Consumer>,
    name: String,
    standalone_partition: u32,
    is_group: bool,
    auto_join: bool,
    create_group: bool,
    joined: Arc<std::sync::atomic::AtomicBool>,
    can_poll: Arc<std::sync::atomic::AtomicBool>,
    generation: Arc<AtomicU64>,
    wake: Arc<Notify>,
    strategy: PollingStrategy,
    options: iggy_common::DeferredPollOptions,
    count: u32,
    auto_commit: bool,
    encryptor: Option<Arc<iggy_common::EncryptorKind>>,
    retry_interval: Duration,
    sender: mpsc::Sender<PrefetchedBatch>,
    capacity: Arc<Semaphore>,
    tasks: JoinSet<CompletedPoll>,
    active: BTreeSet<u32>,
    positions: BTreeMap<u32, PollingStrategy>,
    retry_at: BTreeMap<u32, Instant>,
    partitions: Vec<u32>,
    assignment: Option<(u64, u64)>,
    pending_error: Option<IggyError>,
    epoch: u64,
    cursor: Option<u32>,
}

impl Worker {
    fn new(consumer: &IggyConsumer, sender: mpsc::Sender<PrefetchedBatch>) -> Self {
        let slots = consumer.prefetch_slots();
        Self {
            client: consumer.client.clone(),
            stream: Arc::clone(&consumer.stream_id),
            topic: Arc::clone(&consumer.topic_id),
            consumer: Arc::clone(&consumer.consumer),
            name: consumer.consumer_name.clone(),
            standalone_partition: consumer.partition_id.unwrap_or_default(),
            is_group: consumer.is_consumer_group,
            auto_join: consumer.auto_join_consumer_group,
            create_group: consumer.create_consumer_group_if_not_exists,
            joined: Arc::clone(&consumer.joined_consumer_group),
            can_poll: Arc::clone(&consumer.can_poll),
            generation: Arc::clone(&consumer.fetch_generation),
            wake: Arc::clone(&consumer.fetch_notify),
            strategy: consumer.polling_strategy,
            options: consumer.poll_options,
            count: consumer.batch_length,
            auto_commit: consumer.auto_commit_after_polling,
            encryptor: consumer.encryptor.clone(),
            retry_interval: consumer.reconnection_retry_interval.get_duration(),
            sender,
            capacity: Arc::new(Semaphore::new(slots)),
            tasks: JoinSet::new(),
            active: BTreeSet::new(),
            positions: BTreeMap::new(),
            retry_at: BTreeMap::new(),
            partitions: Vec::new(),
            assignment: None,
            pending_error: None,
            epoch: consumer.fetch_generation.load(ORDERING),
            cursor: None,
        }
    }

    async fn run(mut self) {
        let mut refresh = tokio::time::interval(ASSIGNMENT_REFRESH_INTERVAL);
        refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            if self.sender.is_closed() {
                return;
            }
            if self.epoch != self.generation.load(ORDERING) {
                self.clear();
                self.epoch = self.generation.load(ORDERING);
                refresh.reset_immediately();
            }
            self.flush_error();
            self.schedule();
            let retry_at = self
                .retry_at
                .values()
                .min()
                .copied()
                .unwrap_or_else(|| Instant::now() + ASSIGNMENT_REFRESH_INTERVAL);
            tokio::select! {
                _ = self.sender.closed() => return,
                _ = self.wake.notified() => {},
                _ = tokio::time::sleep_until(retry_at), if !self.retry_at.is_empty() => {
                    self.retry_at.retain(|_, deadline| *deadline > Instant::now());
                },
                _ = refresh.tick() => {
                    if !self.can_poll.load(ORDERING) { continue; }
                    let result = tokio::time::timeout(self.options.request_timeout.get_duration(), self.refresh()).await
                        .unwrap_or(Err(IggyError::TransientNotCommitted));
                    if let Err(error) = result {
                        self.invalidate();
                        self.report(error);
                        refresh.reset_after(self.retry_interval);
                    } else {
                        refresh.reset();
                    }
                },
                Some(completed) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    match completed {
                        Ok(completed) => {
                            self.complete(completed);
                            if self.partitions.is_empty() {
                                refresh.reset_after(self.retry_interval);
                            }
                        },
                        Err(_) => {
                            self.invalidate();
                            self.report(IggyError::ClientShutdown);
                            return;
                        }
                    }
                }
            }
        }
    }

    fn clear(&mut self) {
        self.tasks = JoinSet::new();
        self.active.clear();
        self.positions.clear();
        self.retry_at.clear();
        self.partitions.clear();
        self.assignment = None;
        self.pending_error = None;
    }

    fn invalidate(&mut self) {
        self.epoch = self.generation.fetch_add(1, ORDERING) + 1;
        self.clear();
    }

    async fn refresh(&mut self) -> Result<(), IggyError> {
        if self.is_group && self.auto_join && !self.joined.load(ORDERING) {
            IggyConsumer::initialize_consumer_group(
                self.client.clone(),
                self.create_group,
                Arc::clone(&self.stream),
                Arc::clone(&self.topic),
                Arc::clone(&self.consumer),
                &self.name,
                Arc::clone(&self.joined),
            )
            .await?;
        }
        let (session, generation, mut partitions) = if self.is_group {
            self.client
                .read()
                .await
                .deferred_poll_partitions(&self.stream, &self.topic, &self.consumer.id, true)
                .await?
        } else {
            (0, 0, vec![self.standalone_partition])
        };
        partitions.sort_unstable();
        if self.assignment != Some((session, generation)) || self.partitions != partitions {
            self.invalidate();
            self.partitions = partitions;
            self.assignment = Some((session, generation));
        }
        Ok(())
    }

    fn reserve(&self) -> Option<FetchCapacity> {
        Arc::clone(&self.capacity)
            .try_acquire_owned()
            .ok()
            .map(|permit| FetchCapacity {
                permit: Some(permit),
                wake: Arc::clone(&self.wake),
            })
    }

    fn report(&mut self, error: IggyError) {
        if matches!(error, IggyError::ConsumerGroupMemberNotFound(..)) {
            self.joined.store(false, ORDERING);
            if self.auto_join {
                return;
            }
        }
        if matches!(
            error,
            IggyError::Disconnected | IggyError::Unauthenticated | IggyError::StaleClient
        ) {
            self.can_poll.store(false, ORDERING);
            self.joined.store(false, ORDERING);
        }
        self.pending_error = Some(error);
        self.flush_error();
    }

    fn flush_error(&mut self) {
        let Some(error) = self.pending_error.take() else {
            return;
        };
        let Some(capacity) = self.reserve() else {
            self.pending_error = Some(error);
            return;
        };
        let _ = self.sender.try_send(PrefetchedBatch {
            generation: self.epoch,
            assignment: self.assignment,
            result: Err(error),
            capacity,
        });
    }

    fn schedule(&mut self) {
        if !self.can_poll.load(ORDERING) || self.partitions.is_empty() {
            return;
        }
        let start = self.cursor.map_or(0, |cursor| {
            self.partitions.partition_point(|id| *id <= cursor)
        });
        for index in 0..self.partitions.len() {
            if self.tasks.len() >= MAX_DEFERRED_CONNECTIONS {
                break;
            }
            let partition = self.partitions[(start + index) % self.partitions.len()];
            if self.active.contains(&partition) || self.retry_at.contains_key(&partition) {
                continue;
            }
            let Some(capacity) = self.reserve() else {
                break;
            };
            let client = self.client.clone();
            let stream = Arc::clone(&self.stream);
            let topic = Arc::clone(&self.topic);
            let consumer = Arc::clone(&self.consumer);
            let initial = self.strategy;
            let mut strategy = self.positions.get(&partition).copied();
            let count = self.count;
            let auto_commit = self.auto_commit;
            let options = self.options;
            let encryptor = self.encryptor.clone();
            self.tasks.spawn(async move {
                let started = Instant::now();
                let result = tokio::time::timeout(options.request_timeout.get_duration(), async {
                    let client = client.read().await;
                    let resolved = match strategy {
                        Some(strategy) => strategy,
                        None => match initial.kind {
                            PollingKind::Next => {
                                let stored = client
                                    .get_consumer_offset(
                                        &consumer,
                                        &stream,
                                        &topic,
                                        Some(partition),
                                    )
                                    .await?;
                                let offset = stored
                                    .map(|offset| {
                                        offset
                                            .stored_offset
                                            .checked_add(1)
                                            .ok_or(IggyError::InvalidOffset(offset.stored_offset))
                                    })
                                    .transpose()?
                                    .unwrap_or_default();
                                PollingStrategy::offset(offset)
                            }
                            PollingKind::First => PollingStrategy::offset(0),
                            PollingKind::Last => {
                                let details = client.get_topic(&stream, &topic).await?;
                                let head = details
                                    .as_ref()
                                    .and_then(|topic| {
                                        topic.partitions.iter().find(|entry| entry.id == partition)
                                    })
                                    .ok_or_else(|| {
                                        IggyError::PartitionNotFound(
                                            partition as usize,
                                            (*topic).clone(),
                                            (*stream).clone(),
                                        )
                                    })?;
                                PollingStrategy::offset(
                                    head.current_offset.saturating_sub(u64::from(count) - 1),
                                )
                            }
                            _ => initial,
                        },
                    };
                    strategy = Some(resolved);
                    let mut result = client
                        .poll_messages_deferred(
                            &stream,
                            &topic,
                            Some(partition),
                            &consumer,
                            &resolved,
                            count,
                            auto_commit,
                            options.remaining(started.elapsed())?,
                        )
                        .await?;
                    if result.count > count || result.messages.len() > count as usize {
                        return Err(IggyError::InvalidMessagesCount);
                    }
                    if let Some(encryptor) = encryptor {
                        for message in &mut result.messages {
                            message.payload = Bytes::from(encryptor.decrypt(&message.payload)?);
                            message.header.payload_length = message.payload.len() as u32;
                            if let Some(headers) = &message.user_headers {
                                let headers = encryptor.decrypt(headers)?;
                                message.header.user_headers_length = headers.len() as u32;
                                message.user_headers = Some(Bytes::from(headers));
                            }
                        }
                    }
                    Ok(result)
                })
                .await
                .unwrap_or(Err(IggyError::TransientNotCommitted));
                CompletedPoll {
                    partition,
                    strategy,
                    result,
                    capacity,
                }
            });
            self.active.insert(partition);
            self.cursor = Some(partition);
        }
    }

    fn complete(&mut self, mut completed: CompletedPoll) {
        self.active.remove(&completed.partition);
        if self.epoch != self.generation.load(ORDERING) {
            return;
        }
        if let Some(strategy) = completed.strategy {
            self.positions.insert(completed.partition, strategy);
        }
        if let Ok(result) = &completed.result {
            if result.partition_id == iggy_common::RESYNC_REQUIRED_PARTITION_SENTINEL {
                self.invalidate();
                self.wake.notify_one();
                return;
            }
            if let Some(last) = result.messages.last() {
                match last.header.offset.checked_add(1) {
                    Some(next) => {
                        self.positions
                            .insert(completed.partition, PollingStrategy::offset(next));
                    }
                    None => {
                        completed.result = Err(IggyError::InvalidOffset(last.header.offset));
                    }
                }
            } else {
                return;
            }
        }
        if let Err(error) = completed.result {
            if matches!(
                error,
                IggyError::ConsumerGroupMemberNotFound(..)
                    | IggyError::Disconnected
                    | IggyError::Unauthenticated
                    | IggyError::StaleClient
            ) {
                self.invalidate();
                self.report(error);
                return;
            }
            self.retry_at
                .insert(completed.partition, Instant::now() + self.retry_interval);
            completed.result = Err(error);
        }
        let _ = self.sender.try_send(PrefetchedBatch {
            generation: self.epoch,
            assignment: self.assignment,
            result: completed.result,
            capacity: completed.capacity,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_wrappers::client_wrapper::ClientWrapper;
    use crate::clients::consumer_builder::IggyConsumerBuilder;
    use crate::tcp::tcp_client::TcpClient;
    use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
    use iggy_common::{Consumer, DeferredPollOptions, Identifier};

    #[tokio::test(start_paused = true)]
    async fn byte_and_message_reservations_cover_inflight_and_queued_replies() {
        const BATCH_COUNT: u32 = 10;
        const REPLY_BYTES: u32 = 1024;
        for (bytes, messages) in [
            (REPLY_BYTES * 2, BATCH_COUNT * 10),
            (REPLY_BYTES * 10, BATCH_COUNT * 2),
        ] {
            let consumer = IggyConsumerBuilder::new(
                IggyRwLock::new(ClientWrapper::Tcp(TcpClient::default())),
                "reader".into(),
                Consumer::default(),
                Identifier::numeric(1).unwrap(),
                Identifier::numeric(1).unwrap(),
                None,
                None,
            )
            .batch_length(BATCH_COUNT)
            .poll_options(DeferredPollOptions {
                max_bytes: REPLY_BYTES,
                ..Default::default()
            })
            .prefetch_bytes(bytes)
            .prefetch_messages(messages)
            .build();
            let client = consumer.client.clone();
            let _blocked_connection = client.write().await;
            let (sender, mut receiver) = mpsc::channel(consumer.prefetch_slots());
            let mut worker = Worker::new(&consumer, sender);
            worker.partitions = vec![0, 1, 2, 3];
            worker.schedule();
            assert_eq!(worker.tasks.len(), 2, "reserve before sending");
            assert_eq!(worker.capacity.available_permits(), 0);
            while let Some(completed) = worker.tasks.join_next().await {
                let completed = completed.unwrap();
                assert!(matches!(
                    completed.result,
                    Err(IggyError::TransientNotCommitted)
                ));
                worker.complete(completed);
            }
            assert_eq!(
                receiver.len(),
                2,
                "completed failures retain their bounded queue reservations"
            );
            worker.schedule();
            assert!(
                worker.tasks.is_empty(),
                "a stalled application stops new fetches"
            );
            drop(receiver.recv().await.unwrap());
            worker.retry_at.clear();
            worker.schedule();
            assert_eq!(
                worker.tasks.len(),
                1,
                "draining releases exactly one full-batch reservation"
            );
            while let Some(completed) = worker.tasks.join_next().await {
                worker.complete(completed.unwrap());
            }
            worker.invalidate();
            worker.report(IggyError::Disconnected);
            assert!(
                worker.pending_error.is_some(),
                "retain control errors under backpressure"
            );
            drop(receiver.recv().await.unwrap());
            worker.flush_error();
            assert_ne!(receiver.recv().await.unwrap().generation, worker.epoch);
            let error = receiver.recv().await.unwrap();
            assert_eq!(error.generation, worker.epoch);
            assert!(matches!(error.result, Err(IggyError::Disconnected)));
        }
    }
}
