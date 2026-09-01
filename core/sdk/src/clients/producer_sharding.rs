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

use std::hash::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use iggy_common::{Identifier, IggyByteSize, IggyError, IggyMessage, Partitioning, Sizeable};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast};
use tokio::task::JoinHandle;
use tracing::{debug, error};

use crate::clients::producer::ProducerCoreBackend;
use crate::clients::producer_config::BackgroundConfig;
use crate::clients::producer_error_callback::ErrorCtx;

/// A strategy for distributing messages across shards.
///
/// Implementors of this trait define how to choose a shard for a given batch of messages.
/// This allows customizing message routing based on message content, stream/topic identifiers,
/// or round-robin load balancing.
///
/// [`pick_shard`](Self::pick_shard) must return an index smaller than `num_shards`. The dispatcher
/// normalizes a configured shard count of zero to one, so `num_shards` passed here is never zero.
/// Returning an out-of-range index makes dispatch panic when it indexes the shard list.
pub trait Sharding: Send + Sync + std::fmt::Debug + 'static {
    /// Chooses the zero-based shard index for one batch.
    fn pick_shard(
        &self,
        num_shards: usize,
        messages: &[IggyMessage],
        stream: &Identifier,
        topic: &Identifier,
    ) -> usize;
}

/// A simple round-robin sharding strategy.
/// Distributes messages evenly across all shards by incrementing an atomic counter.
///
/// **WARNING**: This strategy does NOT preserve message ordering across shards.
/// Messages to the same stream/topic may be processed out of order.
/// Use `OrderedSharding` if message ordering is required.
#[derive(Default, Debug)]
pub struct BalancedSharding {
    counter: AtomicUsize,
}

impl Sharding for BalancedSharding {
    fn pick_shard(
        &self,
        num_shards: usize,
        _: &[IggyMessage],
        _: &Identifier,
        _: &Identifier,
    ) -> usize {
        self.counter.fetch_add(1, Ordering::Relaxed) % num_shards
    }
}

/// A sharding strategy that preserves message ordering by routing all messages
/// for the same stream/topic combination to the same shard.
///
/// This ensures that messages sent to the same destination are processed
/// in the order they were dispatched, even when using multiple shards.
#[derive(Default, Debug)]
pub struct OrderedSharding;

impl Sharding for OrderedSharding {
    fn pick_shard(
        &self,
        num_shards: usize,
        _: &[IggyMessage],
        stream: &Identifier,
        topic: &Identifier,
    ) -> usize {
        let mut hasher = DefaultHasher::new();
        stream.hash(&mut hasher);
        topic.hash(&mut hasher);
        (hasher.finish() as usize) % num_shards
    }
}

/// One producer send after the dispatcher has selected its destination metadata.
///
/// A sharding strategy sees the messages, stream, and topic before this value is queued. The
/// optional partitioning value overrides the producer's configured partitioning for this send.
#[derive(Debug)]
pub struct ShardMessage {
    /// Target stream.
    pub stream: Arc<Identifier>,
    /// Target topic.
    pub topic: Arc<Identifier>,
    /// Messages carried by this send.
    pub messages: Vec<IggyMessage>,
    /// Per-send partitioning override, or `None` to use the producer configuration.
    pub partitioning: Option<Arc<Partitioning>>,
}

impl Sizeable for ShardMessage {
    fn get_size_bytes(&self) -> IggyByteSize {
        let mut total = IggyByteSize::new(0);
        total += self.stream.get_size_bytes();
        total += self.topic.get_size_bytes();
        if let Some(partitioning) = &self.partitioning {
            total += partitioning.get_size_bytes();
        }
        for message in &self.messages {
            total += message.get_size_bytes();
        }
        total
    }
}

/// A [`ShardMessage`] together with its charge against the dispatcher's byte budget.
///
/// The optional permit is acquired before the message enters a shard queue and remains owned by
/// this value until the worker finishes the write or drops the message. Keeping permits from merged
/// messages separate avoids the `u32` permit-count limit in Tokio's semaphore API.
pub struct ShardMessageWithPermit {
    /// Routed send and destination metadata.
    pub inner: ShardMessage,
    size_bytes: u64,
    bytes_permit: Option<OwnedSemaphorePermit>,
    merged_bytes_permits: Vec<OwnedSemaphorePermit>,
}

impl ShardMessageWithPermit {
    /// Wraps `msg` with its previously acquired byte-budget permit.
    ///
    /// `bytes_permit` is `None` when the dispatcher's byte budget is unbounded.
    pub fn new(msg: ShardMessage, bytes_permit: Option<OwnedSemaphorePermit>) -> Self {
        let size_bytes = msg.get_size_bytes().as_bytes_u64();
        Self {
            inner: msg,
            size_bytes,
            bytes_permit,
            merged_bytes_permits: Vec::new(),
        }
    }

    fn merge(&mut self, other: Self) {
        self.inner.messages.extend(other.inner.messages);
        self.size_bytes += other.size_bytes;
        // Tokio stores a merged permit count in a u32, so retain permits separately to avoid
        // overflowing when the buffer budget exceeds u32::MAX.
        self.merged_bytes_permits.extend(other.bytes_permit);
        self.merged_bytes_permits.extend(other.merged_bytes_permits);
    }
}

/// Represents one background worker of a
/// [`ProducerDispatcher`](crate::clients::producer_dispatcher::ProducerDispatcher),
/// together with the channel (queue) that feeds it.
///
/// Shards are owned and handled by the [`ProducerDispatcher`]. If the dispatcher can do everything
/// you want, there is no need to handle them directly.
///
/// Each shard owns a task that buffers sends routed to it, merges adjacent sends that share a
/// destination, and writes them through [`ProducerCoreBackend::send_internal`].
///
/// The dispatcher enqueues a batch by sending it on that channel, which returns once the batch is
/// queued. Shards are created and owned by the dispatcher, so they are rarely handled directly.
///
/// # Worker loop
///
/// The task selects over three sources:
///
/// - **An enqueued send.** [`ShardMessageWithPermit`]s are appended to the buffer, which is flushed
///   once it reaches [`batch_length`](BackgroundConfig::batch_length) queued sends or
///   [`batch_size`](BackgroundConfig::batch_size) reported bytes. Either threshold is disabled when
///   configured as `0`.
/// - **The periodic linger deadline.** At each
///   [`linger_time`](BackgroundConfig::linger_time) deadline, a non-empty buffer is flushed whether
///   or not either batching threshold was reached. This bounds how long a non-empty buffer waits for
///   another send before a flush check.
/// - **The stop broadcast** sent by the dispatcher on shutdown. Marks the shard closed
///   so later sends fail with [`IggyError::ProducerClosed`], drains what is still
///   queued, flushes once, then ends the loop.
///   Dropping the [`ProducerDispatcher`] instead of shutting it down provides no completion
///   guarantee and can lose buffered messages.
///
/// [`ProducerDispatcher`]: crate::clients::producer_dispatcher::ProducerDispatcher
pub struct Shard {
    tx: flume::Sender<ShardMessageWithPermit>,
    closed: Arc<AtomicBool>,
    pub(crate) handle: JoinHandle<()>,
}

impl Shard {
    /// Spawns the worker task described in the [`Shard`] type documentation.
    pub fn new(
        core: Arc<impl ProducerCoreBackend>,
        config: Arc<BackgroundConfig>,
        slots_permit: Arc<Semaphore>,
        err_sender: flume::Sender<ErrorCtx>,
        mut stop_rx: broadcast::Receiver<()>,
    ) -> Self {
        let (tx, rx) = flume::bounded::<ShardMessageWithPermit>(256);
        let closed = Arc::new(AtomicBool::new(false));

        let closed_clone = closed.clone();
        let handle = tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut buffer_bytes = 0;
            let mut last_flush = tokio::time::Instant::now();

            loop {
                let deadline = last_flush + config.linger_time.get_duration();
                tokio::select! {
                    maybe_msg = rx.recv_async() => {
                        match maybe_msg {
                            Ok(msg) => {
                                buffer_bytes += msg.size_bytes as usize;
                                buffer.push(msg);
                                debug!(
                                    buffer_len = buffer.len(),
                                    buffer_bytes,
                                    "Added message to buffer"
                                );

                                let exceed_batch_len = config.batch_length != 0 && buffer.len() >= config.batch_length;
                                let exceed_batch_size = config.batch_size != 0 && buffer_bytes >= config.batch_size;

                                if exceed_batch_len || exceed_batch_size {
                                    debug!(
                                        exceed_batch_len,
                                        exceed_batch_size,
                                        "Flushing buffer (trigger: batch_len={}, batch_size={})",
                                        exceed_batch_len,
                                        exceed_batch_size,
                                    );

                                    Self::flush_buffer(&core, &slots_permit, &mut buffer, &mut buffer_bytes, &err_sender).await;
                                    debug!(
                                        new_buffer_len = buffer.len(),
                                        new_buffer_bytes = buffer_bytes,
                                        "Buffer flushed"
                                    );

                                    last_flush = tokio::time::Instant::now();
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        if !buffer.is_empty() {
                            Self::flush_buffer(&core, &slots_permit, &mut buffer, &mut buffer_bytes, &err_sender).await;
                        }
                        last_flush = tokio::time::Instant::now();
                    }
                    _ = stop_rx.recv() => {
                        closed_clone.store(true, Ordering::Release);
                        while let Ok(msg) = rx.try_recv() {
                            buffer_bytes += msg.size_bytes as usize;
                            buffer.push(msg);
                        }
                        if !buffer.is_empty() {
                            Self::flush_buffer(&core, &slots_permit, &mut buffer, &mut buffer_bytes, &err_sender).await;
                        }
                        break;
                    }
                }
            }
        });

        Self { tx, closed, handle }
    }

    /// Drains the buffer and combines adjacent messages with the same destination.
    fn merge_batches(buffer: &mut Vec<ShardMessageWithPermit>) -> Vec<ShardMessageWithPermit> {
        let mut merged_batches: Vec<ShardMessageWithPermit> = Vec::with_capacity(buffer.len());
        for message in buffer.drain(..) {
            if let Some(last) = merged_batches.last_mut()
                && Self::same_destination(&last.inner, &message.inner)
                && last
                    .size_bytes
                    .checked_add(message.size_bytes)
                    .is_some_and(|size_bytes| size_bytes <= u32::MAX as u64)
            {
                last.merge(message);
                continue;
            }
            merged_batches.push(message);
        }
        merged_batches
    }

    async fn flush_buffer(
        core: &Arc<impl ProducerCoreBackend>,
        slots_permit: &Arc<Semaphore>,
        buffer: &mut Vec<ShardMessageWithPermit>,
        buffer_bytes: &mut usize,
        err_sender: &flume::Sender<ErrorCtx>,
    ) {
        if buffer.is_empty() {
            return;
        }

        for msg in Self::merge_batches(buffer) {
            let _slot_permit = slots_permit.acquire().await;

            let result = core
                .send_internal(
                    &msg.inner.stream,
                    &msg.inner.topic,
                    msg.inner.messages,
                    msg.inner.partitioning.clone(),
                )
                .await;

            if let Err(error) = result {
                if let IggyError::ProducerSendFailed {
                    failed,
                    committed,
                    cause,
                    stream_name,
                    topic_name,
                } = &error
                {
                    let ctx = ErrorCtx {
                        cause: cause.to_owned(),
                        stream: msg.inner.stream,
                        stream_name: stream_name.clone(),
                        topic: msg.inner.topic,
                        topic_name: topic_name.clone(),
                        partitioning: msg.inner.partitioning,
                        messages: failed.clone(),
                        committed: committed.clone(),
                    };
                    let _ = err_sender.send_async(ctx).await;
                } else {
                    error!("Background send failed. {error}");
                }
            }
        }
        *buffer_bytes = 0;
    }

    /// Queues one [`ShardMessageWithPermit`] on this worker.
    ///
    /// Returns [`IggyError::ProducerClosed`] after graceful shutdown has closed the shard, or
    /// [`IggyError::BackgroundSendError`] if its worker channel is disconnected.
    pub(crate) async fn send(&self, message: ShardMessageWithPermit) -> Result<(), IggyError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(IggyError::ProducerClosed);
        }

        self.tx.send_async(message).await.map_err(|e| {
            error!("Failed to send_async: {e}");
            IggyError::BackgroundSendError
        })
    }

    fn same_destination(first: &ShardMessage, second: &ShardMessage) -> bool {
        first.stream == second.stream
            && first.topic == second.topic
            && first.partitioning == second.partitioning
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::producer::{MockProducerCoreBackend, no_confirmations};
    use bytes::Bytes;
    use iggy_common::IggyDuration;
    use std::time::Duration;
    use tokio::time::sleep;

    fn dummy_identifier() -> Arc<Identifier> {
        Arc::new(Identifier::numeric(1).unwrap())
    }

    fn dummy_message(size: usize) -> IggyMessage {
        IggyMessage::builder()
            .payload(Bytes::from(vec![0u8; size]))
            .build()
            .unwrap()
    }

    async fn charged_batch(
        budget: &Arc<Semaphore>,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        payload_size: usize,
    ) -> ShardMessageWithPermit {
        let message = ShardMessage {
            stream,
            topic,
            messages: vec![dummy_message(payload_size)],
            partitioning: None,
        };
        let permit = budget
            .clone()
            .acquire_many_owned(message.get_size_bytes().as_bytes_u32())
            .await
            .unwrap();
        ShardMessageWithPermit::new(message, Some(permit))
    }

    #[tokio::test]
    async fn test_merge_batches_keeps_permits_of_merged_batches_charged() {
        let budget = Arc::new(Semaphore::new(10_000));
        let stream = dummy_identifier();
        let topic = dummy_identifier();

        let mut buffer = Vec::new();
        for _ in 0..3 {
            buffer.push(charged_batch(&budget, stream.clone(), topic.clone(), 10).await);
        }
        let charged = 10_000 - budget.available_permits();
        let merged = Shard::merge_batches(&mut buffer);

        assert!(buffer.is_empty());
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].inner.messages.len(), 3);
        assert_eq!(budget.available_permits(), 10_000 - charged);

        drop(merged);
        assert_eq!(budget.available_permits(), 10_000);
    }

    #[cfg(target_pointer_width = "64")]
    #[tokio::test]
    async fn test_merge_batches_keeps_more_than_u32_max_permits_charged() {
        let budget_size = u32::MAX as usize + 1;
        let budget = Arc::new(Semaphore::new(budget_size));
        let stream = dummy_identifier();
        let topic = dummy_identifier();
        let first_permit = budget.clone().acquire_many_owned(u32::MAX).await.unwrap();
        let second_permit = budget.clone().acquire_owned().await.unwrap();
        let mut buffer = vec![
            ShardMessageWithPermit::new(
                ShardMessage {
                    stream: stream.clone(),
                    topic: topic.clone(),
                    messages: vec![dummy_message(1)],
                    partitioning: None,
                },
                Some(first_permit),
            ),
            ShardMessageWithPermit::new(
                ShardMessage {
                    stream,
                    topic,
                    messages: vec![dummy_message(1)],
                    partitioning: None,
                },
                Some(second_permit),
            ),
        ];

        let merged = Shard::merge_batches(&mut buffer);

        assert_eq!(merged.len(), 1);
        assert_eq!(budget.available_permits(), 0);
        drop(merged);
        assert_eq!(budget.available_permits(), budget_size);
    }

    #[test]
    fn test_merge_batches_splits_batches_above_u32_max_bytes() {
        let stream = dummy_identifier();
        let topic = dummy_identifier();
        let mut first = ShardMessageWithPermit::new(
            ShardMessage {
                stream: stream.clone(),
                topic: topic.clone(),
                messages: vec![dummy_message(1)],
                partitioning: None,
            },
            None,
        );
        first.size_bytes = u32::MAX as u64;
        let mut second = ShardMessageWithPermit::new(
            ShardMessage {
                stream,
                topic,
                messages: vec![dummy_message(1)],
                partitioning: None,
            },
            None,
        );
        second.size_bytes = 1;
        let mut buffer = vec![first, second];

        let merged = Shard::merge_batches(&mut buffer);

        assert_eq!(merged.len(), 2);
    }

    #[tokio::test]
    async fn test_shard_keeps_budget_charged_until_merged_batch_is_written() {
        const BUDGET: usize = 10_000;

        let (write_started_tx, write_started_rx) = flume::unbounded::<()>();
        let (release_write_tx, release_write_rx) = flume::unbounded::<()>();

        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(1)
            .returning(move |_, _, _, _| {
                let write_started_tx = write_started_tx.clone();
                let release_write_rx = release_write_rx.clone();
                Box::pin(async move {
                    write_started_tx.send_async(()).await.unwrap();
                    release_write_rx.recv_async().await.unwrap();
                    Ok(no_confirmations())
                })
            });

        let bb = BackgroundConfig::builder()
            .batch_length(3)
            .batch_size(0)
            .linger_time(IggyDuration::new_from_secs(60));
        let config = Arc::new(bb.build());

        let budget = Arc::new(Semaphore::new(BUDGET));
        let slots_permit = Arc::new(Semaphore::new(100));

        let (stop_tx, stop_rx) = broadcast::channel(1);
        let shard = Shard::new(
            Arc::new(mock),
            config,
            slots_permit,
            flume::unbounded().0,
            stop_rx,
        );

        let stream = dummy_identifier();
        let topic = dummy_identifier();
        for _ in 0..3 {
            let batch = charged_batch(&budget, stream.clone(), topic.clone(), 100).await;
            shard.send(batch).await.unwrap();
        }
        let charged = BUDGET - budget.available_permits();
        assert!(charged > 0);

        tokio::time::timeout(Duration::from_secs(1), write_started_rx.recv_async())
            .await
            .expect("the merged write must start")
            .unwrap();
        assert_eq!(
            budget.available_permits(),
            BUDGET - charged,
            "the merged batch must hold every permit it absorbed until the write completes"
        );

        release_write_tx.send_async(()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.available_permits() != BUDGET {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the written batch must give its permits back");

        stop_tx.send(()).unwrap();
        shard.handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_shard_flushes_by_batch_length() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(10)
            .returning(|_, _, _, _| Box::pin(async { Ok(no_confirmations()) }));

        let bb = BackgroundConfig::builder()
            .batch_length(10)
            .linger_time(IggyDuration::new_from_secs(1))
            .batch_size(10_000);
        let config = Arc::new(bb.build());

        let permit_bytes = Arc::new(Semaphore::new(100_000));
        let slots_permit = Arc::new(Semaphore::new(100));

        let (_stop_tx, stop_rx) = broadcast::channel(1);
        let shard = Shard::new(
            Arc::new(mock),
            config,
            slots_permit,
            flume::unbounded().0,
            stop_rx,
        );

        for _ in 0..10 {
            let message = ShardMessage {
                stream: dummy_identifier(),
                topic: dummy_identifier(),
                messages: vec![dummy_message(1)],
                partitioning: None,
            };
            let wrapped = ShardMessageWithPermit::new(
                message,
                Some(permit_bytes.clone().acquire_many_owned(1).await.unwrap()),
            );
            shard.send(wrapped).await.unwrap();
        }

        sleep(Duration::from_millis(500)).await;
    }

    #[tokio::test]
    async fn test_shard_flushes_by_batch_size() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Ok(no_confirmations()) }));

        let bb = BackgroundConfig::builder()
            .batch_length(1000)
            .linger_time(IggyDuration::new_from_secs(1))
            .batch_size(10_000);
        let config = Arc::new(bb.build());

        let permit_bytes = Arc::new(Semaphore::new(10_000));
        let slots_permit = Arc::new(Semaphore::new(100));

        let (_stop_tx, stop_rx) = broadcast::channel(1);
        let shard = Shard::new(
            Arc::new(mock),
            config,
            slots_permit,
            flume::unbounded().0,
            stop_rx,
        );

        let message = ShardMessage {
            stream: dummy_identifier(),
            topic: dummy_identifier(),
            messages: vec![dummy_message(10_000)],
            partitioning: None,
        };
        let wrapped = ShardMessageWithPermit::new(
            message,
            Some(
                permit_bytes
                    .clone()
                    .acquire_many_owned(10_000)
                    .await
                    .unwrap(),
            ),
        );
        shard.send(wrapped).await.unwrap();

        sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_shard_flushes_by_timeout() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Ok(no_confirmations()) }));

        let bb = BackgroundConfig::builder()
            .batch_length(10)
            .linger_time(IggyDuration::new(Duration::from_millis(50)))
            .batch_size(10_000);
        let config = Arc::new(bb.build());

        let permit_bytes = Arc::new(Semaphore::new(10_000));
        let slots_permit = Arc::new(Semaphore::new(100));

        let (_stop_tx, stop_rx) = broadcast::channel(1);
        let shard = Shard::new(
            Arc::new(mock),
            config,
            slots_permit,
            flume::unbounded().0,
            stop_rx,
        );

        let message = ShardMessage {
            stream: dummy_identifier(),
            topic: dummy_identifier(),
            messages: vec![dummy_message(1)],
            partitioning: None,
        };
        let wrapped = ShardMessageWithPermit::new(
            message,
            Some(permit_bytes.clone().acquire_many_owned(1).await.unwrap()),
        );
        shard.send(wrapped).await.unwrap();

        sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_shard_forwards_error() {
        let mut mock = MockProducerCoreBackend::new();
        let error = IggyError::ProducerSendFailed {
            failed: Arc::new(vec![dummy_message(1)]),
            committed: Arc::new(Vec::new()),
            cause: Box::new(IggyError::Error),
            stream_name: "1".to_string(),
            topic_name: "1".to_string(),
        };

        mock.expect_send_internal().returning(move |_, _, _, _| {
            let err = error.clone();
            Box::pin(async move { Err(err) })
        });

        let (err_tx, err_rx) = flume::unbounded();
        let bb = BackgroundConfig::builder();
        let config = Arc::new(bb.build());

        let permit_bytes = Arc::new(Semaphore::new(10_000));
        let slots_permit = Arc::new(Semaphore::new(100));

        let (_stop_tx, stop_rx) = broadcast::channel(1);
        let shard = Shard::new(Arc::new(mock), config, slots_permit, err_tx, stop_rx);

        let message = ShardMessage {
            stream: dummy_identifier(),
            topic: dummy_identifier(),
            messages: vec![dummy_message(1)],
            partitioning: None,
        };
        let wrapped = ShardMessageWithPermit::new(
            message,
            Some(permit_bytes.clone().acquire_many_owned(1).await.unwrap()),
        );
        shard.send(wrapped).await.unwrap();

        let err_ctx = err_rx.recv_async().await.unwrap();
        assert_eq!(err_ctx.cause, Box::new(IggyError::Error));
        assert_eq!(err_ctx.messages.len(), 1);
    }

    #[tokio::test]
    async fn test_shard_send_error_on_closed_channel() {
        let (tx, rx) = flume::bounded::<ShardMessageWithPermit>(1);
        drop(rx);

        let shard = Shard {
            tx,
            closed: Arc::new(AtomicBool::new(false)),
            handle: tokio::spawn(async {}),
        };

        let permit_bytes = Arc::new(Semaphore::new(10_000));

        let message = ShardMessage {
            stream: dummy_identifier(),
            topic: dummy_identifier(),
            messages: vec![dummy_message(1)],
            partitioning: None,
        };
        let wrapped = ShardMessageWithPermit::new(
            message,
            Some(permit_bytes.clone().acquire_many_owned(1).await.unwrap()),
        );

        let result = shard.send(wrapped).await;
        assert!(matches!(result, Err(IggyError::BackgroundSendError)));
    }
}
