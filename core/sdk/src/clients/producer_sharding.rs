/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use crate::clients::producer::ProducerCoreBackend;
use crate::clients::producer_config::BackgroundConfig;
use crate::clients::producer_error_callback::ErrorCtx;
use iggy_common::{Identifier, IggyByteSize, IggyError, IggyMessage, Partitioning, Sizeable};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{OwnedSemaphorePermit, broadcast};
use tokio::task::JoinHandle;
use tracing::{debug, error};

/// A strategy for distributing messages across shards.
///
/// Implementors of this trait define how to choose a shard for a given batch of messages.
/// This allows customizing message routing based on message content, stream/topic identifiers,
/// or round-robin load balancing.
pub trait Sharding: Send + Sync + std::fmt::Debug + 'static {
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
#[derive(Default, Debug)]
pub struct BalancedSharding {
    counter: AtomicUsize,
}

impl Sharding for BalancedSharding {
    /// Picks the next shard in a round-robin fashion.
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

#[derive(Debug)]
pub struct ShardMessage {
    pub stream: Arc<Identifier>,
    pub topic: Arc<Identifier>,
    pub messages: Vec<IggyMessage>,
    pub partitioning: Option<Arc<Partitioning>>,
}

impl Sizeable for ShardMessage {
    fn get_size_bytes(&self) -> IggyByteSize {
        let mut total = IggyByteSize::new(0);
        total += self.stream.get_size_bytes();
        total += self.topic.get_size_bytes();
        for msg in &self.messages {
            total += msg.get_size_bytes();
        }
        total
    }
}

pub struct ShardMessageWithPermits {
    pub inner: ShardMessage,
    _bytes_permit: Option<OwnedSemaphorePermit>,
    _slot_permit: Option<OwnedSemaphorePermit>,
}

impl ShardMessageWithPermits {
    pub fn new(
        msg: ShardMessage,
        permit_bytes: OwnedSemaphorePermit,
        permit_slot: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            inner: msg,
            _bytes_permit: Some(permit_bytes),
            _slot_permit: Some(permit_slot),
        }
    }
}

pub struct Shard {
    tx: flume::Sender<ShardMessageWithPermits>,
    closed: Arc<AtomicBool>,
    pub(crate) _handle: JoinHandle<()>,
}

impl Shard {
    pub fn new(
        core: Arc<impl ProducerCoreBackend>,
        config: Arc<BackgroundConfig>,
        err_sender: flume::Sender<ErrorCtx>,
        mut stop_rx: broadcast::Receiver<()>,
    ) -> Self {
        let (tx, rx) = flume::bounded::<ShardMessageWithPermits>(256);
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
                                buffer_bytes += msg.inner.get_size_bytes().as_bytes_usize();
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

                                    Self::flush_buffer(&core, &mut buffer, &mut buffer_bytes, &err_sender).await;
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
                            Self::flush_buffer(&core, &mut buffer, &mut buffer_bytes, &err_sender).await;
                        }
                        last_flush = tokio::time::Instant::now();
                    }
                    _ = stop_rx.recv() => {
                        closed_clone.store(true, Ordering::Release);
                        if !buffer.is_empty() {
                            Self::flush_buffer(&core, &mut buffer, &mut buffer_bytes, &err_sender).await;
                        }
                        break;
                    }
                }
            }
        });

        Self {
            tx,
            closed,
            _handle: handle,
        }
    }

    async fn flush_buffer(
        core: &Arc<impl ProducerCoreBackend>,
        buffer: &mut Vec<ShardMessageWithPermits>,
        buffer_bytes: &mut usize,
        err_sender: &flume::Sender<ErrorCtx>,
    ) {
        for msg in buffer.drain(..) {
            let result = core
                .send_internal(
                    &msg.inner.stream,
                    &msg.inner.topic,
                    msg.inner.messages,
                    msg.inner.partitioning.clone(),
                )
                .await;

            if let Err(err) = result {
                if let IggyError::ProducerSendFailed {
                    failed,
                    cause,
                    stream_name,
                    topic_name,
                } = &err
                {
                    let ctx = ErrorCtx {
                        cause: cause.to_owned(),
                        stream: msg.inner.stream,
                        stream_name: stream_name.clone(),
                        topic: msg.inner.topic,
                        topic_name: topic_name.clone(),
                        partitioning: msg.inner.partitioning,
                        messages: failed.clone(),
                    };
                    let _ = err_sender.send_async(ctx).await;
                } else {
                    tracing::error!("background send failed: {err}");
                }
            }
        }
        *buffer_bytes = 0;
    }

    pub(crate) async fn send(&self, message: ShardMessageWithPermits) -> Result<(), IggyError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(IggyError::ProducerClosed);
        }

        self.tx.send_async(message).await.map_err(|e| {
            error!("Failed to send_async: {e}");
            IggyError::BackgroundSendError
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::producer::MockProducerCoreBackend;
    use bytes::Bytes;
    use iggy_common::IggyDuration;
    use std::time::Duration;
    use tokio::{sync::Semaphore, time::sleep};

    fn dummy_identifier() -> Arc<Identifier> {
        Arc::new(Identifier::numeric(1).unwrap())
    }

    fn dummy_message(size: usize) -> IggyMessage {
        IggyMessage::builder()
            .payload(Bytes::from(vec![0u8; size]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_shard_flushes_by_batch_length() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(10)
            .returning(|_, _, _, _| Box::pin(async { Ok(()) }));

        let bb = BackgroundConfig::builder()
            .batch_length(10)
            .linger_time(IggyDuration::new_from_secs(1))
            .batch_size(10_000);
        let config = Arc::new(bb.build());

        let (permit_bytes, permit_slot) = (
            Arc::new(Semaphore::new(100_000)),
            Arc::new(Semaphore::new(100)),
        );

        let (_stop_tx, stop_rx) = broadcast::channel(1);
        let shard = Shard::new(Arc::new(mock), config, flume::unbounded().0, stop_rx);

        for _ in 0..10 {
            let message = ShardMessage {
                stream: dummy_identifier(),
                topic: dummy_identifier(),
                messages: vec![dummy_message(1)],
                partitioning: None,
            };
            let wrapped = ShardMessageWithPermits::new(
                message,
                permit_bytes.clone().acquire_many_owned(1).await.unwrap(),
                permit_slot.clone().acquire_owned().await.unwrap(),
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
            .returning(|_, _, _, _| Box::pin(async { Ok(()) }));

        let bb = BackgroundConfig::builder()
            .batch_length(1000)
            .linger_time(IggyDuration::new_from_secs(1))
            .batch_size(10_000);
        let config = Arc::new(bb.build());

        let (permit_bytes, permit_slot) = (
            Arc::new(Semaphore::new(10_000)),
            Arc::new(Semaphore::new(100)),
        );

        let (_stop_tx, stop_rx) = broadcast::channel(1);
        let shard = Shard::new(Arc::new(mock), config, flume::unbounded().0, stop_rx);

        let message = ShardMessage {
            stream: dummy_identifier(),
            topic: dummy_identifier(),
            messages: vec![dummy_message(10_000)],
            partitioning: None,
        };
        let wrapped = ShardMessageWithPermits::new(
            message,
            permit_bytes
                .clone()
                .acquire_many_owned(10_000)
                .await
                .unwrap(),
            permit_slot.clone().acquire_owned().await.unwrap(),
        );
        shard.send(wrapped).await.unwrap();

        sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_shard_flushes_by_timeout() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Ok(()) }));

        let bb = BackgroundConfig::builder()
            .batch_length(10)
            .linger_time(IggyDuration::new(Duration::from_millis(50)))
            .batch_size(10_000);
        let config = Arc::new(bb.build());

        let (permit_bytes, permit_slot) = (
            Arc::new(Semaphore::new(10_000)),
            Arc::new(Semaphore::new(100)),
        );

        let (_stop_tx, stop_rx) = broadcast::channel(1);
        let shard = Shard::new(Arc::new(mock), config, flume::unbounded().0, stop_rx);

        let message = ShardMessage {
            stream: dummy_identifier(),
            topic: dummy_identifier(),
            messages: vec![dummy_message(1)],
            partitioning: None,
        };
        let wrapped = ShardMessageWithPermits::new(
            message,
            permit_bytes.clone().acquire_many_owned(1).await.unwrap(),
            permit_slot.clone().acquire_owned().await.unwrap(),
        );
        shard.send(wrapped).await.unwrap();

        sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_shard_forwards_error() {
        let mut mock = MockProducerCoreBackend::new();
        let error = IggyError::ProducerSendFailed {
            failed: Arc::new(vec![dummy_message(1)]),
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

        let (permit_bytes, permit_slot) = (
            Arc::new(Semaphore::new(10_000)),
            Arc::new(Semaphore::new(100)),
        );

        let (_stop_tx, stop_rx) = broadcast::channel(1);
        let shard = Shard::new(Arc::new(mock), config, err_tx, stop_rx);

        let message = ShardMessage {
            stream: dummy_identifier(),
            topic: dummy_identifier(),
            messages: vec![dummy_message(1)],
            partitioning: None,
        };
        let wrapped = ShardMessageWithPermits::new(
            message,
            permit_bytes.clone().acquire_many_owned(1).await.unwrap(),
            permit_slot.clone().acquire_owned().await.unwrap(),
        );
        shard.send(wrapped).await.unwrap();

        let err_ctx = err_rx.recv_async().await.unwrap();
        assert_eq!(err_ctx.cause, Box::new(IggyError::Error));
        assert_eq!(err_ctx.messages.len(), 1);
    }

    #[tokio::test]
    async fn test_shard_send_error_on_closed_channel() {
        let (tx, rx) = flume::bounded::<ShardMessageWithPermits>(1);
        drop(rx);

        let shard = Shard {
            tx,
            closed: Arc::new(AtomicBool::new(false)),
            _handle: tokio::spawn(async {}),
        };

        let (permit_bytes, permit_slot) = (
            Arc::new(Semaphore::new(10_000)),
            Arc::new(Semaphore::new(100)),
        );

        let message = ShardMessage {
            stream: dummy_identifier(),
            topic: dummy_identifier(),
            messages: vec![dummy_message(1)],
            partitioning: None,
        };
        let wrapped = ShardMessageWithPermits::new(
            message,
            permit_bytes.clone().acquire_many_owned(1).await.unwrap(),
            permit_slot.clone().acquire_owned().await.unwrap(),
        );

        let result = shard.send(wrapped).await;
        assert!(matches!(result, Err(IggyError::BackgroundSendError)));
    }
}
