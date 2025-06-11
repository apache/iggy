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
use crate::clients::producer_config::{BackgroundConfig, BackpressureMode};
use crate::clients::producer_error_callback::ErrorCtx;
use crate::clients::producer_sharding::{Shard, ShardMessage, ShardMessageWithPermits};
use futures::FutureExt;
use iggy_common::{Identifier, IggyError, IggyMessage, Partitioning, Sizeable};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Semaphore, broadcast};
use tokio::task::JoinHandle;

pub struct ProducerDispatcher {
    shards: Vec<Shard>,
    config: Arc<BackgroundConfig>,
    closed: AtomicBool,
    slots_permit: Arc<Semaphore>,
    bytes_permit: Arc<Semaphore>,
    stop_tx: broadcast::Sender<()>,
    _join_handle: JoinHandle<()>,
}

impl ProducerDispatcher {
    pub fn new(core: Arc<impl ProducerCoreBackend>, config: BackgroundConfig) -> Self {
        let mut shards = Vec::with_capacity(config.num_shards);
        let config = Arc::new(config);

        let (err_tx, err_rx) = flume::unbounded::<ErrorCtx>();
        let err_callback = config.error_callback.clone();
        let (stop_tx, _) = broadcast::channel::<()>(1);

        let mut stop_rx = stop_tx.subscribe();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    maybe_message = err_rx.recv_async() => {
                        match maybe_message {
                            Ok(ctx) => {
                                if let Err(panic) = std::panic::AssertUnwindSafe(err_callback.call(ctx))
                                    .catch_unwind()
                                    .await
                                {
                                    tracing::error!("error_callback panicked: {:?}", panic);
                                }
                            }
                            Err(_) => break
                        }
                    }
                    _ = stop_rx.recv() => {
                        tracing::debug!("error-callback worker finished");
                        break
                    }
                }
            }
        });

        for _ in 0..config.num_shards {
            let stop_rx = stop_tx.subscribe();
            shards.push(Shard::new(
                core.clone(),
                config.clone(),
                err_tx.clone(),
                stop_rx,
            ));
        }

        let bytes_permit = {
            let bytes = config.max_buffer_size.as_bytes_usize();
            if bytes == 0 { usize::MAX } else { bytes }
        };

        let slot_permit = if config.max_in_flight == 0 {
            usize::MAX
        } else {
            config.max_in_flight
        };

        Self {
            shards,
            config,
            closed: AtomicBool::new(false),
            bytes_permit: Arc::new(Semaphore::new(bytes_permit)),
            slots_permit: Arc::new(Semaphore::new(slot_permit)),
            stop_tx,
            _join_handle: handle,
        }
    }

    pub async fn dispatch(
        &self,
        messages: Vec<IggyMessage>,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(IggyError::ProducerClosed);
        }

        let shard_message = ShardMessage {
            messages,
            stream,
            topic,
            partitioning,
        };
        let batch_bytes = shard_message.get_size_bytes();

        if batch_bytes > self.config.max_buffer_size {
            return Err(IggyError::BackgroundSendBufferOverflow);
        }

        let permit_bytes = match self
            .bytes_permit
            .clone()
            .try_acquire_many_owned(batch_bytes.as_bytes_u32())
        {
            Ok(perm) => perm,
            Err(_) => match self.config.failure_mode {
                BackpressureMode::FailImmediately => {
                    return Err(IggyError::BackgroundSendBufferOverflow);
                }
                BackpressureMode::Block => self
                    .bytes_permit
                    .clone()
                    .acquire_many_owned(batch_bytes.as_bytes_u32())
                    .await
                    .map_err(|_| IggyError::BackgroundSendError)?,
                BackpressureMode::BlockWithTimeout(timeout_dur) => {
                    match tokio::time::timeout(
                        timeout_dur.get_duration(),
                        self.bytes_permit
                            .clone()
                            .acquire_many_owned(batch_bytes.as_bytes_u32()),
                    )
                    .await
                    {
                        Ok(Ok(perm)) => perm,
                        Ok(Err(_)) => return Err(IggyError::BackgroundSendError),
                        Err(_) => return Err(IggyError::BackgroundSendTimeout),
                    }
                }
            },
        };

        let permit_slot = match self.slots_permit.clone().try_acquire_owned() {
            Ok(perm) => perm,
            Err(_) => match self.config.failure_mode {
                BackpressureMode::FailImmediately => {
                    drop(permit_bytes);
                    return Err(IggyError::BackgroundSendError);
                }
                BackpressureMode::Block => match self.slots_permit.clone().acquire_owned().await {
                    Ok(perm) => perm,
                    Err(_) => {
                        drop(permit_bytes);
                        return Err(IggyError::BackgroundSendError);
                    }
                },
                BackpressureMode::BlockWithTimeout(timeout_dur) => {
                    match tokio::time::timeout(
                        timeout_dur.get_duration(),
                        self.slots_permit.clone().acquire_owned(),
                    )
                    .await
                    {
                        Ok(Ok(perm)) => perm,
                        Ok(Err(_)) => {
                            drop(permit_bytes);
                            return Err(IggyError::BackgroundSendError);
                        }
                        Err(_) => {
                            drop(permit_bytes);
                            return Err(IggyError::BackgroundSendTimeout);
                        }
                    }
                }
            },
        };

        let shard_ix = self.config.sharding.pick_shard(
            self.shards.len(),
            &shard_message.messages,
            &shard_message.stream,
            &shard_message.topic,
        );
        debug_assert!(shard_ix < self.shards.len());
        let shard = &self.shards[shard_ix];

        shard
            .send(ShardMessageWithPermits::new(
                shard_message,
                permit_bytes,
                permit_slot,
            ))
            .await
    }

    pub async fn shutdown(mut self) {
        if self.closed.swap(true, Ordering::Relaxed) {
            return;
        }

        let _ = self.stop_tx.send(());

        for shard in self.shards.drain(..) {
            if let Err(e) = shard._handle.await {
                tracing::error!("shard panicked: {e:?}");
            }
        }

        if let Err(e) = self._join_handle.await {
            tracing::error!("error-worker panicked: {e:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::time::sleep;

    use crate::clients::producer::MockProducerCoreBackend;
    use crate::clients::producer_error_callback::ErrorCallback;
    use crate::clients::producer_sharding::Sharding;

    use super::*;

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
    async fn test_dispatch_successful() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Ok(()) }));

        let msg = dummy_message(5);
        let config = BackgroundConfig::builder()
            .max_buffer_size(100.into())
            .max_in_flight(10)
            .num_shards(1)
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        let result = dispatcher
            .dispatch(vec![msg], dummy_identifier(), dummy_identifier(), None)
            .await;

        sleep(Duration::from_millis(100)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_dispatch_fails_on_buffer_overflow_immediate() {
        let mock = MockProducerCoreBackend::new();

        let msg = dummy_message(200);
        let config = BackgroundConfig::builder()
            .max_buffer_size(100.into())
            .failure_mode(BackpressureMode::FailImmediately)
            .num_shards(1)
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        let result = dispatcher
            .dispatch(vec![msg], dummy_identifier(), dummy_identifier(), None)
            .await;

        assert!(matches!(
            result,
            Err(IggyError::BackgroundSendBufferOverflow)
        ));
    }

    #[tokio::test]
    async fn test_dispatch_times_out_on_block_with_timeout() {
        let mock = MockProducerCoreBackend::new();

        let msg = dummy_message(200);
        let config = BackgroundConfig::builder()
            .max_buffer_size(msg.get_size_bytes() + 100.into())
            .max_in_flight(1)
            .failure_mode(BackpressureMode::BlockWithTimeout(
                Duration::from_millis(50).into(),
            ))
            .num_shards(1)
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        let _keep = dispatcher
            .bytes_permit
            .clone()
            .acquire_many_owned(msg.get_size_bytes().as_bytes_u32() + 100)
            .await;

        let result = dispatcher
            .dispatch(vec![msg], dummy_identifier(), dummy_identifier(), None)
            .await;

        assert!(matches!(result, Err(IggyError::BackgroundSendTimeout)));
    }

    #[tokio::test]
    async fn test_dispatch_waits_and_succeeds_on_block_mode() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Ok(()) }));

        let msg = ShardMessage {
            stream: dummy_identifier(),
            topic: dummy_identifier(),
            messages: vec![dummy_message(5)],
            partitioning: None,
        };

        let config = BackgroundConfig::builder()
            .max_buffer_size(msg.get_size_bytes())
            .max_in_flight(1)
            .failure_mode(BackpressureMode::Block)
            .num_shards(1)
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        let _block = dispatcher
            .bytes_permit
            .clone()
            .acquire_many_owned(msg.get_size_bytes().as_bytes_u32())
            .await
            .unwrap();

        let msg_clone = ShardMessage {
            stream: msg.stream.clone(),
            topic: msg.topic.clone(),
            messages: msg.messages,
            partitioning: msg.partitioning.clone(),
        };

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(_block);
        });

        let result = dispatcher
            .dispatch(
                msg_clone.messages,
                msg_clone.topic,
                msg_clone.stream,
                msg_clone.partitioning,
            )
            .await;

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(result.is_ok());
    }

    #[derive(Clone, Debug)]
    struct TestSharding {
        called: Arc<AtomicUsize>,
    }

    impl Sharding for TestSharding {
        fn pick_shard(
            &self,
            num_shards: usize,
            _messages: &[IggyMessage],
            _stream: &Identifier,
            _topic: &Identifier,
        ) -> usize {
            self.called.fetch_add(1, Ordering::SeqCst);
            num_shards - 1
        }
    }

    #[derive(Clone, Debug)]
    struct TestErrorCallback {
        called: Arc<AtomicUsize>,
    }

    impl ErrorCallback for TestErrorCallback {
        fn call(&self, _ctx: ErrorCtx) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
            self.called.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn test_custom_sharding_and_error_callback() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(1)
            .returning(|_, _, _, _| {
                Box::pin(async {
                    Err(IggyError::ProducerSendFailed {
                        cause: "some_error".to_string(),
                        failed: Arc::new(vec![dummy_message(10)]),
                    })
                })
            });

        let sharding_called = Arc::new(AtomicUsize::new(0));
        let error_called = Arc::new(AtomicUsize::new(0));

        let config = BackgroundConfig::builder()
            .num_shards(1)
            .error_callback(Arc::new(Box::new(TestErrorCallback {
                called: error_called.clone(),
            })))
            .sharding(Box::new(TestSharding {
                called: sharding_called.clone(),
            }))
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        let result = dispatcher
            .dispatch(
                vec![dummy_message(10)],
                dummy_identifier(),
                dummy_identifier(),
                None,
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(result.is_ok());
        assert_eq!(sharding_called.load(Ordering::SeqCst), 1);
        assert_eq!(error_called.load(Ordering::SeqCst), 1);
    }
}
