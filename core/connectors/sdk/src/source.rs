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

use crate::log::{CallbackLayer, LogCallback};
use crate::{ConnectorState, Source, get_runtime};
use serde::de::DeserializeOwned;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

#[repr(C)]
pub struct RawMessage {
    pub offset: u64,
    pub headers_ptr: *const u8,
    pub headers_len: usize,
    pub payload_ptr: *const u8,
    pub payload_len: usize,
}

pub type HandleCallback = extern "C" fn(plugin_id: u32, callback: SendCallback) -> i32;

pub type SendCallback = extern "C" fn(
    plugin_id: u32,
    batch_id: u64,
    messages_ptr: *const u8,
    messages_len: usize,
) -> i32;

pub type BatchResultCallback = extern "C" fn(plugin_id: u32, batch_id: u64, result: u8) -> i32;

/// Delivery result for the single batch currently in flight from a source plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SourceBatchResult {
    /// The runtime sent the complete batch and persisted its candidate state.
    Ack = 0,
    /// The runtime could not send the batch or persist its candidate state.
    Nack = 1,
}

impl TryFrom<u8> for SourceBatchResult {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Ack),
            1 => Ok(Self::Nack),
            _ => Err(()),
        }
    }
}

struct PendingBatch {
    id: u64,
    result_sender: oneshot::Sender<SourceBatchResult>,
}

impl std::fmt::Debug for PendingBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingBatch")
            .field("id", &self.id)
            .finish()
    }
}

#[derive(Debug)]
pub struct SourceContainer<T: Source + std::fmt::Debug> {
    id: u32,
    source: Option<Arc<T>>,
    shutdown: Option<watch::Sender<()>>,
    task: Option<JoinHandle<()>>,
    pending_batch: Arc<Mutex<Option<PendingBatch>>>,
}

impl<T: Source + std::fmt::Debug + 'static> SourceContainer<T> {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            source: None,
            shutdown: None,
            task: None,
            pending_batch: Arc::new(Mutex::new(None)),
        }
    }

    /// # Safety
    /// Do not copy the configuration pointer
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn open<F, C>(
        &mut self,
        id: u32,
        config_ptr: *const u8,
        config_len: usize,
        state_ptr: *const u8,
        state_len: usize,
        log_callback: LogCallback,
        factory: F,
    ) -> i32
    where
        F: FnOnce(u32, C, Option<ConnectorState>) -> T,
        C: DeserializeOwned,
    {
        unsafe {
            _ = Registry::default()
                .with(CallbackLayer::new(log_callback))
                .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
                .try_init();
            let slice = std::slice::from_raw_parts(config_ptr, config_len);
            let Ok(config_str) = std::str::from_utf8(slice) else {
                error!("Failed to read configuration for source connector with ID: {id}");
                return -1;
            };

            let Ok(config) = serde_json::from_str(config_str) else {
                error!("Failed to parse configuration for source connector with ID: {id}");
                return -1;
            };

            let state = if state_ptr.is_null() {
                None
            } else {
                let state = std::slice::from_raw_parts(state_ptr, state_len);
                let state = ConnectorState(state.to_vec());
                Some(state)
            };

            let mut source = factory(id, config, state);
            let runtime = get_runtime();
            let result = runtime.block_on(source.open());
            self.id = id;
            self.source = Some(Arc::new(source));
            if result.is_ok() { 0 } else { 1 }
        }
    }

    /// # Safety
    /// This is safe to invoke
    pub unsafe fn close(&mut self) -> i32 {
        let Some(source) = self.source.take() else {
            error!(
                "Source connector with ID: {} is not initialized - cannot close.",
                self.id
            );
            return -1;
        };

        info!("Closing source connector with ID: {}...", self.id);
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(());
        }

        let runtime = get_runtime();
        if let Some(handle) = self.task.take() {
            let _ = runtime.block_on(handle);
        }

        let Ok(mut source) = Arc::try_unwrap(source) else {
            error!("Source connector with ID: {} was already closed.", self.id);
            return -1;
        };

        runtime.block_on(async {
            if let Err(err) = source.close().await {
                error!(
                    "Failed to close source connector with ID: {}. {err}",
                    self.id
                );
            }
        });
        info!("Closed source connector with ID: {}", self.id);
        0
    }

    /// # Safety
    /// Do not copy the pointer to the messages.
    pub unsafe fn handle(&mut self, callback: SendCallback) -> i32 {
        let Some(source) = self.source.as_ref() else {
            error!(
                "Source connector with ID: {} is not initialized - cannot handle.",
                self.id
            );
            return -1;
        };

        let runtime = get_runtime();
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let plugin_id = self.id;
        let source = Arc::clone(source);
        let pending_batch = Arc::clone(&self.pending_batch);
        let handle = runtime.spawn(async move {
            handle_messages(
                plugin_id,
                source,
                move |plugin_id, batch_id, messages_ptr, messages_len| {
                    callback(plugin_id, batch_id, messages_ptr, messages_len)
                },
                shutdown_rx,
                pending_batch,
            )
            .await;
        });

        self.shutdown = Some(shutdown_tx);
        self.task = Some(handle);
        0
    }

    #[doc(hidden)]
    pub fn complete_batch(&self, batch_id: u64, result: u8) -> i32 {
        let Ok(result) = SourceBatchResult::try_from(result) else {
            error!(
                "Invalid batch result: {result} for source connector with ID: {}",
                self.id
            );
            return -1;
        };

        complete_pending_batch(&self.pending_batch, batch_id, result, self.id)
    }
}

async fn handle_messages<T, F>(
    plugin_id: u32,
    source: Arc<T>,
    callback: F,
    mut shutdown: watch::Receiver<()>,
    pending_batch: Arc<Mutex<Option<PendingBatch>>>,
) where
    T: Source,
    F: Fn(u32, u64, *const u8, usize) -> i32,
{
    let mut batch_id = 1u64;
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!("Shutting down source connector with ID: {plugin_id}");
                break;
            }
            messages = source.poll() => {
                let messages = match messages {
                    Ok(messages) => messages,
                    Err(err) => {
                        error!("Failed to poll messages for source connector with ID: {plugin_id}. {err}");
                        continue;
                    }
                };

                let messages = match postcard::to_allocvec(&messages) {
                    Ok(messages) => messages,
                    Err(err) => {
                        error!("Failed to serialize messages for source connector with ID: {plugin_id}. {err}");
                        if !notify_source(&source, SourceBatchResult::Nack, plugin_id).await {
                            break;
                        }
                        continue;
                    }
                };

                let (result_sender, result_receiver) = oneshot::channel();
                {
                    let mut pending = lock_pending_batch(&pending_batch);
                    *pending = Some(PendingBatch {
                        id: batch_id,
                        result_sender,
                    });
                }

                if callback(plugin_id, batch_id, messages.as_ptr(), messages.len()) != 0 {
                    _ = complete_pending_batch(
                        &pending_batch,
                        batch_id,
                        SourceBatchResult::Nack,
                        plugin_id,
                    );
                }

                let (result, shutting_down) = tokio::select! {
                    biased;
                    result = result_receiver => {
                        (result.unwrap_or(SourceBatchResult::Nack), false)
                    },
                    _ = shutdown.changed() => {
                        _ = complete_pending_batch(
                            &pending_batch,
                            batch_id,
                            SourceBatchResult::Nack,
                            plugin_id,
                        );
                        (SourceBatchResult::Nack, true)
                    }
                };
                if !notify_source(&source, result, plugin_id).await {
                    break;
                }

                if shutting_down {
                    info!("Shutting down source connector with ID: {plugin_id}");
                    break;
                }

                batch_id = batch_id.wrapping_add(1);
                if batch_id == 0 {
                    batch_id = 1;
                }
            }
        }
    }
}

fn complete_pending_batch(
    pending_batch: &Mutex<Option<PendingBatch>>,
    batch_id: u64,
    result: SourceBatchResult,
    plugin_id: u32,
) -> i32 {
    let mut pending = lock_pending_batch(pending_batch);
    let Some(current) = pending.as_ref() else {
        error!("No batch is awaiting a result for source connector with ID: {plugin_id}");
        return -1;
    };
    if current.id != batch_id {
        error!(
            "Batch result ID mismatch for source connector with ID: {plugin_id}. Expected: {}, received: {batch_id}",
            current.id
        );
        return -1;
    }

    let Some(current) = pending.take() else {
        return -1;
    };
    if current.result_sender.send(result).is_err() {
        error!(
            "Failed to deliver batch result for source connector with ID: {plugin_id}, batch ID: {batch_id}"
        );
        return -1;
    }
    0
}

fn lock_pending_batch(
    pending_batch: &Mutex<Option<PendingBatch>>,
) -> MutexGuard<'_, Option<PendingBatch>> {
    pending_batch.lock().unwrap_or_else(PoisonError::into_inner)
}

async fn notify_source<T: Source>(
    source: &Arc<T>,
    result: SourceBatchResult,
    plugin_id: u32,
) -> bool {
    if let Err(err) = source.on_batch_result(result).await {
        error!("Failed to process {result:?} for source connector with ID: {plugin_id}. {err}");
        return false;
    }
    true
}

#[macro_export]
macro_rules! source_connector {
    ($type:ty) => {
        const _: fn() = || {
            fn assert_trait<T: $crate::Source>() {}
            assert_trait::<$type>();
        };

        use dashmap::DashMap;
        use std::sync::LazyLock;
        use $crate::LogCallback;
        use $crate::source::SendCallback;
        use $crate::source::SourceContainer;

        static INSTANCES: LazyLock<DashMap<u32, SourceContainer<$type>>> =
            LazyLock::new(DashMap::new);

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn iggy_source_open(
            id: u32,
            config_ptr: *const u8,
            config_len: usize,
            state_ptr: *const u8,
            state_len: usize,
            log_callback: LogCallback,
        ) -> i32 {
            if INSTANCES.contains_key(&id) {
                // Duplicate id: caller did not close before reopening. Without
                // this guard the existing entry would be silently overwritten,
                // discarding any in-flight buffered data and orphaning tasks.
                return -1;
            }

            let mut container = SourceContainer::new(id);
            let result = container.open(
                id,
                config_ptr,
                config_len,
                state_ptr,
                state_len,
                log_callback,
                <$type>::new,
            );
            INSTANCES.insert(id, container);
            result
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn iggy_source_handle(id: u32, callback: SendCallback) -> i32 {
            let Some(mut instance) = INSTANCES.get_mut(&id) else {
                tracing::error!(
                    "Source connector with ID: {id} was not found and cannot be handled."
                );
                return -1;
            };
            instance.handle(callback)
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        extern "C" fn iggy_source_batch_result(id: u32, batch_id: u64, result: u8) -> i32 {
            let Some(instance) = INSTANCES.get(&id) else {
                tracing::error!(
                    "Source connector with ID: {id} was not found and cannot complete batch {batch_id}."
                );
                return -1;
            };
            instance.complete_batch(batch_id, result)
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn iggy_source_close(id: u32) -> i32 {
            let Some(mut instance) = INSTANCES.remove(&id) else {
                tracing::error!(
                    "Source connector with ID: {id} was not found and cannot be closed."
                );
                return -1;
            };
            instance.1.close()
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        extern "C" fn iggy_source_version() -> *const std::ffi::c_char {
            static VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");
            VERSION.as_ptr() as *const std::ffi::c_char
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProducedMessages, Schema};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[derive(Debug, Default)]
    struct TestSource {
        polls: AtomicUsize,
        results: Mutex<Vec<SourceBatchResult>>,
        fail_batch_result: AtomicBool,
    }

    #[async_trait::async_trait]
    impl Source for TestSource {
        async fn open(&mut self) -> Result<(), crate::Error> {
            Ok(())
        }

        async fn poll(&self) -> Result<ProducedMessages, crate::Error> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Ok(ProducedMessages {
                schema: Schema::Raw,
                messages: Vec::new(),
                state: None,
            })
        }

        async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), crate::Error> {
            self.results
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(result);
            if self.fail_batch_result.load(Ordering::SeqCst) {
                return Err(crate::Error::Storage(
                    "failed to apply batch result".to_string(),
                ));
            }
            Ok(())
        }

        async fn close(&mut self) -> Result<(), crate::Error> {
            Ok(())
        }
    }

    #[test]
    fn given_batch_without_result_should_not_poll_again() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let source = Arc::new(TestSource::default());
            let pending_batch = Arc::new(Mutex::new(None));
            let pending_for_task = Arc::clone(&pending_batch);
            let (shutdown_sender, shutdown_receiver) = watch::channel(());
            let (batch_sender, mut batch_receiver) = mpsc::unbounded_channel();

            let source_for_task = Arc::clone(&source);
            let task = tokio::spawn(handle_messages(
                7,
                source_for_task,
                move |_, batch_id, _, _| {
                    batch_sender
                        .send(batch_id)
                        .expect("batch receiver should remain open");
                    0
                },
                shutdown_receiver,
                pending_for_task,
            ));

            let batch_id = tokio::time::timeout(Duration::from_secs(1), batch_receiver.recv())
                .await
                .expect("first batch was not sent")
                .expect("batch channel closed");
            assert_eq!(batch_id, 1);
            assert_eq!(source.polls.load(Ordering::SeqCst), 1);
            assert!(
                tokio::time::timeout(Duration::from_millis(50), batch_receiver.recv())
                    .await
                    .is_err(),
                "source polled again before the first batch was completed"
            );

            assert_eq!(
                complete_pending_batch(&pending_batch, batch_id, SourceBatchResult::Ack, 7),
                0
            );
            let next_batch_id = tokio::time::timeout(Duration::from_secs(1), batch_receiver.recv())
                .await
                .expect("source did not poll after ACK")
                .expect("batch channel closed");
            assert_eq!(next_batch_id, 2);

            shutdown_sender
                .send(())
                .expect("source task should remain active");
            task.await.expect("source task failed");
            assert_eq!(
                *source
                    .results
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
                vec![SourceBatchResult::Ack, SourceBatchResult::Nack]
            );
        });
    }

    #[test]
    fn given_nack_when_batch_is_pending_should_allow_redelivery() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let source = Arc::new(TestSource::default());
            let pending_batch = Arc::new(Mutex::new(None));
            let pending_for_task = Arc::clone(&pending_batch);
            let (shutdown_sender, shutdown_receiver) = watch::channel(());
            let (batch_sender, mut batch_receiver) = mpsc::unbounded_channel();

            let source_for_task = Arc::clone(&source);
            let task = tokio::spawn(handle_messages(
                9,
                source_for_task,
                move |_, batch_id, _, _| {
                    batch_sender
                        .send(batch_id)
                        .expect("batch receiver should remain open");
                    0
                },
                shutdown_receiver,
                pending_for_task,
            ));

            let batch_id = tokio::time::timeout(Duration::from_secs(1), batch_receiver.recv())
                .await
                .expect("first batch was not sent")
                .expect("batch channel closed");
            assert_eq!(
                complete_pending_batch(&pending_batch, batch_id, SourceBatchResult::Nack, 9),
                0
            );
            let next_batch_id = tokio::time::timeout(Duration::from_secs(1), batch_receiver.recv())
                .await
                .expect("source did not poll after NACK")
                .expect("batch channel closed");
            assert_eq!(next_batch_id, 2);

            shutdown_sender
                .send(())
                .expect("source task should remain active");
            task.await.expect("source task failed");
            assert_eq!(
                *source
                    .results
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
                vec![SourceBatchResult::Nack, SourceBatchResult::Nack]
            );
        });
    }

    #[test]
    fn given_mismatched_batch_id_should_reject_result() {
        let pending_batch = Mutex::new(None);
        let (result_sender, result_receiver) = oneshot::channel();
        *lock_pending_batch(&pending_batch) = Some(PendingBatch {
            id: 41,
            result_sender,
        });

        assert_eq!(
            complete_pending_batch(&pending_batch, 42, SourceBatchResult::Ack, 11),
            -1
        );
        assert_eq!(
            complete_pending_batch(&pending_batch, 41, SourceBatchResult::Ack, 11),
            0
        );

        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        assert_eq!(
            runtime
                .block_on(result_receiver)
                .expect("batch result sender was dropped"),
            SourceBatchResult::Ack
        );
    }

    #[test]
    fn given_batch_result_handler_failure_should_stop_polling() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let source = Arc::new(TestSource {
                fail_batch_result: AtomicBool::new(true),
                ..TestSource::default()
            });
            let pending_batch = Arc::new(Mutex::new(None));
            let pending_for_task = Arc::clone(&pending_batch);
            let (_shutdown_sender, shutdown_receiver) = watch::channel(());
            let (batch_sender, mut batch_receiver) = mpsc::unbounded_channel();

            let source_for_task = Arc::clone(&source);
            let task = tokio::spawn(handle_messages(
                13,
                source_for_task,
                move |_, batch_id, _, _| {
                    batch_sender
                        .send(batch_id)
                        .expect("batch receiver should remain open");
                    0
                },
                shutdown_receiver,
                pending_for_task,
            ));

            let batch_id = tokio::time::timeout(Duration::from_secs(1), batch_receiver.recv())
                .await
                .expect("first batch was not sent")
                .expect("batch channel closed");
            assert_eq!(
                complete_pending_batch(&pending_batch, batch_id, SourceBatchResult::Nack, 13),
                0
            );
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("source task did not stop after batch result failure")
                .expect("source task failed");

            assert_eq!(source.polls.load(Ordering::SeqCst), 1);
            assert_eq!(
                *source
                    .results
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
                vec![SourceBatchResult::Nack]
            );
        });
    }
}
