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
use crate::retry::exponential_backoff;
use crate::{ConnectorState, Source, get_runtime};
use serde::de::DeserializeOwned;
use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError,
    atomic::{AtomicU32, Ordering},
};
use std::time::Duration;
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
};
use tracing::{error, info, warn};
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

const BATCH_RESULT_TIMEOUT: Duration = Duration::from_secs(30);
const NACK_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_NACK_RETRY_DELAY: Duration = Duration::from_secs(5);
const MAX_CONSECUTIVE_NACKS: u32 = 5;

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

#[derive(Debug)]
struct PendingBatch {
    id: u64,
    result_sender: oneshot::Sender<BatchCompletion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchCompletion {
    Applied(SourceBatchResult),
    Stop,
}

#[derive(Debug, Clone, Copy)]
struct BatchPolicy {
    result_timeout: Duration,
    nack_retry_delay: Duration,
    max_nack_retry_delay: Duration,
    max_consecutive_nacks: u32,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self {
            result_timeout: BATCH_RESULT_TIMEOUT,
            nack_retry_delay: NACK_RETRY_DELAY,
            max_nack_retry_delay: MAX_NACK_RETRY_DELAY,
            max_consecutive_nacks: MAX_CONSECUTIVE_NACKS,
        }
    }
}

#[derive(Debug)]
pub struct SourceContainer<T: Source + std::fmt::Debug> {
    id: u32,
    source: Option<Arc<T>>,
    shutdown: Option<watch::Sender<()>>,
    task: Option<JoinHandle<()>>,
    pending_batch: Arc<Mutex<Option<PendingBatch>>>,
    consecutive_nacks: Arc<AtomicU32>,
}

impl<T: Source + std::fmt::Debug + 'static> SourceContainer<T> {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            source: None,
            shutdown: None,
            task: None,
            pending_batch: Arc::new(Mutex::new(None)),
            consecutive_nacks: Arc::new(AtomicU32::new(0)),
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
            // Only a status code crosses the FFI boundary, so log the cause here
            // or it is lost: the runtime can then report no more than "plugin
            // initialization failed", leaving an operator with a skipped
            // connector and nothing to explain why.
            match result {
                Ok(()) => 0,
                Err(error) => {
                    error!("Failed to open source connector with ID: {id}. {error}");
                    1
                }
            }
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
        let consecutive_nacks = Arc::clone(&self.consecutive_nacks);
        let handle = runtime.spawn(async move {
            handle_messages(
                plugin_id,
                source,
                move |plugin_id, batch_id, messages_ptr, messages_len| {
                    callback(plugin_id, batch_id, messages_ptr, messages_len)
                },
                shutdown_rx,
                pending_batch,
                consecutive_nacks,
                BatchPolicy::default(),
            )
            .await;
        });

        self.shutdown = Some(shutdown_tx);
        self.task = Some(handle);
        0
    }

    #[doc(hidden)]
    pub fn complete_batch(&self, batch_id: u64, result: u8) -> i32 {
        let Some(source) = self.source.as_ref() else {
            error!("Source connector with ID: {} is not initialized.", self.id);
            return -1;
        };

        complete_pending_batch(
            &self.pending_batch,
            source,
            &self.consecutive_nacks,
            batch_id,
            result,
            self.id,
            MAX_CONSECUTIVE_NACKS,
        )
    }
}

async fn handle_messages<T, F>(
    plugin_id: u32,
    source: Arc<T>,
    callback: F,
    mut shutdown: watch::Receiver<()>,
    pending_batch: Arc<Mutex<Option<PendingBatch>>>,
    consecutive_nacks: Arc<AtomicU32>,
    policy: BatchPolicy,
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
                        if matches!(
                            apply_batch_result(
                                &source,
                                &consecutive_nacks,
                                SourceBatchResult::Nack,
                                plugin_id,
                                policy.max_consecutive_nacks,
                            )
                            .await,
                            BatchCompletion::Stop
                        ) {
                            break;
                        }
                        sleep_after_nack(&consecutive_nacks, policy).await;
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

                let callback_result = callback(plugin_id, batch_id, messages.as_ptr(), messages.len());
                drop(messages);

                if callback_result != 0 {
                    if !clear_pending_batch(&pending_batch, batch_id, plugin_id) {
                        break;
                    }
                    let completion = apply_batch_result(
                        &source,
                        &consecutive_nacks,
                        SourceBatchResult::Nack,
                        plugin_id,
                        policy.max_consecutive_nacks,
                    )
                    .await;
                    if matches!(completion, BatchCompletion::Stop) {
                        break;
                    }
                    sleep_after_nack(&consecutive_nacks, policy).await;
                    batch_id += 1;
                    continue;
                }

                let (completion, shutting_down) = tokio::select! {
                    biased;
                    result = result_receiver => {
                        (result.unwrap_or(BatchCompletion::Stop), false)
                    },
                    _ = shutdown.changed() => {
                        let completion = if clear_pending_batch(&pending_batch, batch_id, plugin_id) {
                            apply_batch_result(
                                &source,
                                &consecutive_nacks,
                                SourceBatchResult::Nack,
                                plugin_id,
                                policy.max_consecutive_nacks,
                            ).await
                        } else {
                            BatchCompletion::Stop
                        };
                        (completion, true)
                    },
                    _ = tokio::time::sleep(policy.result_timeout) => {
                        warn!(
                            "Timed out waiting for batch result for source connector with ID: {plugin_id}, batch ID: {batch_id}"
                        );
                        let completion = if clear_pending_batch(&pending_batch, batch_id, plugin_id) {
                            apply_batch_result(
                                &source,
                                &consecutive_nacks,
                                SourceBatchResult::Nack,
                                plugin_id,
                                policy.max_consecutive_nacks,
                            ).await
                        } else {
                            BatchCompletion::Stop
                        };
                        (completion, false)
                    }
                };

                if matches!(completion, BatchCompletion::Stop) {
                    break;
                }

                if shutting_down {
                    info!("Shutting down source connector with ID: {plugin_id}");
                    break;
                }

                if matches!(
                    completion,
                    BatchCompletion::Applied(SourceBatchResult::Nack)
                ) {
                    sleep_after_nack(&consecutive_nacks, policy).await;
                }
                batch_id += 1;
            }
        }
    }
}

fn complete_pending_batch<T>(
    pending_batch: &Mutex<Option<PendingBatch>>,
    source: &Arc<T>,
    consecutive_nacks: &Arc<AtomicU32>,
    batch_id: u64,
    result_code: u8,
    plugin_id: u32,
    max_consecutive_nacks: u32,
) -> i32
where
    T: Source + 'static,
{
    let (result, invalid_result) = match SourceBatchResult::try_from(result_code) {
        Ok(result) => (result, false),
        Err(()) => {
            error!(
                "Invalid batch result: {result_code} for source connector with ID: {plugin_id}; treating it as Nack"
            );
            (SourceBatchResult::Nack, true)
        }
    };

    let Some(current) = take_pending_batch(pending_batch, batch_id, plugin_id) else {
        return -1;
    };

    let completion = get_runtime().block_on(apply_batch_result(
        source,
        consecutive_nacks,
        result,
        plugin_id,
        max_consecutive_nacks,
    ));
    if current.result_sender.send(completion).is_err() {
        error!(
            "Failed to deliver batch result for source connector with ID: {plugin_id}, batch ID: {batch_id}"
        );
        return -1;
    }

    if invalid_result || matches!(completion, BatchCompletion::Stop) {
        -1
    } else {
        0
    }
}

fn take_pending_batch(
    pending_batch: &Mutex<Option<PendingBatch>>,
    batch_id: u64,
    plugin_id: u32,
) -> Option<PendingBatch> {
    let mut pending = lock_pending_batch(pending_batch);
    let Some(current) = pending.as_ref() else {
        error!("No batch is awaiting a result for source connector with ID: {plugin_id}");
        return None;
    };
    if current.id != batch_id {
        error!(
            "Batch result ID mismatch for source connector with ID: {plugin_id}. Expected: {}, received: {batch_id}",
            current.id
        );
        return None;
    }

    pending.take_if(|current| current.id == batch_id)
}

fn clear_pending_batch(
    pending_batch: &Mutex<Option<PendingBatch>>,
    batch_id: u64,
    plugin_id: u32,
) -> bool {
    let mut pending = lock_pending_batch(pending_batch);
    if let Some(current) = pending.as_ref()
        && current.id != batch_id
    {
        error!(
            "Batch result ID mismatch for source connector with ID: {plugin_id}. Expected: {}, received: {batch_id}",
            current.id
        );
        return false;
    }
    pending.take_if(|current| current.id == batch_id).is_some()
}

fn lock_pending_batch(
    pending_batch: &Mutex<Option<PendingBatch>>,
) -> MutexGuard<'_, Option<PendingBatch>> {
    pending_batch.lock().unwrap_or_else(PoisonError::into_inner)
}

async fn apply_batch_result<T: Source>(
    source: &Arc<T>,
    consecutive_nacks: &AtomicU32,
    result: SourceBatchResult,
    plugin_id: u32,
    max_consecutive_nacks: u32,
) -> BatchCompletion {
    if let Err(err) = source.on_batch_result(result).await {
        error!("Failed to process {result:?} for source connector with ID: {plugin_id}. {err}");
        return BatchCompletion::Stop;
    }

    let consecutive_nacks = match result {
        SourceBatchResult::Ack => {
            consecutive_nacks.store(0, Ordering::Relaxed);
            0
        }
        SourceBatchResult::Nack => consecutive_nacks.fetch_add(1, Ordering::Relaxed) + 1,
    };
    if consecutive_nacks >= max_consecutive_nacks {
        error!(
            "Stopping source connector with ID: {plugin_id} after {consecutive_nacks} consecutive NACKs"
        );
        return BatchCompletion::Stop;
    }

    BatchCompletion::Applied(result)
}

async fn sleep_after_nack(consecutive_nacks: &AtomicU32, policy: BatchPolicy) {
    tokio::time::sleep(nack_retry_delay(consecutive_nacks, policy)).await;
}

fn nack_retry_delay(consecutive_nacks: &AtomicU32, policy: BatchPolicy) -> Duration {
    let attempt = consecutive_nacks.load(Ordering::Relaxed).saturating_sub(1);
    exponential_backoff(
        policy.nack_retry_delay,
        attempt,
        policy.max_nack_retry_delay,
    )
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
        unsafe extern "C" fn iggy_source_handle_v2(id: u32, callback: SendCallback) -> i32 {
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
            let Some(mut instance) = INSTANCES.get_mut(&id) else {
                tracing::error!(
                    "Source connector with ID: {id} was not found and cannot be closed."
                );
                return -1;
            };
            let result = instance.close();
            drop(instance);
            INSTANCES.remove(&id);
            result
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

    fn test_policy() -> BatchPolicy {
        BatchPolicy {
            result_timeout: Duration::from_millis(500),
            nack_retry_delay: Duration::from_millis(1),
            max_nack_retry_delay: Duration::from_millis(2),
            max_consecutive_nacks: MAX_CONSECUTIVE_NACKS,
        }
    }

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

    async fn complete_test_batch(
        pending_batch: Arc<Mutex<Option<PendingBatch>>>,
        source: Arc<TestSource>,
        consecutive_nacks: Arc<AtomicU32>,
        batch_id: u64,
        result: SourceBatchResult,
        plugin_id: u32,
    ) -> i32 {
        tokio::task::spawn_blocking(move || {
            complete_pending_batch(
                &pending_batch,
                &source,
                &consecutive_nacks,
                batch_id,
                result as u8,
                plugin_id,
                MAX_CONSECUTIVE_NACKS,
            )
        })
        .await
        .expect("batch result task should complete")
    }

    #[test]
    fn given_batch_without_result_should_not_poll_again() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let source = Arc::new(TestSource::default());
            let pending_batch = Arc::new(Mutex::new(None));
            let pending_for_task = Arc::clone(&pending_batch);
            let consecutive_nacks = Arc::new(AtomicU32::new(0));
            let nacks_for_task = Arc::clone(&consecutive_nacks);
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
                nacks_for_task,
                test_policy(),
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
                complete_test_batch(
                    Arc::clone(&pending_batch),
                    Arc::clone(&source),
                    Arc::clone(&consecutive_nacks),
                    batch_id,
                    SourceBatchResult::Ack,
                    7,
                )
                .await,
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
            let consecutive_nacks = Arc::new(AtomicU32::new(0));
            let nacks_for_task = Arc::clone(&consecutive_nacks);
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
                nacks_for_task,
                test_policy(),
            ));

            let batch_id = tokio::time::timeout(Duration::from_secs(1), batch_receiver.recv())
                .await
                .expect("first batch was not sent")
                .expect("batch channel closed");
            assert_eq!(
                complete_test_batch(
                    Arc::clone(&pending_batch),
                    Arc::clone(&source),
                    Arc::clone(&consecutive_nacks),
                    batch_id,
                    SourceBatchResult::Nack,
                    9,
                )
                .await,
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
        let source = Arc::new(TestSource::default());
        let consecutive_nacks = Arc::new(AtomicU32::new(0));
        let pending_batch = Mutex::new(None);
        let (result_sender, result_receiver) = oneshot::channel();
        *lock_pending_batch(&pending_batch) = Some(PendingBatch {
            id: 41,
            result_sender,
        });

        assert_eq!(
            complete_pending_batch(
                &pending_batch,
                &source,
                &consecutive_nacks,
                42,
                SourceBatchResult::Ack as u8,
                11,
                MAX_CONSECUTIVE_NACKS,
            ),
            -1
        );
        assert_eq!(
            complete_pending_batch(
                &pending_batch,
                &source,
                &consecutive_nacks,
                41,
                SourceBatchResult::Ack as u8,
                11,
                MAX_CONSECUTIVE_NACKS,
            ),
            0
        );

        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        assert_eq!(
            runtime
                .block_on(result_receiver)
                .expect("batch result sender was dropped"),
            BatchCompletion::Applied(SourceBatchResult::Ack)
        );
    }

    #[test]
    fn given_unknown_result_code_should_complete_batch_as_nack() {
        let source = Arc::new(TestSource::default());
        let consecutive_nacks = Arc::new(AtomicU32::new(0));
        let pending_batch = Mutex::new(None);
        let (result_sender, result_receiver) = oneshot::channel();
        *lock_pending_batch(&pending_batch) = Some(PendingBatch {
            id: 51,
            result_sender,
        });

        assert_eq!(
            complete_pending_batch(
                &pending_batch,
                &source,
                &consecutive_nacks,
                51,
                99,
                17,
                MAX_CONSECUTIVE_NACKS,
            ),
            -1
        );

        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        assert_eq!(
            runtime
                .block_on(result_receiver)
                .expect("batch result sender was dropped"),
            BatchCompletion::Applied(SourceBatchResult::Nack)
        );
        assert_eq!(
            *source
                .results
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            vec![SourceBatchResult::Nack]
        );
    }

    #[test]
    fn given_result_timeout_should_nack_and_poll_next_batch() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let source = Arc::new(TestSource::default());
            let pending_batch = Arc::new(Mutex::new(None));
            let consecutive_nacks = Arc::new(AtomicU32::new(0));
            let (_shutdown_sender, shutdown_receiver) = watch::channel(());
            let (batch_sender, mut batch_receiver) = mpsc::unbounded_channel();
            let policy = BatchPolicy {
                result_timeout: Duration::from_millis(10),
                ..test_policy()
            };

            let task = tokio::spawn(handle_messages(
                19,
                Arc::clone(&source),
                move |_, batch_id, _, _| {
                    batch_sender
                        .send(batch_id)
                        .expect("batch receiver should remain open");
                    0
                },
                shutdown_receiver,
                Arc::clone(&pending_batch),
                Arc::clone(&consecutive_nacks),
                policy,
            ));

            assert_eq!(batch_receiver.recv().await, Some(1));
            let next_batch_id = tokio::time::timeout(Duration::from_secs(1), batch_receiver.recv())
                .await
                .expect("source did not poll after timed-out batch")
                .expect("batch channel closed");
            assert_eq!(next_batch_id, 2);
            assert_eq!(
                *source
                    .results
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
                vec![SourceBatchResult::Nack]
            );

            task.abort();
            let _ = task.await;
        });
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
            let consecutive_nacks = Arc::new(AtomicU32::new(0));
            let nacks_for_task = Arc::clone(&consecutive_nacks);
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
                nacks_for_task,
                test_policy(),
            ));

            let batch_id = tokio::time::timeout(Duration::from_secs(1), batch_receiver.recv())
                .await
                .expect("first batch was not sent")
                .expect("batch channel closed");
            assert_eq!(
                complete_test_batch(
                    Arc::clone(&pending_batch),
                    Arc::clone(&source),
                    Arc::clone(&consecutive_nacks),
                    batch_id,
                    SourceBatchResult::Nack,
                    13,
                )
                .await,
                -1
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

    #[test]
    fn given_repeated_nacks_when_limit_is_reached_should_stop() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let source = Arc::new(TestSource::default());
            let consecutive_nacks = AtomicU32::new(0);

            for expected_count in 1..MAX_CONSECUTIVE_NACKS {
                assert_eq!(
                    apply_batch_result(
                        &source,
                        &consecutive_nacks,
                        SourceBatchResult::Nack,
                        23,
                        MAX_CONSECUTIVE_NACKS,
                    )
                    .await,
                    BatchCompletion::Applied(SourceBatchResult::Nack)
                );
                assert_eq!(consecutive_nacks.load(Ordering::Relaxed), expected_count);
            }

            assert_eq!(
                apply_batch_result(
                    &source,
                    &consecutive_nacks,
                    SourceBatchResult::Nack,
                    23,
                    MAX_CONSECUTIVE_NACKS,
                )
                .await,
                BatchCompletion::Stop
            );
            assert_eq!(
                consecutive_nacks.load(Ordering::Relaxed),
                MAX_CONSECUTIVE_NACKS
            );
        });
    }

    #[test]
    fn given_ack_after_nack_should_reset_consecutive_nacks() {
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            let source = Arc::new(TestSource::default());
            let consecutive_nacks = AtomicU32::new(3);

            assert_eq!(
                apply_batch_result(
                    &source,
                    &consecutive_nacks,
                    SourceBatchResult::Ack,
                    29,
                    MAX_CONSECUTIVE_NACKS,
                )
                .await,
                BatchCompletion::Applied(SourceBatchResult::Ack)
            );
            assert_eq!(consecutive_nacks.load(Ordering::Relaxed), 0);
        });
    }

    #[test]
    fn given_consecutive_nacks_should_apply_capped_backoff() {
        let consecutive_nacks = AtomicU32::new(1);
        let policy = BatchPolicy {
            nack_retry_delay: Duration::from_millis(100),
            max_nack_retry_delay: Duration::from_millis(350),
            ..test_policy()
        };

        assert_eq!(
            nack_retry_delay(&consecutive_nacks, policy),
            Duration::from_millis(100)
        );
        consecutive_nacks.store(2, Ordering::Relaxed);
        assert_eq!(
            nack_retry_delay(&consecutive_nacks, policy),
            Duration::from_millis(200)
        );
        consecutive_nacks.store(3, Ordering::Relaxed);
        assert_eq!(
            nack_retry_delay(&consecutive_nacks, policy),
            Duration::from_millis(350)
        );
        consecutive_nacks.store(4, Ordering::Relaxed);
        assert_eq!(
            nack_retry_delay(&consecutive_nacks, policy),
            Duration::from_millis(350)
        );
    }
}
