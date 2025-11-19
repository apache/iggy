/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use crate::{ConnectorState, Error, Source, get_runtime};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::{sync::watch, task::JoinHandle};
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

pub type SendCallback = extern "C" fn(plugin_id: u32, messages_ptr: *const u8, messages_len: usize);

#[derive(Debug)]
pub struct SourceContainer<T: Source + std::fmt::Debug> {
    id: u32,
    source: Option<Arc<T>>,
    shutdown: Option<watch::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl<T: Source + std::fmt::Debug + 'static> SourceContainer<T> {
    pub const fn new(id: u32) -> Self {
        Self {
            id,
            source: None,
            shutdown: None,
            task: None,
        }
    }

    /// # Safety
    /// Do not copy the configuration pointer
    pub unsafe fn open<F, C>(
        &mut self,
        id: u32,
        config_ptr: *const u8,
        config_len: usize,
        state_ptr: *const u8,
        state_len: usize,
        factory: F,
    ) -> i32
    where
        F: FnOnce(u32, C, Option<ConnectorState>) -> T,
        C: DeserializeOwned,
    {
        unsafe {
            _ = Registry::default()
                .with(tracing_subscriber::fmt::layer())
                .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
                .try_init();
            let slice = std::slice::from_raw_parts(config_ptr, config_len);
            let Ok(config_str) = std::str::from_utf8(slice) else {
                error!("Failed to read configuration for source connector with ID: {id}",);
                return -1;
            };

            let Ok(config) = serde_json::from_str(config_str) else {
                error!("Failed to parse configuration for source connector with ID: {id}",);
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
        let handle = runtime.spawn(async move {
            let _ = handle_messages(plugin_id, source, callback, shutdown_rx).await;
        });

        self.shutdown = Some(shutdown_tx);
        self.task = Some(handle);
        0
    }
}

async fn handle_messages<T: Source>(
    plugin_id: u32,
    source: Arc<T>,
    callback: SendCallback,
    mut shutdown: watch::Receiver<()>,
) -> Result<(), Error> {
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!("Shutting down source connector with ID: {plugin_id}");
                break;
            }
            messages = source.poll() => {
                let Ok(messages) = messages else {
                    error!("Failed to poll messages for source connector with ID: {plugin_id}");
                    continue;
                };

                let Ok(messages) = postcard::to_allocvec(&messages) else {
                    error!("Failed to serialize messages for source connector with ID: {plugin_id}");
                    continue;
                };

                callback(plugin_id, messages.as_ptr(), messages.len());
            }
        }
    }

    Ok(())
}

#[macro_export]
macro_rules! source_connector {
    ($type:ty) => {
        const _: fn() = || {
            fn assert_trait<T: $crate::Source>() {}
            assert_trait::<$type>();
        };

        use dashmap::DashMap;
        use once_cell::sync::Lazy;
        use $crate::source::SendCallback;
        use $crate::source::SourceContainer;

        static INSTANCES: Lazy<DashMap<u32, SourceContainer<$type>>> = Lazy::new(DashMap::new);

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn open(
            id: u32,
            config_ptr: *const u8,
            config_len: usize,
            state_ptr: *const u8,
            state_len: usize,
        ) -> i32 {
            let mut container = SourceContainer::new(id);
            let result = container.open(
                id,
                config_ptr,
                config_len,
                state_ptr,
                state_len,
                <$type>::new,
            );
            INSTANCES.insert(id, container);
            result
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn handle(id: u32, callback: SendCallback) -> i32 {
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
        unsafe extern "C" fn close(id: u32) -> i32 {
            let Some(mut instance) = INSTANCES.remove(&id) else {
                tracing::error!(
                    "Source connector with ID: {id} was not found and cannot be closed."
                );
                return -1;
            };
            instance.1.close()
        }
    };
}
