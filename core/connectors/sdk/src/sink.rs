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

use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{ConsumedMessage, MessagesMetadata, RawMessages, Sink, TopicMetadata, get_runtime};

pub type ConsumeCallback = extern "C" fn(
    plugin_id: u32,
    topic_meta_ptr: *const u8,
    topic_meta_len: usize,
    messages_meta_ptr: *const u8,
    messages_meta_len: usize,
    messages_ptr: *const u8,
    messages_len: usize,
) -> i32;

#[derive(Debug)]
pub struct SinkContainer<T: Sink + std::fmt::Debug> {
    id: u32,
    sink: Option<T>,
    shutdown: Option<watch::Sender<()>>,
}

impl<T: Sink + std::fmt::Debug> SinkContainer<T> {
    pub const fn new(id: u32) -> Self {
        Self {
            id,
            sink: None,
            shutdown: None,
        }
    }

    /// # Safety
    /// Do not copy the configuration pointer
    pub unsafe fn open<F, C>(
        &mut self,
        id: u32,
        config_ptr: *const u8,
        config_len: usize,
        factory: F,
    ) -> i32
    where
        F: FnOnce(u32, C) -> T,
        C: DeserializeOwned,
    {
        unsafe {
            _ = Registry::default()
                .with(tracing_subscriber::fmt::layer())
                .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
                .try_init();
            let slice = std::slice::from_raw_parts(config_ptr, config_len);
            let Ok(config_str) = std::str::from_utf8(slice) else {
                error!("Failed to read configuration for sink connector with ID: {id}",);
                return -1;
            };

            let Ok(config) = serde_json::from_str(config_str) else {
                error!("Failed to parse configuration for sink connector with ID: {id}",);
                return -1;
            };

            let mut sink = factory(id, config);
            let runtime = get_runtime();
            let result = runtime.block_on(sink.open());
            self.id = id;
            self.sink = Some(sink);
            if result.is_ok() { 0 } else { 1 }
        }
    }

    /// # Safety
    /// This is safe to invoke
    pub unsafe fn close(&mut self) -> i32 {
        let Some(mut sink) = self.sink.take() else {
            error!(
                "Sink connector with ID: {} is not initialized - cannot close.",
                self.id
            );
            return -1;
        };

        info!("Closing sink connector with ID: {}...", self.id);
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }

        let runtime = get_runtime();
        runtime.block_on(async {
            if let Err(err) = sink.close().await {
                error!("Failed to close sink connector with ID: {}. {err}", self.id);
            }
        });
        info!("Closed sink connector with ID: {}", self.id);
        0
    }

    /// # Safety
    /// Do not copy the pointers to the topic metadata, messages metadata, or messages.
    pub unsafe fn consume(
        &self,
        topic_meta_ptr: *const u8,
        topic_meta_len: usize,
        messages_meta_ptr: *const u8,
        messages_meta_len: usize,
        messages_ptr: *const u8,
        messages_len: usize,
    ) -> i32 {
        unsafe {
            let Some(sink) = self.sink.as_ref() else {
                error!(
                    "Sink connector with ID: {} is not initialized - cannot consume messages.",
                    self.id
                );
                return -1;
            };

            let topic_meta_slice = std::slice::from_raw_parts(topic_meta_ptr, topic_meta_len);
            let messages_meta_slice =
                std::slice::from_raw_parts(messages_meta_ptr, messages_meta_len);
            let messages_slice = std::slice::from_raw_parts(messages_ptr, messages_len);

            let Ok(topic_metadata) = postcard::from_bytes::<TopicMetadata>(topic_meta_slice) else {
                error!(
                    "Failed to decode topic metadata by sink connector with ID: {}",
                    self.id
                );
                return -1;
            };

            let Ok(messages_metadata) =
                postcard::from_bytes::<MessagesMetadata>(messages_meta_slice)
            else {
                error!(
                    "Failed to decode messages metadata by sink connector with ID: {} from stream: {}, topic: {}",
                    self.id, topic_metadata.stream, topic_metadata.topic
                );
                return -1;
            };

            let Ok(raw_messages) = postcard::from_bytes::<RawMessages>(messages_slice) else {
                error!(
                    "Failed to decode raw messages by sink connector with ID: {} from stream: {}, topic: {}",
                    self.id, topic_metadata.stream, topic_metadata.topic
                );
                return -1;
            };

            let mut messages = Vec::with_capacity(raw_messages.messages.len());
            for message in raw_messages.messages {
                let headers = if message.headers.is_empty() {
                    None
                } else {
                    let Ok(headers) = postcard::from_bytes(&message.headers) else {
                        error!(
                            "Failed to decode message headers by sink connector with ID: {} from stream: {}, topic: {}",
                            self.id, topic_metadata.stream, topic_metadata.topic
                        );
                        continue;
                    };
                    Some(headers)
                };

                let Ok(payload) = messages_metadata.schema.try_into_payload(message.payload) else {
                    error!(
                        "Failed to decode message payload by sink connector with ID: {} from stream: {}, topic: {}",
                        self.id, topic_metadata.stream, topic_metadata.topic
                    );
                    continue;
                };

                messages.push(ConsumedMessage {
                    id: message.id,
                    offset: message.offset,
                    checksum: message.checksum,
                    timestamp: message.timestamp,
                    origin_timestamp: message.origin_timestamp,
                    headers,
                    payload,
                })
            }

            let runtime = get_runtime();
            let result =
                runtime.block_on(sink.consume(&topic_metadata, messages_metadata, messages));
            if result.is_ok() { 0 } else { 1 }
        }
    }
}

#[macro_export]
macro_rules! sink_connector {
    ($type:ty) => {
        const _: fn() = || {
            fn assert_trait<T: $crate::Sink>() {}
            assert_trait::<$type>();
        };

        use dashmap::DashMap;
        use once_cell::sync::Lazy;
        use $crate::sink::SinkContainer;

        static INSTANCES: Lazy<DashMap<u32, SinkContainer<$type>>> = Lazy::new(DashMap::new);

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn open(id: u32, config_ptr: *const u8, config_len: usize) -> i32 {
            let mut container = SinkContainer::new(id);
            let result = container.open(id, config_ptr, config_len, <$type>::new);
            INSTANCES.insert(id, container);
            result
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn consume(
            id: u32,
            topic_meta_ptr: *const u8,
            topic_meta_len: usize,
            messages_meta_ptr: *const u8,
            messages_meta_len: usize,
            messages_ptr: *const u8,
            messages_len: usize,
        ) -> i32 {
            let Some(instance) = INSTANCES.get(&id) else {
                tracing::error!(
                    "Sink connector with ID: {id} was not found and consume messages cannot be invoked."
                );
                return -1;
            };
            instance.consume(
                topic_meta_ptr,
                topic_meta_len,
                messages_meta_ptr,
                messages_meta_len,
                messages_ptr,
                messages_len,
            )
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn close(id: u32) -> i32 {
            let Some(mut instance) = INSTANCES.remove(&id) else {
                tracing::error!("Sink connector with ID: {id} was not found and cannot be closed.");
                return -1;
            };
            instance.1.close()
        }
    };
}
