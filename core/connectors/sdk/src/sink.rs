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

use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tracing::{error, info};

use crate::log::{LogCallback, init_plugin_logging};
use crate::{
    ConsumedMessage, Error, FlushPartition, MessagesMetadata, Payload, RawMessages, Sink,
    TopicMetadata, get_runtime,
};

/// The version of the sink delivery contract that this SDK implements. The runtime passes the
/// version it requires to `iggy_sink_enable_at_least_once`.
pub const DELIVERY_CONTRACT_VERSION: u32 = 1;

pub type ConsumeCallback = extern "C" fn(
    plugin_id: u32,
    topic_meta_ptr: *const u8,
    topic_meta_len: usize,
    messages_meta_ptr: *const u8,
    messages_meta_len: usize,
    messages_ptr: *const u8,
    messages_len: usize,
) -> i32;

pub type FlushCallback = extern "C" fn(
    plugin_id: u32,
    topic_meta_ptr: *const u8,
    topic_meta_len: usize,
    partitions_ptr: *const u8,
    partitions_len: usize,
) -> i32;

/// The result of a sink call as it crosses the FFI boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkCallStatus {
    Succeeded,
    /// Retrying the same messages may succeed. A plugin built before the delivery contract
    /// reports a failed `consume` this way, and a call it cannot dispatch or decode as
    /// [`SinkCallStatus::DISPATCH_FAILURE`], which reads as permanent.
    TransientFailure,
    /// Retrying the same messages cannot succeed.
    PermanentFailure,
}

impl SinkCallStatus {
    /// The code of a call that the plugin cannot dispatch, such as one for an unknown instance
    /// or with arguments that do not decode. It reads as [`SinkCallStatus::PermanentFailure`].
    pub const DISPATCH_FAILURE: i32 = -1;

    pub const fn code(self) -> i32 {
        match self {
            SinkCallStatus::Succeeded => 0,
            SinkCallStatus::TransientFailure => 1,
            SinkCallStatus::PermanentFailure => 2,
        }
    }

    pub const fn is_success(self) -> bool {
        matches!(self, SinkCallStatus::Succeeded)
    }
}

impl From<&Error> for SinkCallStatus {
    fn from(error: &Error) -> Self {
        if error.is_permanent() {
            SinkCallStatus::PermanentFailure
        } else {
            SinkCallStatus::TransientFailure
        }
    }
}

impl From<i32> for SinkCallStatus {
    /// A call that the plugin cannot dispatch is one that no retry fixes.
    fn from(code: i32) -> Self {
        match code {
            0 => SinkCallStatus::Succeeded,
            2 | SinkCallStatus::DISPATCH_FAILURE => SinkCallStatus::PermanentFailure,
            _ => SinkCallStatus::TransientFailure,
        }
    }
}

#[derive(Debug)]
pub struct SinkContainer<T: Sink + std::fmt::Debug> {
    id: u32,
    sink: Option<T>,
    shutdown: Option<watch::Sender<()>>,
    at_least_once: bool,
}

impl<T: Sink + std::fmt::Debug> SinkContainer<T> {
    pub const fn new(id: u32) -> Self {
        Self {
            id,
            sink: None,
            shutdown: None,
            at_least_once: false,
        }
    }

    /// Turns on at-least-once delivery when the plugin implements `version` of the contract and
    /// the sink accepts it for its configuration. Returns the [`SinkCallStatus`] code.
    pub fn enable_at_least_once(&mut self, version: u32) -> i32 {
        let Some(sink) = self.sink.as_mut() else {
            error!(
                "Sink connector with ID: {} is not initialized - cannot enable at-least-once delivery.",
                self.id
            );
            return SinkCallStatus::DISPATCH_FAILURE;
        };
        if version != DELIVERY_CONTRACT_VERSION {
            error!(
                "Sink connector with ID: {} implements delivery contract version {DELIVERY_CONTRACT_VERSION}, but version {version} was requested.",
                self.id
            );
            return SinkCallStatus::PermanentFailure.code();
        }
        match sink.enable_at_least_once() {
            Ok(()) => {
                self.at_least_once = true;
                info!(
                    "Sink connector with ID: {} delivers at least once.",
                    self.id
                );
                SinkCallStatus::Succeeded.code()
            }
            Err(err) => {
                error!(
                    "Sink connector with ID: {} cannot deliver at least once. {err}",
                    self.id
                );
                SinkCallStatus::from(&err).code()
            }
        }
    }

    /// # Safety
    /// Do not copy the pointers to the topic metadata or the partitions.
    pub unsafe fn flush(
        &self,
        topic_meta_ptr: *const u8,
        topic_meta_len: usize,
        partitions_ptr: *const u8,
        partitions_len: usize,
    ) -> i32 {
        let Some(sink) = self.sink.as_ref() else {
            error!(
                "Sink connector with ID: {} is not initialized - cannot flush.",
                self.id
            );
            return SinkCallStatus::DISPATCH_FAILURE;
        };
        if !self.at_least_once {
            error!(
                "Sink connector with ID: {} was asked to flush without at-least-once delivery enabled.",
                self.id
            );
            return SinkCallStatus::PermanentFailure.code();
        }
        let (topic_meta_slice, partitions_slice) = unsafe {
            (
                std::slice::from_raw_parts(topic_meta_ptr, topic_meta_len),
                std::slice::from_raw_parts(partitions_ptr, partitions_len),
            )
        };
        let topic_metadata = match postcard::from_bytes::<TopicMetadata>(topic_meta_slice) {
            Ok(meta) => meta,
            Err(err) => {
                error!(
                    "Failed to decode topic metadata of a flush by sink connector with ID: {}. {err}",
                    self.id
                );
                return SinkCallStatus::DISPATCH_FAILURE;
            }
        };
        let partitions = match postcard::from_bytes::<Vec<FlushPartition>>(partitions_slice) {
            Ok(partitions) => partitions,
            Err(err) => {
                error!(
                    "Failed to decode the partitions of a flush by sink connector with ID: {}. {err}",
                    self.id
                );
                return SinkCallStatus::DISPATCH_FAILURE;
            }
        };
        match get_runtime().block_on(sink.flush(&topic_metadata, &partitions)) {
            Ok(()) => SinkCallStatus::Succeeded.code(),
            Err(err) => {
                error!(
                    "Failed to flush sink connector with ID: {} for stream: {}, topic: {}. {err}",
                    self.id, topic_metadata.stream, topic_metadata.topic
                );
                SinkCallStatus::from(&err).code()
            }
        }
    }

    /// # Safety
    /// Do not copy the configuration pointer
    pub unsafe fn open<F, C>(
        &mut self,
        id: u32,
        config_ptr: *const u8,
        config_len: usize,
        log_callback: LogCallback,
        factory: F,
    ) -> i32
    where
        F: FnOnce(u32, C) -> T,
        C: DeserializeOwned,
    {
        unsafe {
            init_plugin_logging(log_callback);
            let slice = std::slice::from_raw_parts(config_ptr, config_len);
            let Ok(config_str) = std::str::from_utf8(slice) else {
                error!("Failed to read configuration for sink connector with ID: {id}");
                return -1;
            };

            let config = match serde_json::from_str::<C>(config_str) {
                Ok(cfg) => cfg,
                Err(error) => {
                    error!(
                        "Failed to parse configuration for sink connector with ID: {id}. {error}"
                    );
                    return -1;
                }
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

            let topic_metadata = match postcard::from_bytes::<TopicMetadata>(topic_meta_slice) {
                Ok(meta) => meta,
                Err(err) => {
                    error!(
                        "Failed to decode topic metadata by sink connector with ID: {}. {err}",
                        self.id
                    );
                    return -1;
                }
            };

            let messages_metadata = match postcard::from_bytes::<MessagesMetadata>(
                messages_meta_slice,
            ) {
                Ok(meta) => meta,
                Err(err) => {
                    error!(
                        "Failed to decode messages metadata by sink connector with ID: {} from stream: {}, topic: {}. {err}",
                        self.id, topic_metadata.stream, topic_metadata.topic
                    );
                    return -1;
                }
            };

            let raw_messages = match postcard::from_bytes::<RawMessages>(messages_slice) {
                Ok(messages) => messages,
                Err(err) => {
                    error!(
                        "Failed to decode raw messages by sink connector with ID: {} from stream: {}, topic: {}. {err}",
                        self.id, topic_metadata.stream, topic_metadata.topic
                    );
                    return -1;
                }
            };

            let mut messages = Vec::with_capacity(raw_messages.messages.len());
            for message in raw_messages.messages {
                let headers = if message.headers.is_empty() {
                    None
                } else {
                    match postcard::from_bytes(&message.headers) {
                        Ok(headers) => Some(headers),
                        Err(err) => {
                            error!(
                                "Failed to decode message headers by sink connector with ID: {} from stream: {}, topic: {}. {err}",
                                self.id, topic_metadata.stream, topic_metadata.topic
                            );
                            if self.at_least_once {
                                return SinkCallStatus::PermanentFailure.code();
                            }
                            continue;
                        }
                    }
                };

                // The runtime tags each run from `Payload::schema`, so the
                // tag names a variant here rather than a wire format.
                let payload = match Payload::try_from_schema(
                    messages_metadata.schema,
                    message.payload,
                ) {
                    Ok(payload) => payload,
                    Err(err) => {
                        error!(
                            "Failed to decode message payload by sink connector with ID: {} from stream: {}, topic: {}. {err}",
                            self.id, topic_metadata.stream, topic_metadata.topic
                        );
                        if self.at_least_once {
                            return SinkCallStatus::PermanentFailure.code();
                        }
                        continue;
                    }
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
            match result {
                Ok(()) => SinkCallStatus::Succeeded.code(),
                Err(err) => {
                    error!(
                        "Failed to consume messages by sink connector with ID: {} from stream: {}, topic: {}. {err}",
                        self.id, topic_metadata.stream, topic_metadata.topic
                    );
                    SinkCallStatus::from(&err).code()
                }
            }
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

        use $crate::connector_macro_support::DashMap;
        use $crate::LogCallback;
        use $crate::sink::SinkContainer;
        use std::sync::LazyLock;

        static INSTANCES: LazyLock<DashMap<u32, SinkContainer<$type>>> = LazyLock::new(DashMap::new);

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn iggy_sink_open(
            id: u32,
            config_ptr: *const u8,
            config_len: usize,
            log_callback: LogCallback,
        ) -> i32 {
            if INSTANCES.contains_key(&id) {
                // Duplicate id: caller did not close before reopening. Without
                // this guard the existing entry would be silently overwritten,
                // discarding any in-flight buffered data and orphaning tasks.
                return -1;
            }

            let mut container = SinkContainer::new(id);
            let result = container.open(id, config_ptr, config_len, log_callback, <$type>::new);
            if result != 0 {
                // Rolled back rather than registered, for the reason the
                // source macro gives: a failed open is still stored on the
                // container, and registering it strands an instance nothing
                // outside can name to close.
                return result;
            }
            INSTANCES.insert(id, container);
            result
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn iggy_sink_consume(
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
        extern "C" fn iggy_sink_enable_at_least_once(id: u32, version: u32) -> i32 {
            let Some(mut instance) = INSTANCES.get_mut(&id) else {
                tracing::error!(
                    "Sink connector with ID: {id} was not found and at-least-once delivery cannot be enabled."
                );
                return $crate::sink::SinkCallStatus::DISPATCH_FAILURE;
            };
            instance.enable_at_least_once(version)
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn iggy_sink_flush(
            id: u32,
            topic_meta_ptr: *const u8,
            topic_meta_len: usize,
            partitions_ptr: *const u8,
            partitions_len: usize,
        ) -> i32 {
            let Some(instance) = INSTANCES.get(&id) else {
                tracing::error!("Sink connector with ID: {id} was not found and cannot be flushed.");
                return $crate::sink::SinkCallStatus::DISPATCH_FAILURE;
            };
            instance.flush(topic_meta_ptr, topic_meta_len, partitions_ptr, partitions_len)
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        unsafe extern "C" fn iggy_sink_close(id: u32) -> i32 {
            let Some(mut instance) = INSTANCES.remove(&id) else {
                tracing::error!("Sink connector with ID: {id} was not found and cannot be closed.");
                return -1;
            };
            instance.1.close()
        }

        #[cfg(not(test))]
        #[unsafe(no_mangle)]
        extern "C" fn iggy_sink_version() -> *const std::ffi::c_char {
            static VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");
            VERSION.as_ptr() as *const std::ffi::c_char
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::sink::{DELIVERY_CONTRACT_VERSION, SinkCallStatus, SinkContainer};
    use crate::{
        ConsumedMessage, Error, FlushPartition, MessagesMetadata, Payload, RawMessage, RawMessages,
        Schema, Sink, TopicMetadata,
    };
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    /// Keeps the payloads the container rebuilt, which is the only observable
    /// the FFI entry point produces.
    #[derive(Debug, Default)]
    struct RecordingSink {
        consumed: Arc<Mutex<Vec<Payload>>>,
    }

    #[async_trait]
    impl Sink for RecordingSink {
        async fn open(&mut self) -> Result<(), Error> {
            Ok(())
        }

        async fn consume(
            &self,
            _topic_metadata: &TopicMetadata,
            _messages_metadata: MessagesMetadata,
            messages: Vec<ConsumedMessage>,
        ) -> Result<(), Error> {
            let mut consumed = self.consumed.lock().expect("recorder was poisoned");
            consumed.extend(messages.into_iter().map(|message| message.payload));
            Ok(())
        }

        async fn close(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// Drives one run through the container the way the runtime does: postcard
    /// bytes in, and the payloads the sink was handed out.
    fn consume_one(schema: Schema, payload: Vec<u8>) -> Vec<Payload> {
        let consumed = Arc::new(Mutex::new(Vec::new()));
        let container = SinkContainer {
            id: 1,
            sink: Some(RecordingSink {
                consumed: Arc::clone(&consumed),
            }),
            shutdown: None,
            at_least_once: false,
        };

        let topic_meta = postcard::to_allocvec(&TopicMetadata {
            stream: "test_stream".to_owned(),
            topic: "test_topic".to_owned(),
        })
        .expect("failed to serialize topic metadata");
        let messages_meta = postcard::to_allocvec(&MessagesMetadata {
            partition_id: 0,
            current_offset: 10_000,
            schema,
        })
        .expect("failed to serialize messages metadata");
        let messages = postcard::to_allocvec(&RawMessages {
            schema,
            messages: vec![RawMessage {
                id: 1,
                offset: 0,
                checksum: 0,
                timestamp: 0,
                origin_timestamp: 0,
                headers: Vec::new(),
                payload,
            }],
        })
        .expect("failed to serialize messages");

        // `consume` blocks on the SDK runtime, so this stays a plain test.
        let status = unsafe {
            container.consume(
                topic_meta.as_ptr(),
                topic_meta.len(),
                messages_meta.as_ptr(),
                messages_meta.len(),
                messages.as_ptr(),
                messages.len(),
            )
        };
        assert_eq!(status, 0, "the container must accept the run");

        consumed.lock().expect("recorder was poisoned").clone()
    }

    #[test]
    fn given_a_proto_tagged_run_when_the_container_consumes_should_hand_the_sink_a_proto_payload() {
        let recorded = consume_one(Schema::Proto, br#"{"first":"encoded"}"#.to_vec());

        assert_eq!(recorded.len(), 1);
        let Payload::Proto(text) = &recorded[0] else {
            panic!("expected a proto payload, got {}", recorded[0]);
        };
        assert_eq!(text, r#"{"first":"encoded"}"#);
    }

    #[test]
    fn given_a_json_tagged_run_when_the_container_consumes_should_hand_the_sink_a_json_payload() {
        let recorded = consume_one(Schema::Json, br#"{"first":"encoded"}"#.to_vec());

        assert_eq!(recorded.len(), 1);
        assert!(
            matches!(recorded[0], Payload::Json(_)),
            "got {}",
            recorded[0]
        );
    }

    #[test]
    fn given_a_text_tagged_run_when_the_container_consumes_should_hand_the_sink_a_text_payload() {
        let recorded = consume_one(Schema::Text, b"plain text".to_vec());

        assert_eq!(recorded.len(), 1);
        let Payload::Text(text) = &recorded[0] else {
            panic!("expected a text payload, got {}", recorded[0]);
        };
        assert_eq!(text, "plain text");
    }

    #[test]
    fn given_a_raw_tagged_run_when_the_container_consumes_should_hand_the_sink_a_raw_payload() {
        let recorded = consume_one(Schema::Raw, vec![0xff, 0x00, 0x01]);

        assert_eq!(recorded.len(), 1);
        let Payload::Raw(bytes) = &recorded[0] else {
            panic!("expected a raw payload, got {}", recorded[0]);
        };
        assert_eq!(bytes, &[0xff, 0x00, 0x01]);
    }

    /// A sink whose delivery support and failures a test chooses, recording what it flushed.
    #[derive(Debug, Default)]
    struct DeliveringSink {
        supports_at_least_once: bool,
        consume_error: Option<Error>,
        flush_error: Option<Error>,
        flushed: Arc<Mutex<Vec<FlushPartition>>>,
    }

    #[async_trait]
    impl Sink for DeliveringSink {
        async fn open(&mut self) -> Result<(), Error> {
            Ok(())
        }

        async fn consume(
            &self,
            _topic_metadata: &TopicMetadata,
            _messages_metadata: MessagesMetadata,
            _messages: Vec<ConsumedMessage>,
        ) -> Result<(), Error> {
            self.consume_error.clone().map_or(Ok(()), Err)
        }

        fn enable_at_least_once(&mut self) -> Result<(), Error> {
            if self.supports_at_least_once {
                Ok(())
            } else {
                Err(Error::Unsupported("not in this configuration".to_owned()))
            }
        }

        async fn flush(
            &self,
            _topic_metadata: &TopicMetadata,
            partitions: &[FlushPartition],
        ) -> Result<(), Error> {
            if let Some(error) = &self.flush_error {
                return Err(error.clone());
            }
            self.flushed
                .lock()
                .expect("recorder was poisoned")
                .extend_from_slice(partitions);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn delivering_container(sink: DeliveringSink) -> SinkContainer<DeliveringSink> {
        SinkContainer {
            id: 1,
            sink: Some(sink),
            shutdown: None,
            at_least_once: false,
        }
    }

    fn topic_meta() -> Vec<u8> {
        postcard::to_allocvec(&TopicMetadata {
            stream: "test_stream".to_owned(),
            topic: "test_topic".to_owned(),
        })
        .expect("failed to serialize topic metadata")
    }

    /// Sends one message carrying `payload` under `schema` and returns the status code.
    fn consume_status<T: Sink + std::fmt::Debug>(
        container: &SinkContainer<T>,
        schema: Schema,
        payload: Vec<u8>,
    ) -> i32 {
        let topic_meta = topic_meta();
        let messages_meta = postcard::to_allocvec(&MessagesMetadata {
            partition_id: 0,
            current_offset: 10_000,
            schema,
        })
        .expect("failed to serialize messages metadata");
        let messages = postcard::to_allocvec(&RawMessages {
            schema,
            messages: vec![RawMessage {
                id: 1,
                offset: 0,
                checksum: 0,
                timestamp: 0,
                origin_timestamp: 0,
                headers: Vec::new(),
                payload,
            }],
        })
        .expect("failed to serialize messages");
        unsafe {
            container.consume(
                topic_meta.as_ptr(),
                topic_meta.len(),
                messages_meta.as_ptr(),
                messages_meta.len(),
                messages.as_ptr(),
                messages.len(),
            )
        }
    }

    fn flush_status<T: Sink + std::fmt::Debug>(
        container: &SinkContainer<T>,
        partitions: &[FlushPartition],
    ) -> i32 {
        let topic_meta = topic_meta();
        let partitions =
            postcard::to_allocvec(&partitions.to_vec()).expect("failed to serialize partitions");
        unsafe {
            container.flush(
                topic_meta.as_ptr(),
                topic_meta.len(),
                partitions.as_ptr(),
                partitions.len(),
            )
        }
    }

    #[test]
    fn given_a_sink_without_support_when_enabling_at_least_once_should_refuse_permanently() {
        let mut container = delivering_container(DeliveringSink::default());

        let status = container.enable_at_least_once(DELIVERY_CONTRACT_VERSION);

        assert_eq!(
            SinkCallStatus::from(status),
            SinkCallStatus::PermanentFailure
        );
        assert!(!container.at_least_once);
    }

    #[test]
    fn given_another_contract_version_when_enabling_at_least_once_should_refuse_it() {
        let mut container = delivering_container(DeliveringSink {
            supports_at_least_once: true,
            ..DeliveringSink::default()
        });

        let status = container.enable_at_least_once(DELIVERY_CONTRACT_VERSION + 1);

        assert_eq!(
            SinkCallStatus::from(status),
            SinkCallStatus::PermanentFailure
        );
        assert!(!container.at_least_once);
    }

    #[test]
    fn given_at_least_once_enabled_when_flushed_should_hand_the_sink_the_partitions() {
        let flushed = Arc::new(Mutex::new(Vec::new()));
        let mut container = delivering_container(DeliveringSink {
            supports_at_least_once: true,
            flushed: Arc::clone(&flushed),
            ..DeliveringSink::default()
        });
        let partitions = [
            FlushPartition {
                partition_id: 0,
                offset: 7,
            },
            FlushPartition {
                partition_id: 3,
                offset: 41,
            },
        ];

        assert_eq!(container.enable_at_least_once(DELIVERY_CONTRACT_VERSION), 0);
        let status = flush_status(&container, &partitions);

        assert_eq!(status, 0);
        assert_eq!(*flushed.lock().expect("recorder was poisoned"), partitions);
    }

    #[test]
    fn given_at_least_once_not_enabled_when_flushed_should_refuse_without_calling_the_sink() {
        let flushed = Arc::new(Mutex::new(Vec::new()));
        let container = delivering_container(DeliveringSink {
            supports_at_least_once: true,
            flushed: Arc::clone(&flushed),
            ..DeliveringSink::default()
        });

        let status = flush_status(
            &container,
            &[FlushPartition {
                partition_id: 0,
                offset: 1,
            }],
        );

        assert_eq!(
            SinkCallStatus::from(status),
            SinkCallStatus::PermanentFailure
        );
        assert!(flushed.lock().expect("recorder was poisoned").is_empty());
    }

    #[test]
    fn given_a_closed_instance_when_flushed_should_refuse_the_stale_flush() {
        let flushed = Arc::new(Mutex::new(Vec::new()));
        let mut container = delivering_container(DeliveringSink {
            supports_at_least_once: true,
            flushed: Arc::clone(&flushed),
            ..DeliveringSink::default()
        });
        assert_eq!(container.enable_at_least_once(DELIVERY_CONTRACT_VERSION), 0);
        assert_eq!(unsafe { container.close() }, 0);

        let status = flush_status(
            &container,
            &[FlushPartition {
                partition_id: 0,
                offset: 1,
            }],
        );

        assert_eq!(
            SinkCallStatus::from(status),
            SinkCallStatus::PermanentFailure
        );
        assert!(flushed.lock().expect("recorder was poisoned").is_empty());
    }

    #[test]
    fn given_a_failing_flush_when_flushed_should_report_the_error_class() {
        for (error, expected) in [
            (
                Error::Connection("refused".to_owned()),
                SinkCallStatus::TransientFailure,
            ),
            (
                Error::PermanentHttpError("400".to_owned()),
                SinkCallStatus::PermanentFailure,
            ),
        ] {
            let mut container = delivering_container(DeliveringSink {
                supports_at_least_once: true,
                flush_error: Some(error),
                ..DeliveringSink::default()
            });
            assert_eq!(container.enable_at_least_once(DELIVERY_CONTRACT_VERSION), 0);

            let status = flush_status(
                &container,
                &[FlushPartition {
                    partition_id: 0,
                    offset: 1,
                }],
            );

            assert_eq!(SinkCallStatus::from(status), expected);
        }
    }

    #[test]
    fn given_a_failing_consume_when_consumed_should_report_the_error_class() {
        for (error, expected) in [
            (
                Error::HttpRequestFailed("503".to_owned()),
                SinkCallStatus::TransientFailure,
            ),
            (
                Error::SchemaMismatch("no such column".to_owned()),
                SinkCallStatus::PermanentFailure,
            ),
        ] {
            let container = delivering_container(DeliveringSink {
                consume_error: Some(error),
                ..DeliveringSink::default()
            });

            let status = consume_status(&container, Schema::Raw, vec![1]);

            assert_eq!(SinkCallStatus::from(status), expected);
        }
    }

    #[test]
    fn given_a_payload_that_does_not_decode_when_consumed_should_fail_only_at_least_once() {
        let mut container = delivering_container(DeliveringSink {
            supports_at_least_once: true,
            ..DeliveringSink::default()
        });
        let undecodable = b"{not json".to_vec();

        assert_eq!(
            consume_status(&container, Schema::Json, undecodable.clone()),
            0,
            "without at-least-once the container skips the message as before"
        );
        assert_eq!(container.enable_at_least_once(DELIVERY_CONTRACT_VERSION), 0);
        assert_eq!(
            SinkCallStatus::from(consume_status(&container, Schema::Json, undecodable)),
            SinkCallStatus::PermanentFailure
        );
    }

    #[test]
    fn given_status_codes_when_read_back_should_map_legacy_failures_to_transient() {
        for (code, expected) in [
            (0, SinkCallStatus::Succeeded),
            (1, SinkCallStatus::TransientFailure),
            (2, SinkCallStatus::PermanentFailure),
            (-1, SinkCallStatus::PermanentFailure),
            (7, SinkCallStatus::TransientFailure),
        ] {
            assert_eq!(SinkCallStatus::from(code), expected, "code {code}");
        }
        for status in [
            SinkCallStatus::Succeeded,
            SinkCallStatus::TransientFailure,
            SinkCallStatus::PermanentFailure,
        ] {
            assert_eq!(SinkCallStatus::from(status.code()), status);
        }
    }
}
