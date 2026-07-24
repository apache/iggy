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

use crate::benchmark;
use crate::configs::connectors::SinkConfig;
use crate::context::RuntimeContext;
use crate::log::LOG_CALLBACK;
use crate::metrics::{Metrics, SinkLabels};
use crate::{
    FailedPlugin, PLUGIN_ID, RuntimeError, SinkApi, SinkConnector, SinkConnectorConsumer,
    SinkConnectorPlugin, SinkConnectorWrapper, resolve_plugin_path, transform,
};
use dlopen2::wrapper::Container;
use futures::StreamExt;
use iggy::prelude::{
    AutoCommit, AutoCommitWhen, IggyClient, IggyConsumer, IggyDuration, IggyMessage,
    PollingStrategy,
};
use iggy_connector_sdk::decoders::avro::{AvroConfig, AvroStreamDecoder};
use iggy_connector_sdk::{
    DecodedMessage, MessagesMetadata, RawMessage, RawMessages, ReceivedMessage, Schema,
    StreamDecoder, TopicMetadata, sink::ConsumeCallback, transforms::Transform,
};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Initializes all enabled sink connectors.
///
/// Per-connector failures (path resolution, dlopen, plugin init,
/// consumer/decoder/transform setup) are captured against the offending
/// connector and do not abort the runtime. Connectors that fail before their
/// FFI container can be loaded are returned in the second tuple element so
/// they remain visible in health/status output.
///
/// Only system-level errors that prevent any connector from running are
/// propagated as `Err`.
pub async fn init(
    sink_configs: HashMap<String, SinkConfig>,
    iggy_client: &IggyClient,
) -> Result<(HashMap<String, SinkConnector>, Vec<FailedPlugin>), RuntimeError> {
    let mut sink_connectors: HashMap<String, SinkConnector> = HashMap::new();
    let mut failed_plugins: Vec<FailedPlugin> = Vec::new();

    for (key, config) in sink_configs {
        let name = config.name.clone();
        if !config.enabled {
            warn!("Sink: {name} is disabled ({key})");
            continue;
        }

        let plugin_id = PLUGIN_ID.fetch_add(1, Ordering::SeqCst);

        let path = match resolve_plugin_path(&config.path) {
            Ok(path) => path,
            Err(error) => {
                let message = format!("Failed to resolve plugin path: {error}");
                error!("Sink: {name} ({key}) - {message}");
                failed_plugins.push(FailedPlugin::new(
                    plugin_id,
                    &key,
                    &name,
                    &config.path,
                    config.plugin_config_format,
                    config.enabled,
                    message,
                ));
                continue;
            }
        };

        info!(
            "Initializing sink container with name: {name} ({key}), config version: {}, plugin: {path}",
            &config.version
        );

        if !sink_connectors.contains_key(&path) {
            let container = match unsafe { Container::<SinkApi>::load(&path) } {
                Ok(container) => container,
                Err(error) => {
                    let message = format!("Failed to load sink container from {path}: {error}");
                    error!("Sink: {name} ({key}) - {message}");
                    failed_plugins.push(FailedPlugin::new(
                        plugin_id,
                        &key,
                        &name,
                        &config.path,
                        config.plugin_config_format,
                        config.enabled,
                        message,
                    ));
                    continue;
                }
            };
            info!("Sink container for plugin: {path} loaded successfully.");
            sink_connectors.insert(
                path.clone(),
                SinkConnector {
                    container,
                    plugins: Vec::new(),
                },
            );
        } else {
            info!("Sink container for plugin: {path} is already loaded.");
        }

        let connector = sink_connectors
            .get_mut(&path)
            .expect("sink container was just ensured for this path");
        let version = get_plugin_version(&connector.container);
        let init_error = init_sink(
            &connector.container,
            &config.plugin_config.clone().unwrap_or_default(),
            plugin_id,
        )
        .err()
        .map(|error| error.to_string());

        connector.plugins.push(SinkConnectorPlugin {
            id: plugin_id,
            key: key.clone(),
            name: name.clone(),
            path: path.clone(),
            version,
            config_format: config.plugin_config_format,
            consumers: vec![],
            error: init_error.clone(),
            verbose: config.verbose,
            benchmark: config.benchmark,
        });

        if let Some(error) = init_error {
            error!("Failed to initialize sink container with name: {name} ({key}). {error}");
            continue;
        }

        match setup_sink_consumers(&key, &config, iggy_client).await {
            Ok(consumers) => {
                let connector = sink_connectors
                    .get_mut(&path)
                    .expect("sink connector was inserted above");
                let plugin = connector
                    .plugins
                    .iter_mut()
                    .find(|plugin| plugin.id == plugin_id)
                    .expect("sink plugin was pushed above");
                for (consumer, decoder, batch_size, transforms) in consumers {
                    plugin.consumers.push(SinkConnectorConsumer {
                        consumer,
                        decoder,
                        batch_size,
                        transforms,
                    });
                }
                info!(
                    "Sink container with name: {name} ({key}) initialized successfully with ID: {plugin_id}."
                );
            }
            Err(error) => {
                let message = format!("Failed to set up sink consumers: {error}");
                error!("Sink: {name} ({key}) - {message}");
                let connector = sink_connectors
                    .get_mut(&path)
                    .expect("sink connector was inserted above");
                let close_result = (connector.container.iggy_sink_close)(plugin_id);
                if close_result != 0 {
                    warn!(
                        "iggy_sink_close returned {close_result} while cleaning up failed sink connector with ID: {plugin_id} ({key})"
                    );
                }
                if let Some(plugin) = connector
                    .plugins
                    .iter_mut()
                    .find(|plugin| plugin.id == plugin_id)
                {
                    plugin.error = Some(message);
                }
            }
        }
    }

    Ok((sink_connectors, failed_plugins))
}

pub fn consume(
    sinks: Vec<SinkConnectorWrapper>,
    context: Arc<RuntimeContext>,
) -> Vec<(String, watch::Sender<()>, Vec<JoinHandle<()>>)> {
    let mut handles = Vec::new();
    for sink in sinks {
        for plugin in sink.plugins {
            if let Some(error) = &plugin.error {
                error!(
                    "Failed to initialize sink connector with ID: {}: {error}. Skipping...",
                    plugin.id,
                );
                continue;
            }
            info!("Starting consume for sink with ID: {}...", plugin.id);
            let consumers = plugin
                .consumers
                .into_iter()
                .map(|c| (c.consumer, c.decoder, c.batch_size, c.transforms))
                .collect();
            let (shutdown_tx, task_handles) = spawn_consume_tasks(
                plugin.id,
                &plugin.key,
                consumers,
                sink.callback,
                plugin.verbose,
                plugin.benchmark,
                &context.metrics,
                context.clone(),
            );
            handles.push((plugin.key, shutdown_tx, task_handles));
        }
    }
    handles
}

#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub(crate) fn spawn_consume_tasks(
    plugin_id: u32,
    plugin_key: &str,
    consumers: Vec<(
        IggyConsumer,
        Arc<dyn StreamDecoder>,
        u32,
        Vec<Arc<dyn Transform>>,
    )>,
    callback: ConsumeCallback,
    verbose: bool,
    benchmark: bool,
    metrics: &Arc<Metrics>,
    context: Arc<RuntimeContext>,
) -> (watch::Sender<()>, Vec<JoinHandle<()>>) {
    if benchmark {
        info!(
            "Benchmark mode enabled for sink connector with ID: {plugin_id}, key: {plugin_key}. \
             Per-batch events on target 'iggy_connectors::benchmark'."
        );
    }
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let mut task_handles = Vec::new();
    let labels = Arc::new(SinkLabels::new(plugin_key));
    for (consumer, decoder, batch_size, transforms) in consumers {
        let plugin_key = plugin_key.to_string();
        let metrics = metrics.clone();
        let shutdown_rx = shutdown_rx.clone();
        let context = context.clone();
        let labels = labels.clone();
        let handle = tokio::spawn(async move {
            if let Err(error) = consume_messages(
                plugin_id,
                decoder,
                batch_size,
                callback,
                transforms,
                consumer,
                verbose,
                benchmark,
                &plugin_key,
                &metrics,
                &labels,
                shutdown_rx,
            )
            .await
            {
                error!(
                    "Failed to consume messages for sink connector with ID: {plugin_id}: {error}"
                );
                metrics.inc_errors_with_labels(&labels.counter);
                context
                    .sinks
                    .set_error(&plugin_key, &error.to_string())
                    .await;
            }
        });
        task_handles.push(handle);
    }
    (shutdown_tx, task_handles)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn consume_messages(
    plugin_id: u32,
    decoder: Arc<dyn StreamDecoder>,
    batch_size: u32,
    consume: ConsumeCallback,
    transforms: Vec<Arc<dyn Transform>>,
    mut consumer: IggyConsumer,
    verbose: bool,
    benchmark: bool,
    plugin_key: &str,
    metrics: &Arc<Metrics>,
    labels: &SinkLabels,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<(), RuntimeError> {
    info!("Started consuming messages for sink connector with ID: {plugin_id}");
    let batch_size = batch_size as usize;
    let mut batch = Vec::with_capacity(batch_size);
    let topic_metadata = TopicMetadata {
        stream: consumer.stream().to_string(),
        topic: consumer.topic().to_string(),
    };

    loop {
        let message = tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("Sink connector with ID: {plugin_id} received shutdown signal");
                break;
            }
            msg = consumer.next() => msg,
        };

        let Some(message) = message else {
            break;
        };
        let Ok(message) = message else {
            error!(
                "Failed to receive message for sink connector with ID: {plugin_id} from stream: {}, topic: {}",
                topic_metadata.stream, topic_metadata.topic
            );
            metrics.inc_errors_with_labels(&labels.counter);
            continue;
        };

        let partition_id = message.partition_id;
        let current_offset = message.current_offset;
        let message_offset = message.message.header.offset;
        batch.push(message.message);
        if current_offset != message_offset && batch.len() < batch_size {
            continue;
        }

        let messages = std::mem::take(&mut batch);
        let messages_count = messages.len();
        metrics.inc_messages_consumed_with_labels(&labels.counter, messages_count as u64);
        let messages_metadata = MessagesMetadata {
            partition_id,
            current_offset,
            schema: decoder.schema(),
        };
        if verbose {
            info!(
                "Processing {messages_count} messages for sink connector with ID: {}",
                plugin_id
            );
        } else {
            debug!(
                "Processing {messages_count} messages for sink connector with ID: {}",
                plugin_id
            );
        }
        let start = Instant::now();
        let result = process_messages(
            plugin_id,
            messages_metadata,
            &topic_metadata,
            messages,
            &consume,
            &transforms,
            &decoder,
            metrics,
            labels,
        )
        .await;
        let elapsed = start.elapsed();
        // Total always records; sub-stages only on success (no 0-sample skew).
        metrics.observe_stage_with_labels(&labels.stage_total, elapsed);

        let (processed_count, decode_us, prepare_us, ffi_us) = match &result {
            Ok(timing) => {
                let prepare_elapsed = elapsed
                    .saturating_sub(timing.ffi_elapsed)
                    .saturating_sub(timing.decode_elapsed);
                metrics.observe_stage_with_labels(&labels.stage_decode, timing.decode_elapsed);
                metrics.observe_stage_with_labels(&labels.stage_prepare, prepare_elapsed);
                metrics.observe_stage_with_labels(&labels.stage_ffi, timing.ffi_elapsed);
                (
                    timing.processed_count,
                    benchmark::as_micros(timing.decode_elapsed),
                    benchmark::as_micros(prepare_elapsed),
                    benchmark::as_micros(timing.ffi_elapsed),
                )
            }
            Err(_) => (0, 0, 0, 0),
        };

        if benchmark {
            benchmark::emit_sink_event(
                plugin_key,
                &topic_metadata.stream,
                &topic_metadata.topic,
                partition_id,
                current_offset,
                messages_count,
                processed_count,
                decode_us,
                prepare_us,
                ffi_us,
                benchmark::as_micros(elapsed),
            );
        }

        if let Err(error) = result {
            error!(
                "Failed to process {messages_count} messages for sink connector with ID: {plugin_id}. {error}",
            );
            return Err(error);
        }

        metrics.inc_messages_processed_with_labels(&labels.counter, processed_count as u64);
        if verbose {
            info!(
                "Consumed {messages_count} messages in {:#?} for sink connector with ID: {plugin_id}",
                elapsed
            );
        } else {
            debug!(
                "Consumed {messages_count} messages in {:#?} for sink connector with ID: {plugin_id}",
                elapsed
            );
        }
    }
    info!("Stopped consuming messages for sink connector with ID: {plugin_id}");
    Ok(())
}

fn get_plugin_version(container: &Container<SinkApi>) -> String {
    unsafe {
        let version_ptr = (container.iggy_sink_version)();
        std::ffi::CStr::from_ptr(version_ptr)
            .to_string_lossy()
            .into_owned()
    }
}

pub(crate) fn init_sink(
    container: &Container<SinkApi>,
    plugin_config: &serde_json::Value,
    id: u32,
) -> Result<(), RuntimeError> {
    let plugin_config = serde_json::to_string(plugin_config).expect("Invalid sink plugin config.");
    let result = (container.iggy_sink_open)(
        id,
        plugin_config.as_ptr(),
        plugin_config.len(),
        LOG_CALLBACK,
    );
    if result != 0 {
        let error = format!("Plugin initialization failed (ID: {id})");
        error!("{error}");
        Err(RuntimeError::InvalidConfiguration(error))
    } else {
        Ok(())
    }
}

pub(crate) async fn setup_sink_consumers(
    key: &str,
    config: &SinkConfig,
    iggy_client: &IggyClient,
) -> Result<
    Vec<(
        IggyConsumer,
        Arc<dyn StreamDecoder>,
        u32,
        Vec<Arc<dyn Transform>>,
    )>,
    RuntimeError,
> {
    let transforms = if let Some(transforms_config) = &config.transforms {
        let loaded = transform::load(transforms_config).map_err(|error| {
            RuntimeError::InvalidConfiguration(format!("Failed to load transforms: {error}"))
        })?;
        for t in &loaded {
            info!("Loaded transform: {:?} for sink: {key}", t.r#type());
        }
        loaded
    } else {
        vec![]
    };

    let mut consumers = Vec::new();
    for stream in config.streams.iter() {
        let poll_interval = IggyDuration::from_str(
            stream.poll_interval.as_deref().unwrap_or("5ms"),
        )
        .map_err(|error| {
            RuntimeError::InvalidConfiguration(format!("Invalid poll interval: {error}"))
        })?;
        let default_consumer_group = format!("iggy-connect-sink-{key}");
        let consumer_group = stream
            .consumer_group
            .as_deref()
            .unwrap_or(&default_consumer_group);
        let batch_length = stream.batch_length.unwrap_or(1000);
        for topic in stream.topics.iter() {
            let mut consumer = iggy_client
                .consumer_group(consumer_group, &stream.stream, topic)?
                .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
                .create_consumer_group_if_not_exists()
                .auto_join_consumer_group()
                .polling_strategy(PollingStrategy::next())
                .poll_interval(poll_interval)
                .batch_length(batch_length)
                .build();
            consumer.init().await?;
            let decoder: Arc<dyn StreamDecoder> = match stream.schema {
                Schema::Avro => {
                    let config = AvroConfig {
                        schema_json: stream.avro_schema_json.clone(),
                        schema_path: stream.avro_schema_path.clone(),
                        ..AvroConfig::default()
                    };
                    Arc::new(AvroStreamDecoder::try_new(config).map_err(|error| {
                        RuntimeError::InvalidConfiguration(format!(
                            "Failed to create Avro decoder for stream '{}': {error}",
                            stream.stream
                        ))
                    })?)
                }
                other => other.decoder(),
            };
            consumers.push((consumer, decoder, batch_length, transforms.clone()));
        }
    }
    Ok(consumers)
}

#[allow(clippy::too_many_arguments)]
async fn process_messages(
    plugin_id: u32,
    messages_metadata: MessagesMetadata,
    topic_metadata: &TopicMetadata,
    messages: Vec<IggyMessage>,
    consume: &ConsumeCallback,
    transforms: &Vec<Arc<dyn Transform>>,
    decoder: &Arc<dyn StreamDecoder>,
    metrics: &Arc<Metrics>,
    labels: &SinkLabels,
) -> Result<SinkBatchTiming, RuntimeError> {
    let received = messages.into_iter().map(|message| ReceivedMessage {
        id: message.header.id,
        offset: message.header.offset,
        checksum: message.header.checksum,
        timestamp: message.header.timestamp,
        origin_timestamp: message.header.origin_timestamp,
        headers: message.user_headers_map().unwrap_or(None),
        payload: message.payload.into(),
    });

    let count = received.len();
    // Per-message drops are accumulated and flushed once after the loops to
    // avoid a Family lookup per message under decode/transform/error storms.
    let mut error_count = 0u64;
    let mut filtered_count = 0u64;

    // Decode is timed separately from transform + serialize so the sink's
    // stage="decode" / stage="prepare" labels mean the same as the source's.
    let decode_start = Instant::now();
    let mut decoded = Vec::with_capacity(count);
    for message in received {
        let Ok(payload) = decoder.decode(message.payload) else {
            error!(
                "Failed to decode message payload (id: {}, offset: {}) for sink connector with ID: {plugin_id}",
                message.id, message.offset
            );
            error_count += 1;
            continue;
        };
        decoded.push(DecodedMessage {
            id: Some(message.id),
            offset: Some(message.offset),
            checksum: Some(message.checksum),
            timestamp: Some(message.timestamp),
            origin_timestamp: Some(message.origin_timestamp),
            headers: message.headers,
            payload,
        });
    }
    let decode_elapsed = decode_start.elapsed();

    let mut batches = Vec::<RawMessages>::new();
    for message in decoded {
        let mut current_message = Some(message);
        let mut transform_failed = false;
        for transform in transforms.iter() {
            let Some(message) = current_message.take() else {
                break;
            };
            // Drop-and-continue on a single bad message, mirroring the source
            // side - one malformed payload must not kill the whole batch.
            match transform.transform(topic_metadata, message) {
                Ok(next) => current_message = next,
                Err(error) => {
                    error!(
                        "Transform '{:?}' failed for sink connector with ID: {plugin_id}, stream: {}, topic: {}: {error}",
                        transform.r#type(),
                        topic_metadata.stream,
                        topic_metadata.topic
                    );
                    error_count += 1;
                    transform_failed = true;
                    break;
                }
            }
        }

        if transform_failed {
            continue;
        }

        // Filter contract: transform returning Ok(None) is an intentional drop.
        let Some(message) = current_message else {
            filtered_count += 1;
            continue;
        };

        let Some(id) = message.id else {
            error!(
                "ID should be present. Failed to process message for sink connector with ID: {plugin_id}"
            );
            error_count += 1;
            continue;
        };

        let Some(offset) = message.offset else {
            error!(
                "Offset should be present. Failed to process message with ID: {id} for sink connector with ID: {plugin_id}"
            );
            error_count += 1;
            continue;
        };

        let Some(checksum) = message.checksum else {
            error!(
                "Checksum should be present. Failed to process message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}"
            );
            error_count += 1;
            continue;
        };

        let Some(timestamp) = message.timestamp else {
            error!(
                "Timestamp should be present. Failed to process message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}"
            );
            error_count += 1;
            continue;
        };

        let Some(origin_timestamp) = message.origin_timestamp else {
            error!(
                "Origin timestamp should be present. Failed to process message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}"
            );
            error_count += 1;
            continue;
        };

        let payload_schema = message.payload.schema();
        let Ok(payload) = message.payload.try_into_vec() else {
            error!(
                "Failed to get message payload for message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}"
            );
            error_count += 1;
            continue;
        };

        let headers = match message.headers {
            Some(headers) => match postcard::to_allocvec(&headers) {
                Ok(bytes) => bytes,
                Err(error) => {
                    error!(
                        "Failed to serialize headers for message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}. {error}"
                    );
                    error_count += 1;
                    continue;
                }
            },
            None => vec![],
        };

        let raw_message = RawMessage {
            id,
            offset,
            checksum,
            timestamp,
            origin_timestamp,
            headers,
            payload,
        };
        match batches.last_mut() {
            Some(batch) if batch.schema == payload_schema => batch.messages.push(raw_message),
            _ => batches.push(RawMessages {
                schema: payload_schema,
                messages: vec![raw_message],
            }),
        }
    }

    metrics.inc_errors_by_with_labels(&labels.counter, error_count);
    if filtered_count > 0 {
        metrics.inc_messages_filtered_with_labels(&labels.counter, filtered_count);
    }

    let processed_count = batches.iter().map(|batch| batch.messages.len()).sum();
    if batches.is_empty() {
        batches.push(RawMessages {
            schema: messages_metadata.schema,
            messages: Vec::new(),
        });
    }

    let topic_meta = postcard::to_allocvec(topic_metadata).map_err(|error| {
        error!(
            "Failed to serialize topic metadata for sink connector with ID: {plugin_id}. {error}"
        );
        RuntimeError::FailedToSerializeTopicMetadata
    })?;

    let mut ffi_elapsed = Duration::ZERO;
    for batch in batches {
        let messages_metadata = MessagesMetadata {
            partition_id: messages_metadata.partition_id,
            current_offset: messages_metadata.current_offset,
            schema: batch.schema,
        };
        let messages_meta = postcard::to_allocvec(&messages_metadata).map_err(|error| {
            error!(
                "Failed to serialize messages metadata for sink connector with ID: {plugin_id}. {error}"
            );
            RuntimeError::FailedToSerializeMessagesMetadata
        })?;
        let messages = postcard::to_allocvec(&batch).map_err(|error| {
            error!("Failed to serialize messages for sink connector with ID: {plugin_id}. {error}");
            RuntimeError::FailedToSerializeRawMessages
        })?;

        let ffi_start = Instant::now();
        (consume)(
            plugin_id,
            topic_meta.as_ptr(),
            topic_meta.len(),
            messages_meta.as_ptr(),
            messages_meta.len(),
            messages.as_ptr(),
            messages.len(),
        );
        ffi_elapsed += ffi_start.elapsed();
    }

    Ok(SinkBatchTiming {
        processed_count,
        decode_elapsed,
        ffi_elapsed,
    })
}

struct SinkBatchTiming {
    processed_count: usize,
    decode_elapsed: Duration,
    ffi_elapsed: Duration,
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Mutex, thread};

    use async_trait::async_trait;
    use iggy_connector_sdk::{
        ConsumedMessage, Error, Payload, Sink,
        sink::SinkContainer,
        transforms::{ProtoConvert, ProtoConvertConfig, TransformType},
    };

    use super::*;

    static CAPTURED_BATCHES: Mutex<Vec<(u32, CapturedBatch)>> = Mutex::new(Vec::new());
    static CONSUMED_BATCHES: Mutex<Vec<(u32, ConsumedBatch)>> = Mutex::new(Vec::new());
    static SINK_CONTAINER: Mutex<Option<SinkContainer<RecordingSink>>> = Mutex::new(None);

    struct CapturedBatch {
        metadata_schema: Schema,
        batch_schema: Schema,
        offsets: Vec<u64>,
        payloads: Vec<Vec<u8>>,
    }

    struct ConsumedBatch {
        metadata_schema: Schema,
        offsets: Vec<u64>,
    }

    #[derive(Debug)]
    struct RecordingSink {
        plugin_id: u32,
    }

    #[async_trait]
    impl Sink for RecordingSink {
        async fn open(&mut self) -> Result<(), Error> {
            Ok(())
        }

        async fn consume(
            &self,
            _topic_metadata: &TopicMetadata,
            messages_metadata: MessagesMetadata,
            messages: Vec<ConsumedMessage>,
        ) -> Result<(), Error> {
            CONSUMED_BATCHES
                .lock()
                .expect("consumed batches lock should succeed")
                .push((
                    self.plugin_id,
                    ConsumedBatch {
                        metadata_schema: messages_metadata.schema,
                        offsets: messages.into_iter().map(|message| message.offset).collect(),
                    },
                ));
            Ok(())
        }

        async fn close(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    struct RawToText;

    impl Transform for RawToText {
        fn r#type(&self) -> TransformType {
            TransformType::ProtoConvert
        }

        fn transform(
            &self,
            _metadata: &TopicMetadata,
            message: DecodedMessage,
        ) -> Result<Option<DecodedMessage>, Error> {
            let Payload::Raw(payload) = message.payload else {
                return Ok(Some(message));
            };
            let text = String::from_utf8(payload).map_err(|_| Error::InvalidTextPayload)?;
            Ok(Some(DecodedMessage {
                payload: Payload::Text(text),
                ..message
            }))
        }
    }

    struct FailingTransform;

    impl Transform for FailingTransform {
        fn r#type(&self) -> TransformType {
            TransformType::ProtoConvert
        }

        fn transform(
            &self,
            _metadata: &TopicMetadata,
            _message: DecodedMessage,
        ) -> Result<Option<DecodedMessage>, Error> {
            Err(Error::InvalidPayloadType)
        }
    }

    extern "C" fn capture_schemas(
        plugin_id: u32,
        _topic_meta_ptr: *const u8,
        _topic_meta_len: usize,
        messages_meta_ptr: *const u8,
        messages_meta_len: usize,
        messages_ptr: *const u8,
        messages_len: usize,
    ) -> i32 {
        unsafe {
            let messages_metadata = postcard::from_bytes::<MessagesMetadata>(
                std::slice::from_raw_parts(messages_meta_ptr, messages_meta_len),
            )
            .expect("messages metadata should deserialize");
            let raw_messages = postcard::from_bytes::<RawMessages>(std::slice::from_raw_parts(
                messages_ptr,
                messages_len,
            ))
            .expect("raw messages should deserialize");
            CAPTURED_BATCHES
                .lock()
                .expect("capture lock should succeed")
                .push((
                    plugin_id,
                    CapturedBatch {
                        metadata_schema: messages_metadata.schema,
                        batch_schema: raw_messages.schema,
                        offsets: raw_messages
                            .messages
                            .iter()
                            .map(|message| message.offset)
                            .collect(),
                        payloads: raw_messages
                            .messages
                            .into_iter()
                            .map(|message| message.payload)
                            .collect(),
                    },
                ));
        }
        0
    }

    extern "C" fn consume_through_sdk(
        _plugin_id: u32,
        topic_meta_ptr: *const u8,
        topic_meta_len: usize,
        messages_meta_ptr: *const u8,
        messages_meta_len: usize,
        messages_ptr: *const u8,
        messages_len: usize,
    ) -> i32 {
        let (topic_metadata, messages_metadata, messages) = unsafe {
            (
                std::slice::from_raw_parts(topic_meta_ptr, topic_meta_len).to_vec(),
                std::slice::from_raw_parts(messages_meta_ptr, messages_meta_len).to_vec(),
                std::slice::from_raw_parts(messages_ptr, messages_len).to_vec(),
            )
        };
        thread::spawn(move || {
            let container = SINK_CONTAINER
                .lock()
                .expect("sink container lock should succeed");
            let Some(container) = container.as_ref() else {
                return -1;
            };
            unsafe {
                container.consume(
                    topic_metadata.as_ptr(),
                    topic_metadata.len(),
                    messages_metadata.as_ptr(),
                    messages_metadata.len(),
                    messages.as_ptr(),
                    messages.len(),
                )
            }
        })
        .join()
        .expect("SDK consume thread should succeed")
    }

    extern "C" fn discard_log(
        _level: u8,
        _target_ptr: *const u8,
        _target_len: usize,
        _message_ptr: *const u8,
        _message_len: usize,
    ) {
    }

    fn take_captured_batches(plugin_id: u32) -> Vec<CapturedBatch> {
        let mut captured = CAPTURED_BATCHES
            .lock()
            .expect("capture lock should succeed");
        let all_batches = std::mem::take(&mut *captured);
        let (matching, remaining): (Vec<_>, Vec<_>) = all_batches
            .into_iter()
            .partition(|(captured_plugin_id, _)| *captured_plugin_id == plugin_id);
        *captured = remaining;
        matching.into_iter().map(|(_, batch)| batch).collect()
    }

    fn take_consumed_batches(plugin_id: u32) -> Vec<ConsumedBatch> {
        let mut consumed = CONSUMED_BATCHES
            .lock()
            .expect("consumed batches lock should succeed");
        let all_batches = std::mem::take(&mut *consumed);
        let (matching, remaining): (Vec<_>, Vec<_>) = all_batches
            .into_iter()
            .partition(|(consumed_plugin_id, _)| *consumed_plugin_id == plugin_id);
        *consumed = remaining;
        matching.into_iter().map(|(_, batch)| batch).collect()
    }

    fn proto_convert() -> Arc<dyn Transform> {
        let schema_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../sdk/examples");
        let schema_path = schema_dir.join("user.proto");
        Arc::new(ProtoConvert::new(ProtoConvertConfig {
            source_format: Schema::Json,
            target_format: Schema::Proto,
            schema_path: Some(schema_path),
            message_type: Some("com.example.User".to_string()),
            include_paths: vec![schema_dir],
            ..ProtoConvertConfig::default()
        }))
    }

    fn mixed_schema_messages() -> Vec<IggyMessage> {
        vec![
            message(1, 1, "[]"),
            message(2, 2, r#"{"id":2,"name":"second"}"#),
            message(3, 3, r#"{"id":3,"name":"third"}"#),
            message(4, 4, "null"),
            message(5, 5, r#"{"id":5,"name":"fifth"}"#),
        ]
    }

    fn message(id: u128, offset: u64, payload: &'static str) -> IggyMessage {
        let mut message = IggyMessage::builder()
            .id(id)
            .payload(payload.into())
            .build()
            .expect("message should build");
        message.header.offset = offset;
        message
    }

    #[tokio::test]
    async fn given_transform_changes_schema_when_crossing_ffi_should_use_output_schema() {
        let metrics = Arc::new(Metrics::init());
        let labels = SinkLabels::new("schema-aware");
        let decoder: Arc<dyn StreamDecoder> = Schema::Raw.decoder();
        let transforms: Vec<Arc<dyn Transform>> = vec![Arc::new(RawToText)];
        let consume: ConsumeCallback = capture_schemas;
        let plugin_id = 1;
        let message = message(1, 0, "transformed");

        let timing = process_messages(
            plugin_id,
            MessagesMetadata {
                partition_id: 1,
                current_offset: 0,
                schema: Schema::Raw,
            },
            &TopicMetadata {
                stream: "stream".to_string(),
                topic: "topic".to_string(),
            },
            vec![message],
            &consume,
            &transforms,
            &decoder,
            &metrics,
            &labels,
        )
        .await
        .expect("message processing should succeed");

        let mut captured = take_captured_batches(plugin_id);
        assert_eq!(captured.len(), 1);
        let captured = captured.pop().expect("FFI callback should capture schemas");
        assert_eq!(timing.processed_count, 1);
        assert_eq!(captured.metadata_schema, Schema::Text);
        assert_eq!(captured.batch_schema, Schema::Text);
        assert_eq!(captured.offsets, vec![0]);
        assert_eq!(captured.payloads, vec![b"transformed".to_vec()]);
    }

    #[tokio::test]
    async fn given_mixed_transform_schemas_when_crossing_ffi_should_preserve_all_messages_in_order()
    {
        let plugin_id = 2;
        let metrics = Arc::new(Metrics::init());
        let labels = SinkLabels::new("mixed-schema");
        let decoder: Arc<dyn StreamDecoder> = Schema::Json.decoder();
        let transforms = vec![proto_convert()];
        let consume: ConsumeCallback = capture_schemas;

        let timing = process_messages(
            plugin_id,
            MessagesMetadata {
                partition_id: 1,
                current_offset: 5,
                schema: Schema::Json,
            },
            &TopicMetadata {
                stream: "stream".to_string(),
                topic: "topic".to_string(),
            },
            mixed_schema_messages(),
            &consume,
            &transforms,
            &decoder,
            &metrics,
            &labels,
        )
        .await
        .expect("message processing should succeed");

        let captured = take_captured_batches(plugin_id);
        assert_eq!(timing.processed_count, 5);
        assert_eq!(
            metrics.get_errors("mixed-schema", crate::metrics::ConnectorType::Sink),
            0
        );
        assert_eq!(captured.len(), 4);
        assert_eq!(
            captured
                .iter()
                .map(|batch| batch.metadata_schema)
                .collect::<Vec<_>>(),
            vec![Schema::Proto, Schema::Raw, Schema::Proto, Schema::Raw]
        );
        assert!(
            captured
                .iter()
                .all(|batch| batch.metadata_schema == batch.batch_schema)
        );
        assert_eq!(
            captured
                .iter()
                .flat_map(|batch| batch.offsets.iter().copied())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
    }

    #[tokio::test]
    async fn given_transform_failure_when_processing_should_count_error_without_filtering() {
        let plugin_id = 3;
        let connector_key = "transform-failure";
        let metrics = Arc::new(Metrics::init());
        let labels = SinkLabels::new(connector_key);
        let decoder: Arc<dyn StreamDecoder> = Schema::Raw.decoder();
        let transforms: Vec<Arc<dyn Transform>> = vec![Arc::new(FailingTransform)];
        let consume: ConsumeCallback = capture_schemas;

        let timing = process_messages(
            plugin_id,
            MessagesMetadata {
                partition_id: 1,
                current_offset: 1,
                schema: Schema::Raw,
            },
            &TopicMetadata {
                stream: "stream".to_string(),
                topic: "topic".to_string(),
            },
            vec![message(1, 1, "invalid")],
            &consume,
            &transforms,
            &decoder,
            &metrics,
            &labels,
        )
        .await
        .expect("message processing should succeed");

        assert_eq!(timing.processed_count, 0);
        assert_eq!(
            metrics.get_errors(connector_key, crate::metrics::ConnectorType::Sink),
            1
        );
        assert_eq!(
            metrics.get_messages_filtered(connector_key, crate::metrics::ConnectorType::Sink),
            0
        );
        take_captured_batches(plugin_id);
    }

    #[test]
    fn given_mixed_transform_schemas_when_consumed_by_sdk_should_reconstruct_every_message() {
        let plugin_id = 4;
        let metrics = Arc::new(Metrics::init());
        let labels = SinkLabels::new("sdk-schema-reconstruction");
        let decoder: Arc<dyn StreamDecoder> = Schema::Json.decoder();
        let transforms = vec![proto_convert()];
        let consume: ConsumeCallback = consume_through_sdk;
        let config = b"{}";
        let mut container = SinkContainer::new(plugin_id);
        let open_result = unsafe {
            container.open::<_, serde_json::Value>(
                plugin_id,
                config.as_ptr(),
                config.len(),
                discard_log,
                |plugin_id, _config| RecordingSink { plugin_id },
            )
        };
        assert_eq!(open_result, 0);
        *SINK_CONTAINER
            .lock()
            .expect("sink container lock should succeed") = Some(container);

        let runtime = tokio::runtime::Runtime::new().expect("test runtime should initialize");
        let timing = runtime
            .block_on(process_messages(
                plugin_id,
                MessagesMetadata {
                    partition_id: 1,
                    current_offset: 5,
                    schema: Schema::Json,
                },
                &TopicMetadata {
                    stream: "stream".to_string(),
                    topic: "topic".to_string(),
                },
                mixed_schema_messages(),
                &consume,
                &transforms,
                &decoder,
                &metrics,
                &labels,
            ))
            .expect("message processing should succeed");
        drop(runtime);

        let consumed = take_consumed_batches(plugin_id);
        assert_eq!(timing.processed_count, 5);
        assert_eq!(consumed.len(), 4);
        assert_eq!(
            consumed
                .iter()
                .map(|batch| batch.metadata_schema)
                .collect::<Vec<_>>(),
            vec![Schema::Proto, Schema::Raw, Schema::Proto, Schema::Raw]
        );
        assert_eq!(
            consumed
                .iter()
                .flat_map(|batch| batch.offsets.iter().copied())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );

        let mut container = SINK_CONTAINER
            .lock()
            .expect("sink container lock should succeed")
            .take()
            .expect("sink container should be initialized");
        assert_eq!(unsafe { container.close() }, 0);
    }
}
