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

use dashmap::DashMap;
use dlopen2::wrapper::Container;
use flume::{Receiver, Sender};
use iggy::prelude::{
    DirectConfig, HeaderKey, HeaderValue, IggyClient, IggyDuration, IggyError, IggyMessage,
    IggyProducer,
};
use iggy_connector_sdk::encoders::avro::{AvroEncoderConfig, AvroStreamEncoder};
use iggy_connector_sdk::{
    ConnectorState, DecodedMessage, ProducedMessages, Schema, StreamEncoder, TopicMetadata,
    source::HandleCallback, transforms::Transform,
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::{Arc, LazyLock, atomic::Ordering},
    time::Instant,
};
use tracing::{debug, error, info, trace, warn};

use crate::benchmark;
use crate::configs::connectors::SourceConfig;
use crate::context::RuntimeContext;
use crate::log::LOG_CALLBACK;
use crate::metrics::SourceLabels;
use crate::{
    FailedPlugin, PLUGIN_ID, RuntimeError, SourceApi, SourceConnector, SourceConnectorPlugin,
    SourceConnectorProducer, SourceConnectorWrapper, resolve_plugin_path,
    state::{FileStateProvider, StateProvider, StateStorage},
    transform,
};
use iggy_connector_sdk::api::ConnectorStatus;
use prometheus_client::metrics::counter::Counter;
use tokio::task::JoinHandle;

pub(crate) struct SourceSenderEntry {
    pub(crate) sender: Sender<ProducedMessages>,
    // Owned errors counter (Arc<AtomicU64> inside) so the FFI callback bumps
    // it with one relaxed atomic - no Family RwLock + HashMap lookup per call.
    pub(crate) error_counter: Counter,
}

pub(crate) static SOURCE_SENDERS: LazyLock<DashMap<u32, SourceSenderEntry>> =
    LazyLock::new(DashMap::new);

pub(crate) fn cleanup_sender(plugin_id: u32) {
    SOURCE_SENDERS.remove(&plugin_id);
}

/// Initializes all enabled source connectors.
///
/// Per-connector failures (path resolution, dlopen, state load, plugin init,
/// producer/encoder/transform setup) are captured against the offending
/// connector and do not abort the runtime. Connectors that fail before their
/// FFI container can be loaded are returned in the second tuple element so
/// they remain visible in health/status output.
///
/// Only system-level errors that prevent any connector from running (e.g. a
/// poisoned global state) are propagated as `Err`.
pub async fn init(
    source_configs: HashMap<String, SourceConfig>,
    iggy_client: &IggyClient,
    state_path: &str,
) -> Result<(HashMap<String, SourceConnector>, Vec<FailedPlugin>), RuntimeError> {
    let mut source_connectors: HashMap<String, SourceConnector> = HashMap::new();
    let mut failed_plugins: Vec<FailedPlugin> = Vec::new();

    for (key, config) in source_configs {
        let name = config.name.clone();
        if !config.enabled {
            warn!("Source: {name} is disabled ({key})");
            continue;
        }

        let plugin_id = PLUGIN_ID.fetch_add(1, Ordering::SeqCst);

        let path = match resolve_plugin_path(&config.path) {
            Ok(path) => path,
            Err(error) => {
                let message = format!("Failed to resolve plugin path: {error}");
                error!("Source: {name} ({key}) - {message}");
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
            "Initializing source container with name: {name} ({key}), config version: {}, plugin: {path}",
            &config.version
        );

        let state_storage = get_state_storage(state_path, &key);
        let state = match &state_storage {
            StateStorage::File(file) => match file.load().await {
                Ok(state) => state,
                Err(error) => {
                    let message = format!("Failed to load source state: {error}");
                    error!("Source: {name} ({key}) - {message}");
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
            },
        };

        if !source_connectors.contains_key(&path) {
            let container = match unsafe { Container::<SourceApi>::load(&path) } {
                Ok(container) => container,
                Err(error) => {
                    let message = format!("Failed to load source container from {path}: {error}");
                    error!("Source: {name} ({key}) - {message}");
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
            info!("Source container for plugin: {path} loaded successfully.");
            source_connectors.insert(
                path.clone(),
                SourceConnector {
                    container,
                    plugins: Vec::new(),
                },
            );
        } else {
            info!("Source container for plugin: {path} is already loaded.");
        }

        let connector = source_connectors
            .get_mut(&path)
            .expect("source container was just ensured for this path");
        let version = get_plugin_version(&connector.container);
        let init_error = init_source(
            &connector.container,
            &config.plugin_config.clone().unwrap_or_default(),
            plugin_id,
            state,
        )
        .err()
        .map(|error| error.to_string());

        connector.plugins.push(SourceConnectorPlugin {
            id: plugin_id,
            key: key.clone(),
            name: name.clone(),
            path: path.clone(),
            version,
            config_format: config.plugin_config_format,
            producer: None,
            transforms: vec![],
            state_storage,
            error: init_error.clone(),
            verbose: config.verbose,
            benchmark: config.benchmark,
        });

        if let Some(error) = init_error {
            error!("Source container with name: {name} ({key}) failed to initialize: {error}");
            continue;
        }

        match setup_source_producer(&key, &config, iggy_client).await {
            Ok((producer, encoder, transforms)) => {
                let connector = source_connectors
                    .get_mut(&path)
                    .expect("source connector was inserted above");
                let plugin = connector
                    .plugins
                    .iter_mut()
                    .find(|plugin| plugin.id == plugin_id)
                    .expect("source plugin was pushed above");
                plugin.producer = Some(SourceConnectorProducer { producer, encoder });
                plugin.transforms = transforms;
                info!(
                    "Source container with name: {name} ({key}) initialized successfully with ID: {plugin_id}."
                );
            }
            Err(error) => {
                let message = format!("Failed to set up source producer: {error}");
                error!("Source: {name} ({key}) - {message}");
                let connector = source_connectors
                    .get_mut(&path)
                    .expect("source connector was inserted above");
                let close_result = (connector.container.iggy_source_close)(plugin_id);
                if close_result != 0 {
                    warn!(
                        "iggy_source_close returned {close_result} while cleaning up failed source connector with ID: {plugin_id} ({key})"
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

    Ok((source_connectors, failed_plugins))
}

fn get_plugin_version(container: &Container<SourceApi>) -> String {
    unsafe {
        let version_ptr = (container.iggy_source_version)();
        std::ffi::CStr::from_ptr(version_ptr)
            .to_string_lossy()
            .into_owned()
    }
}

pub(crate) fn init_source(
    container: &Container<SourceApi>,
    plugin_config: &serde_json::Value,
    id: u32,
    state: Option<ConnectorState>,
) -> Result<(), RuntimeError> {
    trace!("Initializing source plugin with config: {plugin_config:?} (ID: {id})");
    let plugin_config =
        serde_json::to_string(plugin_config).expect("Invalid source plugin config.");
    let state_ptr = state.as_ref().map_or(std::ptr::null(), |s| s.0.as_ptr());
    let state_len = state.as_ref().map_or(0, |s| s.0.len());
    let result = (container.iggy_source_open)(
        id,
        plugin_config.as_ptr(),
        plugin_config.len(),
        state_ptr,
        state_len,
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

pub(crate) fn get_state_storage(state_path: &str, key: &str) -> StateStorage {
    let path = format!("{state_path}/source_{key}.state");
    StateStorage::File(FileStateProvider::new(path))
}

pub(crate) async fn setup_source_producer(
    key: &str,
    config: &SourceConfig,
    iggy_client: &IggyClient,
) -> Result<
    (
        IggyProducer,
        Arc<dyn StreamEncoder>,
        Vec<Arc<dyn Transform>>,
    ),
    RuntimeError,
> {
    let transforms = if let Some(transforms_config) = &config.transforms {
        let loaded = transform::load(transforms_config).map_err(|error| {
            RuntimeError::InvalidConfiguration(format!("Failed to load transforms: {error}"))
        })?;
        for t in &loaded {
            info!("Loaded transform: {:?} for source: {key}", t.r#type());
        }
        loaded
    } else {
        vec![]
    };

    let mut last_producer = None;
    let mut last_encoder = None;
    for stream in config.streams.iter() {
        let linger_time = IggyDuration::from_str(stream.linger_time.as_deref().unwrap_or("5ms"))
            .map_err(|error| {
                RuntimeError::InvalidConfiguration(format!("Invalid linger time: {error}"))
            })?;
        let batch_length = stream.batch_length.unwrap_or(1000);
        let producer = iggy_client
            .producer(&stream.stream, &stream.topic)?
            .direct(
                DirectConfig::builder()
                    .batch_length(batch_length)
                    .linger_time(linger_time)
                    .build(),
            )
            .build();
        producer.init().await?;
        let encoder: Arc<dyn StreamEncoder> = match stream.schema {
            Schema::Avro => {
                let config = AvroEncoderConfig {
                    schema_json: stream.avro_schema_json.clone(),
                    schema_path: stream.avro_schema_path.clone(),
                    ..AvroEncoderConfig::default()
                };
                Arc::new(AvroStreamEncoder::try_new(config).map_err(|error| {
                    RuntimeError::InvalidConfiguration(format!(
                        "Failed to create Avro encoder for stream '{}': {error}",
                        stream.stream
                    ))
                })?)
            }
            other => other.encoder(),
        };
        last_encoder = Some(encoder);
        last_producer = Some(producer);
    }

    let producer = last_producer.ok_or_else(|| {
        RuntimeError::InvalidConfiguration("No streams configured for source".to_string())
    })?;
    let encoder = last_encoder.ok_or_else(|| {
        RuntimeError::InvalidConfiguration("No encoder configured for source".to_string())
    })?;

    Ok((producer, encoder, transforms))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn source_forwarding_loop(
    plugin_id: u32,
    plugin_key: String,
    verbose: bool,
    benchmark: bool,
    producer: IggyProducer,
    encoder: Arc<dyn StreamEncoder>,
    transforms: Vec<Arc<dyn Transform>>,
    state_storage: StateStorage,
    receiver: Receiver<ProducedMessages>,
    context: Arc<RuntimeContext>,
    labels: Arc<SourceLabels>,
) {
    info!("Source connector with ID: {plugin_id} started.");
    if benchmark {
        info!(
            "Benchmark mode enabled for source connector with ID: {plugin_id}, key: {plugin_key}. \
             Per-batch events on target 'iggy_connectors::benchmark'."
        );
    }
    context
        .sources
        .update_status(
            &plugin_key,
            ConnectorStatus::Running,
            Some(&context.metrics),
        )
        .await;

    let mut number = 1u64;
    let topic_metadata = TopicMetadata {
        stream: producer.stream().to_string(),
        topic: producer.topic().to_string(),
    };

    while let Ok(produced_messages) = receiver.recv_async().await {
        let total_start = Instant::now();
        let count = produced_messages.messages.len();
        context
            .metrics
            .inc_messages_produced_with_labels(&labels.counter, count as u64);
        if verbose {
            info!("Source connector with ID: {plugin_id} received {count} messages");
        } else {
            debug!("Source connector with ID: {plugin_id} received {count} messages");
        }
        let schema = produced_messages.schema;
        let mut messages: Vec<DecodedMessage> = Vec::with_capacity(count);
        let mut decode_errors = 0u64;
        let decode_start = Instant::now();
        for message in produced_messages.messages {
            let Ok(payload) = schema.try_into_payload(message.payload) else {
                error!(
                    "Failed to decode message payload with schema: {schema} for source connector with ID: {plugin_id}",
                );
                decode_errors += 1;
                continue;
            };

            debug!(
                "Source connector with ID: {plugin_id}] received message: {number} | schema: {schema} | payload: {payload}"
            );
            messages.push(DecodedMessage {
                id: message.id,
                offset: None,
                headers: message.headers,
                checksum: message.checksum,
                timestamp: message.timestamp,
                origin_timestamp: message.origin_timestamp,
                payload,
            });
            number += 1;
        }
        context
            .metrics
            .inc_errors_by_with_labels(&labels.counter, decode_errors);
        let decode_elapsed = decode_start.elapsed();
        context
            .metrics
            .observe_stage_with_labels(&labels.stage_decode, decode_elapsed);

        let prepare_start = Instant::now();
        let iggy_messages = process_messages(
            plugin_id,
            &encoder,
            &topic_metadata,
            messages,
            &transforms,
            &context.metrics,
            &labels,
        );
        let prepare_elapsed = prepare_start.elapsed();
        context
            .metrics
            .observe_stage_with_labels(&labels.stage_prepare, prepare_elapsed);
        let sent_count = iggy_messages.len();

        let iggy_send_start = Instant::now();
        let send_result = producer.send(iggy_messages).await;
        let iggy_send_elapsed = iggy_send_start.elapsed();
        context
            .metrics
            .observe_stage_with_labels(&labels.stage_iggy_send, iggy_send_elapsed);

        // Total histogram + emit (below) run regardless of send outcome.
        let mut state_save_us: Option<u64> = None;
        if let Err(error) = send_result {
            let error_msg = format!(
                "Failed to send {sent_count} messages to stream: {}, topic: {} by source connector with ID: {plugin_id}. {error}",
                producer.stream(),
                producer.topic(),
            );
            error!("{error_msg}");
            context.metrics.inc_errors_with_labels(&labels.counter);
            context.sources.set_error(&plugin_key, &error_msg).await;
        } else {
            context
                .metrics
                .inc_messages_sent_with_labels(&labels.counter, sent_count as u64);

            if verbose {
                info!(
                    "Sent {sent_count} of {count} messages to stream: {}, topic: {} by source connector with ID: {plugin_id}",
                    producer.stream(),
                    producer.topic()
                );
            } else {
                debug!(
                    "Sent {sent_count} of {count} messages to stream: {}, topic: {} by source connector with ID: {plugin_id}",
                    producer.stream(),
                    producer.topic()
                );
            }

            if let Some(state) = produced_messages.state {
                let state_save_start = Instant::now();
                match &state_storage {
                    StateStorage::File(file) => {
                        if let Err(error) = file.save(state).await {
                            let error_msg = format!(
                                "Failed to save state for source connector with ID: {plugin_id}. {error}"
                            );
                            error!("{error_msg}");
                            context.metrics.inc_errors_with_labels(&labels.counter);
                            context.sources.set_error(&plugin_key, &error_msg).await;
                        } else {
                            debug!("State saved for source connector with ID: {plugin_id}");
                            let state_save_elapsed = state_save_start.elapsed();
                            context.metrics.observe_stage_with_labels(
                                &labels.stage_state_save,
                                state_save_elapsed,
                            );
                            state_save_us = Some(benchmark::as_micros(state_save_elapsed));
                        }
                    }
                }
            } else {
                debug!("No state provided for source connector with ID: {plugin_id}");
            }
        }

        let total_elapsed = total_start.elapsed();
        context
            .metrics
            .observe_stage_with_labels(&labels.stage_total, total_elapsed);

        if benchmark {
            benchmark::emit_source_event(
                &plugin_key,
                &topic_metadata.stream,
                &topic_metadata.topic,
                count,
                sent_count,
                benchmark::as_micros(decode_elapsed),
                benchmark::as_micros(prepare_elapsed),
                benchmark::as_micros(iggy_send_elapsed),
                state_save_us,
                benchmark::as_micros(total_elapsed),
            );
        }
    }

    info!("Source connector with ID: {plugin_id} stopped.");
    context
        .sources
        .update_status(
            &plugin_key,
            ConnectorStatus::Stopped,
            Some(&context.metrics),
        )
        .await;
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_source_handler(
    plugin_id: u32,
    plugin_key: &str,
    verbose: bool,
    benchmark: bool,
    producer: IggyProducer,
    encoder: Arc<dyn StreamEncoder>,
    transforms: Vec<Arc<dyn Transform>>,
    state_storage: StateStorage,
    callback: HandleCallback,
    context: Arc<RuntimeContext>,
) -> Vec<JoinHandle<()>> {
    let (sender, receiver) = flume::unbounded();
    let plugin_key = plugin_key.to_string();
    let labels = Arc::new(SourceLabels::new(&plugin_key));
    SOURCE_SENDERS.insert(
        plugin_id,
        SourceSenderEntry {
            sender,
            error_counter: context.metrics.error_counter(&labels.counter),
        },
    );

    let blocking_handle = tokio::task::spawn_blocking(move || {
        callback(plugin_id, handle_produced_messages);
    });
    let handler_task = tokio::spawn(async move {
        source_forwarding_loop(
            plugin_id,
            plugin_key,
            verbose,
            benchmark,
            producer,
            encoder,
            transforms,
            state_storage,
            receiver,
            context,
            labels,
        )
        .await;
    });

    vec![blocking_handle, handler_task]
}

pub fn handle(
    sources: Vec<SourceConnectorWrapper>,
    context: Arc<RuntimeContext>,
) -> Vec<(String, Vec<JoinHandle<()>>)> {
    let mut handles = Vec::new();
    for source in sources {
        for plugin in source.plugins {
            let plugin_id = plugin.id;
            let plugin_key = plugin.key.clone();

            if let Some(error) = &plugin.error {
                error!(
                    "Failed to initialize source connector with ID: {plugin_id}: {error}. Skipping...",
                );
                continue;
            }
            info!("Starting handler for source connector with ID: {plugin_id}...");

            let Some(producer_wrapper) = plugin.producer else {
                error!("Producer not initialized for source connector with ID: {plugin_id}");
                continue;
            };

            let handler_tasks = spawn_source_handler(
                plugin_id,
                &plugin_key,
                plugin.verbose,
                plugin.benchmark,
                producer_wrapper.producer,
                producer_wrapper.encoder,
                plugin.transforms,
                plugin.state_storage,
                source.callback,
                context.clone(),
            );

            handles.push((plugin_key, handler_tasks));
        }
    }
    handles
}

fn process_messages(
    id: u32,
    encoder: &Arc<dyn StreamEncoder>,
    topic_metadata: &TopicMetadata,
    messages: Vec<DecodedMessage>,
    transforms: &Vec<Arc<dyn Transform>>,
    metrics: &Arc<crate::metrics::Metrics>,
    labels: &SourceLabels,
) -> Vec<IggyMessage> {
    let mut iggy_messages = Vec::with_capacity(messages.len());
    // Accumulate per-message drops, flush once after the loop - one Family
    // lookup instead of one per message under filter/error storms.
    let mut error_count = 0u64;
    let mut filtered_count = 0u64;
    for message in messages {
        let mut current_message = Some(message);
        let mut transform_failed = false;
        for transform in transforms.iter() {
            let Some(message) = current_message.take() else {
                break;
            };

            match transform.transform(topic_metadata, message) {
                Ok(next) => current_message = next,
                Err(error) => {
                    error!(
                        "Transform '{:?}' failed for source connector with ID: {id}, stream: {}, topic: {}: {error}",
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

        let Ok(payload) = encoder.encode(message.payload) else {
            error!(
                "Failed to encode message payload for source connector with ID: {id}, stream: {}, topic: {}",
                topic_metadata.stream, topic_metadata.topic
            );
            error_count += 1;
            continue;
        };

        let Ok(iggy_message) = build_iggy_message(payload, message.id, message.headers) else {
            error!(
                "Failed to build Iggy message for source connector with ID: {id}, stream: {}, topic: {}",
                topic_metadata.stream, topic_metadata.topic
            );
            error_count += 1;
            continue;
        };

        iggy_messages.push(iggy_message);
    }
    metrics.inc_errors_by_with_labels(&labels.counter, error_count);
    if filtered_count > 0 {
        metrics.inc_messages_filtered_with_labels(&labels.counter, filtered_count);
    }
    iggy_messages
}

pub(crate) extern "C" fn handle_produced_messages(
    plugin_id: u32,
    messages_ptr: *const u8,
    messages_len: usize,
) {
    unsafe {
        // Entry missing = SOURCE_SENDERS cleaned up at shutdown; benign race
        // expected on stop/restart. No metric (would conflate with real failures).
        let Some(entry) = SOURCE_SENDERS.get(&plugin_id) else {
            tracing::trace!(
                plugin_id,
                "dropping produced batch: sender already cleaned up"
            );
            return;
        };
        let messages = std::slice::from_raw_parts(messages_ptr, messages_len);
        match postcard::from_bytes::<ProducedMessages>(messages) {
            Ok(messages) => {
                if let Err(send_error) = entry.sender.send(messages) {
                    error!(
                        "Failed to send messages for source connector with ID: {plugin_id}. Channel closed: {send_error}"
                    );
                    entry.error_counter.inc();
                }
            }
            Err(err) => {
                error!(
                    "Failed to deserialize produced messages for source connector with ID: {plugin_id}. {err}"
                );
                entry.error_counter.inc();
            }
        }
    }
}

fn build_iggy_message(
    payload: Vec<u8>,
    id: Option<u128>,
    headers: Option<BTreeMap<HeaderKey, HeaderValue>>,
) -> Result<IggyMessage, IggyError> {
    match (id, headers) {
        (Some(id), Some(h)) => IggyMessage::builder()
            .payload(payload.into())
            .id(id)
            .user_headers(h)
            .build(),
        (Some(id), None) => IggyMessage::builder()
            .payload(payload.into())
            .id(id)
            .build(),
        (None, Some(h)) => IggyMessage::builder()
            .payload(payload.into())
            .user_headers(h)
            .build(),
        (None, None) => IggyMessage::builder().payload(payload.into()).build(),
    }
}
