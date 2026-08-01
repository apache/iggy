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

use crossfire::{AsyncRx, MTx, SendTimeoutError, TrySendError, mpsc::Array};
use dashmap::DashMap;
use dlopen2::wrapper::Container;
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
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
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

pub(crate) const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

// crossfire eagerly allocates the whole ring and asserts capacity < 2^31;
// the cap keeps a config typo from panicking the process or committing
// gigabytes up front.
const MAX_CHANNEL_CAPACITY: usize = 65_536;

const SEND_RETRY_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) type BatchSender = MTx<Array<ProducedMessages>>;
pub(crate) type BatchReceiver = AsyncRx<Array<ProducedMessages>>;

pub(crate) struct SourceSenderEntry {
    pub(crate) sender: BatchSender,
    // Owned errors counter (Arc<AtomicU64> inside) so the FFI callback bumps
    // it with one relaxed atomic - no Family RwLock + HashMap lookup per call.
    pub(crate) error_counter: Counter,
    pub(crate) shutdown: Arc<AtomicBool>,
    // Latched across callback invocations so sustained overload logs one
    // warn per backpressure episode, not one per batch; cleared only by an
    // uncontended fast-path send (i.e. genuine recovery).
    pub(crate) backpressure_active: Arc<AtomicBool>,
}

pub(crate) static SOURCE_SENDERS: LazyLock<DashMap<u32, SourceSenderEntry>> =
    LazyLock::new(DashMap::new);

pub(crate) fn cleanup_sender(plugin_id: u32) {
    SOURCE_SENDERS.remove(&plugin_id);
}

/// Unwedges a send callback stuck in its full-channel backoff so
/// `iggy_source_close` (which blocks until the plugin's polling task exits)
/// cannot deadlock while the forwarding loop is itself blocked on a slow Iggy.
pub(crate) fn signal_shutdown(plugin_id: u32) {
    if let Some(entry) = SOURCE_SENDERS.get(&plugin_id) {
        entry.shutdown.store(true, Ordering::Release);
    }
}

/// Sets every live instance's shutdown flag. Process shutdown stops
/// connectors sequentially, and instances loaded from the same plugin
/// library share one tokio runtime - without this, an instance wedged in
/// backpressure later in the list keeps holding a runtime worker that an
/// earlier instance's close needs to observe its own shutdown.
pub(crate) fn signal_shutdown_all() {
    for entry in SOURCE_SENDERS.iter() {
        entry.shutdown.store(true, Ordering::Release);
    }
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
            channel_capacity: config.channel_capacity,
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
    receiver: BatchReceiver,
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

    while let Ok(produced_messages) = receiver.recv().await {
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
    channel_capacity: Option<usize>,
    producer: IggyProducer,
    encoder: Arc<dyn StreamEncoder>,
    transforms: Vec<Arc<dyn Transform>>,
    state_storage: StateStorage,
    callback: HandleCallback,
    context: Arc<RuntimeContext>,
) -> Vec<JoinHandle<()>> {
    let configured = channel_capacity.unwrap_or(DEFAULT_CHANNEL_CAPACITY);
    let capacity = configured.clamp(1, MAX_CHANNEL_CAPACITY);
    if capacity != configured {
        warn!(
            "Source connector with ID: {plugin_id} channel_capacity: {configured} clamped to {capacity}"
        );
    }
    let (sender, receiver) = crossfire::mpsc::bounded_blocking_async(capacity);
    info!("Source connector with ID: {plugin_id} forwarding channel capacity: {capacity} batches");
    let plugin_key = plugin_key.to_string();
    let labels = Arc::new(SourceLabels::new(&plugin_key));
    SOURCE_SENDERS.insert(
        plugin_id,
        SourceSenderEntry {
            sender,
            error_counter: context.metrics.error_counter(&labels.counter),
            shutdown: Arc::new(AtomicBool::new(false)),
            backpressure_active: Arc::new(AtomicBool::new(false)),
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
                plugin.channel_capacity,
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
        // Clone out and drop the guard: the backoff loop below may run for a
        // while, and holding the shard guard would block cleanup_sender.
        let Some((sender, error_counter, shutdown, backpressure_active)) =
            SOURCE_SENDERS.get(&plugin_id).map(|entry| {
                (
                    entry.sender.clone(),
                    entry.error_counter.clone(),
                    entry.shutdown.clone(),
                    entry.backpressure_active.clone(),
                )
            })
        else {
            tracing::trace!(
                plugin_id,
                "dropping produced batch: sender already cleaned up"
            );
            return;
        };
        let messages = std::slice::from_raw_parts(messages_ptr, messages_len);
        match postcard::from_bytes::<ProducedMessages>(messages) {
            Ok(messages) => {
                send_with_backpressure(
                    plugin_id,
                    &sender,
                    &shutdown,
                    &backpressure_active,
                    &error_counter,
                    messages,
                );
            }
            Err(err) => {
                error!(
                    "Failed to deserialize produced messages for source connector with ID: {plugin_id}. {err}"
                );
                error_counter.inc();
            }
        }
    }
}

// Parks a worker thread of the plugin library's shared tokio runtime - a
// deliberate exception to the never-block-the-executor rule: the park IS
// the backpressure, propagating a full channel into the plugin's polling
// loop instead of buffering without bound. Every park is bounded by
// SEND_RETRY_INTERVAL with the shutdown flag re-read in between, so
// iggy_source_close (which waits on the polling task this runs in) cannot
// deadlock on a hung Iggy. Instances loaded from the same .so share that
// runtime, so a saturated sibling can still delay another instance's
// close; signal_shutdown_all covers the process-shutdown path, and the
// complete fix (handing the worker off via block_in_place) belongs in the
// SDK.
fn send_with_backpressure(
    plugin_id: u32,
    sender: &BatchSender,
    shutdown: &AtomicBool,
    backpressure_active: &AtomicBool,
    error_counter: &Counter,
    messages: ProducedMessages,
) {
    let mut messages = match sender.try_send(messages) {
        Ok(()) => {
            // An uncontended send is the recovery signal; a send that only
            // succeeded after stalling below is not.
            if backpressure_active.swap(false, Ordering::Relaxed) {
                info!(
                    "Forwarding channel for source connector with ID: {plugin_id} recovered from backpressure"
                );
            }
            return;
        }
        Err(TrySendError::Full(returned)) => returned,
        Err(TrySendError::Disconnected(returned)) => {
            log_channel_closed(plugin_id, returned.messages.len(), error_counter);
            return;
        }
    };
    if shutdown.load(Ordering::Acquire) {
        drop_during_shutdown(plugin_id, messages.messages.len(), error_counter);
        return;
    }
    if !backpressure_active.swap(true, Ordering::Relaxed) {
        warn!(
            "Forwarding channel for source connector with ID: {plugin_id} is full. Backpressuring the plugin's polling task."
        );
    }
    loop {
        match sender.send_timeout(messages, SEND_RETRY_INTERVAL) {
            Ok(()) => return,
            Err(SendTimeoutError::Timeout(returned)) => {
                if shutdown.load(Ordering::Acquire) {
                    drop_during_shutdown(plugin_id, returned.messages.len(), error_counter);
                    return;
                }
                messages = returned;
            }
            Err(SendTimeoutError::Disconnected(returned)) => {
                log_channel_closed(plugin_id, returned.messages.len(), error_counter);
                return;
            }
        }
    }
}

fn drop_during_shutdown(plugin_id: u32, message_count: usize, error_counter: &Counter) {
    error!(
        "Dropping {message_count} produced messages for source connector with ID: {plugin_id}. Channel still full during shutdown."
    );
    error_counter.inc();
}

fn log_channel_closed(plugin_id: u32, message_count: usize, error_counter: &Counter) {
    error!(
        "Failed to send {message_count} produced messages for source connector with ID: {plugin_id}. Channel closed."
    );
    error_counter.inc();
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

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::ProducedMessage;

    fn batch() -> ProducedMessages {
        ProducedMessages {
            schema: Schema::Raw,
            messages: vec![ProducedMessage {
                id: None,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                headers: None,
                payload: vec![0u8],
            }],
            state: None,
        }
    }

    fn bounded_channel(capacity: usize) -> (BatchSender, BatchReceiver) {
        crossfire::mpsc::bounded_blocking_async(capacity)
    }

    // Shutdown relies on buffered batches surviving sender drop; crossfire's
    // docs don't promise it, so this pins the behavior against upgrades.
    #[tokio::test]
    async fn given_buffered_batches_when_senders_drop_should_drain_before_disconnect() {
        let (sender, receiver) = bounded_channel(4);
        for _ in 0..3 {
            sender.try_send(batch()).expect("channel has capacity");
        }
        drop(sender);
        for _ in 0..3 {
            assert!(
                receiver.recv().await.is_ok(),
                "buffered batch must drain after sender drop"
            );
        }
        assert!(
            receiver.recv().await.is_err(),
            "drained channel with no senders must disconnect"
        );
    }

    #[test]
    fn given_full_channel_when_shutdown_signaled_should_drop_batch_and_count_error() {
        let (sender, receiver) = bounded_channel(1);
        sender.try_send(batch()).expect("fills the channel");
        let shutdown = AtomicBool::new(true);
        let backpressure_active = AtomicBool::new(false);
        let error_counter = Counter::default();
        send_with_backpressure(
            0,
            &sender,
            &shutdown,
            &backpressure_active,
            &error_counter,
            batch(),
        );
        assert_eq!(
            error_counter.get(),
            1,
            "batch dropped during shutdown must count as an error"
        );
        assert!(
            receiver.try_recv().is_ok(),
            "pre-existing batch must still be queued"
        );
        assert!(
            receiver.try_recv().is_err(),
            "shutdown-dropped batch must not have been enqueued"
        );
    }

    #[test]
    fn given_backoff_in_progress_when_shutdown_signaled_should_unblock_and_drop() {
        let (sender, _receiver) = bounded_channel(1);
        sender.try_send(batch()).expect("fills the channel");
        let shutdown = Arc::new(AtomicBool::new(false));
        let error_counter = Counter::default();
        let sender_in_loop = sender.clone();
        let shutdown_in_loop = shutdown.clone();
        let counter_in_loop = error_counter.clone();
        let blocked = std::thread::spawn(move || {
            let backpressure_active = AtomicBool::new(false);
            send_with_backpressure(
                0,
                &sender_in_loop,
                &shutdown_in_loop,
                &backpressure_active,
                &counter_in_loop,
                batch(),
            );
        });
        // Let the loop enter backoff before flipping the flag, so a refactor
        // that reads the flag once up front cannot pass this test.
        std::thread::sleep(Duration::from_millis(30));
        shutdown.store(true, Ordering::Release);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !blocked.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "send_with_backpressure must unblock after the shutdown signal"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        blocked.join().expect("blocked thread panicked");
        assert_eq!(
            error_counter.get(),
            1,
            "batch dropped after mid-backoff shutdown must count as an error"
        );
    }

    #[test]
    fn given_registered_entry_when_signal_shutdown_called_should_set_flag() {
        // SOURCE_SENDERS is process-global; an id far above anything the
        // runtime allocates keeps this isolated from sibling tests.
        let plugin_id = u32::MAX;
        let (sender, _receiver) = bounded_channel(1);
        SOURCE_SENDERS.insert(
            plugin_id,
            SourceSenderEntry {
                sender,
                error_counter: Counter::default(),
                shutdown: Arc::new(AtomicBool::new(false)),
                backpressure_active: Arc::new(AtomicBool::new(false)),
            },
        );
        signal_shutdown(plugin_id);
        let flag = SOURCE_SENDERS
            .get(&plugin_id)
            .expect("entry inserted above")
            .shutdown
            .load(Ordering::Acquire);
        cleanup_sender(plugin_id);
        assert!(flag, "signal_shutdown must set the registered entry's flag");
    }

    #[test]
    fn given_multiple_registered_entries_when_signal_shutdown_all_called_should_set_every_flag() {
        let plugin_ids = [u32::MAX - 1, u32::MAX - 2];
        for plugin_id in plugin_ids {
            let (sender, _receiver) = bounded_channel(1);
            SOURCE_SENDERS.insert(
                plugin_id,
                SourceSenderEntry {
                    sender,
                    error_counter: Counter::default(),
                    shutdown: Arc::new(AtomicBool::new(false)),
                    backpressure_active: Arc::new(AtomicBool::new(false)),
                },
            );
        }
        signal_shutdown_all();
        let flags: Vec<bool> = plugin_ids
            .iter()
            .map(|plugin_id| {
                SOURCE_SENDERS
                    .get(plugin_id)
                    .expect("entry inserted above")
                    .shutdown
                    .load(Ordering::Acquire)
            })
            .collect();
        for plugin_id in plugin_ids {
            cleanup_sender(plugin_id);
        }
        assert!(
            flags.iter().all(|flag_set| *flag_set),
            "signal_shutdown_all must set every registered entry's flag"
        );
    }

    #[test]
    fn given_registered_entry_when_callback_invoked_should_deliver_batch_to_channel() {
        let plugin_id = u32::MAX - 3;
        let (sender, receiver) = bounded_channel(2);
        SOURCE_SENDERS.insert(
            plugin_id,
            SourceSenderEntry {
                sender,
                error_counter: Counter::default(),
                shutdown: Arc::new(AtomicBool::new(false)),
                backpressure_active: Arc::new(AtomicBool::new(false)),
            },
        );
        let bytes = postcard::to_allocvec(&batch()).expect("batch serializes");
        handle_produced_messages(plugin_id, bytes.as_ptr(), bytes.len());
        cleanup_sender(plugin_id);
        let delivered = receiver
            .try_recv()
            .expect("callback must enqueue the deserialized batch");
        assert_eq!(delivered.messages.len(), 1);
    }

    #[test]
    fn given_invalid_payload_when_callback_invoked_should_count_error() {
        let plugin_id = u32::MAX - 4;
        let (sender, _receiver) = bounded_channel(1);
        let error_counter = Counter::default();
        SOURCE_SENDERS.insert(
            plugin_id,
            SourceSenderEntry {
                sender,
                error_counter: error_counter.clone(),
                shutdown: Arc::new(AtomicBool::new(false)),
                backpressure_active: Arc::new(AtomicBool::new(false)),
            },
        );
        let garbage = [0xFFu8; 3];
        handle_produced_messages(plugin_id, garbage.as_ptr(), garbage.len());
        cleanup_sender(plugin_id);
        assert_eq!(
            error_counter.get(),
            1,
            "deserialize failure must count as an error"
        );
    }

    #[test]
    fn given_unregistered_plugin_when_callback_invoked_should_drop_silently() {
        let bytes = postcard::to_allocvec(&batch()).expect("batch serializes");
        handle_produced_messages(u32::MAX - 5, bytes.as_ptr(), bytes.len());
    }

    #[test]
    fn given_backpressure_latched_when_uncontended_send_succeeds_should_clear_latch() {
        let (sender, receiver) = bounded_channel(2);
        let shutdown = AtomicBool::new(false);
        let backpressure_active = AtomicBool::new(true);
        let error_counter = Counter::default();
        send_with_backpressure(
            0,
            &sender,
            &shutdown,
            &backpressure_active,
            &error_counter,
            batch(),
        );
        assert!(
            !backpressure_active.load(Ordering::Relaxed),
            "uncontended send must clear the backpressure latch"
        );
        assert!(receiver.try_recv().is_ok(), "batch must be delivered");
        assert_eq!(error_counter.get(), 0);
    }

    #[test]
    fn given_disconnected_channel_when_sending_should_count_error() {
        let (sender, receiver) = bounded_channel(1);
        drop(receiver);
        let shutdown = AtomicBool::new(false);
        let backpressure_active = AtomicBool::new(false);
        let error_counter = Counter::default();
        send_with_backpressure(
            0,
            &sender,
            &shutdown,
            &backpressure_active,
            &error_counter,
            batch(),
        );
        assert_eq!(
            error_counter.get(),
            1,
            "batch lost to a closed channel must count as an error"
        );
    }

    #[test]
    fn given_full_channel_when_receiver_frees_capacity_should_deliver_batch() {
        let (sender, receiver) = bounded_channel(1);
        sender.try_send(batch()).expect("fills the channel");
        let delivered = Arc::new(AtomicBool::new(false));
        let delivered_signal = delivered.clone();
        // Keeps the receiver alive until the send lands, so the backoff loop
        // can only exit through the success path.
        let drainer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            let drained = receiver.try_recv();
            while !delivered_signal.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(5));
            }
            drained
        });
        let shutdown = AtomicBool::new(false);
        let backpressure_active = AtomicBool::new(false);
        let error_counter = Counter::default();
        send_with_backpressure(
            0,
            &sender,
            &shutdown,
            &backpressure_active,
            &error_counter,
            batch(),
        );
        delivered.store(true, Ordering::Release);
        assert!(
            drainer.join().expect("drainer thread panicked").is_ok(),
            "drainer must have freed a slot from the full channel"
        );
        assert_eq!(
            error_counter.get(),
            0,
            "backpressured send must succeed once capacity frees up"
        );
    }
}
