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
use crate::configs::connectors::{SinkConfig, StreamConsumerConfig};
use crate::context::RuntimeContext;
use crate::log::LOG_CALLBACK;
use crate::metrics::{ConnectorType, Metrics, SinkLabels};
use crate::stream::IggyClients;
use crate::{
    FailedPlugin, PLUGIN_ID, RuntimeError, SinkApi, SinkConnector, SinkConnectorConsumer,
    SinkConnectorPlugin, SinkConnectorWrapper, close_plugin_instance, resolve_plugin_path,
    transform,
};
use dlopen2::wrapper::Container;
use futures::{Stream, StreamExt};
use iggy::prelude::{
    Client, CommitPolicy, IggyClient, IggyConsumer, IggyDuration, IggyError, IggyMessage,
    NonZeroIggyDuration, PollOptions, PollingStrategy, ReadError, UserClient,
};
use iggy_connector_sdk::decoders::avro::{AvroConfig, AvroStreamDecoder};
use iggy_connector_sdk::{
    DecodedMessage, MessagesMetadata, RawMessage, RawMessages, ReceivedMessage, Schema,
    StreamDecoder, TopicMetadata, sink::ConsumeCallback, transforms::Transform,
};
use std::{
    collections::HashMap,
    pin::Pin,
    str::FromStr,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

const DEFAULT_BATCH_LENGTH: u32 = 1000;

/// Initializes all enabled sink connectors.
///
/// Per-connector failures (path resolution, dlopen, plugin init, client
/// connect, consumer/decoder/transform setup) are captured against the offending
/// connector and do not abort the runtime. Connectors that fail before their
/// FFI container can be loaded are returned in the second tuple element so
/// they remain visible in health/status output.
///
/// Only system-level errors that prevent any connector from running are
/// propagated as `Err`.
pub async fn init(
    sink_configs: HashMap<String, SinkConfig>,
    iggy_clients: &IggyClients,
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
            client: None,
            error: init_error.clone(),
            verbose: config.verbose,
            benchmark: config.benchmark,
        });

        if let Some(error) = init_error {
            error!("Failed to initialize sink container with name: {name} ({key}). {error}");
            continue;
        }

        let setup = async {
            let client = iggy_clients.connect_sink_client().await?;
            match setup_sink_consumers(&key, &config, &client).await {
                Ok(consumers) => Ok((client, consumers)),
                Err(error) => {
                    release_sink_client(client, plugin_id).await;
                    Err(error)
                }
            }
        };
        match setup.await {
            Ok((client, consumers)) => {
                let connector = sink_connectors
                    .get_mut(&path)
                    .expect("sink connector was inserted above");
                let plugin = connector
                    .plugins
                    .iter_mut()
                    .find(|plugin| plugin.id == plugin_id)
                    .expect("sink plugin was pushed above");
                plugin.client = Some(client);
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
                let close = connector.container.iggy_sink_close;
                close_plugin_instance(&|id| close(id), ConnectorType::Sink, plugin_id, &key);
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
            // Only a sink whose setup failed has no client, and its error says why.
            let Some(client) = plugin.client else {
                error!(
                    "Failed to initialize sink connector with ID: {}: {}. Skipping...",
                    plugin.id,
                    plugin.error.as_deref().unwrap_or_default(),
                );
                continue;
            };
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
                client,
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

/// Every task holds a reference to the `client` of this start, and the task
/// that ends last logs it out and shuts it down. A consumer shutdown can still
/// store offsets after a stop stops waiting for its task, so the client must
/// outlive it.
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
    client: Arc<IggyClient>,
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
    if consumers.is_empty() {
        // Without a task, nothing else shuts the client down.
        tokio::spawn(release_sink_client(client, plugin_id));
        return (shutdown_tx, Vec::new());
    }
    // Each task gets its own reference and none stays here, so the task that
    // ends last finds the client unshared.
    let clients = vec![client; consumers.len()];
    let mut task_handles = Vec::new();
    let labels = Arc::new(SinkLabels::new(plugin_key));
    for ((mut consumer, decoder, batch_size, transforms), client) in
        consumers.into_iter().zip(clients)
    {
        let plugin_key = plugin_key.to_string();
        let metrics = metrics.clone();
        let shutdown_rx = shutdown_rx.clone();
        let context = context.clone();
        let labels = labels.clone();
        let topic_metadata = TopicMetadata {
            stream: consumer.stream().to_string(),
            topic: consumer.topic().to_string(),
        };
        let handle = tokio::spawn(async move {
            if let Err(error) = consume_messages(
                plugin_id,
                decoder,
                batch_size,
                callback,
                transforms,
                topic_metadata,
                &mut ConsumerMessages {
                    consumer: &mut consumer,
                    client: &client,
                },
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
                    .set_error(&plugin_key, &error.to_string(), Some(&metrics))
                    .await;
            }
            // The polls also committed the batches the consumer still holds,
            // and only a shutdown moves the offsets back over them.
            if let Err(error) = consumer.shutdown().await {
                error!(
                    "Failed to shut down the consumer of sink connector with ID: {plugin_id}: {error}"
                );
            }
            release_sink_client(client, plugin_id).await;
        });
        task_handles.push(handle);
    }
    (shutdown_tx, task_handles)
}

/// Logs the client of a sink start out and shuts it down when the caller holds
/// its last reference. Each holder calls it once it is done with the client. A
/// closed connection alone keeps its session, and with it the group members,
/// until the session times out.
pub(crate) async fn release_sink_client(client: Arc<IggyClient>, plugin_id: u32) {
    let Some(client) = Arc::into_inner(client) else {
        return;
    };
    if let Err(error) = client.logout_user().await {
        warn!("Failed to log out the Iggy client of sink connector with ID: {plugin_id}: {error}");
    }
    if let Err(error) = client.shutdown().await {
        error!(
            "Failed to shut down the Iggy client of sink connector with ID: {plugin_id}: {error}"
        );
    }
}

/// The reads of a sink task, and the calls that resume a partition that its reader stopped.
pub(crate) trait SinkMessages:
    Stream<Item = Result<iggy::prelude::ReceivedMessage, ReadError>> + Unpin
{
    async fn abandon_operation(&mut self, operation: u64) -> Result<(), IggyError>;

    async fn seek(&mut self, partition_id: u32, start: PollingStrategy) -> Result<(), IggyError>;
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn consume_messages(
    plugin_id: u32,
    decoder: Arc<dyn StreamDecoder>,
    batch_size: u32,
    consume: ConsumeCallback,
    transforms: Vec<Arc<dyn Transform>>,
    topic_metadata: TopicMetadata,
    messages: &mut impl SinkMessages,
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
    // The partition of the batch and its current offset at the last poll. The
    // plugin learns one partition per batch, so another partition flushes it.
    let mut buffered = None;
    let mut next = None;

    loop {
        let message = match next.take() {
            Some(message) => Some(message),
            None => {
                let message = tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("Sink connector with ID: {plugin_id} received shutdown signal");
                        None
                    }
                    msg = messages.next() => msg,
                };
                match message {
                    Some(Ok(message)) => Some(message),
                    Some(Err(error)) => {
                        metrics.inc_errors_with_labels(&labels.counter);
                        handle_read_error(messages, error, plugin_id, &topic_metadata).await;
                        continue;
                    }
                    None => None,
                }
            }
        };

        // The poll committed the batch held here, so a stop still hands it to
        // the plugin instead of dropping it.
        let stopping = message.is_none();
        if let Some(message) = message {
            if buffered.is_some_and(|(partition_id, _)| partition_id != message.partition_id) {
                next = Some(message);
            } else {
                let message_offset = message.message.header.offset;
                buffered = Some((message.partition_id, message.current_offset));
                batch.push(message.message);
                if message.current_offset != message_offset && batch.len() < batch_size {
                    continue;
                }
            }
        }
        let Some((partition_id, current_offset)) = buffered.take() else {
            if stopping {
                break;
            }
            continue;
        };

        let messages = std::mem::take(&mut batch);
        let messages_count = messages.len();
        metrics.inc_messages_consumed_with_labels(&labels.counter, messages_count as u64);
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
            partition_id,
            current_offset,
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

        let (processed_count, runs, decode_us, prepare_us, ffi_us) = match &result {
            Ok(timing) => {
                let prepare_elapsed = elapsed
                    .saturating_sub(timing.ffi_elapsed)
                    .saturating_sub(timing.decode_elapsed);
                metrics.observe_stage_with_labels(&labels.stage_decode, timing.decode_elapsed);
                metrics.observe_stage_with_labels(&labels.stage_prepare, prepare_elapsed);
                metrics.observe_stage_with_labels(&labels.stage_ffi, timing.ffi_elapsed);
                (
                    timing.processed_count,
                    timing.runs,
                    benchmark::as_micros(timing.decode_elapsed),
                    benchmark::as_micros(prepare_elapsed),
                    benchmark::as_micros(timing.ffi_elapsed),
                )
            }
            Err(_) => (0, 0, 0, 0, 0),
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
                runs,
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
        if stopping {
            break;
        }
    }
    info!("Stopped consuming messages for sink connector with ID: {plugin_id}");
    Ok(())
}

/// The reader stops a partition after a read with an unknown outcome, a message that does not
/// decode, or a new partition history. Each would wait for a restart, so the sink resumes the
/// partition. After an unknown outcome it resumes after the stored offset. The abandoned request
/// can still store its offset later, so the sink can skip the messages of that request. The
/// reader retries any other error by itself.
async fn handle_read_error(
    messages: &mut impl SinkMessages,
    error: ReadError,
    plugin_id: u32,
    topic_metadata: &TopicMetadata,
) {
    let TopicMetadata { stream, topic } = topic_metadata;
    let (partition_id, start) = match (error.partition_id, &error.error, error.skip) {
        (
            Some(partition_id),
            IggyError::FetchProgressUncertain(operation)
            | IggyError::PartitionLaneBlocked(_, operation),
            _,
        ) => {
            warn!(
                "Abandoning operation: {operation} with an unknown outcome, which blocks partition: {partition_id} of sink connector with ID: {plugin_id} from stream: {stream}, topic: {topic}. The partition resumes after its stored offset. {}",
                error.error
            );
            match messages.abandon_operation(*operation).await {
                // The reader reports a lost fetch for each of its partitions, and the first
                // abandon frees the operation for all of them.
                Ok(()) | Err(IggyError::OperationNotRetained(_)) => {}
                Err(abandon_error) => warn!(
                    "Failed to abandon operation: {operation} of sink connector with ID: {plugin_id}: {abandon_error}"
                ),
            }
            (partition_id, PollingStrategy::next())
        }
        (Some(partition_id), _, Some(skip)) => {
            warn!(
                "Skipping a message that does not decode in partition: {partition_id} of sink connector with ID: {plugin_id} from stream: {stream}, topic: {topic}. {}",
                error.error
            );
            (partition_id, skip)
        }
        (Some(partition_id), IggyError::HistoryUnavailable, None) => {
            warn!(
                "Partition: {partition_id} of sink connector with ID: {plugin_id} from stream: {stream}, topic: {topic} has a new history. It resumes after its stored offset. {}",
                error.error
            );
            (partition_id, PollingStrategy::next())
        }
        _ => {
            error!(
                "Failed to receive message for sink connector with ID: {plugin_id} from stream: {stream}, topic: {topic}"
            );
            return;
        }
    };
    if let Err(seek_error) = messages.seek(partition_id, start).await {
        error!(
            "Failed to resume partition: {partition_id} of sink connector with ID: {plugin_id} from stream: {stream}, topic: {topic}: {seek_error}"
        );
    }
}

/// A sink consumer with the client that holds the operations of its reads.
struct ConsumerMessages<'a> {
    consumer: &'a mut IggyConsumer,
    client: &'a IggyClient,
}

impl Stream for ConsumerMessages<'_> {
    type Item = Result<iggy::prelude::ReceivedMessage, ReadError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.consumer.poll_next_unpin(cx)
    }
}

impl SinkMessages for ConsumerMessages<'_> {
    async fn abandon_operation(&mut self, operation: u64) -> Result<(), IggyError> {
        self.client.abandon_operation(operation).await
    }

    async fn seek(&mut self, partition_id: u32, start: PollingStrategy) -> Result<(), IggyError> {
        self.consumer.seek(partition_id, start).await
    }
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
        let poll_options = poll_options(stream)?;
        let default_consumer_group = format!("iggy-connect-sink-{key}");
        let consumer_group = stream
            .consumer_group
            .as_deref()
            .unwrap_or(&default_consumer_group);
        for topic in stream.topics.iter() {
            let mut consumer = iggy_client
                .consumer_group(consumer_group, &stream.stream, topic)?
                .create_consumer_group_if_not_exists()
                .polling_strategy(PollingStrategy::next())
                .poll_options(poll_options)
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
            consumers.push((consumer, decoder, poll_options.count, transforms.clone()));
        }
    }
    Ok(consumers)
}

/// The server stores the offset of each reply as part of the read. While the
/// primary stays up the plugin gets each message at most once, and a failover
/// before the store replicates can deliver the last batch again. A stream
/// without `max_wait` waits one second.
fn poll_options(stream: &StreamConsumerConfig) -> Result<PollOptions, RuntimeError> {
    if stream.poll_interval.is_some() {
        return Err(RuntimeError::UnsupportedPollInterval(stream.stream.clone()));
    }
    let mut options = PollOptions {
        count: stream.batch_length.unwrap_or(DEFAULT_BATCH_LENGTH),
        commit: CommitPolicy::OnRead,
        ..PollOptions::wait(IggyDuration::ONE_SECOND)
    };
    if let Some(max_wait) = stream.max_wait.as_deref() {
        // A zero wait answers at once, so an idle topic would be polled constantly.
        let max_wait = NonZeroIggyDuration::from_str(max_wait).map_err(|error| {
            RuntimeError::InvalidConfiguration(format!("Invalid max wait: {error}"))
        })?;
        options.max_wait = max_wait.into();
    }
    if let Some(min_count) = stream.min_count {
        options.min_count = min_count;
    }
    Ok(options)
}

#[allow(clippy::too_many_arguments)]
async fn process_messages(
    plugin_id: u32,
    partition_id: u32,
    current_offset: u64,
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

    // One `Schema` tag covers a whole FFI call, so messages are grouped into
    // contiguous runs of the same payload variant and each run is sent on its
    // own. No decoder mixes variants within one configured instance, but a
    // transform can: `ProtoConvert` returns `Payload::Raw` when it can encode a
    // message against its descriptor and `Payload::Proto` when it cannot, which
    // turns on the message rather than the config. A uniform batch stays one run
    // and one call.
    let mut runs: Vec<(Schema, Vec<RawMessage>)> = Vec::with_capacity(1);
    let mut remaining = decoded.len();
    for message in decoded {
        remaining -= 1;
        let mut current_message = Some(message);
        let mut transform_failed = false;
        for transform in transforms.iter() {
            let Some(message) = current_message.take() else {
                break;
            };
            // Sink batches can deliver valid siblings after a transform failure.
            // Source batches instead reject the checkpoint for the entire batch.
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

        // Read the tag off the payload before `try_into_vec` consumes it. The
        // decoder's own schema names the format it reads, not the variant it
        // returned, and a transform may have changed the variant since.
        let schema = message.payload.schema();
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
        match runs.last_mut() {
            Some((run_schema, run)) if *run_schema == schema => run.push(raw_message),
            _ => {
                // Only the first run is sized to the batch. Reserving what is
                // left for every run would cost O(n^2) slots on a batch that
                // alternates variants, and any run after the first is rare
                // enough to be worth growing on demand.
                let mut run = if runs.is_empty() {
                    Vec::with_capacity(remaining + 1)
                } else {
                    Vec::new()
                };
                run.push(raw_message);
                runs.push((schema, run));
            }
        }
    }

    metrics.inc_errors_by_with_labels(&labels.counter, error_count);
    if filtered_count > 0 {
        metrics.inc_messages_filtered_with_labels(&labels.counter, filtered_count);
    }

    // A batch that lost every message still reaches the sink, as it always has.
    // There is no payload to read a tag from, so it carries the stream's
    // configured schema. That is the decoder's wire format rather than a payload
    // variant, which is safe only because the run is empty: a tag is read back
    // once per message, so an empty run never has it interpreted. Anything that
    // starts branching on the tag for whole-batch behaviour has to revisit this.
    if runs.is_empty() {
        runs.push((decoder.schema(), Vec::new()));
    }

    // An empty batch is still one FFI call, so it is still one run.
    let run_count = runs.len();
    let mut processed_count = 0usize;

    let topic_meta = postcard::to_allocvec(topic_metadata).map_err(|error| {
        error!(
            "Failed to serialize topic metadata for sink connector with ID: {plugin_id}. {error}"
        );
        RuntimeError::FailedToSerializeTopicMetadata
    })?;

    let mut ffi_elapsed = Duration::ZERO;
    for (schema, run) in runs {
        let messages_metadata = MessagesMetadata {
            partition_id,
            current_offset,
            schema,
        };
        let messages_meta = postcard::to_allocvec(&messages_metadata).map_err(|error| {
            error!(
                "Failed to serialize messages metadata for sink connector with ID: {plugin_id}. {error}"
            );
            RuntimeError::FailedToSerializeMessagesMetadata
        })?;

        let run_len = run.len();
        let messages = postcard::to_allocvec(&RawMessages {
            schema,
            messages: run,
        })
        .map_err(|error| {
            error!("Failed to serialize messages for sink connector with ID: {plugin_id}. {error}");
            RuntimeError::FailedToSerializeRawMessages
        })?;

        let ffi_start = Instant::now();
        let result = (consume)(
            plugin_id,
            topic_meta.as_ptr(),
            topic_meta.len(),
            messages_meta.as_ptr(),
            messages_meta.len(),
            messages.as_ptr(),
            messages.len(),
        );
        ffi_elapsed += ffi_start.elapsed();
        if result == 0 {
            processed_count += run_len;
        } else {
            error!(
                "Failed to consume {run_len} messages for sink connector with ID: {plugin_id}, stream: {}, topic: {}, schema: {schema}, status: {result}",
                topic_metadata.stream, topic_metadata.topic
            );
            metrics.inc_errors_with_labels(&labels.counter);
        }
    }

    // Counted once every call has been made, so a serialisation failure above
    // cannot leave calls on the counter that never happened.
    metrics.inc_sink_runs_with_labels(&labels.counter, run_count as u64);

    Ok(SinkBatchTiming {
        processed_count,
        runs: run_count,
        decode_elapsed,
        ffi_elapsed,
    })
}

struct SinkBatchTiming {
    processed_count: usize,
    /// FFI calls the batch was split into, one per contiguous payload variant.
    runs: usize,
    decode_elapsed: Duration,
    /// Summed over every run, so one batch is one `stage_ffi` sample however
    /// many calls it took.
    ffi_elapsed: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use iggy::prelude::IggyMessageHeader;
    use iggy_connector_sdk::transforms::TransformType;
    use iggy_connector_sdk::transforms::{ProtoConvert, ProtoConvertConfig};
    use iggy_connector_sdk::{Error, Payload};
    use prost::Message as _;
    use std::sync::LazyLock;
    use std::sync::atomic::{AtomicU32, AtomicUsize};
    use tracing::{Level, Subscriber};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};
    use tracing_subscriber::registry::Registry;

    /// One entry per FFI call the stub sink received, keyed by plugin id so
    /// tests sharing the binary do not read each other's batches.
    static CONSUMED: LazyLock<DashMap<u32, Vec<ConsumedBatch>>> = LazyLock::new(DashMap::new);

    static TEST_PLUGIN_ID: AtomicU32 = AtomicU32::new(u32::MAX / 2);

    struct ConsumedBatch {
        partition_id: u32,
        metadata_schema: Schema,
        messages_schema: Schema,
        payloads: Vec<Vec<u8>>,
        offsets: Vec<u64>,
    }

    /// Runs the stub sink fails, keyed by plugin id and matched on the run's
    /// schema, so a test reads the failure off the run split rather than off
    /// a call counter.
    static FAILING_SCHEMAS: LazyLock<DashMap<u32, Schema>> = LazyLock::new(DashMap::new);

    /// High-water offset deliberately unrelated to any message offset in a
    /// batch: `current_offset` is the partition's head from the poll, and a
    /// helper that made it equal the last message's offset would let a test
    /// assert the wrong contract without failing.
    const TEST_CURRENT_OFFSET: u64 = 10_000;

    /// Records the batch the FFI call carried and returns its run schema.
    fn capture_batch(
        plugin_id: u32,
        messages_meta_ptr: *const u8,
        messages_meta_len: usize,
        messages_ptr: *const u8,
        messages_len: usize,
    ) -> Schema {
        let messages_meta =
            unsafe { std::slice::from_raw_parts(messages_meta_ptr, messages_meta_len) };
        let messages = unsafe { std::slice::from_raw_parts(messages_ptr, messages_len) };
        let metadata = postcard::from_bytes::<MessagesMetadata>(messages_meta)
            .expect("failed to deserialize messages metadata");
        let raw = postcard::from_bytes::<RawMessages>(messages).expect("failed to deserialize");

        CONSUMED.entry(plugin_id).or_default().push(ConsumedBatch {
            partition_id: metadata.partition_id,
            metadata_schema: metadata.schema,
            messages_schema: raw.schema,
            offsets: raw.messages.iter().map(|message| message.offset).collect(),
            payloads: raw
                .messages
                .into_iter()
                .map(|message| message.payload)
                .collect(),
        });
        metadata.schema
    }

    extern "C" fn capturing_consume(
        plugin_id: u32,
        _topic_meta_ptr: *const u8,
        _topic_meta_len: usize,
        messages_meta_ptr: *const u8,
        messages_meta_len: usize,
        messages_ptr: *const u8,
        messages_len: usize,
    ) -> i32 {
        capture_batch(
            plugin_id,
            messages_meta_ptr,
            messages_meta_len,
            messages_ptr,
            messages_len,
        );
        0
    }

    /// Captures like `capturing_consume`, then fails the run whose schema the
    /// test registered in `FAILING_SCHEMAS`.
    extern "C" fn selectively_failing_consume(
        plugin_id: u32,
        _topic_meta_ptr: *const u8,
        _topic_meta_len: usize,
        messages_meta_ptr: *const u8,
        messages_meta_len: usize,
        messages_ptr: *const u8,
        messages_len: usize,
    ) -> i32 {
        let schema = capture_batch(
            plugin_id,
            messages_meta_ptr,
            messages_meta_len,
            messages_ptr,
            messages_len,
        );
        match FAILING_SCHEMAS.get(&plugin_id) {
            Some(failing) if *failing == schema => 1,
            _ => 0,
        }
    }

    extern "C" fn always_failing_consume(
        plugin_id: u32,
        _topic_meta_ptr: *const u8,
        _topic_meta_len: usize,
        messages_meta_ptr: *const u8,
        messages_meta_len: usize,
        messages_ptr: *const u8,
        messages_len: usize,
    ) -> i32 {
        capture_batch(
            plugin_id,
            messages_meta_ptr,
            messages_meta_len,
            messages_ptr,
            messages_len,
        );
        1
    }

    /// Rewrites every payload to the configured variant, standing in for a
    /// third-party transform that changes the payload type.
    struct RetaggingTransform {
        payloads: Vec<Payload>,
        next: std::sync::Mutex<usize>,
    }

    impl Transform for RetaggingTransform {
        fn r#type(&self) -> TransformType {
            TransformType::AvroConvert
        }

        fn transform(
            &self,
            _metadata: &TopicMetadata,
            mut message: DecodedMessage,
        ) -> Result<Option<DecodedMessage>, Error> {
            let mut next = self.next.lock().expect("transform counter was poisoned");
            message.payload = self.payloads[*next % self.payloads.len()].clone();
            *next += 1;
            Ok(Some(message))
        }
    }

    /// Drops every message, standing in for a filter transform that matches the
    /// whole batch.
    struct DroppingTransform;

    impl Transform for DroppingTransform {
        fn r#type(&self) -> TransformType {
            TransformType::AvroConvert
        }

        fn transform(
            &self,
            _metadata: &TopicMetadata,
            _message: DecodedMessage,
        ) -> Result<Option<DecodedMessage>, Error> {
            Ok(None)
        }
    }

    fn next_plugin_id() -> u32 {
        TEST_PLUGIN_ID.fetch_add(1, Ordering::Relaxed)
    }

    fn test_message(offset: u64, payload: Vec<u8>) -> IggyMessage {
        IggyMessage {
            header: IggyMessageHeader {
                checksum: 0,
                id: u128::from(offset) + 1,
                offset,
                timestamp: 0,
                origin_timestamp: 0,
                user_headers_length: 0,
                payload_length: payload.len() as u32,
                reserved: 0,
            },
            payload: payload.into(),
            user_headers: None,
        }
    }

    fn avro_schema() -> apache_avro::Schema {
        apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}"#,
        )
        .expect("failed to parse Avro schema")
    }

    fn avro_datum(schema: &apache_avro::Schema, id: i64) -> Vec<u8> {
        let record = apache_avro::types::Value::Record(vec![(
            "id".to_owned(),
            apache_avro::types::Value::Long(id),
        )]);
        apache_avro::writer::datum::GenericDatumWriter::builder(schema)
            .build()
            .expect("failed to build Avro writer")
            .write_value_to_vec(record)
            .expect("failed to encode Avro datum")
    }

    async fn run(
        plugin_id: u32,
        decoder: Arc<dyn StreamDecoder>,
        transforms: Vec<Arc<dyn Transform>>,
        messages: Vec<IggyMessage>,
    ) -> SinkBatchTiming {
        run_with(plugin_id, capturing_consume, decoder, transforms, messages)
            .await
            .0
    }

    /// Drives one batch through `process_messages` with the given stub sink
    /// and hands back the metrics it wrote, so a test can read counters.
    async fn run_with(
        plugin_id: u32,
        consume: ConsumeCallback,
        decoder: Arc<dyn StreamDecoder>,
        transforms: Vec<Arc<dyn Transform>>,
        messages: Vec<IggyMessage>,
    ) -> (SinkBatchTiming, Arc<Metrics>) {
        let metrics = Arc::new(Metrics::init());
        let labels = SinkLabels::new("test_sink");
        let topic_metadata = TopicMetadata {
            stream: "test_stream".to_owned(),
            topic: "test_topic".to_owned(),
        };

        let timing = process_messages(
            plugin_id,
            0,
            TEST_CURRENT_OFFSET,
            &topic_metadata,
            messages,
            &consume,
            &transforms,
            &decoder,
            &metrics,
            &labels,
        )
        .await
        .expect("processing the batch should succeed");
        (timing, metrics)
    }

    /// Four messages retagged `Text, Text, Raw, Text`, which the runtime
    /// splits into three runs.
    fn split_batch() -> (Arc<RetaggingTransform>, Vec<IggyMessage>) {
        let transform = Arc::new(RetaggingTransform {
            payloads: vec![
                Payload::Text("first".to_owned()),
                Payload::Text("second".to_owned()),
                Payload::Raw(vec![9]),
                Payload::Text("fourth".to_owned()),
            ],
            next: std::sync::Mutex::new(0),
        });
        let messages = (0..4)
            .map(|offset| test_message(offset, br#"{"id":1}"#.to_vec()))
            .collect();
        (transform, messages)
    }

    fn captured(plugin_id: u32) -> Vec<ConsumedBatch> {
        CONSUMED
            .remove(&plugin_id)
            .map(|(_, batches)| batches)
            .unwrap_or_default()
    }

    /// A recovery call of the sink loop.
    #[derive(Debug, PartialEq)]
    enum Recovery {
        Abandon(u64),
        Seek(u32, PollingStrategy),
    }

    /// Counts the warnings logged under its subscriber.
    struct Warnings(Arc<AtomicUsize>);

    impl<S: Subscriber> Layer<S> for Warnings {
        fn on_event(&self, event: &tracing::Event<'_>, _: LayerContext<'_, S>) {
            if *event.metadata().level() == Level::WARN {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Hands reads to the sink loop and records its recovery calls in order.
    struct FakeMessages<S> {
        reads: S,
        abandon: Result<(), IggyError>,
        calls: Vec<Recovery>,
    }

    impl<S> FakeMessages<S> {
        fn new(reads: S) -> Self {
            Self {
                reads,
                abandon: Ok(()),
                calls: Vec::new(),
            }
        }
    }

    impl<S> Stream for FakeMessages<S>
    where
        S: Stream<Item = Result<iggy::prelude::ReceivedMessage, ReadError>> + Unpin,
    {
        type Item = S::Item;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.reads.poll_next_unpin(cx)
        }
    }

    impl<S> SinkMessages for FakeMessages<S>
    where
        S: Stream<Item = Result<iggy::prelude::ReceivedMessage, ReadError>> + Unpin,
    {
        /// As the client does, the first abandon frees `operation`.
        async fn abandon_operation(&mut self, operation: u64) -> Result<(), IggyError> {
            let abandoned = self.calls.contains(&Recovery::Abandon(operation));
            self.calls.push(Recovery::Abandon(operation));
            if abandoned {
                return Err(IggyError::OperationNotRetained(operation));
            }
            self.abandon.clone()
        }

        async fn seek(
            &mut self,
            partition_id: u32,
            start: PollingStrategy,
        ) -> Result<(), IggyError> {
            self.calls.push(Recovery::Seek(partition_id, start));
            Ok(())
        }
    }

    /// Runs the sink loop until `messages` end, into the capturing stub sink.
    async fn consume_reads(
        plugin_id: u32,
        messages: &mut impl SinkMessages,
        metrics: &Arc<Metrics>,
    ) {
        const BATCH_SIZE: u32 = 16;
        let (_shutdown, shutdown_rx) = watch::channel(());
        consume_messages(
            plugin_id,
            Schema::Json.decoder(),
            BATCH_SIZE,
            capturing_consume,
            Vec::new(),
            TopicMetadata {
                stream: "test_stream".to_owned(),
                topic: "test_topic".to_owned(),
            },
            messages,
            false,
            false,
            "test_sink",
            metrics,
            &SinkLabels::new("test_sink"),
            shutdown_rx,
        )
        .await
        .expect("consuming the stream should succeed");
    }

    #[tokio::test]
    async fn given_an_avro_stream_when_batch_is_tagged_should_use_the_decoded_payload_schema() {
        let plugin_id = next_plugin_id();
        let schema = avro_schema();
        let decoder = Arc::new(
            AvroStreamDecoder::try_new(AvroConfig {
                schema_json: Some(schema.canonical_form()),
                ..AvroConfig::default()
            })
            .expect("failed to build the Avro decoder"),
        );
        let messages = vec![
            test_message(0, avro_datum(&schema, 1)),
            test_message(1, avro_datum(&schema, 2)),
        ];

        let timing = run(plugin_id, decoder, Vec::new(), messages).await;
        let batches = captured(plugin_id);

        assert_eq!(timing.processed_count, 2);
        assert_eq!(batches.len(), 1, "a uniform batch is one FFI call");
        // The decoder reads Avro but returns JSON, so JSON is what the sink is
        // told it has.
        assert_eq!(batches[0].metadata_schema, Schema::Json);
        assert_eq!(batches[0].messages_schema, Schema::Json);
        for payload in &batches[0].payloads {
            serde_json::from_slice::<serde_json::Value>(payload).expect("payload should be JSON");
        }
    }

    #[tokio::test]
    async fn given_a_transform_changing_the_variant_when_batch_is_tagged_should_follow_the_transform()
     {
        let plugin_id = next_plugin_id();
        let transform = Arc::new(RetaggingTransform {
            payloads: vec![Payload::Avro(vec![1, 2, 3])],
            next: std::sync::Mutex::new(0),
        });
        let messages = vec![test_message(0, br#"{"id":1}"#.to_vec())];

        run(plugin_id, Schema::Json.decoder(), vec![transform], messages).await;
        let batches = captured(plugin_id);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].metadata_schema, Schema::Avro);
        assert_eq!(batches[0].messages_schema, Schema::Avro);
    }

    #[tokio::test]
    async fn given_replies_of_two_partitions_when_consumed_should_send_each_partition_in_its_own_batch()
     {
        // Larger than any batch here, so only a partition head or another
        // partition flushes one.
        const BATCH_SIZE: u32 = 16;
        let plugin_id = next_plugin_id();
        let received = |partition_id, offset, current_offset| {
            iggy::prelude::ReceivedMessage::new(
                test_message(offset, br#"{"id":1}"#.to_vec()),
                current_offset,
                partition_id,
                iggy::prelude::PartitionContext::default(),
            )
        };
        // Byte-limited replies stop each partition short of its head.
        let messages = futures::stream::iter([
            received(1, 0, 2),
            received(1, 1, 2),
            received(2, 0, 1),
            received(1, 2, 2),
            received(2, 1, 1),
        ])
        .map(Ok);
        let (_shutdown, shutdown_rx) = watch::channel(());

        consume_messages(
            plugin_id,
            Schema::Json.decoder(),
            BATCH_SIZE,
            capturing_consume,
            Vec::new(),
            TopicMetadata {
                stream: "test_stream".to_owned(),
                topic: "test_topic".to_owned(),
            },
            &mut FakeMessages::new(messages),
            false,
            false,
            "test_sink",
            &Arc::new(Metrics::init()),
            &SinkLabels::new("test_sink"),
            shutdown_rx,
        )
        .await
        .expect("consuming the stream should succeed");

        let batches: Vec<_> = captured(plugin_id)
            .into_iter()
            .map(|batch| (batch.partition_id, batch.offsets))
            .collect();
        assert_eq!(
            batches,
            [(1, vec![0, 1]), (2, vec![0]), (1, vec![2]), (2, vec![1])]
        );
    }

    #[tokio::test]
    async fn given_a_batch_short_of_the_partition_head_when_shutdown_arrives_should_still_send_it()
    {
        const BATCH_SIZE: u32 = 16;
        const PARTITION_ID: u32 = 1;
        let plugin_id = next_plugin_id();
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let received = |offset| {
            iggy::prelude::ReceivedMessage::new(
                test_message(offset, br#"{"id":1}"#.to_vec()),
                TEST_CURRENT_OFFSET,
                PARTITION_ID,
                iggy::prelude::PartitionContext::default(),
            )
        };
        // A byte-limited reply ends short of the head, so the loop asks for
        // more. That poll parks, and the shutdown arrives while it waits.
        let messages = Box::pin(
            futures::stream::iter([received(0), received(1)])
                .map(Ok)
                .chain(futures::stream::once(async move {
                    shutdown_tx
                        .send(())
                        .expect("the sink loop should still listen for shutdown");
                    std::future::pending().await
                })),
        );

        consume_messages(
            plugin_id,
            Schema::Json.decoder(),
            BATCH_SIZE,
            capturing_consume,
            Vec::new(),
            TopicMetadata {
                stream: "test_stream".to_owned(),
                topic: "test_topic".to_owned(),
            },
            &mut FakeMessages::new(messages),
            false,
            false,
            "test_sink",
            &Arc::new(Metrics::init()),
            &SinkLabels::new("test_sink"),
            shutdown_rx,
        )
        .await
        .expect("consuming the stream should succeed");

        let batches: Vec<_> = captured(plugin_id)
            .into_iter()
            .map(|batch| (batch.partition_id, batch.offsets))
            .collect();
        assert_eq!(
            batches,
            [(PARTITION_ID, vec![0, 1])],
            "the poll committed this batch, so a shutdown must not drop it"
        );
    }

    #[tokio::test]
    async fn given_reads_that_stop_a_partition_when_consumed_should_resume_each_partition() {
        const UNCERTAIN_OPERATION: u64 = 7;
        const BLOCKING_OPERATION: u64 = 8;
        const UNDECODED_OFFSET: u64 = 5;
        let plugin_id = next_plugin_id();
        let metrics = Arc::new(Metrics::init());
        let skip = PollingStrategy::offset(UNDECODED_OFFSET + 1);
        let stopped = |partition_id, error, skip| ReadError {
            partition_id: Some(partition_id),
            error,
            skip,
            offset: None,
            record: None,
        };
        let mut messages = FakeMessages::new(futures::stream::iter([
            Err(stopped(
                1,
                IggyError::FetchProgressUncertain(UNCERTAIN_OPERATION),
                None,
            )),
            Err(stopped(
                2,
                IggyError::PartitionLaneBlocked(2, BLOCKING_OPERATION),
                None,
            )),
            Err(stopped(3, IggyError::CannotDecryptData, Some(skip))),
            Err(stopped(4, IggyError::HistoryUnavailable, None)),
            Ok(iggy::prelude::ReceivedMessage::new(
                test_message(0, br#"{"id":1}"#.to_vec()),
                0,
                1,
                iggy::prelude::PartitionContext::default(),
            )),
        ]));

        consume_reads(plugin_id, &mut messages, &metrics).await;

        assert_eq!(
            messages.calls,
            [
                Recovery::Abandon(UNCERTAIN_OPERATION),
                Recovery::Seek(1, PollingStrategy::next()),
                Recovery::Abandon(BLOCKING_OPERATION),
                Recovery::Seek(2, PollingStrategy::next()),
                Recovery::Seek(3, skip),
                Recovery::Seek(4, PollingStrategy::next()),
            ]
        );
        assert_eq!(metrics.get_errors("test_sink", ConnectorType::Sink), 4);
        let batches: Vec<_> = captured(plugin_id)
            .into_iter()
            .map(|batch| (batch.partition_id, batch.offsets))
            .collect();
        assert_eq!(
            batches,
            [(1, vec![0])],
            "the loop reads on after it resumes a partition"
        );
    }

    #[tokio::test]
    async fn given_a_read_error_that_the_reader_retries_when_consumed_should_only_count_it() {
        let plugin_id = next_plugin_id();
        let metrics = Arc::new(Metrics::init());
        let mut messages = FakeMessages::new(futures::stream::iter([
            Err(ReadError {
                partition_id: None,
                error: IggyError::Disconnected,
                skip: None,
                offset: None,
                record: None,
            }),
            Err(ReadError {
                partition_id: Some(1),
                error: IggyError::TransientNotAccepted,
                skip: None,
                offset: None,
                record: None,
            }),
        ]));

        consume_reads(plugin_id, &mut messages, &metrics).await;

        assert_eq!(messages.calls, []);
        assert_eq!(metrics.get_errors("test_sink", ConnectorType::Sink), 2);
    }

    #[tokio::test]
    async fn given_an_abandon_that_fails_when_resuming_should_still_seek_the_partition() {
        const OPERATION: u64 = 7;
        let plugin_id = next_plugin_id();
        let mut messages = FakeMessages {
            abandon: Err(IggyError::FeatureUnavailable),
            ..FakeMessages::new(futures::stream::iter([Err(ReadError {
                partition_id: Some(1),
                error: IggyError::FetchProgressUncertain(OPERATION),
                skip: None,
                offset: None,
                record: None,
            })]))
        };

        consume_reads(plugin_id, &mut messages, &Arc::new(Metrics::init())).await;

        assert_eq!(
            messages.calls,
            [
                Recovery::Abandon(OPERATION),
                Recovery::Seek(1, PollingStrategy::next())
            ],
            "the seek still tries to resume the partition"
        );
    }

    #[tokio::test]
    async fn given_a_lost_fetch_of_two_partitions_when_resuming_should_warn_once_per_partition() {
        const OPERATION: u64 = 7;
        let plugin_id = next_plugin_id();
        let lost = |partition_id| ReadError {
            partition_id: Some(partition_id),
            error: IggyError::FetchProgressUncertain(OPERATION),
            skip: None,
            offset: None,
            record: None,
        };
        let mut messages = FakeMessages::new(futures::stream::iter([Err(lost(1)), Err(lost(2))]));
        let warnings = Arc::new(AtomicUsize::new(0));
        let _subscriber = tracing::subscriber::set_default(
            Registry::default().with(Warnings(Arc::clone(&warnings))),
        );

        consume_reads(plugin_id, &mut messages, &Arc::new(Metrics::init())).await;

        assert_eq!(
            messages.calls,
            [
                Recovery::Abandon(OPERATION),
                Recovery::Seek(1, PollingStrategy::next()),
                Recovery::Abandon(OPERATION),
                Recovery::Seek(2, PollingStrategy::next()),
            ]
        );
        assert_eq!(
            warnings.load(Ordering::Relaxed),
            2,
            "the second abandon finds the operation freed by the first, which is no failure"
        );
    }

    #[tokio::test]
    async fn given_mixed_payload_variants_when_batch_is_processed_should_split_into_runs() {
        let plugin_id = next_plugin_id();
        let (transform, messages) = split_batch();

        let timing = run(plugin_id, Schema::Json.decoder(), vec![transform], messages).await;
        let batches = captured(plugin_id);

        assert_eq!(timing.processed_count, 4, "no message may be lost");
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].metadata_schema, Schema::Text);
        assert_eq!(batches[0].offsets, vec![0, 1]);
        assert_eq!(batches[1].metadata_schema, Schema::Raw);
        assert_eq!(batches[1].offsets, vec![2]);
        assert_eq!(batches[2].metadata_schema, Schema::Text);
        assert_eq!(batches[2].offsets, vec![3]);
    }

    #[tokio::test]
    async fn given_a_failing_run_when_a_batch_is_split_should_debit_only_that_run() {
        let plugin_id = next_plugin_id();
        FAILING_SCHEMAS.insert(plugin_id, Schema::Raw);
        let (transform, messages) = split_batch();

        let (timing, metrics) = run_with(
            plugin_id,
            selectively_failing_consume,
            Schema::Json.decoder(),
            vec![transform],
            messages,
        )
        .await;
        let batches = captured(plugin_id);

        assert_eq!(timing.processed_count, 3, "only the failing run is lost");
        assert_eq!(timing.runs, 3);
        assert_eq!(batches.len(), 3, "a failing run does not stop later runs");
        assert_eq!(batches[2].offsets, vec![3]);
        assert_eq!(metrics.get_errors("test_sink", ConnectorType::Sink), 1);
        assert_eq!(metrics.get_sink_runs("test_sink"), 3);
    }

    #[tokio::test]
    async fn given_every_run_failing_when_a_batch_is_split_should_report_no_processed_messages() {
        let plugin_id = next_plugin_id();
        let (transform, messages) = split_batch();

        let (timing, metrics) = run_with(
            plugin_id,
            always_failing_consume,
            Schema::Json.decoder(),
            vec![transform],
            messages,
        )
        .await;

        assert_eq!(timing.processed_count, 0);
        assert_eq!(captured(plugin_id).len(), 3, "every run is still attempted");
        assert_eq!(
            metrics.get_errors("test_sink", ConnectorType::Sink),
            3,
            "one error per failed run"
        );
    }

    #[tokio::test]
    async fn given_a_batch_that_lost_every_message_when_processed_should_still_be_one_run() {
        let plugin_id = next_plugin_id();
        let messages = (0..3)
            .map(|offset| test_message(offset, br#"{"id":1}"#.to_vec()))
            .collect();

        let timing = run(
            plugin_id,
            Schema::Json.decoder(),
            vec![Arc::new(DroppingTransform)],
            messages,
        )
        .await;
        let batches = captured(plugin_id);

        assert_eq!(timing.processed_count, 0);
        assert_eq!(timing.runs, 1, "an empty batch is still one FFI call");
        assert_eq!(batches.len(), 1);
        assert!(batches[0].offsets.is_empty());
        // Nothing is left to read a variant from, so the run carries the
        // stream's configured schema rather than a payload's.
        assert_eq!(batches[0].metadata_schema, Schema::Json);
    }

    #[tokio::test]
    async fn given_a_uniform_batch_when_batch_is_processed_should_count_one_run() {
        let plugin_id = next_plugin_id();
        let messages = (0..3)
            .map(|offset| test_message(offset, br#"{"id":1}"#.to_vec()))
            .collect();

        let (timing, metrics) = run_with(
            plugin_id,
            capturing_consume,
            Schema::Json.decoder(),
            Vec::new(),
            messages,
        )
        .await;

        assert_eq!(timing.runs, 1);
        assert_eq!(metrics.get_sink_runs("test_sink"), 1);
        assert_eq!(captured(plugin_id).len(), 1);
    }

    #[tokio::test]
    async fn given_no_surviving_messages_when_batch_is_processed_should_still_call_the_sink_once() {
        let plugin_id = next_plugin_id();
        // An Avro decoder, so the fallback tag cannot be confused with
        // `Schema::default()`.
        let decoder = Arc::new(
            AvroStreamDecoder::try_new(AvroConfig {
                schema_json: Some(avro_schema().canonical_form()),
                ..AvroConfig::default()
            })
            .expect("failed to build the Avro decoder"),
        );
        let messages = vec![test_message(0, b"not an avro datum".to_vec())];

        let timing = run(plugin_id, decoder, Vec::new(), messages).await;
        let batches = captured(plugin_id);

        assert_eq!(timing.processed_count, 0);
        assert_eq!(batches.len(), 1);
        assert!(batches[0].offsets.is_empty());
        // Nothing survived to read a tag from, so the stream's configured
        // schema stands in.
        assert_eq!(batches[0].metadata_schema, Schema::Avro);
    }

    #[tokio::test]
    async fn given_a_proto_convert_transform_when_a_batch_mixes_variants_should_split_into_runs() {
        // `ProtoConvert` encodes a top-level object against its descriptor and
        // falls back to proto text for anything else, so one configured
        // instance returns `Raw` for one message and `Proto` for the next.
        let plugin_id = next_plugin_id();
        let descriptor = protox_parse::parse(
            "record.proto",
            r#"syntax = "proto3";
            message StringRecord {
                string first = 1;
            }"#,
        )
        .expect("test schema must parse");
        let transform = Arc::new(ProtoConvert::new(ProtoConvertConfig {
            source_format: Schema::Json,
            target_format: Schema::Proto,
            message_type: Some("StringRecord".to_owned()),
            descriptor_set: Some(
                prost_types::FileDescriptorSet {
                    file: vec![descriptor],
                }
                .encode_to_vec(),
            ),
            ..ProtoConvertConfig::default()
        }));
        let messages = vec![
            test_message(0, br#"{"first":"encoded"}"#.to_vec()),
            test_message(1, br#"[1,2,3]"#.to_vec()),
            test_message(2, br#"{"first":"encoded"}"#.to_vec()),
        ];

        let timing = run(plugin_id, Schema::Json.decoder(), vec![transform], messages).await;
        let batches = captured(plugin_id);

        assert_eq!(timing.processed_count, 3, "no message may be lost");
        assert_eq!(
            batches.len(),
            3,
            "each variant change starts a new FFI call"
        );
        assert_eq!(batches[0].metadata_schema, Schema::Raw);
        assert_eq!(batches[0].offsets, vec![0]);
        assert_eq!(batches[1].metadata_schema, Schema::Proto);
        assert_eq!(batches[1].offsets, vec![1]);
        assert_eq!(batches[2].metadata_schema, Schema::Raw);
        assert_eq!(batches[2].offsets, vec![2]);

        // The tag has to survive the trip back, or the sink sees a variant the
        // transform never produced.
        let rebuilt =
            Payload::try_from_schema(batches[1].messages_schema, batches[1].payloads[0].clone())
                .expect("the proto run must rebuild");
        assert_eq!(rebuilt.schema(), Schema::Proto);
    }

    #[test]
    fn given_max_wait_and_min_count_when_building_poll_options_should_wait_for_a_batch() {
        let unset = poll_options(&StreamConsumerConfig::default()).expect("defaults are valid");
        assert_eq!(
            unset,
            PollOptions {
                count: DEFAULT_BATCH_LENGTH,
                commit: CommitPolicy::OnRead,
                ..PollOptions::wait(IggyDuration::ONE_SECOND)
            }
        );

        let stream = StreamConsumerConfig {
            batch_length: Some(2000),
            max_wait: Some("30s".to_owned()),
            min_count: Some(500),
            ..StreamConsumerConfig::default()
        };
        let options = poll_options(&stream).expect("a 30 s wait is valid");

        assert_eq!(options.count, 2000);
        assert_eq!(options.max_wait.get_duration(), Duration::from_secs(30));
        assert_eq!(options.min_count, 500);
        assert_eq!(options.commit, CommitPolicy::OnRead);
        assert_eq!(options.validate(), Ok(()));
    }

    #[test]
    fn given_a_zero_max_wait_when_building_poll_options_should_refuse_it() {
        for max_wait in ["0", "unlimited"] {
            let stream = StreamConsumerConfig {
                max_wait: Some(max_wait.to_owned()),
                ..StreamConsumerConfig::default()
            };
            assert!(
                matches!(
                    poll_options(&stream),
                    Err(RuntimeError::InvalidConfiguration(_))
                ),
                "max_wait {max_wait} would poll an idle topic constantly"
            );
        }
    }

    #[test]
    fn given_a_poll_interval_when_building_poll_options_should_refuse_it() {
        let stream = StreamConsumerConfig {
            stream: "orders".to_owned(),
            poll_interval: Some("3s".to_owned()),
            max_wait: Some("3s".to_owned()),
            min_count: Some(500),
            ..StreamConsumerConfig::default()
        };

        let error = poll_options(&stream).expect_err("poll_interval is no longer supported");

        assert!(
            matches!(&error, RuntimeError::UnsupportedPollInterval(name) if name == "orders"),
            "unexpected error: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("max_wait") && message.contains("min_count"),
            "the error must name the settings that replace poll_interval, got: {message}"
        );
    }
}
