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

use crate::configs::connectors::SinkConfig;
use crate::{
    PLUGIN_ID, RuntimeError, SinkApi, SinkConnector, SinkConnectorConsumer, SinkConnectorPlugin,
    SinkConnectorWrapper, resolve_plugin_path, transform,
};
use dlopen2::wrapper::Container;
use futures::StreamExt;
use iggy::prelude::{
    AutoCommit, AutoCommitWhen, IggyClient, IggyConsumer, IggyDuration, IggyMessage,
    PollingStrategy,
};
use iggy_connector_sdk::{
    DecodedMessage, MessagesMetadata, RawMessage, RawMessages, ReceivedMessage, StreamDecoder,
    TopicMetadata, sink::ConsumeCallback, transforms::Transform,
};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
use tracing::{error, info, warn};

pub async fn init(
    sink_configs: HashMap<String, SinkConfig>,
    iggy_client: &IggyClient,
) -> Result<HashMap<String, SinkConnector>, RuntimeError> {
    let mut sink_connectors: HashMap<String, SinkConnector> = HashMap::new();
    for (key, config) in sink_configs {
        let name = config.name;
        if !config.enabled {
            warn!("Sink: {name} is disabled ({key})");
            continue;
        }

        let plugin_id = PLUGIN_ID.load(Ordering::Relaxed);
        let path = resolve_plugin_path(&config.path);
        info!(
            "Initializing sink container with name: {name} ({key}), config version: {}, plugin: {path}",
            &config.version
        );
        if let Some(container) = sink_connectors.get_mut(&path) {
            info!("Sink container for plugin: {path} is already loaded.",);
            init_sink(
                &container.container,
                &config.plugin_config.unwrap_or_default(),
                plugin_id,
            );
            container.plugins.push(SinkConnectorPlugin {
                id: plugin_id,
                key: key.to_owned(),
                name: name.to_owned(),
                path: path.to_owned(),
                config_format: config.plugin_config_format,
                consumers: vec![],
            });
        } else {
            let container: Container<SinkApi> =
                unsafe { Container::load(&path).expect("Failed to load sink container") };
            info!("Sink container for plugin: {path} loaded successfully.",);
            init_sink(
                &container,
                &config.plugin_config.unwrap_or_default(),
                plugin_id,
            );
            sink_connectors.insert(
                path.to_owned(),
                SinkConnector {
                    container,
                    plugins: vec![SinkConnectorPlugin {
                        id: plugin_id,
                        key: key.to_owned(),
                        name: name.to_owned(),
                        path: path.to_owned(),
                        config_format: config.plugin_config_format,
                        consumers: vec![],
                    }],
                },
            );
        }

        info!(
            "Sink container with name: {name} ({key}), initialized successfully with ID: {plugin_id}."
        );
        PLUGIN_ID.fetch_add(1, Ordering::Relaxed);

        let transforms = if let Some(transforms_config) = config.transforms {
            let transforms =
                transform::load(&transforms_config).expect("Failed to load transforms");
            let types = transforms
                .iter()
                .map(|t| t.r#type().into())
                .collect::<Vec<&'static str>>()
                .join(", ");
            info!("Enabled transforms for sink: {name} ({key}): {types}",);
            transforms
        } else {
            vec![]
        };

        let connector = sink_connectors
            .get_mut(&path)
            .expect("Failed to get sink connector");
        let plugin = connector
            .plugins
            .iter_mut()
            .find(|p| p.id == plugin_id)
            .expect("Failed to get sink plugin");

        for stream in config.streams.iter() {
            let poll_interval =
                IggyDuration::from_str(stream.poll_interval.as_deref().unwrap_or("5ms"))
                    .expect("Invalid poll interval");
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
                plugin.consumers.push(SinkConnectorConsumer {
                    consumer,
                    decoder: stream.schema.decoder(),
                    batch_size: batch_length,
                    transforms: transforms.clone(),
                });
            }
        }
    }

    Ok(sink_connectors)
}

pub fn consume(sinks: Vec<SinkConnectorWrapper>) {
    for sink in sinks {
        for plugin in sink.plugins {
            info!("Starting consume for sink with ID: {}...", plugin.id);
            for consumer in plugin.consumers {
                tokio::spawn(async move {
                    if let Err(error) = consume_messages(
                        plugin.id,
                        consumer.decoder,
                        consumer.batch_size,
                        sink.callback,
                        consumer.transforms,
                        consumer.consumer,
                    )
                    .await
                    {
                        error!(
                            "Failed to consume messages for sink connector with ID: {}. {error}",
                            plugin.id
                        );
                        return;
                    }
                    info!(
                        "Consume messages for sink connector with ID: {} started successfully.",
                        plugin.id
                    );
                });
            }
        }
    }
}

async fn consume_messages(
    plugin_id: u32,
    decoder: Arc<dyn StreamDecoder>,
    batch_size: u32,
    consume: ConsumeCallback,
    transforms: Vec<Arc<dyn Transform>>,
    mut consumer: IggyConsumer,
) -> Result<(), RuntimeError> {
    info!("Started consuming messages for sink connector with ID: {plugin_id}");
    let batch_size = batch_size as usize;
    let mut batch = Vec::with_capacity(batch_size);
    let topic_metadata = TopicMetadata {
        stream: consumer.stream().to_string(),
        topic: consumer.topic().to_string(),
    };

    while let Some(message) = consumer.next().await {
        let Ok(message) = message else {
            error!("Failed to receive message.");
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
        let messages_metadata = MessagesMetadata {
            partition_id,
            current_offset,
            schema: decoder.schema(),
        };
        info!(
            "Processing {messages_count} messages for sink connector with ID: {}",
            plugin_id
        );
        let start = Instant::now();
        if let Err(error) = process_messages(
            plugin_id,
            messages_metadata,
            &topic_metadata,
            messages,
            &consume,
            &transforms,
            &decoder,
        )
        .await
        {
            error!(
                "Failed to process {messages_count} messages for sink connector with ID: {plugin_id}. {error}",
            );
            continue;
        }

        let elapsed = start.elapsed();
        info!(
            "Consumed {messages_count} messages in {:#?} for sink connector with ID: {plugin_id}",
            elapsed
        );
    }
    info!("Stopped consuming messages for sink connector with ID: {plugin_id}");
    Ok(())
}

fn init_sink(container: &Container<SinkApi>, plugin_config: &serde_json::Value, id: u32) {
    let plugin_config = serde_json::to_string(plugin_config).expect("Invalid sink plugin config.");
    (container.open)(id, plugin_config.as_ptr(), plugin_config.len());
}

async fn process_messages(
    plugin_id: u32,
    messages_metadata: MessagesMetadata,
    topic_metadata: &TopicMetadata,
    messages: Vec<IggyMessage>,
    consume: &ConsumeCallback,
    transforms: &Vec<Arc<dyn Transform>>,
    decoder: &Arc<dyn StreamDecoder>,
) -> Result<(), RuntimeError> {
    let messages = messages.into_iter().map(|message| ReceivedMessage {
        id: message.header.id,
        offset: message.header.offset,
        checksum: message.header.checksum,
        timestamp: message.header.timestamp,
        origin_timestamp: message.header.origin_timestamp,
        headers: message.user_headers_map().unwrap_or_default(),
        payload: message.payload.into(),
    });

    let count = messages.len();
    let decoded_messages = messages.into_iter().flat_map(|message| {
        let Ok(payload) = decoder.decode(message.payload) else {
            return None;
        };

        Some(DecodedMessage {
            id: Some(message.id),
            offset: Some(message.offset),
            checksum: Some(message.checksum),
            timestamp: Some(message.timestamp),
            origin_timestamp: Some(message.origin_timestamp),
            headers: message.headers,
            payload,
        })
    });
    let mut messages = Vec::with_capacity(count);
    for message in decoded_messages {
        let mut current_message = Some(message);
        for transform in transforms.iter() {
            let Some(message) = current_message else {
                break;
            };

            current_message = transform.transform(topic_metadata, message)?;
        }

        // The transform may return no message based on some conditions
        let Some(message) = current_message else {
            continue;
        };

        let Some(id) = message.id else {
            error!(
                "ID should be present. Failed to process message for sink connector with ID: {plugin_id}"
            );
            continue;
        };

        let Some(offset) = message.offset else {
            error!(
                "Offset should be present. Failed to process message with ID: {id} for sink connector with ID: {plugin_id}"
            );
            continue;
        };

        let Some(checksum) = message.checksum else {
            error!(
                "Checksum should be present. Failed to process message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}"
            );
            continue;
        };

        let Some(timestamp) = message.timestamp else {
            error!(
                "Timestamp should be present. Failed to process message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}"
            );
            continue;
        };

        let Some(origin_timestamp) = message.origin_timestamp else {
            error!(
                "Origin timestamp should be present. Failed to process message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}"
            );
            continue;
        };

        let Ok(payload) = message.payload.try_into_vec() else {
            error!(
                "Failed to get message payload for message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}"
            );
            continue;
        };

        let headers: Result<Vec<u8>, RuntimeError> = if let Some(headers) = message.headers {
            Ok(postcard::to_allocvec(&headers).map_err(|error| {
                error!("Failed to serialize headers for message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}. {error}");
                RuntimeError::FailedToSerializeHeaders
            })?)
        } else {
            Ok(vec![])
        };

        let Ok(headers) = headers else {
            error!(
                "Failed to serialize message headers for message with ID: {id}, offset: {offset} for sink connector with ID: {plugin_id}"
            );
            continue;
        };

        messages.push(RawMessage {
            id,
            offset,
            checksum,
            timestamp,
            origin_timestamp,
            headers,
            payload,
        });
    }

    let topic_meta = postcard::to_allocvec(topic_metadata).map_err(|error| {
        error!(
            "Failed to serialize topic metadata for sink connector with ID: {plugin_id}. {error}"
        );
        RuntimeError::FailedToSerializeTopicMetadata
    })?;

    let messages_meta = postcard::to_allocvec(&messages_metadata).map_err(|error| {
        error!(
            "Failed to serialize messages metadata for sink connector with ID: {plugin_id}. {error}"
        );
        RuntimeError::FailedToSerializeMessagesMetadata
    })?;

    let messages = postcard::to_allocvec(&RawMessages {
        schema: decoder.schema(),
        messages,
    })
    .map_err(|error| {
        error!("Failed to serialize messages for sink connector with ID: {plugin_id}. {error}");
        RuntimeError::FailedToSerializeRawMessages
    })?;

    (consume)(
        plugin_id,
        topic_meta.as_ptr(),
        topic_meta.len(),
        messages_meta.as_ptr(),
        messages_meta.len(),
        messages.as_ptr(),
        messages.len(),
    );

    Ok(())
}
