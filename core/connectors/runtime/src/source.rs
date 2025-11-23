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

use dashmap::DashMap;
use dlopen2::wrapper::Container;
use flume::{Receiver, Sender};
use iggy::prelude::{
    DirectConfig, HeaderKey, HeaderValue, IggyClient, IggyDuration, IggyError, IggyMessage,
};
use iggy_connector_sdk::{
    ConnectorState, DecodedMessage, Error, ProducedMessages, StreamEncoder, TopicMetadata,
    transforms::Transform,
};
use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, atomic::Ordering},
};
use tracing::{debug, error, info, trace, warn};

use crate::configs::connectors::SourceConfig;
use crate::{
    PLUGIN_ID, RuntimeError, SourceApi, SourceConnector, SourceConnectorPlugin,
    SourceConnectorProducer, SourceConnectorWrapper, resolve_plugin_path,
    state::{FileStateProvider, StateProvider, StateStorage},
    transform,
};

pub static SOURCE_SENDERS: Lazy<DashMap<u32, Sender<ProducedMessages>>> = Lazy::new(DashMap::new);

pub async fn init(
    source_configs: HashMap<String, SourceConfig>,
    iggy_client: &IggyClient,
    state_path: &str,
) -> Result<HashMap<String, SourceConnector>, RuntimeError> {
    let mut source_connectors: HashMap<String, SourceConnector> = HashMap::new();
    for (key, config) in source_configs {
        let name = config.name;
        if !config.enabled {
            warn!("Source: {name} is disabled ({key})");
            continue;
        }

        let plugin_id = PLUGIN_ID.load(Ordering::Relaxed);
        let path = resolve_plugin_path(&config.path);
        info!(
            "Initializing source container with name: {name} ({key}), config version: {}, plugin: {path}",
            &config.version
        );
        let state_storage = get_state_storage(state_path, &key);
        let state = match &state_storage {
            StateStorage::File(file) => file.load().await?,
        };
        if let Some(container) = source_connectors.get_mut(&path) {
            info!("Source container for plugin: {path} is already loaded.",);
            init_source(
                &container.container,
                &config.plugin_config.unwrap_or_default(),
                plugin_id,
                state,
            );
            container.plugins.push(SourceConnectorPlugin {
                id: plugin_id,
                key: key.to_owned(),
                name: name.to_owned(),
                path: path.to_owned(),
                config_format: config.plugin_config_format,
                producer: None,
                transforms: vec![],
                state_storage,
            });
        } else {
            let container: Container<SourceApi> =
                unsafe { Container::load(&path).expect("Failed to load source container") };
            info!("Source container for plugin: {path} loaded successfully.",);
            init_source(
                &container,
                &config.plugin_config.unwrap_or_default(),
                plugin_id,
                state,
            );
            source_connectors.insert(
                path.to_owned(),
                SourceConnector {
                    container,
                    plugins: vec![SourceConnectorPlugin {
                        id: plugin_id,
                        key: key.to_owned(),
                        name: name.to_owned(),
                        path: path.to_owned(),
                        config_format: config.plugin_config_format,
                        producer: None,
                        transforms: vec![],
                        state_storage,
                    }],
                },
            );
        }

        info!(
            "Source container with name: {name} ({key}), initialized successfully with ID: {plugin_id}."
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
            info!("Enabled transforms for source: {name} ({key}): {types}",);
            transforms
        } else {
            vec![]
        };

        let connector = source_connectors
            .get_mut(&path)
            .expect("Failed to get source connector");
        let plugin = connector
            .plugins
            .iter_mut()
            .find(|p| p.id == plugin_id)
            .expect("Failed to get source plugin");

        for stream in config.streams.iter() {
            let linger_time =
                IggyDuration::from_str(stream.linger_time.as_deref().unwrap_or("5ms"))
                    .expect("Invalid send interval");
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
            plugin.producer = Some(SourceConnectorProducer {
                producer,
                encoder: stream.schema.encoder(),
            });
            plugin.transforms = transforms.clone();
        }
    }

    Ok(source_connectors)
}

fn init_source(
    container: &Container<SourceApi>,
    plugin_config: &serde_json::Value,
    id: u32,
    state: Option<ConnectorState>,
) {
    trace!("Initializing source plugin with config: {plugin_config:?} (ID: {id})");
    let plugin_config =
        serde_json::to_string(plugin_config).expect("Invalid source plugin config.");
    let state_ptr = state.as_ref().map_or(std::ptr::null(), |s| s.0.as_ptr());
    let state_len = state.as_ref().map_or(0, |s| s.0.len());
    (container.open)(
        id,
        plugin_config.as_ptr(),
        plugin_config.len(),
        state_ptr,
        state_len,
    );
}

fn get_state_storage(state_path: &str, key: &str) -> StateStorage {
    let path = format!("{state_path}/source_{key}.state");
    StateStorage::File(FileStateProvider::new(path))
}

pub fn handle(sources: Vec<SourceConnectorWrapper>) {
    for source in sources {
        for plugin in source.plugins {
            let plugin_id = plugin.id;
            info!("Starting handler for source connector with ID: {plugin_id}...");
            let handle = source.callback;
            tokio::task::spawn_blocking(move || {
                handle(plugin_id, handle_produced_messages);
            });
            info!("Handler for source connector with ID: {plugin_id} started successfully.");

            let (sender, receiver): (Sender<ProducedMessages>, Receiver<ProducedMessages>) =
                flume::unbounded();
            SOURCE_SENDERS.insert(plugin_id, sender);
            tokio::spawn(async move {
                info!("Source connector with ID: {plugin_id} started.");
                let Some(producer) = &plugin.producer else {
                    error!("Producer not initialized for source connector with ID: {plugin_id}");
                    return;
                };
                let encoder = producer.encoder.clone();
                let producer = &producer.producer;
                let mut number = 1u64;

                let topic_metadata = TopicMetadata {
                    stream: producer.stream().to_string(),
                    topic: producer.topic().to_string(),
                };

                while let Ok(produced_messages) = receiver.recv_async().await {
                    let count = produced_messages.messages.len();
                    info!("Source connector with ID: {plugin_id} received {count} messages",);
                    let schema = produced_messages.schema;
                    let mut messages: Vec<DecodedMessage> = Vec::with_capacity(count);
                    for message in produced_messages.messages {
                        let Ok(payload) = schema.try_into_payload(message.payload) else {
                            error!(
                                "Failed to decode message payload with schema: {} for source connector with ID: {plugin_id}",
                                produced_messages.schema
                            );
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

                    let Ok(iggy_messages) = process_messages(
                        plugin_id,
                        &encoder,
                        &topic_metadata,
                        messages,
                        &plugin.transforms,
                    ) else {
                        error!(
                            "Failed to process {count} messages by source connector with ID: {plugin_id} before sending them to stream: {}, topic: {}.",
                            producer.stream(),
                            producer.topic()
                        );
                        continue;
                    };

                    if let Err(error) = producer.send(iggy_messages).await {
                        error!(
                            "Failed to send {count} messages to stream: {}, topic: {} by source connector with ID: {plugin_id}. {error}",
                            producer.stream(),
                            producer.topic(),
                        );
                        continue;
                    }

                    info!(
                        "Sent {count} messages to stream: {}, topic: {} by source connector with ID: {plugin_id}",
                        producer.stream(),
                        producer.topic()
                    );

                    let Some(state) = produced_messages.state else {
                        debug!("No state provided for source connector with ID: {plugin_id}");
                        continue;
                    };

                    match &plugin.state_storage {
                        StateStorage::File(file) => {
                            if let Err(error) = file.save(state).await {
                                error!(
                                    "Failed to save state for source connector with ID: {plugin_id}. {error}"
                                );
                                continue;
                            }
                            debug!("State saved for source connector with ID: {plugin_id}");
                        }
                    }
                }
            });
        }
    }
}

fn process_messages(
    id: u32,
    encoder: &Arc<dyn StreamEncoder>,
    topic_metadata: &TopicMetadata,
    messages: Vec<DecodedMessage>,
    transforms: &Vec<Arc<dyn Transform>>,
) -> Result<Vec<IggyMessage>, Error> {
    let mut iggy_messages = Vec::with_capacity(messages.len());
    for message in messages {
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

        let Ok(payload) = encoder.encode(message.payload) else {
            error!(
                "Failed to encode message payload for source connector with ID: {id}, stream: {}, topic: {}",
                topic_metadata.stream, topic_metadata.topic
            );
            continue;
        };

        let Ok(iggy_message) = build_iggy_message(payload, message.id, message.headers) else {
            error!(
                "Failed to build Iggy message for source connector with ID: {id}, stream: {}, topic: {}",
                topic_metadata.stream, topic_metadata.topic
            );
            continue;
        };

        iggy_messages.push(iggy_message);
    }
    Ok(iggy_messages)
}

extern "C" fn handle_produced_messages(
    plugin_id: u32,
    messages_ptr: *const u8,
    messages_len: usize,
) {
    unsafe {
        if let Some(sender) = SOURCE_SENDERS.get(&plugin_id) {
            let messages = std::slice::from_raw_parts(messages_ptr, messages_len);
            match postcard::from_bytes::<ProducedMessages>(messages) {
                Ok(messages) => {
                    let _ = sender.send(messages);
                }
                Err(err) => {
                    error!(
                        "Failed to deserialize produced messages for source connector with ID: {plugin_id}. {err}"
                    );
                }
            }
        }
    }
}

fn build_iggy_message(
    payload: Vec<u8>,
    id: Option<u128>,
    headers: Option<HashMap<HeaderKey, HeaderValue>>,
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
