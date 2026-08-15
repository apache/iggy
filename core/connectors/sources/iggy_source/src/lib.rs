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

use async_trait::async_trait;
use iggy::prelude::{
    Client, CompressionAlgorithm, Consumer, Identifier, IggyClient, IggyError, IggyMessage,
    MessageClient, PollingStrategy, StreamClient, TopicClient, TopicCreateOptions,
};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    retry::{exponential_backoff, jitter, parse_duration},
    source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{sync::Mutex, time::sleep};
use tracing::{debug, error, info, warn};

source_connector!(IggySource);

const CONNECTOR_NAME: &str = "Iggy source";
const DEFAULT_POLL_INTERVAL: &str = "2s";
const DEFAULT_RETRY_INTERVAL: &str = "1s";
const DEFAULT_MAX_RETRY_INTERVAL: &str = "60s";
const DEFAULT_BATCH_SIZE: u32 = 100;
const AUTO_CREATED_PARTITIONS_COUNT: u32 = 1;

/// Configuration for the Iggy source connector, replicating a topic from an
/// upstream Iggy cluster. `connection_string` points at the upstream cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IggySourceConfig {
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_string: SecretString,
    pub upstream_stream: String,
    pub upstream_topic: String,
    pub poll_interval: Option<String>,
    pub batch_size: Option<u32>,
    pub initial_offset: Option<String>,
    pub include_user_headers: Option<bool>,
    pub retry_interval: Option<String>,
    pub max_retry_interval: Option<String>,
    pub verbose_logging: Option<bool>,
}

/// Starting point for a partition that has no saved offset in the state yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialOffset {
    Earliest,
    Latest,
    Offset(u64),
}

impl FromStr for InitialOffset {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "earliest" => Ok(Self::Earliest),
            "latest" => Ok(Self::Latest),
            other => other.parse::<u64>().map(Self::Offset).map_err(|_| ()),
        }
    }
}

/// Persisted state. `offsets` maps each upstream partition to the offset of
/// the last message that was handed to the runtime and therefore successfully
/// produced to the downstream cluster; the runtime persists the state only
/// after a successful downstream send, so a restart resumes from `offset + 1`.
#[derive(Debug, Serialize, Deserialize)]
struct State {
    offsets: HashMap<u32, u64>,
    messages_synced: u64,
    errors_count: u64,
}

#[derive(Debug)]
pub struct IggySource {
    id: u32,
    config: IggySourceConfig,
    client: Option<IggyClient>,
    state: Mutex<State>,
    partitions: Vec<u32>,
    stream_id: Option<Identifier>,
    topic_id: Option<Identifier>,
    poll_interval: Duration,
    retry_interval: Duration,
    max_retry_interval: Duration,
    consecutive_failures: AtomicU64,
    initial_offset: InitialOffset,
    batch_size: u32,
    include_user_headers: bool,
    verbose: bool,
}

impl IggySource {
    pub fn new(id: u32, config: IggySourceConfig, state: Option<ConnectorState>) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let restored_state = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector ID: {id}. \
                     Offsets: {:?}, messages synced: {}, errors: {}",
                    s.offsets, s.messages_synced, s.errors_count
                );
            });

        let poll_interval = parse_duration(config.poll_interval.as_deref(), DEFAULT_POLL_INTERVAL);
        let retry_interval =
            parse_duration(config.retry_interval.as_deref(), DEFAULT_RETRY_INTERVAL);
        let max_retry_interval = parse_duration(
            config.max_retry_interval.as_deref(),
            DEFAULT_MAX_RETRY_INTERVAL,
        );

        let initial_offset = config
            .initial_offset
            .as_deref()
            .map(InitialOffset::from_str)
            .and_then(Result::ok)
            .unwrap_or_else(|| {
                warn!(
                    "Invalid initial offset {:?} for {CONNECTOR_NAME} connector ID: {id}, \
                     defaulting to earliest",
                    config.initial_offset
                );
                InitialOffset::Earliest
            });

        let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let include_user_headers = config.include_user_headers.unwrap_or(true);

        IggySource {
            id,
            config,
            client: None,
            state: Mutex::new(restored_state.unwrap_or(State {
                offsets: HashMap::new(),
                messages_synced: 0,
                errors_count: 0,
            })),
            partitions: Vec::new(),
            stream_id: None,
            topic_id: None,
            poll_interval,
            retry_interval,
            max_retry_interval,
            consecutive_failures: AtomicU64::new(0),
            initial_offset,
            batch_size,
            include_user_headers,
            verbose,
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    async fn ensure_stream_and_topic(
        &self,
        client: &IggyClient,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), Error> {
        match client.get_stream(stream_id).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                warn!(
                    "Upstream stream '{}' does not exist, creating it for {CONNECTOR_NAME} \
                     connector ID: {}",
                    self.config.upstream_stream, self.id
                );
                client
                    .create_stream(&self.config.upstream_stream)
                    .await
                    .map_err(|e| {
                        Error::InitError(format!(
                            "Failed to create upstream stream '{}': {e}",
                            self.config.upstream_stream
                        ))
                    })?;
            }
            Err(e) => {
                return Err(Error::InitError(format!(
                    "Failed to check upstream stream '{}': {e}",
                    self.config.upstream_stream
                )));
            }
        }

        match client.get_topic(stream_id, topic_id).await {
            Ok(Some(_)) => Ok(()),
            Ok(None) => {
                warn!(
                    "Upstream topic '{}' does not exist, creating it with \
                     {AUTO_CREATED_PARTITIONS_COUNT} partition(s) for {CONNECTOR_NAME} \
                     connector ID: {}",
                    self.config.upstream_topic, self.id
                );
                client
                    .create_topic(
                        stream_id,
                        &self.config.upstream_topic,
                        &TopicCreateOptions {
                            partitions_count: Some(AUTO_CREATED_PARTITIONS_COUNT),
                            compression_algorithm: Some(CompressionAlgorithm::None),
                            ..TopicCreateOptions::default()
                        },
                    )
                    .await
                    .map_err(|e| {
                        Error::InitError(format!(
                            "Failed to create upstream topic '{}': {e}",
                            self.config.upstream_topic
                        ))
                    })?;
                Ok(())
            }
            Err(e) => Err(Error::InitError(format!(
                "Failed to check upstream topic '{}': {e}",
                self.config.upstream_topic
            ))),
        }
    }
}

#[async_trait]
impl Source for IggySource {
    async fn open(&mut self) -> Result<(), Error> {
        let redacted = redact_connection_string(self.config.connection_string.expose_secret());
        info!(
            "Opening {CONNECTOR_NAME} connector ID: {}, upstream: {}/{} at {}",
            self.id, self.config.upstream_stream, self.config.upstream_topic, redacted
        );

        let client =
            IggyClient::from_connection_string(self.config.connection_string.expose_secret())
                .map_err(|e| {
                    Error::InitError(format!("Failed to parse upstream connection string: {e}"))
                })?;

        client.connect().await.map_err(|e| {
            Error::InitError(format!("Failed to connect to upstream Iggy cluster: {e}"))
        })?;

        let stream_id = Identifier::named(&self.config.upstream_stream).map_err(|_| {
            Error::InvalidConfigValue(format!(
                "Invalid upstream stream name '{}'",
                self.config.upstream_stream
            ))
        })?;
        let topic_id = Identifier::named(&self.config.upstream_topic).map_err(|_| {
            Error::InvalidConfigValue(format!(
                "Invalid upstream topic name '{}'",
                self.config.upstream_topic
            ))
        })?;

        self.stream_id = Some(stream_id.clone());
        self.topic_id = Some(topic_id.clone());

        self.ensure_stream_and_topic(&client, &stream_id, &topic_id)
            .await?;

        let topic = client
            .get_topic(&stream_id, &topic_id)
            .await
            .map_err(|e| Error::InitError(format!("Failed to fetch upstream topic details: {e}")))?
            .ok_or_else(|| {
                Error::InitError(format!(
                    "Upstream topic '{}/{}' not found after creation",
                    self.config.upstream_stream, self.config.upstream_topic
                ))
            })?;
        self.partitions = topic
            .partitions
            .iter()
            .map(|partition| partition.id)
            .collect();

        self.client = Some(client);
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, partitions: {}, initial offset: {:?}, \
             poll interval: {:?}, batch size: {}",
            self.id,
            self.partitions.len(),
            self.initial_offset,
            self.poll_interval,
            self.batch_size
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        sleep(self.poll_interval).await;

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::InitError("Upstream client not connected".to_string()))?;
        let stream_id = self
            .stream_id
            .as_ref()
            .ok_or_else(|| Error::InitError("Upstream stream not initialized".to_string()))?;
        let topic_id = self
            .topic_id
            .as_ref()
            .ok_or_else(|| Error::InitError("Upstream topic not initialized".to_string()))?;

        let failures = self.consecutive_failures.load(Ordering::Relaxed);
        if failures > 0 {
            let delay = jitter(exponential_backoff(
                self.retry_interval,
                failures as u32,
                self.max_retry_interval,
            ));
            debug!(
                "Backing off for {delay:?} after {failures} consecutive failures for \
                 {CONNECTOR_NAME} connector ID: {}",
                self.id
            );
            sleep(delay).await;
        }

        let consumer = Consumer::default();

        let saved_offsets: HashMap<u32, u64> = {
            let state = self.state.lock().await;
            state.offsets.clone()
        };

        let mut messages = Vec::with_capacity(self.partitions.len() * self.batch_size as usize);
        let mut new_offsets: HashMap<u32, u64> = HashMap::new();
        let mut reset_offsets: Vec<u32> = Vec::new();
        let mut errors_in_cycle: u64 = 0;
        let mut any_success = false;
        let mut connection_failure = false;

        for &partition_id in &self.partitions {
            let strategy = next_strategy(
                self.initial_offset,
                saved_offsets.get(&partition_id).copied(),
            );
            let polled = client
                .poll_messages(
                    stream_id,
                    topic_id,
                    Some(partition_id),
                    &consumer,
                    &strategy,
                    self.batch_size,
                    false,
                )
                .await;

            match polled {
                Ok(polled) => {
                    any_success = true;
                    if polled.messages.is_empty() {
                        continue;
                    }
                    let last_offset = polled
                        .messages
                        .last()
                        .map(|message| message.header.offset)
                        .unwrap_or_default();
                    for message in &polled.messages {
                        match build_produced_message(message, self.include_user_headers) {
                            Ok(produced) => messages.push(produced),
                            Err(e) => {
                                error!(
                                    "Failed to convert upstream message at offset {} on \
                                     partition {partition_id} for {CONNECTOR_NAME} connector \
                                     ID: {}: {e}",
                                    message.header.offset, self.id
                                );
                                errors_in_cycle += 1;
                            }
                        }
                    }
                    new_offsets.insert(partition_id, last_offset);
                }
                Err(IggyError::InvalidOffset(offset)) => {
                    warn!(
                        "Saved offset {offset} for partition {partition_id} no longer valid \
                         for {CONNECTOR_NAME} connector ID: {}, resetting to initial offset",
                        self.id
                    );
                    reset_offsets.push(partition_id);
                    errors_in_cycle += 1;
                }
                Err(e) => {
                    error!(
                        "Failed to poll partition {partition_id} for {CONNECTOR_NAME} \
                         connector ID: {}: {e}",
                        self.id
                    );
                    errors_in_cycle += 1;
                    connection_failure = true;
                    break;
                }
            }
        }

        let (persisted_state, total_synced) = {
            let mut state = self.state.lock().await;
            for (partition_id, offset) in new_offsets {
                state.offsets.insert(partition_id, offset);
            }
            for partition_id in reset_offsets {
                state.offsets.remove(&partition_id);
            }
            state.messages_synced += messages.len() as u64;
            state.errors_count += errors_in_cycle;
            let persisted = self.serialize_state(&state);
            (persisted, state.messages_synced)
        };

        if connection_failure && !any_success {
            self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        } else {
            self.consecutive_failures.store(0, Ordering::Relaxed);
        }

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} connector ID: {} polled {} messages from {} partition(s). \
                 Total synced: {}, errors in cycle: {}",
                self.id,
                messages.len(),
                self.partitions.len(),
                total_synced,
                errors_in_cycle
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} connector ID: {} polled {} messages from {} partition(s). \
                 Total synced: {}, errors in cycle: {}",
                self.id,
                messages.len(),
                self.partitions.len(),
                total_synced,
                errors_in_cycle
            );
        }

        Ok(ProducedMessages {
            schema: Schema::Raw,
            messages,
            state: persisted_state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(client) = self.client.take()
            && let Err(e) = client.disconnect().await
        {
            warn!(
                "Failed to disconnect from upstream cluster for {CONNECTOR_NAME} \
                 connector ID: {}: {e}",
                self.id
            );
        }

        let state = self.state.lock().await;
        info!(
            "{CONNECTOR_NAME} connector ID: {} closed. Total messages synced: {}, total errors: {}",
            self.id, state.messages_synced, state.errors_count
        );
        Ok(())
    }
}

fn next_strategy(initial: InitialOffset, saved_offset: Option<u64>) -> PollingStrategy {
    match saved_offset {
        Some(offset) => PollingStrategy::offset(offset.saturating_add(1)),
        None => match initial {
            InitialOffset::Earliest => PollingStrategy::first(),
            InitialOffset::Latest => PollingStrategy::last(),
            InitialOffset::Offset(offset) => PollingStrategy::offset(offset),
        },
    }
}

fn build_produced_message(
    message: &IggyMessage,
    include_user_headers: bool,
) -> Result<ProducedMessage, Error> {
    let headers = if include_user_headers {
        message.user_headers_map().map_err(|e| {
            Error::InvalidRecordValue(format!("Failed to parse upstream user headers: {e}"))
        })?
    } else {
        None
    };

    Ok(ProducedMessage {
        id: (message.header.id != 0).then_some(message.header.id),
        headers,
        checksum: None,
        timestamp: None,
        origin_timestamp: (message.header.origin_timestamp != 0)
            .then_some(message.header.origin_timestamp),
        payload: message.payload.to_vec(),
    })
}

fn redact_connection_string(connection_string: &str) -> String {
    let Some(scheme_end) = connection_string.find("://") else {
        return "***".to_string();
    };
    let Some(rest) = connection_string.get(scheme_end + 3..) else {
        return "***".to_string();
    };
    let Some(at_offset) = rest.find('@') else {
        return "***".to_string();
    };
    let userinfo_end = scheme_end + 3 + at_offset;
    let userinfo = &connection_string[scheme_end + 3..userinfo_end];
    let redact_from = match userinfo.find(':') {
        Some(colon) => scheme_end + 3 + colon + 1,
        None => scheme_end + 3,
    };
    let mut redacted = connection_string.to_string();
    redacted.replace_range(redact_from..userinfo_end, "***");
    redacted
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> IggySourceConfig {
        IggySourceConfig {
            connection_string: SecretString::from(
                "iggy+tcp://iggy:iggy@127.0.0.1:8090".to_string(),
            ),
            upstream_stream: "upstream_stream".to_string(),
            upstream_topic: "upstream_topic".to_string(),
            poll_interval: Some("100ms".to_string()),
            batch_size: Some(50),
            initial_offset: Some("0".to_string()),
            include_user_headers: Some(true),
            retry_interval: Some("100ms".to_string()),
            max_retry_interval: Some("5s".to_string()),
            verbose_logging: Some(false),
        }
    }

    #[test]
    fn given_persisted_state_should_restore_offsets_and_counts() {
        let state = State {
            offsets: HashMap::from([(0, 42), (1, 7)]),
            messages_synced: 500,
            errors_count: 3,
        };

        let serialized = rmp_serde::to_vec(&state).expect("Failed to serialize state");
        let connector_state = ConnectorState(serialized);

        let source = IggySource::new(1, test_config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert_eq!(restored.offsets.get(&0), Some(&42));
            assert_eq!(restored.offsets.get(&1), Some(&7));
            assert_eq!(restored.messages_synced, 500);
            assert_eq!(restored.errors_count, 3);
        });
    }

    #[test]
    fn given_no_state_should_start_fresh() {
        let source = IggySource::new(1, test_config(), None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert!(state.offsets.is_empty());
            assert_eq!(state.messages_synced, 0);
            assert_eq!(state.errors_count, 0);
        });
    }

    #[test]
    fn given_invalid_state_should_start_fresh() {
        let invalid_state = ConnectorState(b"not valid msgpack".to_vec());
        let source = IggySource::new(1, test_config(), Some(invalid_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert!(state.offsets.is_empty());
            assert_eq!(state.messages_synced, 0);
            assert_eq!(state.errors_count, 0);
        });
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let original = State {
            offsets: HashMap::from([(2, 1000)]),
            messages_synced: 1000,
            errors_count: 5,
        };

        let serialized = rmp_serde::to_vec(&original).expect("Failed to serialize");
        let deserialized: State =
            rmp_serde::from_slice(&serialized).expect("Failed to deserialize");

        assert_eq!(original.offsets, deserialized.offsets);
        assert_eq!(original.messages_synced, deserialized.messages_synced);
        assert_eq!(original.errors_count, deserialized.errors_count);
    }

    #[test]
    fn serialize_state_helper_should_produce_valid_connector_state() {
        let source = IggySource::new(1, test_config(), None);
        let state = State {
            offsets: HashMap::from([(0, 42)]),
            messages_synced: 42,
            errors_count: 0,
        };

        let connector_state = source.serialize_state(&state);
        assert!(connector_state.is_some());

        let bytes = connector_state.unwrap().0;
        let restored: State = rmp_serde::from_slice(&bytes).expect("Failed to deserialize state");
        assert_eq!(restored.messages_synced, 42);
    }

    #[test]
    fn config_should_deserialize_from_json_with_defaults() {
        let json = r#"{
            "connection_string": "iggy+tcp://iggy:iggy@127.0.0.1:8090",
            "upstream_stream": "stream",
            "upstream_topic": "topic"
        }"#;

        let config: IggySourceConfig = serde_json::from_str(json).expect("Failed to parse config");

        assert_eq!(config.upstream_stream, "stream");
        assert_eq!(config.upstream_topic, "topic");
        assert!(config.poll_interval.is_none());
        assert!(config.initial_offset.is_none());
    }

    #[test]
    fn config_should_apply_defaults_in_new() {
        let json = r#"{
            "connection_string": "iggy+tcp://iggy:iggy@127.0.0.1:8090",
            "upstream_stream": "stream",
            "upstream_topic": "topic"
        }"#;

        let config: IggySourceConfig = serde_json::from_str(json).expect("Failed to parse config");
        let source = IggySource::new(1, config, None);

        assert_eq!(source.poll_interval, Duration::from_secs(2));
        assert_eq!(source.retry_interval, Duration::from_secs(1));
        assert_eq!(source.max_retry_interval, Duration::from_secs(60));
        assert_eq!(source.batch_size, DEFAULT_BATCH_SIZE);
        assert!(source.include_user_headers);
        assert_eq!(source.initial_offset, InitialOffset::Earliest);
        assert!(!source.verbose);
    }

    #[test]
    fn given_invalid_initial_offset_should_default_to_earliest() {
        let config = IggySourceConfig {
            initial_offset: Some("not-an-offset".to_string()),
            ..test_config()
        };

        let source = IggySource::new(1, config, None);
        assert_eq!(source.initial_offset, InitialOffset::Earliest);
    }

    #[test]
    fn initial_offset_should_parse_semantics() {
        assert_eq!(
            "earliest".parse::<InitialOffset>(),
            Ok(InitialOffset::Earliest)
        );
        assert_eq!("Latest".parse::<InitialOffset>(), Ok(InitialOffset::Latest));
        assert_eq!("42".parse::<InitialOffset>(), Ok(InitialOffset::Offset(42)));
        assert!("invalid".parse::<InitialOffset>().is_err());
    }

    #[test]
    fn next_strategy_should_jump_after_saved_offset() {
        assert_eq!(
            next_strategy(InitialOffset::Earliest, Some(41)),
            PollingStrategy::offset(42)
        );
        assert_eq!(
            next_strategy(InitialOffset::Earliest, None),
            PollingStrategy::first()
        );
        assert_eq!(
            next_strategy(InitialOffset::Latest, None),
            PollingStrategy::last()
        );
        assert_eq!(
            next_strategy(InitialOffset::Offset(7), None),
            PollingStrategy::offset(7)
        );
    }

    #[test]
    fn redact_connection_string_should_hide_credentials() {
        let redacted = redact_connection_string("iggy+tcp://iggy:password@127.0.0.1:8090");
        assert_eq!(redacted, "iggy+tcp://iggy:***@127.0.0.1:8090");
        assert!(!redacted.contains("password"));

        let token_redacted = redact_connection_string("iggy+tcp://iggypat-abc123@127.0.0.1:8090");
        assert_eq!(token_redacted, "iggy+tcp://***@127.0.0.1:8090");
        assert!(!token_redacted.contains("abc123"));

        assert_eq!(redact_connection_string("not-a-url"), "***");
    }
}
