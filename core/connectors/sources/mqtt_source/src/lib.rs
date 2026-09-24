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

mod driver;

use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr, time::Duration};
use tokio::{sync::Mutex, time::sleep};
use tracing::{debug, error, info, warn};

use driver::{AckToken, MqttDriver};

source_connector!(MqttSource);

const CONNECTOR_NAME: &str = "MQTT source";
const DEFAULT_KEEP_ALIVE: &str = "30s";
const DEFAULT_POLL_TIMEOUT: &str = "1s";
const DEFAULT_REQUEST_CAPACITY: usize = 32;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_BATCH_TIMEOUT: &str = "10ms";

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MqttProtocol {
    #[serde(rename = "mqtt311")]
    Mqtt311,
    #[serde(rename = "mqtt5")]
    #[default]
    Mqtt5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Qos {
    Zero,
    One,
    Two,
}

impl TryFrom<u8> for Qos {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Zero),
            1 => Ok(Self::One),
            2 => Ok(Self::Two),
            value => Err(Error::InvalidConfigValue(format!(
                "qos {value} is unsupported; expected 0, 1, or 2"
            ))),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct MqttSourceConfig {
    pub broker_url: String,
    pub subscriptions: Vec<String>,
    #[serde(default)]
    pub protocol: MqttProtocol,
    #[serde(default = "default_qos")]
    pub qos: u8,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<SecretString>,
    #[serde(default)]
    pub clean_start: bool,
    pub session_expiry_interval: Option<u32>,
    pub keep_alive: Option<String>,
    pub poll_timeout: Option<String>,
    pub request_capacity: Option<usize>,
    pub batch_size: Option<usize>,
    pub batch_timeout: Option<String>,
    pub verbose_logging: Option<bool>,
}

pub struct MqttSource {
    id: u32,
    config: MqttSourceConfig,
    poll_timeout: Duration,
    batch_size: usize,
    batch_timeout: Duration,
    driver: Mutex<Option<MqttDriver>>,
    state: Mutex<State>,
    pending_batch: Mutex<Option<PendingBatch>>,
}

impl fmt::Debug for MqttSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MqttSource")
            .field("id", &self.id)
            .field("config", &self.config)
            .field("poll_timeout", &self.poll_timeout)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct State {
    acknowledged_messages: u64,
}

#[derive(Debug)]
struct PendingBatch {
    ack_tokens: Vec<AckToken>,
    candidate_state: State,
}

impl MqttSource {
    pub fn new(id: u32, config: MqttSourceConfig, state: Option<ConnectorState>) -> Self {
        let restored_state = state.and_then(|state| {
            state.deserialize::<State>(CONNECTOR_NAME, id).inspect(|state| {
                info!(
                    "Restored MQTT source state for connector with ID {id}. Acknowledged messages: {}",
                    state.acknowledged_messages
                );
            })
        });

        Self {
            id,
            config,
            poll_timeout: Duration::from_secs(1),
            batch_size: DEFAULT_BATCH_SIZE,
            batch_timeout: Duration::from_millis(10),
            driver: Mutex::new(None),
            state: Mutex::new(restored_state.unwrap_or_default()),
            pending_batch: Mutex::new(None),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn validate_config(&self) -> Result<(Qos, Duration, Duration, usize, usize, Duration), Error> {
        if self.config.broker_url.trim().is_empty() {
            return Err(Error::InvalidConfigValue(
                "broker_url must not be empty".to_string(),
            ));
        }
        if self.config.subscriptions.is_empty()
            || self
                .config
                .subscriptions
                .iter()
                .any(|topic| topic.trim().is_empty())
        {
            return Err(Error::InvalidConfigValue(
                "subscriptions must contain at least one non-empty topic".to_string(),
            ));
        }
        if self.config.username.is_some() != self.config.password.is_some() {
            return Err(Error::InvalidConfigValue(
                "username and password must be configured together".to_string(),
            ));
        }

        let qos = Qos::try_from(self.config.qos)?;
        let keep_alive = parse_duration(
            self.config.keep_alive.as_deref(),
            DEFAULT_KEEP_ALIVE,
            "keep_alive",
        )?;
        let poll_timeout = parse_duration(
            self.config.poll_timeout.as_deref(),
            DEFAULT_POLL_TIMEOUT,
            "poll_timeout",
        )?;
        let request_capacity = self
            .config
            .request_capacity
            .unwrap_or(DEFAULT_REQUEST_CAPACITY);
        if request_capacity == 0 {
            return Err(Error::InvalidConfigValue(
                "request_capacity must be greater than zero".to_string(),
            ));
        }

        let batch_size = self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        if batch_size == 0 {
            return Err(Error::InvalidConfigValue(
                "batch_size must be greater than zero".to_string(),
            ));
        }
        let batch_timeout = parse_duration(
            self.config.batch_timeout.as_deref(),
            DEFAULT_BATCH_TIMEOUT,
            "batch_timeout",
        )?;

        let minimum_keep_alive = match self.config.protocol {
            MqttProtocol::Mqtt311 => Duration::from_secs(1),
            MqttProtocol::Mqtt5 => Duration::from_secs(5),
        };
        if !keep_alive.is_zero() && keep_alive < minimum_keep_alive {
            return Err(Error::InvalidConfigValue(format!(
                "keep_alive must be at least {:?} for {:?}",
                minimum_keep_alive, self.config.protocol
            )));
        }

        Ok((
            qos,
            keep_alive,
            poll_timeout,
            request_capacity,
            batch_size,
            batch_timeout,
        ))
    }

    async fn current_state(&self) -> State {
        self.state.lock().await.clone()
    }

    async fn collect_batch(
        &self,
        driver: &mut MqttDriver,
    ) -> Result<Vec<driver::ReceivedMessage>, Error> {
        let Some(first) = driver.next_message(self.poll_timeout).await? else {
            return Ok(Vec::new());
        };

        let mut messages = Vec::with_capacity(self.batch_size);
        messages.push(first);
        let deadline = tokio::time::Instant::now() + self.batch_timeout;
        while messages.len() < self.batch_size {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let Some(message) = driver.next_message(remaining).await? else {
                break;
            };
            messages.push(message);
        }
        Ok(messages)
    }
}

#[async_trait]
impl Source for MqttSource {
    async fn open(&mut self) -> Result<(), Error> {
        let (qos, keep_alive, poll_timeout, request_capacity, batch_size, batch_timeout) =
            self.validate_config()?;
        let driver = match MqttDriver::connect(
            self.id,
            &self.config,
            qos,
            keep_alive,
            poll_timeout,
            request_capacity,
        )
        .await
        {
            Ok(driver) => driver,
            Err(error) => {
                error!(
                    "Failed to connect MQTT source connector with ID {} to broker: {error}",
                    self.id
                );
                return Err(error);
            }
        };
        self.poll_timeout = poll_timeout;
        self.batch_size = batch_size;
        self.batch_timeout = batch_timeout;
        *self.driver.lock().await = Some(driver);
        info!(
            "Opened {CONNECTOR_NAME} connector with ID {} using {:?}, QoS {}, batch_size {}, batch_timeout {:?}, for {} subscription(s)",
            self.id,
            self.config.protocol,
            self.config.qos,
            self.batch_size,
            self.batch_timeout,
            self.config.subscriptions.len()
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        sleep(Duration::from_millis(10)).await;
        if self.pending_batch.lock().await.is_some() {
            return Err(Error::InvalidState);
        }

        let mut driver = self
            .driver
            .lock()
            .await
            .take()
            .ok_or_else(|| Error::InitError("MQTT driver is not initialized".to_string()))?;
        let received = self.collect_batch(&mut driver).await;
        *self.driver.lock().await = Some(driver);
        let received = received?;
        if received.is_empty() {
            return Ok(empty_messages());
        }

        let state = self.current_state().await;
        let candidate_state = State {
            acknowledged_messages: state
                .acknowledged_messages
                .saturating_add(received.len() as u64),
        };
        let persisted_state = self.serialize_state(&candidate_state).ok_or_else(|| {
            Error::Serialization("failed to serialize MQTT source state".to_string())
        })?;
        let mut messages = Vec::with_capacity(received.len());
        let mut ack_tokens = Vec::with_capacity(received.len());
        for received in received {
            if self.config.verbose_logging.unwrap_or(false) {
                debug!(
                    "Received MQTT message for {CONNECTOR_NAME} connector with ID {}: topic={}, qos={:?}, packet_id={:?}, retain={}",
                    self.id,
                    received.message.topic,
                    received.message.metadata.qos,
                    received.message.metadata.packet_id,
                    received.message.metadata.retain
                );
            }
            if let Some(ack_token) = received.ack_token {
                ack_tokens.push(ack_token);
            }
            messages.push(ProducedMessage {
                id: None,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                headers: Some(received.message.headers),
                payload: received.message.payload,
            });
        }
        *self.pending_batch.lock().await = Some(PendingBatch {
            ack_tokens,
            candidate_state,
        });

        Ok(ProducedMessages {
            schema: Schema::Raw,
            messages,
            state: Some(persisted_state),
        })
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let Some(mut pending_batch) = self.pending_batch.lock().await.take() else {
            return Ok(());
        };
        if result == SourceBatchResult::Nack {
            warn!(
                "NACK received for MQTT source connector with ID {}; broker redelivery depends on the configured QoS",
                self.id
            );
            return Ok(());
        }

        if pending_batch.ack_tokens.is_empty() {
            *self.state.lock().await = pending_batch.candidate_state;
            return Ok(());
        }

        let Some(mut driver) = self.driver.lock().await.take() else {
            *self.pending_batch.lock().await = Some(pending_batch);
            return Err(Error::InitError(
                "MQTT driver is not initialized".to_string(),
            ));
        };
        let acknowledgement = driver
            .acknowledge_batch(
                &mut pending_batch.ack_tokens,
                self.poll_timeout,
                self.batch_size,
            )
            .await;
        *self.driver.lock().await = Some(driver);
        if let Err(error) = acknowledgement {
            *self.pending_batch.lock().await = Some(pending_batch);
            return Err(error);
        }
        *self.state.lock().await = pending_batch.candidate_state;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.pending_batch.lock().await.take();
        self.driver.lock().await.take();
        info!("Closed {CONNECTOR_NAME} connector with ID {}", self.id);
        Ok(())
    }
}

fn default_qos() -> u8 {
    1
}

fn empty_messages() -> ProducedMessages {
    ProducedMessages {
        schema: Schema::Raw,
        messages: Vec::new(),
        state: None,
    }
}

fn parse_duration(value: Option<&str>, default: &str, field: &str) -> Result<Duration, Error> {
    let value = value.unwrap_or(default);
    HumanDuration::from_str(value)
        .map(|duration| *duration)
        .map_err(|error| Error::InvalidConfigValue(format!("{field}: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MqttSourceConfig {
        MqttSourceConfig {
            broker_url: "mqtt://localhost:1883".to_string(),
            subscriptions: vec!["devices/+/telemetry".to_string()],
            protocol: MqttProtocol::Mqtt5,
            qos: 1,
            client_id: Some("test-source".to_string()),
            username: None,
            password: None,
            clean_start: false,
            session_expiry_interval: Some(3600),
            keep_alive: Some("30s".to_string()),
            poll_timeout: Some("100ms".to_string()),
            request_capacity: Some(4),
            batch_size: Some(3),
            batch_timeout: Some("10ms".to_string()),
            verbose_logging: None,
        }
    }

    #[test]
    fn given_persisted_state_should_restore_acknowledged_messages() {
        let state = State {
            acknowledged_messages: 42,
        };
        let persisted = ConnectorState::serialize(&state, CONNECTOR_NAME, 7);
        let source = MqttSource::new(7, test_config(), persisted);

        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let restored = source.current_state().await;
            assert_eq!(restored.acknowledged_messages, 42);
        });
    }

    #[test]
    fn given_no_state_should_start_fresh() {
        let source = MqttSource::new(7, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let state = source.current_state().await;
            assert_eq!(state.acknowledged_messages, 0);
        });
    }

    #[test]
    fn given_invalid_state_should_start_fresh() {
        let source = MqttSource::new(
            7,
            test_config(),
            Some(ConnectorState(b"not valid msgpack".to_vec())),
        );
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let state = source.current_state().await;
            assert_eq!(state.acknowledged_messages, 0);
        });
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let original = State {
            acknowledged_messages: 17,
        };
        let persisted = ConnectorState::serialize(&original, CONNECTOR_NAME, 7)
            .expect("state should serialize");
        let restored = persisted
            .deserialize::<State>(CONNECTOR_NAME, 7)
            .expect("state should deserialize");

        assert_eq!(restored.acknowledged_messages, 17);
    }

    #[test]
    fn given_ack_without_mqtt_token_should_commit_candidate_state() {
        let source = MqttSource::new(7, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            *source.pending_batch.lock().await = Some(PendingBatch {
                ack_tokens: Vec::new(),
                candidate_state: State {
                    acknowledged_messages: 1,
                },
            });

            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("ack should commit state");
            assert_eq!(source.current_state().await.acknowledged_messages, 1);
        });
    }

    #[test]
    fn given_nack_should_not_commit_candidate_state() {
        let source = MqttSource::new(7, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let publish = rumqttc::v5::mqttbytes::v5::Publish::new(
                "devices/test",
                rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
                b"payload".as_slice(),
                None,
            );
            let ack_token = driver::normalize_mqtt5(publish)
                .expect("MQTT publish should normalize")
                .ack_token
                .expect("QoS 1 publish should have an acknowledgement token");
            *source.pending_batch.lock().await = Some(PendingBatch {
                ack_tokens: vec![ack_token],
                candidate_state: State {
                    acknowledged_messages: 1,
                },
            });

            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("nack should be handled");
            assert_eq!(source.current_state().await.acknowledged_messages, 0);
            assert!(source.pending_batch.lock().await.is_none());
        });
    }

    #[test]
    fn given_each_supported_qos_should_pass_configuration_validation() {
        for qos in 0..=2 {
            let mut config = test_config();
            config.qos = qos;
            let source = MqttSource::new(7, config, None);

            assert!(source.validate_config().is_ok());
        }
    }

    #[test]
    fn given_zero_batch_size_should_reject_configuration() {
        let mut config = test_config();
        config.batch_size = Some(0);
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_invalid_batch_timeout_should_reject_configuration() {
        let mut config = test_config();
        config.batch_timeout = Some("not a duration".to_string());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_missing_batch_configuration_should_use_bounded_defaults() {
        let mut config = test_config();
        config.batch_size = None;
        config.batch_timeout = None;
        let source = MqttSource::new(7, config, None);

        let (_, _, _, _, batch_size, batch_timeout) = source
            .validate_config()
            .expect("default batch config should be valid");
        assert_eq!(batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(batch_timeout, Duration::from_millis(10));
    }

    #[test]
    fn given_unsupported_qos_should_reject_configuration() {
        let mut config = test_config();
        config.qos = 3;
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_username_without_password_should_reject_configuration() {
        let mut config = test_config();
        config.username = Some("user".to_string());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_password_without_username_should_reject_configuration() {
        let mut config = test_config();
        config.password = Some(SecretString::new("password".to_string().into()));
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_empty_broker_url_should_reject_configuration() {
        let mut config = test_config();
        config.broker_url.clear();
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_empty_subscriptions_should_reject_configuration() {
        let mut config = test_config();
        config.subscriptions.clear();
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_empty_topic_filter_should_reject_configuration() {
        let mut config = test_config();
        config.subscriptions = vec![String::new()];
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_zero_request_capacity_should_reject_configuration() {
        let mut config = test_config();
        config.request_capacity = Some(0);
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_invalid_keep_alive_duration_should_reject_configuration() {
        let mut config = test_config();
        config.keep_alive = Some("not a duration".to_string());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_invalid_poll_timeout_duration_should_reject_configuration() {
        let mut config = test_config();
        config.poll_timeout = Some("not a duration".to_string());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_mqtt311_subsecond_keep_alive_should_reject_configuration() {
        let mut config = test_config();
        config.protocol = MqttProtocol::Mqtt311;
        config.keep_alive = Some("500ms".to_string());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_mqtt311_should_accept_its_keep_alive_requirement() {
        let mut config = test_config();
        config.protocol = MqttProtocol::Mqtt311;
        config.keep_alive = Some("1s".to_string());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_ok());
    }

    #[test]
    fn given_mqtt5_should_reject_a_subsecond_keep_alive() {
        let mut config = test_config();
        config.keep_alive = Some("1s".to_string());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }
}
