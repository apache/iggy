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
use base64::Engine;
use humantime::Duration as HumanDuration;
use iggy_common::{HeaderKey, HeaderValue};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    str::FromStr,
    time::Duration,
};
use tokio::{sync::Mutex, time::sleep};
use tracing::{debug, error, info, warn};
use url::Url;

use driver::{AckToken, MqttDriver};
use driver::{Mqtt5EnvelopeProperties, MqttMessage};

source_connector!(MqttSource);

const CONNECTOR_NAME: &str = "MQTT source";
const DEFAULT_KEEP_ALIVE: &str = "30s";
const DEFAULT_POLL_TIMEOUT: &str = "1s";
const DEFAULT_REQUEST_CAPACITY: usize = 32;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_BATCH_TIMEOUT: &str = "10ms";
const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_SESSION_EXPIRY_INTERVAL: u32 = 3600;

/// MQTT wire protocol selected for the broker connection.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MqttProtocol {
    #[serde(rename = "mqtt311")]
    Mqtt311,
    #[serde(rename = "mqtt5")]
    #[default]
    Mqtt5,
}

/// Subscription delivery level requested from the broker.
///
/// QoS 0 has no acknowledgement token. QoS 1 and QoS 2 produce a token that
/// remains pending until the corresponding Iggy batch is acknowledged.
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
    /// MQTT broker URL, using `mqtt://`, `mqtts://`, or `ssl://`.
    pub broker_url: String,
    /// Topic filters subscribed to by this connector instance.
    pub subscriptions: Vec<String>,
    /// Exact topic-filter QoS overrides applied on top of `qos`.
    #[serde(default)]
    pub subscription_qos: BTreeMap<String, u8>,
    /// MQTT protocol version used for the connection.
    #[serde(default)]
    pub protocol: MqttProtocol,
    /// Preserves MQTT 5 extended properties in a JSON payload envelope.
    #[serde(default)]
    pub include_metadata: bool,
    /// Default subscription QoS when no per-filter override exists.
    #[serde(default = "default_qos")]
    pub qos: u8,
    /// Optional CA and client-authentication material for TLS connections.
    #[serde(default)]
    pub tls: Option<MqttTlsConfig>,
    /// Explicit broker client ID. A connector-specific ID is generated when absent.
    pub client_id: Option<String>,
    /// Broker username, which must be paired with `password`.
    pub username: Option<String>,
    /// Broker password, kept secret in memory and never included in debug output.
    pub password: Option<SecretString>,
    /// Whether the broker should discard the previous session on connect.
    #[serde(default)]
    pub clean_start: bool,
    /// MQTT 5 session expiry interval in seconds.
    #[serde(default = "default_session_expiry_interval")]
    pub session_expiry_interval: Option<u32>,
    /// MQTT keep-alive interval.
    pub keep_alive: Option<String>,
    /// Maximum time to wait for a message during batch collection.
    pub poll_timeout: Option<String>,
    /// Capacity of rumqttc's request channel.
    pub request_capacity: Option<usize>,
    /// Maximum number of messages in one Iggy source batch.
    pub batch_size: Option<usize>,
    /// Maximum time to wait after the first message before flushing a batch.
    pub batch_timeout: Option<String>,
    /// Number of retries for a broker acknowledgement after the initial attempt.
    /// After retries are exhausted, the source abandons the broker acknowledgement
    /// and continues. The broker may redeliver the message later, producing duplicates.
    #[serde(default)]
    pub max_retries: Option<u32>,
    /// Enables per-message MQTT logging for troubleshooting.
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct MqttTlsConfig {
    /// Optional custom CA bundle. System roots are used when this is absent.
    pub ca_file: Option<String>,
    /// Client certificate for mutual TLS, paired with `client_key_file`.
    pub client_cert_file: Option<String>,
    /// Client private key for mutual TLS, paired with `client_cert_file`.
    pub client_key_file: Option<String>,
}

pub struct MqttSource {
    id: u32,
    config: MqttSourceConfig,
    poll_timeout: Duration,
    batch_size: usize,
    batch_timeout: Duration,
    // The driver is moved out briefly during event-loop I/O to avoid holding a
    // mutex guard across await points.
    driver: Mutex<Option<MqttDriver>>,
    // At most one batch is staged because the SDK reports one batch result at a
    // time. It holds the MQTT acknowledgement tokens for that batch.
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

#[derive(Debug)]
struct PendingBatch {
    replay_messages: Vec<ReplayMessage>,
    schema: Schema,
    // Tokens are kept separately because the runtime acknowledges Iggy before
    // the driver acknowledges MQTT.
    ack_tokens: Vec<AckToken>,
    // The SDK permits one batch in flight. A NACK leaves this false so poll()
    // can replay the batch before reading newer MQTT messages.
    in_flight: bool,
}

#[derive(Debug, Clone)]
struct ReplayMessage {
    headers: Option<BTreeMap<HeaderKey, HeaderValue>>,
    payload: Vec<u8>,
}

impl PendingBatch {
    fn replay(&self) -> ProducedMessages {
        ProducedMessages {
            schema: self.schema,
            messages: self
                .replay_messages
                .iter()
                .cloned()
                .map(|message| ProducedMessage {
                    id: None,
                    checksum: None,
                    timestamp: None,
                    origin_timestamp: None,
                    headers: message.headers,
                    payload: message.payload,
                })
                .collect(),
            state: None,
        }
    }
}

impl MqttSource {
    pub fn new(id: u32, config: MqttSourceConfig, _state: Option<ConnectorState>) -> Self {
        Self {
            id,
            config,
            poll_timeout: Duration::from_secs(1),
            batch_size: DEFAULT_BATCH_SIZE,
            batch_timeout: Duration::from_millis(10),
            driver: Mutex::new(None),
            pending_batch: Mutex::new(None),
        }
    }

    fn validate_config(&self) -> Result<(Qos, Duration, Duration, usize, usize, Duration), Error> {
        // Validate static values before opening the network connection so an
        // operator sees configuration errors during initialization.
        if self.config.broker_url.trim().is_empty() {
            return Err(Error::InvalidConfigValue(
                "broker_url must not be empty".to_string(),
            ));
        }
        if self.config.include_metadata && self.config.protocol == MqttProtocol::Mqtt311 {
            return Err(Error::InvalidConfigValue(
                "include_metadata is only supported with protocol = \"mqtt5\"".to_string(),
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
        let mut unique_subscriptions = HashSet::with_capacity(self.config.subscriptions.len());
        for subscription in &self.config.subscriptions {
            if !unique_subscriptions.insert(subscription) {
                return Err(Error::InvalidConfigValue(format!(
                    "subscriptions contains duplicate topic filter: {subscription}"
                )));
            }
        }
        // An override is keyed by the exact configured filter. An unknown key
        // would otherwise be accepted but never used.
        for subscription in self.config.subscription_qos.keys() {
            if !self
                .config
                .subscriptions
                .iter()
                .any(|topic| topic == subscription)
            {
                return Err(Error::InvalidConfigValue(format!(
                    "subscription_qos contains unknown topic filter: {subscription}"
                )));
            }
        }
        if self.config.username.is_some() != self.config.password.is_some() {
            return Err(Error::InvalidConfigValue(
                "username and password must be configured together".to_string(),
            ));
        }
        // Validate URL options before rumqttc parses them because its setters
        // assert on some invalid values instead of returning an error.
        validate_broker_url_options(&self.config)?;
        // Validate TLS scheme and client-auth field pairing before opening the
        // broker connection. Certificate parsing happens in the driver.
        validate_tls_config(&self.config)?;

        let qos = Qos::try_from(self.config.qos)?;
        for subscription in &self.config.subscriptions {
            qos_for_subscription(&self.config, subscription, qos)?;
        }
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
        if poll_timeout.is_zero() {
            return Err(Error::InvalidConfigValue(
                "poll_timeout must be greater than zero".to_string(),
            ));
        }
        let request_capacity = self
            .config
            .request_capacity
            .unwrap_or(DEFAULT_REQUEST_CAPACITY);
        if request_capacity == 0 {
            return Err(Error::InvalidConfigValue(
                "request_capacity must be greater than zero".to_string(),
            ));
        }
        if request_capacity < self.config.subscriptions.len() {
            return Err(Error::InvalidConfigValue(format!(
                "request_capacity must be at least the number of subscriptions ({})",
                self.config.subscriptions.len()
            )));
        }

        // These bounds protect both the plugin-owned batch and the MQTT request
        // channel from configurations that would otherwise never make progress.
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

        validate_keep_alive(self.config.protocol, keep_alive, "keep_alive")?;
        if !self.config.clean_start && !has_stable_configured_client_id(&self.config) {
            return Err(Error::InvalidConfigValue(
                "client_id must be configured and non-empty when clean_start is false".to_string(),
            ));
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

    fn max_retries(&self) -> u32 {
        self.config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
    }

    async fn collect_batch(
        &self,
        driver: &mut MqttDriver,
    ) -> Result<Vec<driver::ReceivedMessage>, Error> {
        // Start the batch timeout only after the first message arrives. A quiet
        // connector therefore remains cheap while partial batches still flush.
        let Some(first) = driver.next_message(self.poll_timeout).await? else {
            return Ok(Vec::new());
        };

        let mut messages = Vec::new();
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

pub(crate) fn qos_for_subscription(
    config: &MqttSourceConfig,
    subscription: &str,
    default_qos: Qos,
) -> Result<Qos, Error> {
    // The global QoS is the fallback; an exact filter override wins when present.
    match config.subscription_qos.get(subscription).copied() {
        Some(value) => Qos::try_from(value),
        None => Ok(default_qos),
    }
}

#[async_trait]
impl Source for MqttSource {
    async fn open(&mut self) -> Result<(), Error> {
        // Opening validates configuration and creates the broker session. Polling
        // assumes this driver is ready.
        let (qos, keep_alive, poll_timeout, request_capacity, batch_size, batch_timeout) =
            match self.validate_config() {
                Ok(values) => values,
                Err(error) => {
                    error!(
                        "Failed to validate MQTT source connector with ID {}: {error}",
                        self.id
                    );
                    return Err(error);
                }
            };
        let driver = match MqttDriver::connect(
            self.id,
            &self.config,
            qos,
            keep_alive,
            poll_timeout,
            request_capacity,
            batch_size,
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
        // The SDK permits one source batch in flight. A second poll would lose
        // the association between messages, candidate state, and ACK tokens.
        {
            let mut pending_batch = self.pending_batch.lock().await;
            if let Some(pending_batch) = pending_batch.as_mut() {
                if pending_batch.in_flight {
                    return Err(Error::InvalidState);
                }
                pending_batch.in_flight = true;
                return Ok(pending_batch.replay());
            }
        }

        let mut driver = self
            .driver
            .lock()
            .await
            .take()
            .ok_or_else(|| Error::InitError("MQTT driver is not initialized".to_string()))?;
        if let Err(error) = driver
            .flush_deferred_acknowledgements(self.poll_timeout, self.max_retries())
            .await
        {
            *self.driver.lock().await = Some(driver);
            debug!(
                "Deferred MQTT acknowledgements remain queued for connector with ID {}; retrying before reading new messages: {error}",
                self.id
            );
            return Ok(empty_messages());
        }
        // Move the driver out while polling so network I/O does not occur while
        // holding the source mutex. Restore it before returning.
        let received = self.collect_batch(&mut driver).await;
        *self.driver.lock().await = Some(driver);
        let received = received?;
        if received.is_empty() {
            return Ok(empty_messages());
        }

        let mut replay_messages = Vec::with_capacity(received.len());
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
            let payload = if self.config.include_metadata {
                serialize_mqtt5_envelope(&received.message)?
            } else {
                received.message.payload
            };
            replay_messages.push(ReplayMessage {
                headers: Some(received.message.headers.clone()),
                payload: payload.clone(),
            });
        }
        let schema = if self.config.include_metadata {
            Schema::Json
        } else {
            Schema::Raw
        };
        let pending_batch = PendingBatch {
            replay_messages,
            schema,
            ack_tokens,
            in_flight: true,
        };
        let produced_messages = pending_batch.replay();
        *self.pending_batch.lock().await = Some(pending_batch);

        Ok(produced_messages)
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let mut pending_batches = self.pending_batch.lock().await;
        let Some(pending_batch) = pending_batches.as_mut() else {
            return Ok(());
        };
        if result == SourceBatchResult::Nack {
            pending_batch.in_flight = false;
            warn!(
                "NACK received for MQTT source connector with ID {}; replaying the batch before reading new messages",
                self.id
            );
            return Ok(());
        }

        let Some(mut pending_batch) = pending_batches.take() else {
            return Ok(());
        };
        drop(pending_batches);

        if pending_batch.ack_tokens.is_empty() {
            // QoS 0 has no broker token, so there is no broker acknowledgement
            // to perform after Iggy accepts the batch.
            return Ok(());
        }

        let Some(mut driver) = self.driver.lock().await.take() else {
            *self.pending_batch.lock().await = Some(pending_batch);
            return Err(Error::InitError(
                "MQTT driver is not initialized".to_string(),
            ));
        };
        // MQTT acknowledgements are sent only after Iggy acknowledges the
        // batch, preserving at-least-once delivery. If rumqttc's request queue
        // is full, the driver keeps the remaining tokens for a later poll.
        driver.defer_acknowledgements(&mut pending_batch.ack_tokens);
        let acknowledgement = driver
            .flush_deferred_acknowledgements(self.poll_timeout, self.max_retries())
            .await;
        *self.driver.lock().await = Some(driver);
        if let Err(error) = acknowledgement {
            warn!(
                "MQTT acknowledgement retries exhausted for connector with ID {}; retaining the broker acknowledgement for a later poll: {error}",
                self.id
            );
            return Ok(());
        }
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

fn default_session_expiry_interval() -> Option<u32> {
    Some(DEFAULT_SESSION_EXPIRY_INTERVAL)
}

#[derive(Debug, Serialize)]
struct MqttEnvelope<'a> {
    version: u8,
    topic: &'a str,
    properties: Option<&'a Mqtt5EnvelopeProperties>,
    payload_base64: String,
}

fn serialize_mqtt5_envelope(message: &MqttMessage) -> Result<Vec<u8>, Error> {
    let envelope = MqttEnvelope {
        version: 1,
        topic: &message.topic,
        properties: message.mqtt5_properties.as_ref(),
        payload_base64: base64::engine::general_purpose::STANDARD.encode(&message.payload),
    };
    serde_json::to_vec(&envelope).map_err(|error| {
        Error::Serialization(format!("failed to serialize MQTT envelope: {error}"))
    })
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

fn validate_broker_url_options(config: &MqttSourceConfig) -> Result<(), Error> {
    let broker_url = Url::parse(&config.broker_url)
        .map_err(|error| Error::InvalidConfigValue(format!("broker_url: {error}")))?;
    if !broker_url.username().is_empty() || broker_url.password().is_some() {
        return Err(Error::InvalidConfigValue(
            "broker_url must not contain credentials; configure username and password separately"
                .to_string(),
        ));
    }
    let keep_alive_secs = broker_url
        .query_pairs()
        .filter(|(key, _)| key == "keep_alive_secs")
        .map(|(_, value)| value.into_owned())
        .last();
    if let Some(keep_alive_secs) = keep_alive_secs {
        let keep_alive_secs = keep_alive_secs.parse::<u64>().map_err(|_| {
            Error::InvalidConfigValue(
                "broker_url keep_alive_secs must be an unsigned integer".to_string(),
            )
        })?;
        validate_keep_alive(
            config.protocol,
            Duration::from_secs(keep_alive_secs),
            "broker_url keep_alive_secs",
        )?;
    }

    if let Some(inflight_num) = broker_url
        .query_pairs()
        .filter(|(key, _)| key == "inflight_num")
        .map(|(_, value)| value.into_owned())
        .last()
    {
        let inflight_num = inflight_num.parse::<u16>().map_err(|_| {
            Error::InvalidConfigValue(
                "broker_url inflight_num must be an unsigned 16-bit integer".to_string(),
            )
        })?;
        if inflight_num == 0 {
            return Err(Error::InvalidConfigValue(
                "broker_url inflight_num must be greater than zero".to_string(),
            ));
        }
    }

    if config.protocol == MqttProtocol::Mqtt311 && has_empty_configured_client_id(config) {
        let clean_session = broker_url
            .query_pairs()
            .filter(|(key, _)| key == "clean_session")
            .map(|(_, value)| value.into_owned())
            .last();
        if matches!(clean_session.as_deref(), Some("false")) {
            return Err(Error::InvalidConfigValue(
                "broker_url clean_session=false requires a non-empty client_id".to_string(),
            ));
        }
    }

    Ok(())
}

fn validate_keep_alive(
    protocol: MqttProtocol,
    keep_alive: Duration,
    field: &str,
) -> Result<(), Error> {
    let minimum_keep_alive = match protocol {
        MqttProtocol::Mqtt311 => Duration::from_secs(1),
        MqttProtocol::Mqtt5 => Duration::from_secs(5),
    };
    let invalid = match protocol {
        MqttProtocol::Mqtt311 => !keep_alive.is_zero() && keep_alive < minimum_keep_alive,
        MqttProtocol::Mqtt5 => keep_alive < minimum_keep_alive,
    };
    if invalid {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must be at least {:?} for {:?}",
            minimum_keep_alive, protocol
        )));
    }
    Ok(())
}

fn has_stable_configured_client_id(config: &MqttSourceConfig) -> bool {
    config
        .client_id
        .as_deref()
        .is_some_and(|client_id| !client_id.trim().is_empty())
}

fn has_empty_configured_client_id(config: &MqttSourceConfig) -> bool {
    config
        .client_id
        .as_deref()
        .is_some_and(|client_id| client_id.trim().is_empty())
}

fn validate_tls_config(config: &MqttSourceConfig) -> Result<(), Error> {
    // `tls` is meaningful only for a secure broker URL. The transport itself is
    // built later by the driver after these consistency checks pass.
    let Some(tls) = &config.tls else {
        return Ok(());
    };

    let broker_url = Url::parse(&config.broker_url)
        .map_err(|error| Error::InvalidConfigValue(format!("broker_url: {error}")))?;
    if !matches!(broker_url.scheme(), "mqtts" | "ssl") {
        return Err(Error::InvalidConfigValue(
            "tls requires an mqtts:// or ssl:// broker_url".to_string(),
        ));
    }
    if tls.client_cert_file.is_some() != tls.client_key_file.is_some() {
        return Err(Error::InvalidConfigValue(
            "tls client_cert_file and client_key_file must be configured together".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MqttSourceConfig {
        MqttSourceConfig {
            broker_url: "mqtt://localhost:1883".to_string(),
            subscriptions: vec!["devices/+/telemetry".to_string()],
            subscription_qos: BTreeMap::new(),
            protocol: MqttProtocol::Mqtt5,
            include_metadata: false,
            qos: 1,
            tls: None,
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
            max_retries: None,
            verbose_logging: None,
        }
    }

    #[test]
    fn given_no_max_retries_should_use_default() {
        let source = MqttSource::new(7, test_config(), None);

        assert_eq!(source.max_retries(), DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn given_missing_session_expiry_interval_should_use_nonzero_default() {
        let config: MqttSourceConfig = serde_json::from_str(
            r#"{
                "broker_url": "mqtt://localhost:1883",
                "subscriptions": ["devices/test"]
            }"#,
        )
        .expect("config should deserialize");

        assert_eq!(
            config.session_expiry_interval,
            Some(DEFAULT_SESSION_EXPIRY_INTERVAL)
        );
    }

    #[test]
    fn given_explicit_zero_session_expiry_interval_should_preserve_it() {
        let config: MqttSourceConfig = serde_json::from_str(
            r#"{
                "broker_url": "mqtt://localhost:1883",
                "subscriptions": ["devices/test"],
                "session_expiry_interval": 0
            }"#,
        )
        .expect("config should deserialize");

        assert_eq!(config.session_expiry_interval, Some(0));
    }

    #[test]
    fn given_configured_max_retries_should_use_configured_value() {
        let mut config = test_config();
        config.max_retries = Some(2);
        let source = MqttSource::new(7, config, None);

        assert_eq!(source.max_retries(), 2);
    }

    #[test]
    fn given_nack_should_replay_pending_mqtt_batch() {
        let source = MqttSource::new(7, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let publish = rumqttc::v5::mqttbytes::v5::Publish::new(
                "devices/test",
                rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
                b"payload".as_slice(),
                None,
            );
            let ack_token = driver::normalize_mqtt5(publish, true)
                .expect("MQTT publish should normalize")
                .ack_token
                .expect("QoS 1 publish should have an acknowledgement token");
            *source.pending_batch.lock().await = Some(PendingBatch {
                replay_messages: vec![ReplayMessage {
                    headers: None,
                    payload: b"payload".to_vec(),
                }],
                schema: Schema::Raw,
                ack_tokens: vec![ack_token],
                in_flight: true,
            });

            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("nack should be handled");
            assert!(
                !source
                    .pending_batch
                    .lock()
                    .await
                    .as_ref()
                    .expect("NACKed batch should be retained")
                    .in_flight
            );

            let replayed = source.poll().await.expect("replay poll should succeed");
            assert_eq!(replayed.schema, Schema::Raw);
            assert_eq!(replayed.messages.len(), 1);
            assert_eq!(replayed.messages[0].payload, b"payload");
            assert!(replayed.state.is_none());
        });
    }

    #[test]
    fn given_repeated_nack_should_replay_same_batch_before_new_messages() {
        let source = MqttSource::new(7, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            *source.pending_batch.lock().await = Some(PendingBatch {
                replay_messages: vec![ReplayMessage {
                    headers: None,
                    payload: b"payload".to_vec(),
                }],
                schema: Schema::Raw,
                ack_tokens: Vec::new(),
                in_flight: false,
            });

            let first_replay = source.poll().await.expect("first replay should succeed");
            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("first replay NACK should be handled");
            let second_replay = source.poll().await.expect("second replay should succeed");

            assert_eq!(first_replay.messages.len(), second_replay.messages.len());
            assert_eq!(
                first_replay.messages[0].payload,
                second_replay.messages[0].payload
            );
            assert!(source.pending_batch.lock().await.is_some());
        });
    }

    #[test]
    fn given_replayed_qos_zero_batch_when_acked_should_clear_pending_batch() {
        let source = MqttSource::new(7, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            *source.pending_batch.lock().await = Some(PendingBatch {
                replay_messages: vec![ReplayMessage {
                    headers: None,
                    payload: b"payload".to_vec(),
                }],
                schema: Schema::Raw,
                ack_tokens: Vec::new(),
                in_flight: false,
            });

            source.poll().await.expect("replay should succeed");
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("ACK should clear replayed batch");
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
    fn given_mqtt311_metadata_enabled_should_reject_configuration() {
        let mut config = test_config();
        config.protocol = MqttProtocol::Mqtt311;
        config.include_metadata = true;
        let source = MqttSource::new(7, config, None);

        let result = source.validate_config();

        assert!(matches!(
            result,
            Err(Error::InvalidConfigValue(message))
                if message == "include_metadata is only supported with protocol = \"mqtt5\""
        ));
    }

    #[test]
    fn given_mqtt5_metadata_enabled_should_serialize_payload_and_properties() {
        let properties = rumqttc::v5::mqttbytes::v5::PublishProperties {
            correlation_data: Some(vec![0, 255].into()),
            ..Default::default()
        };
        let publish = rumqttc::v5::mqttbytes::v5::Publish::new(
            "devices/test",
            rumqttc::v5::mqttbytes::QoS::AtMostOnce,
            vec![0, 255],
            Some(properties),
        );
        let message = driver::normalize_mqtt5(publish, true)
            .expect("MQTT 5 publish should normalize")
            .message;

        let serialized = serialize_mqtt5_envelope(&message).expect("envelope should serialize");
        let envelope: serde_json::Value =
            serde_json::from_slice(&serialized).expect("envelope should be valid JSON");

        assert_eq!(envelope["version"], 1);
        assert_eq!(envelope["topic"], "devices/test");
        assert_eq!(envelope["payload_base64"], "AP8=");
        assert_eq!(envelope["properties"]["correlation_data_base64"], "AP8=");
        assert_eq!(message.headers.len(), 5);
    }

    #[test]
    fn given_oversized_topic_should_be_preserved_by_mqtt5_envelope() {
        let topic = format!("devices/{}/telemetry", "x".repeat(280));
        let publish = rumqttc::v5::mqttbytes::v5::Publish::new(
            topic.clone(),
            rumqttc::v5::mqttbytes::QoS::AtMostOnce,
            b"payload".to_vec(),
            None,
        );
        let message = driver::normalize_mqtt5(publish, true)
            .expect("MQTT 5 publish should normalize")
            .message;

        let serialized = serialize_mqtt5_envelope(&message).expect("envelope should serialize");
        let envelope: serde_json::Value =
            serde_json::from_slice(&serialized).expect("envelope should be valid JSON");

        assert_eq!(envelope["topic"], topic);
        assert_eq!(envelope["payload_base64"], "cGF5bG9hZA==");
        assert!(
            !message
                .headers
                .contains_key(&"mqtt.topic".try_into().unwrap())
        );
    }

    #[test]
    fn given_unknown_subscription_qos_override_should_reject_configuration() {
        let mut config = test_config();
        config
            .subscription_qos
            .insert("devices/+/status".to_string(), 2);
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_invalid_subscription_qos_override_should_reject_configuration() {
        let mut config = test_config();
        config
            .subscription_qos
            .insert("devices/+/telemetry".to_string(), 3);
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_duplicate_subscription_filters_should_reject_configuration() {
        let mut config = test_config();
        config.subscriptions.push("devices/+/telemetry".to_string());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_subscription_qos_override_should_use_default_for_other_filters() {
        let mut config = test_config();
        config.subscriptions.push("devices/+/status".to_string());
        config
            .subscription_qos
            .insert("devices/+/telemetry".to_string(), 2);
        let source = MqttSource::new(7, config, None);
        let (default_qos, _, _, _, _, _) = source
            .validate_config()
            .expect("subscription QoS configuration should be valid");

        assert_eq!(
            qos_for_subscription(&source.config, "devices/+/telemetry", default_qos)
                .expect("override should be valid"),
            Qos::Two
        );
        assert_eq!(
            qos_for_subscription(&source.config, "devices/+/status", default_qos)
                .expect("default QoS should be valid"),
            Qos::One
        );
    }

    #[test]
    fn given_mqtt311_subscription_qos_should_resolve_each_filter() {
        let mut config = test_config();
        config.protocol = MqttProtocol::Mqtt311;
        config.subscriptions.push("devices/+/status".to_string());
        config
            .subscription_qos
            .insert("devices/+/status".to_string(), 0);
        let source = MqttSource::new(7, config, None);
        let (default_qos, _, _, _, _, _) = source
            .validate_config()
            .expect("MQTT 3.1.1 subscription configuration should be valid");

        assert_eq!(
            qos_for_subscription(&source.config, "devices/+/telemetry", default_qos)
                .expect("default QoS should be valid"),
            Qos::One
        );
        assert_eq!(
            qos_for_subscription(&source.config, "devices/+/status", default_qos)
                .expect("override should be valid"),
            Qos::Zero
        );
    }

    #[test]
    fn given_mqtt5_subscription_qos_should_resolve_each_filter() {
        let mut config = test_config();
        config.subscriptions.push("devices/+/status".to_string());
        config
            .subscription_qos
            .insert("devices/+/status".to_string(), 2);
        let source = MqttSource::new(7, config, None);
        let (default_qos, _, _, _, _, _) = source
            .validate_config()
            .expect("MQTT 5 subscription configuration should be valid");

        assert_eq!(
            qos_for_subscription(&source.config, "devices/+/telemetry", default_qos)
                .expect("default QoS should be valid"),
            Qos::One
        );
        assert_eq!(
            qos_for_subscription(&source.config, "devices/+/status", default_qos)
                .expect("override should be valid"),
            Qos::Two
        );
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
    fn given_tls_client_certificate_without_key_should_reject_configuration() {
        let mut config = test_config();
        config.broker_url = "mqtts://localhost:8883".to_string();
        config.tls = Some(MqttTlsConfig {
            ca_file: None,
            client_cert_file: Some("client.pem".to_string()),
            client_key_file: None,
        });
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_tls_with_plaintext_broker_should_reject_configuration() {
        let mut config = test_config();
        config.tls = Some(MqttTlsConfig {
            ca_file: None,
            client_cert_file: None,
            client_key_file: None,
        });
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
    fn given_zero_poll_timeout_should_reject_configuration() {
        let mut config = test_config();
        config.poll_timeout = Some("0s".to_string());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_request_capacity_smaller_than_subscription_count_should_reject_configuration() {
        let mut config = test_config();
        config.subscriptions = vec![
            "devices/+/telemetry".to_string(),
            "devices/+/status".to_string(),
        ];
        config.request_capacity = Some(1);
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

    #[test]
    fn given_mqtt5_zero_keep_alive_should_reject_configuration() {
        let mut config = test_config();
        config.keep_alive = Some("0s".to_string());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_mqtt5_url_zero_keep_alive_should_reject_configuration() {
        let mut config = test_config();
        config.broker_url = "mqtt://localhost:1883?keep_alive_secs=0".to_string();
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_mqtt5_url_keep_alive_below_minimum_should_reject_configuration() {
        let mut config = test_config();
        config.broker_url = "mqtt://localhost:1883?keep_alive_secs=4".to_string();
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_url_credentials_should_reject_configuration() {
        let mut config = test_config();
        config.broker_url = "mqtt://user:password@localhost:1883".to_string();
        let source = MqttSource::new(7, config, None);

        assert!(matches!(
            source.validate_config(),
            Err(Error::InvalidConfigValue(message))
                if message == "broker_url must not contain credentials; configure username and password separately"
        ));
    }

    #[test]
    fn given_url_zero_inflight_should_reject_configuration() {
        for protocol in [MqttProtocol::Mqtt311, MqttProtocol::Mqtt5] {
            let mut config = test_config();
            config.protocol = protocol;
            config.broker_url = "mqtt://localhost:1883?inflight_num=0".to_string();
            let source = MqttSource::new(7, config, None);

            assert!(matches!(
                source.validate_config(),
                Err(Error::InvalidConfigValue(message))
                    if message == "broker_url inflight_num must be greater than zero"
            ));
        }
    }

    #[test]
    fn given_mqtt311_persistent_session_with_empty_client_id_should_reject_configuration() {
        let mut config = test_config();
        config.protocol = MqttProtocol::Mqtt311;
        config.clean_start = false;
        config.client_id = Some(String::new());
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_mqtt311_url_persistent_session_with_empty_client_id_should_reject_configuration() {
        let mut config = test_config();
        config.protocol = MqttProtocol::Mqtt311;
        config.clean_start = true;
        config.client_id = Some(String::new());
        config.broker_url = "mqtt://localhost:1883?clean_session=false".to_string();
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_mqtt311_persistent_session_without_client_id_should_reject_configuration() {
        let mut config = test_config();
        config.protocol = MqttProtocol::Mqtt311;
        config.clean_start = false;
        config.client_id = None;
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_mqtt5_persistent_session_without_client_id_should_reject_configuration() {
        let mut config = test_config();
        config.clean_start = false;
        config.client_id = None;
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_clean_session_without_client_id_should_use_generated_client_id() {
        let mut config = test_config();
        config.clean_start = true;
        config.client_id = None;
        let source = MqttSource::new(7, config, None);

        assert!(source.validate_config().is_ok());
    }
}
