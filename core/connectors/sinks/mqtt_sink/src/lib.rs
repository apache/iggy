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

//! MQTT sink plugin: converts Iggy batches into MQTT publications.
//!
//! The runtime owns polling and offset commits. This crate owns the broker
//! connection, publication acknowledgement, retry policy, and topic mapping.

mod driver;
mod routing;

use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::retry::retry_backoff;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use rumqttc::QoS as MqttQos;
use secrecy::SecretString;
use serde::Deserialize;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::{
    str::FromStr,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use url::Url;

use driver::{MqttDriver, MqttProtocol, validate_tls_config};
use routing::{
    DEFAULT_TOPIC_TEMPLATE, PreparedPublication, TopicMode, prepare_batch_topic,
    prepare_publication_with_batch_topic, validate_topic, validate_topic_template,
};

sink_connector!(MqttSink);

const CONNECTOR_NAME: &str = "MQTT sink";
const DEFAULT_KEEP_ALIVE: &str = "30s";
const DEFAULT_PUBLISH_TIMEOUT: &str = "30s";
const DEFAULT_SHUTDOWN_TIMEOUT: &str = "30s";
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_RETRY_DELAY: &str = "30s";
const DEFAULT_TOPIC: &str = "iggy.messages";
const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_REQUEST_CAPACITY: usize = 32;
const DEFAULT_SESSION_EXPIRY_INTERVAL: u32 = 3600;

#[derive(Debug, Deserialize)]
pub struct MqttSinkConfig {
    // Credentials are kept secret so logging a deserialized config cannot
    // accidentally expose the broker password.
    pub broker_url: String,
    #[serde(default)]
    pub protocol: MqttProtocol,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<SecretString>,
    #[serde(default)]
    pub tls: Option<MqttTlsConfig>,
    #[serde(default)]
    pub clean_start: bool,
    #[serde(default = "default_session_expiry_interval")]
    pub session_expiry_interval: Option<u32>,
    #[serde(default = "default_topic")]
    pub topic: Option<String>,
    #[serde(default)]
    pub topic_mode: TopicMode,
    pub topic_template: Option<String>,
    pub fallback_topic: Option<String>,
    #[serde(default = "default_qos")]
    pub qos: u8,
    #[serde(default)]
    pub retain: bool,
    pub keep_alive: Option<String>,
    pub request_capacity: Option<usize>,
    pub publish_timeout: Option<String>,
    pub shutdown_timeout: Option<String>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub max_in_flight: Option<usize>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct MqttTlsConfig {
    pub ca_file: Option<String>,
    pub client_cert_file: Option<String>,
    pub client_key_file: Option<String>,
}

fn default_session_expiry_interval() -> Option<u32> {
    Some(DEFAULT_SESSION_EXPIRY_INTERVAL)
}

fn default_topic() -> Option<String> {
    Some(DEFAULT_TOPIC.to_string())
}

fn default_qos() -> u8 {
    1
}

fn validate_broker_url(config: &MqttSinkConfig) -> Result<(), Error> {
    let url = Url::parse(&config.broker_url)
        .map_err(|error| Error::InvalidConfigValue(format!("broker_url: {error}")))?;

    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::InvalidConfigValue(
            "broker_url must not contain credentials; configure username and password separately"
                .to_string(),
        ));
    }

    for (option, field) in [
        ("keep_alive_secs", "keep_alive"),
        ("inflight_num", "max_in_flight"),
        ("clean_session", "clean_start"),
        ("clean_start", "clean_start"),
        ("request_channel_capacity_num", "request_capacity"),
        ("client_id", "client_id"),
    ] {
        if url.query_pairs().any(|(key, _)| key == option) {
            return Err(Error::InvalidConfigValue(format!(
                "broker_url must not contain {option}; configure {field} separately"
            )));
        }
    }

    Ok(())
}

#[derive(Debug)]
pub struct MqttSink {
    id: u32,
    config: MqttSinkConfig,
    keep_alive: Duration,
    publish_timeout: Duration,
    shutdown_timeout: Duration,
    retry_delay: Duration,
    max_retry_delay: Duration,
    max_retries: u32,
    max_in_flight: usize,
    verbose: bool,
    driver: Mutex<Option<MqttDriver>>,
    publish_lock: Arc<Mutex<()>>,
    messages_published: AtomicU64,
    publish_errors: AtomicU64,
    in_flight_publications: AtomicU64,
}

impl MqttSink {
    pub fn new(id: u32, config: MqttSinkConfig) -> Self {
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES).max(1);
        let max_in_flight = config.max_in_flight.unwrap_or(1).max(1);

        Self {
            id,
            verbose: config.verbose_logging.unwrap_or(false),
            config,
            keep_alive: Duration::from_secs(30),
            publish_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(30),
            retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(30),
            max_retries,
            max_in_flight,
            driver: Mutex::new(None),
            publish_lock: Arc::new(Mutex::new(())),
            messages_published: AtomicU64::new(0),
            publish_errors: AtomicU64::new(0),
            in_flight_publications: AtomicU64::new(0),
        }
    }

    fn qos(&self) -> Result<MqttQos, Error> {
        match self.config.qos {
            0 => Ok(MqttQos::AtMostOnce),
            1 => Ok(MqttQos::AtLeastOnce),
            2 => Ok(MqttQos::ExactlyOnce),
            qos => Err(Error::InvalidConfigValue(format!(
                "qos {qos} is unsupported; expected 0, 1, or 2"
            ))),
        }
    }

    fn validate_config(
        &self,
    ) -> Result<(Duration, Duration, Duration, Duration, Duration, usize), Error> {
        if self.config.broker_url.trim().is_empty() {
            return Err(Error::InvalidConfigValue(
                "broker_url must not be empty".to_string(),
            ));
        }
        if self.config.username.is_some() != self.config.password.is_some() {
            return Err(Error::InvalidConfigValue(
                "username and password must be configured together".to_string(),
            ));
        }
        validate_broker_url(&self.config)?;
        validate_tls_config(&self.config)?;
        let keep_alive = parse_duration(
            self.config.keep_alive.as_deref(),
            DEFAULT_KEEP_ALIVE,
            "keep_alive",
        )?;
        validate_keep_alive(self.config.protocol, keep_alive, "keep_alive")?;
        let publish_timeout = parse_duration(
            self.config.publish_timeout.as_deref(),
            DEFAULT_PUBLISH_TIMEOUT,
            "publish_timeout",
        )?;
        let shutdown_timeout = parse_duration(
            self.config.shutdown_timeout.as_deref(),
            DEFAULT_SHUTDOWN_TIMEOUT,
            "shutdown_timeout",
        )?;
        let retry_delay = parse_duration(
            self.config.retry_delay.as_deref(),
            DEFAULT_RETRY_DELAY,
            "retry_delay",
        )?;
        let max_retry_delay = parse_duration(
            self.config.max_retry_delay.as_deref(),
            DEFAULT_MAX_RETRY_DELAY,
            "max_retry_delay",
        )?;
        self.qos()?;
        if let Some(topic) = self.config.topic.as_deref() {
            validate_topic(topic, "topic")?;
        }
        let topic_template = self
            .config
            .topic_template
            .as_deref()
            .unwrap_or(DEFAULT_TOPIC_TEMPLATE);
        validate_topic_template(topic_template, "topic_template")?;
        let fallback_topic = self
            .config
            .fallback_topic
            .as_deref()
            .unwrap_or(topic_template);
        validate_topic_template(fallback_topic, "fallback_topic")?;
        if self.config.request_capacity == Some(0) {
            return Err(Error::InvalidConfigValue(
                "request_capacity must be greater than zero".to_string(),
            ));
        }
        let request_capacity = self
            .config
            .request_capacity
            .unwrap_or(DEFAULT_REQUEST_CAPACITY);
        let max_in_flight = self.config.max_in_flight.unwrap_or(1);
        if max_in_flight > request_capacity {
            return Err(Error::InvalidConfigValue(
                "max_in_flight must not exceed request_capacity".to_string(),
            ));
        }
        if !self.config.clean_start
            && !self
                .config
                .client_id
                .as_deref()
                .is_some_and(|client_id| !client_id.trim().is_empty())
        {
            return Err(Error::InvalidConfigValue(
                "client_id must be present and non-empty when clean_start is false".to_string(),
            ));
        }
        Ok((
            keep_alive,
            publish_timeout,
            shutdown_timeout,
            retry_delay,
            max_retry_delay.max(retry_delay),
            max_in_flight.max(1),
        ))
    }

    fn client_id(&self) -> String {
        self.config
            .client_id
            .clone()
            .unwrap_or_else(|| format!("iggy-mqtt-sink-{}", self.id))
    }

    async fn connect_driver(&self) -> Result<MqttDriver, Error> {
        MqttDriver::connect(
            self.id,
            &self.config,
            self.client_id(),
            self.keep_alive,
            self.publish_timeout,
            self.config
                .request_capacity
                .unwrap_or(DEFAULT_REQUEST_CAPACITY),
            self.max_in_flight,
        )
        .await
    }

    fn finish_unconfirmed_publications(&self, confirmed: &[bool]) {
        let unfinished = confirmed.iter().filter(|confirmed| !**confirmed).count() as u64;
        self.in_flight_publications
            .fetch_sub(unfinished, Ordering::Relaxed);
        self.publish_errors.fetch_add(unfinished, Ordering::Relaxed);
    }

    fn prepare_publications(
        &self,
        topic_metadata: &TopicMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<Vec<PreparedPublication>, Error> {
        let batch_topic = prepare_batch_topic(
            topic_metadata,
            self.config.topic_mode,
            self.config.topic.as_deref(),
            self.config.topic_template.as_deref(),
        )
        .map_err(|error| {
            warn!(
                "Rejecting MQTT batch for connector with ID {} because batch topic is invalid, stream: {}, topic: {}: {error}",
                self.id, topic_metadata.stream, topic_metadata.topic
            );
            self.publish_errors.fetch_add(1, Ordering::Relaxed);
            error
        })?;
        let mut publications = Vec::with_capacity(messages.len());
        for message in messages {
            let offset = message.offset;
            match prepare_publication_with_batch_topic(
                message,
                topic_metadata,
                self.config.topic_mode,
                batch_topic.as_ref(),
                self.config.topic_template.as_deref(),
                self.config.fallback_topic.as_deref(),
                self.config.protocol,
            ) {
                Ok(publication) => publications.push(publication),
                Err(error) => {
                    warn!(
                        "Rejecting MQTT batch for connector with ID {} because record is invalid, stream: {}, topic: {}, offset: {}: {error}",
                        self.id, topic_metadata.stream, topic_metadata.topic, offset
                    );
                    self.publish_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(error);
                }
            }
        }
        Ok(publications)
    }

    async fn publish_messages(
        &self,
        topic_metadata: &TopicMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<u64, Error> {
        let qos = self.qos()?;
        let publications = self.prepare_publications(topic_metadata, messages)?;
        if publications.is_empty() {
            return Ok(0);
        }
        // A single event loop cannot safely be driven by concurrent consumers.
        // Serializing MQTT delivery keeps acknowledgement events associated
        // with the publication that caused them.
        let _publish_guard = self.publish_lock.lock().await;
        let mut confirmed = vec![false; publications.len()];
        self.in_flight_publications
            .fetch_add(publications.len() as u64, Ordering::Relaxed);
        let mut driver = self.driver.lock().await.take();

        let mut attempts = 1;
        loop {
            let mut active_driver = match driver.take() {
                Some(driver) => driver,
                None => match self.connect_driver().await {
                    Ok(reconnected) => reconnected,
                    Err(error) if attempts == self.max_retries => {
                        self.finish_unconfirmed_publications(&confirmed);
                        return Err(error);
                    }
                    Err(error) => {
                        warn!(
                            "Retrying MQTT connection for connector with ID {} (attempt {attempts}/{}): {error}",
                            self.id, self.max_retries
                        );
                        let delay = retry_backoff(self.retry_delay, attempts, self.max_retry_delay);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                },
            };
            let pending_indices: Vec<usize> = confirmed
                .iter()
                .enumerate()
                .filter_map(|(index, is_confirmed)| (!is_confirmed).then_some(index))
                .collect();
            let mut failure = None;

            for window_indices in pending_indices.chunks(self.max_in_flight) {
                let window: Vec<_> = window_indices
                    .iter()
                    .map(|&index| &publications[index])
                    .collect();
                match active_driver
                    .publish_window(&window, qos, self.config.retain, self.publish_timeout)
                    .await
                {
                    Ok(()) => {
                        for &index in window_indices {
                            if !confirmed[index] {
                                confirmed[index] = true;
                                self.in_flight_publications.fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(window_error) => {
                        for local_index in &window_error.confirmed {
                            let index = window_indices[*local_index];
                            if !confirmed[index] {
                                confirmed[index] = true;
                                self.in_flight_publications.fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                        failure = Some(window_error);
                        break;
                    }
                }
            }

            let window_error = match failure {
                Some(window_error) => window_error,
                None => {
                    *self.driver.lock().await = Some(active_driver);
                    return Ok(confirmed.iter().filter(|confirmed| **confirmed).count() as u64);
                }
            };
            // A failed QoS 2 handshake can leave rumqttc's outgoing window
            // full. Do not reuse the event loop after any window failure.
            drop(active_driver);
            if !window_error.retryable {
                self.finish_unconfirmed_publications(&confirmed);
                return Err(window_error.error);
            }
            if attempts >= self.max_retries {
                self.finish_unconfirmed_publications(&confirmed);
                return Err(window_error.error);
            }

            warn!(
                "Discarding uncertain MQTT connection for connector with ID {} before retry {}/{}: {}",
                self.id,
                attempts + 1,
                self.max_retries,
                window_error.error
            );
            let delay = retry_backoff(self.retry_delay, attempts, self.max_retry_delay);
            tokio::time::sleep(delay).await;
            attempts += 1;
        }
    }
}

#[async_trait]
impl Sink for MqttSink {
    async fn open(&mut self) -> Result<(), Error> {
        let (
            keep_alive,
            publish_timeout,
            shutdown_timeout,
            retry_delay,
            max_retry_delay,
            max_in_flight,
        ) = self.validate_config()?;
        self.keep_alive = keep_alive;
        self.publish_timeout = publish_timeout;
        self.shutdown_timeout = shutdown_timeout;
        self.retry_delay = retry_delay;
        self.max_retry_delay = max_retry_delay;
        self.max_in_flight = max_in_flight;
        let qos = self.qos()?;
        // Connection establishment waits for CONNACK, so a successful open
        // means the broker accepted the session rather than only queueing a
        // local rumqttc request.
        let mut attempt = 1;
        let driver = loop {
            match self.connect_driver().await {
                Ok(driver) => break driver,
                Err(Error::Connection(error)) if attempt < self.max_retries => {
                    warn!(
                        "Retrying MQTT connection for connector with ID {} (attempt {attempt}/{}): {error}",
                        self.id, self.max_retries
                    );
                    let delay = retry_backoff(self.retry_delay, attempt, self.max_retry_delay);
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        };
        *self.driver.get_mut() = Some(driver);
        info!(
            "Opened {CONNECTOR_NAME} connector with ID {}, protocol: {:?}, topic: {:?}, mode: {:?}, qos: {:?}, retain: {}",
            self.id,
            self.config.protocol,
            self.config.topic,
            self.config.topic_mode,
            qos,
            self.config.retain
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        _messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let published = self.publish_messages(topic_metadata, messages).await?;
        self.messages_published
            .fetch_add(published, Ordering::Relaxed);
        if self.verbose {
            info!(
                "Published {published} messages to MQTT for connector with ID {}",
                self.id
            );
        } else {
            debug!(
                "Published {published} messages to MQTT for connector with ID {}",
                self.id
            );
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let shutdown_started = Instant::now();
        let _publish_guard = timeout(self.shutdown_timeout, self.publish_lock.lock())
            .await
            .map_err(|_| {
                let unfinished = self.in_flight_publications.load(Ordering::Relaxed);
                error!(
                    "MQTT shutdown timed out for connector with ID {} with {unfinished} unfinished publications",
                    self.id
                );
                Error::Connection(format!(
                    "MQTT shutdown timed out with {unfinished} unfinished publications"
                ))
            })?;
        let remaining = self
            .shutdown_timeout
            .saturating_sub(shutdown_started.elapsed());
        let driver = self.driver.lock().await.take();
        if let Some(mut driver) = driver {
            if remaining.is_zero() {
                error!(
                    "MQTT shutdown timed out for connector with ID {} before disconnecting",
                    self.id
                );
                return Err(Error::Connection(
                    "MQTT shutdown timed out before disconnecting".to_string(),
                ));
            }
            timeout(remaining, driver.disconnect(remaining))
                .await
                .map_err(|_| {
                    error!(
                        "MQTT shutdown timed out for connector with ID {} while disconnecting",
                        self.id
                    );
                    Error::Connection("MQTT shutdown timed out while disconnecting".to_string())
                })??;
        }
        info!(
            "Closed {CONNECTOR_NAME} connector with ID {} processed {} messages with {} errors and {} unfinished publications",
            self.id,
            self.messages_published.load(Ordering::Relaxed),
            self.publish_errors.load(Ordering::Relaxed),
            self.in_flight_publications.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

fn parse_duration(value: Option<&str>, default: &str, field: &str) -> Result<Duration, Error> {
    let value = value.unwrap_or(default);
    HumanDuration::from_str(value)
        .map(|duration| *duration)
        .map_err(|error| Error::InvalidConfigValue(format!("{field}: {error}")))
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

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Payload;

    fn test_config() -> MqttSinkConfig {
        MqttSinkConfig {
            broker_url: "mqtt://localhost:1883".to_string(),
            protocol: MqttProtocol::Mqtt5,
            client_id: Some("test-client".to_string()),
            username: None,
            password: None,
            tls: None,
            clean_start: false,
            session_expiry_interval: Some(DEFAULT_SESSION_EXPIRY_INTERVAL),
            topic: Some("test/topic".to_string()),
            topic_mode: TopicMode::Static,
            topic_template: None,
            fallback_topic: None,
            qos: 1,
            retain: false,
            keep_alive: Some("30s".to_string()),
            request_capacity: Some(32),
            publish_timeout: Some("5s".to_string()),
            shutdown_timeout: Some("5s".to_string()),
            max_retries: Some(5),
            retry_delay: Some("1s".to_string()),
            max_retry_delay: Some("30s".to_string()),
            max_in_flight: Some(1),
            verbose_logging: Some(false),
        }
    }

    fn test_messages() -> Vec<ConsumedMessage> {
        vec![
            ConsumedMessage {
                id: 1,
                offset: 10,
                checksum: 1,
                timestamp: 1,
                origin_timestamp: 1,
                headers: None,
                payload: Payload::Raw(b"first".to_vec()),
            },
            ConsumedMessage {
                id: 2,
                offset: 11,
                checksum: 1,
                timestamp: 1,
                origin_timestamp: 1,
                headers: None,
                payload: Payload::Raw(b"second".to_vec()),
            },
        ]
    }

    #[test]
    fn given_qos_one_when_new_should_keep_default_delivery_mode() {
        let sink = MqttSink::new(1, test_config());
        assert_eq!(sink.config.qos, 1);
        assert_eq!(sink.max_in_flight, 1);
    }

    #[test]
    fn given_no_max_retries_should_use_default() {
        let mut config = test_config();
        config.max_retries = None;
        let sink = MqttSink::new(1, config);

        assert_eq!(sink.max_retries, DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn given_keep_alive_above_protocol_minimum_should_accept_configuration() {
        let sink = MqttSink::new(1, test_config());

        assert!(sink.validate_config().is_ok());
    }

    #[test]
    fn given_mqtt311_zero_keep_alive_should_accept_configuration() {
        let mut config = test_config();
        config.protocol = MqttProtocol::Mqtt311;
        config.keep_alive = Some("0s".to_string());
        let sink = MqttSink::new(1, config);

        assert!(sink.validate_config().is_ok());
    }

    #[test]
    fn given_mqtt5_zero_keep_alive_should_reject_configuration() {
        let mut config = test_config();
        config.keep_alive = Some("0s".to_string());
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message.contains("keep_alive")
        ));
    }

    #[test]
    fn given_keep_alive_below_protocol_minimum_should_reject_configuration() {
        let mut config = test_config();
        config.protocol = MqttProtocol::Mqtt311;
        config.keep_alive = Some("500ms".to_string());
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message.contains("keep_alive")
        ));
    }

    #[test]
    fn given_unparsable_keep_alive_should_reject_configuration() {
        let mut config = test_config();
        config.keep_alive = Some("not a duration".to_string());
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message.contains("keep_alive")
        ));
    }

    #[test]
    fn given_supported_qos_when_qos_should_map_to_rumqttc() {
        for qos in [0, 1, 2] {
            let mut config = test_config();
            config.qos = qos;
            let sink = MqttSink::new(1, config);
            assert!(sink.qos().is_ok());
        }
    }

    #[test]
    fn given_unsupported_qos_when_qos_should_return_configuration_error() {
        let mut config = test_config();
        config.qos = 3;
        let sink = MqttSink::new(1, config);
        assert!(matches!(sink.qos(), Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_max_in_flight_above_request_capacity_when_validating_should_return_configuration_error()
     {
        let mut config = test_config();
        config.request_capacity = Some(2);
        config.max_in_flight = Some(3);
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_unknown_topic_template_placeholder_when_validating_should_return_configuration_error()
    {
        let mut config = test_config();
        config.topic_template = Some("{stream}/{partition}".to_string());
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message.contains("unsupported placeholder")
        ));
    }

    #[test]
    fn given_broker_url_with_credentials_when_validating_should_return_configuration_error() {
        let mut config = test_config();
        config.broker_url = "mqtt://user:password@localhost:1883".to_string();
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message.contains("must not contain credentials")
        ));
    }

    #[test]
    fn given_configured_credentials_when_validating_should_accept_broker_url_without_credentials() {
        let mut config = test_config();
        config.username = Some("user".to_string());
        config.password = Some(SecretString::from("password"));
        let sink = MqttSink::new(1, config);

        assert!(sink.validate_config().is_ok());
    }

    #[test]
    fn given_broker_url_transport_option_when_validating_should_return_configuration_error() {
        for (option, field) in [
            ("keep_alive_secs", "keep_alive"),
            ("inflight_num", "max_in_flight"),
            ("clean_session", "clean_start"),
            ("clean_start", "clean_start"),
            ("request_channel_capacity_num", "request_capacity"),
            ("client_id", "client_id"),
        ] {
            let mut config = test_config();
            config.broker_url = format!("mqtt://localhost:1883?{option}=value");
            let sink = MqttSink::new(1, config);

            assert!(matches!(
                sink.validate_config(),
                Err(Error::InvalidConfigValue(message))
                    if message == format!(
                        "broker_url must not contain {option}; configure {field} separately"
                    )
            ));
        }
    }

    #[test]
    fn given_client_certificate_without_private_key_when_validating_should_return_configuration_error()
     {
        let mut config = test_config();
        config.broker_url = "mqtts://localhost:8883".to_string();
        config.tls = Some(MqttTlsConfig {
            ca_file: Some("ca.pem".to_string()),
            client_cert_file: Some("client.pem".to_string()),
            client_key_file: None,
        });
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message.contains("together")
        ));
    }

    #[test]
    fn given_mutual_tls_without_ca_file_when_validating_should_accept_configuration() {
        let mut config = test_config();
        config.broker_url = "mqtts://localhost:8883".to_string();
        config.tls = Some(MqttTlsConfig {
            ca_file: None,
            client_cert_file: Some("client.pem".to_string()),
            client_key_file: Some("client.key".to_string()),
        });
        let sink = MqttSink::new(1, config);

        assert!(sink.validate_config().is_ok());
    }

    #[test]
    fn given_tls_file_with_unencrypted_broker_url_when_validating_should_return_configuration_error()
     {
        let mut config = test_config();
        config.tls = Some(MqttTlsConfig {
            ca_file: Some("ca.pem".to_string()),
            client_cert_file: None,
            client_key_file: None,
        });
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message.contains("mqtts:// or ssl://")
        ));
    }

    #[test]
    fn given_missing_client_id_when_client_id_should_use_connector_id() {
        let mut config = test_config();
        config.client_id = None;
        let sink = MqttSink::new(42, config);
        assert_eq!(sink.client_id(), "iggy-mqtt-sink-42");
    }

    #[test]
    fn given_blank_client_id_when_clean_start_is_false_should_return_configuration_error() {
        let mut config = test_config();
        config.client_id = Some("   ".to_string());
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message))
                if message == "client_id must be present and non-empty when clean_start is false"
        ));
    }

    #[test]
    fn given_unconfirmed_publications_when_finishing_should_clear_and_report_them() {
        let sink = MqttSink::new(1, test_config());
        sink.in_flight_publications.store(1, Ordering::Relaxed);

        sink.finish_unconfirmed_publications(&[true, false]);

        assert_eq!(sink.in_flight_publications.load(Ordering::Relaxed), 0);
        assert_eq!(sink.publish_errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn given_static_mode_when_preparing_batch_should_share_topic_storage() {
        let sink = MqttSink::new(1, test_config());
        let metadata = TopicMetadata {
            stream: "devices".to_string(),
            topic: "telemetry".to_string(),
        };
        let publications = sink
            .prepare_publications(&metadata, test_messages())
            .unwrap();

        assert!(Arc::ptr_eq(
            publications[0]
                .topic
                .as_shared()
                .expect("static topics must be shared"),
            publications[1]
                .topic
                .as_shared()
                .expect("static topics must be shared"),
        ));
    }

    #[test]
    fn given_template_mode_when_preparing_batch_should_share_topic_storage() {
        let mut config = test_config();
        config.topic_mode = TopicMode::Template;
        config.topic_template = Some("{stream}/output/{topic}".to_string());
        let sink = MqttSink::new(1, config);
        let metadata = TopicMetadata {
            stream: "devices".to_string(),
            topic: "telemetry".to_string(),
        };

        let publications = sink
            .prepare_publications(&metadata, test_messages())
            .unwrap();

        assert!(Arc::ptr_eq(
            publications[0]
                .topic
                .as_shared()
                .expect("template topics must be shared"),
            publications[1]
                .topic
                .as_shared()
                .expect("template topics must be shared"),
        ));
        assert_eq!(publications[0].topic.as_str(), "devices/output/telemetry");
    }

    #[test]
    fn given_malformed_dynamic_record_when_preparing_should_reject_batch() {
        let mut config = test_config();
        config.topic_mode = TopicMode::Dynamic;
        config.fallback_topic = Some("fallback/{stream}/{topic}".to_string());
        let sink = MqttSink::new(1, config);
        let metadata = TopicMetadata {
            stream: "devices".to_string(),
            topic: "telemetry".to_string(),
        };
        let malformed = br#"{
            "version": 1,
            "payload_base64": "not base64"
        }"#;
        let mut malformed = malformed.to_vec();
        let malformed = simd_json::to_owned_value(&mut malformed).expect("JSON must parse");
        let messages = vec![
            ConsumedMessage {
                id: 1,
                offset: 10,
                checksum: 1,
                timestamp: 1,
                origin_timestamp: 1,
                headers: None,
                payload: Payload::Raw(b"valid".to_vec()),
            },
            ConsumedMessage {
                id: 2,
                offset: 11,
                checksum: 1,
                timestamp: 1,
                origin_timestamp: 1,
                headers: None,
                payload: Payload::Json(malformed),
            },
        ];

        let error = sink
            .prepare_publications(&metadata, messages)
            .expect_err("malformed record must reject the batch");

        assert!(matches!(error, Error::InvalidRecordValue(_)));
        assert_eq!(sink.publish_errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn given_invalid_batch_topic_when_preparing_should_count_publish_error() {
        let mut config = test_config();
        config.topic = Some("invalid\0topic".to_string());
        let sink = MqttSink::new(1, config);
        let metadata = TopicMetadata {
            stream: "devices".to_string(),
            topic: "telemetry".to_string(),
        };

        let error = sink
            .prepare_publications(&metadata, test_messages())
            .expect_err("invalid batch topic must reject the batch");

        assert!(matches!(error, Error::InvalidRecordValue(_)));
        assert_eq!(sink.publish_errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn given_no_active_driver_when_closed_should_release_sink() {
        let mut sink = MqttSink::new(1, test_config());
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");

        runtime
            .block_on(sink.close())
            .expect("sink without a driver must close");
    }

    #[test]
    fn given_publications_in_flight_when_shutdown_times_out_should_report_them() {
        let mut sink = MqttSink::new(1, test_config());
        sink.shutdown_timeout = Duration::from_millis(10);
        sink.in_flight_publications.store(2, Ordering::Relaxed);
        let publish_lock = Arc::clone(&sink.publish_lock);
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");

        runtime.block_on(async {
            let _publish_guard = publish_lock.lock().await;
            let result = timeout(Duration::from_secs(1), sink.close())
                .await
                .expect("shutdown must respect its timeout");

            assert!(
                matches!(result, Err(Error::Connection(message)) if message.contains("2 unfinished publications")),
                "shutdown must report the number of publications that did not drain"
            );
        });
    }

    #[test]
    fn given_minimal_json_when_deserialized_should_require_broker_url() {
        let error = serde_json::from_str::<MqttSinkConfig>("{}").unwrap_err();
        assert!(error.to_string().contains("broker_url"));
        let config: MqttSinkConfig = serde_json::from_str(
            r#"{"broker_url":"mqtt://localhost:1883","client_id":"test-client"}"#,
        )
        .unwrap();
        let sink = MqttSink::new(1, config);
        assert_eq!(sink.config.topic.as_deref(), Some(DEFAULT_TOPIC));
        assert_eq!(sink.config.qos, 1);
        assert_eq!(sink.max_in_flight, 1);
        assert_eq!(sink.shutdown_timeout, Duration::from_secs(30));
    }
}
