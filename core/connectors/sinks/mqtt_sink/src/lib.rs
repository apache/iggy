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
use std::sync::atomic::{AtomicU64, Ordering};
use std::{str::FromStr, time::Duration};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use driver::{MqttDriver, MqttProtocol};
use routing::{TopicMode, prepare_publication, validate_topic};

sink_connector!(MqttSink);

const CONNECTOR_NAME: &str = "MQTT sink";
const DEFAULT_BROKER_URL: &str = "mqtt://127.0.0.1:1883";
const DEFAULT_KEEP_ALIVE: &str = "30s";
const DEFAULT_PUBLISH_TIMEOUT: &str = "30s";
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_RETRY_DELAY: &str = "30s";
const DEFAULT_TOPIC: &str = "iggy.messages";
const DEFAULT_MAX_RETRIES: u32 = 3;

#[derive(Debug, Deserialize)]
pub struct MqttSinkConfig {
    // Credentials are kept secret so logging a deserialized config cannot
    // accidentally expose the broker password.
    #[serde(default = "default_broker_url")]
    pub broker_url: String,
    #[serde(default)]
    pub protocol: MqttProtocol,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<SecretString>,
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
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub max_in_flight: Option<usize>,
    pub verbose_logging: Option<bool>,
}

fn default_broker_url() -> String {
    DEFAULT_BROKER_URL.to_string()
}

fn default_topic() -> Option<String> {
    Some(DEFAULT_TOPIC.to_string())
}

fn default_qos() -> u8 {
    1
}

#[derive(Debug)]
pub struct MqttSink {
    id: u32,
    config: MqttSinkConfig,
    keep_alive: Duration,
    publish_timeout: Duration,
    retry_delay: Duration,
    max_retry_delay: Duration,
    max_retries: u32,
    max_in_flight: usize,
    verbose: bool,
    driver: Mutex<Option<MqttDriver>>,
    publish_lock: Mutex<()>,
    messages_published: AtomicU64,
    publish_errors: AtomicU64,
}

impl MqttSink {
    pub fn new(id: u32, config: MqttSinkConfig) -> Self {
        let keep_alive = parse_duration(
            config.keep_alive.as_deref().unwrap_or(DEFAULT_KEEP_ALIVE),
            Duration::from_secs(30),
            "keep_alive",
            id,
        );
        let publish_timeout = parse_duration(
            config
                .publish_timeout
                .as_deref()
                .unwrap_or(DEFAULT_PUBLISH_TIMEOUT),
            Duration::from_secs(30),
            "publish_timeout",
            id,
        );
        let retry_delay = parse_duration(
            config.retry_delay.as_deref().unwrap_or(DEFAULT_RETRY_DELAY),
            Duration::from_secs(1),
            "retry_delay",
            id,
        );
        let max_retry_delay = parse_duration(
            config
                .max_retry_delay
                .as_deref()
                .unwrap_or(DEFAULT_MAX_RETRY_DELAY),
            Duration::from_secs(30),
            "max_retry_delay",
            id,
        );
        let keep_alive = if keep_alive < Duration::from_secs(5) {
            warn!("keep_alive must be at least 5s for MQTT sink connector ID: {id}; using 5s");
            Duration::from_secs(5)
        } else {
            keep_alive
        };
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES).max(1);
        let max_in_flight = config.max_in_flight.unwrap_or(1).max(1);

        Self {
            id,
            verbose: config.verbose_logging.unwrap_or(false),
            config,
            keep_alive,
            publish_timeout,
            retry_delay,
            max_retry_delay: max_retry_delay.max(retry_delay),
            max_retries,
            max_in_flight,
            driver: Mutex::new(None),
            publish_lock: Mutex::new(()),
            messages_published: AtomicU64::new(0),
            publish_errors: AtomicU64::new(0),
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

    fn validate_config(&self) -> Result<(), Error> {
        self.qos()?;
        if let Some(topic) = self.config.topic.as_deref() {
            validate_topic(topic, "topic")?;
        }
        if let Some(fallback_topic) = self.config.fallback_topic.as_deref() {
            validate_topic(fallback_topic, "fallback_topic")?;
        }
        if let Some(topic_template) = self.config.topic_template.as_deref()
            && topic_template.trim().is_empty()
        {
            return Err(Error::InvalidConfigValue(
                "topic_template must not be empty".to_string(),
            ));
        }
        if self.config.request_capacity == Some(0) {
            return Err(Error::InvalidConfigValue(
                "request_capacity must be greater than zero".to_string(),
            ));
        }
        let request_capacity = self.config.request_capacity.unwrap_or(32);
        let max_in_flight = self.config.max_in_flight.unwrap_or(1);
        if max_in_flight > request_capacity {
            return Err(Error::InvalidConfigValue(
                "max_in_flight must not exceed request_capacity".to_string(),
            ));
        }
        Ok(())
    }

    fn client_id(&self) -> String {
        self.config
            .client_id
            .clone()
            .unwrap_or_else(|| format!("iggy-mqtt-sink-{}", self.id))
    }

    async fn publish_messages(
        &self,
        topic_metadata: &TopicMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<u64, Error> {
        // A single event loop cannot safely be driven by concurrent consumers.
        // Serializing the batch also keeps acknowledgement events associated
        // with the publication that caused them.
        let _publish_guard = self.publish_lock.lock().await;
        let qos = self.qos()?;
        let mut publications = Vec::with_capacity(messages.len());
        for message in messages {
            publications.push(prepare_publication(
                &message,
                topic_metadata,
                self.config.topic_mode,
                self.config.topic.as_deref(),
                self.config.topic_template.as_deref(),
                self.config.fallback_topic.as_deref(),
                self.config.protocol,
            )?);
        }
        let mut driver = self
            .driver
            .lock()
            .await
            .take()
            .ok_or_else(|| Error::InitError("MQTT driver is not initialized".into()))?;
        let mut confirmed = vec![false; publications.len()];
        let mut attempts = 0;
        loop {
            attempts += 1;
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
                match driver
                    .publish_window(&window, qos, self.config.retain, self.publish_timeout)
                    .await
                {
                    Ok(window_confirmed) => {
                        for local_index in window_confirmed {
                            confirmed[window_indices[local_index]] = true;
                        }
                    }
                    Err(window_error) => {
                        for local_index in window_error.confirmed {
                            confirmed[window_indices[local_index]] = true;
                        }
                        failure = Some(window_error.error);
                        break;
                    }
                }
            }

            if failure.is_none() {
                *self.driver.lock().await = Some(driver);
                return Ok(confirmed.iter().filter(|confirmed| **confirmed).count() as u64);
            }

            let Some(error) = failure else {
                continue;
            };
            if attempts >= self.max_retries {
                self.publish_errors.fetch_add(
                    confirmed.iter().filter(|confirmed| !**confirmed).count() as u64,
                    Ordering::Relaxed,
                );
                *self.driver.lock().await = Some(driver);
                return Err(error);
            }

            warn!(
                "Retrying MQTT publish window for connector ID: {} (attempt {attempts}/{}): {error}",
                self.id, self.max_retries
            );
            if let Err(reconnect_error) = driver.reconnect().await {
                *self.driver.lock().await = Some(driver);
                return Err(reconnect_error);
            }
            let delay = retry_backoff(self.retry_delay, attempts, self.max_retry_delay);
            tokio::time::sleep(delay).await;
        }
    }
}

#[async_trait]
impl Sink for MqttSink {
    async fn open(&mut self) -> Result<(), Error> {
        self.validate_config()?;
        if self.config.username.is_some() != self.config.password.is_some() {
            return Err(Error::InvalidConfigValue(
                "username and password must be configured together".to_string(),
            ));
        }
        let qos = self.qos()?;
        let client_id = self.client_id();
        // Connection establishment waits for CONNACK, so a successful open
        // means the broker accepted the session rather than only queueing a
        // local rumqttc request.
        let mut driver = None;
        for attempt in 1..=self.max_retries {
            match MqttDriver::connect(
                self.id,
                &self.config,
                client_id.clone(),
                self.keep_alive,
                self.publish_timeout,
                self.config.request_capacity.unwrap_or(32),
                self.max_in_flight,
            )
            .await
            {
                Ok(connected) => {
                    driver = Some(connected);
                    break;
                }
                Err(error) if attempt < self.max_retries => {
                    warn!(
                        "Retrying MQTT connection for connector ID: {} (attempt {attempt}/{}): {error}",
                        self.id, self.max_retries
                    );
                    let delay = retry_backoff(self.retry_delay, attempt, self.max_retry_delay);
                    tokio::time::sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
        let driver = driver.ok_or_else(|| Error::InitError("MQTT connection failed".into()))?;
        *self.driver.get_mut() = Some(driver);
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, protocol: {:?}, topic: {:?}, mode: {:?}, qos: {:?}, retain: {}",
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
                "Published {published} messages to MQTT for connector ID: {}",
                self.id
            );
        } else {
            debug!(
                "Published {published} messages to MQTT for connector ID: {}",
                self.id
            );
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        // Taking the driver out prevents new publication work from borrowing
        // it while the event loop is being drained and disconnected.
        if let Some(mut driver) = self.driver.lock().await.take() {
            driver.disconnect(self.publish_timeout).await?;
        }
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {} processed {} messages with {} errors",
            self.id,
            self.messages_published.load(Ordering::Relaxed),
            self.publish_errors.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

fn parse_duration(raw: &str, default: Duration, field: &str, id: u32) -> Duration {
    match HumanDuration::from_str(raw) {
        Ok(duration) if !duration.is_zero() => duration.into(),
        _ => {
            warn!("Invalid {field} for MQTT sink connector ID: {id}; using {default:?}");
            default
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MqttSinkConfig {
        MqttSinkConfig {
            broker_url: "mqtt://localhost:1883".to_string(),
            protocol: MqttProtocol::Mqtt5,
            client_id: Some("test-client".to_string()),
            username: None,
            password: None,
            topic: Some("test/topic".to_string()),
            topic_mode: TopicMode::Static,
            topic_template: None,
            fallback_topic: None,
            qos: 1,
            retain: false,
            keep_alive: Some("30s".to_string()),
            request_capacity: Some(32),
            publish_timeout: Some("5s".to_string()),
            max_retries: Some(3),
            retry_delay: Some("1s".to_string()),
            max_retry_delay: Some("30s".to_string()),
            max_in_flight: Some(1),
            verbose_logging: Some(false),
        }
    }

    #[test]
    fn given_qos_one_when_new_should_keep_default_delivery_mode() {
        let sink = MqttSink::new(1, test_config());
        assert_eq!(sink.config.qos, 1);
        assert_eq!(sink.max_in_flight, 1);
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
    fn given_missing_client_id_when_client_id_should_use_connector_id() {
        let mut config = test_config();
        config.client_id = None;
        let sink = MqttSink::new(42, config);
        assert_eq!(sink.client_id(), "iggy-mqtt-sink-42");
    }

    #[test]
    fn given_minimal_json_when_deserialized_should_apply_defaults() {
        let config: MqttSinkConfig = serde_json::from_str("{}").unwrap();
        let sink = MqttSink::new(1, config);
        assert_eq!(sink.config.broker_url, DEFAULT_BROKER_URL);
        assert_eq!(sink.config.topic.as_deref(), Some(DEFAULT_TOPIC));
        assert_eq!(sink.config.qos, 1);
        assert_eq!(sink.max_in_flight, 1);
    }
}
