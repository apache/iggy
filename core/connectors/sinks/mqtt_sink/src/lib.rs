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
use rumqttc_v4::QoS as MqttQos;
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
use uuid::Uuid;

use driver::{MqttDriver, MqttProtocol, PublishWindowProgress, validate_tls_config};
use routing::{
    DEFAULT_TOPIC_TEMPLATE, PreparedPublication, PublicationOptions, TopicMode,
    prepare_batch_topic, prepare_publication_with_batch_topic, validate_topic,
    validate_topic_template,
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
const DEFAULT_MAX_IN_FLIGHT: usize = 10;
const DEFAULT_SESSION_EXPIRY_INTERVAL: u32 = 3600;
const DEFAULT_MAX_PACKET_SIZE: usize = 1024 * 1024;
const MAX_PUBLISH_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const MAX_KEEP_ALIVE: Duration = Duration::from_secs(u16::MAX as u64);
const GENERATED_CLIENT_ID_PREFIX: &str = "iggymqttsink";
const GENERATED_CLIENT_ID_LENGTH: usize = 23;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttSinkConfig {
    pub broker_url: String,
    #[serde(default)]
    pub protocol: MqttProtocol,
    pub client_id: Option<String>,
    pub username: Option<String>,
    // Credentials are kept secret so logging a deserialized config cannot
    // accidentally expose the broker password.
    pub password: Option<SecretString>,
    #[serde(default)]
    pub tls: Option<MqttTlsConfig>,
    #[serde(default = "default_clean_start")]
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
    pub max_packet_size: Option<usize>,
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

fn default_clean_start() -> bool {
    true
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
        ("max_incoming_packet_size_bytes", "max_packet_size"),
        ("max_outgoing_packet_size_bytes", "max_packet_size"),
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
    client_id: String,
    keep_alive: Duration,
    publish_timeout: Duration,
    shutdown_timeout: Duration,
    retry_delay: Duration,
    max_retry_delay: Duration,
    max_retries: u32,
    max_in_flight: usize,
    max_packet_size: usize,
    verbose: bool,
    driver: Mutex<Option<MqttDriver>>,
    publish_lock: Arc<Mutex<()>>,
    messages_published: AtomicU64,
    publish_errors: AtomicU64,
    in_flight_publications: AtomicU64,
}

impl MqttSink {
    pub fn new(id: u32, config: MqttSinkConfig) -> Self {
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let max_in_flight = config.max_in_flight.unwrap_or(DEFAULT_MAX_IN_FLIGHT);
        let max_packet_size = config.max_packet_size.unwrap_or(DEFAULT_MAX_PACKET_SIZE);
        let client_id = config.client_id.clone().unwrap_or_else(generated_client_id);

        Self {
            id,
            verbose: config.verbose_logging.unwrap_or(false),
            config,
            client_id,
            keep_alive: Duration::from_secs(30),
            publish_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(30),
            retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(30),
            max_retries,
            max_in_flight,
            max_packet_size,
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
        validate_max_duration(publish_timeout, MAX_PUBLISH_TIMEOUT, "publish_timeout")?;
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
        if self.max_packet_size == 0 {
            return Err(Error::InvalidConfigValue(
                "max_packet_size must be greater than zero".to_string(),
            ));
        }
        let request_capacity = self
            .config
            .request_capacity
            .unwrap_or(DEFAULT_REQUEST_CAPACITY);
        let max_in_flight = self.config.max_in_flight.unwrap_or(DEFAULT_MAX_IN_FLIGHT);
        if max_in_flight == 0 {
            return Err(Error::InvalidConfigValue(
                "max_in_flight must be greater than zero".to_string(),
            ));
        }
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
            max_in_flight,
        ))
    }

    fn client_id(&self) -> &str {
        &self.client_id
    }

    async fn connect_driver(&self) -> Result<MqttDriver, Error> {
        MqttDriver::connect(
            self.id,
            &self.config,
            self.client_id().to_owned(),
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

    fn mark_window_confirmed(
        confirmed: &mut [bool],
        window_indices: &[usize],
        confirmed_indexes: &[usize],
    ) -> usize {
        let mut newly_confirmed = 0;
        for &local_index in confirmed_indexes {
            let index = window_indices[local_index];
            if !confirmed[index] {
                confirmed[index] = true;
                newly_confirmed += 1;
            }
        }
        newly_confirmed
    }

    fn publish_window_deadline(&self) -> Result<Instant, Error> {
        Instant::now()
            .checked_add(self.publish_timeout)
            .ok_or_else(|| Error::InvalidConfigValue("publish_timeout is too large".to_string()))
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
                PublicationOptions {
                    protocol: self.config.protocol,
                    qos: self.config.qos,
                    retain: self.config.retain,
                },
            ) {
                Ok(publication) => {
                    if publication.packet_size > self.max_packet_size {
                        warn!(
                            "Skipping oversized MQTT record for connector with ID {}: stream: {}, topic: {}, offset: {}, packet_size: {}, max_packet_size: {}",
                            self.id,
                            topic_metadata.stream,
                            topic_metadata.topic,
                            offset,
                            publication.packet_size,
                            self.max_packet_size
                        );
                        self.publish_errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        publications.push(publication);
                    }
                }
                Err(error) => {
                    warn!(
                        "Skipping invalid MQTT record for connector with ID {}: stream: {}, topic: {}, offset: {}: {error}",
                        self.id, topic_metadata.stream, topic_metadata.topic, offset
                    );
                    self.publish_errors.fetch_add(1, Ordering::Relaxed);
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
        let mut skipped = 0_u64;
        self.in_flight_publications
            .fetch_add(publications.len() as u64, Ordering::Relaxed);
        let mut driver = self.driver.lock().await.take();

        let mut connection_retries = 0;
        // Driver recreation restarts window enumeration, so retain the current window's state.
        let mut requeued_window_retries = None;
        'publish: loop {
            let mut active_driver = match driver.take() {
                Some(driver) => driver,
                None => match self.connect_driver().await {
                    Ok(reconnected) => reconnected,
                    Err(Error::Connection(error)) if connection_retries < self.max_retries => {
                        warn!(
                            "Retrying MQTT connection for connector with ID {} (retry {}/{}): {error}",
                            self.id,
                            connection_retries + 1,
                            self.max_retries
                        );
                        let delay = retry_backoff(
                            self.retry_delay,
                            connection_retries + 1,
                            self.max_retry_delay,
                        );
                        tokio::time::sleep(delay).await;
                        connection_retries += 1;
                        continue;
                    }
                    Err(error) => {
                        self.finish_unconfirmed_publications(&confirmed);
                        return Err(error);
                    }
                },
            };
            connection_retries = 0;
            if let Err(error) = active_driver.discard_queued_events().await {
                self.finish_unconfirmed_publications(&confirmed);
                return Err(error);
            }
            let max_packet_size = active_driver.max_packet_size();
            if max_packet_size < self.max_packet_size {
                for (index, publication) in publications.iter().enumerate() {
                    if confirmed[index] || publication.packet_size <= max_packet_size {
                        continue;
                    }
                    warn!(
                        "Skipping oversized MQTT record for connector with ID {}: stream: {}, topic: {}, offset: {}, packet_size: {}, max_packet_size: {}",
                        self.id,
                        topic_metadata.stream,
                        topic_metadata.topic,
                        publication.offset,
                        publication.packet_size,
                        max_packet_size
                    );
                    confirmed[index] = true;
                    skipped += 1;
                    self.in_flight_publications.fetch_sub(1, Ordering::Relaxed);
                    self.publish_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            let pending_indices = unconfirmed_publication_indexes(&confirmed);
            for (window_number, window_indices) in
                pending_indices.chunks(self.max_in_flight).enumerate()
            {
                let mut window_retries = if window_number == 0 {
                    requeued_window_retries.take().unwrap_or(0)
                } else {
                    0
                };
                let window: Vec<_> = window_indices
                    .iter()
                    .map(|&index| &publications[index])
                    .collect();
                let mut window_state = active_driver.new_publish_window(window.len(), qos);
                let initial_indexes: Vec<_> = (0..window.len()).collect();
                if let Err(error) = active_driver
                    .enqueue_window(
                        &window,
                        &initial_indexes,
                        qos,
                        self.config.retain,
                        &mut window_state,
                    )
                    .await
                {
                    self.finish_unconfirmed_publications(&confirmed);
                    return Err(error);
                }
                loop {
                    let window_deadline = match self.publish_window_deadline() {
                        Ok(deadline) => deadline,
                        Err(error) => {
                            self.finish_unconfirmed_publications(&confirmed);
                            return Err(error);
                        }
                    };
                    match active_driver
                        .drive_window(&mut window_state, window_deadline)
                        .await
                    {
                        Ok(PublishWindowProgress::Complete) => {
                            for &index in window_indices {
                                if !confirmed[index] {
                                    confirmed[index] = true;
                                    self.in_flight_publications.fetch_sub(1, Ordering::Relaxed);
                                }
                            }
                            break;
                        }
                        // rumqttc retains unacknowledged requests only when the broker resumes
                        // the previous session. A fresh session clears its pending queue.
                        Ok(PublishWindowProgress::SessionPresent(true)) => {
                            if window_retries >= self.max_retries {
                                self.finish_unconfirmed_publications(&confirmed);
                                return Err(Error::Connection(
                                    "MQTT reconnect retry limit reached before unconfirmed publications were acknowledged".to_string(),
                                ));
                            }
                            window_retries += 1;
                            debug!(
                                "MQTT reconnect for connector with ID {} resumed the previous session; keeping sink packet mappings for rumqttc retransmission (retry {}/{})",
                                self.id, window_retries, self.max_retries,
                            );
                        }
                        Ok(PublishWindowProgress::SessionPresent(false)) => {
                            let confirmed_indexes = window_state.confirmed_indexes();
                            let newly_confirmed = Self::mark_window_confirmed(
                                &mut confirmed,
                                window_indices,
                                &confirmed_indexes,
                            );
                            self.in_flight_publications
                                .fetch_sub(newly_confirmed as u64, Ordering::Relaxed);
                            let unconfirmed = window.len() - confirmed_indexes.len();
                            if window_retries >= self.max_retries {
                                self.finish_unconfirmed_publications(&confirmed);
                                return Err(Error::Connection(
                                    "MQTT session was not resumed before unconfirmed publications could be requeued".to_string(),
                                ));
                            }
                            warn!(
                                "MQTT reconnect for connector with ID {} did not resume the previous session; recreating the driver and requeuing {unconfirmed} unconfirmed publications (retry {}/{})",
                                self.id,
                                window_retries + 1,
                                self.max_retries,
                            );
                            let delay = retry_backoff(
                                self.retry_delay,
                                window_retries + 1,
                                self.max_retry_delay,
                            );
                            tokio::time::sleep(delay).await;
                            window_retries += 1;
                            requeued_window_retries = Some(window_retries);
                            // Leaving this scope drops the driver and its stale event loop.
                            continue 'publish;
                        }
                        Err(window_error) => {
                            let confirmed_indexes = window_state.confirmed_indexes();
                            let newly_confirmed = Self::mark_window_confirmed(
                                &mut confirmed,
                                window_indices,
                                &confirmed_indexes,
                            );
                            self.in_flight_publications
                                .fetch_sub(newly_confirmed as u64, Ordering::Relaxed);
                            if !window_error.retryable || window_retries >= self.max_retries {
                                self.finish_unconfirmed_publications(&confirmed);
                                return Err(window_error.error);
                            }
                            if window_error.requires_requeue {
                                // Recreate the driver before requeuing because the failed window
                                // leaves packet mappings and acknowledgement events uncertain. A
                                // fresh driver prevents stale MQTT events from confirming a retried
                                // publication.
                                warn!(
                                    "Retrying MQTT publication for connector with ID {} with a new driver (retry {}/{}): {}",
                                    self.id,
                                    window_retries + 1,
                                    self.max_retries,
                                    window_error.error
                                );
                                let delay = retry_backoff(
                                    self.retry_delay,
                                    window_retries + 1,
                                    self.max_retry_delay,
                                );
                                tokio::time::sleep(delay).await;
                                window_retries += 1;
                                requeued_window_retries = Some(window_retries);
                                continue 'publish;
                            }
                            if window_error.requires_clean
                                && let Err(error) = active_driver.clean().await
                            {
                                self.finish_unconfirmed_publications(&confirmed);
                                return Err(error);
                            }
                            warn!(
                                "Retrying MQTT publication for connector with ID {} (retry {}/{}): {}",
                                self.id,
                                window_retries + 1,
                                self.max_retries,
                                window_error.error
                            );
                            let delay = retry_backoff(
                                self.retry_delay,
                                window_retries + 1,
                                self.max_retry_delay,
                            );
                            tokio::time::sleep(delay).await;
                            window_retries += 1;
                        }
                    }
                }
            }
            *self.driver.lock().await = Some(active_driver);
            return Ok(confirmed.iter().filter(|confirmed| **confirmed).count() as u64 - skipped);
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
        ) = match self.validate_config() {
            Ok(values) => values,
            Err(error) => {
                error!(
                    "Failed to validate MQTT sink connector with ID {}: {error}",
                    self.id
                );
                return Err(error);
            }
        };
        self.keep_alive = keep_alive;
        self.publish_timeout = publish_timeout;
        self.shutdown_timeout = shutdown_timeout;
        self.retry_delay = retry_delay;
        self.max_retry_delay = max_retry_delay;
        self.max_in_flight = max_in_flight;
        let qos = match self.qos() {
            Ok(qos) => qos,
            Err(error) => {
                error!(
                    "Failed to validate MQTT sink connector with ID {}: {error}",
                    self.id
                );
                return Err(error);
            }
        };
        // Connection establishment waits for CONNACK, so a successful open
        // means the broker accepted the session rather than only queueing a
        // local rumqttc request.
        let mut retries = 0;
        let driver = loop {
            match self.connect_driver().await {
                Ok(driver) => break driver,
                Err(Error::Connection(error)) if retries < self.max_retries => {
                    warn!(
                        "Retrying MQTT connection for connector with ID {} (retry {}/{}): {error}",
                        self.id,
                        retries + 1,
                        self.max_retries
                    );
                    let delay = retry_backoff(self.retry_delay, retries + 1, self.max_retry_delay);
                    tokio::time::sleep(delay).await;
                    retries += 1;
                }
                Err(error) => {
                    error!(
                        "Failed to open MQTT sink connector with ID {}: {error}",
                        self.id
                    );
                    return Err(error);
                }
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

fn generated_client_id() -> String {
    let suffix = Uuid::new_v4().simple().to_string();
    let suffix_length = GENERATED_CLIENT_ID_LENGTH - GENERATED_CLIENT_ID_PREFIX.len();
    format!("{GENERATED_CLIENT_ID_PREFIX}{}", &suffix[..suffix_length])
}

fn unconfirmed_publication_indexes(confirmed: &[bool]) -> Vec<usize> {
    confirmed
        .iter()
        .enumerate()
        .filter_map(|(index, is_confirmed)| (!is_confirmed).then_some(index))
        .collect()
}

fn parse_duration(value: Option<&str>, default: &str, field: &str) -> Result<Duration, Error> {
    let value = value.unwrap_or(default);
    HumanDuration::from_str(value)
        .map(|duration| *duration)
        .map_err(|error| Error::InvalidConfigValue(format!("{field}: {error}")))
}

fn validate_max_duration(duration: Duration, maximum: Duration, field: &str) -> Result<(), Error> {
    if duration > maximum {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must not exceed {}s",
            maximum.as_secs()
        )));
    }
    Ok(())
}

fn validate_keep_alive(
    protocol: MqttProtocol,
    keep_alive: Duration,
    field: &str,
) -> Result<(), Error> {
    if keep_alive > MAX_KEEP_ALIVE {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must not exceed {}s",
            MAX_KEEP_ALIVE.as_secs()
        )));
    }
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
            max_packet_size: None,
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
    fn given_no_max_in_flight_when_new_should_use_pipelining_default() {
        let mut config = test_config();
        config.max_in_flight = None;
        let sink = MqttSink::new(1, config);
        assert_eq!(sink.config.qos, 1);
        assert_eq!(sink.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
    }

    #[test]
    fn given_no_max_retries_should_use_default() {
        let mut config = test_config();
        config.max_retries = None;
        let sink = MqttSink::new(1, config);

        assert_eq!(sink.max_retries, DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn given_zero_max_retries_should_not_retry_after_initial_attempt() {
        let mut config = test_config();
        config.max_retries = Some(0);
        let sink = MqttSink::new(1, config);

        assert_eq!(sink.max_retries, 0);
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
    fn given_keep_alive_above_rumqttc_limit_should_reject_configuration() {
        let mut config = test_config();
        config.keep_alive = Some("65536s".to_string());
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message == "keep_alive must not exceed 65535s"
        ));
    }

    #[test]
    fn given_unknown_sink_config_field_should_reject_configuration() {
        let error = serde_json::from_str::<MqttSinkConfig>(
            r#"{"broker_url":"mqtt://localhost:1883","clean_session":true}"#,
        )
        .expect_err("unknown fields must reject the configuration");

        assert!(error.to_string().contains("unknown field `clean_session`"));
    }

    #[test]
    fn given_publish_timeout_above_one_hour_when_validating_should_return_configuration_error() {
        let mut config = test_config();
        config.publish_timeout = Some("1h1s".to_string());
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message))
                if message == "publish_timeout must not exceed 3600s"
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
    fn given_zero_max_in_flight_when_validating_should_return_configuration_error() {
        let mut config = test_config();
        config.max_in_flight = Some(0);
        let sink = MqttSink::new(1, config);

        assert_eq!(sink.max_in_flight, 0);
        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message.contains("max_in_flight")
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
    fn given_reserved_static_topic_when_validating_should_return_configuration_error() {
        let mut config = test_config();
        config.topic = Some("$SYS/broker/uptime".to_string());
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message.contains("must not start")
        ));
    }

    #[test]
    fn given_reserved_fallback_topic_when_validating_should_return_configuration_error() {
        let mut config = test_config();
        config.fallback_topic = Some("$SYS/{stream}/{topic}".to_string());
        let sink = MqttSink::new(1, config);

        assert!(matches!(
            sink.validate_config(),
            Err(Error::InvalidConfigValue(message)) if message.contains("must not start")
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
            ("max_incoming_packet_size_bytes", "max_packet_size"),
            ("max_outgoing_packet_size_bytes", "max_packet_size"),
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
    fn given_clean_start_without_client_id_when_constructing_should_generate_short_client_id() {
        let mut config = test_config();
        config.client_id = None;
        config.clean_start = true;
        let sink = MqttSink::new(42, config);
        let mut other_config = test_config();
        other_config.client_id = None;
        other_config.clean_start = true;
        let other_sink = MqttSink::new(42, other_config);

        assert_eq!(sink.client_id().len(), GENERATED_CLIENT_ID_LENGTH);
        assert!(
            sink.client_id()
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        );
        assert!(sink.client_id().starts_with(GENERATED_CLIENT_ID_PREFIX));
        assert_ne!(sink.client_id(), other_sink.client_id());
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
    fn given_session_reset_with_partially_confirmed_window_should_requeue_only_unconfirmed() {
        let mut confirmed = vec![false; 3];
        let window_indices = [0, 1, 2];

        let newly_confirmed =
            MqttSink::mark_window_confirmed(&mut confirmed, &window_indices, &[1]);

        assert_eq!(newly_confirmed, 1);
        assert_eq!(unconfirmed_publication_indexes(&confirmed), vec![0, 2]);
    }

    #[test]
    fn given_unreachable_broker_when_reconnecting_should_stop_at_max_retries() {
        let mut config = test_config();
        config.broker_url = "mqtt://127.0.0.1:1".to_string();
        config.max_retries = Some(2);
        let mut sink = MqttSink::new(1, config);
        sink.publish_timeout = Duration::from_millis(100);
        sink.retry_delay = Duration::from_millis(1);
        sink.max_retry_delay = Duration::from_millis(1);
        let metadata = TopicMetadata {
            stream: "devices".to_string(),
            topic: "telemetry".to_string(),
        };
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");

        let error = runtime.block_on(async {
            timeout(
                Duration::from_secs(1),
                sink.publish_messages(&metadata, test_messages()),
            )
            .await
            .expect("reconnect attempts must finish")
            .expect_err("unreachable broker must return an error")
        });

        assert!(matches!(error, Error::Connection(_)));
        assert_eq!(sink.in_flight_publications.load(Ordering::Relaxed), 0);
        assert_eq!(sink.publish_errors.load(Ordering::Relaxed), 2);
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
        assert_eq!(
            publications[0].packet_size,
            driver::publication_packet_size(
                &publications[0],
                sink.config.protocol,
                sink.config.qos,
                sink.config.retain,
            )
        );
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
    fn given_malformed_dynamic_record_when_preparing_should_skip_record_and_keep_valid_publications()
     {
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
            ConsumedMessage {
                id: 3,
                offset: 12,
                checksum: 1,
                timestamp: 1,
                origin_timestamp: 1,
                headers: None,
                payload: Payload::Raw(b"also-valid".to_vec()),
            },
        ];

        let publications = sink
            .prepare_publications(&metadata, messages)
            .expect("malformed record must not reject valid records");

        assert_eq!(publications.len(), 2);
        assert_eq!(publications[0].payload.as_ref(), b"valid");
        assert_eq!(publications[1].payload.as_ref(), b"also-valid");
        assert_eq!(sink.publish_errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn given_oversized_record_when_preparing_should_skip_record_and_keep_valid_publications() {
        let mut config = test_config();
        config.max_packet_size = Some(32);
        let sink = MqttSink::new(1, config);
        let metadata = TopicMetadata {
            stream: "devices".to_string(),
            topic: "telemetry".to_string(),
        };
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
                payload: Payload::Raw(vec![b'x'; 64]),
            },
        ];

        let publications = sink
            .prepare_publications(&metadata, messages)
            .expect("oversized record must not reject valid records");

        assert_eq!(publications.len(), 1);
        assert_eq!(publications[0].payload.as_ref(), b"valid");
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
        assert!(sink.config.clean_start);
        assert_eq!(sink.config.qos, 1);
        assert_eq!(sink.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
        assert_eq!(sink.shutdown_timeout, Duration::from_secs(30));
    }
}
