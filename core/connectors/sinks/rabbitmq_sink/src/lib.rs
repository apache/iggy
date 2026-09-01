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
use iggy::prelude::HeaderKind;
use iggy_connector_sdk::retry::{exponential_backoff, jitter};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{ConfirmSelectOptions, ExchangeDeclareOptions},
    publisher_confirm::{Confirmation, PublisherConfirm},
    types::{AMQPValue, ByteArray, FieldTable, ShortString},
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{debug, info, warn};

sink_connector!(RabbitMQSink);

#[derive(Debug)]
struct RabbitMqState {
    connection: Connection,
    channel: Channel,
}

#[derive(Debug)]
pub struct RabbitMQSink {
    id: u32,
    amqp_url: SecretString,
    exchange: String,
    exchange_type: String,
    routing_key: String,
    include_metadata: bool,
    verbose: bool,
    durable_exchange: bool,
    delivery_mode: u8,
    timeout: Duration,
    state: Mutex<Option<RabbitMqState>>,
    reconnecting: AtomicBool,
    max_retries: u32,
    retry_delay: Duration,
    max_retry_delay: Duration,
    messages_published: AtomicU64,
    publish_errors: AtomicU64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RabbitMQSinkConfig {
    #[serde(
        default = "default_amqp_url",
        serialize_with = "iggy_common::serde_secret::serialize_secret"
    )]
    amqp_url: SecretString,
    #[serde(default)]
    exchange: Option<String>,
    #[serde(default = "default_exchange_type")]
    exchange_type: Option<String>,
    #[serde(default)]
    routing_key: Option<String>,
    #[serde(default = "default_true")]
    include_metadata: Option<bool>,
    #[serde(default)]
    verbose_logging: Option<bool>,
    #[serde(default = "default_max_retries")]
    max_retries: Option<u32>,
    #[serde(default = "default_retry_delay_secs")]
    retry_delay_secs: Option<u64>,
    #[serde(default = "default_max_retry_delay_secs")]
    max_retry_delay_secs: Option<u64>,
    #[serde(default = "default_true")]
    durable_exchange: Option<bool>,
    #[serde(default = "default_delivery_mode")]
    delivery_mode: Option<String>,
    #[serde(default = "default_timeout_secs")]
    timeout_secs: Option<u64>,
}

fn default_exchange_type() -> Option<String> {
    Some("topic".into())
}

fn default_amqp_url() -> SecretString {
    SecretString::from("amqp://guest:guest@localhost:5672")
}

fn default_delivery_mode() -> Option<String> {
    Some("persistent".into())
}

fn default_timeout_secs() -> Option<u64> {
    Some(30)
}

fn default_true() -> Option<bool> {
    Some(true)
}

fn default_max_retries() -> Option<u32> {
    Some(3)
}
fn default_retry_delay_secs() -> Option<u64> {
    Some(1)
}
fn default_max_retry_delay_secs() -> Option<u64> {
    Some(5)
}

impl RabbitMQSink {
    pub fn new(id: u32, config: RabbitMQSinkConfig) -> Self {
        let delivery_mode = match config.delivery_mode.as_deref() {
            Some("non_persistent") => 1,
            Some("persistent") => 2,
            Some(other) => {
                warn!(
                    "Unknown delivery_mode: {other}, defaulting to persistent for connector ID: {id}"
                );
                2
            }
            None => 2,
        };
        RabbitMQSink {
            id,
            amqp_url: config.amqp_url,
            exchange: config.exchange.unwrap_or_else(|| "iggy_events".into()),
            exchange_type: config.exchange_type.unwrap_or_else(|| "topic".into()),
            routing_key: config.routing_key.unwrap_or_else(|| "iggy.messages".into()),
            include_metadata: config.include_metadata.unwrap_or(true),
            verbose: config.verbose_logging.unwrap_or(false),
            durable_exchange: config.durable_exchange.unwrap_or(true),
            delivery_mode,
            timeout: Duration::from_secs(config.timeout_secs.unwrap_or(30)),
            state: Mutex::new(None),
            reconnecting: AtomicBool::new(false),
            max_retries: config.max_retries.unwrap_or(3),
            retry_delay: Duration::from_secs(config.retry_delay_secs.unwrap_or(1)),
            max_retry_delay: Duration::from_secs(config.max_retry_delay_secs.unwrap_or(5)),
            messages_published: AtomicU64::new(0),
            publish_errors: AtomicU64::new(0),
        }
    }

    fn exchange_kind(&self) -> Result<ExchangeKind, Error> {
        match self.exchange_type.as_str() {
            "direct" => Ok(ExchangeKind::Direct),
            "topic" => Ok(ExchangeKind::Topic),
            "fanout" => Ok(ExchangeKind::Fanout),
            "headers" => Ok(ExchangeKind::Headers),
            other => Err(Error::InvalidConfigValue(format!(
                "unknown exchange_type: {other}. Valid: direct, topic, fanout, headers"
            ))),
        }
    }

    async fn publish_batch_with_retry(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<u64, Error> {
        let mut attempts = 0u32;
        let mut confirmed: usize = 0;

        loop {
            let channel = {
                let guard = self.state.lock().await;
                match guard.as_ref() {
                    Some(state) => state.channel.clone(),
                    None => {
                        drop(guard);
                        self.reconnect().await?;
                        self.state
                            .lock()
                            .await
                            .as_ref()
                            .map(|s| s.channel.clone())
                            .ok_or_else(|| Error::Connection("RabbitMQ not connected".into()))?
                    }
                }
            };

            let mut last_error: Option<Error> = None;
            let mut last_retryable = false;
            let mut in_flight: Vec<(PublisherConfirm, u64)> =
                Vec::with_capacity(messages.len() - confirmed);
            for message in &messages[confirmed..] {
                let body = message.payload.try_to_bytes()?;
                let mut props = BasicProperties::default()
                    .with_delivery_mode(self.delivery_mode)
                    .with_message_id(ShortString::from(message.offset.to_string()));
                let headers = self.build_headers(topic_metadata, messages_metadata, message);
                if !headers.inner().is_empty() {
                    props = props.with_headers(headers);
                }

                let confirm = match timeout(
                    self.timeout,
                    channel.basic_publish(
                        &self.exchange,
                        &self.routing_key,
                        lapin::options::BasicPublishOptions {
                            mandatory: true,
                            ..Default::default()
                        },
                        &body,
                        props,
                    ),
                )
                .await
                {
                    Ok(Ok(confirm)) => confirm,
                    Ok(Err(e)) => {
                        last_error = Some(Error::CannotStoreData(e.to_string()));
                        last_retryable = is_lapin_error_retryable(&e);
                        self.clear_state().await;
                        break;
                    }
                    Err(_) => {
                        last_error = Some(Error::CannotStoreData("publish timed out".into()));
                        last_retryable = true;
                        break;
                    }
                };
                in_flight.push((confirm, message.offset));
            }

            // Confirms resolve in publish order over already-overlapped RTTs; returns are
            // matched by message_id (lapin's Return-to-confirm FIFO has no delivery tag), so a
            // post-publish failure re-publishes the already-delivered tail (at-least-once in batch).
            for (confirm, offset) in in_flight {
                match timeout(self.timeout, confirm).await {
                    Ok(Ok(Confirmation::Ack(None))) => confirmed += 1,
                    Ok(Ok(Confirmation::Ack(Some(returned)))) => {
                        let returned_id = returned.delivery.properties.message_id();
                        last_error = Some(Error::InvalidRecordValue(format!(
                            "message offset {offset} (id {returned_id:?}) returned as unroutable by RabbitMQ"
                        )));
                        last_retryable = false;
                        break;
                    }
                    Ok(Ok(Confirmation::Nack(_))) => {
                        last_error =
                            Some(Error::CannotStoreData("message nack'd by RabbitMQ".into()));
                        last_retryable = true;
                        break;
                    }
                    Ok(Ok(Confirmation::NotRequested)) => {
                        last_error = Some(Error::CannotStoreData(
                            "publisher confirms not enabled".into(),
                        ));
                        last_retryable = false;
                        break;
                    }
                    Ok(Err(e)) => {
                        last_error = Some(Error::CannotStoreData(format!("publish rejected: {e}")));
                        last_retryable = is_lapin_error_retryable(&e);
                        self.clear_state().await;
                        break;
                    }
                    Err(_) => {
                        last_error = Some(Error::CannotStoreData(
                            "publisher confirmation timed out".into(),
                        ));
                        last_retryable = true;
                        break;
                    }
                }
            }

            if last_error.is_none() {
                return Ok(confirmed as u64);
            }

            let error = last_error.unwrap();
            attempts += 1;

            if !last_retryable || attempts >= self.max_retries {
                self.publish_errors
                    .fetch_add((messages.len() - confirmed) as u64, Ordering::Relaxed);
                return Err(Error::CannotStoreData(format!(
                    "batch publish failed after {attempts} attempts: {error}"
                )));
            }

            match self.reconnect().await {
                Ok(_) => {}
                Err(reconnect_error) => {
                    self.publish_errors
                        .fetch_add((messages.len() - confirmed) as u64, Ordering::Relaxed);
                    return Err(Error::Connection(format!(
                        "failed to reconnect: {reconnect_error}"
                    )));
                }
            }

            let delay = jitter(exponential_backoff(
                self.retry_delay,
                attempts.saturating_sub(1),
                self.max_retry_delay,
            ));
            warn!(
                "Transient RabbitMQ publish error for connector ID: {} (attempt {attempts}/{}): {error}. Retrying in {:?}.",
                self.id, self.max_retries, delay
            );
            tokio::time::sleep(delay).await;
        }
    }

    async fn clear_state(&self) {
        *self.state.lock().await = None;
    }

    fn build_headers(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        message: &ConsumedMessage,
    ) -> FieldTable {
        let mut headers = FieldTable::default();
        if let Some(user_headers) = &message.headers
            && !user_headers.is_empty()
        {
            for (key, value) in user_headers {
                let name = ShortString::from(key.to_string_value());
                let amqp_value = match value.kind() {
                    HeaderKind::String => AMQPValue::LongString(value.to_string_value().into()),
                    _ => AMQPValue::ByteArray(ByteArray::from(value.as_bytes())),
                };
                headers.insert(name, amqp_value);
            }
        }
        if self.include_metadata {
            headers.insert(
                "iggy_stream".into(),
                AMQPValue::LongString(topic_metadata.stream.clone().into()),
            );
            headers.insert(
                "iggy_topic".into(),
                AMQPValue::LongString(topic_metadata.topic.clone().into()),
            );
            headers.insert(
                "iggy_partition_id".into(),
                AMQPValue::LongUInt(messages_metadata.partition_id),
            );
            headers.insert(
                "iggy_offset".into(),
                AMQPValue::LongLongInt(message.offset as i64),
            );
        }
        headers
    }

    async fn reconnect(&self) -> Result<(), Error> {
        if self
            .reconnecting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tokio::time::sleep(self.retry_delay).await;
            return Ok(());
        }

        warn!("Reconnecting RabbitMQ sink ID: {}", self.id);
        let result = async {
            let conn = timeout(
                self.timeout,
                Connection::connect(
                    self.amqp_url.expose_secret(),
                    ConnectionProperties::default(),
                ),
            )
            .await
            .map_err(|_| Error::Connection("connection timed out".into()))?
            .map_err(|e| Error::Connection(e.to_string()))?;
            let channel = conn
                .create_channel()
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            let exchange_kind = self.exchange_kind()?;
            channel
                .exchange_declare(
                    &self.exchange,
                    exchange_kind,
                    ExchangeDeclareOptions {
                        durable: self.durable_exchange,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            *self.state.lock().await = Some(RabbitMqState {
                connection: conn,
                channel,
            });
            Ok::<(), Error>(())
        }
        .await;
        self.reconnecting.store(false, Ordering::Release);
        result
    }
}

#[async_trait]
impl Sink for RabbitMQSink {
    async fn open(&mut self) -> Result<(), Error> {
        let exchange_kind = self.exchange_kind()?;
        let conn = timeout(
            self.timeout,
            Connection::connect(
                self.amqp_url.expose_secret(),
                ConnectionProperties::default(),
            ),
        )
        .await
        .map_err(|_| Error::Connection("connection timed out".into()))?
        .map_err(|e| Error::Connection(e.to_string()))?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        channel
            .confirm_select(lapin::options::ConfirmSelectOptions::default())
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        channel
            .exchange_declare(
                &self.exchange,
                exchange_kind,
                ExchangeDeclareOptions {
                    durable: self.durable_exchange,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        *self.state.get_mut() = Some(RabbitMqState {
            connection: conn,
            channel,
        });
        info!(
            "Opened RabbitMQ sink ID: {}, connected to exchange: {}",
            self.id, self.exchange
        );

        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let published = self
            .publish_batch_with_retry(topic_metadata, &messages_metadata, &messages)
            .await?;
        self.messages_published
            .fetch_add(published, Ordering::Relaxed);
        if self.verbose {
            info!(
                "Published {published} messages to exchange: {}",
                self.exchange
            );
        } else {
            debug!(
                "Published {published} messages to exchange: {}",
                self.exchange
            );
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let published = self.messages_published.load(Ordering::Relaxed);
        let errors = self.publish_errors.load(Ordering::Relaxed);
        info!(
            "RabbitMQ sink ID: {} processed {} messages with {} errors",
            self.id, published, errors
        );

        if let Some(state) = self.state.get_mut().take() {
            state
                .channel
                .close(200, "OK")
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            state
                .connection
                .close(200, "OK")
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
        }
        Ok(())
    }
}

fn is_lapin_error_retryable(error: &lapin::Error) -> bool {
    match error {
        lapin::Error::InvalidChannelState(_)
        | lapin::Error::InvalidConnectionState(_)
        | lapin::Error::IOError(_)
        | lapin::Error::MissingHeartbeatError => true,
        // AMQP soft channel errors: 405 RESOURCE_LOCKED, 320 CONNECTION_FORCED.
        lapin::Error::ProtocolError(amqp_error) => matches!(amqp_error.get_id(), 320 | 405),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::{HeaderKey, HeaderValue};
    use iggy_connector_sdk::{Payload, Schema};
    use std::str::FromStr;

    fn test_config() -> RabbitMQSinkConfig {
        RabbitMQSinkConfig {
            amqp_url: SecretString::from("amqp://guest:guest@localhost:5672"),
            exchange: None,
            exchange_type: None,
            routing_key: None,
            include_metadata: Some(true),
            verbose_logging: Some(false),
            max_retries: Some(3),
            retry_delay_secs: Some(1),
            max_retry_delay_secs: Some(5),
            durable_exchange: None,
            delivery_mode: None,
            timeout_secs: None,
        }
    }

    fn test_sink(config: RabbitMQSinkConfig) -> RabbitMQSink {
        RabbitMQSink::new(1, config)
    }

    fn test_message(offset: u64) -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Text("payload".into()),
        }
    }

    #[test]
    fn given_offset_above_u32_max_when_build_headers_should_encode_full_value() {
        let sink = test_sink(test_config());
        let topic = TopicMetadata {
            stream: "test_stream".into(),
            topic: "test_topic".into(),
        };
        let metadata = MessagesMetadata {
            partition_id: 0,
            current_offset: 0,
            schema: Schema::Json,
        };
        let headers = sink.build_headers(&topic, &metadata, &test_message(u32::MAX as u64 + 1));
        match headers.inner().get(&ShortString::from("iggy_offset")) {
            Some(AMQPValue::LongLongInt(value)) => assert_eq!(*value, u32::MAX as i64 + 1),
            other => panic!("expected LongLongInt, got {other:?}"),
        }
    }

    #[test]
    fn given_user_headers_when_build_headers_should_preserve_string_and_binary() {
        let mut user_headers = std::collections::BTreeMap::new();
        user_headers.insert(
            HeaderKey::from_str("content-type").unwrap(),
            HeaderValue::from_str("application/json").unwrap(),
        );
        user_headers.insert(
            HeaderKey::from_str("trace-id").unwrap(),
            HeaderValue::try_from(vec![0xDE, 0xAD, 0xBE, 0xEF]).unwrap(),
        );
        let mut message = test_message(0);
        message.headers = Some(user_headers);

        let sink = test_sink(test_config());
        let topic = TopicMetadata {
            stream: "test_stream".into(),
            topic: "test_topic".into(),
        };
        let metadata = MessagesMetadata {
            partition_id: 0,
            current_offset: 0,
            schema: Schema::Json,
        };
        let headers = sink.build_headers(&topic, &metadata, &message);
        let inner = headers.inner();
        assert!(matches!(
            inner.get(&ShortString::from("content-type")),
            Some(AMQPValue::LongString(s)) if s.as_bytes() == b"application/json"
        ));
        assert!(matches!(
            inner.get(&ShortString::from("trace-id")),
            Some(AMQPValue::ByteArray(b)) if b.as_slice() == [0xDE, 0xAD, 0xBE, 0xEF]
        ));
    }

    #[test]
    fn given_persistent_delivery_mode_when_new_should_set_delivery_mode_2() {
        let sink = test_sink(test_config());
        assert_eq!(sink.delivery_mode, 2);
    }

    #[test]
    fn given_non_persistent_delivery_mode_when_new_should_set_delivery_mode_1() {
        let mut config = test_config();
        config.delivery_mode = Some("non_persistent".into());
        let sink = test_sink(config);
        assert_eq!(sink.delivery_mode, 1);
    }

    #[test]
    fn given_durable_exchange_when_new_should_default_to_true() {
        let sink = test_sink(test_config());
        assert!(sink.durable_exchange);
    }

    #[test]
    fn given_known_exchange_type_when_exchange_kind_should_map_to_lapin_kind() {
        let sink = test_sink(test_config());
        assert!(matches!(sink.exchange_kind(), Ok(ExchangeKind::Topic)));
    }

    #[test]
    fn given_unknown_exchange_type_when_exchange_kind_should_error() {
        let mut config = test_config();
        config.exchange_type = Some("mystery".into());
        let sink = test_sink(config);
        assert!(matches!(
            sink.exchange_kind(),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_transient_lapin_error_when_is_lapin_error_retryable_should_return_true() {
        let errors = [
            lapin::Error::InvalidChannelState(lapin::ChannelState::Closed),
            lapin::Error::InvalidConnectionState(lapin::ConnectionState::Error),
            lapin::Error::IOError(std::sync::Arc::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset by peer",
            ))),
            lapin::Error::MissingHeartbeatError,
            lapin::Error::ProtocolError(lapin::protocol::AMQPError::new(
                lapin::protocol::AMQPErrorKind::Soft(
                    lapin::protocol::AMQPSoftError::RESOURCELOCKED,
                ),
                "RESOURCE_LOCKED".into(),
            )),
        ];
        for error in errors {
            assert!(
                is_lapin_error_retryable(&error),
                "expected {error:?} to be retryable"
            );
        }
    }

    #[test]
    fn given_permanent_lapin_error_when_is_lapin_error_retryable_should_return_false() {
        let errors = [
            lapin::Error::ProtocolError(lapin::protocol::AMQPError::new(
                lapin::protocol::AMQPErrorKind::Soft(
                    lapin::protocol::AMQPSoftError::PRECONDITIONFAILED,
                ),
                "PRECONDITION_FAILED".into(),
            )),
            lapin::Error::InvalidChannel(0),
        ];
        for error in errors {
            assert!(
                !is_lapin_error_retryable(&error),
                "expected {error:?} to be permanent"
            );
        }
    }

    #[test]
    fn given_timeout_secs_when_new_should_set_timeout() {
        let mut config = test_config();
        config.timeout_secs = Some(7);
        let sink = test_sink(config);
        assert_eq!(sink.timeout, Duration::from_secs(7));
    }

    #[test]
    fn given_minimal_config_when_deserialized_should_apply_defaults() {
        let config: RabbitMQSinkConfig = serde_json::from_str("{}").unwrap();
        let sink = RabbitMQSink::new(1, config);
        assert_eq!(
            sink.amqp_url.expose_secret(),
            "amqp://guest:guest@localhost:5672"
        );
        assert_eq!(sink.exchange, "iggy_events");
        assert_eq!(sink.exchange_type, "topic");
        assert_eq!(sink.routing_key, "iggy.messages");
        assert!(sink.include_metadata);
        assert!(sink.durable_exchange);
        assert_eq!(sink.delivery_mode, 2);
        assert_eq!(sink.max_retries, 3);
        assert_eq!(sink.retry_delay, Duration::from_secs(1));
        assert_eq!(sink.max_retry_delay, Duration::from_secs(5));
        assert_eq!(sink.timeout, Duration::from_secs(30));
    }

    #[test]
    fn given_unknown_delivery_mode_when_new_should_default_to_persistent() {
        let mut config = test_config();
        config.delivery_mode = Some("fancy".into());
        let sink = test_sink(config);
        assert_eq!(sink.delivery_mode, 2);
    }
}
