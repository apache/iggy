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
use iggy_connector_sdk::retry::{exponential_backoff, jitter};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{ConfirmSelectOptions, ExchangeDeclareOptions},
    types::AMQPValue,
    types::FieldTable,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
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
    amqp_url: String,
    exchange: String,
    exchange_type: String,
    routing_key: String,
    include_metadata: bool,
    verbose: bool,
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
    #[serde(default = "default_amqp_url")]
    amqp_url: String,
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
}

fn default_exchange_type() -> Option<String> {
    Some("topic".into())
}

fn default_amqp_url() -> String {
    "amqp://guest:guest@localhost:5672".into()
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
        RabbitMQSink {
            id,
            amqp_url: config.amqp_url,
            exchange: config.exchange.unwrap_or_else(|| "iggy_events".into()),
            exchange_type: config.exchange_type.unwrap_or_else(|| "topic".into()),
            routing_key: config.routing_key.unwrap_or_else(|| "iggy.messages".into()),
            include_metadata: config.include_metadata.unwrap_or(true),
            verbose: config.verbose_logging.unwrap_or(false),
            state: Mutex::new(None),
            reconnecting: AtomicBool::new(false),
            max_retries: config.max_retries.unwrap_or(3),
            retry_delay: Duration::from_secs(config.retry_delay_secs.unwrap_or(1)),
            max_retry_delay: Duration::from_secs(config.max_retry_delay_secs.unwrap_or(5)),
            messages_published: AtomicU64::new(0),
            publish_errors: AtomicU64::new(0),
        }
    }

    async fn publish_batch_with_retry(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<u64, Error> {
        let mut attempts = 0u32;

        loop {
            let channel = {
                let guard = self.state.lock().await;
                guard
                    .as_ref()
                    .map(|s| s.channel.clone())
                    .ok_or_else(|| Error::Connection("RabbitMQ not connected".into()))?
            };

            let mut last_error: Option<Error> = None;
            let mut published: u64 = 0;
            for message in messages {
                let body = message.payload.clone().try_into_vec()?;
                let mut props = BasicProperties::default();
                if self.include_metadata {
                    let mut headers = FieldTable::default();
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
                        AMQPValue::LongUInt(message.offset.try_into().unwrap_or(u32::MAX)),
                    );
                    props = props.with_headers(headers);
                }

                let confirm = channel
                    .basic_publish(
                        &self.exchange,
                        &self.routing_key,
                        lapin::options::BasicPublishOptions {
                            mandatory: true,
                            ..Default::default()
                        },
                        &body,
                        props,
                    )
                    .await
                    .map_err(|e| Error::CannotStoreData(e.to_string()))?;
                match confirm.await {
                    Ok(_) => published += 1,
                    Err(e) => {
                        last_error = Some(Error::CannotStoreData(format!("publish rejected: {e}")));
                        break;
                    }
                }
            }

            if last_error.is_none() {
                return Ok(published);
            }

            let error = last_error.unwrap();
            attempts += 1;

            if !is_publish_retryable(&error) || attempts >= self.max_retries {
                self.publish_errors
                    .fetch_add(messages.len() as u64 - published, Ordering::Relaxed);
                return Err(Error::CannotStoreData(format!(
                    "batch publish failed after {attempts} attempts: {error}"
                )));
            }

            match self.reconnect().await {
                Ok(_) => {}
                Err(reconnect_error) => {
                    self.publish_errors
                        .fetch_add(messages.len() as u64, Ordering::Relaxed);
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
            let conn = Connection::connect(&self.amqp_url, ConnectionProperties::default())
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            let channel = conn
                .create_channel()
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            let exchange_kind = match self.exchange_type.as_str() {
                "direct" => ExchangeKind::Direct,
                "topic" => ExchangeKind::Topic,
                "fanout" => ExchangeKind::Fanout,
                "headers" => ExchangeKind::Headers,
                other => {
                    return Err(Error::InvalidConfigValue(format!(
                        "unknown exchange_type: {other}"
                    )));
                }
            };
            channel
                .exchange_declare(
                    &self.exchange,
                    exchange_kind,
                    ExchangeDeclareOptions::default(),
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
        let exchange_kind = match self.exchange_type.as_str() {
            "direct" => ExchangeKind::Direct,
            "topic" => ExchangeKind::Topic,
            "fanout" => ExchangeKind::Fanout,
            "headers" => ExchangeKind::Headers,
            other => {
                return Err(Error::InvalidConfigValue(format!(
                    "unknown exchange_type: {other}. Valid: direct, topic, fanout, headers"
                )));
            }
        };
        let conn = Connection::connect(&self.amqp_url, ConnectionProperties::default())
            .await
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
                ExchangeDeclareOptions::default(),
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

fn is_publish_retryable(error: &Error) -> bool {
    let msg = error.to_string().to_lowercase();
    msg.contains("connection")
        || msg.contains("timeout")
        || msg.contains("broken pipe")
        || msg.contains("reset by peer")
        || msg.contains("resource locked")
        || msg.contains("channel closed")
}
