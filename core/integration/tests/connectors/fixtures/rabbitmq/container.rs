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

use crate::connectors::fixtures;
use futures::StreamExt;
use integration::harness::TestBinaryError;
use lapin::{
    Connection, ConnectionProperties, ExchangeKind,
    options::{BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
};
use std::time::Duration;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;
use uuid::Uuid;

const RABBITMQ_IMAGE: &str = "docker.io/rabbitmq";
const RABBITMQ_TAG: &str = "4.0-management";
const RABBITMQ_PORT: u16 = 5672;
const RABBITMQ_READY_MSG: &str = "Time to start RabbitMQ:";
const RABBITMQ_BOOT_ATTEMPTS: usize = 60;
const RABBITMQ_BOOT_INTERVAL_MS: u64 = 1000;

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";
pub(super) const DEFAULT_EXCHANGE: &str = "iggy_events";
pub(super) const DEFAULT_EXCHANGE_TYPE: &str = "topic";
pub(super) const DEFAULT_ROUTING_KEY: &str = "iggy.messages";
pub(super) const DEFAULT_CONSUMER_GROUP: &str = "rabbitmq_sink_test_cg";

pub(super) const ENV_SINK_AMQP_URL: &str = "IGGY_CONNECTORS_SINK_RABBITMQ_PLUGIN_CONFIG_AMQP_URL";
pub(super) const ENV_SINK_EXCHANGE: &str = "IGGY_CONNECTORS_SINK_RABBITMQ_PLUGIN_CONFIG_EXCHANGE";
pub(super) const ENV_SINK_EXCHANGE_TYPE: &str =
    "IGGY_CONNECTORS_SINK_RABBITMQ_PLUGIN_CONFIG_EXCHANGE_TYPE";
pub(super) const ENV_SINK_ROUTING_KEY: &str =
    "IGGY_CONNECTORS_SINK_RABBITMQ_PLUGIN_CONFIG_ROUTING_KEY";
pub(super) const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_RABBITMQ_STREAMS_0_STREAM";
pub(super) const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_RABBITMQ_STREAMS_0_TOPICS";
pub(super) const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_RABBITMQ_STREAMS_0_SCHEMA";
pub(super) const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_RABBITMQ_STREAMS_0_CONSUMER_GROUP";
pub(super) const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_RABBITMQ_PATH";
pub(super) const ENV_SINK_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SINK_RABBITMQ_PLUGIN_CONFIG_INCLUDE_METADATA";

#[derive(PartialEq)]
pub(super) enum RabbitMqExchangeSetup {
    Topic,
    Fanout,
    Direct,
    Headers,
}
pub struct RabbitMqContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub(super) amqp_url: String,
    pub(super) queue_names: Vec<String>,
    pub(super) exchange_setup: RabbitMqExchangeSetup,
}

impl RabbitMqContainer {
    async fn start_container() -> Result<(ContainerAsync<GenericImage>, String), TestBinaryError> {
        let container = GenericImage::new(RABBITMQ_IMAGE, RABBITMQ_TAG)
            .with_exposed_port(RABBITMQ_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout(RABBITMQ_READY_MSG))
            .with_mapped_port(0, RABBITMQ_PORT.tcp())
            .with_container_name(fixtures::unique_container_name("rabbitmq"))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "RabbitMqContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let mapped_port = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "RabbitMqContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?
            .map_to_host_port_ipv4(RABBITMQ_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "RabbitMqContainer".to_string(),
                message: "No mapping for RabbitMQ port".to_string(),
            })?;
        let amqp_url = format!("amqp://guest:guest@127.0.0.1:{mapped_port}");
        Ok((container, amqp_url))
    }

    async fn start_with(
        exchange_setup: RabbitMqExchangeSetup,
        queue_count: usize,
    ) -> Result<Self, TestBinaryError> {
        let (container, amqp_url) = Self::start_container().await?;
        let queue_names = (0..queue_count)
            .map(|index| format!("test_queue_{}_{}", Uuid::new_v4().simple(), index))
            .collect();

        let instance = Self {
            container,
            amqp_url,
            exchange_setup,
            queue_names,
        };
        instance.wait_until_ready().await?;

        info!("RabbitMQ container available at {}", instance.amqp_url);
        Ok(instance)
    }

    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        Self::start_with(RabbitMqExchangeSetup::Topic, 1).await
    }

    pub(super) async fn start_fanout() -> Result<Self, TestBinaryError> {
        Self::start_with(RabbitMqExchangeSetup::Fanout, 2).await
    }

    pub(super) async fn start_direct() -> Result<Self, TestBinaryError> {
        Self::start_with(RabbitMqExchangeSetup::Direct, 1).await
    }

    pub(super) async fn start_headers() -> Result<Self, TestBinaryError> {
        Self::start_with(RabbitMqExchangeSetup::Headers, 1).await
    }

    async fn wait_until_ready(&self) -> Result<(), TestBinaryError> {
        let mut last_error = None;

        for _ in 0..RABBITMQ_BOOT_ATTEMPTS {
            match Connection::connect(&self.amqp_url, ConnectionProperties::default()).await {
                Ok(_) => {
                    return self.setup_exchange_and_queue().await;
                }
                Err(error) => last_error = Some(error.to_string()),
            }
            sleep(Duration::from_millis(RABBITMQ_BOOT_INTERVAL_MS)).await;
        }

        let detail = last_error
            .map(|error| format!(" Last error: {error}"))
            .unwrap_or_default();
        Err(TestBinaryError::FixtureSetup {
            fixture_type: "RabbitMqContainer".to_string(),
            message: format!("RabbitMQ did not become ready.{detail}"),
        })
    }

    async fn setup_exchange_and_queue(&self) -> Result<(), TestBinaryError> {
        let conn = Connection::connect(&self.amqp_url, ConnectionProperties::default())
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "RabbitMqContainer".to_string(),
                message: format!("Failed to create connection for consume: {e}"),
            })?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "RabbitMqContainer".to_string(),
                message: format!("Failed to create channel for consume: {e}"),
            })?;

        let exchange_kind = match self.exchange_setup {
            RabbitMqExchangeSetup::Topic => ExchangeKind::Topic,
            RabbitMqExchangeSetup::Fanout => ExchangeKind::Fanout,
            RabbitMqExchangeSetup::Direct => ExchangeKind::Direct,
            RabbitMqExchangeSetup::Headers => ExchangeKind::Headers,
        };
        channel
            .exchange_declare(
                DEFAULT_EXCHANGE,
                exchange_kind,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "RabbitMqContainer".to_string(),
                message: format!("Failed to declare exchange for consume: {e}"),
            })?;

        for queue_name in &self.queue_names {
            channel
                .queue_declare(
                    queue_name,
                    QueueDeclareOptions {
                        auto_delete: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "RabbitMqContainer".to_string(),
                    message: format!("Failed to create queue for consume: {e}"),
                })?;

            let mut bind_arguments = FieldTable::default();
            if self.exchange_setup == RabbitMqExchangeSetup::Headers {
                bind_arguments.insert("x-match".into(), AMQPValue::LongString("all".into()));
                bind_arguments.insert("x-user".into(), AMQPValue::LongString("alice".into()));
            }
            channel
                .queue_bind(
                    queue_name,
                    DEFAULT_EXCHANGE,
                    DEFAULT_ROUTING_KEY,
                    QueueBindOptions::default(),
                    bind_arguments,
                )
                .await
                .map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "RabbitMqContainer".to_string(),
                    message: format!("Failed to bind queue to exchange for consume: {e}"),
                })?;
        }

        Ok(())
    }
}

pub struct ConsumedDelivery {
    pub data: Vec<u8>,
    pub headers: lapin::types::FieldTable,
}

pub trait RabbitMqOps: Sync {
    fn container(&self) -> &RabbitMqContainer;

    fn queue_names(&self) -> &[String] {
        &self.container().queue_names
    }

    async fn consume_messages(
        &self,
        count: usize,
    ) -> Result<Vec<ConsumedDelivery>, TestBinaryError> {
        self.consume_messages_from(&self.container().queue_names[0], count)
            .await
    }

    async fn consume_messages_from(
        &self,
        queue_name: &str,
        count: usize,
    ) -> Result<Vec<ConsumedDelivery>, TestBinaryError> {
        self.consume_messages_from_with_timeout(queue_name, count, Duration::from_secs(60))
            .await
    }

    async fn consume_messages_from_with_timeout(
        &self,
        queue_name: &str,
        count: usize,
        timeout: Duration,
    ) -> Result<Vec<ConsumedDelivery>, TestBinaryError> {
        let conn = Connection::connect(&self.container().amqp_url, ConnectionProperties::default())
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to connect to RabbitMQ for consume: {e}"),
            })?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to create channel for consume: {e}"),
            })?;

        let mut consumer = channel
            .basic_consume(
                queue_name,
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to start consumer: {e}"),
            })?;

        let mut messages = Vec::with_capacity(count);
        let deadline = tokio::time::Instant::now() + timeout;
        while messages.len() < count && tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_secs(1), consumer.next()).await {
                Ok(Some(delivery)) => {
                    let delivery = delivery.map_err(|e| TestBinaryError::InvalidState {
                        message: format!("Consumer error: {e}"),
                    })?;
                    let data = delivery.data.clone();
                    let headers = delivery.properties.headers().clone().unwrap_or_default();
                    delivery.ack(Default::default()).await.map_err(|e| {
                        TestBinaryError::InvalidState {
                            message: format!("Failed to ack message: {e}"),
                        }
                    })?;
                    messages.push(ConsumedDelivery { data, headers });
                }
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        Ok(messages)
    }
}
