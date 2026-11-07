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

use std::collections::HashMap;

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions},
};

use super::container::{
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SOURCE_AMQP_URL, ENV_SOURCE_BATCH_SIZE,
    ENV_SOURCE_BATCH_TIMEOUT, ENV_SOURCE_INCLUDE_METADATA, ENV_SOURCE_PATH, ENV_SOURCE_QUEUE,
    ENV_SOURCE_STREAMS_0_SCHEMA, ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC,
    RabbitMqContainer,
};

pub struct RabbitMqSourceFixture {
    container: RabbitMqContainer,
    http: reqwest::Client,
    metadata: bool,
}

impl RabbitMqSourceFixture {
    pub async fn stop_broker(&self) -> Result<(), String> {
        self.container.stop_broker().await
    }

    pub async fn start_broker(&self) -> Result<(), String> {
        self.container.start_broker().await
    }

    pub async fn acknowledged_messages(&self) -> Result<Option<u64>, String> {
        let queue = &self.container.queue_names[0];
        let response = self
            .management_get(&format!("/api/queues/%2F/{queue}"))
            .await?;
        if !response.is_object() {
            return Err(format!(
                "RabbitMQ returned unexpected queue data: {response}"
            ));
        }
        match response.pointer("/message_stats/ack") {
            Some(value) => value.as_u64().map(Some).ok_or_else(|| {
                format!("RabbitMQ returned an invalid acknowledgement count: {value}")
            }),
            None => Ok(None),
        }
    }

    pub async fn consumer_connections(&self) -> Result<Vec<String>, String> {
        let consumers: Vec<serde_json::Value> =
            serde_json::from_value(self.management_get("/api/consumers/%2F").await?)
                .map_err(|error| error.to_string())?;
        consumers
            .iter()
            .filter(|consumer| {
                consumer["queue"]["name"].as_str() == Some(&self.container.queue_names[0])
            })
            .map(|consumer| {
                consumer["channel_details"]["connection_name"]
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        format!("RabbitMQ returned unexpected consumer data: {consumer}")
                    })
            })
            .collect()
    }

    pub async fn close_connection(&self, connection: &str) -> Result<(), String> {
        let mut url = reqwest::Url::parse(&self.container.management_url)
            .map_err(|error| error.to_string())?;
        url.path_segments_mut()
            .map_err(|()| "RabbitMQ management URL cannot be a base URL".to_string())?
            .extend(["api", "connections", connection]);
        self.http
            .delete(url)
            .basic_auth("iggy_test", Some("iggy_test_password"))
            .send()
            .await
            .map_err(|error| error.to_string())?
            .error_for_status()
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub async fn consumer_count(&self) -> Result<usize, String> {
        let consumers: Vec<serde_json::Value> =
            serde_json::from_value(self.management_get("/api/consumers/%2F").await?)
                .map_err(|error| error.to_string())?;
        let mut count = 0;
        for consumer in consumers {
            let queue = consumer
                .pointer("/queue/name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("RabbitMQ returned unexpected consumer data: {consumer}"))?;
            if queue == self.container.queue_names[0] {
                count += 1;
            }
        }
        Ok(count)
    }

    pub async fn unacknowledged_messages(&self) -> Result<u32, String> {
        let queue = &self.container.queue_names[0];
        let queue = self
            .management_get(&format!("/api/queues/%2F/{queue}"))
            .await?;
        queue
            .get("messages_unacknowledged")
            .and_then(serde_json::Value::as_u64)
            .map(|count| count as u32)
            .or_else(|| queue.as_object().map(|_| 0))
            .ok_or_else(|| format!("RabbitMQ returned unexpected queue data: {queue}"))
    }

    pub async fn get_ready_message(&self) -> Result<bool, String> {
        let connection =
            Connection::connect(&self.container.amqp_url, ConnectionProperties::default())
                .await
                .map_err(|error| error.to_string())?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|error| error.to_string())?;
        let delivery = channel
            .basic_get(
                self.container.queue_names[0].as_str().into(),
                BasicGetOptions { no_ack: true },
            )
            .await
            .map_err(|error| error.to_string())?;
        connection
            .close(200, "OK".into())
            .await
            .map_err(|error| error.to_string())?;
        Ok(delivery.is_some())
    }

    pub async fn publish(&self, payload: &[u8], properties: BasicProperties) -> Result<(), String> {
        let connection =
            Connection::connect(&self.container.amqp_url, ConnectionProperties::default())
                .await
                .map_err(|error| error.to_string())?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|error| error.to_string())?;
        channel
            .basic_publish(
                "".into(),
                self.container.queue_names[0].as_str().into(),
                BasicPublishOptions::default(),
                payload,
                properties,
            )
            .await
            .map_err(|error| error.to_string())?
            .await
            .map_err(|error| error.to_string())?;
        connection
            .close(200, "OK".into())
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn management_get(&self, path: &str) -> Result<serde_json::Value, String> {
        self.http
            .get(format!("{}{path}", self.container.management_url))
            .basic_auth("iggy_test", Some("iggy_test_password"))
            .send()
            .await
            .map_err(|error| error.to_string())?
            .error_for_status()
            .map_err(|error| error.to_string())?
            .json()
            .await
            .map_err(|error| error.to_string())
    }

    fn envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (ENV_SOURCE_AMQP_URL.into(), self.container.amqp_url.clone()),
            (
                ENV_SOURCE_QUEUE.into(),
                self.container.queue_names[0].clone(),
            ),
            (
                ENV_SOURCE_INCLUDE_METADATA.into(),
                self.metadata.to_string(),
            ),
            (
                ENV_SOURCE_STREAMS_0_STREAM.into(),
                DEFAULT_TEST_STREAM.into(),
            ),
            (ENV_SOURCE_STREAMS_0_TOPIC.into(), DEFAULT_TEST_TOPIC.into()),
            (
                ENV_SOURCE_STREAMS_0_SCHEMA.into(),
                if self.metadata { "json" } else { "raw" }.into(),
            ),
            (
                ENV_SOURCE_PATH.into(),
                "../../target/debug/libiggy_connector_rabbitmq_source".into(),
            ),
        ])
    }
}

#[async_trait]
impl TestFixture for RabbitMqSourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            container: RabbitMqContainer::start().await?,
            http: reqwest::Client::new(),
            metadata: false,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.envs()
    }
}

pub struct RabbitMqSourceMetadataFixture(RabbitMqSourceFixture);

pub struct RabbitMqSourceBatchFixture(RabbitMqSourceFixture);

impl std::ops::Deref for RabbitMqSourceBatchFixture {
    type Target = RabbitMqSourceFixture;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl TestFixture for RabbitMqSourceBatchFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let fixture = RabbitMqSourceFixture::setup().await?;
        let publish = async {
            let connection =
                Connection::connect(&fixture.container.amqp_url, ConnectionProperties::default())
                    .await?;
            let channel = connection.create_channel().await?;
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await?;
            for payload in [b"batch-1", b"batch-2", b"batch-3"] {
                let confirmation = channel
                    .basic_publish(
                        "".into(),
                        fixture.container.queue_names[0].as_str().into(),
                        BasicPublishOptions::default(),
                        payload,
                        BasicProperties::default(),
                    )
                    .await?
                    .await?;
                if !confirmation.is_ack() {
                    return Err("RabbitMQ did not confirm the publication".into());
                }
            }
            connection.close(200, "OK".into()).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };
        publish
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "RabbitMqSourceBatchFixture".to_string(),
                message: format!("Failed to publish batch: {error}"),
            })?;
        Ok(Self(fixture))
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.0.envs();
        envs.insert(ENV_SOURCE_BATCH_SIZE.into(), "3".into());
        envs.insert(ENV_SOURCE_BATCH_TIMEOUT.into(), "5s".into());
        envs
    }
}

impl std::ops::Deref for RabbitMqSourceMetadataFixture {
    type Target = RabbitMqSourceFixture;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl TestFixture for RabbitMqSourceMetadataFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self(RabbitMqSourceFixture {
            container: RabbitMqContainer::start().await?,
            http: reqwest::Client::new(),
            metadata: true,
        }))
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.0.envs()
    }
}
