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

use super::container::{
    DEFAULT_CONSUMER_GROUP, DEFAULT_EXCHANGE, DEFAULT_EXCHANGE_TYPE, DEFAULT_ROUTING_KEY,
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SINK_AMQP_URL, ENV_SINK_EXCHANGE,
    ENV_SINK_EXCHANGE_TYPE, ENV_SINK_INCLUDE_METADATA, ENV_SINK_PATH, ENV_SINK_ROUTING_KEY,
    ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM,
    ENV_SINK_STREAMS_0_TOPICS, RabbitMqContainer, RabbitMqOps,
};
use async_trait::async_trait;
use iggy_connector_sdk::Schema;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;

pub struct RabbitMqSinkFixture {
    container: RabbitMqContainer,
    include_metadata: bool,
    schema: Schema,
}

impl RabbitMqOps for RabbitMqSinkFixture {
    fn container(&self) -> &RabbitMqContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for RabbitMqSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = RabbitMqContainer::start().await?;
        Ok(Self {
            container,
            include_metadata: true,
            schema: Schema::Json,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SINK_AMQP_URL.to_string(),
            self.container.amqp_url.clone(),
        );
        envs.insert(ENV_SINK_EXCHANGE.to_string(), DEFAULT_EXCHANGE.into());
        envs.insert(
            ENV_SINK_EXCHANGE_TYPE.to_string(),
            DEFAULT_EXCHANGE_TYPE.into(),
        );
        envs.insert(ENV_SINK_ROUTING_KEY.to_string(), DEFAULT_ROUTING_KEY.into());
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.into(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", DEFAULT_TEST_TOPIC),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_SCHEMA.to_string(),
            self.schema.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            DEFAULT_CONSUMER_GROUP.into(),
        );
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_rabbitmq_sink".into(),
        );
        envs.insert(
            ENV_SINK_INCLUDE_METADATA.to_string(),
            self.include_metadata.to_string(),
        );
        envs
    }
}

pub struct RabbitMqSinkWithoutMetadataFixture {
    inner: RabbitMqSinkFixture,
}

impl std::ops::Deref for RabbitMqSinkWithoutMetadataFixture {
    type Target = RabbitMqSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for RabbitMqSinkWithoutMetadataFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = RabbitMqContainer::start().await?;
        Ok(Self {
            inner: RabbitMqSinkFixture {
                container,
                include_metadata: false,
                schema: Schema::Json,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

pub struct RabbitMqSinkRawSchemaFixture {
    inner: RabbitMqSinkFixture,
}

impl std::ops::Deref for RabbitMqSinkRawSchemaFixture {
    type Target = RabbitMqSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for RabbitMqSinkRawSchemaFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = RabbitMqContainer::start().await?;
        Ok(Self {
            inner: RabbitMqSinkFixture {
                container,
                include_metadata: true,
                schema: Schema::Raw,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

pub struct RabbitMqSinkFanoutFixture {
    inner: RabbitMqSinkFixture,
}

impl std::ops::Deref for RabbitMqSinkFanoutFixture {
    type Target = RabbitMqSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for RabbitMqSinkFanoutFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = RabbitMqContainer::start_fanout().await?;
        Ok(Self {
            inner: RabbitMqSinkFixture {
                container,
                include_metadata: true,
                schema: Schema::Json,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(ENV_SINK_EXCHANGE_TYPE.to_string(), "fanout".into());
        envs
    }
}

pub struct RabbitMqSinkDirectFixture {
    inner: RabbitMqSinkFixture,
}

impl std::ops::Deref for RabbitMqSinkDirectFixture {
    type Target = RabbitMqSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for RabbitMqSinkDirectFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = RabbitMqContainer::start_direct().await?;
        Ok(Self {
            inner: RabbitMqSinkFixture {
                container,
                include_metadata: true,
                schema: Schema::Json,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(ENV_SINK_EXCHANGE_TYPE.to_string(), "direct".into());
        envs
    }
}
