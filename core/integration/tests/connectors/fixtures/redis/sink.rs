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
use iggy_connector_sdk::Schema;
use integration::harness::{TestBinaryError, TestFixture};

use super::container::{
    DEFAULT_CONSUMER_GROUP, DEFAULT_STREAM_KEY, DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC,
    ENV_SINK_CONNECTION_URL, ENV_SINK_INCLUDE_METADATA, ENV_SINK_PATH, ENV_SINK_PAYLOAD_FORMAT,
    ENV_SINK_STREAM_KEY, ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA,
    ENV_SINK_STREAMS_0_STREAM, ENV_SINK_STREAMS_0_TOPICS, RedisContainer, RedisOps,
};

pub struct RedisSinkFixture {
    container: RedisContainer,
    include_metadata: bool,
    payload_format: &'static str,
    schema: Schema,
}

impl RedisOps for RedisSinkFixture {
    fn container(&self) -> &RedisContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for RedisSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            container: RedisContainer::start().await?,
            include_metadata: true,
            payload_format: "json",
            schema: Schema::Json,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                ENV_SINK_CONNECTION_URL.to_string(),
                self.container.url.clone(),
            ),
            (ENV_SINK_STREAM_KEY.to_string(), DEFAULT_STREAM_KEY.into()),
            (
                ENV_SINK_INCLUDE_METADATA.to_string(),
                self.include_metadata.to_string(),
            ),
            (
                ENV_SINK_PAYLOAD_FORMAT.to_string(),
                self.payload_format.to_string(),
            ),
            (
                ENV_SINK_STREAMS_0_STREAM.to_string(),
                DEFAULT_TEST_STREAM.into(),
            ),
            (
                ENV_SINK_STREAMS_0_TOPICS.to_string(),
                format!("[{DEFAULT_TEST_TOPIC}]"),
            ),
            (
                ENV_SINK_STREAMS_0_SCHEMA.to_string(),
                self.schema.to_string(),
            ),
            (
                ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
                DEFAULT_CONSUMER_GROUP.into(),
            ),
            (
                ENV_SINK_PATH.to_string(),
                "../../target/debug/libiggy_connector_redis_sink".into(),
            ),
        ])
    }
}

pub struct RedisSinkNoMetadataFixture {
    inner: RedisSinkFixture,
}

impl std::ops::Deref for RedisSinkNoMetadataFixture {
    type Target = RedisSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for RedisSinkNoMetadataFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: RedisSinkFixture {
                container: RedisContainer::start().await?,
                include_metadata: false,
                payload_format: "json",
                schema: Schema::Json,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

pub struct RedisSinkRawFixture {
    inner: RedisSinkFixture,
}

impl std::ops::Deref for RedisSinkRawFixture {
    type Target = RedisSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for RedisSinkRawFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: RedisSinkFixture {
                container: RedisContainer::start().await?,
                include_metadata: true,
                payload_format: "binary",
                schema: Schema::Raw,
            },
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}
