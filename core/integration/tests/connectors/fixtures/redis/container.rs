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
use std::time::Duration;

use integration::harness::TestBinaryError;
use redis::aio::MultiplexedConnection;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;

use crate::connectors::fixtures;

const REDIS_IMAGE: &str = "docker.io/redis";
const REDIS_TAG: &str = "7";
const REDIS_PORT: u16 = 6379;
const REDIS_READY_MSG: &str = "Ready to accept connections";
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 100;

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";
pub(super) const DEFAULT_CONSUMER_GROUP: &str = "redis_sink_test_cg";
pub(super) const DEFAULT_STREAM_KEY: &str = "iggy:events";

pub(super) const ENV_SINK_CONNECTION_URL: &str =
    "IGGY_CONNECTORS_SINK_REDIS_PLUGIN_CONFIG_CONNECTION_URL";
pub(super) const ENV_SINK_STREAM_KEY: &str = "IGGY_CONNECTORS_SINK_REDIS_PLUGIN_CONFIG_STREAM_KEY";
pub(super) const ENV_SINK_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SINK_REDIS_PLUGIN_CONFIG_INCLUDE_METADATA";
pub(super) const ENV_SINK_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SINK_REDIS_PLUGIN_CONFIG_PAYLOAD_FORMAT";
pub(super) const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_REDIS_STREAMS_0_STREAM";
pub(super) const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_REDIS_STREAMS_0_TOPICS";
pub(super) const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_REDIS_STREAMS_0_SCHEMA";
pub(super) const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_REDIS_STREAMS_0_CONSUMER_GROUP";
pub(super) const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_REDIS_PATH";

/// One Redis Stream entry as `(entry id, field name -> raw value)`.
pub type StreamEntry = (String, HashMap<String, Vec<u8>>);

pub struct RedisContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub(super) url: String,
}

impl RedisContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(REDIS_IMAGE, REDIS_TAG)
            .with_exposed_port(REDIS_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout(REDIS_READY_MSG))
            .with_mapped_port(0, REDIS_PORT.tcp())
            .with_container_name(fixtures::unique_container_name("redis"))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "RedisContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let mapped_port = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "RedisContainer".to_string(),
                message: format!("Failed to get ports: {e}"),
            })?
            .map_to_host_port_ipv4(REDIS_PORT)
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "RedisContainer".to_string(),
                message: "No mapping for Redis port".to_string(),
            })?;
        let url = format!("redis://127.0.0.1:{mapped_port}");
        info!("Redis container available at {url}");
        Ok(Self { container, url })
    }
}

pub trait RedisOps: Sync {
    fn container(&self) -> &RedisContainer;

    async fn connect(&self) -> Result<MultiplexedConnection, TestBinaryError> {
        let client = redis::Client::open(self.container().url.as_str()).map_err(|e| {
            TestBinaryError::InvalidState {
                message: format!("Failed to open Redis client: {e}"),
            }
        })?;
        client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to connect to Redis: {e}"),
            })
    }

    async fn read_entries(&self, key: &str) -> Result<Vec<StreamEntry>, TestBinaryError> {
        let mut connection = self.connect().await?;
        redis::cmd("XRANGE")
            .arg(key)
            .arg("-")
            .arg("+")
            .query_async(&mut connection)
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("XRANGE {key} failed: {e}"),
            })
    }

    async fn wait_for_entries(
        &self,
        key: &str,
        count: usize,
    ) -> Result<Vec<StreamEntry>, TestBinaryError> {
        for _ in 0..POLL_ATTEMPTS {
            let entries = self.read_entries(key).await?;
            if entries.len() >= count {
                return Ok(entries);
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!("Expected {count} entries in Redis stream {key}"),
        })
    }

    async fn append_entry(
        &self,
        key: &str,
        id: &str,
        payload: &[u8],
    ) -> Result<(), TestBinaryError> {
        let mut connection = self.connect().await?;
        redis::cmd("XADD")
            .arg(key)
            .arg(id)
            .arg("payload")
            .arg(payload)
            .query_async::<String>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("XADD {key} {id} failed: {e}"),
            })
    }
}
