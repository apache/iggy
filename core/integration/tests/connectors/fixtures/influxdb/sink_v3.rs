/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use super::container_v3::{
    DEFAULT_TEST_STREAM_V3, DEFAULT_TEST_TOPIC_V3, ENV_V3_SINK_BUCKET, ENV_V3_SINK_DB,
    ENV_V3_SINK_ORG, ENV_V3_SINK_PATH, ENV_V3_SINK_PAYLOAD_FORMAT,
    ENV_V3_SINK_STREAMS_0_CONSUMER_GROUP, ENV_V3_SINK_STREAMS_0_SCHEMA,
    ENV_V3_SINK_STREAMS_0_STREAM, ENV_V3_SINK_STREAMS_0_TOPICS, ENV_V3_SINK_TOKEN, ENV_V3_SINK_URL,
    ENV_V3_SINK_VERSION, HEALTH_CHECK_ATTEMPTS_V3, HEALTH_CHECK_INTERVAL_MS_V3, INFLUXDB3_DB,
    INFLUXDB3_TOKEN, InfluxDb3Container, InfluxDb3Ops, create_http_client_v3,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

const POLL_ATTEMPTS_V3: usize = 100;
const POLL_INTERVAL_MS_V3: u64 = 50;

pub struct InfluxDb3SinkFixture {
    container: InfluxDb3Container,
    http_client: HttpClient,
}

impl InfluxDb3Ops for InfluxDb3SinkFixture {
    fn container(&self) -> &InfluxDb3Container {
        &self.container
    }
    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl InfluxDb3SinkFixture {
    /// Poll until at least `expected` rows exist in the test database under
    /// `measurement` (SQL table name).
    pub async fn wait_for_points(
        &self,
        measurement: &str,
        expected: usize,
    ) -> Result<usize, TestBinaryError> {
        let sql = format!("SELECT * FROM \"{measurement}\"");
        info!("V3 wait_for_points SQL: {sql}");
        for _ in 0..POLL_ATTEMPTS_V3 {
            match self.query_count(&sql).await {
                Ok(n) if n >= expected => {
                    info!("Found {n} rows in InfluxDB 3 (expected {expected})");
                    return Ok(n);
                }
                Ok(_) | Err(_) => {}
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS_V3)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!("Expected at least {expected} rows after {POLL_ATTEMPTS_V3} attempts"),
        })
    }

    pub async fn setup() -> Result<Self, TestBinaryError> {
        let container = InfluxDb3Container::start().await?;
        let http_client = create_http_client_v3();

        let fixture = Self {
            container,
            http_client,
        };

        for attempt in 0..HEALTH_CHECK_ATTEMPTS_V3 {
            let url = format!("{}/ping", fixture.container.base_url);
            match fixture.http_client.get(&url).send().await {
                Ok(resp) if resp.status().as_u16() == 200 || resp.status().as_u16() == 204 => {
                    info!("InfluxDB 3 /ping OK after {} attempts", attempt + 1);
                    return Ok(fixture);
                }
                Ok(resp) => {
                    info!(
                        "InfluxDB 3 /ping status {} (attempt {})",
                        resp.status(),
                        attempt + 1
                    );
                }
                Err(e) => {
                    info!("InfluxDB 3 /ping error on attempt {}: {e}", attempt + 1);
                }
            }
            sleep(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS_V3)).await;
        }

        Err(TestBinaryError::FixtureSetup {
            fixture_type: "InfluxDb3Sink".to_string(),
            message: format!(
                "InfluxDB 3 /ping did not respond after {HEALTH_CHECK_ATTEMPTS_V3} attempts"
            ),
        })
    }
}

#[async_trait]
impl TestFixture for InfluxDb3SinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::setup().await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(ENV_V3_SINK_URL.to_string(), self.container.base_url.clone());
        envs.insert(ENV_V3_SINK_TOKEN.to_string(), INFLUXDB3_TOKEN.to_string());
        envs.insert(ENV_V3_SINK_DB.to_string(), INFLUXDB3_DB.to_string());
        envs.insert(ENV_V3_SINK_VERSION.to_string(), "v3".to_string());
        // org and bucket set to empty placeholders for V3 (adapter ignores them)
        envs.insert(ENV_V3_SINK_ORG.to_string(), "".to_string());
        envs.insert(ENV_V3_SINK_BUCKET.to_string(), INFLUXDB3_DB.to_string());
        envs.insert(
            ENV_V3_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM_V3.to_string(),
        );
        envs.insert(
            ENV_V3_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", DEFAULT_TEST_TOPIC_V3),
        );
        envs.insert(ENV_V3_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(ENV_V3_SINK_PAYLOAD_FORMAT.to_string(), "json".to_string());
        envs.insert(
            ENV_V3_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            "influxdb3_sink_cg".to_string(),
        );
        envs.insert(
            ENV_V3_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_influxdb_sink".to_string(),
        );
        envs
    }
}
