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

use super::container::{
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SINK_BUCKET, ENV_SINK_ORG, ENV_SINK_PATH,
    ENV_SINK_STREAMS_0_CONSUMER_GROUP, ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM,
    ENV_SINK_STREAMS_0_TOPICS, ENV_SINK_TOKEN, ENV_SINK_URL, HEALTH_CHECK_ATTEMPTS,
    HEALTH_CHECK_INTERVAL_MS, INFLUXDB_BUCKET, INFLUXDB_ORG, INFLUXDB_TOKEN, InfluxDbContainer,
    InfluxDbOps, create_http_client,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;

pub struct InfluxDbSinkFixture {
    container: InfluxDbContainer,
    http_client: HttpClient,
}

impl InfluxDbOps for InfluxDbSinkFixture {
    fn container(&self) -> &InfluxDbContainer {
        &self.container
    }
    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl InfluxDbSinkFixture {
    /// Poll until at least `expected` points exist in the bucket.
    pub async fn wait_for_points(
        &self,
        measurement: &str,
        expected: usize,
    ) -> Result<usize, TestBinaryError> {
        let flux = format!(
            r#"from(bucket:"{b}") |> range(start:-1h) |> filter(fn:(r)=>r._measurement=="{m}") |> filter(fn:(r)=>r._field=="offset")"#,
            b = INFLUXDB_BUCKET,
            m = measurement,
        );
        info!("Flux Query {} ", flux);
        for _ in 0..POLL_ATTEMPTS {
            match self.query_count(&flux).await {
                Ok(n) if n >= expected => {
                    info!("Found {n} points in InfluxDB (expected {expected})");
                    return Ok(n);
                }
                Ok(_) | Err(_) => {}
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!("Expected at least {expected} points after {POLL_ATTEMPTS} attempts"),
        })
    }
}

#[async_trait]
impl TestFixture for InfluxDbSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = InfluxDbContainer::start().await?;
        let http_client = create_http_client();

        let fixture = Self {
            container,
            http_client,
        };

        // Same /ping readiness probe as the source fixture.
        for attempt in 0..HEALTH_CHECK_ATTEMPTS {
            let url = format!("{}/ping", fixture.container.base_url);
            match fixture.http_client.get(&url).send().await {
                Ok(resp) if resp.status().as_u16() == 204 => {
                    info!("InfluxDB /ping OK after {} attempts", attempt + 1);
                    return Ok(fixture);
                }
                Ok(resp) => {
                    info!(
                        "InfluxDB /ping status {} (attempt {})",
                        resp.status(),
                        attempt + 1
                    );
                }
                Err(e) => {
                    info!("InfluxDB /ping error on attempt {}: {e}", attempt + 1);
                }
            }
            sleep(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS)).await;
        }

        Err(TestBinaryError::FixtureSetup {
            fixture_type: "InfluxDbSink".to_string(),
            message: format!(
                "InfluxDB /ping did not return 204 after {HEALTH_CHECK_ATTEMPTS} attempts"
            ),
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(ENV_SINK_URL.to_string(), self.container.base_url.clone());
        envs.insert(ENV_SINK_ORG.to_string(), INFLUXDB_ORG.to_string());
        envs.insert(ENV_SINK_TOKEN.to_string(), INFLUXDB_TOKEN.to_string());
        envs.insert(ENV_SINK_BUCKET.to_string(), INFLUXDB_BUCKET.to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", DEFAULT_TEST_TOPIC),
        );
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            "influxdb_sink_cg".to_string(),
        );
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_influxdb_sink".to_string(),
        );
        envs
    }
}
