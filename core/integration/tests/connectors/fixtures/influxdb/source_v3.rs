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
    DEFAULT_TEST_STREAM_V3, DEFAULT_TEST_TOPIC_V3, ENV_V3_SOURCE_BATCH_SIZE,
    ENV_V3_SOURCE_CURSOR_FIELD, ENV_V3_SOURCE_DB, ENV_V3_SOURCE_ORG, ENV_V3_SOURCE_PATH,
    ENV_V3_SOURCE_PAYLOAD_FORMAT, ENV_V3_SOURCE_POLL_INTERVAL, ENV_V3_SOURCE_QUERY,
    ENV_V3_SOURCE_STREAMS_0_SCHEMA, ENV_V3_SOURCE_STREAMS_0_STREAM, ENV_V3_SOURCE_STREAMS_0_TOPIC,
    ENV_V3_SOURCE_TOKEN, ENV_V3_SOURCE_URL, ENV_V3_SOURCE_VERSION,
    HEALTH_CHECK_ATTEMPTS_V3, HEALTH_CHECK_INTERVAL_MS_V3, INFLUXDB3_DB, INFLUXDB3_TOKEN,
    InfluxDb3Container, InfluxDb3Ops, create_http_client_v3,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

pub struct InfluxDb3SourceFixture {
    pub(super) container: InfluxDb3Container,
    pub(super) http_client: HttpClient,
}

impl InfluxDb3Ops for InfluxDb3SourceFixture {
    fn container(&self) -> &InfluxDb3Container {
        &self.container
    }
    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl InfluxDb3SourceFixture {
    /// Write line-protocol lines into the test database.
    pub async fn write_lines(&self, lines: &[&str]) -> Result<(), TestBinaryError> {
        InfluxDb3Ops::write_lines(self, lines).await
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
            fixture_type: "InfluxDb3Source".to_string(),
            message: format!(
                "InfluxDB 3 /ping did not respond after {HEALTH_CHECK_ATTEMPTS_V3} attempts"
            ),
        })
    }
}

#[async_trait]
impl TestFixture for InfluxDb3SourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::setup().await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        // SQL query template with $cursor and $limit placeholders.
        // InfluxDB 3 stores time as `time` column (not `_time`).
        // The connector runtime substitutes $cursor and $limit before sending.
        let sql_query = "SELECT * FROM sensor_readings \
             WHERE time > '$cursor' \
             ORDER BY time \
             LIMIT $limit"
            .to_string();

        let mut envs = HashMap::new();
        envs.insert(
            ENV_V3_SOURCE_URL.to_string(),
            self.container.base_url.clone(),
        );
        envs.insert(ENV_V3_SOURCE_TOKEN.to_string(), INFLUXDB3_TOKEN.to_string());
        envs.insert(ENV_V3_SOURCE_DB.to_string(), INFLUXDB3_DB.to_string());
        envs.insert(ENV_V3_SOURCE_VERSION.to_string(), "v3".to_string());
        envs.insert(ENV_V3_SOURCE_ORG.to_string(), "".to_string());
        envs.insert(ENV_V3_SOURCE_QUERY.to_string(), sql_query);
        envs.insert(ENV_V3_SOURCE_POLL_INTERVAL.to_string(), "100ms".to_string());
        envs.insert(ENV_V3_SOURCE_BATCH_SIZE.to_string(), "100".to_string());
        // InfluxDB 3 names the time column "time", not "_time" (V2 default).
        envs.insert(ENV_V3_SOURCE_CURSOR_FIELD.to_string(), "time".to_string());
        envs.insert(
            ENV_V3_SOURCE_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM_V3.to_string(),
        );
        envs.insert(
            ENV_V3_SOURCE_STREAMS_0_TOPIC.to_string(),
            DEFAULT_TEST_TOPIC_V3.to_string(),
        );
        envs.insert(
            ENV_V3_SOURCE_STREAMS_0_SCHEMA.to_string(),
            "json".to_string(),
        );
        envs.insert(ENV_V3_SOURCE_PAYLOAD_FORMAT.to_string(), "json".to_string());
        envs.insert(
            ENV_V3_SOURCE_PATH.to_string(),
            "../../target/debug/libiggy_connector_influxdb_source".to_string(),
        );
        envs
    }
}
