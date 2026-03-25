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
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SOURCE_BATCH_SIZE, ENV_SOURCE_ORG,
    ENV_SOURCE_PATH, ENV_SOURCE_PAYLOAD_COLUMN, ENV_SOURCE_PAYLOAD_FORMAT,
    ENV_SOURCE_POLL_INTERVAL, ENV_SOURCE_QUERY, ENV_SOURCE_STREAMS_0_SCHEMA,
    ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC, ENV_SOURCE_TOKEN, ENV_SOURCE_URL,
    HEALTH_CHECK_ATTEMPTS, HEALTH_CHECK_INTERVAL_MS, INFLUXDB_BUCKET, INFLUXDB_ORG, INFLUXDB_TOKEN,
    InfluxDbContainer, InfluxDbOps, create_http_client,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

/// Options for the source fixture variant.
#[derive(Debug, Clone, Default)]
pub struct InfluxDbSourceOptions {
    /// Override `payload_format`. `None` → not set (connector uses "json").
    pub payload_format: Option<String>,
    /// Override `payload_column`. `None` → not set (whole-row JSON wrapping).
    pub payload_column: Option<String>,
}

pub struct InfluxDbSourceFixture {
    pub(super) container: InfluxDbContainer,
    pub(super) http_client: HttpClient,
    pub options: InfluxDbSourceOptions,
}

impl InfluxDbOps for InfluxDbSourceFixture {
    fn container(&self) -> &InfluxDbContainer {
        &self.container
    }
    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl InfluxDbSourceFixture {
    /// Write line-protocol lines into the test bucket.
    pub async fn write_lines(&self, lines: &[&str]) -> Result<(), TestBinaryError> {
        InfluxDbOps::write_lines(self, lines).await
    }

    pub async fn setup_with_options(
        options: InfluxDbSourceOptions,
    ) -> Result<Self, TestBinaryError> {
        let container = InfluxDbContainer::start().await?;
        let http_client = create_http_client();

        let fixture = Self {
            container,
            http_client,
            options,
        };

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
            fixture_type: "InfluxDbSource".to_string(),
            message: format!(
                "InfluxDB /ping did not return 204 after {HEALTH_CHECK_ATTEMPTS} attempts"
            ),
        })
    }
}

#[async_trait]
impl TestFixture for InfluxDbSourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::setup_with_options(InfluxDbSourceOptions::default()).await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let default_flux = format!(
            r#"from(bucket:"{b}") |> range(start: -1h) |> filter(fn: (r) => r._time > time(v: "$cursor")) |> sort(columns: ["_time"]) |> limit(n: $limit)"#,
            b = INFLUXDB_BUCKET,
        );

        let payload_format = self
            .options
            .payload_format
            .clone()
            .unwrap_or_else(|| "json".to_string());
        let schema = match payload_format.as_str() {
            "text" | "utf8" => "text",
            "raw" | "base64" => "raw",
            _ => "json",
        };

        let mut envs = HashMap::new();
        envs.insert(ENV_SOURCE_URL.to_string(), self.container.base_url.clone());
        envs.insert(ENV_SOURCE_ORG.to_string(), INFLUXDB_ORG.to_string());
        envs.insert(ENV_SOURCE_TOKEN.to_string(), INFLUXDB_TOKEN.to_string());
        envs.insert(ENV_SOURCE_QUERY.to_string(), default_flux);
        envs.insert(ENV_SOURCE_POLL_INTERVAL.to_string(), "100ms".to_string());
        envs.insert(ENV_SOURCE_BATCH_SIZE.to_string(), "100".to_string());
        envs.insert(
            ENV_SOURCE_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
            DEFAULT_TEST_TOPIC.to_string(),
        );
        envs.insert(ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), schema.to_string());
        envs.insert(ENV_SOURCE_PAYLOAD_FORMAT.to_string(), payload_format);
        if let Some(col) = &self.options.payload_column {
            envs.insert(ENV_SOURCE_PAYLOAD_COLUMN.to_string(), col.clone());
        }
        envs.insert(
            ENV_SOURCE_PATH.to_string(),
            "../../target/debug/libiggy_connector_influxdb_source".to_string(),
        );
        envs
    }
}

// ── Typed fixture variants used by format-specific test files ─────────────────

pub struct InfluxDbSourceTextFixture(pub InfluxDbSourceFixture);
pub struct InfluxDbSourceRawFixture(pub InfluxDbSourceFixture);

macro_rules! delegate_source_fixture {
    ($wrapper:ident, $opts:expr) => {
        #[async_trait]
        impl TestFixture for $wrapper {
            async fn setup() -> Result<Self, TestBinaryError> {
                InfluxDbSourceFixture::setup_with_options($opts)
                    .await
                    .map(Self)
            }

            fn connectors_runtime_envs(&self) -> HashMap<String, String> {
                self.0.connectors_runtime_envs()
            }
        }

        impl $wrapper {
            pub async fn write_lines(&self, lines: &[&str]) -> Result<(), TestBinaryError> {
                self.0.write_lines(lines).await
            }
        }
    };
}

delegate_source_fixture!(
    InfluxDbSourceTextFixture,
    InfluxDbSourceOptions {
        payload_format: Some("text".to_string()),
        payload_column: Some("_value".to_string()),
    }
);

delegate_source_fixture!(
    InfluxDbSourceRawFixture,
    InfluxDbSourceOptions {
        payload_format: Some("raw".to_string()),
        payload_column: Some("_value".to_string()),
    }
);
