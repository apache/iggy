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
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SINK_BUCKET, ENV_SINK_INCLUDE_CHECKSUM,
    ENV_SINK_INCLUDE_METADATA, ENV_SINK_INCLUDE_ORIGIN_TIMESTAMP, ENV_SINK_INCLUDE_PARTITION_TAG,
    ENV_SINK_INCLUDE_STREAM_TAG, ENV_SINK_INCLUDE_TOPIC_TAG, ENV_SINK_ORG, ENV_SINK_PATH,
    ENV_SINK_PAYLOAD_FORMAT, ENV_SINK_PRECISION, ENV_SINK_STREAMS_0_CONSUMER_GROUP,
    ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM, ENV_SINK_STREAMS_0_TOPICS,
    ENV_SINK_TOKEN, ENV_SINK_URL, HEALTH_CHECK_ATTEMPTS, HEALTH_CHECK_INTERVAL_MS, INFLUXDB_BUCKET,
    INFLUXDB_ORG, INFLUXDB_TOKEN, InfluxDbContainer, InfluxDbOps, create_http_client,
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

/// Controls which variant of the sink fixture is instantiated.
#[derive(Debug, Clone, Default)]
pub struct InfluxDbSinkOptions {
    /// Override `payload_format` sent to the connector runtime. `None` → "json".
    pub payload_format: Option<String>,
    /// Override `precision`. `None` → "us".
    pub precision: Option<String>,
    pub include_metadata: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
    pub include_stream_tag: Option<bool>,
    pub include_topic_tag: Option<bool>,
    pub include_partition_tag: Option<bool>,
}

pub struct InfluxDbSinkFixture {
    container: InfluxDbContainer,
    http_client: HttpClient,
    pub options: InfluxDbSinkOptions,
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
    /// Poll until at least `expected` points exist in the bucket under `measurement`.
    pub async fn wait_for_points(
        &self,
        measurement: &str,
        expected: usize,
    ) -> Result<usize, TestBinaryError> {
        let flux = format!(
            r#"from(bucket:"{b}") |> range(start:-1h) |> filter(fn:(r)=>r._measurement=="{m}")"#,
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

    pub async fn setup_with_options(options: InfluxDbSinkOptions) -> Result<Self, TestBinaryError> {
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
            fixture_type: "InfluxDbSink".to_string(),
            message: format!(
                "InfluxDB /ping did not return 204 after {HEALTH_CHECK_ATTEMPTS} attempts"
            ),
        })
    }
}

#[async_trait]
impl TestFixture for InfluxDbSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::setup_with_options(InfluxDbSinkOptions::default()).await
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

        let payload_format = self
            .options
            .payload_format
            .clone()
            .unwrap_or_else(|| "json".to_string());
        let schema = match payload_format.as_str() {
            "text" | "utf8" => "text",
            "base64" | "raw" => "raw",
            _ => "json",
        };
        envs.insert(ENV_SINK_PAYLOAD_FORMAT.to_string(), payload_format);
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), schema.to_string());

        if let Some(precision) = &self.options.precision {
            envs.insert(ENV_SINK_PRECISION.to_string(), precision.clone());
        }
        if let Some(v) = self.options.include_metadata {
            envs.insert(ENV_SINK_INCLUDE_METADATA.to_string(), v.to_string());
        }
        if let Some(v) = self.options.include_checksum {
            envs.insert(ENV_SINK_INCLUDE_CHECKSUM.to_string(), v.to_string());
        }
        if let Some(v) = self.options.include_origin_timestamp {
            envs.insert(ENV_SINK_INCLUDE_ORIGIN_TIMESTAMP.to_string(), v.to_string());
        }
        if let Some(v) = self.options.include_stream_tag {
            envs.insert(ENV_SINK_INCLUDE_STREAM_TAG.to_string(), v.to_string());
        }
        if let Some(v) = self.options.include_topic_tag {
            envs.insert(ENV_SINK_INCLUDE_TOPIC_TAG.to_string(), v.to_string());
        }
        if let Some(v) = self.options.include_partition_tag {
            envs.insert(ENV_SINK_INCLUDE_PARTITION_TAG.to_string(), v.to_string());
        }

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

// ── Typed fixture variants used by format-specific test files ─────────────────

pub struct InfluxDbSinkTextFixture(pub InfluxDbSinkFixture);
pub struct InfluxDbSinkBase64Fixture(pub InfluxDbSinkFixture);
pub struct InfluxDbSinkNoMetadataFixture(pub InfluxDbSinkFixture);
pub struct InfluxDbSinkNsPrecisionFixture(pub InfluxDbSinkFixture);

macro_rules! delegate_fixture {
    ($wrapper:ident, $opts:expr) => {
        #[async_trait]
        impl TestFixture for $wrapper {
            async fn setup() -> Result<Self, TestBinaryError> {
                InfluxDbSinkFixture::setup_with_options($opts)
                    .await
                    .map(Self)
            }

            fn connectors_runtime_envs(&self) -> HashMap<String, String> {
                self.0.connectors_runtime_envs()
            }
        }

        impl $wrapper {
            pub async fn wait_for_points(
                &self,
                measurement: &str,
                expected: usize,
            ) -> Result<usize, TestBinaryError> {
                self.0.wait_for_points(measurement, expected).await
            }
        }
    };
}

delegate_fixture!(
    InfluxDbSinkTextFixture,
    InfluxDbSinkOptions {
        payload_format: Some("text".to_string()),
        ..Default::default()
    }
);

delegate_fixture!(
    InfluxDbSinkBase64Fixture,
    InfluxDbSinkOptions {
        payload_format: Some("base64".to_string()),
        ..Default::default()
    }
);

delegate_fixture!(
    InfluxDbSinkNoMetadataFixture,
    InfluxDbSinkOptions {
        payload_format: Some("json".to_string()),
        include_metadata: Some(false),
        include_checksum: Some(false),
        include_origin_timestamp: Some(false),
        include_stream_tag: Some(false),
        include_topic_tag: Some(false),
        include_partition_tag: Some(false),
        ..Default::default()
    }
);

delegate_fixture!(
    InfluxDbSinkNsPrecisionFixture,
    InfluxDbSinkOptions {
        payload_format: Some("json".to_string()),
        precision: Some("ns".to_string()),
        ..Default::default()
    }
);
