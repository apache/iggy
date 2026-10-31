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

use super::container::OtelCollectorContainer;
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time::sleep;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

// Everything else, including the auth header, comes from
// `tests/connectors/otlp/sink_config/otlp.toml`.
const ENV_ENDPOINT: &str = "IGGY_CONNECTORS_SINK_OTLP_PLUGIN_CONFIG_ENDPOINT";
const ENV_COMPRESSION: &str = "IGGY_CONNECTORS_SINK_OTLP_PLUGIN_CONFIG_COMPRESSION";
const ENV_MAX_REQUEST_BYTES: &str = "IGGY_CONNECTORS_SINK_OTLP_PLUGIN_CONFIG_MAX_REQUEST_BYTES";

const MOCK_POLL_ATTEMPTS: usize = 200;
const MOCK_POLL_INTERVAL_MS: u64 = 50;

/// How a fixture configures the sink on top of the shared connector file.
#[derive(Debug, Clone, Copy)]
pub struct OtlpSinkSetup {
    pub compression: &'static str,
    pub max_request_bytes: Option<u64>,
}

impl OtlpSinkSetup {
    const DEFAULT: Self = Self {
        compression: "none",
        max_request_bytes: None,
    };

    fn envs(self, endpoint: &str) -> HashMap<String, String> {
        let mut envs = HashMap::from([
            (ENV_ENDPOINT.to_string(), endpoint.to_string()),
            (ENV_COMPRESSION.to_string(), self.compression.to_string()),
        ]);
        if let Some(limit) = self.max_request_bytes {
            envs.insert(ENV_MAX_REQUEST_BYTES.to_string(), limit.to_string());
        }
        envs
    }
}

pub trait OtlpSinkVariant: Send + Sync + 'static {
    const SETUP: OtlpSinkSetup;
}

/// The sink exporting to a real OpenTelemetry Collector, configured by `V`.
pub struct OtlpCollectorFixture<V> {
    collector: OtelCollectorContainer,
    _variant: PhantomData<V>,
}

impl<V> OtlpCollectorFixture<V> {
    pub fn collector(&self) -> &OtelCollectorContainer {
        &self.collector
    }
}

#[async_trait]
impl<V: OtlpSinkVariant> TestFixture for OtlpCollectorFixture<V> {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            collector: OtelCollectorContainer::start().await?,
            _variant: PhantomData,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        V::SETUP.envs(&self.collector.http_endpoint)
    }
}

pub struct HttpProtoLogs;
impl OtlpSinkVariant for HttpProtoLogs {
    const SETUP: OtlpSinkSetup = OtlpSinkSetup::DEFAULT;
}

pub struct HttpProtoLogsGzip;
impl OtlpSinkVariant for HttpProtoLogsGzip {
    const SETUP: OtlpSinkSetup = OtlpSinkSetup {
        compression: "gzip",
        ..OtlpSinkSetup::DEFAULT
    };
}

/// Small enough that every request holds a single message of the test payload.
pub const SMALL_MAX_REQUEST_BYTES: u64 = 256;

pub struct HttpProtoLogsSmallRequests;
impl OtlpSinkVariant for HttpProtoLogsSmallRequests {
    const SETUP: OtlpSinkSetup = OtlpSinkSetup {
        max_request_bytes: Some(SMALL_MAX_REQUEST_BYTES),
        ..OtlpSinkSetup::DEFAULT
    };
}

pub type OtlpHttpProtoLogsFixture = OtlpCollectorFixture<HttpProtoLogs>;
pub type OtlpHttpGzipFixture = OtlpCollectorFixture<HttpProtoLogsGzip>;
pub type OtlpHttpSmallRequestsFixture = OtlpCollectorFixture<HttpProtoLogsSmallRequests>;

/// The sink exporting over OTLP/HTTP to a local mock, configured by `V`, for responses a
/// real collector does not produce on demand and for the exact bytes of each request.
pub struct OtlpMockFixture<V> {
    server: MockServer,
    _variant: PhantomData<V>,
}

impl<V> OtlpMockFixture<V> {
    /// Answer the first requests with `statuses` in order, then every later one with the last
    /// of them.
    pub async fn respond_with(&self, statuses: &[u16]) {
        let Some((last, first)) = statuses.split_last() else {
            return;
        };
        for status in first {
            Mock::given(method("POST"))
                .and(path("/v1/logs"))
                .respond_with(ResponseTemplate::new(*status))
                .up_to_n_times(1)
                .mount(&self.server)
                .await;
        }
        Mock::given(method("POST"))
            .and(path("/v1/logs"))
            .respond_with(ResponseTemplate::new(*last))
            .mount(&self.server)
            .await;
    }

    /// Answer every export with a redirect to `location`.
    pub async fn redirect_to(&self, location: &str) {
        Mock::given(method("POST"))
            .and(path("/v1/logs"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", location))
            .mount(&self.server)
            .await;
    }

    pub async fn received(&self) -> Vec<Request> {
        self.server.received_requests().await.unwrap_or_default()
    }

    pub async fn wait_for_requests(
        &self,
        expected: usize,
    ) -> Result<Vec<Request>, TestBinaryError> {
        for _ in 0..MOCK_POLL_ATTEMPTS {
            let requests = self.received().await;
            if requests.len() >= expected {
                return Ok(requests);
            }
            sleep(Duration::from_millis(MOCK_POLL_INTERVAL_MS)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected {expected} requests, got {} after {MOCK_POLL_ATTEMPTS} polls",
                self.received().await.len()
            ),
        })
    }
}

#[async_trait]
impl<V: OtlpSinkVariant> TestFixture for OtlpMockFixture<V> {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            server: MockServer::start().await,
            _variant: PhantomData,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        V::SETUP.envs(&self.server.uri())
    }
}

pub type OtlpHttpMockFixture = OtlpMockFixture<HttpProtoLogs>;
pub type OtlpHttpMockSmallRequestsFixture = OtlpMockFixture<HttpProtoLogsSmallRequests>;
