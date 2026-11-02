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

use crate::connectors::fixtures;
use integration::harness::TestBinaryError;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;
use tracing::info;

const COLLECTOR_IMAGE: &str = "docker.io/otel/opentelemetry-collector";
const COLLECTOR_TAG: &str = "0.162.0";
const COLLECTOR_HTTP_PORT: u16 = 4318;
const COLLECTOR_CONFIG_PATH: &str = "/etc/otelcol/config.yaml";
const COLLECTOR_READY_MESSAGE: &str = "Everything is ready";

// The file exporter writes every export the receiver accepts to stdout as one OTLP/JSON
// line. The collector logs to stderr, so stdout holds exports and nothing else.
const COLLECTOR_CONFIG: &str = r#"
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  file:
    path: /dev/stdout
    flush_interval: 50ms
service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [file]
"#;

const POLL_ATTEMPTS: usize = 200;
const POLL_INTERVAL_MS: u64 = 100;

/// The top-level field of an OTLP/JSON logs export.
const LOGS_FIELD: &str = "resourceLogs";

/// A real OpenTelemetry Collector that accepts OTLP logs over HTTP and echoes every export
/// it received to its stdout.
pub struct OtelCollectorContainer {
    container: ContainerAsync<GenericImage>,
    pub(super) http_endpoint: String,
}

impl OtelCollectorContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(COLLECTOR_IMAGE, COLLECTOR_TAG)
            .with_exposed_port(COLLECTOR_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr(COLLECTOR_READY_MESSAGE))
            .with_copy_to(COLLECTOR_CONFIG_PATH, COLLECTOR_CONFIG.as_bytes().to_vec())
            .with_container_name(fixtures::unique_container_name("otelcol"))
            .start()
            .await
            .map_err(|e| setup_error(format!("Failed to start container: {e}")))?;

        let host = container
            .get_host()
            .await
            .map_err(|e| setup_error(format!("Failed to get host: {e}")))?;
        let http_port = container
            .get_host_port_ipv4(COLLECTOR_HTTP_PORT)
            .await
            .map_err(|e| setup_error(format!("Failed to get HTTP port: {e}")))?;

        let http_endpoint = format!("http://{host}:{http_port}");
        info!("OpenTelemetry Collector listening on {http_endpoint}");

        Ok(Self {
            container,
            http_endpoint,
        })
    }

    /// Every logs export the collector has received so far, in arrival order.
    pub async fn exports(&self) -> Result<Vec<ExportLogsServiceRequest>, TestBinaryError> {
        let stdout =
            self.container
                .stdout_to_vec()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to read collector stdout: {e}"),
                })?;

        let mut exports = Vec::new();
        for line in stdout.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let value: serde_json::Value =
                serde_json::from_slice(line).map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Collector wrote a line that is not JSON: {e}"),
                })?;
            if value.get(LOGS_FIELD).is_none() {
                continue;
            }
            let export =
                serde_json::from_value(value).map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Collector wrote an export that is not OTLP/JSON: {e}"),
                })?;
            exports.push(export);
        }
        Ok(exports)
    }

    /// Poll until the received exports hold `expected` log records.
    pub async fn wait_for_log_records(
        &self,
        expected: usize,
    ) -> Result<Vec<ExportLogsServiceRequest>, TestBinaryError> {
        let mut last = 0;
        for _ in 0..POLL_ATTEMPTS {
            let exports = self.exports().await?;
            last = log_record_count(&exports);
            if last >= expected {
                return Ok(exports);
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected {expected} items from the collector, got {last} after {POLL_ATTEMPTS} polls"
            ),
        })
    }
}

pub fn log_record_count(exports: &[ExportLogsServiceRequest]) -> usize {
    exports
        .iter()
        .flat_map(|export| &export.resource_logs)
        .flat_map(|resource| &resource.scope_logs)
        .map(|scope| scope.log_records.len())
        .sum()
}

fn setup_error(message: String) -> TestBinaryError {
    TestBinaryError::FixtureSetup {
        fixture_type: "OtelCollectorContainer".to_string(),
        message,
    }
}
