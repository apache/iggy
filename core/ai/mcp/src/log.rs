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

use crate::configs::{McpTransport, TelemetryConfig, TelemetryTransport};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::logs::log_processor_with_async_runtime;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::span_processor_with_async_runtime;
use tracing::{error, info};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub struct Telemetry {
    logger_provider: SdkLoggerProvider,
    tracer_provider: SdkTracerProvider,
}

impl Telemetry {
    pub async fn shutdown(self) {
        // Provider shutdown blocks while the processors finish on Tokio workers.
        if let Err(error) = tokio::task::spawn_blocking(move || {
            if let Err(error) = self.tracer_provider.shutdown() {
                error!("Failed to shut down telemetry traces: {error}");
            }
            if let Err(error) = self.logger_provider.shutdown() {
                error!("Failed to shut down telemetry logs: {error}");
            }
        })
        .await
        {
            error!("Telemetry shutdown task failed: {error}");
        }
    }
}

pub fn init_logging(
    telemetry_config: &TelemetryConfig,
    transport: McpTransport,
    version: &'static str,
) -> Option<Telemetry> {
    // STDIO transport needs stderr output with no ANSI codes
    // HTTP transport can use normal stdout with ANSI
    let (default_level, use_stderr, use_ansi) = match transport {
        McpTransport::Stdio => ("DEBUG", true, false),
        McpTransport::Http => ("INFO", false, true),
    };

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    if telemetry_config.enabled {
        let telemetry = init_telemetry(telemetry_config, version);

        let service_name = telemetry_config.service_name.clone();
        let tracer = telemetry.tracer_provider.tracer(service_name);
        global::set_tracer_provider(telemetry.tracer_provider.clone());
        global::set_text_map_propagator(TraceContextPropagator::new());

        if use_stderr {
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(use_ansi);
            let otel_logs_layer = OpenTelemetryTracingBridge::new(&telemetry.logger_provider);
            let otel_traces_layer = OpenTelemetryLayer::new(tracer);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_logs_layer)
                .with(otel_traces_layer)
                .init();
        } else {
            let fmt_layer = tracing_subscriber::fmt::layer().with_ansi(use_ansi);
            let otel_logs_layer = OpenTelemetryTracingBridge::new(&telemetry.logger_provider);
            let otel_traces_layer = OpenTelemetryLayer::new(tracer);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_logs_layer)
                .with(otel_traces_layer)
                .init();
        }

        info!(
            "Logging initialized with telemetry enabled, service name: {}",
            telemetry_config.service_name
        );
        return Some(telemetry);
    } else if use_stderr {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .with_ansi(use_ansi)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_ansi(use_ansi))
            .with(env_filter)
            .init();
    }
    None
}

fn init_telemetry(telemetry_config: &TelemetryConfig, version: &'static str) -> Telemetry {
    let service_name = telemetry_config.service_name.clone();
    let resource = Resource::builder()
        .with_service_name(service_name)
        .with_attribute(KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
            version,
        ))
        .build();

    let logger_provider = init_logs_exporter(telemetry_config, resource.clone());
    let tracer_provider = init_traces_exporter(telemetry_config, resource);

    Telemetry {
        logger_provider,
        tracer_provider,
    }
}

fn init_logs_exporter(
    telemetry_config: &TelemetryConfig,
    resource: Resource,
) -> opentelemetry_sdk::logs::SdkLoggerProvider {
    match telemetry_config.logs.transport {
        TelemetryTransport::Grpc => opentelemetry_sdk::logs::SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(
                opentelemetry_otlp::LogExporter::builder()
                    .with_tonic()
                    .with_endpoint(telemetry_config.logs.endpoint.clone())
                    .build()
                    .expect("Failed to initialize gRPC logger."),
            )
            .build(),
        TelemetryTransport::Http => {
            let log_exporter = opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .with_endpoint(telemetry_config.logs.endpoint.clone())
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .build()
                .expect("Failed to initialize HTTP logger.");
            opentelemetry_sdk::logs::SdkLoggerProvider::builder()
                .with_resource(resource)
                .with_log_processor(
                    log_processor_with_async_runtime::BatchLogProcessor::builder(
                        log_exporter,
                        Tokio,
                    )
                    .build(),
                )
                .build()
        }
    }
}

fn init_traces_exporter(
    telemetry_config: &TelemetryConfig,
    resource: Resource,
) -> opentelemetry_sdk::trace::SdkTracerProvider {
    match telemetry_config.traces.transport {
        TelemetryTransport::Grpc => opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(
                opentelemetry_otlp::SpanExporter::builder()
                    .with_tonic()
                    .with_endpoint(telemetry_config.traces.endpoint.clone())
                    .build()
                    .expect("Failed to initialize gRPC tracer."),
            )
            .build(),
        TelemetryTransport::Http => {
            let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(telemetry_config.traces.endpoint.clone())
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .build()
                .expect("Failed to initialize HTTP tracer.");
            opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_resource(resource)
                .with_span_processor(
                    span_processor_with_async_runtime::BatchSpanProcessor::builder(
                        trace_exporter,
                        Tokio,
                    )
                    .build(),
                )
                .build()
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Bytes;
    use axum::http::{HeaderMap, StatusCode, Uri};
    use opentelemetry::logs::{LogRecord, Logger, LoggerProvider};
    use opentelemetry::trace::{Span, Tracer, TracerProvider};
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot};

    use super::init_telemetry;
    use crate::configs::{
        TelemetryConfig, TelemetryLogsConfig, TelemetryTracesConfig, TelemetryTransport,
    };

    const SIGNAL_COUNT: usize = 2;
    const TEST_SCOPE: &str = "mcp-telemetry-test";
    const LOG_BODY: &str = "mcp-telemetry-log";
    const TRACE_NAME: &str = "mcp-telemetry-span";

    #[tokio::test]
    async fn given_http_telemetry_when_shutdown_should_export_logs_and_traces() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local telemetry collector");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("collector address")
        );
        let (requests, mut received) = mpsc::channel(SIGNAL_COUNT);
        let collector = Router::new().fallback(move |uri: Uri, headers: HeaderMap, body: Bytes| {
            let requests = requests.clone();
            async move {
                requests
                    .try_send((uri, headers, body))
                    .expect("record exported signal");
                StatusCode::OK
            }
        });
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, collector)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .expect("serve telemetry collector");
        });
        let config = TelemetryConfig {
            enabled: true,
            logs: TelemetryLogsConfig {
                transport: TelemetryTransport::Http,
                endpoint: format!("{endpoint}/v1/logs"),
            },
            traces: TelemetryTracesConfig {
                transport: TelemetryTransport::Http,
                endpoint: format!("{endpoint}/v1/traces"),
            },
            ..TelemetryConfig::default()
        };
        let telemetry = init_telemetry(&config, env!("CARGO_PKG_VERSION"));
        let logger = telemetry.logger_provider.logger(TEST_SCOPE);
        let mut record = logger.create_log_record();
        record.set_body(LOG_BODY.into());
        logger.emit(record);
        let tracer = telemetry.tracer_provider.tracer(TEST_SCOPE);
        let mut span = tracer.start(TRACE_NAME);
        span.end();

        telemetry.shutdown().await;
        stop.send(()).expect("stop telemetry collector");
        server.await.expect("collector task should complete");

        let requests: Vec<_> = std::iter::from_fn(|| received.try_recv().ok()).collect();
        assert_eq!(
            requests.len(),
            SIGNAL_COUNT,
            "both signals must be exported"
        );
        for (path, marker) in [("/v1/logs", LOG_BODY), ("/v1/traces", TRACE_NAME)] {
            let (_, headers, body) = requests
                .iter()
                .find(|(uri, _, _)| uri.path() == path)
                .expect("signal must reach its configured endpoint");
            assert_eq!(headers["content-type"], "application/x-protobuf");
            assert!(
                body.windows(marker.len())
                    .any(|bytes| bytes == marker.as_bytes()),
                "exported signal must contain its emitted record: {path}"
            );
        }
    }
}
