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

use crate::configs::runtime::{LogFormat, LoggingConfig, TelemetryConfig, TelemetryTransport};
use iggy_connector_sdk::LogCallback;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::log_processor_with_async_runtime;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::trace::span_processor_with_async_runtime;
use tracing::info;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

pub fn init_logging(
    telemetry_config: &TelemetryConfig,
    logging_config: &LoggingConfig,
    version: &'static str,
) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));

    // Box so text and JSON share one type, leaving only telemetry to branch on.
    let fmt_layer = match logging_config.format {
        LogFormat::Text => tracing_subscriber::fmt::layer().boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer().json().boxed(),
    };

    if telemetry_config.enabled {
        let (logger_provider, tracer_provider) = init_telemetry(telemetry_config, version);
        let tracer = tracer_provider.tracer(telemetry_config.service_name.clone());
        global::set_tracer_provider(tracer_provider);
        global::set_text_map_propagator(TraceContextPropagator::new());
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(OpenTelemetryTracingBridge::new(&logger_provider))
            .with(OpenTelemetryLayer::new(tracer))
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    }

    if telemetry_config.enabled {
        info!(
            "Logging initialized (format: {}, telemetry enabled, service name: {})",
            logging_config.format, telemetry_config.service_name
        );
    } else {
        info!("Logging initialized (format: {})", logging_config.format);
    }
}

fn init_telemetry(
    telemetry_config: &TelemetryConfig,
    version: &'static str,
) -> (
    opentelemetry_sdk::logs::SdkLoggerProvider,
    opentelemetry_sdk::trace::SdkTracerProvider,
) {
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

    (logger_provider, tracer_provider)
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

/// Log callback that routes plugin logs through the runtime's tracing subscriber.
/// This function is passed to plugins via FFI so their logs appear in the runtime's
/// output and OTEL telemetry.
pub extern "C" fn runtime_log_callback(
    level: u8,
    target_ptr: *const u8,
    target_len: usize,
    message_ptr: *const u8,
    message_len: usize,
) {
    let target = unsafe {
        std::str::from_utf8(std::slice::from_raw_parts(target_ptr, target_len))
            .unwrap_or("connector")
    };
    let message = unsafe {
        std::str::from_utf8(std::slice::from_raw_parts(message_ptr, message_len))
            .unwrap_or("<invalid utf8>")
    };

    match level {
        0 => tracing::trace!(target: "connector", connector_target = target,  message),
        1 => tracing::debug!(target: "connector", connector_target = target,  message),
        2 => tracing::info!(target: "connector", connector_target = target,  message),
        3 => tracing::warn!(target: "connector", connector_target = target,  message),
        _ => tracing::error!(target: "connector", connector_target = target,  message),
    }
}

pub const LOG_CALLBACK: LogCallback = runtime_log_callback;

#[cfg(test)]
mod tests {
    use opentelemetry::logs::{LogRecord, Logger, LoggerProvider};
    use opentelemetry::trace::{Span, Tracer, TracerProvider};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::init_telemetry;
    use crate::configs::runtime::{
        TelemetryConfig, TelemetryLogsConfig, TelemetryTracesConfig, TelemetryTransport,
    };

    const TEST_SCOPE: &str = "connectors-telemetry-test";
    const LOG_BODY: &str = "connector-telemetry-log";
    const TRACE_NAME: &str = "connector-telemetry-span";

    #[test]
    fn given_http_telemetry_when_flushed_should_export_logs_and_traces() {
        let runtime = tokio::runtime::Runtime::new().expect("Tokio runtime should start");
        runtime.block_on(async {
            let collector = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("content-type", "application/x-protobuf"),
                )
                .mount(&collector)
                .await;
            let config = TelemetryConfig {
                enabled: true,
                logs: TelemetryLogsConfig {
                    transport: TelemetryTransport::Http,
                    endpoint: format!("{}/v1/logs", collector.uri()),
                },
                traces: TelemetryTracesConfig {
                    transport: TelemetryTransport::Http,
                    endpoint: format!("{}/v1/traces", collector.uri()),
                },
                ..TelemetryConfig::default()
            };
            let (logger_provider, tracer_provider) =
                init_telemetry(&config, env!("CARGO_PKG_VERSION"));
            let logger = logger_provider.logger(TEST_SCOPE);
            let mut record = logger.create_log_record();
            record.set_body(LOG_BODY.into());
            logger.emit(record);
            let tracer = tracer_provider.tracer(TEST_SCOPE);
            let mut span = tracer.start(TRACE_NAME);
            span.end();

            let results = tokio::task::spawn_blocking(move || {
                (
                    logger_provider.force_flush(),
                    tracer_provider.force_flush(),
                    logger_provider.shutdown(),
                    tracer_provider.shutdown(),
                )
            })
            .await
            .expect("telemetry flush task should complete");
            assert!(results.0.is_ok(), "log export failed: {:?}", results.0);
            assert!(results.1.is_ok(), "trace export failed: {:?}", results.1);
            assert!(results.2.is_ok(), "logger shutdown failed: {:?}", results.2);
            assert!(results.3.is_ok(), "tracer shutdown failed: {:?}", results.3);

            let requests = collector
                .received_requests()
                .await
                .expect("collector should record requests");
            assert_eq!(
                requests.len(),
                2,
                "both telemetry signals should be exported"
            );
            for (path, marker) in [("/v1/logs", LOG_BODY), ("/v1/traces", TRACE_NAME)] {
                let request = requests
                    .iter()
                    .find(|request| request.url.path() == path)
                    .expect("each signal should reach its configured endpoint");
                assert_eq!(
                    request
                        .headers
                        .get("content-type")
                        .expect("OTLP content type"),
                    "application/x-protobuf"
                );
                assert!(
                    request
                        .body
                        .windows(marker.len())
                        .any(|bytes| bytes == marker.as_bytes()),
                    "the exported signal should contain its emitted record: {path}"
                );
            }
        });
    }
}
