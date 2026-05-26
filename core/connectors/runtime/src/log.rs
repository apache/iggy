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

use crate::configs::runtime::{LogFormat, LoggingConfig, TelemetryConfig, TelemetryTransport};
use iggy_connector_sdk::LogCallback;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing::info;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init_logging(
    telemetry_config: &TelemetryConfig,
    logging_config: &LoggingConfig,
    version: &'static str,
) {
    let env_filter =
        || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));

    match (telemetry_config.enabled, logging_config.format) {
        (true, LogFormat::Text) => {
            let (logger_provider, tracer_provider) = init_telemetry(telemetry_config, version);
            let tracer = tracer_provider.tracer(telemetry_config.service_name.clone());
            global::set_tracer_provider(tracer_provider);
            global::set_text_map_propagator(TraceContextPropagator::new());
            tracing_subscriber::registry()
                .with(env_filter())
                .with(tracing_subscriber::fmt::layer())
                .with(OpenTelemetryTracingBridge::new(&logger_provider))
                .with(OpenTelemetryLayer::new(tracer))
                .init();
        }
        (true, LogFormat::Json) => {
            let (logger_provider, tracer_provider) = init_telemetry(telemetry_config, version);
            let tracer = tracer_provider.tracer(telemetry_config.service_name.clone());
            global::set_tracer_provider(tracer_provider);
            global::set_text_map_propagator(TraceContextPropagator::new());
            tracing_subscriber::registry()
                .with(env_filter())
                .with(tracing_subscriber::fmt::layer().json())
                .with(OpenTelemetryTracingBridge::new(&logger_provider))
                .with(OpenTelemetryLayer::new(tracer))
                .init();
        }
        (false, LogFormat::Text) => {
            tracing_subscriber::registry()
                .with(env_filter())
                .with(tracing_subscriber::fmt::layer())
                .init();
        }
        (false, LogFormat::Json) => {
            tracing_subscriber::registry()
                .with(env_filter())
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
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
                .with_batch_exporter(log_exporter)
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
                .with_batch_exporter(trace_exporter)
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
