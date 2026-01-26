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

use crate::configs::{McpTransport, TelemetryConfig, TelemetryTransport};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing::info;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init_logging(
    telemetry_config: &TelemetryConfig,
    transport: McpTransport,
    version: &'static str,
) {
    // STDIO transport needs stderr output with no ANSI codes
    // HTTP transport can use normal stdout with ANSI
    let (default_level, use_stderr, use_ansi) = match transport {
        McpTransport::Stdio => ("DEBUG", true, false),
        McpTransport::Http => ("INFO", false, true),
    };

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    if telemetry_config.enabled {
        let (logger_provider, tracer_provider) = init_telemetry(telemetry_config, version);

        let service_name = telemetry_config.service_name.clone();
        let tracer = tracer_provider.tracer(service_name);
        global::set_tracer_provider(tracer_provider);
        global::set_text_map_propagator(TraceContextPropagator::new());

        if use_stderr {
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(use_ansi);
            let otel_logs_layer = OpenTelemetryTracingBridge::new(&logger_provider);
            let otel_traces_layer = OpenTelemetryLayer::new(tracer);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_logs_layer)
                .with(otel_traces_layer)
                .init();
        } else {
            let fmt_layer = tracing_subscriber::fmt::layer().with_ansi(use_ansi);
            let otel_logs_layer = OpenTelemetryTracingBridge::new(&logger_provider);
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
                .with_http_client(reqwest::Client::new())
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
                .with_http_client(reqwest::Client::new())
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
