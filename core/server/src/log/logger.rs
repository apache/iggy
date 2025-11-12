/* Licensed to the Apache Software Foundation (ASF) under one
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

use crate::VERSION;
use crate::configs::server::{TelemetryConfig, TelemetryTransport};
use crate::configs::system::LoggingConfig;
use crate::log::runtime::CompioRuntime;
use crate::server_error::LogError;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::log_processor_with_async_runtime;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::span_processor_with_async_runtime;
use std::io::{self, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tracing::{Level, event, info, trace};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::field::{RecordFields, VisitOutput};
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::format::DefaultVisitor;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{
    EnvFilter, Layer, Registry, filter::LevelFilter, fmt, fmt::MakeWriter, fmt::format::Format,
    reload, reload::Handle,
};

const IGGY_LOG_FILE_PREFIX: &str = "iggy-server.log";

// Writer that does nothing
struct NullWriter;
impl Write for NullWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// Wrapper around Arc<Mutex<Vec<String>>> to implement Write
struct VecStringWriter(Arc<Mutex<Vec<String>>>);
impl Write for VecStringWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut lock = self.0.lock().unwrap();
        lock.push(String::from_utf8_lossy(buf).into_owned());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Just nop, we don't need to flush anything
        Ok(())
    }
}

// This struct exists solely to implement MakeWriter
struct VecStringMakeWriter(Arc<Mutex<Vec<String>>>);
impl<'a> MakeWriter<'a> for VecStringMakeWriter {
    type Writer = VecStringWriter;

    fn make_writer(&'a self) -> Self::Writer {
        VecStringWriter(self.0.clone())
    }
}

pub trait EarlyLogDumper {
    fn dump_to_file<W: Write>(&self, writer: &mut W);
    fn dump_to_stdout(&self);
}

impl EarlyLogDumper for Logging {
    fn dump_to_file<W: Write>(&self, writer: &mut W) {
        let early_logs_buffer = self.early_logs_buffer.lock().unwrap();
        for log in early_logs_buffer.iter() {
            writer.write_all(log.as_bytes()).unwrap();
        }
    }

    fn dump_to_stdout(&self) {
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        let early_logs_buffer = self.early_logs_buffer.lock().unwrap();
        for log in early_logs_buffer.iter() {
            handle.write_all(log.as_bytes()).unwrap();
        }
    }
}

// Make reload::Layer::new more readable
type ReloadHandle = Handle<Box<dyn Layer<Registry> + Send + Sync>, Registry>;

pub struct Logging {
    stdout_guard: Option<WorkerGuard>,
    stdout_reload_handle: Option<ReloadHandle>,

    file_guard: Option<WorkerGuard>,
    file_reload_handle: Option<ReloadHandle>,

    filtering_stdout_reload_handle: Option<ReloadHandle>,
    filtering_file_reload_handle: Option<ReloadHandle>,

    early_logs_buffer: Arc<Mutex<Vec<String>>>,

    telemetry_config: TelemetryConfig,
}

impl Logging {
    pub fn new(telemetry_config: TelemetryConfig) -> Self {
        Self {
            stdout_guard: None,
            stdout_reload_handle: None,
            file_guard: None,
            file_reload_handle: None,
            filtering_stdout_reload_handle: None,
            filtering_file_reload_handle: None,
            early_logs_buffer: Arc::new(Mutex::new(vec![])),
            telemetry_config,
        }
    }

    pub fn early_init(&mut self) {
        // Initialize layers
        // First layer is filtering based on severity
        // Second layer will just consume drain log entries and has first layer as a dependency
        // Third layer will write to a safe buffer and has first layer as a dependency
        // All layers will be replaced during late_init
        let mut layers = vec![];

        let filtering_level = Self::get_filtering_level(None);
        let (filtering_stdout_layer, filtering_stdout_reload_handle) =
            reload::Layer::new(filtering_level.boxed());
        self.filtering_stdout_reload_handle = Some(filtering_stdout_reload_handle);

        let (filtering_file_layer, filtering_file_reload_handle) =
            reload::Layer::new(filtering_level.boxed());
        self.filtering_file_reload_handle = Some(filtering_file_reload_handle);

        let stdout_layer = fmt::Layer::default()
            .event_format(Self::get_log_format())
            .with_writer(|| NullWriter);
        let (stdout_layer, stdout_layer_reload_handle) = reload::Layer::new(stdout_layer.boxed());
        self.stdout_reload_handle = Some(stdout_layer_reload_handle);
        layers.push(stdout_layer.and_then(filtering_stdout_layer));

        let file_layer = fmt::Layer::default()
            .event_format(Self::get_log_format())
            .with_target(true)
            .with_writer(VecStringMakeWriter(self.early_logs_buffer.clone()))
            .with_ansi(false);
        let (file_layer, file_layer_reload_handle) = reload::Layer::new(file_layer.boxed());
        self.file_reload_handle = Some(file_layer_reload_handle);
        layers.push(file_layer.and_then(filtering_file_layer));

        if !self.telemetry_config.enabled {
            // This is moment when we can start logging something and not worry about losing it.
            Registry::default()
                .with(layers)
                .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
                .init();
            Self::print_build_info();
            return;
        }

        let service_name = self.telemetry_config.service_name.to_owned();
        let resource = Resource::builder()
            .with_service_name(service_name.to_owned())
            .with_attribute(KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                VERSION,
            ))
            .build();

        let logger_provider = match self.telemetry_config.logs.transport {
            TelemetryTransport::GRPC => opentelemetry_sdk::logs::SdkLoggerProvider::builder()
                .with_resource(resource.clone())
                .with_batch_exporter(
                    opentelemetry_otlp::LogExporter::builder()
                        .with_tonic()
                        .with_endpoint(self.telemetry_config.logs.endpoint.clone())
                        .build()
                        .expect("Failed to initialize gRPC logger."),
                )
                .build(),
            TelemetryTransport::HTTP => {
                let log_exporter = opentelemetry_otlp::LogExporter::builder()
                    .with_http()
                    .with_http_client(reqwest::Client::new())
                    .with_endpoint(self.telemetry_config.logs.endpoint.clone())
                    .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                    .build()
                    .expect("Failed to initialize HTTP logger.");
                opentelemetry_sdk::logs::SdkLoggerProvider::builder()
                    .with_resource(resource.clone())
                    .with_log_processor(
                        log_processor_with_async_runtime::BatchLogProcessor::builder(
                            log_exporter,
                            CompioRuntime,
                        )
                        .build(),
                    )
                    .build()
            }
        };

        let tracer_provider = match self.telemetry_config.traces.transport {
            TelemetryTransport::GRPC => opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_batch_exporter(
                    opentelemetry_otlp::SpanExporter::builder()
                        .with_tonic()
                        .with_endpoint(self.telemetry_config.traces.endpoint.clone())
                        .build()
                        .expect("Failed to initialize gRPC tracer."),
                )
                .build(),
            TelemetryTransport::HTTP => {
                let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
                    .with_http()
                    .with_http_client(reqwest::Client::new())
                    .with_endpoint(self.telemetry_config.traces.endpoint.clone())
                    .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                    .build()
                    .expect("Failed to initialize HTTP tracer.");
                opentelemetry_sdk::trace::SdkTracerProvider::builder()
                    .with_resource(resource.clone())
                    .with_span_processor(
                        span_processor_with_async_runtime::BatchSpanProcessor::builder(
                            trace_exporter,
                            CompioRuntime,
                        )
                        .build(),
                    )
                    .build()
            }
        };

        let tracer = tracer_provider.tracer(service_name);
        global::set_tracer_provider(tracer_provider.clone());
        global::set_text_map_propagator(TraceContextPropagator::new());

        Registry::default()
            .with(layers)
            .with(OpenTelemetryTracingBridge::new(&logger_provider))
            .with(OpenTelemetryLayer::new(tracer))
            .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
            .init();
        Self::print_build_info();
    }

    pub fn late_init(
        &mut self,
        base_directory: String,
        config: &LoggingConfig,
    ) -> Result<(), LogError> {
        // Write to stdout and file at the same time.
        // Use the non_blocking appender to avoid blocking the threads.
        // Use the rolling appender to avoid having a huge log file.
        // Make sure logs are dumped to the file during graceful shutdown.

        trace!("Logging config: {}", config);

        let filtering_level = Self::get_filtering_level(Some(config));

        self.filtering_stdout_reload_handle
            .as_ref()
            .ok_or(LogError::FilterReloadFailure)?
            .modify(|layer| *layer = filtering_level.boxed())
            .expect("Failed to modify stdout filtering layer");

        self.filtering_file_reload_handle
            .as_ref()
            .ok_or(LogError::FilterReloadFailure)?
            .modify(|layer| *layer = filtering_level.boxed())
            .expect("Failed to modify file filtering layer");

        // Initialize non-blocking stdout layer
        let (non_blocking_stdout, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
        let stdout_layer = fmt::Layer::default()
            .with_ansi(true)
            .event_format(Self::get_log_format())
            .with_writer(non_blocking_stdout)
            .boxed();
        self.stdout_guard = Some(stdout_guard);

        self.stdout_reload_handle
            .as_ref()
            .ok_or(LogError::StdoutReloadFailure)?
            .modify(|layer| *layer = stdout_layer)
            .expect("Failed to modify stdout layer");

        self.dump_to_stdout();

        // Initialize directory and file for logs
        let base_directory = PathBuf::from(base_directory);
        let logs_subdirectory = PathBuf::from(config.path.clone());
        let logs_path = base_directory.join(logs_subdirectory.clone());
        let file_appender =
            tracing_appender::rolling::hourly(logs_path.clone(), IGGY_LOG_FILE_PREFIX);
        let (mut non_blocking_file, file_guard) = tracing_appender::non_blocking(file_appender);

        self.dump_to_file(&mut non_blocking_file);

        let file_layer = fmt::layer()
            .event_format(Self::get_log_format())
            .with_target(true)
            .with_writer(non_blocking_file)
            .with_ansi(false)
            .fmt_fields(NoAnsiFields {})
            .boxed();

        self.file_guard = Some(file_guard);
        self.file_reload_handle
            .as_ref()
            .ok_or(LogError::FileReloadFailure)?
            .modify(|layer| *layer = file_layer)
            .expect("Failed to modify file layer");
        let level = filtering_level.to_string();

        let print = format!(
            "Logging initialized, logs will be stored at: {logs_path:?}. Logs will be rotated hourly. Log level is: {level}."
        );

        match filtering_level {
            LevelFilter::OFF => (),
            LevelFilter::ERROR => event!(Level::ERROR, "{}", print),
            LevelFilter::WARN => event!(Level::WARN, "{}", print),
            LevelFilter::INFO => event!(Level::INFO, "{}", print),
            LevelFilter::DEBUG => event!(Level::DEBUG, "{}", print),
            LevelFilter::TRACE => event!(Level::TRACE, "{}", print),
        }

        Ok(())
    }

    // RUST_LOG always takes precedence over config
    fn get_filtering_level(config: Option<&LoggingConfig>) -> LevelFilter {
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            // Parse log level from RUST_LOG env variable
            if let Ok(level) = LevelFilter::from_str(&rust_log.to_uppercase()) {
                level
            } else {
                println!("Invalid RUST_LOG value: {rust_log}, falling back to info");
                LevelFilter::INFO
            }
        } else {
            // Parse log level from config
            if let Some(config) = config {
                if let Ok(level) = LevelFilter::from_str(&config.level.to_uppercase()) {
                    level
                } else {
                    println!(
                        "Invalid log level in config: {}, falling back to info",
                        config.level
                    );
                    LevelFilter::INFO
                }
            } else {
                // config not provided
                LevelFilter::INFO
            }
        }
    }

    fn get_log_format() -> Format {
        Format::default().with_thread_names(true)
    }

    fn _install_log_rotation_handler(&self) {
        todo!("Implement log rotation handler based on size and retention time");
    }

    fn print_build_info() {
        if option_env!("IGGY_CI_BUILD") == Some("true") {
            let hash = option_env!("VERGEN_GIT_SHA").unwrap_or("unknown");
            let built_at = option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("unknown");
            let rust_version = option_env!("VERGEN_RUSTC_SEMVER").unwrap_or("unknown");
            let target = option_env!("VERGEN_CARGO_TARGET_TRIPLE").unwrap_or("unknown");
            info!(
                "Version: {VERSION}, hash: {}, built at: {} using rust version: {} for target: {}",
                hash, built_at, rust_version, target
            );
        } else {
            info!(
                "It seems that you are a developer. Environment variable IGGY_CI_BUILD is not set to 'true', skipping build info print."
            )
        }
    }
}

impl Default for Logging {
    fn default() -> Self {
        Self::new(TelemetryConfig::default())
    }
}

// This is a workaround for a bug with `with_ansi` setting in tracing
// Bug thread: https://github.com/tokio-rs/tracing/issues/3116
struct NoAnsiFields {}

impl<'writer> FormatFields<'writer> for NoAnsiFields {
    fn format_fields<R: RecordFields>(
        &self,
        writer: fmt::format::Writer<'writer>,
        fields: R,
    ) -> std::fmt::Result {
        let mut a = DefaultVisitor::new(writer, true);
        fields.record(&mut a);
        a.finish()
    }
}
