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
use rolling_file::{BasicRollingFileAppender, RollingConditionBasic};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::field::{RecordFields, VisitOutput};
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::format::DefaultVisitor;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{
    EnvFilter, Layer, Registry, filter::LevelFilter, fmt, fmt::MakeWriter, fmt::format::Format,
    layer::Layered, reload, reload::Handle,
};

const IGGY_LOG_FILE_PREFIX: &str = "iggy-server.log";
const MIN_DISK_SPACE_BYTES: u64 = 10 * 1024 * 1024;

// Writer that does nothing
struct NullWriter;
impl Write for NullWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Wrapper around Arc<Mutex<Vec<String>>> to implement Write
struct VecStringWriter(Arc<Mutex<Vec<String>>>);
impl Write for VecStringWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut lock = self.0.lock().unwrap();
        lock.push(String::from_utf8_lossy(buf).into_owned());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
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

// Type aliases for reload handles with different subscriber types
type ReloadHandle<S> = Handle<Box<dyn Layer<S> + Send + Sync>, S>;
type EnvFilterReloadHandle = Handle<EnvFilter, Registry>;

// Subscriber type after applying EnvFilter to Registry
type FilteredRegistry = Layered<reload::Layer<EnvFilter, Registry>, Registry>;

pub struct Logging {
    stdout_guard: Option<WorkerGuard>,
    stdout_reload_handle: Option<ReloadHandle<FilteredRegistry>>,

    file_guard: Option<WorkerGuard>,
    file_reload_handle: Option<ReloadHandle<FilteredRegistry>>,

    env_filter_reload_handle: Option<EnvFilterReloadHandle>,

    otel_logs_reload_handle: Option<ReloadHandle<FilteredRegistry>>,
    otel_traces_reload_handle: Option<ReloadHandle<FilteredRegistry>>,

    early_logs_buffer: Arc<Mutex<Vec<String>>>,
    rotation_should_stop: Arc<AtomicBool>,
}

impl Logging {
    pub fn new() -> Self {
        Self {
            stdout_guard: None,
            stdout_reload_handle: None,
            file_guard: None,
            file_reload_handle: None,
            env_filter_reload_handle: None,
            otel_logs_reload_handle: None,
            otel_traces_reload_handle: None,
            early_logs_buffer: Arc::new(Mutex::new(vec![])),
            rotation_should_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn early_init(&mut self) {
        // Initialize layers with placeholders that will be replaced during late_init.
        // This allows logging to work before config is parsed.
        //
        // Layer structure:
        // - EnvFilter: Applied FIRST via .with() to filter all subsequent layers
        // - Stdout layer: writes to NullWriter (discarded), replaced with real stdout in late_init
        // - File layer: writes to in-memory buffer, replaced with rolling file in late_init
        // - Telemetry layers: no-op placeholders (LevelFilter::OFF), replaced if telemetry enabled
        //
        // EnvFilter MUST be applied first. All subsequent layers are typed for FilteredRegistry.
        // This ensures the filter is evaluated before any output layer processes events.

        // EnvFilter applied FIRST - wraps all subsequent layers.
        // RUST_LOG takes precedence; otherwise defaults to INFO until late_init reloads with config.
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));
        let (env_filter_layer, env_filter_reload_handle) = reload::Layer::new(env_filter);
        self.env_filter_reload_handle = Some(env_filter_reload_handle);

        // All output layers are typed for FilteredRegistry (Registry + EnvFilter)
        let mut layers: Vec<Box<dyn Layer<FilteredRegistry> + Send + Sync>> = vec![];

        // Telemetry layers - no-op placeholders (LevelFilter::OFF) replaced in late_init if enabled
        let (otel_logs_layer, otel_logs_reload_handle) =
            reload::Layer::new(LevelFilter::OFF.boxed());
        self.otel_logs_reload_handle = Some(otel_logs_reload_handle);
        layers.push(Box::new(otel_logs_layer));

        let (otel_traces_layer, otel_traces_reload_handle) =
            reload::Layer::new(LevelFilter::OFF.boxed());
        self.otel_traces_reload_handle = Some(otel_traces_reload_handle);
        layers.push(Box::new(otel_traces_layer));

        // Output layers
        let stdout_layer = fmt::Layer::default()
            .event_format(Self::get_log_format())
            .with_writer(|| NullWriter);
        let (stdout_layer, stdout_layer_reload_handle) = reload::Layer::new(stdout_layer.boxed());
        self.stdout_reload_handle = Some(stdout_layer_reload_handle);
        layers.push(Box::new(stdout_layer));

        let file_layer = fmt::Layer::default()
            .event_format(Self::get_log_format())
            .with_target(true)
            .with_writer(VecStringMakeWriter(self.early_logs_buffer.clone()))
            .with_ansi(false);
        let (file_layer, file_layer_reload_handle) = reload::Layer::new(file_layer.boxed());
        self.file_reload_handle = Some(file_layer_reload_handle);
        layers.push(Box::new(file_layer));

        Registry::default()
            .with(env_filter_layer)
            .with(layers)
            .init();
        Self::print_build_info();
    }

    pub fn late_init(
        &mut self,
        base_directory: String,
        config: &LoggingConfig,
        telemetry_config: &TelemetryConfig,
    ) -> Result<(), LogError> {
        // Write to stdout and file at the same time.
        // Use the non_blocking appender to avoid blocking the threads.
        // Use the rolling appender to avoid having a huge log file.
        // Make sure logs are dumped to the file during graceful shutdown.

        trace!("Logging config: {config}");

        // Reload EnvFilter with config level if RUST_LOG is not set.
        // Config level supports EnvFilter syntax (e.g., "warn,server=debug,iggy=trace").
        let log_filter = if std::env::var("RUST_LOG").is_ok() {
            // RUST_LOG takes precedence - don't reload, keep what was set in early_init
            std::env::var("RUST_LOG").unwrap()
        } else {
            // Use config level as EnvFilter directive
            let env_filter = EnvFilter::new(&config.level);
            self.env_filter_reload_handle
                .as_ref()
                .ok_or(LogError::FilterReloadFailure)?
                .modify(|filter| *filter = env_filter)
                .expect("Failed to modify EnvFilter");
            config.level.clone()
        };

        // Initialize non-blocking stdout layer
        let (non_blocking_stdout, stdout_guard) = tracing_appender::non_blocking(io::stdout());
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

        // Initialize file logging if enabled
        let logs_path = if config.file_enabled {
            let base_directory = PathBuf::from(base_directory);
            let logs_subdirectory = PathBuf::from(config.path.clone());
            let logs_subdirectory = logs_subdirectory
                .canonicalize()
                .unwrap_or(logs_subdirectory);
            let logs_path = base_directory.join(logs_subdirectory);

            if let Err(e) = fs::create_dir_all(&logs_path) {
                warn!("Failed to create logs directory {logs_path:?}: {e}");
                return Err(LogError::FileReloadFailure);
            }

            // Check available disk space (at least 10MB)
            let min_disk_space: u64 = MIN_DISK_SPACE_BYTES;
            if let Ok(available_space) = fs2::available_space(&logs_path) {
                if available_space < min_disk_space {
                    warn!(
                        "Low disk space for logs. Available: {available_space} bytes, Recommended: {min_disk_space} bytes"
                    );
                }
            } else {
                warn!("Failed to check available disk space for logs directory: {logs_path:?}");
            }

            let max_files = Self::calculate_max_files(
                config.max_total_size.as_bytes_u64(),
                config.max_size.as_bytes_u64(),
            );

            let condition = RollingConditionBasic::new()
                .max_size(config.max_size.as_bytes_u64())
                .hourly();

            let file_appender = BasicRollingFileAppender::new(
                logs_path.join(IGGY_LOG_FILE_PREFIX),
                condition,
                max_files,
            )
            .map_err(|e| {
                error!("Failed to create file appender: {e}");
                LogError::FileReloadFailure
            })?;

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

            Some(logs_path)
        } else {
            None
        };

        // Initialize telemetry if enabled
        if telemetry_config.enabled {
            self.init_telemetry(telemetry_config)?;
        }

        self._install_log_rotation_handler(config, logs_path.as_ref());

        if let Some(logs_path) = logs_path {
            info!(
                "Logging initialized, logs will be stored at: {logs_path:?}. Logs will be rotated based on size. Log filter: {log_filter}."
            );
        } else {
            info!("Logging initialized (file output disabled). Log filter: {log_filter}.");
        }

        Ok(())
    }

    fn init_telemetry(&mut self, telemetry_config: &TelemetryConfig) -> Result<(), LogError> {
        let service_name = telemetry_config.service_name.to_owned();
        let resource = Resource::builder()
            .with_service_name(service_name.clone())
            .with_attribute(KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                VERSION,
            ))
            .build();

        let logger_provider = match telemetry_config.logs.transport {
            TelemetryTransport::GRPC => opentelemetry_sdk::logs::SdkLoggerProvider::builder()
                .with_resource(resource.clone())
                .with_batch_exporter(
                    opentelemetry_otlp::LogExporter::builder()
                        .with_tonic()
                        .with_endpoint(telemetry_config.logs.endpoint.clone())
                        .build()
                        .expect("Failed to initialize gRPC logger."),
                )
                .build(),
            TelemetryTransport::HTTP => {
                let log_exporter = opentelemetry_otlp::LogExporter::builder()
                    .with_http()
                    .with_http_client(reqwest::Client::new())
                    .with_endpoint(telemetry_config.logs.endpoint.clone())
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

        let tracer_provider = match telemetry_config.traces.transport {
            TelemetryTransport::GRPC => opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_batch_exporter(
                    opentelemetry_otlp::SpanExporter::builder()
                        .with_tonic()
                        .with_endpoint(telemetry_config.traces.endpoint.clone())
                        .build()
                        .expect("Failed to initialize gRPC tracer."),
                )
                .build(),
            TelemetryTransport::HTTP => {
                let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
                    .with_http()
                    .with_http_client(reqwest::Client::new())
                    .with_endpoint(telemetry_config.traces.endpoint.clone())
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

        // Reload telemetry layers with actual implementations
        self.otel_logs_reload_handle
            .as_ref()
            .ok_or(LogError::FilterReloadFailure)?
            .modify(|layer| *layer = OpenTelemetryTracingBridge::new(&logger_provider).boxed())
            .expect("Failed to modify telemetry logs layer");

        self.otel_traces_reload_handle
            .as_ref()
            .ok_or(LogError::FilterReloadFailure)?
            .modify(|layer| *layer = OpenTelemetryLayer::new(tracer).boxed())
            .expect("Failed to modify telemetry traces layer");

        info!(
            "Telemetry initialized with service name: {config_service_name}",
            config_service_name = telemetry_config.service_name
        );
        Ok(())
    }

    fn get_log_format() -> Format {
        Format::default().with_thread_names(true)
    }

    fn print_build_info() {
        if option_env!("IGGY_CI_BUILD") == Some("true") {
            let hash = option_env!("VERGEN_GIT_SHA").unwrap_or("unknown");
            let built_at = option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("unknown");
            let rust_version = option_env!("VERGEN_RUSTC_SEMVER").unwrap_or("unknown");
            let target = option_env!("VERGEN_CARGO_TARGET_TRIPLE").unwrap_or("unknown");
            info!(
                "Version: {VERSION}, hash: {hash}, built at: {built_at} using rust version: {rust_version} for target: {target}"
            );
        } else {
            info!(
                "It seems that you are a developer. Environment variable IGGY_CI_BUILD is not set to 'true', skipping build info print."
            )
        }
    }

    fn calculate_max_files(max_total_size_bytes: u64, max_file_size_bytes: u64) -> usize {
        if max_file_size_bytes == 0 {
            return 10;
        }

        let max_files = max_total_size_bytes / max_file_size_bytes;
        max_files.clamp(1, 1000) as usize
    }

    fn _install_log_rotation_handler(&self, config: &LoggingConfig, logs_path: Option<&PathBuf>) {
        if let Some(logs_path) = logs_path {
            let path = logs_path.to_path_buf();
            let max_size = config.max_size.as_bytes_u64();
            let retention = config.retention.get_duration();
            let check_interval = config.rotation_check_interval;
            let should_stop = Arc::clone(&self.rotation_should_stop);

            std::thread::spawn(move || {
                loop {
                    if should_stop.load(Ordering::Relaxed) {
                        break;
                    }

                    std::thread::sleep(Duration::from_secs(check_interval));

                    if should_stop.load(Ordering::Relaxed) {
                        break;
                    }

                    Self::cleanup_log_files(&path, retention, max_size);
                }
            });
        }
    }

    fn read_log_files(
        logs_path: &PathBuf,
    ) -> Vec<(fs::DirEntry, std::time::SystemTime, Duration, u64)> {
        use std::fs;
        use std::time::UNIX_EPOCH;

        let entries = match fs::read_dir(logs_path) {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read log directory {logs_path:?}: {e}");
                return Vec::new();
            }
        };

        let mut file_entries = Vec::new();

        for entry in entries.flatten() {
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(e) => {
                    warn!(
                        "Failed to get metadata for {entry_path:?}: {e}",
                        entry_path = entry.path()
                    );
                    continue;
                }
            };

            if !metadata.is_file() {
                continue;
            }

            let modified = match metadata.modified() {
                Ok(modified) => modified,
                Err(e) => {
                    warn!(
                        "Failed to get modification time for {entry_path:?}: {e}",
                        entry_path = entry.path()
                    );
                    continue;
                }
            };

            let elapsed = match modified.duration_since(UNIX_EPOCH) {
                Ok(elapsed) => elapsed,
                Err(e) => {
                    warn!(
                        "Failed to calculate elapsed time for {entry_path:?}: {e}",
                        entry_path = entry.path()
                    );
                    continue;
                }
            };

            let file_size = metadata.len();
            file_entries.push((entry, modified, elapsed, file_size));
        }

        file_entries
    }

    fn cleanup_log_files(logs_path: &PathBuf, retention: Duration, max_size_bytes: u64) {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        debug!(
            "Starting log cleanup for directory: {logs_path:?}, retention: {retention:?}, max_size: {max_size_bytes} bytes"
        );

        let mut file_entries = Self::read_log_files(logs_path);

        debug!(
            "Processed {file_entries_len} log files from directory: {logs_path:?}",
            file_entries_len = file_entries.len(),
        );

        let mut removed_files_count = 0;

        if !retention.is_zero() {
            let cutoff = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(now) => now - retention,
                Err(e) => {
                    warn!("Failed to get current time: {e}");
                    return;
                }
            };

            for (entry, _, elapsed, _) in &file_entries {
                if *elapsed < cutoff {
                    if let Err(e) = fs::remove_file(entry.path()) {
                        warn!(
                            "Failed to remove old log file {entry_path:?}: {e}",
                            entry_path = entry.path()
                        );
                    } else {
                        debug!(
                            "Removed old log file: {entry_path:?}",
                            entry_path = entry.path()
                        );
                        removed_files_count += 1;
                    }
                }
            }
        }

        if max_size_bytes == 0 {
            return;
        }

        file_entries = Self::read_log_files(logs_path);

        let total_size: u64 = file_entries.iter().map(|(_, _, _, size)| *size).sum();

        if total_size > max_size_bytes {
            file_entries.sort_by_key(|(_, modified, _, _)| *modified);

            let mut current_size = total_size;
            for (entry, _, _, file_size) in file_entries {
                if current_size <= max_size_bytes {
                    break;
                }

                if let Err(e) = fs::remove_file(entry.path()) {
                    warn!(
                        "Failed to remove log file {entry_path:?}: {e}",
                        entry_path = entry.path()
                    );
                } else {
                    debug!(
                        "Removed log file to control size: {entry_path:?}",
                        entry_path = entry.path()
                    );
                    current_size = current_size.saturating_sub(file_size);
                    removed_files_count += 1;
                }
            }
        }

        info!(
            "Completed log cleanup for directory: {logs_path:?}. Removed {removed_files_count} files."
        );
    }
}

impl Default for Logging {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Logging {
    fn drop(&mut self) {
        self.rotation_should_stop.store(true, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::IggyByteSize;
    use tempfile::TempDir;

    #[test]
    fn test_log_directory_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let base_path = temp_dir.path().to_str().unwrap().to_string();
        let log_subdir = "test_logs".to_string();

        let log_path = PathBuf::from(&base_path).join(&log_subdir);
        assert!(!log_path.exists());
        fs::create_dir_all(&log_path).expect("Failed to create log directory");
        assert!(log_path.exists());
    }

    #[test]
    fn test_disk_space_check() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let log_path = temp_dir.path();
        let result = fs2::available_space(log_path);
        assert!(result.is_ok());

        let available_space = result.unwrap();
        assert!(available_space > 0);
    }

    #[test]
    fn test_user_defined_max_total_size_parsing() {
        let size = IggyByteSize::from(5_000_000_000); // 5 GB
        assert_eq!(size.as_bytes_u64(), 5_000_000_000);
    }

    #[test]
    fn test_user_defined_rotation_check_interval_parsing() {
        let interval: u64 = 1800; // 30min
        assert_eq!(interval, 1800);
    }

    #[test]
    fn test_logging_config_default_values() {
        let config = LoggingConfig::default();
        assert_eq!(config.max_total_size.as_bytes_u64(), 4_000_000_000); // 4 GB
        assert_eq!(config.rotation_check_interval, 3600); // 1h
    }

    #[test]
    fn test_graceful_shutdown_flag() {
        let logging = Logging::new();
        assert!(!logging.rotation_should_stop.load(Ordering::Relaxed));
        drop(logging);
    }

    #[test]
    fn test_config_values_used_in_handlers() {
        let config = LoggingConfig {
            path: "logs".to_string(),
            level: "info".to_string(),
            file_enabled: true,
            max_size: IggyByteSize::from(500_000_000), // 500 MB
            max_total_size: IggyByteSize::from(5_000_000_000), // 5 GB
            rotation_check_interval: 1800,             // 30min
            retention: Duration::from_secs(7 * 24 * 60 * 60).into(), // 7 Days
            sysinfo_print_interval: Duration::from_secs(10).into(), // 10s
        };

        assert_eq!(config.rotation_check_interval, 1800);
        assert_eq!(config.max_total_size.as_bytes_u64(), 5_000_000_000);
    }

    #[test]
    fn test_calculate_max_files() {
        assert_eq!(Logging::calculate_max_files(0, 100), 1);
        assert_eq!(Logging::calculate_max_files(100, 0), 10);
        assert_eq!(Logging::calculate_max_files(1000, 100), 10);
        assert_eq!(Logging::calculate_max_files(500, 100), 5);
        assert_eq!(Logging::calculate_max_files(2000, 100), 20);
        assert_eq!(Logging::calculate_max_files(1000000, 1), 1000);
        assert_eq!(Logging::calculate_max_files(50, 100), 1);
    }

    #[test]
    fn test_calculate_max_files_with_values() {
        let total_size = 10_000_000_000; // 10 GB
        let file_size = 500_000_000; // 500 MB
        assert_eq!(Logging::calculate_max_files(total_size, file_size), 20);

        let total_size = 5_000_000_000; // 5 GB
        let file_size = 250_000_000; // 250 MB
        assert_eq!(Logging::calculate_max_files(total_size, file_size), 20);

        let total_size = 1_000_000_000; // 1 GB
        let file_size = 100_000_000; // 100 MB
        assert_eq!(Logging::calculate_max_files(total_size, file_size), 10);
    }

    #[test]
    fn test_cleanup_log_files_functions() {
        use std::time::Duration;
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let log_path = temp_dir.path().to_path_buf();
        Logging::cleanup_log_files(&log_path, Duration::from_secs(3600), 1024 * 1024);
    }

    #[test]
    fn test_logging_creation() {
        let logging = Logging::new();
        assert!(logging.stdout_guard.is_none());
        assert!(logging.file_guard.is_none());
        assert!(logging.env_filter_reload_handle.is_none());
    }
}
