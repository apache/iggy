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

mod batch;
mod exporter;
mod signal;

use async_trait::async_trait;
use batch::{Packed, Prepared};
use exporter::{ExportError, ExporterSettings, HttpExporter};
use iggy_connector_sdk::retry::{RetryPolicy, retry_async};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Schema, Sink, TopicMetadata, sink_connector,
};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use reqwest::Url;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use signal::{ExportRequest, ExportResponse};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

sink_connector!(OtlpSink);

const DEFAULT_TIMEOUT: &str = "10s";
/// About 45 s of backoff with the default delays, long enough for a collector to restart.
/// The runtime commits offsets before `consume` runs, so a request that runs out of
/// attempts is lost.
const DEFAULT_MAX_RETRIES: u32 = 8;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_RETRY_DELAY: &str = "10s";
/// Just under the 4 MiB gRPC receive limit that collectors and most OTLP backends default to.
const DEFAULT_MAX_REQUEST_BYTES: usize = 4_000_000;
/// The OTLP/HTTP path of the logs signal, appended to the endpoint.
const LOGS_PATH: &str = "/v1/logs";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    #[default]
    None,
    Gzip,
}

// `Deserialize` only. Nothing re-serializes a plugin config, and leaving `Serialize` off is
// what keeps the header values, which carry API keys, unserializable.
#[derive(Debug, Clone, Deserialize)]
pub struct OtlpSinkConfig {
    /// The base URL, `http(s)://host:4318`, to which `/v1/logs` is appended.
    pub endpoint: String,
    pub compression: Option<Compression>,
    /// Sent with every export as HTTP headers. Values are secrets.
    pub headers: Option<HashMap<String, SecretString>>,
    /// Per-attempt timeout, e.g. "10s". Must be greater than zero.
    pub timeout: Option<String>,
    /// Total attempts per request, including the first. `0` and `1` disable retries.
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    /// Upper bound on one export request's encoded size. Messages are never split, so a
    /// single message above it is still sent, on its own.
    pub max_request_bytes: Option<usize>,
    pub verbose_logging: Option<bool>,
}

/// Config resolved and checked in `open()`: defaults applied, durations parsed.
#[derive(Debug)]
struct Settings {
    endpoint: Url,
    gzip: bool,
    timeout: Duration,
    retry: RetryPolicy,
    max_request_bytes: usize,
    verbose: bool,
}

impl Settings {
    fn resolve(id: u32, config: &OtlpSinkConfig) -> Result<Self, Error> {
        let endpoint = Url::parse(&config.endpoint).map_err(|error| {
            Error::InvalidConfigValue(format!("endpoint is not a URL: {error}"))
        })?;
        // `localhost:4318` parses as a URL with the scheme `localhost`.
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(Error::InvalidConfigValue(format!(
                "endpoint must start with http:// or https://, not \"{}:\"",
                endpoint.scheme()
            )));
        }
        // The per-signal endpoint variable of OTLP exporters, OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
        // holds the full URL. Copied here, it would get the path a second time and every
        // export would fail with a 404.
        if endpoint.path().trim_end_matches('/').ends_with(LOGS_PATH) {
            return Err(Error::InvalidConfigValue(format!(
                "endpoint must be the base URL without \"{LOGS_PATH}\", which the sink appends"
            )));
        }

        let timeout = parse_duration(config.timeout.as_deref(), DEFAULT_TIMEOUT, "timeout")?;
        if timeout.is_zero() {
            return Err(Error::InvalidConfigValue(
                "timeout must be greater than 0".to_owned(),
            ));
        }
        let retry_delay = parse_duration(
            config.retry_delay.as_deref(),
            DEFAULT_RETRY_DELAY,
            "retry_delay",
        )?;
        let max_retry_delay = parse_duration(
            config.max_retry_delay.as_deref(),
            DEFAULT_MAX_RETRY_DELAY,
            "max_retry_delay",
        )?;
        if retry_delay > max_retry_delay {
            warn!(
                "OTLP sink ID: {id} retry_delay ({retry_delay:?}) exceeds max_retry_delay ({max_retry_delay:?}), so every backoff is max_retry_delay"
            );
        }

        let max_request_bytes = config
            .max_request_bytes
            .unwrap_or(DEFAULT_MAX_REQUEST_BYTES);
        if max_request_bytes == 0 {
            return Err(Error::InvalidConfigValue(
                "max_request_bytes must be greater than 0".to_owned(),
            ));
        }

        let mut header_names = HashSet::new();
        for name in config.headers.iter().flatten().map(|(name, _)| name) {
            if !header_names.insert(name.to_ascii_lowercase()) {
                return Err(Error::InvalidConfigValue(format!(
                    "header \"{name}\" is set twice: header names are case-insensitive"
                )));
            }
        }

        Ok(Self {
            endpoint,
            gzip: config.compression.unwrap_or_default() == Compression::Gzip,
            timeout,
            retry: RetryPolicy {
                max_attempts: config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
                base_delay: retry_delay,
                max_delay: max_retry_delay,
            },
            max_request_bytes,
            verbose: config.verbose_logging.unwrap_or(false),
        })
    }
}

/// What `open()` builds from the config.
#[derive(Debug)]
struct Opened {
    settings: Settings,
    exporter: HttpExporter,
}

impl Opened {
    fn new(id: u32, config: &OtlpSinkConfig) -> Result<Self, Error> {
        let settings = Settings::resolve(id, config)?;
        let headers = config
            .headers
            .iter()
            .flatten()
            .map(|(key, value)| (key.clone(), value.expose_secret().to_owned()))
            .collect();
        let exporter = HttpExporter::new(&ExporterSettings {
            endpoint: &settings.endpoint,
            path: LOGS_PATH,
            gzip: settings.gzip,
            timeout: settings.timeout,
            headers,
        })?;
        Ok(Self { settings, exporter })
    }
}

/// Export counters, logged at `close()` and, with `verbose_logging`, after every batch.
///
/// The runtime has no registry a plugin can publish metrics to, so these do not reach the
/// runtime's `/metrics` endpoint. Its own `iggy_connector_messages_processed_total` and
/// `iggy_connector_errors_total` still count every call to this sink.
#[derive(Debug, Default)]
struct Counters {
    messages_sent: AtomicU64,
    messages_failed: AtomicU64,
    messages_dropped: AtomicU64,
    batches_sent: AtomicU64,
    batches_failed: AtomicU64,
    items_rejected: AtomicU64,
    retries: AtomicU64,
    batch_latency_micros: AtomicU64,
}

impl Counters {
    fn record_attempts(&self, attempts: u32, elapsed: Duration) {
        self.retries
            .fetch_add(u64::from(attempts.saturating_sub(1)), Ordering::Relaxed);
        self.batch_latency_micros.fetch_add(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

impl fmt::Display for Counters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let batches = load(&self.batches_sent) + load(&self.batches_failed);
        let average_latency_ms = load(&self.batch_latency_micros)
            .checked_div(batches)
            .unwrap_or(0) as f64
            / 1_000.0;
        write!(
            f,
            "messages_sent: {}, messages_failed: {}, messages_dropped: {}, batches_sent: {}, \
             batches_failed: {}, items_rejected: {}, retries: {}, average_batch_latency_ms: {average_latency_ms:.3}",
            load(&self.messages_sent),
            load(&self.messages_failed),
            load(&self.messages_dropped),
            load(&self.batches_sent),
            load(&self.batches_failed),
            load(&self.items_rejected),
            load(&self.retries),
        )
    }
}

#[derive(Debug)]
pub struct OtlpSink {
    id: u32,
    config: OtlpSinkConfig,
    /// Set by `open()`.
    opened: Option<Opened>,
    counters: Counters,
}

impl OtlpSink {
    pub fn new(id: u32, config: OtlpSinkConfig) -> Self {
        Self {
            id,
            config,
            opened: None,
            counters: Counters::default(),
        }
    }

    async fn send_all<R: ExportRequest>(
        &self,
        opened: &Opened,
        prepared: Prepared,
        received: usize,
        schema: Schema,
    ) -> Result<(), Error> {
        let Prepared {
            requests,
            dropped,
            first_error: decode_error,
        } = prepared;

        if dropped > 0 {
            self.counters
                .messages_dropped
                .fetch_add(dropped, Ordering::Relaxed);
            warn!(
                "OTLP sink ID: {} dropped {dropped} of {received} messages that hold no protobuf OTLP export request (stream schema: {schema}). First reason: {}",
                self.id,
                decode_error
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            );
        }
        let oversized = requests.iter().filter(|packed| packed.oversized).count();
        if oversized > 0 {
            warn!(
                "OTLP sink ID: {} sending {oversized} messages alone because each exceeds max_request_bytes ({})",
                self.id, opened.settings.max_request_bytes
            );
        }

        let request_count = requests.len();
        let mut exported = 0u64;
        let mut export_error = None;
        for packed in requests {
            let messages = packed.messages;
            match self.send::<R>(opened, packed).await {
                Ok(()) => {
                    exported += messages;
                    self.counters
                        .messages_sent
                        .fetch_add(messages, Ordering::Relaxed);
                    self.counters.batches_sent.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    self.counters
                        .messages_failed
                        .fetch_add(messages, Ordering::Relaxed);
                    self.counters.batches_failed.fetch_add(1, Ordering::Relaxed);
                    export_error.get_or_insert(error);
                }
            }
        }

        if opened.settings.verbose {
            info!(
                "OTLP sink ID: {} exported {exported} of {received} messages in {request_count} requests. {}",
                self.id, self.counters
            );
        } else {
            debug!(
                "OTLP sink ID: {} exported {exported} of {received} messages in {request_count} requests",
                self.id
            );
        }

        // A failed export is the more urgent of the two, so the SDK logs it first.
        export_error.or(decode_error).map_or(Ok(()), Err)
    }

    /// Export one request with bounded retry. The runtime commits offsets before `consume`
    /// runs, so this is the only redelivery a transient failure gets.
    async fn send<R: ExportRequest>(&self, opened: &Opened, packed: Packed) -> Result<(), Error> {
        let context = format!(
            "OTLP sink ID: {} export of {} {} from {} messages",
            self.id,
            packed.items,
            R::ITEM_NAME,
            packed.messages
        );
        let exporter = &opened.exporter;
        let body = exporter.prepare_body(packed.body)?;

        let mut attempts = 0u32;
        let started = Instant::now();
        let result = retry_async(
            opened.settings.retry,
            &context,
            ExportError::is_retryable,
            || {
                attempts += 1;
                exporter.export::<R::Response>(body.clone())
            },
        )
        .await;
        self.counters.record_attempts(attempts, started.elapsed());

        match result {
            Ok(response) => {
                // Rejected items were seen and refused by the receiver, which the
                // specification says must not be retried.
                if let Some((rejected, reason)) = response.rejected() {
                    self.counters
                        .items_rejected
                        .fetch_add(u64::try_from(rejected).unwrap_or(0), Ordering::Relaxed);
                    warn!(
                        "{context}: receiver rejected {rejected} {}: {reason}",
                        R::ITEM_NAME
                    );
                }
                Ok(())
            }
            Err(failure) => {
                error!("{context} {failure}");
                Err(failure.into_error().into())
            }
        }
    }
}

#[async_trait]
impl Sink for OtlpSink {
    async fn open(&mut self) -> Result<(), Error> {
        // The SDK does not log the error `open` returns, so the reason would be lost.
        let opened = Opened::new(self.id, &self.config).inspect_err(|error| {
            error!(
                "Failed to open OTLP sink connector ID: {}: {error}",
                self.id
            )
        })?;
        info!(
            "Opened OTLP sink connector ID: {}, endpoint: {}",
            self.id,
            endpoint_for_log(&opened.settings.endpoint)
        );
        self.opened = Some(opened);
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }
        let opened = self.opened.as_ref().ok_or_else(|| {
            Error::InitError(format!(
                "OTLP sink ID: {} consume() called before open()",
                self.id
            ))
        })?;

        let received = messages.len();
        let prepared = batch::pack_requests::<ExportLogsServiceRequest>(
            messages,
            opened.settings.max_request_bytes,
        );
        self.send_all::<ExportLogsServiceRequest>(
            opened,
            prepared,
            received,
            messages_metadata.schema,
        )
        .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.opened = None;
        info!(
            "Closed OTLP sink connector ID: {}. {}",
            self.id, self.counters
        );
        Ok(())
    }
}

fn parse_duration(value: Option<&str>, default: &str, field: &str) -> Result<Duration, Error> {
    let value = value.unwrap_or(default);
    humantime::parse_duration(value).map_err(|_| {
        Error::InvalidConfigValue(format!(
            "{field} \"{value}\" is not a duration such as \"10s\" or \"500ms\""
        ))
    })
}

/// The endpoint without credentials or query string, safe to log.
fn endpoint_for_log(endpoint: &Url) -> Url {
    let mut url = endpoint.clone();
    // Both only fail for URLs that cannot carry credentials in the first place.
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn test_config() -> OtlpSinkConfig {
        OtlpSinkConfig {
            endpoint: "http://localhost:4318".to_owned(),
            compression: None,
            headers: None,
            timeout: None,
            max_retries: None,
            retry_delay: None,
            max_retry_delay: None,
            max_request_bytes: None,
            verbose_logging: None,
        }
    }

    fn open(sink: &mut OtlpSink) -> Result<(), Error> {
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(sink.open())
    }

    fn topic() -> TopicMetadata {
        TopicMetadata {
            stream: "s".to_owned(),
            topic: "t".to_owned(),
        }
    }

    fn raw_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 0,
            current_offset: 0,
            schema: Schema::Raw,
        }
    }

    fn raw_message(payload: Vec<u8>) -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: iggy_connector_sdk::Payload::Raw(payload),
        }
    }

    #[test]
    fn given_minimal_config_should_apply_defaults() {
        let settings = Settings::resolve(1, &test_config()).expect("valid config");

        assert!(!settings.gzip);
        assert_eq!(settings.timeout, Duration::from_secs(10));
        assert_eq!(settings.retry.max_attempts, DEFAULT_MAX_RETRIES);
        assert_eq!(settings.retry.base_delay, Duration::from_secs(1));
        assert_eq!(settings.retry.max_delay, Duration::from_secs(10));
        assert_eq!(settings.max_request_bytes, DEFAULT_MAX_REQUEST_BYTES);
    }

    #[test]
    fn given_toml_config_should_parse_every_field() {
        let config: OtlpSinkConfig = toml::from_str(
            r#"
            endpoint = "https://otlp.example.com:4318"
            compression = "gzip"
            timeout = "5s"
            max_retries = 5
            retry_delay = "200ms"
            max_retry_delay = "3s"
            max_request_bytes = 1048576
            verbose_logging = true

            [headers]
            authorization = "Bearer secret-token"
            "#,
        )
        .expect("valid config");

        assert_eq!(config.compression, Some(Compression::Gzip));
        assert_eq!(config.max_retries, Some(5));
        assert_eq!(config.max_request_bytes, Some(1_048_576));
        let headers = config.headers.expect("headers");
        assert_eq!(
            headers["authorization"].expose_secret(),
            "Bearer secret-token"
        );
    }

    #[test]
    fn given_header_secret_should_not_appear_in_debug_output() {
        let mut config = test_config();
        config.headers = Some(HashMap::from([(
            "authorization".to_owned(),
            SecretString::from("Bearer secret-token"),
        )]));
        let mut sink = OtlpSink::new(1, config);
        assert!(!format!("{sink:?}").contains("secret-token"));

        open(&mut sink).expect("valid config");

        assert!(sink.opened.is_some());
        assert!(
            !format!("{sink:?}").contains("secret-token"),
            "the exporter shows the header value"
        );
    }

    #[test]
    fn given_bundled_example_config_should_parse() {
        #[derive(Deserialize)]
        struct ConnectorFile {
            plugin_config: OtlpSinkConfig,
        }

        let file: ConnectorFile =
            toml::from_str(include_str!("../config.toml")).expect("config.toml parses");

        assert_eq!(file.plugin_config.endpoint, "http://localhost:4318");
    }

    #[test]
    fn given_zero_max_request_bytes_should_fail_open() {
        let mut config = test_config();
        config.max_request_bytes = Some(0);
        let mut sink = OtlpSink::new(1, config);

        assert!(matches!(open(&mut sink), Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_unreachable_endpoint_should_open_without_contacting_it() {
        let mut config = test_config();
        // Port 9 (discard) is closed on test hosts, so a connect would be refused.
        config.endpoint = "http://127.0.0.1:9".to_owned();
        let mut sink = OtlpSink::new(1, config);

        assert!(open(&mut sink).is_ok());
        assert!(sink.opened.is_some());
    }

    #[test]
    fn given_endpoint_that_is_not_a_url_should_fail_open() {
        let mut config = test_config();
        config.endpoint = "not a url".to_owned();
        let mut sink = OtlpSink::new(1, config);

        assert!(matches!(open(&mut sink), Err(Error::InvalidConfigValue(_))));
        assert!(sink.opened.is_none());
    }

    #[test]
    fn given_endpoint_that_ends_with_the_logs_path_should_fail_open() {
        for endpoint in [
            "http://localhost:4318/v1/logs",
            "https://otlp.example.com/otlp/v1/logs/",
        ] {
            let mut config = test_config();
            config.endpoint = endpoint.to_owned();
            let mut sink = OtlpSink::new(1, config);

            assert!(
                matches!(open(&mut sink), Err(Error::InvalidConfigValue(_))),
                "{endpoint} would get the logs path twice"
            );
        }
    }

    #[test]
    fn given_header_names_that_differ_only_in_case_should_fail_open() {
        let mut config = test_config();
        config.headers = Some(HashMap::from([
            ("Authorization".to_owned(), SecretString::from("a")),
            ("authorization".to_owned(), SecretString::from("b")),
        ]));
        let mut sink = OtlpSink::new(1, config);

        assert!(matches!(open(&mut sink), Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_endpoint_without_scheme_should_fail_open() {
        let mut config = test_config();
        config.endpoint = "localhost:4318".to_owned();
        let mut sink = OtlpSink::new(1, config);

        assert!(matches!(open(&mut sink), Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_zero_timeout_should_fail_open() {
        let mut config = test_config();
        config.timeout = Some("0s".to_owned());
        let mut sink = OtlpSink::new(1, config);

        assert!(matches!(open(&mut sink), Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_unparsable_duration_should_fail_open() {
        let mut config = test_config();
        config.retry_delay = Some("fast".to_owned());
        let mut sink = OtlpSink::new(1, config);

        assert!(matches!(open(&mut sink), Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_consume_before_open_should_fail() {
        let sink = OtlpSink::new(1, test_config());
        let result = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(sink.consume(&topic(), raw_metadata(), vec![raw_message(vec![])]));

        assert!(matches!(result, Err(Error::InitError(_))));
    }

    #[test]
    fn given_failed_export_and_dropped_message_should_return_the_export_failure() {
        let mut config = test_config();
        // Port 9 (discard) is closed on test hosts, so the export is refused.
        config.endpoint = "http://127.0.0.1:9".to_owned();
        config.max_retries = Some(1);
        let mut sink = OtlpSink::new(1, config);
        let valid = ExportLogsServiceRequest {
            resource_logs: vec![Default::default()],
        }
        .encode_to_vec();

        let result = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(async {
                sink.open().await.expect("valid config");
                sink.consume(
                    &topic(),
                    raw_metadata(),
                    vec![raw_message(vec![0xff, 0xff]), raw_message(valid)],
                )
                .await
            });

        assert!(
            matches!(result, Err(Error::CannotStoreData(_))),
            "{result:?}"
        );
    }

    #[test]
    fn given_endpoint_with_credentials_should_redact_them_for_logs() {
        let endpoint = Url::parse("https://user:pass@otlp.example.com:4318/otlp?key=abc")
            .expect("valid endpoint");

        let logged = endpoint_for_log(&endpoint);

        assert_eq!(logged.as_str(), "https://otlp.example.com:4318/otlp");
    }

    #[test]
    fn given_counters_should_report_average_batch_latency() {
        let counters = Counters::default();
        counters.batches_sent.store(2, Ordering::Relaxed);
        counters
            .batch_latency_micros
            .store(3_000, Ordering::Relaxed);

        assert!(
            counters
                .to_string()
                .contains("average_batch_latency_ms: 1.500")
        );
    }

    #[test]
    fn given_attempts_should_count_only_retries() {
        let counters = Counters::default();

        counters.record_attempts(3, Duration::from_millis(1));
        counters.record_attempts(1, Duration::from_millis(1));

        assert_eq!(counters.retries.load(Ordering::Relaxed), 2);
    }
}
