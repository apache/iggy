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

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use flate2::{Compression, write::GzEncoder};
use http::uri::PathAndQuery;
use iggy_connector_sdk::retry::parse_duration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
    owned_value_to_serde_json, sink_connector,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, trace_service_client::TraceServiceClient,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tonic::client::Grpc as TonicGrpc;
use tonic::codec::CompressionEncoding;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::{MetadataKey, MetadataValue};
use tonic::transport::Channel;
use tracing::{debug, info, warn};

mod from_json;

sink_connector!(OtlpSink);

/// Which OTLP signal this sink handles. Must match the Iggy topic content.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtlpSignal {
    Traces,
    Metrics,
    Logs,
}

/// How messages are stored in the Iggy topic.
///
/// `Json` (default): messages are JSON produced by the otlp_source JSON path.
/// The sink reconstructs OTLP proto from the JSON before forwarding.
///
/// `Proto`: messages are raw prost-encoded OTLP proto bytes (one
/// `Export*ServiceRequest` per message). The sink forwards them directly,
/// with zero deserialization overhead.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageFormat {
    #[default]
    Json,
    Proto,
}

/// Wire transport used to forward OTLP data to the downstream receiver.
///
/// `Grpc` (default): forwards via OTLP/gRPC (`opentelemetry.proto.collector.*`
/// service RPCs). Supports per-message gzip compression and custom gRPC metadata.
///
/// `Http`: forwards via OTLP/HTTP (`POST /v1/{traces,metrics,logs}` with
/// `Content-Type: application/x-protobuf`). Useful when the downstream endpoint
/// does not expose a gRPC port. Custom headers and gzip body encoding are
/// supported through the same `headers` and `compression` config fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    #[default]
    Grpc,
    Http,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpSinkConfig {
    /// gRPC or HTTP endpoint, e.g. "http://quickwit:7281"
    pub endpoint: String,
    pub signal: OtlpSignal,
    #[serde(default)]
    pub transport: Transport,
    #[serde(default)]
    pub format: StorageFormat,
    #[serde(default)]
    pub compression: bool,
    /// Extra headers sent with every export request.
    /// For gRPC these become metadata entries; for HTTP they become request headers.
    /// QW uses these to route to a specific index:
    ///   qw-otel-traces-index = "flows3"
    ///   qw-otel-logs-index   = "otel-logs-v0_7"
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Per-request timeout in humantime format, e.g. "30s". Bounds each gRPC
    /// unary call (and channel connect) and each HTTP request so a hung backend
    /// cannot stall `consume()` indefinitely. Defaults to 30s.
    #[serde(default)]
    pub request_timeout: Option<String>,
}

enum GrpcClient {
    Traces(TraceServiceClient<Channel>),
    Metrics(MetricsServiceClient<Channel>),
    Logs(LogsServiceClient<Channel>),
}

impl std::fmt::Debug for GrpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrpcClient::Traces(_) => write!(f, "TraceServiceClient"),
            GrpcClient::Metrics(_) => write!(f, "MetricsServiceClient"),
            GrpcClient::Logs(_) => write!(f, "LogsServiceClient"),
        }
    }
}

enum Client {
    Grpc(GrpcClient),
    Http(reqwest::Client),
}

/// Cumulative export counters, logged at close() and periodically at debug level.
#[derive(Debug, Default)]
struct Counters {
    messages_sent: AtomicU64,
    batches_sent: AtomicU64,
    batches_failed: AtomicU64,
}

#[derive(Debug)]
pub struct OtlpSink {
    id: u32,
    config: OtlpSinkConfig,
    client: Option<Client>,
    // Retained for proto-mode raw passthrough: one Channel shared with typed client.
    raw_channel: Option<Channel>,
    // Pre-parsed metadata entries derived from config.headers at open() time (gRPC only).
    metadata: Vec<(
        MetadataKey<tonic::metadata::Ascii>,
        MetadataValue<tonic::metadata::Ascii>,
    )>,
    counters: Arc<Counters>,
}

impl OtlpSink {
    pub fn new(id: u32, config: OtlpSinkConfig) -> Self {
        Self {
            id,
            config,
            client: None,
            raw_channel: None,
            metadata: Vec::new(),
            counters: Arc::new(Counters::default()),
        }
    }

    fn with_grpc_headers<T>(&self, msg: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(msg);
        for (k, v) in &self.metadata {
            req.metadata_mut().insert(k.clone(), v.clone());
        }
        req
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Client::Grpc(GrpcClient::Traces(_)) => write!(f, "GrpcTraceServiceClient"),
            Client::Grpc(GrpcClient::Metrics(_)) => write!(f, "GrpcMetricsServiceClient"),
            Client::Grpc(GrpcClient::Logs(_)) => write!(f, "GrpcLogsServiceClient"),
            Client::Http(_) => write!(f, "HttpClient"),
        }
    }
}

#[async_trait]
impl Sink for OtlpSink {
    async fn open(&mut self) -> Result<(), Error> {
        let request_timeout = parse_duration(self.config.request_timeout.as_deref(), "30s");
        self.client = Some(match self.config.transport {
            Transport::Http => Client::Http(
                reqwest::Client::builder()
                    .timeout(request_timeout)
                    .build()
                    .map_err(|e| Error::InitError(format!("HTTP client: {e}")))?,
            ),
            Transport::Grpc => {
                for (k, v) in &self.config.headers {
                    let key = MetadataKey::from_bytes(k.as_bytes())
                        .map_err(|e| Error::InvalidConfigValue(format!("header key '{k}': {e}")))?;
                    let val = MetadataValue::try_from(v.as_str()).map_err(|e| {
                        Error::InvalidConfigValue(format!("header value for '{k}': {e}"))
                    })?;
                    self.metadata.push((key, val));
                }

                let channel = Channel::from_shared(self.config.endpoint.clone())
                    .map_err(|e| Error::InvalidConfigValue(format!("endpoint: {e}")))?
                    .timeout(request_timeout)
                    .connect_timeout(request_timeout)
                    .connect()
                    .await
                    .map_err(|e| Error::InitError(format!("OTLP endpoint: {e}")))?;

                self.raw_channel = Some(channel.clone());

                Client::Grpc(match self.config.signal {
                    OtlpSignal::Traces => {
                        let mut c = TraceServiceClient::new(channel);
                        if self.config.compression {
                            c = c
                                .send_compressed(CompressionEncoding::Gzip)
                                .accept_compressed(CompressionEncoding::Gzip);
                        }
                        GrpcClient::Traces(c)
                    }
                    OtlpSignal::Metrics => {
                        let mut c = MetricsServiceClient::new(channel);
                        if self.config.compression {
                            c = c
                                .send_compressed(CompressionEncoding::Gzip)
                                .accept_compressed(CompressionEncoding::Gzip);
                        }
                        GrpcClient::Metrics(c)
                    }
                    OtlpSignal::Logs => {
                        let mut c = LogsServiceClient::new(channel);
                        if self.config.compression {
                            c = c
                                .send_compressed(CompressionEncoding::Gzip)
                                .accept_compressed(CompressionEncoding::Gzip);
                        }
                        GrpcClient::Logs(c)
                    }
                })
            }
        });

        info!(
            "Opened OTLP sink connector ID: {}, signal: {:?}, transport: {:?}, endpoint: {}",
            self.id, self.config.signal, self.config.transport, self.config.endpoint
        );
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let total = messages.len();
        debug!(
            "OTLP sink connector ID: {} received {total} messages, schema: {}",
            self.id, messages_metadata.schema
        );

        match self.export(&messages_metadata, &messages, total).await {
            Ok(exported) => {
                self.counters
                    .messages_sent
                    .fetch_add(exported, Ordering::Relaxed);
                self.counters.batches_sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.counters.batches_failed.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        let _ = self.client.take();
        let _ = self.raw_channel.take();
        info!(
            "Closed OTLP sink connector ID: {}. Counters: messages_sent={}, batches_sent={}, batches_failed={}",
            self.id,
            self.counters.messages_sent.load(Ordering::Relaxed),
            self.counters.batches_sent.load(Ordering::Relaxed),
            self.counters.batches_failed.load(Ordering::Relaxed),
        );
        Ok(())
    }
}

impl OtlpSink {
    async fn export(
        &self,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
        total: usize,
    ) -> Result<u64, Error> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::InitError("OTLP sink client not initialized".into()))?;

        match client {
            Client::Grpc(grpc) => {
                if matches!(self.config.format, StorageFormat::Proto) {
                    self.export_grpc_raw_proto(messages, total).await
                } else {
                    self.export_grpc(grpc, messages_metadata, messages, total)
                        .await
                }
            }
            Client::Http(http) => {
                if matches!(self.config.format, StorageFormat::Proto) {
                    self.export_http_raw_proto(http, messages, total).await
                } else {
                    self.export_http(http, messages_metadata, messages, total)
                        .await
                }
            }
        }
    }

    async fn export_grpc(
        &self,
        client: &GrpcClient,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
        total: usize,
    ) -> Result<u64, Error> {
        match client {
            GrpcClient::Traces(c) => {
                let req = build_trace_request(&self.config.format, messages_metadata, messages)?;
                if req.resource_spans.is_empty() {
                    return Ok(0);
                }
                let span_count: usize = req
                    .resource_spans
                    .iter()
                    .flat_map(|rs| &rs.scope_spans)
                    .map(|ss| ss.spans.len())
                    .sum();
                c.clone()
                    .export(self.with_grpc_headers(req))
                    .await
                    .map_err(|e| Error::CannotStoreData(format!("OTLP traces export: {e}")))?;
                debug!(
                    "OTLP sink connector ID: {} exported {span_count} spans",
                    self.id
                );
                Ok(total as u64)
            }
            GrpcClient::Metrics(c) => {
                let req = build_metrics_request(&self.config.format, messages_metadata, messages)?;
                if req.resource_metrics.is_empty() {
                    return Ok(0);
                }
                c.clone()
                    .export(self.with_grpc_headers(req))
                    .await
                    .map_err(|e| Error::CannotStoreData(format!("OTLP metrics export: {e}")))?;
                debug!(
                    "OTLP sink connector ID: {} exported metrics batch ({total} messages)",
                    self.id
                );
                Ok(total as u64)
            }
            GrpcClient::Logs(c) => {
                let req = build_logs_request(&self.config.format, messages_metadata, messages)?;
                if req.resource_logs.is_empty() {
                    return Ok(0);
                }
                c.clone()
                    .export(self.with_grpc_headers(req))
                    .await
                    .map_err(|e| Error::CannotStoreData(format!("OTLP logs export: {e}")))?;
                debug!(
                    "OTLP sink connector ID: {} exported logs batch ({total} messages)",
                    self.id
                );
                Ok(total as u64)
            }
        }
    }

    async fn export_http(
        &self,
        client: &reqwest::Client,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
        total: usize,
    ) -> Result<u64, Error> {
        let (url_path, proto_bytes) = match &self.config.signal {
            OtlpSignal::Traces => {
                let req = build_trace_request(&self.config.format, messages_metadata, messages)?;
                if req.resource_spans.is_empty() {
                    return Ok(0);
                }
                let mut buf = Vec::new();
                req.encode(&mut buf)
                    .map_err(|e| Error::WriteFailure(format!("proto encode traces: {e}")))?;
                ("/v1/traces", buf)
            }
            OtlpSignal::Metrics => {
                let req = build_metrics_request(&self.config.format, messages_metadata, messages)?;
                if req.resource_metrics.is_empty() {
                    return Ok(0);
                }
                let mut buf = Vec::new();
                req.encode(&mut buf)
                    .map_err(|e| Error::WriteFailure(format!("proto encode metrics: {e}")))?;
                ("/v1/metrics", buf)
            }
            OtlpSignal::Logs => {
                let req = build_logs_request(&self.config.format, messages_metadata, messages)?;
                if req.resource_logs.is_empty() {
                    return Ok(0);
                }
                let mut buf = Vec::new();
                req.encode(&mut buf)
                    .map_err(|e| Error::WriteFailure(format!("proto encode logs: {e}")))?;
                ("/v1/logs", buf)
            }
        };

        let base = self.config.endpoint.trim_end_matches('/');
        let url = format!("{base}{url_path}");

        let mut builder = client
            .post(&url)
            .header("Content-Type", "application/x-protobuf");

        let body = if self.config.compression {
            let mut enc = GzEncoder::new(Vec::new(), Compression::default());
            enc.write_all(&proto_bytes)
                .map_err(|e| Error::HttpRequestFailed(format!("gzip encode: {e}")))?;
            let compressed = enc
                .finish()
                .map_err(|e| Error::HttpRequestFailed(format!("gzip finish: {e}")))?;
            builder = builder.header("Content-Encoding", "gzip");
            compressed
        } else {
            proto_bytes
        };

        for (k, v) in &self.config.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }

        let resp = builder
            .body(body)
            .send()
            .await
            .map_err(|e| Error::HttpRequestFailed(format!("OTLP HTTP {url_path}: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            if status.is_client_error() {
                let id = self.id;
                warn!(
                    "OTLP sink connector ID: {id} HTTP {url_path} returned {status}: {body_text}"
                );
            } else {
                return Err(Error::HttpRequestFailed(format!(
                    "OTLP HTTP {url_path} returned {status}: {body_text}"
                )));
            }
        }

        debug!(
            "OTLP sink connector ID: {} HTTP {url_path} exported successfully",
            self.id
        );
        Ok(total as u64)
    }

    async fn export_grpc_raw_proto(
        &self,
        messages: &[ConsumedMessage],
        total: usize,
    ) -> Result<u64, Error> {
        let channel = self
            .raw_channel
            .as_ref()
            .ok_or_else(|| Error::InitError("raw gRPC channel not initialized".into()))?;

        let path = match &self.config.signal {
            OtlpSignal::Traces => PathAndQuery::from_static(
                "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
            ),
            OtlpSignal::Metrics => PathAndQuery::from_static(
                "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
            ),
            OtlpSignal::Logs => PathAndQuery::from_static(
                "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            ),
        };

        let mut grpc_client = TonicGrpc::new(channel.clone());
        let mut sent = 0u64;

        for msg in messages {
            let raw = match &msg.payload {
                Payload::Raw(bytes) => bytes,
                _ => {
                    warn!(
                        "OTLP sink connector ID: {} (proto mode): expected raw payload",
                        self.id
                    );
                    continue;
                }
            };
            // tower::Buffer requires poll_ready before every call.
            grpc_client
                .ready()
                .await
                .map_err(|e| Error::CannotStoreData(format!("OTLP gRPC channel not ready: {e}")))?;
            let mut request = tonic::Request::new(Bytes::copy_from_slice(raw));
            for (k, v) in &self.metadata {
                request.metadata_mut().insert(k.clone(), v.clone());
            }
            grpc_client
                .unary(request, path.clone(), RawCodec)
                .await
                .map_err(|e| Error::CannotStoreData(format!("OTLP raw gRPC export: {e}")))?;
            sent += 1;
        }

        debug!(
            "OTLP sink connector ID: {} raw proto gRPC exported {sent}/{total} messages",
            self.id
        );
        Ok(sent)
    }

    async fn export_http_raw_proto(
        &self,
        client: &reqwest::Client,
        messages: &[ConsumedMessage],
        total: usize,
    ) -> Result<u64, Error> {
        let url_path = match &self.config.signal {
            OtlpSignal::Traces => "/v1/traces",
            OtlpSignal::Metrics => "/v1/metrics",
            OtlpSignal::Logs => "/v1/logs",
        };
        let base = self.config.endpoint.trim_end_matches('/');
        let url = format!("{base}{url_path}");
        let mut sent = 0u64;

        for msg in messages {
            let raw = match &msg.payload {
                Payload::Raw(bytes) => bytes,
                _ => {
                    warn!(
                        "OTLP sink connector ID: {} (proto mode): expected raw payload",
                        self.id
                    );
                    continue;
                }
            };

            let body = if self.config.compression {
                let mut enc = GzEncoder::new(Vec::new(), Compression::default());
                enc.write_all(raw)
                    .map_err(|e| Error::HttpRequestFailed(format!("gzip encode: {e}")))?;
                enc.finish()
                    .map_err(|e| Error::HttpRequestFailed(format!("gzip finish: {e}")))?
            } else {
                raw.to_vec()
            };

            let mut builder = client
                .post(&url)
                .header("Content-Type", "application/x-protobuf");
            if self.config.compression {
                builder = builder.header("Content-Encoding", "gzip");
            }
            for (k, v) in &self.config.headers {
                builder = builder.header(k.as_str(), v.as_str());
            }

            let resp = builder
                .body(body)
                .send()
                .await
                .map_err(|e| Error::HttpRequestFailed(format!("OTLP HTTP {url_path}: {e}")))?;

            let status = resp.status();
            if !status.is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                if status.is_client_error() {
                    let id = self.id;
                    warn!(
                        "OTLP sink connector ID: {id} HTTP {url_path} returned {status}: {body_text}"
                    );
                } else {
                    return Err(Error::HttpRequestFailed(format!(
                        "OTLP HTTP {url_path} returned {status}: {body_text}"
                    )));
                }
            }
            sent += 1;
        }

        debug!(
            "OTLP sink connector ID: {} HTTP {url_path} raw proto exported {sent}/{total} messages",
            self.id
        );
        Ok(sent)
    }
}

fn build_trace_request(
    format: &StorageFormat,
    meta: &MessagesMetadata,
    messages: &[ConsumedMessage],
) -> Result<ExportTraceServiceRequest, Error> {
    match format {
        StorageFormat::Proto => {
            let bytes = collect_raw_bytes(meta, messages);
            if bytes.is_empty() {
                return Ok(ExportTraceServiceRequest::default());
            }
            let mut merged = ExportTraceServiceRequest::default();
            for b in bytes {
                match ExportTraceServiceRequest::decode(bytes::Bytes::copy_from_slice(b)) {
                    Ok(r) => merged.resource_spans.extend(r.resource_spans),
                    Err(e) => warn!("Failed to decode OTLP trace proto: {e}"),
                }
            }
            Ok(merged)
        }
        StorageFormat::Json => {
            let jsons = collect_json_values(meta, messages);
            Ok(from_json::traces_from_json(&jsons))
        }
    }
}

fn build_metrics_request(
    format: &StorageFormat,
    meta: &MessagesMetadata,
    messages: &[ConsumedMessage],
) -> Result<ExportMetricsServiceRequest, Error> {
    match format {
        StorageFormat::Proto => {
            let bytes = collect_raw_bytes(meta, messages);
            let mut merged = ExportMetricsServiceRequest::default();
            for b in bytes {
                match ExportMetricsServiceRequest::decode(bytes::Bytes::copy_from_slice(b)) {
                    Ok(r) => merged.resource_metrics.extend(r.resource_metrics),
                    Err(e) => warn!("Failed to decode OTLP metrics proto: {e}"),
                }
            }
            Ok(merged)
        }
        StorageFormat::Json => {
            let jsons = collect_json_values(meta, messages);
            Ok(from_json::metrics_from_json(&jsons))
        }
    }
}

fn build_logs_request(
    format: &StorageFormat,
    meta: &MessagesMetadata,
    messages: &[ConsumedMessage],
) -> Result<ExportLogsServiceRequest, Error> {
    match format {
        StorageFormat::Proto => {
            let bytes = collect_raw_bytes(meta, messages);
            let mut merged = ExportLogsServiceRequest::default();
            for b in bytes {
                match ExportLogsServiceRequest::decode(bytes::Bytes::copy_from_slice(b)) {
                    Ok(r) => merged.resource_logs.extend(r.resource_logs),
                    Err(e) => warn!("Failed to decode OTLP logs proto: {e}"),
                }
            }
            Ok(merged)
        }
        StorageFormat::Json => {
            let jsons = collect_json_values(meta, messages);
            Ok(from_json::logs_from_json(&jsons))
        }
    }
}

fn collect_raw_bytes<'a>(
    meta: &MessagesMetadata,
    messages: &'a [ConsumedMessage],
) -> Vec<&'a [u8]> {
    let mut out = Vec::with_capacity(messages.len());
    for msg in messages {
        match &msg.payload {
            Payload::Raw(b) => out.push(b.as_slice()),
            _ => warn!(
                "OTLP sink (proto mode): expected raw payload, got schema: {}",
                meta.schema
            ),
        }
    }
    out
}

fn collect_json_values(
    meta: &MessagesMetadata,
    messages: &[ConsumedMessage],
) -> Vec<serde_json::Value> {
    let mut out = Vec::with_capacity(messages.len());
    for msg in messages {
        match &msg.payload {
            Payload::Json(v) => out.push(owned_value_to_serde_json(v)),
            _ => warn!(
                "OTLP sink (json mode): expected JSON payload, got schema: {}",
                meta.schema
            ),
        }
    }
    out
}

// Passthrough codec: encodes raw Bytes as-is into the gRPC frame, ignores the
// response body (OTLP Export responses carry no payload, only trailers).
#[derive(Default)]
struct RawCodec;
struct RawEncoder;
struct RawDecoder;

impl Encoder for RawEncoder {
    type Item = Bytes;
    type Error = tonic::Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Self::Error> {
        let len = src.remaining();
        Ok(Some(src.copy_to_bytes(len)))
    }
}

impl Codec for RawCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = RawEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> RawEncoder {
        RawEncoder
    }

    fn decoder(&mut self) -> RawDecoder {
        RawDecoder
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(signal: OtlpSignal) -> OtlpSinkConfig {
        OtlpSinkConfig {
            endpoint: "http://localhost:7281".to_string(),
            signal,
            transport: Transport::Grpc,
            format: StorageFormat::Json,
            compression: false,
            headers: HashMap::new(),
            request_timeout: None,
        }
    }

    #[test]
    fn given_new_sink_client_should_not_be_initialized() {
        let sink = OtlpSink::new(1, test_config(OtlpSignal::Traces));
        assert!(sink.client.is_none());
    }

    #[test]
    fn given_traces_signal_config_should_store_correctly() {
        let sink = OtlpSink::new(1, test_config(OtlpSignal::Traces));
        assert!(matches!(sink.config.signal, OtlpSignal::Traces));
    }

    #[test]
    fn given_http_transport_config_should_store_correctly() {
        let mut config = test_config(OtlpSignal::Metrics);
        config.transport = Transport::Http;
        let sink = OtlpSink::new(2, config);
        assert!(matches!(sink.config.transport, Transport::Http));
    }

    #[test]
    fn given_new_sink_counters_should_be_zero() {
        let sink = OtlpSink::new(1, test_config(OtlpSignal::Logs));
        assert_eq!(sink.counters.messages_sent.load(Ordering::Relaxed), 0);
        assert_eq!(sink.counters.batches_sent.load(Ordering::Relaxed), 0);
        assert_eq!(sink.counters.batches_failed.load(Ordering::Relaxed), 0);
    }
}
