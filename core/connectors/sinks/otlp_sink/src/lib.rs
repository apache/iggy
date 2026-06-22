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
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
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
use tonic::codec::CompressionEncoding;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpSinkConfig {
    /// gRPC endpoint, e.g. "http://quickwit:7281"
    pub endpoint: String,
    pub signal: OtlpSignal,
    #[serde(default)]
    pub format: StorageFormat,
    #[serde(default)]
    pub compression: bool,
    /// Extra gRPC metadata headers sent with every export request.
    /// QW uses these to route to a specific index:
    ///   qw-otel-traces-index = "flows3"
    ///   qw-otel-logs-index   = "otel-logs-v0_7"
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

enum Client {
    Traces(TraceServiceClient<Channel>),
    Metrics(MetricsServiceClient<Channel>),
    Logs(LogsServiceClient<Channel>),
}

#[derive(Debug)]
pub struct OtlpSink {
    id: u32,
    config: OtlpSinkConfig,
    client: Option<Client>,
    // Pre-parsed metadata entries derived from config.headers at open() time.
    metadata: Vec<(MetadataKey<tonic::metadata::Ascii>, MetadataValue<tonic::metadata::Ascii>)>,
}

impl OtlpSink {
    pub fn new(id: u32, config: OtlpSinkConfig) -> Self {
        Self {
            id,
            config,
            client: None,
            metadata: Vec::new(),
        }
    }

    fn with_headers<T>(&self, msg: T) -> tonic::Request<T> {
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
            Client::Traces(_) => write!(f, "TraceServiceClient"),
            Client::Metrics(_) => write!(f, "MetricsServiceClient"),
            Client::Logs(_) => write!(f, "LogsServiceClient"),
        }
    }
}

#[async_trait]
impl Sink for OtlpSink {
    async fn open(&mut self) -> Result<(), Error> {
        for (k, v) in &self.config.headers {
            let key = MetadataKey::from_bytes(k.as_bytes())
                .map_err(|e| Error::InvalidConfigValue(format!("header key '{k}': {e}")))?;
            let val = MetadataValue::try_from(v.as_str())
                .map_err(|e| Error::InvalidConfigValue(format!("header value for '{k}': {e}")))?;
            self.metadata.push((key, val));
        }

        let channel = Channel::from_shared(self.config.endpoint.clone())
            .map_err(|e| Error::InvalidConfigValue(format!("endpoint: {e}")))?
            .connect()
            .await
            .map_err(|e| Error::InitError(format!("OTLP endpoint: {e}")))?;

        self.client = Some(match self.config.signal {
            OtlpSignal::Traces => {
                let mut c = TraceServiceClient::new(channel);
                if self.config.compression {
                    c = c
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip);
                }
                Client::Traces(c)
            }
            OtlpSignal::Metrics => {
                let mut c = MetricsServiceClient::new(channel);
                if self.config.compression {
                    c = c
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip);
                }
                Client::Metrics(c)
            }
            OtlpSignal::Logs => {
                let mut c = LogsServiceClient::new(channel);
                if self.config.compression {
                    c = c
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip);
                }
                Client::Logs(c)
            }
        });

        info!(
            "Opened OTLP sink connector ID: {}, signal: {:?}, endpoint: {}",
            self.id, self.config.signal, self.config.endpoint
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

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::InitError("OTLP sink client not initialized".into()))?;

        match client {
            Client::Traces(c) => {
                let req = build_trace_request(&self.config.format, &messages_metadata, &messages)?;
                if req.resource_spans.is_empty() {
                    return Ok(());
                }
                let span_count: usize = req
                    .resource_spans
                    .iter()
                    .flat_map(|rs| &rs.scope_spans)
                    .map(|ss| ss.spans.len())
                    .sum();
                c.clone()
                    .export(self.with_headers(req))
                    .await
                    .map_err(|e| Error::HttpRequestFailed(format!("OTLP traces export: {e}")))?;
                debug!(
                    "OTLP sink connector ID: {} exported {span_count} spans",
                    self.id
                );
            }
            Client::Metrics(c) => {
                let req =
                    build_metrics_request(&self.config.format, &messages_metadata, &messages)?;
                if req.resource_metrics.is_empty() {
                    return Ok(());
                }
                c.clone()
                    .export(self.with_headers(req))
                    .await
                    .map_err(|e| Error::HttpRequestFailed(format!("OTLP metrics export: {e}")))?;
                debug!(
                    "OTLP sink connector ID: {} exported metrics batch ({total} messages)",
                    self.id
                );
            }
            Client::Logs(c) => {
                let req = build_logs_request(&self.config.format, &messages_metadata, &messages)?;
                if req.resource_logs.is_empty() {
                    return Ok(());
                }
                c.clone()
                    .export(self.with_headers(req))
                    .await
                    .map_err(|e| Error::HttpRequestFailed(format!("OTLP logs export: {e}")))?;
                debug!(
                    "OTLP sink connector ID: {} exported logs batch ({total} messages)",
                    self.id
                );
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let _ = self.client.take();
        info!("Closed OTLP sink connector ID: {}", self.id);
        Ok(())
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
            // Proto mode: each message is a full ExportTraceServiceRequest; merge them.
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

fn collect_json_values(meta: &MessagesMetadata, messages: &[ConsumedMessage]) -> Vec<serde_json::Value> {
    let mut out = Vec::with_capacity(messages.len());
    for msg in messages {
        match &msg.payload {
            Payload::Json(v) => {
                match simd_json::to_string(v)
                    .ok()
                    .and_then(|s| serde_json::from_str(&s).ok())
                {
                    Some(val) => out.push(val),
                    None => warn!("OTLP sink: failed to convert JSON payload"),
                }
            }
            _ => warn!(
                "OTLP sink (json mode): expected JSON payload, got schema: {}",
                meta.schema
            ),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(signal: OtlpSignal) -> OtlpSinkConfig {
        OtlpSinkConfig {
            endpoint: "http://localhost:7281".to_string(),
            signal,
            format: StorageFormat::Json,
            compression: false,
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
}
