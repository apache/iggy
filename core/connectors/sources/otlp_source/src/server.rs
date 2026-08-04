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

use crate::StorageFormat;
use crate::convert;
use iggy_connector_sdk::ProducedMessage;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::{LogsService, LogsServiceServer},
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};
use opentelemetry_proto::tonic::metrics::v1::metric;
use prost::Message as ProstMessage;
use tokio::sync::{mpsc, oneshot};
use tonic::codec::CompressionEncoding;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

pub async fn run_grpc_server(
    incoming: TcpIncoming,
    tx: mpsc::Sender<ProducedMessage>,
    shutdown: oneshot::Receiver<()>,
    format: StorageFormat,
) {
    let logs_svc = LogsServiceImpl {
        tx: tx.clone(),
        format,
    };
    let metrics_svc = MetricsServiceImpl {
        tx: tx.clone(),
        format,
    };
    let trace_svc = TraceServiceImpl { tx, format };

    // OTel SDKs and the Collector's OTLP exporter gzip-compress payloads by
    // default, so every service must accept gzip on the wire. Send-side gzip is
    // deliberately not advertised: an export response carries at most a
    // partial_success and serializes to a couple of bytes, where the gzip header
    // alone outweighs the body.
    let logs_server = LogsServiceServer::new(logs_svc).accept_compressed(CompressionEncoding::Gzip);
    let metrics_server =
        MetricsServiceServer::new(metrics_svc).accept_compressed(CompressionEncoding::Gzip);
    let trace_server =
        TraceServiceServer::new(trace_svc).accept_compressed(CompressionEncoding::Gzip);

    if let Err(err) = tonic::transport::Server::builder()
        .add_service(logs_server)
        .add_service(metrics_server)
        .add_service(trace_server)
        .serve_with_incoming_shutdown(incoming, async {
            let _ = shutdown.await;
            info!("OTLP gRPC server received shutdown signal");
        })
        .await
    {
        error!("OTLP gRPC server error: {err}");
    }
}

struct LogsServiceImpl {
    tx: mpsc::Sender<ProducedMessage>,
    format: StorageFormat,
}

struct MetricsServiceImpl {
    tx: mpsc::Sender<ProducedMessage>,
    format: StorageFormat,
}

struct TraceServiceImpl {
    tx: mpsc::Sender<ProducedMessage>,
    format: StorageFormat,
}

#[tonic::async_trait]
impl LogsService for LogsServiceImpl {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let batch = encode_or_convert(request.into_inner(), self.format, "logs");
        let rejected = send_messages(&self.tx, batch, "logs");
        let partial_success = (rejected > 0).then(|| ExportLogsPartialSuccess {
            rejected_log_records: rejected,
            error_message: "channel full; records dropped".to_string(),
        });
        Ok(Response::new(ExportLogsServiceResponse { partial_success }))
    }
}

#[tonic::async_trait]
impl MetricsService for MetricsServiceImpl {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let batch = encode_or_convert(request.into_inner(), self.format, "metrics");
        let rejected = send_messages(&self.tx, batch, "metrics");
        let partial_success = (rejected > 0).then(|| ExportMetricsPartialSuccess {
            rejected_data_points: rejected,
            error_message: "channel full; data points dropped".to_string(),
        });
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success,
        }))
    }
}

#[tonic::async_trait]
impl TraceService for TraceServiceImpl {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let batch = encode_or_convert(request.into_inner(), self.format, "traces");
        let rejected = send_messages(&self.tx, batch, "traces");
        let partial_success = (rejected > 0).then(|| ExportTracePartialSuccess {
            rejected_spans: rejected,
            error_message: "channel full; spans dropped".to_string(),
        });
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success,
        }))
    }
}

trait OtlpBatch {
    fn into_json_messages(self) -> Vec<ProducedMessage>;

    /// Number of individual OTLP records in the request. The `rejected_*` fields
    /// of `partial_success` are defined per record, so proto mode (one blob for
    /// the whole request) cannot report the message count.
    fn record_count(&self) -> i64;
}

impl OtlpBatch for ExportLogsServiceRequest {
    fn into_json_messages(self) -> Vec<ProducedMessage> {
        convert::export_logs_to_messages(self)
    }

    fn record_count(&self) -> i64 {
        self.resource_logs
            .iter()
            .flat_map(|resource| &resource.scope_logs)
            .map(|scope| scope.log_records.len() as i64)
            .sum()
    }
}

impl OtlpBatch for ExportMetricsServiceRequest {
    fn into_json_messages(self) -> Vec<ProducedMessage> {
        convert::export_metrics_to_messages(self)
    }

    fn record_count(&self) -> i64 {
        self.resource_metrics
            .iter()
            .flat_map(|resource| &resource.scope_metrics)
            .flat_map(|scope| &scope.metrics)
            .map(|metric| match &metric.data {
                Some(metric::Data::Gauge(gauge)) => gauge.data_points.len() as i64,
                Some(metric::Data::Sum(sum)) => sum.data_points.len() as i64,
                Some(metric::Data::Histogram(histogram)) => histogram.data_points.len() as i64,
                Some(metric::Data::ExponentialHistogram(histogram)) => {
                    histogram.data_points.len() as i64
                }
                Some(metric::Data::Summary(summary)) => summary.data_points.len() as i64,
                None => 0,
            })
            .sum()
    }
}

impl OtlpBatch for ExportTraceServiceRequest {
    fn into_json_messages(self) -> Vec<ProducedMessage> {
        convert::export_traces_to_messages(self)
    }

    fn record_count(&self) -> i64 {
        self.resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .map(|scope| scope.spans.len() as i64)
            .sum()
    }
}

/// A request turned into messages, plus what each message is worth in OTLP
/// records so drops can be reported in the units the spec expects.
struct EncodedBatch {
    messages: Vec<ProducedMessage>,
    records_per_message: i64,
    /// Records lost before anything reached the channel, i.e. a proto encode
    /// failure. Reported as rejected so the client does not read a dropped
    /// batch as a full success.
    rejected: i64,
}

fn encode_or_convert<R>(req: R, format: StorageFormat, signal: &str) -> EncodedBatch
where
    R: ProstMessage + OtlpBatch,
{
    match format {
        StorageFormat::Json => EncodedBatch {
            messages: req.into_json_messages(),
            records_per_message: 1,
            rejected: 0,
        },
        StorageFormat::Proto => {
            let records = req.record_count();
            let mut buf = Vec::with_capacity(req.encoded_len());
            if let Err(e) = req.encode(&mut buf) {
                warn!("Failed to encode {signal} proto, rejecting {records} records: {e}");
                return EncodedBatch {
                    messages: vec![],
                    records_per_message: records,
                    rejected: records,
                };
            }
            EncodedBatch {
                messages: vec![ProducedMessage {
                    id: None,
                    checksum: None,
                    timestamp: None,
                    origin_timestamp: None,
                    headers: None,
                    payload: buf,
                }],
                records_per_message: records,
                rejected: 0,
            }
        }
    }
}

fn send_messages(tx: &mpsc::Sender<ProducedMessage>, batch: EncodedBatch, signal: &str) -> i64 {
    let total = batch.messages.len() as i64;
    let mut dropped: i64 = 0;
    for message in batch.messages {
        if tx.try_send(message).is_err() {
            dropped += 1;
        }
    }
    if dropped > 0 {
        warn!("OTLP channel full, dropped {dropped}/{total} {signal} messages");
    }
    batch.rejected + dropped * batch.records_per_message
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum,
    };
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    fn logs_request(records: usize) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default(); records],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn traces_request(spans: usize) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span::default(); spans],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn metrics_request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn given_log_request_should_count_every_record() {
        assert_eq!(logs_request(7).record_count(), 7);
        assert_eq!(logs_request(0).record_count(), 0);
    }

    #[test]
    fn given_trace_request_should_count_every_span() {
        assert_eq!(traces_request(4).record_count(), 4);
    }

    #[test]
    fn given_metric_request_should_count_data_points_not_metrics() {
        let request = metrics_request(vec![
            Metric {
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![NumberDataPoint::default(); 3],
                })),
                ..Default::default()
            },
            Metric {
                data: Some(metric::Data::Sum(Sum {
                    data_points: vec![NumberDataPoint::default(); 2],
                    ..Default::default()
                })),
                ..Default::default()
            },
            Metric {
                data: Some(metric::Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint::default(); 1],
                    ..Default::default()
                })),
                ..Default::default()
            },
            Metric {
                data: None,
                ..Default::default()
            },
        ]);

        assert_eq!(request.record_count(), 6);
    }

    #[tokio::test]
    async fn given_json_format_should_reject_one_record_per_dropped_message() {
        let (tx, _rx) = mpsc::channel(1);
        let batch = encode_or_convert(logs_request(3), StorageFormat::Json, "logs");
        assert_eq!(batch.messages.len(), 3);
        assert_eq!(batch.records_per_message, 1);

        let rejected = send_messages(&tx, batch, "logs");

        assert_eq!(rejected, 2, "one message fits the channel, two are dropped");
    }

    #[tokio::test]
    async fn given_proto_format_should_reject_the_whole_batch_when_dropped() {
        let (tx, _rx) = mpsc::channel(1);
        // Fill the single slot so the proto blob cannot be enqueued.
        tx.try_send(ProducedMessage {
            id: None,
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: None,
            payload: vec![],
        })
        .expect("channel has one free slot");

        let batch = encode_or_convert(traces_request(500), StorageFormat::Proto, "traces");
        assert_eq!(batch.messages.len(), 1, "proto mode emits a single blob");
        assert_eq!(batch.records_per_message, 500);

        let rejected = send_messages(&tx, batch, "traces");

        assert_eq!(
            rejected, 500,
            "rejected_* counts records, not the one dropped message"
        );
    }

    #[tokio::test]
    async fn given_proto_format_should_report_no_rejects_when_enqueued() {
        let (tx, _rx) = mpsc::channel(1);
        let batch = encode_or_convert(traces_request(500), StorageFormat::Proto, "traces");

        assert_eq!(send_messages(&tx, batch, "traces"), 0);
    }
}
