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
    // default, so every service must accept gzip on the wire. Responses are tiny
    // (empty partial_success), but advertising gzip on send is harmless and lets
    // clients negotiate it.
    let logs_server = LogsServiceServer::new(logs_svc)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);
    let metrics_server = MetricsServiceServer::new(metrics_svc)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);
    let trace_server = TraceServiceServer::new(trace_svc)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

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
        let (messages, rejected) = encode_or_convert(request.into_inner(), self.format, "logs");
        send_messages(&self.tx, messages, "logs")?;
        let partial_success = (rejected > 0).then(|| ExportLogsPartialSuccess {
            rejected_log_records: rejected,
            error_message: "log records could not be serialized".to_string(),
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
        let (messages, rejected) = encode_or_convert(request.into_inner(), self.format, "metrics");
        send_messages(&self.tx, messages, "metrics")?;
        let partial_success = (rejected > 0).then(|| ExportMetricsPartialSuccess {
            rejected_data_points: rejected,
            error_message: "data points could not be serialized".to_string(),
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
        let (messages, rejected) = encode_or_convert(request.into_inner(), self.format, "traces");
        send_messages(&self.tx, messages, "traces")?;
        let partial_success = (rejected > 0).then(|| ExportTracePartialSuccess {
            rejected_spans: rejected,
            error_message: "spans could not be serialized".to_string(),
        });
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success,
        }))
    }
}

trait IntoMessages {
    fn into_json_messages(self) -> Vec<ProducedMessage>;
}

impl IntoMessages for ExportLogsServiceRequest {
    fn into_json_messages(self) -> Vec<ProducedMessage> {
        convert::export_logs_to_messages(self)
    }
}

impl IntoMessages for ExportMetricsServiceRequest {
    fn into_json_messages(self) -> Vec<ProducedMessage> {
        convert::export_metrics_to_messages(self)
    }
}

impl IntoMessages for ExportTraceServiceRequest {
    fn into_json_messages(self) -> Vec<ProducedMessage> {
        convert::export_traces_to_messages(self)
    }
}

/// Returns the messages to enqueue plus the count of records that could not be
/// serialized. Those rejects are permanent (the same bytes will fail again), so
/// the caller surfaces them via OTLP `partial_success` rather than retrying.
fn encode_or_convert<R>(req: R, format: StorageFormat, signal: &str) -> (Vec<ProducedMessage>, i64)
where
    R: ProstMessage + IntoMessages,
{
    match format {
        StorageFormat::Json => (req.into_json_messages(), 0),
        StorageFormat::Proto => {
            let mut buf = Vec::new();
            if let Err(e) = req.encode(&mut buf) {
                warn!("Failed to encode {signal} proto: {e}");
                // Proto mode packs the whole request into a single message, so a
                // re-encode failure rejects it wholesale.
                return (Vec::new(), 1);
            }
            let message = ProducedMessage {
                id: None,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                headers: None,
                payload: buf,
            };
            (vec![message], 0)
        }
    }
}

fn send_messages(
    tx: &mpsc::Sender<ProducedMessage>,
    messages: Vec<ProducedMessage>,
    signal: &str,
) -> Result<(), Status> {
    for message in messages {
        if let Err(err) = tx.try_send(message) {
            // OTLP treats partial_success as a permanent reject, so a full or closed
            // channel must surface as a retryable gRPC error instead. Dropping into
            // partial_success here would lose telemetry under load with no retry.
            return match err {
                mpsc::error::TrySendError::Full(_) => {
                    warn!("OTLP ingest channel full, asking {signal} client to retry");
                    Err(Status::resource_exhausted("ingest channel full"))
                }
                mpsc::error::TrySendError::Closed(_) => {
                    warn!("OTLP ingest channel closed while receiving {signal}");
                    Err(Status::unavailable("ingest channel closed"))
                }
            };
        }
    }
    Ok(())
}
