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

use crate::convert;
use iggy_connector_sdk::ProducedMessage;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::{LogsService, LogsServiceServer},
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

pub async fn run_grpc_server(
    addr: SocketAddr,
    tx: mpsc::Sender<ProducedMessage>,
    shutdown: oneshot::Receiver<()>,
) {
    let logs_svc = LogsServiceImpl { tx: tx.clone() };
    let metrics_svc = MetricsServiceImpl { tx: tx.clone() };
    let trace_svc = TraceServiceImpl { tx };

    info!("OTLP gRPC server starting on {addr}");

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
        .serve_with_shutdown(addr, async {
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
}

struct MetricsServiceImpl {
    tx: mpsc::Sender<ProducedMessage>,
}

struct TraceServiceImpl {
    tx: mpsc::Sender<ProducedMessage>,
}

#[tonic::async_trait]
impl LogsService for LogsServiceImpl {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let messages = convert::export_logs_to_messages(request.into_inner());
        send_messages(&self.tx, messages, "logs").await;
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for MetricsServiceImpl {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let messages = convert::export_metrics_to_messages(request.into_inner());
        send_messages(&self.tx, messages, "metrics").await;
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl TraceService for TraceServiceImpl {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let messages = convert::export_traces_to_messages(request.into_inner());
        send_messages(&self.tx, messages, "traces").await;
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

async fn send_messages(
    tx: &mpsc::Sender<ProducedMessage>,
    messages: Vec<ProducedMessage>,
    signal: &str,
) {
    for message in messages {
        if let Err(err) = tx.try_send(message) {
            warn!("OTLP channel full, dropping {signal} message: {err}");
        }
    }
}
