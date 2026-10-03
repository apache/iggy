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

//! In-process fake of the two BigQuery surfaces the sink uses:
//!
//! - REST `tables.get`, served by axum, returning a configurable schema or
//!   HTTP error.
//! - gRPC `BigQueryWrite` (`GetWriteStream` and `AppendRows`), served by
//!   tonic. No server stubs are published for this service, so the routing
//!   below is written by hand in the shape `tonic-build` generates.
//!
//! Each `AppendRows` call takes the next scripted outcome (default: success)
//! and records the decoded Arrow rows, so tests can assert on exactly what
//! the sink sent.

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use gcloud_googleapis::cloud::bigquery::storage::v1::append_rows_request::Rows;
use gcloud_googleapis::cloud::bigquery::storage::v1::append_rows_response::{
    AppendResult, Response,
};
use gcloud_googleapis::cloud::bigquery::storage::v1::{
    AppendRowsRequest, AppendRowsResponse, GetWriteStreamRequest, RowError, WriteStream,
};
use gcloud_googleapis::rpc::Status as RpcStatus;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::{BoxFuture, BoxStream, Context, Poll, Service, http};
use tonic::server::{Grpc, NamedService, StreamingService, UnaryService};
use tonic::{Code, Status};
use tonic_prost::ProstCodec;

const GET_WRITE_STREAM: &str = "/google.cloud.bigquery.storage.v1.BigQueryWrite/GetWriteStream";
const APPEND_ROWS: &str = "/google.cloud.bigquery.storage.v1.BigQueryWrite/AppendRows";

/// What the next `AppendRows` call answers.
#[derive(Debug, Clone)]
pub enum AppendScript {
    Ok,
    /// Response with row errors at these request indexes. Nothing is stored.
    RowErrors(Vec<i64>),
    /// Response carrying an error status. Nothing is stored.
    ResponseError(Code),
    /// The RPC itself fails.
    CallError(Code),
}

/// One `AppendRows` request as the fake saw it.
#[derive(Debug, Clone)]
pub struct ReceivedAppend {
    pub write_stream: String,
    pub default_missing_value_interpretation: i32,
    pub batch: RecordBatch,
    pub stored: bool,
}

#[derive(Debug)]
struct TableResponse {
    status: u16,
    body: String,
}

#[derive(Debug, Default)]
struct FakeState {
    table: Mutex<VecDeque<TableResponse>>,
    table_calls: Mutex<usize>,
    get_stream_error: Mutex<Option<Code>>,
    script: Mutex<VecDeque<AppendScript>>,
    appends: Mutex<Vec<ReceivedAppend>>,
}

pub struct FakeBigQuery {
    pub rest_url: String,
    pub grpc_addr: String,
    state: Arc<FakeState>,
}

impl FakeBigQuery {
    /// Start both servers with `schema_fields` (the JSON array body of
    /// `schema.fields`) as the table schema.
    pub async fn start(schema_fields: &str) -> Self {
        let state = Arc::new(FakeState::default());
        state
            .table
            .lock()
            .unwrap()
            .push_back(TableResponse::schema(schema_fields));

        let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rest_addr = rest.local_addr().unwrap();
        let router = Router::new()
            .route(
                "/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}",
                get(tables_get),
            )
            .with_state(state.clone());
        tokio::spawn(async move {
            axum::serve(rest, router).await.unwrap();
        });

        let grpc = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let grpc_addr: SocketAddr = grpc.local_addr().unwrap();
        let service = BigQueryWriteService {
            state: state.clone(),
        };
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(grpc))
                .await
                .unwrap();
        });

        FakeBigQuery {
            rest_url: format!("http://{rest_addr}"),
            grpc_addr: grpc_addr.to_string(),
            state,
        }
    }

    /// Queue `tables.get` responses ahead of the schema given to `start`.
    /// Once the queue holds one entry it is repeated forever.
    pub fn table_responses(&self, responses: &[(u16, &str)]) {
        let mut table = self.state.table.lock().unwrap();
        let last = table.pop_back();
        table.clear();
        for (status, body) in responses {
            table.push_back(TableResponse {
                status: *status,
                body: (*body).to_owned(),
            });
        }
        if let Some(last) = last {
            table.push_back(last);
        }
    }

    pub fn fail_get_write_stream(&self, code: Code) {
        *self.state.get_stream_error.lock().unwrap() = Some(code);
    }

    pub fn script(&self, outcomes: &[AppendScript]) {
        self.state
            .script
            .lock()
            .unwrap()
            .extend(outcomes.iter().cloned());
    }

    pub fn pending_script(&self) -> usize {
        self.state.script.lock().unwrap().len()
    }

    pub fn table_calls(&self) -> usize {
        *self.state.table_calls.lock().unwrap()
    }

    pub fn appends(&self) -> Vec<ReceivedAppend> {
        self.state.appends.lock().unwrap().clone()
    }

    /// Batches the fake accepted, in order.
    pub fn stored(&self) -> Vec<RecordBatch> {
        self.appends()
            .into_iter()
            .filter(|a| a.stored)
            .map(|a| a.batch)
            .collect()
    }
}

impl TableResponse {
    fn schema(fields: &str) -> Self {
        TableResponse {
            status: 200,
            body: format!(r#"{{"schema":{{"fields":[{fields}]}}}}"#),
        }
    }
}

async fn tables_get(State(state): State<Arc<FakeState>>) -> (StatusCode, String) {
    *state.table_calls.lock().unwrap() += 1;
    let mut table = state.table.lock().unwrap();
    let response = if table.len() > 1 {
        table.pop_front().unwrap()
    } else {
        let only = table.front().unwrap();
        TableResponse {
            status: only.status,
            body: only.body.clone(),
        }
    };
    (
        StatusCode::from_u16(response.status).unwrap(),
        response.body,
    )
}

// ─── gRPC service ────────────────────────────────────────────────────────────

#[derive(Clone)]
struct BigQueryWriteService {
    state: Arc<FakeState>,
}

impl NamedService for BigQueryWriteService {
    const NAME: &'static str = "google.cloud.bigquery.storage.v1.BigQueryWrite";
}

impl<B> Service<http::Request<B>> for BigQueryWriteService
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let state = self.state.clone();
        match request.uri().path() {
            GET_WRITE_STREAM => Box::pin(async move {
                let mut grpc = Grpc::new(ProstCodec::default());
                Ok(grpc.unary(GetWriteStreamHandler { state }, request).await)
            }),
            APPEND_ROWS => Box::pin(async move {
                let mut grpc = Grpc::new(ProstCodec::default());
                Ok(grpc.streaming(AppendRowsHandler { state }, request).await)
            }),
            _ => Box::pin(async move { Ok(Status::unimplemented("not faked").into_http()) }),
        }
    }
}

struct GetWriteStreamHandler {
    state: Arc<FakeState>,
}

impl UnaryService<GetWriteStreamRequest> for GetWriteStreamHandler {
    type Response = WriteStream;
    type Future = BoxFuture<tonic::Response<WriteStream>, Status>;

    fn call(&mut self, request: tonic::Request<GetWriteStreamRequest>) -> Self::Future {
        let error = *self.state.get_stream_error.lock().unwrap();
        Box::pin(async move {
            if let Some(code) = error {
                return Err(Status::new(code, "scripted GetWriteStream failure"));
            }
            Ok(tonic::Response::new(WriteStream {
                name: request.into_inner().name,
                ..Default::default()
            }))
        })
    }
}

struct AppendRowsHandler {
    state: Arc<FakeState>,
}

impl StreamingService<AppendRowsRequest> for AppendRowsHandler {
    type Response = AppendRowsResponse;
    type ResponseStream = BoxStream<AppendRowsResponse>;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, Status>;

    fn call(
        &mut self,
        request: tonic::Request<tonic::Streaming<AppendRowsRequest>>,
    ) -> Self::Future {
        let state = self.state.clone();
        Box::pin(async move {
            let mut requests = request.into_inner();
            let mut responses = Vec::new();
            while let Some(append) = requests.next().await {
                let append = append?;
                let script = state
                    .script
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(AppendScript::Ok);
                if let AppendScript::CallError(code) = script {
                    return Err(Status::new(code, "scripted AppendRows failure"));
                }
                let batch = decode_arrow(&append);
                let stored = matches!(script, AppendScript::Ok);
                state.appends.lock().unwrap().push(ReceivedAppend {
                    write_stream: append.write_stream.clone(),
                    default_missing_value_interpretation: append
                        .default_missing_value_interpretation,
                    batch,
                    stored,
                });
                responses.push(Ok(response_for(script, append.write_stream)));
            }
            let stream: Self::ResponseStream = Box::pin(tokio_stream::iter(responses));
            Ok(tonic::Response::new(stream))
        })
    }
}

fn response_for(script: AppendScript, write_stream: String) -> AppendRowsResponse {
    match script {
        AppendScript::Ok | AppendScript::CallError(_) => AppendRowsResponse {
            write_stream,
            response: Some(Response::AppendResult(AppendResult { offset: None })),
            ..Default::default()
        },
        AppendScript::RowErrors(indexes) => AppendRowsResponse {
            write_stream,
            row_errors: indexes
                .into_iter()
                .map(|index| RowError {
                    index,
                    code: 1,
                    message: format!("scripted error for row {index}"),
                })
                .collect(),
            response: Some(Response::Error(RpcStatus {
                code: Code::InvalidArgument as i32,
                message: "rows rejected".into(),
                details: Vec::new(),
            })),
            ..Default::default()
        },
        AppendScript::ResponseError(code) => AppendRowsResponse {
            write_stream,
            response: Some(Response::Error(RpcStatus {
                code: code as i32,
                message: "scripted response error".into(),
                details: Vec::new(),
            })),
            ..Default::default()
        },
    }
}

fn decode_arrow(append: &AppendRowsRequest) -> RecordBatch {
    let Some(Rows::ArrowRows(arrow)) = &append.rows else {
        panic!("sink must send Arrow rows");
    };
    let mut bytes = arrow
        .writer_schema
        .as_ref()
        .expect("writer schema")
        .serialized_schema
        .clone();
    bytes.extend_from_slice(
        &arrow
            .rows
            .as_ref()
            .expect("record batch")
            .serialized_record_batch,
    );
    let reader = StreamReader::try_new(Cursor::new(bytes), None).expect("valid Arrow IPC");
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().expect("valid batch");
    assert_eq!(batches.len(), 1, "one record batch per request");
    batches.into_iter().next().unwrap()
}
