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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use iggy_connector_scopedb_sink::ScopeDBSinkConfig;
use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, Payload, Schema, TopicMetadata};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub enum Reply {
    Commit,
    RowErrors { indexes: Vec<u64>, truncated: bool },
    Reject(StatusCode),
    Ambiguous,
    InvalidCount,
    Blocked(watch::Receiver<bool>),
    Hang,
}

#[derive(Debug, Clone)]
pub struct Append {
    pub rows: Vec<Value>,
    pub headers: HeaderMap,
    pub bytes: usize,
}

#[derive(Debug)]
pub struct Backend {
    pub table: Value,
    pub catalog_replies: VecDeque<StatusCode>,
    pub catalog_calls: usize,
    pub replies: VecDeque<Reply>,
    pub appends: Vec<Append>,
    pub stored: Vec<Value>,
    received: watch::Sender<usize>,
}

pub struct FakeScopeDB {
    pub endpoint: String,
    pub state: Arc<Mutex<Backend>>,
    task: JoinHandle<()>,
    received: watch::Receiver<usize>,
}

impl FakeScopeDB {
    pub async fn start(table: Value) -> Self {
        let (record, received) = watch::channel(0);
        let state = Arc::new(Mutex::new(Backend {
            table,
            catalog_replies: VecDeque::new(),
            catalog_calls: 0,
            replies: VecDeque::new(),
            appends: Vec::new(),
            stored: Vec::new(),
            received: record,
        }));
        let router = Router::new()
            .route(
                "/v1/catalog/databases/scopedb/schemas/public/tables/events",
                get(catalog),
            )
            .route(
                "/v1/catalog/databases/scopedb/schemas/public/tables/events/ingest",
                axum::routing::post(append),
            )
            .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake ScopeDB");
        let endpoint = format!("http://{}", listener.local_addr().expect("bound address"));
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("serve fake ScopeDB");
        });
        Self {
            endpoint,
            state,
            task,
            received,
        }
    }

    pub fn config(&self) -> ScopeDBSinkConfig {
        serde_json::from_value(json!({
            "endpoint": self.endpoint, "table":"events", "include_metadata":false,
            "batch_size":2, "max_retries":3, "retry_delay":"0s", "max_retry_delay":"0s", "timeout":"1s"
        })).expect("valid test config")
    }

    pub async fn wait_for_appends(&mut self, count: usize) {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.received.wait_for(|received| *received >= count),
        )
        .await
        .expect("request arrives")
        .expect("backend stays open");
    }

    pub fn script(&self, replies: impl IntoIterator<Item = Reply>) {
        self.state
            .lock()
            .expect("backend lock")
            .replies
            .extend(replies);
    }
}

impl Drop for FakeScopeDB {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn catalog(State(state): State<Arc<Mutex<Backend>>>) -> Response {
    let (status, table) = {
        let mut state = state.lock().expect("backend lock");
        state.catalog_calls += 1;
        (
            state.catalog_replies.pop_front().unwrap_or(StatusCode::OK),
            state.table.clone(),
        )
    };
    if status == StatusCode::OK {
        ([("etag", "\"test-schema\"")], Json(table)).into_response()
    } else {
        (status, Json(json!({"message":"catalog unavailable"}))).into_response()
    }
}

async fn append(
    State(state): State<Arc<Mutex<Backend>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    assert_eq!(
        headers.get("content-type").expect("content type"),
        "application/x-ndjson"
    );
    assert_eq!(headers.get("content-encoding").expect("encoding"), "zstd");
    let decoded = zstd::stream::decode_all(body.as_ref()).expect("valid zstd append");
    let text = std::str::from_utf8(&decoded).expect("valid UTF-8 NDJSON");
    let rows: Vec<Value> = text
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("valid row JSON"))
        .collect();
    let reply = {
        let mut state = state.lock().expect("backend lock");
        state.appends.push(Append {
            rows: rows.clone(),
            headers,
            bytes: decoded.len(),
        });
        state.received.send_replace(state.appends.len());
        let reply = state.replies.pop_front().unwrap_or(Reply::Commit);
        if matches!(
            reply,
            Reply::Commit | Reply::Ambiguous | Reply::InvalidCount
        ) {
            state.stored.extend(rows.iter().cloned());
        }
        reply
    };
    match reply {
        Reply::Commit => Json(json!({"num_rows_inserted":rows.len()})).into_response(),
        Reply::RowErrors { indexes, truncated } => {
            let errors: Vec<_> = indexes.into_iter().map(|index| json!({"row_index":index,"column":"id","message":"invalid integer"})).collect();
            (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"code":"invalid_schema","message":"invalid rows","retryable":false,
                "details":{"status":"rejected","row_errors":errors,"row_errors_truncated":truncated}}))).into_response()
        }
        Reply::Reject(StatusCode::SERVICE_UNAVAILABLE) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"message":"append rejected", "retryable":true,"details":{"status":"rejected"}}))).into_response(),
        Reply::Reject(status) => {
            let code = match status {
                StatusCode::TOO_MANY_REQUESTS => "too_many_requests",
                StatusCode::BAD_REQUEST => "bad_request",
                StatusCode::FORBIDDEN => "permission_denied",
                _ => "append_failed",
            };
            (status, Json(json!({"error":{"code":code,"message":"append rejected"}}))).into_response()
        }
        Reply::Ambiguous => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":{"code":"service_unavailable", "message":"append acknowledgement lost"}}))).into_response(),
        Reply::InvalidCount => Json(json!({"num_rows_inserted":0})).into_response(),
        Reply::Blocked(mut gate) => {
            gate.wait_for(|ready| *ready).await.expect("release response");
            (StatusCode::TOO_MANY_REQUESTS, Json(json!({"error":{"code":"too_many_requests","message":"busy"}}))).into_response()
        }
        Reply::Hang => std::future::pending().await,
    }
}

pub fn table(columns: &[(&str, &str)]) -> Value {
    let columns: Vec<_> = columns
        .iter()
        .map(|(name, data_type)| json!({"name":name,"data_type":data_type}))
        .collect();
    json!({"name":"events", "columns":columns})
}

pub fn topic() -> TopicMetadata {
    TopicMetadata {
        stream: "orders".into(),
        topic: "events".into(),
    }
}

pub fn metadata(schema: Schema) -> MessagesMetadata {
    MessagesMetadata {
        partition_id: 3,
        current_offset: 99_999,
        schema,
    }
}

pub fn message(offset: u64, payload: Payload) -> ConsumedMessage {
    ConsumedMessage {
        id: u128::MAX,
        offset,
        checksum: 0,
        timestamp: 1_700_000_000_000_001,
        origin_timestamp: 0,
        headers: None,
        payload,
    }
}

pub fn json_message(offset: u64, value: Value) -> ConsumedMessage {
    let mut bytes = serde_json::to_vec(&value).expect("encode JSON");
    message(
        offset,
        Payload::Json(simd_json::to_owned_value(&mut bytes).expect("valid JSON")),
    )
}
