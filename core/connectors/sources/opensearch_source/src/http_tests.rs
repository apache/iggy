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

use crate::{HitKey, MAX_WATERMARK_HITS, State};
use crate::{OpenSearchSource, OpenSearchSourceConfig};
use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::routing::{get, head, post};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessages, Source, source::SourceBatchResult,
};
use secrecy::SecretString;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

const TEST_INDEX: &str = "test_documents";
const STALL: std::time::Duration = std::time::Duration::from_secs(30);

async fn ack(source: &OpenSearchSource) {
    source
        .on_batch_result(SourceBatchResult::Ack)
        .await
        .expect("ACK should be applied");
}

async fn nack(source: &OpenSearchSource) {
    source
        .on_batch_result(SourceBatchResult::Nack)
        .await
        .expect("NACK should be applied");
}

async fn start_server(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://127.0.0.1:{port}")
}

fn base_config(url: &str) -> OpenSearchSourceConfig {
    OpenSearchSourceConfig {
        url: url.to_string(),
        index: TEST_INDEX.to_string(),
        username: None,
        password: None,
        query: None,
        polling_interval: Some("1ms".to_string()),
        batch_size: Some(10),
        timestamp_field: Some("timestamp".to_string()),
        verbose_logging: false,
        max_retries: Some(1),
        retry_delay: Some("1ms".to_string()),
        retry_max_delay: Some("10ms".to_string()),
        max_open_retries: Some(1),
        open_retry_max_delay: Some("10ms".to_string()),
        circuit_breaker_threshold: None,
        circuit_breaker_cool_down: None,
        request_timeout: None,
        tiebreaker_field: None,
    }
}

fn search_hit(doc_id: &str, timestamp: &str, extra: Value) -> Value {
    let mut source = json!({
        "id": 1,
        "timestamp": timestamp,
    });
    if let Some(obj) = extra.as_object() {
        for (key, value) in obj {
            source[key] = value.clone();
        }
    }
    json!({
        "_index": TEST_INDEX,
        "_id": doc_id,
        "_source": source,
        "sort": [timestamp, doc_id]
    })
}

fn search_response(hits: Vec<Value>) -> Value {
    json!({
        "hits": {
            "hits": hits
        }
    })
}

fn mock_router<F, Fut>(index_exists: StatusCode, search_fn: F) -> Router
where
    F: Fn(Request) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = (StatusCode, String)> + Send + 'static,
{
    Router::new()
        .route(
            "/test_documents/_search",
            post(move |request: Request| {
                let search_fn = search_fn.clone();
                async move { search_fn(request).await }
            }),
        )
        .route("/test_documents", head(move || async move { index_exists }))
}

fn empty_search_router(index_exists: StatusCode) -> Router {
    mock_router(index_exists, |_| async move {
        (StatusCode::OK, search_response(vec![]).to_string())
    })
}

#[tokio::test]
async fn given_index_exists_when_open_should_succeed() {
    let app = empty_search_router(StatusCode::OK);
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.expect("open should succeed");
    assert!(source.client_initialized());
}

#[tokio::test]
async fn given_missing_index_when_open_should_return_init_error() {
    let app = empty_search_router(StatusCode::NOT_FOUND);
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    let error = source.open().await.expect_err("open should fail");
    assert!(matches!(error, Error::InitError(_)));
    let text = error.to_string();
    assert!(text.contains("does not exist"));
    assert!(text.contains(TEST_INDEX));
}

#[tokio::test]
async fn given_search_hits_when_poll_should_produce_json_messages() {
    let hits = vec![search_hit(
        "doc-1",
        "2024-01-01T00:00:00Z",
        json!({"name": "alpha"}),
    )];
    let body = search_response(hits).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let produced = source.poll().await.expect("poll should succeed");
    assert_eq!(produced.schema, iggy_connector_sdk::Schema::Json);
    assert_eq!(produced.messages.len(), 1);
    let payload: Value =
        serde_json::from_slice(&produced.messages[0].payload).expect("valid json payload");
    assert_eq!(payload["name"], "alpha");
    assert!(produced.state.is_some());
    ack(&source).await;

    let (fetched, polls, _) = source.test_metrics().await;
    assert_eq!(fetched, 1);
    assert_eq!(polls, 1);
}

#[tokio::test]
async fn given_empty_search_when_poll_should_count_empty_poll_and_stage_nothing() {
    let app = empty_search_router(StatusCode::OK);
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let produced = source.poll().await.expect("poll should succeed");
    assert!(produced.messages.is_empty());
    assert!(produced.state.is_none());
    assert!(source.pending_state.lock().await.is_none());

    let (published, polls, _) = source.test_metrics().await;
    assert_eq!((published, polls), (0, 1));
}

#[tokio::test]
async fn given_search_after_cursor_when_second_poll_should_request_next_page() {
    let request_count = Arc::new(AtomicUsize::new(0));
    let captured_bodies: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let count = request_count.clone();
    let bodies = captured_bodies.clone();

    let first_page = search_response(vec![search_hit("doc-1", "2024-01-01T00:00:00Z", json!({}))]);
    let second_page = search_response(vec![search_hit("doc-2", "2024-01-02T00:00:00Z", json!({}))]);

    let app = mock_router(StatusCode::OK, move |request: Request| {
        let first_page = first_page.clone();
        let second_page = second_page.clone();
        let count = count.clone();
        let bodies = bodies.clone();
        async move {
            let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap_or_default();
            if let Ok(body) = serde_json::from_slice::<Value>(&bytes) {
                bodies.lock().await.push(body);
            }
            let page = count.fetch_add(1, Ordering::SeqCst);
            let response = if page == 0 { first_page } else { second_page };
            (StatusCode::OK, response.to_string())
        }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let first = source.poll().await.expect("first poll");
    assert_eq!(first.messages.len(), 1);
    ack(&source).await;

    let second = source.poll().await.expect("second poll");
    assert_eq!(second.messages.len(), 1);
    ack(&source).await;

    let bodies = captured_bodies.lock().await;
    assert_eq!(bodies.len(), 2);
    assert!(bodies[0].get("search_after").is_none());
    assert!(bodies[1].get("search_after").is_some());

    let (fetched, _, _) = source.test_metrics().await;
    assert_eq!(fetched, 2);
}

#[tokio::test]
async fn given_hit_when_poll_should_set_stable_message_id_and_document_id_header() {
    let body =
        search_response(vec![search_hit("doc-1", "2024-01-01T00:00:00Z", json!({}))]).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let produced = source.poll().await.expect("poll should succeed");
    let message = &produced.messages[0];
    assert_eq!(message.id, Some(crate::message_id(TEST_INDEX, "doc-1")));
    let headers = message.headers.as_ref().expect("headers");
    let value = headers
        .get(&*crate::DOCUMENT_ID_HEADER_KEY)
        .expect("document id header");
    assert_eq!(value.as_str().expect("string header"), "doc-1");
    let index = headers
        .get(&*crate::INDEX_HEADER_KEY)
        .expect("index header");
    assert_eq!(index.as_str().expect("string header"), TEST_INDEX);
}

#[tokio::test]
async fn given_same_id_in_different_indices_when_poll_should_set_distinct_message_ids() {
    let mut first = search_hit("doc-1", "2024-01-01T00:00:00Z", json!({}));
    first["_index"] = json!("logs-a");
    let mut second = search_hit("doc-1", "2024-01-01T00:00:01Z", json!({}));
    second["_index"] = json!("logs-b");
    let body = search_response(vec![first, second]).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let produced = source.poll().await.expect("poll should succeed");
    assert_eq!(produced.messages.len(), 2);
    assert_eq!(
        produced.messages[0].id,
        Some(crate::message_id("logs-a", "doc-1"))
    );
    assert_ne!(produced.messages[0].id, produced.messages[1].id);
}

#[tokio::test]
async fn given_nack_when_batch_is_staged_should_keep_committed_state() {
    let body =
        search_response(vec![search_hit("doc-1", "2024-01-01T00:00:00Z", json!({}))]).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let produced = source.poll().await.expect("poll should succeed");
    assert_eq!(produced.messages.len(), 1);
    nack(&source).await;

    let (fetched, polls, _) = source.test_metrics().await;
    assert_eq!((fetched, polls), (0, 0));
    assert!(source.test_last_sort().await.is_none());
    assert!(source.pending_state.lock().await.is_none());
}

#[tokio::test]
async fn given_nack_when_polling_again_should_request_same_page() {
    let captured_bodies: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let bodies = captured_bodies.clone();
    let page =
        search_response(vec![search_hit("doc-1", "2024-01-01T00:00:00Z", json!({}))]).to_string();
    let app = mock_router(StatusCode::OK, move |request: Request| {
        let page = page.clone();
        let bodies = bodies.clone();
        async move {
            let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap_or_default();
            if let Ok(body) = serde_json::from_slice::<Value>(&bytes) {
                bodies.lock().await.push(body);
            }
            (StatusCode::OK, page)
        }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    source.poll().await.expect("first poll");
    nack(&source).await;
    let replay = source.poll().await.expect("replayed poll");
    assert_eq!(
        replay.messages.len(),
        1,
        "NACKed document must be polled again"
    );

    let bodies = captured_bodies.lock().await;
    assert_eq!(bodies.len(), 2);
    assert!(bodies[0].get("search_after").is_none());
    assert!(
        bodies[1].get("search_after").is_none(),
        "NACK must not advance the cursor"
    );
}

#[tokio::test]
async fn given_ack_when_batch_is_staged_should_commit_candidate_state() {
    let body =
        search_response(vec![search_hit("doc-1", "2024-01-01T00:00:00Z", json!({}))]).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    source.poll().await.expect("poll should succeed");
    assert!(
        source.test_last_sort().await.is_none(),
        "cursor must not move before the batch result"
    );
    ack(&source).await;

    let (fetched, polls, _) = source.test_metrics().await;
    assert_eq!((fetched, polls), (1, 1));
    assert!(source.test_last_sort().await.is_some());
    assert!(source.pending_state.lock().await.is_none());
}

#[tokio::test]
async fn given_custom_query_when_search_should_include_query_in_body() {
    let captured: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let cap = captured.clone();
    let app = mock_router(StatusCode::OK, move |request: Request| {
        let cap = cap.clone();
        async move {
            let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap_or_default();
            if let Ok(body) = serde_json::from_slice::<Value>(&bytes) {
                *cap.lock().await = Some(body);
            }
            (StatusCode::OK, search_response(vec![]).to_string())
        }
    });
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.query = Some(json!({ "term": { "status": "active" } }));
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();
    source.poll().await.unwrap();

    let body = captured.lock().await.clone().expect("search body captured");
    assert_eq!(body["query"]["term"]["status"], "active");
}

#[tokio::test]
async fn given_stalled_response_when_poll_should_time_out_and_fail() {
    let app = mock_router(StatusCode::OK, |_| async move {
        tokio::time::sleep(STALL).await;
        (StatusCode::OK, search_response(vec![]).to_string())
    });
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.request_timeout = Some("100ms".to_string());
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    let started = std::time::Instant::now();
    let error = source.poll().await.expect_err("stalled poll should fail");

    assert!(matches!(error, Error::Connection(_)), "got {error:?}");
    assert!(started.elapsed() < STALL);
    let (_, _, errors) = source.test_metrics().await;
    assert_eq!(errors, 1);
}

#[tokio::test]
async fn given_stalled_first_response_when_poll_should_retry_and_succeed() {
    let calls = Arc::new(AtomicUsize::new(0));
    let app = mock_router(StatusCode::OK, {
        let calls = calls.clone();
        move |_| {
            let calls = calls.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    tokio::time::sleep(STALL).await;
                }
                (
                    StatusCode::OK,
                    search_response(vec![search_hit("a", "2024-01-01T00:00:00Z", json!({}))])
                        .to_string(),
                )
            }
        }
    });
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.request_timeout = Some("100ms".to_string());
    config.max_retries = Some(2);
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    let produced = source.poll().await.expect("retry should recover the poll");

    assert_eq!(produced.messages.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn given_nested_timestamp_field_when_poll_should_record_last_poll_timestamp() {
    let hit = search_hit(
        "doc-1",
        "2024-01-01T00:00:00Z",
        json!({ "event": { "created": "2024-01-01T00:00:00Z" } }),
    );
    let body = search_response(vec![hit]).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.timestamp_field = Some("event.created".to_string());
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    let produced = source.poll().await.expect("poll should succeed");

    assert_eq!(produced.messages[0].origin_timestamp, None);
    let state: State = produced
        .state
        .expect("moved cursor stages state")
        .deserialize(crate::CONNECTOR_NAME, 1)
        .expect("state deserializes");
    assert_eq!(
        state
            .last_poll_timestamp
            .map(|timestamp| timestamp.timestamp()),
        Some(1_704_067_200)
    );
}

/// Answers `HEAD` with 200 and the first `stalled_attempts` searches with headers plus a
/// partial body that never completes. Later searches get `body` in full.
async fn start_body_stalling_server(
    stalled_attempts: usize,
    body: String,
    searches: Arc<AtomicUsize>,
) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let body = body.clone();
            let searches = searches.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut chunk = [0u8; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&chunk[..read]),
                    }
                }
                let response = if request.starts_with(b"HEAD") {
                    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
                } else if searches.fetch_add(1, Ordering::SeqCst) < stalled_attempts {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{{",
                        body.len() + 2
                    )
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                };
                let _ = stream.write_all(response.as_bytes()).await;
                tokio::time::sleep(STALL).await;
            });
        }
    });
    format!("http://127.0.0.1:{port}")
}

#[tokio::test]
async fn given_body_stalled_after_headers_when_poll_should_fail_as_connection_error() {
    let searches = Arc::new(AtomicUsize::new(0));
    let base = start_body_stalling_server(usize::MAX, String::new(), searches.clone()).await;
    let mut config = base_config(&base);
    config.request_timeout = Some("100ms".to_string());
    config.max_retries = Some(2);
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    let error = source.poll().await.expect_err("stalled body should fail");

    assert!(matches!(error, Error::Connection(_)), "got {error:?}");
    assert_eq!(
        searches.load(Ordering::SeqCst),
        2,
        "body timeout must retry"
    );
}

#[tokio::test]
async fn given_body_stalled_on_first_attempt_when_poll_should_retry_and_succeed() {
    let searches = Arc::new(AtomicUsize::new(0));
    let body =
        search_response(vec![search_hit("a", "2024-01-01T00:00:00Z", json!({}))]).to_string();
    let base = start_body_stalling_server(1, body, searches.clone()).await;
    let mut config = base_config(&base);
    config.request_timeout = Some("100ms".to_string());
    config.max_retries = Some(2);
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    let produced = source.poll().await.expect("retry should recover the poll");

    assert_eq!(produced.messages.len(), 1);
    assert_eq!(searches.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn given_search_failure_when_poll_should_increment_error_count() {
    let app = mock_router(StatusCode::OK, |_| async move {
        (StatusCode::INTERNAL_SERVER_ERROR, "boom".to_string())
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let error = source.poll().await.expect_err("poll should fail");
    assert!(matches!(error, Error::HttpRequestFailed(_)));

    let (_, _, errors) = source.test_metrics().await;
    assert_eq!(errors, 1);
}

#[tokio::test]
async fn given_basic_auth_when_search_should_send_authorization_header() {
    let captured: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let cap = captured.clone();
    let app = mock_router(StatusCode::OK, move |request: Request| {
        let cap = cap.clone();
        async move {
            let auth = request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_string();
            *cap.lock().await = auth;
            (StatusCode::OK, search_response(vec![]).to_string())
        }
    });
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.username = Some("iggy".to_string());
    config.password = Some(SecretString::from("secret"));
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();
    source.poll().await.unwrap();

    let auth = captured.lock().await.clone();
    assert!(auth.starts_with("Basic "));
}

#[tokio::test]
async fn given_cursor_set_when_empty_poll_should_preserve_cursor() {
    let request_count = Arc::new(AtomicUsize::new(0));
    let count = request_count.clone();

    let first_page = search_response(vec![search_hit("doc-1", "2024-01-01T00:00:00Z", json!({}))]);
    let empty_page = search_response(vec![]);

    let app = mock_router(StatusCode::OK, move |_| {
        let first_page = first_page.clone();
        let empty_page = empty_page.clone();
        let count = count.clone();
        async move {
            let page = count.fetch_add(1, Ordering::SeqCst);
            let response = if page == 0 { first_page } else { empty_page };
            (StatusCode::OK, response.to_string())
        }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    source.poll().await.expect("first poll");
    ack(&source).await;
    let cursor_after_first = source.test_last_sort().await;
    assert!(
        cursor_after_first.is_some(),
        "cursor must be set after non-empty poll"
    );

    source.poll().await.expect("empty poll");
    let cursor_after_empty = source.test_last_sort().await;
    assert_eq!(
        cursor_after_empty, cursor_after_first,
        "empty poll must not reset cursor to None"
    );
}

fn hit_with_sort_no_source(doc_id: &str, timestamp: &str) -> Value {
    json!({
        "_id": doc_id,
        "sort": [timestamp, doc_id]
    })
}

async fn poll_single_page(hits: Vec<Value>) -> (OpenSearchSource, ProducedMessages) {
    let body = search_response(hits).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();
    let produced = source.poll().await.expect("poll should succeed");
    (source, produced)
}

#[tokio::test]
async fn given_all_hits_missing_source_when_poll_should_skip_them_and_advance_cursor_on_ack() {
    let (source, produced) = poll_single_page(vec![
        hit_with_sort_no_source("doc-1", "2024-01-01T00:00:00Z"),
        hit_with_sort_no_source("doc-2", "2024-01-02T00:00:00Z"),
    ])
    .await;

    assert!(produced.messages.is_empty());
    assert!(
        produced.state.is_some(),
        "a moved cursor must be handed to the runtime for persistence"
    );
    assert!(source.test_last_sort().await.is_none());
    ack(&source).await;
    let cursor = source.test_last_sort().await.expect("cursor advanced");
    assert_eq!(
        cursor[1].as_str(),
        Some("doc-2"),
        "cursor must move past hits that have no body, or the poll would repeat forever"
    );
}

#[tokio::test]
async fn given_trailing_hit_missing_source_when_poll_should_publish_prefix_and_advance_past_it() {
    let (source, produced) = poll_single_page(vec![
        search_hit("doc-1", "2024-01-01T00:00:00Z", json!({})),
        hit_with_sort_no_source("doc-2", "2024-01-02T00:00:00Z"),
    ])
    .await;

    assert_eq!(produced.messages.len(), 1);
    ack(&source).await;
    let cursor = source.test_last_sort().await.expect("cursor set");
    assert_eq!(cursor[1].as_str(), Some("doc-2"));
}

#[tokio::test]
async fn given_middle_hit_missing_source_when_poll_should_publish_other_hits() {
    let (source, produced) = poll_single_page(vec![
        search_hit("doc-a", "2024-01-01T00:00:00Z", json!({"name": "a"})),
        hit_with_sort_no_source("doc-b", "2024-01-02T00:00:00Z"),
        search_hit("doc-c", "2024-01-03T00:00:00Z", json!({"name": "c"})),
    ])
    .await;

    let names: Vec<Value> = produced
        .messages
        .iter()
        .map(|message| serde_json::from_slice::<Value>(&message.payload).unwrap()["name"].clone())
        .collect();
    assert_eq!(names, vec![json!("a"), json!("c")]);
    ack(&source).await;
    let cursor = source.test_last_sort().await.expect("cursor set");
    assert_eq!(cursor[1].as_str(), Some("doc-c"));
}

#[tokio::test]
async fn given_hit_without_sort_tuple_when_poll_should_fail_without_advancing_cursor() {
    let mut unsorted = search_hit("doc-2", "2024-01-02T00:00:00Z", json!({}));
    unsorted.as_object_mut().unwrap().remove("sort");
    let body = search_response(vec![
        search_hit("doc-1", "2024-01-01T00:00:00Z", json!({})),
        unsorted,
    ])
    .to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let error = source
        .poll()
        .await
        .expect_err("missing sort tuple must fail");
    assert!(matches!(error, Error::SchemaMismatch(_)), "got {error:?}");
    assert!(source.test_last_sort().await.is_none());
    let (_, _, errors) = source.test_metrics().await;
    assert_eq!(errors, 1, "error counter must be incremented");
}

#[tokio::test]
async fn given_poll_when_search_sent_should_request_safe_pagination() {
    let requests = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
    let captured = requests.clone();
    let app = mock_router(StatusCode::OK, move |request| {
        let captured = captured.clone();
        async move {
            let query = request.uri().query().unwrap_or_default().to_string();
            let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap();
            captured
                .lock()
                .await
                .push((query, serde_json::from_slice(&bytes).unwrap()));
            (StatusCode::OK, search_response(vec![]).to_string())
        }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();
    source.poll().await.unwrap();

    let requests = requests.lock().await;
    let (query, body) = &requests[0];
    assert_eq!(body["track_total_hits"], json!(false));
    assert!(
        query.contains("allow_partial_search_results=false"),
        "partial results can hide documents behind the cursor"
    );
    assert_eq!(
        body["sort"][0]["timestamp"]["missing"],
        json!("_first"),
        "documents without the timestamp must sort first, or they park the cursor at the end"
    );
}

#[tokio::test]
async fn given_hit_sorted_on_missing_value_sentinel_when_poll_should_fail_without_advancing_cursor()
{
    let mut sentinel = search_hit("doc-1", "2024-01-01T00:00:00Z", json!({}));
    sentinel["sort"] = json!([i64::MAX, "doc-1", TEST_INDEX]);
    let body = search_response(vec![sentinel]).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let error = source.poll().await.expect_err("sentinel sort must fail");

    assert!(matches!(error, Error::SchemaMismatch(_)), "got {error:?}");
    assert!(source.test_last_sort().await.is_none());
}

#[tokio::test]
async fn given_poll_without_open_should_return_connection_error() {
    let source = OpenSearchSource::new(1, base_config("http://127.0.0.1:9"), None);
    let error = source.poll().await.expect_err("poll without open");
    assert!(matches!(error, Error::Connection(_)));
}

#[tokio::test]
async fn given_open_when_close_should_clear_client() {
    let app = empty_search_router(StatusCode::OK);
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();
    source.close().await.expect("close should succeed");
    assert!(!source.client_initialized());
}

#[tokio::test]
async fn given_epoch_seconds_timestamp_should_update_last_poll_timestamp() {
    let hit = search_hit("doc-1", "2024-01-15T10:00:00Z", json!({}));
    let mut hit = hit;
    // Override _source.timestamp with epoch-seconds integer to exercise the numeric
    // branch of parse_document_timestamp. Sort tuple stays as the RFC3339 string that
    // search_hit() placed there; sort is used only for cursor positioning.
    hit["_source"]["timestamp"] = json!(1_705_312_200_i64);
    let body = search_response(vec![hit]).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();
    source.poll().await.unwrap();
    ack(&source).await;

    let last_ts = source.test_last_poll_timestamp().await;
    assert!(
        last_ts.is_some(),
        "epoch-seconds timestamp must be parsed and stored in state"
    );
}

#[tokio::test]
async fn given_transient_search_errors_when_poll_should_retry_and_succeed() {
    let request_count = Arc::new(AtomicUsize::new(0));
    let count = request_count.clone();

    let app = mock_router(StatusCode::OK, move |_| {
        let count = count.clone();
        async move {
            let attempt = count.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                (StatusCode::SERVICE_UNAVAILABLE, "temporary".to_string())
            } else {
                (StatusCode::OK, search_response(vec![]).to_string())
            }
        }
    });
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.max_retries = Some(3);
    config.retry_delay = Some("1ms".to_string());
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    source
        .poll()
        .await
        .expect("poll should succeed after retries");
    assert!(
        request_count.load(Ordering::SeqCst) >= 3,
        "search should be retried after transient 503"
    );
}

async fn poll_against_status(status: StatusCode, max_retries: u32) -> (usize, Error) {
    let request_count = Arc::new(AtomicUsize::new(0));
    let count = request_count.clone();
    let app = mock_router(StatusCode::OK, move |_| {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            (status, "failure".to_string())
        }
    });
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.max_retries = Some(max_retries);
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    let error = source.poll().await.expect_err("poll should fail");
    (request_count.load(Ordering::SeqCst), error)
}

#[tokio::test]
async fn given_permanent_4xx_when_poll_should_not_retry() {
    let (requests, error) = poll_against_status(StatusCode::BAD_REQUEST, 3).await;
    assert_eq!(requests, 1, "a permanent 4xx must not be retried");
    assert!(matches!(error, Error::PermanentHttpError(_)));
}

#[tokio::test]
async fn given_persistent_503_when_poll_should_stop_at_attempt_budget() {
    let (requests, error) = poll_against_status(StatusCode::SERVICE_UNAVAILABLE, 3).await;
    assert_eq!(requests, 3, "max_retries is a total attempt count");
    assert!(matches!(error, Error::HttpRequestFailed(_)));
}

#[tokio::test]
async fn given_partial_search_response_when_poll_should_fail_without_moving_cursor() {
    for partial_marker in [
        json!({ "timed_out": true }),
        json!({ "_shards": { "total": 3, "successful": 2, "failed": 1 } }),
    ] {
        let mut body =
            search_response(vec![search_hit("doc-2", "2024-01-02T00:00:00Z", json!({}))]);
        body.as_object_mut()
            .unwrap()
            .extend(partial_marker.as_object().unwrap().clone());
        let body = body.to_string();
        let app = mock_router(StatusCode::OK, move |_| {
            let body = body.clone();
            async move { (StatusCode::OK, body) }
        });
        let base = start_server(app).await;
        let mut source = OpenSearchSource::new(1, base_config(&base), None);
        source.open().await.unwrap();

        let error = source.poll().await.expect_err("partial page must fail");

        assert!(
            matches!(error, Error::HttpRequestFailed(_)),
            "got {error:?}"
        );
        assert!(source.test_last_sort().await.is_none());
    }
}

#[tokio::test]
async fn given_permanent_error_when_polled_repeatedly_should_not_open_circuit_breaker() {
    let request_count = Arc::new(AtomicUsize::new(0));
    let count = request_count.clone();
    let app = mock_router(StatusCode::OK, move |_| {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            (StatusCode::BAD_REQUEST, "bad query".to_string())
        }
    });
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.circuit_breaker_threshold = Some(1);
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    for _ in 0..3 {
        let error = source
            .poll()
            .await
            .expect_err("permanent error must surface");
        assert!(
            matches!(error, Error::PermanentHttpError(_)),
            "got {error:?}"
        );
    }
    assert_eq!(request_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn given_circuit_breaker_open_when_poll_should_return_empty_without_error() {
    let app = mock_router(StatusCode::OK, |_| async move {
        (StatusCode::INTERNAL_SERVER_ERROR, "boom".to_string())
    });
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.circuit_breaker_threshold = Some(1);
    config.circuit_breaker_cool_down = Some("60s".to_string());
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    let _ = source.poll().await.expect_err("first poll should fail");

    let produced = source
        .poll()
        .await
        .expect("open circuit should skip search");
    assert!(produced.messages.is_empty());
    assert!(produced.state.is_none());
}

const SAME_TIMESTAMP: &str = "2024-01-01T00:00:00Z";

fn scripted_router(pages: Vec<Value>) -> (Router, Arc<Mutex<Vec<Value>>>) {
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let router = mock_router(StatusCode::OK, {
        let bodies = bodies.clone();
        move |request: Request| {
            let pages = pages.clone();
            let bodies = bodies.clone();
            let calls = calls.clone();
            async move {
                let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
                    .await
                    .unwrap_or_default();
                if let Ok(body) = serde_json::from_slice::<Value>(&bytes) {
                    bodies.lock().await.push(body);
                }
                let page = calls.fetch_add(1, Ordering::SeqCst).min(pages.len() - 1);
                (StatusCode::OK, pages[page].to_string())
            }
        }
    });
    (router, bodies)
}

fn same_timestamp_hit(doc_id: &str) -> Value {
    search_hit(doc_id, SAME_TIMESTAMP, json!({}))
}

fn restart_tuple() -> Value {
    json!([SAME_TIMESTAMP, "", ""])
}

#[tokio::test]
async fn given_late_visible_hit_with_smaller_id_when_polling_again_should_publish_it() {
    let (app, bodies) = scripted_router(vec![
        search_response(vec![same_timestamp_hit("zz")]),
        search_response(vec![same_timestamp_hit("aa"), same_timestamp_hit("zz")]),
    ]);
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let first = source.poll().await.expect("first poll");
    assert_eq!(first.messages.len(), 1);
    ack(&source).await;
    let second = source.poll().await.expect("second poll");

    assert_eq!(second.messages.len(), 1);
    assert_eq!(
        second.messages[0].id,
        Some(crate::message_id(TEST_INDEX, "aa"))
    );
    let bodies = bodies.lock().await;
    assert_eq!(bodies[1]["search_after"], restart_tuple());
}

#[tokio::test]
async fn given_only_published_hits_when_polling_again_should_publish_nothing_and_stage_nothing() {
    let (app, _) = scripted_router(vec![search_response(vec![same_timestamp_hit("a")])]);
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();
    source.poll().await.expect("first poll");
    ack(&source).await;
    let cursor_before = source.test_last_sort().await;

    let second = source.poll().await.expect("second poll");

    assert!(second.messages.is_empty());
    assert!(second.state.is_none());
    assert_eq!(source.test_last_sort().await, cursor_before);
}

#[tokio::test]
async fn given_full_page_of_published_hits_when_polling_should_step_to_the_next_page() {
    let (app, bodies) = scripted_router(vec![
        search_response(vec![same_timestamp_hit("a"), same_timestamp_hit("b")]),
        search_response(vec![same_timestamp_hit("c")]),
        search_response(vec![same_timestamp_hit("a"), same_timestamp_hit("b")]),
        search_response(vec![same_timestamp_hit("c"), same_timestamp_hit("d")]),
    ]);
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.batch_size = Some(2);
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();
    for _ in 0..2 {
        source.poll().await.expect("poll that publishes");
        ack(&source).await;
    }

    let third = source
        .poll()
        .await
        .expect("poll that steps past published hits");

    assert_eq!(third.messages.len(), 1);
    let bodies = bodies.lock().await;
    assert_eq!(bodies.len(), 4);
    assert_eq!(bodies[1]["search_after"], json!([SAME_TIMESTAMP, "b"]));
    assert_eq!(bodies[2]["search_after"], restart_tuple());
    assert_eq!(bodies[3]["search_after"], json!([SAME_TIMESTAMP, "b"]));
}

#[tokio::test]
async fn given_persisted_state_when_first_poll_should_scan_from_the_watermark() {
    let (app, bodies) = scripted_router(vec![search_response(vec![])]);
    let base = start_server(app).await;
    let state = State {
        last_sort: Some(vec![json!(SAME_TIMESTAMP), json!("doc-9")]),
        ..State::default()
    };
    let persisted = ConnectorState::serialize(&state, "test", 1);
    let mut source = OpenSearchSource::new(1, base_config(&base), persisted);
    source.open().await.unwrap();

    source.poll().await.expect("poll");

    assert_eq!(bodies.lock().await[0]["search_after"], restart_tuple());
}

#[tokio::test]
async fn given_too_many_hits_at_one_timestamp_when_polling_should_fail_without_moving() {
    let (app, _) = scripted_router(vec![search_response(vec![same_timestamp_hit("fresh")])]);
    let base = start_server(app).await;
    let state = State {
        last_sort: Some(vec![json!(SAME_TIMESTAMP), json!("doc-0")]),
        watermark_hits: (0..MAX_WATERMARK_HITS)
            .map(|number| HitKey {
                index: TEST_INDEX.to_string(),
                id: format!("doc-{number}"),
            })
            .collect(),
        ..State::default()
    };
    let persisted = ConnectorState::serialize(&state, "test", 1);
    let mut source = OpenSearchSource::new(1, base_config(&base), persisted);
    source.open().await.unwrap();
    let cursor_before = source.test_last_sort().await;

    let error = source.poll().await.expect_err("cap should fail the poll");

    assert!(matches!(error, Error::SchemaMismatch(_)), "got {error:?}");
    assert_eq!(source.test_last_sort().await, cursor_before);
}

fn keyword_mapping_router(
    field: &str,
    mapping: Value,
    pages: Vec<Value>,
) -> (Router, Arc<Mutex<Vec<Value>>>) {
    let (router, bodies) = scripted_router(pages);
    let body = json!({
        TEST_INDEX: { "mappings": { field: { "full_name": field, "mapping": { field: mapping } } } }
    });
    let router = router.route(
        &format!("/{TEST_INDEX}/_mapping/field/{field}"),
        get(move || {
            let body = body.clone();
            async move { (StatusCode::OK, body.to_string()) }
        }),
    );
    (router, bodies)
}

#[tokio::test]
async fn given_keyword_tiebreaker_when_polling_should_sort_on_it_instead_of_id() {
    let (app, bodies) = keyword_mapping_router(
        "event_id",
        json!({ "type": "keyword" }),
        vec![search_response(vec![])],
    );
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.tiebreaker_field = Some("event_id".to_string());
    let mut source = OpenSearchSource::new(1, config, None);
    source
        .open()
        .await
        .expect("keyword tie-breaker is accepted");

    source.poll().await.expect("poll");

    let sort = bodies.lock().await[0]["sort"].clone();
    assert_eq!(sort[1], json!({ "event_id": { "order": "asc" } }));
}

#[tokio::test]
async fn given_documents_sharing_an_id_when_keyword_tiebreaker_set_should_keep_both() {
    let routed_hit = |event_id: &str| {
        json!({
            "_index": TEST_INDEX,
            "_id": "routed",
            "_source": { "timestamp": SAME_TIMESTAMP },
            "sort": [SAME_TIMESTAMP, event_id, TEST_INDEX]
        })
    };
    let (app, _) = keyword_mapping_router(
        "event_id",
        json!({ "type": "keyword" }),
        vec![search_response(vec![
            routed_hit("event-a"),
            routed_hit("event-b"),
        ])],
    );
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.tiebreaker_field = Some("event_id".to_string());
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    let produced = source.poll().await.expect("poll");

    let ids: Vec<_> = produced.messages.iter().map(|message| message.id).collect();
    assert_eq!(
        ids,
        vec![
            Some(crate::message_id(TEST_INDEX, "event-a")),
            Some(crate::message_id(TEST_INDEX, "event-b")),
        ]
    );
    for message in &produced.messages {
        let headers = message.headers.as_ref().expect("headers");
        let document_id = headers
            .get(&*crate::DOCUMENT_ID_HEADER_KEY)
            .expect("document id header");
        assert_eq!(document_id.as_str().expect("string header"), "routed");
    }
}

#[tokio::test]
async fn given_keyword_tiebreaker_when_hit_lacks_its_value_should_fail_with_schema_mismatch() {
    let hit = json!({
        "_index": TEST_INDEX,
        "_id": "a",
        "_source": { "timestamp": SAME_TIMESTAMP },
        "sort": [SAME_TIMESTAMP]
    });
    let (app, _) = keyword_mapping_router(
        "event_id",
        json!({ "type": "keyword" }),
        vec![search_response(vec![hit])],
    );
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.tiebreaker_field = Some("event_id".to_string());
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();

    let error = source.poll().await.expect_err("hit without tie-breaker");

    assert!(matches!(error, Error::SchemaMismatch(_)), "got {error:?}");
}

#[tokio::test]
async fn given_default_tiebreaker_when_polling_should_sort_on_id() {
    let (app, bodies) = scripted_router(vec![search_response(vec![])]);
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    source.poll().await.expect("poll");

    let sort = bodies.lock().await[0]["sort"].clone();
    assert_eq!(sort[1], json!({ "_id": { "order": "asc" } }));
}

#[tokio::test]
async fn given_non_keyword_tiebreaker_when_open_should_return_invalid_config() {
    let (app, _) = keyword_mapping_router("event_id", json!({ "type": "text" }), vec![]);
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.tiebreaker_field = Some("event_id".to_string());
    let mut source = OpenSearchSource::new(1, config, None);

    let error = source.open().await.expect_err("text field is rejected");

    assert!(
        matches!(error, Error::InvalidConfigValue(_)),
        "got {error:?}"
    );
}
