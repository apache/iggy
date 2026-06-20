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

use crate::state_manager::{SourceState, create_state_storage};
use crate::{OpenSearchSource, OpenSearchSourceConfig, StateConfig};
use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::routing::{head, post};
use iggy_connector_sdk::{Error, Source};
use secrecy::SecretString;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

const TEST_INDEX: &str = "test_documents";

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
        state: None,
        max_retries: Some(1),
        retry_delay: Some("1ms".to_string()),
        retry_max_delay: Some("10ms".to_string()),
        max_open_retries: Some(1),
        open_retry_max_delay: Some("10ms".to_string()),
        circuit_breaker_threshold: None,
        circuit_breaker_cool_down: None,
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
async fn given_invalid_url_when_open_should_fail() {
    let mut config = base_config("http://127.0.0.1:1");
    config.url = "not-a-valid-url".to_string();
    let mut source = OpenSearchSource::new(1, config, None);
    let error = source.open().await.expect_err("open should fail");
    assert!(matches!(error, Error::InvalidConfigValue(_)));
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

    let (fetched, polls, _, _) = source.test_metrics().await;
    assert_eq!(fetched, 1);
    assert_eq!(polls, 1);
}

#[tokio::test]
async fn given_empty_search_when_poll_should_increment_empty_poll_count() {
    let app = empty_search_router(StatusCode::OK);
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let produced = source.poll().await.expect("poll should succeed");
    assert!(produced.messages.is_empty());

    let (_, _, _, empty_polls) = source.test_metrics().await;
    assert_eq!(empty_polls, 1);
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

    let second = source.poll().await.expect("second poll");
    assert_eq!(second.messages.len(), 1);

    let bodies = captured_bodies.lock().await;
    assert_eq!(bodies.len(), 2);
    assert!(bodies[0].get("search_after").is_none());
    assert!(bodies[1].get("search_after").is_some());

    let (fetched, _, _, _) = source.test_metrics().await;
    assert_eq!(fetched, 2);
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
async fn given_search_failure_when_poll_should_increment_error_count() {
    let app = mock_router(StatusCode::OK, |_| async move {
        (StatusCode::INTERNAL_SERVER_ERROR, "boom".to_string())
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    let error = source.poll().await.expect_err("poll should fail");
    assert!(matches!(error, Error::Storage(_)));

    let (_, _, errors, _) = source.test_metrics().await;
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
async fn given_batch_where_all_hits_lack_sort_when_poll_should_return_error() {
    let hit = json!({
        "_id": "doc-1",
        "_source": { "id": 1, "timestamp": "2024-01-01T00:00:00Z" }
    });
    let body = search_response(vec![hit]).to_string();
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
        .expect_err("all-no-sort batch must fail");
    assert!(
        matches!(error, Error::Storage(_)),
        "expected Storage error, got {error:?}"
    );

    let (_, _, errors, _) = source.test_metrics().await;
    assert_eq!(errors, 1, "error counter must be incremented");
    assert!(
        source.test_search_after().await.is_none(),
        "cursor must not advance when batch errors"
    );
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
    let cursor_after_first = source.test_search_after().await;
    assert!(
        cursor_after_first.is_some(),
        "cursor must be set after non-empty poll"
    );

    source.poll().await.expect("empty poll");
    let cursor_after_empty = source.test_search_after().await;
    assert_eq!(
        cursor_after_empty, cursor_after_first,
        "empty poll must not reset cursor to None"
    );
}

#[tokio::test]
async fn given_all_hits_missing_source_when_poll_should_return_error() {
    let hit = json!({
        "_id": "doc-1",
        "sort": ["2024-01-01T00:00:00Z", "doc-1"]
    });
    let body = search_response(vec![hit]).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;
    let mut source = OpenSearchSource::new(1, base_config(&base), None);
    source.open().await.unwrap();

    // All hits have valid sort tuples but none have _source; cursor must not advance.
    let error = source
        .poll()
        .await
        .expect_err("all-_source-absent batch must error");
    assert!(
        matches!(error, Error::Storage(_)),
        "expected Storage error, got {error:?}"
    );
    assert!(
        source.test_search_after().await.is_none(),
        "cursor must not advance when entire batch has no _source"
    );
}

#[tokio::test]
async fn given_trailing_hit_without_source_when_poll_should_advance_cursor_past_it() {
    let request_count = Arc::new(AtomicUsize::new(0));
    let count = request_count.clone();

    let first_page = search_response(vec![
        search_hit("doc-1", "2024-01-01T00:00:00Z", json!({})),
        json!({
            "_id": "doc-2",
            "sort": ["2024-01-02T00:00:00Z", "doc-2"]
        }),
    ]);
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

    let produced = source.poll().await.expect("first poll");
    assert_eq!(
        produced.messages.len(),
        1,
        "only doc-1 published; doc-2 has no _source"
    );

    let cursor = source.test_search_after().await;
    assert!(
        cursor.is_some(),
        "cursor must be set after batch with trailing no-_source hit"
    );
    let cursor_vals = cursor.unwrap();
    assert_eq!(
        cursor_vals[1].as_str(),
        Some("doc-2"),
        "cursor must point to doc-2 (trailing no-_source), not doc-1"
    );

    // Second poll must get empty page (cursor past doc-2), not re-fetch doc-2.
    let produced2 = source.poll().await.expect("second poll");
    assert!(
        produced2.messages.is_empty(),
        "doc-2 (no _source) must not be re-fetched after cursor advances past it"
    );
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
async fn given_enabled_file_state_when_open_close_should_persist_state() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let base_path = temp_dir.path().to_string_lossy().to_string();

    let hits = vec![search_hit("doc-1", "2024-01-01T00:00:00Z", json!({}))];
    let body = search_response(hits).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;

    let mut config = base_config(&base);
    config.state = Some(StateConfig {
        enabled: true,
        storage_type: Some("file".to_string()),
        storage_config: Some(json!({ "base_path": base_path })),
        state_id: Some("opensearch_test_state".to_string()),
    });

    let mut source = OpenSearchSource::new(1, config.clone(), None);
    source.open().await.unwrap();
    source.poll().await.unwrap();
    source.close().await.unwrap();

    let mut reloaded = OpenSearchSource::new(2, config, None);
    reloaded.open().await.unwrap();
    let (fetched, polls, _, _) = reloaded.test_metrics().await;
    assert_eq!(fetched, 1);
    assert_eq!(polls, 1);
    assert!(
        reloaded.test_search_after().await.is_some(),
        "search_after cursor must be restored from file state"
    );
}

#[tokio::test]
async fn given_runtime_state_when_open_should_not_load_stale_file_state() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let base_path = temp_dir.path().to_string_lossy().to_string();
    let state_id = "opensearch_runtime_authoritative";

    let hits = vec![search_hit("doc-1", "2024-01-01T00:00:00Z", json!({}))];
    let body = search_response(hits).to_string();
    let app = mock_router(StatusCode::OK, move |_| {
        let body = body.clone();
        async move { (StatusCode::OK, body) }
    });
    let base = start_server(app).await;

    let mut config = base_config(&base);
    config.state = Some(StateConfig {
        enabled: true,
        storage_type: Some("file".to_string()),
        storage_config: Some(json!({ "base_path": base_path.clone() })),
        state_id: Some(state_id.to_string()),
    });

    let mut source = OpenSearchSource::new(1, config.clone(), None);
    source.open().await.unwrap();
    let produced = source.poll().await.expect("poll");
    let runtime_state = produced.state.expect("runtime state persisted");
    source.close().await.unwrap();

    let stale_state = SourceState {
        id: state_id.to_string(),
        last_updated: iggy_common::Utc::now(),
        version: crate::state_manager::SOURCE_STATE_VERSION,
        data: json!({
            "search_after": ["1970-01-01T00:00:00Z", "stale-doc"],
            "poll_count": 99
        }),
        metadata: None,
    };
    let storage = create_state_storage(config.state.as_ref().unwrap()).expect("file storage");
    storage
        .save_source_state(&stale_state)
        .await
        .expect("write stale file");

    let mut restarted = OpenSearchSource::new(2, config, Some(runtime_state));
    restarted.open().await.unwrap();

    let (_, polls, _, _) = restarted.test_metrics().await;
    assert_eq!(
        polls, 1,
        "runtime ConnectorState must not be overwritten by stale file"
    );
}

#[tokio::test]
async fn given_epoch_seconds_timestamp_should_update_last_poll_timestamp() {
    let hit = search_hit("doc-1", "2024-01-15T10:00:00Z", json!({}));
    let mut hit = hit;
    // Override _source.timestamp with epoch-seconds integer to exercise the numeric
    // branch of parse_document_timestamp. Sort tuple stays as the RFC3339 string that
    // search_hit() placed there — sort is used only for cursor positioning.
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

    let last_ts = source.test_last_poll_timestamp().await;
    assert!(
        last_ts.is_some(),
        "epoch-seconds timestamp must be parsed and stored in state"
    );
}

#[tokio::test]
async fn given_verbose_logging_when_poll_should_succeed() {
    let app = empty_search_router(StatusCode::OK);
    let base = start_server(app).await;
    let mut config = base_config(&base);
    config.verbose_logging = true;
    let mut source = OpenSearchSource::new(1, config, None);
    source.open().await.unwrap();
    source.poll().await.expect("verbose poll should succeed");
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
