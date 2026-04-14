/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! InfluxDB V2 source — Flux queries, annotated-CSV responses, Token auth.

use crate::common::{
    PayloadFormat, Row, V2SourceConfig, V2State, apply_query_params, is_timestamp_after,
    parse_csv_rows, parse_scalar, validate_cursor,
};
use base64::{Engine as _, engine::general_purpose};
use iggy_connector_influxdb_common::ApiVersion;
use iggy_connector_sdk::{Error, ProducedMessage, Schema};
use reqwest_middleware::ClientWithMiddleware;
use serde_json::json;
use std::sync::OnceLock;
use uuid::Uuid;

// Allocated once; reused on every poll to avoid a per-call Box allocation.
static ADAPTER: OnceLock<Box<dyn iggy_connector_influxdb_common::InfluxDbAdapter>> =
    OnceLock::new();

fn adapter() -> &'static dyn iggy_connector_influxdb_common::InfluxDbAdapter {
    &**ADAPTER.get_or_init(|| ApiVersion::V2.make_adapter())
}

/// Maximum multiple of `batch_size` by which `already_seen` may inflate the
/// query limit. Prevents an unbounded request to InfluxDB when the cursor
/// is stuck at the same timestamp for many consecutive polls (analogous to
/// V3's `stuck_batch_cap_factor`).
const MAX_SKIP_INFLATION_FACTOR: u64 = 10;

/// Render the final Flux query by substituting `$cursor` and `$limit`.
///
/// The limit is inflated by `already_seen` (rows at the current cursor
/// timestamp that were delivered in a previous batch) so that re-fetching
/// with `>= cursor` returns enough rows to skip them and still fill a full
/// batch. Inflation is capped at `MAX_SKIP_INFLATION_FACTOR × batch_size`
/// to prevent excessively large queries when the cursor is stuck.
fn render_query(config: &V2SourceConfig, cursor: &str, already_seen: u64) -> Result<String, Error> {
    validate_cursor(cursor)?;
    let batch = config.batch_size.unwrap_or(500) as u64;
    // Cap inflation so a stuck cursor cannot issue arbitrarily large queries.
    let capped_seen = already_seen.min(batch.saturating_mul(MAX_SKIP_INFLATION_FACTOR));
    let limit = batch.saturating_add(capped_seen).to_string();
    Ok(apply_query_params(&config.query, cursor, &limit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Row;

    fn row(pairs: &[(&str, &str)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    const BASE_CURSOR: &str = "1970-01-01T00:00:00Z";
    const T1: &str = "2024-01-01T00:00:00Z";
    const T2: &str = "2024-01-01T00:00:01Z";
    const T3: &str = "2024-01-01T00:00:02Z";

    #[test]
    fn process_rows_empty_returns_empty() {
        let result = process_rows(
            &[],
            BASE_CURSOR,
            0,
            "_time",
            true,
            None,
            PayloadFormat::Json,
            1000,
            0,
        )
        .unwrap();
        assert!(result.messages.is_empty());
        assert!(result.max_cursor.is_none());
        assert_eq!(result.skipped, 0);
        assert_eq!(result.rows_at_max_cursor, 0);
    }

    #[test]
    fn process_rows_single_row_produces_one_message() {
        let rows = vec![row(&[("_time", T1), ("_value", "42")])];
        let result = process_rows(
            &rows,
            BASE_CURSOR,
            0,
            "_time",
            true,
            None,
            PayloadFormat::Json,
            1000,
            0,
        )
        .unwrap();
        assert_eq!(result.messages.len(), 1);
        assert_eq!(result.max_cursor.as_deref(), Some(T1));
        assert_eq!(result.rows_at_max_cursor, 1);
        assert_eq!(result.skipped, 0);
    }

    #[test]
    fn process_rows_skips_already_seen_at_cursor() {
        // Three rows all at T1, cursor=T1, already_seen=1 → skip first, produce two.
        let rows = vec![
            row(&[("_time", T1), ("_value", "1")]),
            row(&[("_time", T1), ("_value", "2")]),
            row(&[("_time", T1), ("_value", "3")]),
        ];
        let result = process_rows(
            &rows,
            T1,
            1,
            "_time",
            true,
            None,
            PayloadFormat::Json,
            1000,
            0,
        )
        .unwrap();
        assert_eq!(result.skipped, 1);
        assert_eq!(result.messages.len(), 2);
    }

    #[test]
    fn process_rows_does_not_skip_beyond_already_seen() {
        // already_seen=1 but there are 3 rows at cursor; only the first should be skipped.
        let rows = vec![
            row(&[("_time", T1)]),
            row(&[("_time", T1)]),
            row(&[("_time", T1)]),
        ];
        let result = process_rows(
            &rows,
            T1,
            1,
            "_time",
            true,
            None,
            PayloadFormat::Json,
            1000,
            0,
        )
        .unwrap();
        assert_eq!(result.skipped, 1);
        assert_eq!(result.messages.len(), 2);
    }

    #[test]
    fn process_rows_tracks_latest_max_cursor() {
        let rows = vec![
            row(&[("_time", T1)]),
            row(&[("_time", T3)]),
            row(&[("_time", T2)]),
        ];
        let result = process_rows(
            &rows,
            BASE_CURSOR,
            0,
            "_time",
            true,
            None,
            PayloadFormat::Json,
            1000,
            0,
        )
        .unwrap();
        assert_eq!(result.max_cursor.as_deref(), Some(T3));
        assert_eq!(result.rows_at_max_cursor, 1);
    }

    #[test]
    fn process_rows_counts_rows_at_max_cursor() {
        let rows = vec![
            row(&[("_time", T1)]),
            row(&[("_time", T2)]),
            row(&[("_time", T2)]),
        ];
        let result = process_rows(
            &rows,
            BASE_CURSOR,
            0,
            "_time",
            true,
            None,
            PayloadFormat::Json,
            1000,
            0,
        )
        .unwrap();
        assert_eq!(result.max_cursor.as_deref(), Some(T2));
        assert_eq!(result.rows_at_max_cursor, 2);
    }

    #[test]
    fn process_rows_message_ids_are_sequential_from_uuid_base() {
        let rows = vec![row(&[("_time", T1)]), row(&[("_time", T2)])];
        let result = process_rows(
            &rows,
            BASE_CURSOR,
            0,
            "_time",
            true,
            None,
            PayloadFormat::Json,
            1000,
            100,
        )
        .unwrap();
        assert_eq!(result.messages[0].id, Some(100u128));
        assert_eq!(result.messages[1].id, Some(101u128));
    }

    #[test]
    fn process_rows_message_timestamps_use_now_micros() {
        let rows = vec![row(&[("_time", T1)])];
        let result = process_rows(
            &rows,
            BASE_CURSOR,
            0,
            "_time",
            true,
            None,
            PayloadFormat::Json,
            999_999,
            0,
        )
        .unwrap();
        assert_eq!(result.messages[0].timestamp, Some(999_999));
        assert_eq!(result.messages[0].origin_timestamp, Some(999_999));
    }

    #[test]
    fn process_rows_row_without_cursor_field_still_produces_message() {
        let rows = vec![row(&[("_value", "42")])]; // no _time field
        let result = process_rows(
            &rows,
            BASE_CURSOR,
            0,
            "_time",
            true,
            None,
            PayloadFormat::Json,
            1000,
            0,
        )
        .unwrap();
        assert_eq!(result.messages.len(), 1);
        assert!(result.max_cursor.is_none());
    }
}

// ── Query execution ───────────────────────────────────────────────────────────

pub(crate) async fn run_query(
    client: &ClientWithMiddleware,
    config: &V2SourceConfig,
    auth: &str,
    cursor: &str,
    already_seen: u64,
) -> Result<String, Error> {
    let query = render_query(config, cursor, already_seen)?;
    let base = config.url.trim_end_matches('/');
    let adp = adapter();
    let (url, body) = adp.build_query(base, &query, "", Some(&config.org))?;

    let response = client
        .post(url)
        .header("Authorization", auth)
        .header("Content-Type", adp.query_content_type())
        .header("Accept", adp.query_accept_header())
        .json(&body)
        .send()
        .await
        .map_err(|e| Error::Storage(format!("InfluxDB V2 query failed: {e}")))?;

    let status = response.status();
    if status.is_success() {
        return response
            .text()
            .await
            .map_err(|e| Error::Storage(format!("Failed to read V2 response: {e}")));
    }

    let body_text = response
        .text()
        .await
        .unwrap_or_else(|_| "failed to read response body".to_string());

    if iggy_connector_sdk::retry::is_transient_status(status) {
        Err(Error::Storage(format!(
            "InfluxDB V2 query failed with status {status}: {body_text}"
        )))
    } else {
        Err(Error::PermanentHttpError(format!(
            "InfluxDB V2 query failed with status {status}: {body_text}"
        )))
    }
}

// ── Message building ──────────────────────────────────────────────────────────

fn build_payload(
    row: &Row,
    payload_column: Option<&str>,
    payload_format: PayloadFormat,
    include_metadata: bool,
) -> Result<Vec<u8>, Error> {
    if let Some(col) = payload_column {
        let raw = row
            .get(col)
            .cloned()
            .ok_or_else(|| Error::InvalidRecordValue(format!("Missing payload column '{col}'")))?;
        return match payload_format {
            PayloadFormat::Json => {
                let v: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
                    Error::InvalidRecordValue(format!(
                        "Payload column '{col}' is not valid JSON: {e}"
                    ))
                })?;
                serde_json::to_vec(&v)
                    .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}")))
            }
            PayloadFormat::Text => Ok(raw.into_bytes()),
            PayloadFormat::Raw => general_purpose::STANDARD
                .decode(raw.as_bytes())
                .map_err(|e| {
                    Error::InvalidRecordValue(format!("Failed to decode payload as base64: {e}"))
                }),
        };
    }

    // Single pass over the row: extract envelope fields and build json_row
    // simultaneously, avoiding the second HashMap lookups that the two-pass
    // approach required.
    // parse_scalar is called only when the result will actually be used —
    // skipping it for metadata fields when include_metadata=false avoids
    // three failed parse attempts (bool, i64, f64) per discarded field.
    let mut json_row = serde_json::Map::new();
    let mut measurement: &str = "";
    let mut field_name: &str = "";
    let mut timestamp_str: &str = "";
    let mut field_value = serde_json::Value::Null;

    for (key, val_str) in row {
        match key.as_str() {
            "_measurement" => {
                measurement = val_str;
                if include_metadata {
                    json_row.insert(key.clone(), parse_scalar(val_str));
                }
            }
            "_field" => {
                field_name = val_str;
                if include_metadata {
                    json_row.insert(key.clone(), parse_scalar(val_str));
                }
            }
            "_time" => {
                timestamp_str = val_str;
                // _time always included (needed for cursor tracking by consumers)
                json_row.insert(key.clone(), parse_scalar(val_str));
            }
            "_value" => {
                let parsed = parse_scalar(val_str);
                field_value = parsed.clone();
                json_row.insert(key.clone(), parsed);
            }
            _ => {
                if include_metadata {
                    json_row.insert(key.clone(), parse_scalar(val_str));
                }
            }
        }
    }

    let wrapped = json!({
        "measurement": measurement,
        "field":       field_name,
        "timestamp":   timestamp_str,
        "value":       field_value,
        "row":         json_row,
    });

    serde_json::to_vec(&wrapped)
        .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}")))
}

pub(crate) struct PollResult {
    pub messages: Vec<ProducedMessage>,
    pub max_cursor: Option<String>,
    pub rows_at_max_cursor: u64,
    pub skipped: u64,
    pub schema: Schema,
}

// ── Row processing (pure, testable without HTTP) ──────────────────────────────

/// Result of processing a batch of V2 rows into Iggy messages.
pub(crate) struct RowProcessingResult {
    pub messages: Vec<ProducedMessage>,
    pub max_cursor: Option<String>,
    pub rows_at_max_cursor: u64,
    pub skipped: u64,
}

/// Convert a slice of V2 query rows into Iggy messages.
///
/// Skips the first `already_seen` rows whose `cursor_field` value equals
/// `cursor` — these were delivered in the previous batch and re-appear
/// because V2's `>= $cursor` query semantics are inclusive. All other rows
/// become messages with UUIDs derived from `uuid_base` (sequential, no PRNG
/// per message) and timestamps set to `now_micros`.
#[allow(clippy::too_many_arguments)] // Each parameter controls a distinct axis of behaviour.
pub(crate) fn process_rows(
    rows: &[Row],
    cursor: &str,
    already_seen: u64,
    cursor_field: &str,
    include_metadata: bool,
    payload_col: Option<&str>,
    payload_format: PayloadFormat,
    now_micros: u64,
    uuid_base: u128,
) -> Result<RowProcessingResult, Error> {
    let mut messages = Vec::with_capacity(rows.len());
    let mut max_cursor: Option<String> = None;
    let mut rows_at_max_cursor = 0u64;
    let mut skipped = 0u64;

    for (i, row) in rows.iter().enumerate() {
        // Single lookup for cursor_field — used for both skip logic and max-cursor tracking.
        let cv = row.get(cursor_field);
        if let Some(cv) = cv
            && cv == cursor
            && skipped < already_seen
        {
            skipped += 1;
            continue;
        }

        if let Some(cv) = cv {
            match &max_cursor {
                None => {
                    max_cursor = Some(cv.clone());
                    rows_at_max_cursor = 1;
                }
                Some(current) => {
                    if is_timestamp_after(cv, current) {
                        max_cursor = Some(cv.clone());
                        rows_at_max_cursor = 1;
                    } else if cv == current {
                        rows_at_max_cursor += 1;
                    }
                }
            }
        }

        let payload = build_payload(row, payload_col, payload_format, include_metadata)?;
        messages.push(ProducedMessage {
            // Unique per message within the batch without repeated PRNG calls.
            id: Some(uuid_base.wrapping_add(i as u128)),
            checksum: None,
            timestamp: Some(now_micros),
            origin_timestamp: Some(now_micros),
            headers: None,
            payload,
        });
    }

    Ok(RowProcessingResult {
        messages,
        max_cursor,
        rows_at_max_cursor,
        skipped,
    })
}

pub(crate) async fn poll(
    client: &ClientWithMiddleware,
    config: &V2SourceConfig,
    auth: &str,
    state: &V2State,
    payload_format: PayloadFormat,
) -> Result<PollResult, Error> {
    let cursor = state
        .last_timestamp
        .clone()
        .or_else(|| config.initial_offset.clone())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());

    let already_seen = state.cursor_row_count;
    let response_data = run_query(client, config, auth, &cursor, already_seen).await?;
    let rows = parse_csv_rows(&response_data)?;

    let cursor_field = config.cursor_field.as_deref().unwrap_or("_time");
    let include_metadata = config.include_metadata.unwrap_or(true);
    let payload_col = config.payload_column.as_deref();

    // Captured once per poll to avoid a syscall and PRNG invocation per message.
    let now_micros = iggy_common::Utc::now().timestamp_micros() as u64;
    let uuid_base = Uuid::new_v4().as_u128();

    let result = process_rows(
        &rows,
        &cursor,
        already_seen,
        cursor_field,
        include_metadata,
        payload_col,
        payload_format,
        now_micros,
        uuid_base,
    )?;

    let schema = if payload_col.is_some() {
        payload_format.schema()
    } else {
        Schema::Json
    };

    Ok(PollResult {
        messages: result.messages,
        max_cursor: result.max_cursor,
        rows_at_max_cursor: result.rows_at_max_cursor,
        skipped: result.skipped,
        schema,
    })
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use axum::Router;
    use axum::extract::Request;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use secrecy::SecretString;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    // ── helpers ───────────────────────────────────────────────────────────────

    async fn start_server(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://127.0.0.1:{port}")
    }

    fn make_client() -> ClientWithMiddleware {
        let raw = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        iggy_connector_sdk::retry::build_retry_client(
            raw,
            1,
            Duration::from_millis(1),
            Duration::from_millis(10),
            "test",
        )
    }

    fn make_config(url: &str) -> V2SourceConfig {
        V2SourceConfig {
            url: url.to_string(),
            org: "test_org".to_string(),
            token: SecretString::from("test_token"),
            query: "SELECT * FROM t WHERE time >= '$cursor' LIMIT $limit".to_string(),
            poll_interval: None,
            batch_size: Some(10),
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: Some("1ms".to_string()),
            timeout: Some("5s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("10ms".to_string()),
            retry_max_delay: Some("10ms".to_string()),
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        }
    }

    const CURSOR: &str = "1970-01-01T00:00:00Z";

    // ── run_query ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn run_query_returns_body_on_200() {
        let csv = "_time,_value\n2024-01-01T00:00:00Z,42\n";
        let app = Router::new().route(
            "/api/v2/query",
            post(move || async move { (StatusCode::OK, csv) }),
        );
        let base = start_server(app).await;
        let result = run_query(&make_client(), &make_config(&base), "Token tok", CURSOR, 0)
            .await
            .unwrap();
        assert!(result.contains("_value"));
        assert!(result.contains("42"));
    }

    #[tokio::test]
    async fn run_query_empty_body_on_200() {
        let app = Router::new().route("/api/v2/query", post(|| async { (StatusCode::OK, "") }));
        let base = start_server(app).await;
        let result = run_query(&make_client(), &make_config(&base), "Token tok", CURSOR, 0)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn run_query_500_returns_transient_error() {
        let app = Router::new().route(
            "/api/v2/query",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let base = start_server(app).await;
        let result = run_query(&make_client(), &make_config(&base), "Token tok", CURSOR, 0).await;
        assert!(matches!(result, Err(Error::Storage(_))));
    }

    #[tokio::test]
    async fn run_query_400_returns_permanent_error() {
        let app = Router::new().route("/api/v2/query", post(|| async { StatusCode::BAD_REQUEST }));
        let base = start_server(app).await;
        let result = run_query(&make_client(), &make_config(&base), "Token tok", CURSOR, 0).await;
        assert!(matches!(result, Err(Error::PermanentHttpError(_))));
    }

    #[tokio::test]
    async fn run_query_sends_token_authorization_header() {
        let captured: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured.clone();
        let app = Router::new().route(
            "/api/v2/query",
            post(move |headers: HeaderMap| {
                let cap = cap2.clone();
                async move {
                    *cap.lock().await = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    StatusCode::OK
                }
            }),
        );
        let base = start_server(app).await;
        let _ = run_query(
            &make_client(),
            &make_config(&base),
            "Token my_token",
            CURSOR,
            0,
        )
        .await;
        assert_eq!(*captured.lock().await, "Token my_token");
    }

    #[tokio::test]
    async fn run_query_sends_org_in_query_params() {
        let captured_uri: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured_uri.clone();
        let app = Router::new().route(
            "/api/v2/query",
            post(move |request: Request| {
                let cap = cap2.clone();
                async move {
                    *cap.lock().await = request.uri().to_string();
                    StatusCode::OK
                }
            }),
        );
        let base = start_server(app).await;
        let _ = run_query(&make_client(), &make_config(&base), "Token tok", CURSOR, 0).await;
        assert!(captured_uri.lock().await.contains("org=test_org"));
    }

    #[tokio::test]
    async fn run_query_request_body_contains_substituted_query() {
        let captured_body: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured_body.clone();
        let app = Router::new().route(
            "/api/v2/query",
            post(move |request: Request| {
                let cap = cap2.clone();
                async move {
                    let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
                        .await
                        .unwrap();
                    *cap.lock().await = String::from_utf8_lossy(&bytes).to_string();
                    StatusCode::OK
                }
            }),
        );
        let base = start_server(app).await;
        let cursor = "2024-01-01T00:00:00Z";
        let _ = run_query(&make_client(), &make_config(&base), "Token tok", cursor, 0).await;
        let body = captured_body.lock().await;
        // The $cursor placeholder should be replaced with the cursor value
        assert!(body.contains(cursor));
        // $limit should be replaced with the batch size (10)
        assert!(body.contains("10"));
        // The raw placeholders must NOT appear in the sent query
        assert!(!body.contains("$cursor"));
        assert!(!body.contains("$limit"));
    }

    // ── poll() end-to-end ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn poll_returns_messages_for_csv_response() {
        let csv = "_time,_value\n\
                   2024-01-01T00:00:01Z,42\n\
                   2024-01-01T00:00:02Z,43\n";
        let app = Router::new().route(
            "/api/v2/query",
            post(move || async move { (StatusCode::OK, csv) }),
        );
        let base = start_server(app).await;
        let state = V2State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Token tok",
            &state,
            PayloadFormat::Json,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 2);
        assert_eq!(result.max_cursor.as_deref(), Some("2024-01-01T00:00:02Z"));
        assert_eq!(result.rows_at_max_cursor, 1);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.schema, Schema::Json);
    }

    #[tokio::test]
    async fn poll_advances_cursor_to_latest_out_of_order_timestamp() {
        // Rows arrive in non-chronological order; max_cursor must still be the latest.
        let csv = "_time,_value\n\
                   2024-01-01T00:00:01Z,10\n\
                   2024-01-01T00:00:03Z,30\n\
                   2024-01-01T00:00:02Z,20\n";
        let app = Router::new().route(
            "/api/v2/query",
            post(move || async move { (StatusCode::OK, csv) }),
        );
        let base = start_server(app).await;
        let state = V2State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Token tok",
            &state,
            PayloadFormat::Json,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 3);
        assert_eq!(result.max_cursor.as_deref(), Some("2024-01-01T00:00:03Z"));
    }

    #[tokio::test]
    async fn poll_skips_already_seen_rows_at_cursor() {
        // State says we already delivered 1 row at T1.
        // Server returns 3 rows all at T1 → first must be skipped.
        let t1 = "2024-01-01T00:00:01Z";
        let csv = format!("_time,_value\n{t1},1\n{t1},2\n{t1},3\n");
        let app = Router::new().route(
            "/api/v2/query",
            post(move || async move { (StatusCode::OK, csv) }),
        );
        let base = start_server(app).await;
        let state = V2State {
            last_timestamp: Some(t1.to_string()),
            cursor_row_count: 1,
            processed_rows: 5,
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Token tok",
            &state,
            PayloadFormat::Json,
        )
        .await
        .unwrap();
        assert_eq!(result.skipped, 1);
        assert_eq!(result.messages.len(), 2);
        assert_eq!(result.rows_at_max_cursor, 2);
    }

    #[tokio::test]
    async fn poll_empty_csv_returns_no_messages() {
        let app = Router::new().route("/api/v2/query", post(|| async { (StatusCode::OK, "") }));
        let base = start_server(app).await;
        let state = V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            ..V2State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Token tok",
            &state,
            PayloadFormat::Json,
        )
        .await
        .unwrap();
        assert!(result.messages.is_empty());
        assert!(result.max_cursor.is_none());
        assert_eq!(result.skipped, 0);
    }

    #[tokio::test]
    async fn poll_propagates_http_error() {
        let app = Router::new().route(
            "/api/v2/query",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let base = start_server(app).await;
        let state = V2State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Token tok",
            &state,
            PayloadFormat::Json,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn poll_counts_rows_at_same_max_cursor() {
        // Two rows share the latest timestamp; rows_at_max_cursor must be 2.
        let t1 = "2024-01-01T00:00:01Z";
        let t2 = "2024-01-01T00:00:02Z";
        let csv = format!("_time,_value\n{t1},1\n{t2},2\n{t2},3\n");
        let app = Router::new().route(
            "/api/v2/query",
            post(move || async move { (StatusCode::OK, csv) }),
        );
        let base = start_server(app).await;
        let state = V2State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Token tok",
            &state,
            PayloadFormat::Json,
        )
        .await
        .unwrap();
        assert_eq!(result.max_cursor.as_deref(), Some(t2));
        assert_eq!(result.rows_at_max_cursor, 2);
        assert_eq!(result.messages.len(), 3);
    }

    #[tokio::test]
    async fn poll_schema_matches_payload_format() {
        // When a payload_column is configured the schema should reflect
        // the format (Text here), not always Json.
        let csv = "_time,data\n2024-01-01T00:00:01Z,hello\n";
        let app = Router::new().route(
            "/api/v2/query",
            post(move || async move { (StatusCode::OK, csv) }),
        );
        let base = start_server(app).await;
        let config = V2SourceConfig {
            payload_column: Some("data".to_string()),
            ..make_config(&base)
        };
        let state = V2State::default();
        let result = poll(
            &make_client(),
            &config,
            "Token tok",
            &state,
            PayloadFormat::Text,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 1);
        assert_eq!(result.schema, Schema::Text);
        // The raw text should be the payload bytes
        assert_eq!(result.messages[0].payload, b"hello");
    }

    #[tokio::test]
    async fn poll_permanent_http_error_propagates() {
        let app = Router::new().route("/api/v2/query", post(|| async { StatusCode::BAD_REQUEST }));
        let base = start_server(app).await;
        let state = V2State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Token tok",
            &state,
            PayloadFormat::Json,
        )
        .await;
        assert!(matches!(result, Err(Error::PermanentHttpError(_))));
    }
}
