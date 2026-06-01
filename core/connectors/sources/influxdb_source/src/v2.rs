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
    PayloadFormat, Row, RowContext, V2SourceConfig, V2State, apply_query_params, parse_csv_rows,
    parse_scalar, validate_cursor,
};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use iggy_connector_sdk::{Error, ProducedMessage, Schema};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde_json::json;
use uuid::Uuid;

fn build_query(
    base: &str,
    query: &str,
    org: Option<&str>,
) -> Result<(Url, serde_json::Value), Error> {
    let mut url = Url::parse(&format!("{base}/api/v2/query"))
        .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;
    if let Some(o) = org {
        url.query_pairs_mut().append_pair("org", o);
    }
    let body = json!({
        "query": query,
        "dialect": {
            "annotations": ["datatype", "group", "default"],
            "delimiter": ",",
            "header": true,
            "commentPrefix": "#"
        }
    });
    Ok((url, body))
}

/// Maximum multiple of `batch_size` by which `already_seen` may inflate the
/// query limit. Prevents an unbounded request to InfluxDB when the cursor
/// is stuck at the same timestamp for many consecutive polls (analogous to
/// V3's `stuck_batch_cap_factor`).
const MAX_SKIP_INFLATION_FACTOR: u64 = 10;
const MAX_RESPONSE_BODY_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

/// Render the final Flux query by substituting `$cursor` and `$limit`.
///
/// The limit is inflated by `already_seen` (rows at the current cursor
/// timestamp that were delivered in a previous batch) so that re-fetching
/// with `>= cursor` returns enough rows to skip them and still fill a full
/// batch. Inflation is capped at `MAX_SKIP_INFLATION_FACTOR × batch_size`
/// to prevent excessively large queries when the cursor is stuck.
fn render_query(config: &V2SourceConfig, cursor: &str, already_seen: u64) -> Result<String, Error> {
    validate_cursor(cursor)?;
    let batch = config.batch_size.unwrap_or(500).max(1) as u64;
    // Cap inflation so a stuck cursor cannot issue arbitrarily large queries.
    let capped_seen = already_seen.min(batch.saturating_mul(MAX_SKIP_INFLATION_FACTOR));
    let limit = batch.saturating_add(capped_seen).to_string();
    Ok(apply_query_params(&config.query, cursor, &limit, ""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Row, RowContext};
    use std::sync::Arc;

    fn row(pairs: &[(&str, &str)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    Arc::<str>::from(*k),
                    serde_json::Value::String(v.to_string()),
                )
            })
            .collect()
    }

    const BASE_CURSOR: &str = "1970-01-01T00:00:00Z";
    const T1: &str = "2024-01-01T00:00:00Z";
    const T2: &str = "2024-01-01T00:00:01Z";
    const T3: &str = "2024-01-01T00:00:02Z";

    fn ctx(current_cursor: &str, now_micros: u64) -> RowContext<'_> {
        RowContext {
            cursor_field: "_time",
            current_cursor,
            include_metadata: true,
            payload_col: None,
            payload_format: PayloadFormat::Json,
            now_micros,
        }
    }

    #[test]
    fn process_rows_empty_returns_empty() {
        let result = process_rows(&[], &ctx(BASE_CURSOR, 1000), 0).unwrap();
        assert!(result.messages.is_empty());
        assert!(result.max_cursor.is_none());
        assert_eq!(result.skipped, 0);
        assert_eq!(result.rows_at_max_cursor, 0);
    }

    #[test]
    fn process_rows_single_row_produces_one_message() {
        let rows = vec![row(&[("_time", T1), ("_value", "42")])];
        let result = process_rows(&rows, &ctx(BASE_CURSOR, 1000), 0).unwrap();
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
        let result = process_rows(&rows, &ctx(T1, 1000), 1).unwrap();
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
        let result = process_rows(&rows, &ctx(T1, 1000), 1).unwrap();
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
        let result = process_rows(&rows, &ctx(BASE_CURSOR, 1000), 0).unwrap();
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
        let result = process_rows(&rows, &ctx(BASE_CURSOR, 1000), 0).unwrap();
        assert_eq!(result.max_cursor.as_deref(), Some(T2));
        assert_eq!(result.rows_at_max_cursor, 2);
    }

    #[test]
    fn process_rows_message_ids_are_some_and_unique() {
        let rows = vec![row(&[("_time", T1)]), row(&[("_time", T2)])];
        let result = process_rows(&rows, &ctx(BASE_CURSOR, 1000), 0).unwrap();
        assert!(result.messages[0].id.is_some());
        assert!(result.messages[1].id.is_some());
        assert_ne!(result.messages[0].id, result.messages[1].id);
    }

    #[test]
    fn process_rows_message_timestamps_use_now_micros() {
        let rows = vec![row(&[("_time", T1)])];
        let result = process_rows(&rows, &ctx(BASE_CURSOR, 999_999), 0).unwrap();
        assert_eq!(result.messages[0].timestamp, Some(999_999));
        assert_eq!(result.messages[0].origin_timestamp, Some(999_999));
    }

    #[test]
    fn process_rows_row_without_cursor_field_still_produces_message() {
        let rows = vec![row(&[("_value", "42")])]; // no _time field
        let result = process_rows(&rows, &ctx(BASE_CURSOR, 1000), 0).unwrap();
        assert_eq!(result.messages.len(), 1);
        assert!(result.max_cursor.is_none());
    }

    #[test]
    fn process_rows_message_ids_stable_across_repoll() {
        // IDs must be deterministic: same rows must produce the same IDs on re-poll.
        let rows = vec![
            row(&[("_time", T1), ("_value", "10")]),
            row(&[("_time", T2), ("_value", "20")]),
        ];
        let c = ctx(BASE_CURSOR, 0);
        let first = process_rows(&rows, &c, 0).unwrap();
        let second = process_rows(&rows, &c, 0).unwrap();
        assert_eq!(
            first.messages[0].id, second.messages[0].id,
            "row at T1 must have the same ID on re-poll"
        );
        assert_eq!(
            first.messages[1].id, second.messages[1].id,
            "row at T2 must have the same ID on re-poll"
        );
    }

    #[test]
    fn process_rows_rows_at_same_timestamp_get_distinct_stable_ids() {
        // Two rows sharing a cursor timestamp must get different IDs (position-disambiguated).
        let rows = vec![
            row(&[("_time", T1), ("_value", "a")]),
            row(&[("_time", T1), ("_value", "b")]),
        ];
        let c = ctx(BASE_CURSOR, 0);
        let result = process_rows(&rows, &c, 0).unwrap();
        assert_ne!(
            result.messages[0].id, result.messages[1].id,
            "two rows at the same timestamp must have distinct IDs"
        );
        // Stability: IDs unchanged on re-poll.
        let result2 = process_rows(&rows, &c, 0).unwrap();
        assert_eq!(result.messages[0].id, result2.messages[0].id);
        assert_eq!(result.messages[1].id, result2.messages[1].id);
    }

    // ── build_payload with payload_column ─────────────────────────────────────

    #[test]
    fn process_rows_payload_column_json_format_parses_and_reserializes() {
        // payload_column + Json format: the CSV string value is parsed as JSON,
        // normalized (compact), and written as the message bytes.
        let rows = vec![row(&[
            ("_time", T1),
            ("data", r#"{"sensor":"temp","v":42}"#),
        ])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "_time",
                current_cursor: BASE_CURSOR,
                include_metadata: true,
                payload_col: Some("data"),
                payload_format: PayloadFormat::Json,
                now_micros: 1000,
            },
            0,
        )
        .unwrap();
        assert_eq!(result.messages.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&result.messages[0].payload).unwrap();
        assert_eq!(body["sensor"], "temp");
        assert_eq!(body["v"], 42);
    }

    #[test]
    fn process_rows_payload_column_json_invalid_returns_error() {
        // payload_column + Json format: non-JSON string must return Err.
        let rows = vec![row(&[("_time", T1), ("data", "not-json")])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "_time",
                current_cursor: BASE_CURSOR,
                include_metadata: true,
                payload_col: Some("data"),
                payload_format: PayloadFormat::Json,
                now_micros: 1000,
            },
            0,
        );
        assert!(
            result.is_err(),
            "invalid JSON payload column must return Err"
        );
    }

    #[test]
    fn process_rows_payload_column_raw_decodes_base64() {
        // payload_column + Raw format: base64-encoded CSV value is decoded to bytes.
        use base64::{Engine as _, engine::general_purpose};
        let encoded = general_purpose::STANDARD.encode(b"binary\x00data");
        let rows = vec![row(&[("_time", T1), ("blob", &encoded)])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "_time",
                current_cursor: BASE_CURSOR,
                include_metadata: true,
                payload_col: Some("blob"),
                payload_format: PayloadFormat::Raw,
                now_micros: 1000,
            },
            0,
        )
        .unwrap();
        assert_eq!(result.messages[0].payload, b"binary\x00data");
    }

    #[test]
    fn process_rows_payload_column_raw_invalid_base64_returns_error() {
        let rows = vec![row(&[("_time", T1), ("blob", "!!!not-base64!!!")])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "_time",
                current_cursor: BASE_CURSOR,
                include_metadata: true,
                payload_col: Some("blob"),
                payload_format: PayloadFormat::Raw,
                now_micros: 1000,
            },
            0,
        );
        assert!(
            result.is_err(),
            "invalid base64 payload column must return Err"
        );
    }

    #[test]
    fn process_rows_measurement_and_field_columns_included_in_metadata() {
        // When include_metadata=true, _measurement and _field must appear in
        // the wrapped JSON payload (confirming the uncovered branches at v2.rs:361-370).
        let rows = vec![row(&[
            ("_time", T1),
            ("_measurement", "cpu"),
            ("_field", "usage"),
            ("_value", "55"),
        ])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "_time",
                current_cursor: BASE_CURSOR,
                include_metadata: true,
                payload_col: None,
                payload_format: PayloadFormat::Json,
                now_micros: 1000,
            },
            0,
        )
        .unwrap();
        assert_eq!(result.messages.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&result.messages[0].payload).unwrap();
        assert_eq!(body["measurement"], "cpu");
        assert_eq!(body["field"], "usage");
        // Row sub-object must contain the raw columns when include_metadata=true.
        assert!(body["row"]["_measurement"].is_string());
        assert!(body["row"]["_field"].is_string());
    }

    #[test]
    fn process_rows_measurement_and_field_excluded_when_no_metadata() {
        // include_metadata=false: _measurement and _field must NOT appear in row.
        let rows = vec![row(&[
            ("_time", T1),
            ("_measurement", "cpu"),
            ("_field", "usage"),
            ("_value", "55"),
        ])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "_time",
                current_cursor: BASE_CURSOR,
                include_metadata: false,
                payload_col: None,
                payload_format: PayloadFormat::Json,
                now_micros: 1000,
            },
            0,
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&result.messages[0].payload).unwrap();
        // row sub-object must not contain _measurement or _field when metadata excluded
        assert!(
            body["row"].get("_measurement").is_none(),
            "_measurement must be excluded when include_metadata=false"
        );
    }

    // ── render_query ──────────────────────────────────────────────────────────

    fn minimal_config(batch_size: Option<u32>, query: &str) -> V2SourceConfig {
        use secrecy::SecretString;
        V2SourceConfig {
            url: "http://localhost:8086".to_string(),
            org: "o".to_string(),
            token: SecretString::from("t"),
            query: query.to_string(),
            batch_size,
            poll_interval: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: None,
            retry_delay: None,
            timeout: None,
            max_open_retries: None,
            open_retry_max_delay: None,
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        }
    }

    /// Bug: batch_size=0 causes render_query to produce LIMIT 0, silently stalling
    /// the connector. The limit must be floored to 1.
    #[test]
    fn render_query_zero_batch_size_floors_to_limit_one() {
        let cfg = minimal_config(Some(0), "SELECT * FROM t LIMIT $limit");
        let q = render_query(&cfg, "1970-01-01T00:00:00Z", 0).unwrap();
        assert!(
            q.contains("LIMIT 1"),
            "batch_size=0 must floor to LIMIT 1, got: {q}"
        );
    }

    #[test]
    fn render_query_normal_batch_size_preserved() {
        let cfg = minimal_config(Some(42), "SELECT * FROM t LIMIT $limit");
        let q = render_query(&cfg, "1970-01-01T00:00:00Z", 0).unwrap();
        assert!(
            q.contains("LIMIT 42"),
            "batch_size=42 must produce LIMIT 42: {q}"
        );
    }

    #[test]
    fn process_rows_missing_payload_column_returns_error() {
        // If the specified payload_column is absent from the row, return Err.
        let rows = vec![row(&[("_time", T1), ("other", "value")])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "_time",
                current_cursor: BASE_CURSOR,
                include_metadata: true,
                payload_col: Some("missing_col"),
                payload_format: PayloadFormat::Json,
                now_micros: 1000,
            },
            0,
        );
        assert!(result.is_err(), "missing payload column must return Err");
    }

    #[test]
    fn process_rows_skip_normalizes_rfc3339_formats() {
        // Cursor stored as "Z" suffix; rows return "+00:00" offset — same instant.
        // String equality misses the skip, causing re-delivery. Datetime comparison fixes it.
        let cursor_z = "2024-01-01T00:00:00Z";
        let rows = vec![
            row(&[("_time", "2024-01-01T00:00:00+00:00"), ("val", "1")]),
            row(&[("_time", "2024-01-01T00:00:00+00:00"), ("val", "2")]),
            row(&[("_time", "2024-01-01T00:00:01Z"), ("val", "3")]),
        ];
        // already_seen=1: first row at cursor must be skipped
        let result = process_rows(&rows, &ctx(cursor_z, 0), 1).unwrap();
        assert_eq!(
            result.skipped, 1,
            "+00:00 and Z represent the same instant — skip must fire"
        );
        assert_eq!(result.messages.len(), 2);
    }

    #[test]
    fn process_rows_ids_are_globally_positioned() {
        // With already_seen=N, the first post-skip row is at global position N.
        // Using batch-relative messages.len() gives position 0 — collision with the
        // same row from a prior batch where it had already_seen=0 and position 0.
        // Fix: use already_seen + messages.len() so each row's ID encodes its global slot.
        let rows = vec![
            row(&[("_time", T1), ("val", "a")]), // global pos 0 — will be skipped
            row(&[("_time", T1), ("val", "b")]), // global pos 1 — first emitted
            row(&[("_time", T1), ("val", "c")]), // global pos 2
        ];
        // Baseline: already_seen=0, all three emitted
        let base = process_rows(&rows, &ctx(T1, 0), 0).unwrap();
        // With skip: already_seen=1, row 0 skipped, rows 1 and 2 emitted
        let skipped = process_rows(&rows, &ctx(T1, 0), 1).unwrap();
        assert_eq!(skipped.skipped, 1);
        assert_eq!(skipped.messages.len(), 2);
        // The first emitted row with skip must have the SAME id as row[1] without skip.
        assert_eq!(
            skipped.messages[0].id, base.messages[1].id,
            "first post-skip row must have same id as it had in the unskipped baseline"
        );
        assert_eq!(
            skipped.messages[1].id, base.messages[2].id,
            "second post-skip row must have same id as it had in the unskipped baseline"
        );
    }

    #[test]
    fn process_rows_rows_at_max_cursor_normalized_across_rfc3339_formats() {
        // Both rows are the same instant ("Z" vs "+00:00"). String equality counts only
        // the one that string-matches max_cursor, under-counting ties at max timestamp.
        // Under-counting causes cursor_row_count to be too low → skip-N under-fires on
        // the next poll → re-delivery.
        let rows = vec![
            row(&[("_time", "2024-01-01T00:00:00Z"), ("val", "1")]),
            row(&[("_time", "2024-01-01T00:00:00+00:00"), ("val", "2")]),
        ];
        let result = process_rows(&rows, &ctx(BASE_CURSOR, 0), 0).unwrap();
        assert_eq!(
            result.rows_at_max_cursor, 2,
            "Z and +00:00 are the same instant — both must count toward rows_at_max_cursor"
        );
        assert_eq!(result.messages.len(), 2);
    }

    // ── NEW regression tests: prevent poisoned cursor persistence ─────────────

    #[test]
    fn process_rows_unparsable_cursor_does_not_become_max_cursor_when_later_row_is_valid() {
        let rows = vec![
            row(&[("_time", "not-a-timestamp"), ("_value", "1")]),
            row(&[("_time", "2024-01-01T00:00:00Z"), ("_value", "2")]),
        ];
        let result = process_rows(&rows, &ctx(BASE_CURSOR, 0), 0).unwrap();
        assert_eq!(result.max_cursor.as_deref(), Some("2024-01-01T00:00:00Z"));
        assert_eq!(result.rows_at_max_cursor, 1);
        assert_eq!(result.messages.len(), 2);
    }

    #[test]
    fn process_rows_all_unparsable_cursor_values_yield_no_max_cursor() {
        let rows = vec![
            row(&[("_time", "bad1"), ("_value", "1")]),
            row(&[("_time", "bad2"), ("_value", "2")]),
        ];
        let result = process_rows(&rows, &ctx(BASE_CURSOR, 0), 0).unwrap();
        assert!(result.max_cursor.is_none());
        assert_eq!(result.rows_at_max_cursor, 0);
        assert_eq!(result.messages.len(), 2);
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
    let (url, body) = build_query(base, &query, Some(&config.org))?;

    let mut response = client
        .post(url)
        .header("Authorization", auth)
        .header("Content-Type", "application/json")
        .header("Accept", "text/csv")
        .json(&body)
        .send()
        .await
        .map_err(|e| Error::Storage(format!("InfluxDB V2 query failed: {e}")))?;

    let status = response.status();
    if status.is_success() {
        // Stream chunk-by-chunk with a hard byte cap to mirror the V3 path and
        // prevent OOM when MAX_SKIP_INFLATION_FACTOR inflates the effective batch.
        if response
            .content_length()
            .is_some_and(|n| n as usize > MAX_RESPONSE_BODY_BYTES)
        {
            return Err(Error::Storage(format!(
                "InfluxDB V2 response body exceeds {MAX_RESPONSE_BODY_BYTES} byte cap; \
                 reduce batch_size to avoid OOM"
            )));
        }
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| Error::Storage(format!("Failed to read V2 response: {e}")))?
        {
            buf.extend_from_slice(&chunk);
            if buf.len() > MAX_RESPONSE_BODY_BYTES {
                return Err(Error::Storage(format!(
                    "InfluxDB V2 response body exceeded {MAX_RESPONSE_BODY_BYTES} byte cap \
                     while streaming; reduce batch_size to avoid OOM"
                )));
            }
        }
        return String::from_utf8(buf)
            .map_err(|e| Error::Storage(format!("V2 response body is not valid UTF-8: {e}")));
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
        // V2 CSV values are always Value::String; extract once and reuse.
        let raw = row
            .get(col)
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::InvalidRecordValue(format!("Missing payload column '{col}'")))?;
        return match payload_format {
            PayloadFormat::Json => {
                let v: serde_json::Value = serde_json::from_str(raw).map_err(|e| {
                    Error::InvalidRecordValue(format!(
                        "Payload column '{col}' is not valid JSON: {e}"
                    ))
                })?;
                serde_json::to_vec(&v)
                    .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}")))
            }
            PayloadFormat::Text => Ok(raw.as_bytes().to_vec()),
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

    // V2 CSV values arrive as Value::String; extract the &str once and call
    // parse_scalar to infer bool / i64 / f64 / string type from the raw text.
    for (key, val) in row {
        let val_str = val.as_str().unwrap_or("");
        match key.as_ref() {
            "_measurement" => {
                measurement = val_str;
                if include_metadata {
                    json_row.insert(key.to_string(), parse_scalar(val_str));
                }
            }
            "_field" => {
                field_name = val_str;
                if include_metadata {
                    json_row.insert(key.to_string(), parse_scalar(val_str));
                }
            }
            "_time" => {
                timestamp_str = val_str;
                // _time always included (needed for cursor tracking by consumers)
                json_row.insert(key.to_string(), parse_scalar(val_str));
            }
            "_value" => {
                let parsed = parse_scalar(val_str);
                field_value = parsed.clone();
                json_row.insert(key.to_string(), parsed);
            }
            _ => {
                if include_metadata {
                    json_row.insert(key.to_string(), parse_scalar(val_str));
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

#[derive(Debug)]
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

/// Converts a slice of V2 query rows into Iggy messages.
///
/// ## Cursor semantics and deduplication
///
/// InfluxDB V2 Flux queries use `>= $cursor` (inclusive), so the first batch after
/// a cursor advance will re-include any rows whose timestamp equals the new cursor.
/// `already_seen` is the count of such rows delivered in the previous batch; this
/// function skips exactly that many leading rows that match `ctx.current_cursor`,
/// preventing duplicate delivery across batch boundaries.
///
/// `already_seen` is a separate parameter rather than part of [`RowContext`] because
/// it is V2-specific: V3 uses strict `> cursor` and never needs to skip rows.
///
/// ## Cursor tracking
///
/// Each row's cursor field is compared as a timestamp. The highest timestamp seen
/// among emitted rows becomes `max_cursor` in the result. `rows_at_max_cursor`
/// counts how many emitted rows share that timestamp — the caller uses this to
/// detect when a batch is stuck (all rows share the same timestamp and fill the
/// entire batch), at which point the effective batch size is inflated.
///
/// Rows that are missing the cursor field still produce messages; they do not
/// contribute to cursor tracking and are excluded from skip logic.
///
/// ## Message identity
///
/// A single random UUID is generated per call; per-message IDs are derived by
/// adding the message's position to that base, keeping PRNG work O(1) per batch.
///
/// ## Parameters
///
/// - `rows`: Rows returned by the Flux query for this poll.
/// - `ctx`: Shared context (cursor field name, current cursor value, payload config,
///   wall-clock time in microseconds).
/// - `already_seen`: Number of rows at `ctx.current_cursor` to skip — rows already
///   delivered in the previous batch that the `>=` query re-included.
///
/// ## Returns
///
/// A [`RowProcessingResult`] containing:
/// - `messages`: One [`ProducedMessage`] per non-skipped row.
/// - `max_cursor`: Highest cursor timestamp seen among emitted rows, if any.
/// - `rows_at_max_cursor`: Count of emitted rows sharing `max_cursor`.
/// - `skipped`: Number of rows skipped due to `already_seen` deduplication.
pub(crate) fn process_rows(
    rows: &[Row],
    ctx: &RowContext<'_>,
    already_seen: u64,
) -> Result<RowProcessingResult, Error> {
    let mut messages = Vec::with_capacity(rows.len());
    let mut max_cursor: Option<String> = None;
    let mut max_cursor_parsed: Option<DateTime<Utc>> = None;
    let mut rows_at_max_cursor = 0u64;
    let mut skipped = 0u64;
    // Generate the base UUID once per poll; derive per-message IDs by addition.
    // This is O(1) PRNG calls per batch instead of O(n), measurable at batch ≥ 100.
    let id_base = Uuid::new_v4().as_u128();
    // Parse once outside the loop to normalise RFC 3339 variants (Z vs +00:00)
    // so skip logic compares instants, not strings.
    let current_cursor_dt: Option<DateTime<Utc>> = ctx.current_cursor.parse().ok();

    for row in rows.iter() {
        // Single lookup for cursor_field — used for both skip logic and max-cursor tracking.
        // V2 CSV rows store all values as Value::String; .as_str() is always Some.
        let cv = row.get(ctx.cursor_field).and_then(|v| v.as_str());
        // Parse once per row; DateTime<Utc> is Copy so reuse in skip, cursor, and ID logic.
        let cv_dt: Option<DateTime<Utc>> = cv.and_then(|s| s.parse().ok());
        if skipped < already_seen {
            let cursor_matches = match current_cursor_dt {
                Some(ref cc_dt) => cv_dt.as_ref() == Some(cc_dt),
                None => cv == Some(ctx.current_cursor),
            };
            if cursor_matches {
                skipped += 1;
                continue;
            }
        }

        // FIX: Only parsed cursor timestamps may influence max_cursor.
        // Otherwise, garbage strings can be persisted into state.last_timestamp and
        // permanently brick the connector until state is manually cleared.
        if let (Some(cv), Some(cv_dt)) = (cv, cv_dt) {
            match max_cursor_parsed {
                None => {
                    max_cursor = Some(cv.to_string());
                    max_cursor_parsed = Some(cv_dt);
                    rows_at_max_cursor = 1;
                }
                Some(current_dt) => {
                    if cv_dt > current_dt {
                        max_cursor = Some(cv.to_string());
                        max_cursor_parsed = Some(cv_dt);
                        rows_at_max_cursor = 1;
                    } else if cv_dt == current_dt {
                        rows_at_max_cursor += 1;
                    }
                }
            }
        }

        // Stable ID: cursor timestamp nanoseconds + DB-absolute row position.
        // Position = already_seen + emitted so far; encodes the row's global slot
        // in the result set so IDs are stable across re-polls with changing already_seen.
        let global_pos = already_seen as u128 + messages.len() as u128;
        let msg_id = cv_dt
            .and_then(|dt| dt.timestamp_nanos_opt())
            .map(|nanos| (nanos as u128).wrapping_add(global_pos))
            .unwrap_or_else(|| id_base.wrapping_add(global_pos));

        let payload = build_payload(
            row,
            ctx.payload_col,
            ctx.payload_format,
            ctx.include_metadata,
        )?;
        messages.push(ProducedMessage {
            id: Some(msg_id),
            checksum: None,
            timestamp: Some(ctx.now_micros),
            origin_timestamp: Some(ctx.now_micros),
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
    include_metadata: bool,
) -> Result<PollResult, Error> {
    let cursor = state
        .last_timestamp
        .clone()
        .or_else(|| config.initial_offset.clone())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());

    let already_seen = state.cursor_row_count;
    let response_data = run_query(client, config, auth, &cursor, already_seen).await?;
    let rows = parse_csv_rows(&response_data)?;

    let ctx = RowContext {
        cursor_field: config.cursor_field.as_deref().unwrap_or("_time"),
        current_cursor: &cursor,
        include_metadata,
        payload_col: config.payload_column.as_deref(),
        payload_format,
        now_micros: iggy_common::Utc::now().timestamp_micros() as u64,
    };

    let result = process_rows(&rows, &ctx, already_seen)?;

    // Guard: messages emitted but none contained the cursor field. max_cursor stays None;
    // apply_v2_cursor_advance is a noop; cursor never advances; infinite re-delivery with
    // no CB trip. Covers both all-emitted (skipped=0) and mixed (some skipped + some
    // emitted without cursor field, where skipped > 0).
    if !result.messages.is_empty() && result.max_cursor.is_none() {
        tracing::error!(
            "V2 source: {} row(s) processed but none contained the cursor field '{}'. \
             Cursor cannot advance. Tripping circuit breaker.",
            rows.len(),
            ctx.cursor_field,
        );
        return Err(Error::InvalidState);
    }

    // Detect all-skipped-at-cap livelock: every row was at the current cursor and
    // skipped; max_cursor stayed None; cursor cannot advance. Fire at cap-1 (not just
    // cap) to catch the off-by-one: when already_seen = cap-1 and the server returns
    // exactly cap-1 rows (all at cursor), the guard at >= cap would miss it and the
    // connector spins with cursor_row_count fixed at cap-1 forever.
    let batch_u64 = config.batch_size.unwrap_or(500).max(1) as u64;
    let cap = batch_u64.saturating_mul(MAX_SKIP_INFLATION_FACTOR);
    if result.max_cursor.is_none() && result.skipped > 0 && already_seen.saturating_add(1) >= cap {
        tracing::error!(
            "V2 source stuck-cursor livelock: already_seen ({already_seen}) reached \
             inflation cap ({MAX_SKIP_INFLATION_FACTOR}x{batch_u64}={cap}), \
             skipped={}. Cursor cannot advance. \
             Manual state clear required to recover: remove the persisted state for \
             this connector and restart.",
            result.skipped,
        );
        return Err(Error::InvalidState);
    }

    let schema = if ctx.payload_col.is_some() {
        ctx.payload_format.schema()
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
            true,
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
            true,
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
            true,
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
            true,
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
            true,
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
            true,
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
            true,
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
            true,
        )
        .await;
        assert!(matches!(result, Err(Error::PermanentHttpError(_))));
    }

    // ── build_query ──────────────────────────────────────────────────────────

    const BASE: &str = "http://localhost:8086";

    #[test]
    fn build_query_url_path_and_org_param() {
        let (url, body) = build_query(
            BASE,
            "from(bucket:\"b\") |> range(start:-1h)",
            Some("myorg"),
        )
        .unwrap();
        assert!(
            url.path().ends_with("/api/v2/query"),
            "wrong path: {}",
            url.path()
        );
        assert!(
            url.query().unwrap_or("").contains("org=myorg"),
            "missing org param"
        );
        assert!(body["query"].is_string());
        let annotations = body["dialect"]["annotations"].as_array().unwrap();
        assert!(annotations.iter().any(|v| v.as_str() == Some("datatype")));
        assert!(annotations.iter().any(|v| v.as_str() == Some("group")));
        assert!(annotations.iter().any(|v| v.as_str() == Some("default")));
    }

    #[test]
    fn build_query_without_org_omits_param() {
        let (url, _) = build_query(BASE, "SELECT 1", None).unwrap();
        assert!(url.query().is_none_or(|q| !q.contains("org=")));
    }

    #[test]
    fn build_query_invalid_base_returns_error() {
        assert!(build_query("not-a-url", "SELECT 1", None).is_err());
    }

    #[tokio::test]
    async fn poll_livelock_at_inflation_cap_returns_error() {
        // When cursor_row_count >= MAX_SKIP_INFLATION_FACTOR × batch_size and all rows
        // are skipped, poll() must return an error to trip the circuit breaker.
        // Without this, the connector loops forever: every poll skips all rows,
        // max_cursor stays None, and cursor_row_count never decreases.
        let t = "2024-01-01T00:00:00Z";
        // Return MAX_SKIP_INFLATION_FACTOR × batch_size (= 10 × 10 = 100) rows all at cursor.
        let batch_size: usize = 10;
        let row_count = batch_size * (MAX_SKIP_INFLATION_FACTOR as usize);
        let csv: String = std::iter::once("_time,_value\n".to_string())
            .chain((0..row_count).map(|i| format!("{t},{i}\n")))
            .collect();
        let app = Router::new().route(
            "/api/v2/query",
            post(move || async move { (StatusCode::OK, csv) }),
        );
        let base = start_server(app).await;
        let mut config = make_config(&base);
        config.batch_size = Some(batch_size as u32);
        // already_seen = cap = 10 × 10 = 100: exactly at the livelock threshold.
        let state = V2State {
            last_timestamp: Some(t.to_string()),
            cursor_row_count: row_count as u64,
            processed_rows: 0,
        };
        let result = poll(
            &make_client(),
            &config,
            "Token tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await;
        assert!(
            result.is_err(),
            "poll at inflation cap with all rows skipped must return an error to trip the CB"
        );
        assert!(
            matches!(result, Err(Error::InvalidState)),
            "expected InvalidState livelock error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn poll_livelock_at_cap_minus_one_returns_error() {
        // Off-by-one: guard fires at already_seen >= cap but misses already_seen = cap-1.
        // When the server returns exactly cap-1 rows (all at cursor), all are skipped,
        // cursor_row_count is reset to cap-1, and the next poll repeats identically — livelock.
        let t = "2024-01-01T00:00:00Z";
        let batch_size: usize = 10;
        let row_count = batch_size * (MAX_SKIP_INFLATION_FACTOR as usize) - 1; // cap-1 = 99
        let csv: String = std::iter::once("_time,_value\n".to_string())
            .chain((0..row_count).map(|i| format!("{t},{i}\n")))
            .collect();
        let app = Router::new().route(
            "/api/v2/query",
            post(move || async move { (StatusCode::OK, csv) }),
        );
        let base = start_server(app).await;
        let mut config = make_config(&base);
        config.batch_size = Some(batch_size as u32);
        let state = V2State {
            last_timestamp: Some(t.to_string()),
            cursor_row_count: row_count as u64, // = cap - 1 = 99
            processed_rows: 0,
        };
        let result = poll(
            &make_client(),
            &config,
            "Token tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await;
        assert!(
            result.is_err(),
            "poll at cap-1 with all rows skipped must return an error to trip the CB"
        );
        assert!(
            matches!(result, Err(Error::InvalidState)),
            "expected InvalidState livelock error at cap-1, got {result:?}"
        );
    }
}
