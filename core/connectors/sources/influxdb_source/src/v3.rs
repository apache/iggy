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

//! InfluxDB V3 source — SQL queries, JSONL responses, Bearer auth.
//!
//! V3 uses strict `> cursor` semantics. DataFusion/Parquet does not guarantee
//! stable ordering for rows that share the same timestamp, so the V2 skip-N
//! approach is not safe here. If all rows in a batch share the same timestamp,
//! the cursor cannot advance — the effective batch size is doubled each poll
//! up to `stuck_batch_cap_factor × batch_size`. If the cap is reached, the
//! circuit breaker is tripped.

use crate::common::{
    DEFAULT_V3_CURSOR_FIELD, PayloadFormat, Row, RowContext, V3SourceConfig, V3State,
    apply_query_params, is_timestamp_after, parse_jsonl_rows, timestamps_equal, validate_cursor,
};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use iggy_connector_sdk::{Error, ProducedMessage, Schema};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde_json::json;
use tracing::warn;
use uuid::Uuid;

pub(crate) const DEFAULT_STUCK_CAP_FACTOR: u32 = 10;
/// Upper bound for `stuck_batch_cap_factor`. A value of 1000 with batch_size=1000
/// would issue 1,000,000-row queries before tripping the circuit breaker.
pub(crate) const MAX_STUCK_CAP_FACTOR: u32 = 100;

/// Hard cap on buffered JSONL response body size.
///
/// `MAX_STUCK_CAP_FACTOR` can inflate the effective batch to 100 × `batch_size`,
/// making unbounded `response.text()` a real OOM vector under misconfiguration.
/// Streaming stops and returns an error once this many bytes have been read.
const MAX_RESPONSE_BODY_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

/// InfluxDB V3 query endpoint expects this exact string for JSONL response format.
const QUERY_FORMAT_JSONL: &str = "jsonl";

fn build_query(base: &str, query: &str, db: &str) -> Result<(Url, serde_json::Value), Error> {
    let url = Url::parse(&format!("{base}/api/v3/query_sql"))
        .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;
    let body = json!({
        "db":     db,
        "q":      query,
        "format": QUERY_FORMAT_JSONL
    });
    Ok((url, body))
}

// ── Query execution ───────────────────────────────────────────────────────────

pub(crate) async fn run_query(
    client: &ClientWithMiddleware,
    config: &V3SourceConfig,
    auth: &str,
    cursor: &str,
    effective_batch: u32,
    offset: u64,
) -> Result<String, Error> {
    validate_cursor(cursor)?;
    let q = apply_query_params(
        &config.query,
        cursor,
        &effective_batch.to_string(),
        &offset.to_string(), /* &str */
    );
    let base = config.url.trim_end_matches('/');
    let (url, body) = build_query(base, &q, &config.db)?;

    let mut response = client
        .post(url)
        .header("Authorization", auth)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| Error::Storage(format!("InfluxDB V3 query failed: {e}")))?;

    let status = response.status();
    if status.is_success() {
        // Stream chunk-by-chunk with a hard byte cap to prevent OOM when
        // MAX_STUCK_CAP_FACTOR inflates the effective batch to 100 × batch_size.
        if response
            .content_length()
            .is_some_and(|n| n as usize > MAX_RESPONSE_BODY_BYTES)
        {
            return Err(Error::Storage(format!(
                "InfluxDB V3 response body exceeds {MAX_RESPONSE_BODY_BYTES} byte cap; \
                 reduce batch_size to avoid OOM"
            )));
        }
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| Error::Storage(format!("Failed to read V3 response: {e}")))?
        {
            buf.extend_from_slice(&chunk);
            if buf.len() > MAX_RESPONSE_BODY_BYTES {
                return Err(Error::Storage(format!(
                    "InfluxDB V3 response body exceeded {MAX_RESPONSE_BODY_BYTES} byte cap \
                     while streaming; reduce batch_size to avoid OOM"
                )));
            }
        }
        return String::from_utf8(buf)
            .map_err(|e| Error::Storage(format!("V3 response body is not valid UTF-8: {e}")));
    }

    let body_text = response
        .text()
        .await
        .unwrap_or_else(|_| "failed to read response body".to_string());

    // 404 "database not found" means the namespace has not been written to yet;
    // treat it as empty rather than a failure so the circuit breaker stays healthy.
    // Any other 404 (e.g. "table not found") is a permanent error — don't swallow it.
    if status.as_u16() == 404 {
        if body_text.to_lowercase().contains("database not found") {
            return Ok(String::new());
        }
        return Err(Error::PermanentHttpError(format!(
            "InfluxDB V3 query failed with status {status}: {body_text}"
        )));
    }

    if iggy_connector_sdk::retry::is_transient_status(status) {
        Err(Error::Storage(format!(
            "InfluxDB V3 query failed with status {status}: {body_text}"
        )))
    } else {
        Err(Error::PermanentHttpError(format!(
            "InfluxDB V3 query failed with status {status}: {body_text}"
        )))
    }
}

// ── Message building ──────────────────────────────────────────────────────────

fn build_payload(
    row: &Row,
    payload_column: Option<&str>,
    payload_format: PayloadFormat,
    include_metadata: bool,
    cursor_field: &str,
) -> Result<Vec<u8>, Error> {
    if let Some(col) = payload_column {
        let raw = row
            .get(col)
            .cloned()
            .ok_or_else(|| Error::InvalidRecordValue(format!("Missing payload column '{col}'")))?;
        return match payload_format {
            // raw is already a serde_json::Value — serialize directly, no re-parse.
            PayloadFormat::Json => serde_json::to_vec(&raw)
                .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}"))),
            PayloadFormat::Text => match raw {
                serde_json::Value::String(s) => Ok(s.into_bytes()),
                other => serde_json::to_vec(&other)
                    .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}"))),
            },
            PayloadFormat::Raw => {
                let s = raw.as_str().ok_or_else(|| {
                    Error::InvalidRecordValue(format!(
                        "Payload column '{col}' must be a string value for Raw format"
                    ))
                })?;
                general_purpose::STANDARD.decode(s.as_bytes()).map_err(|e| {
                    Error::InvalidRecordValue(format!("Failed to decode payload as base64: {e}"))
                })
            }
        };
    }

    // V3 rows carry typed serde_json::Values — clone directly, no parse_scalar needed.
    // When include_metadata=false, exclude the cursor column (timestamp).
    let json_row: serde_json::Map<_, _> = row
        .iter()
        .filter(|(k, _)| include_metadata || k.as_str() != cursor_field)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    serde_json::to_vec(&json_row)
        .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}")))
}

/// Compute the next effective batch size when the batch is stuck.
/// Doubles until it reaches `cap`. Returns `None` if already at cap.
pub(crate) fn next_stuck_batch_size(current: u32, base: u32, cap_factor: u32) -> Option<u32> {
    let cap = base.saturating_mul(cap_factor);
    if current >= cap {
        None
    } else {
        Some(current.saturating_mul(2).min(cap))
    }
}

// ── Poll ──────────────────────────────────────────────────────────────────────

pub(crate) struct PollResult {
    pub messages: Vec<ProducedMessage>,
    pub new_state: V3State,
    pub schema: Schema,
    /// Set to true when the stuck-timestamp cap was reached and the circuit
    /// breaker should be tripped by the caller.
    pub trip_circuit_breaker: bool,
}

// ── Row processing (pure, testable without HTTP) ──────────────────────────────

/// Normalize a raw timestamp from InfluxDB V3 JSONL into a cursor-safe RFC 3339 string.
///
/// InfluxDB 3 Core returns timestamps without a timezone suffix and with nanosecond
/// precision (e.g. `"2026-04-26T02:32:20.526360865"`). The only required fix is
/// appending `"Z"` when no timezone suffix is present (InfluxDB always stores UTC).
///
/// Full nanosecond precision is intentionally preserved — truncating to milliseconds
/// would place the cursor BEFORE the actual row timestamps within the same millisecond,
/// causing `WHERE time > '$cursor'` to re-deliver already-seen rows on subsequent polls.
/// InfluxDB 3's DataFusion SQL engine handles RFC 3339 strings with any number of
/// fractional digits in WHERE clause timestamp comparisons.
fn normalize_v3_timestamp(ts: &str) -> String {
    // Fast path: already a valid RFC 3339 timestamp with timezone suffix.
    if chrono::DateTime::parse_from_rfc3339(ts).is_ok() {
        return ts.to_string();
    }
    // Slow path: no timezone suffix — append "Z" to make it RFC 3339 compliant.
    format!("{ts}Z")
}

/// Result of processing a batch of V3 rows into Iggy messages.
#[derive(Debug)]
pub(crate) struct RowProcessingResult {
    pub messages: Vec<ProducedMessage>,
    pub max_cursor: Option<String>,
    /// `true` when every row's `cursor_field` value equals `current_cursor`.
    /// Combined with `rows.len() >= effective_batch`, this signals a stuck batch:
    /// all returned rows are at the current cursor, meaning the cursor cannot
    /// advance with `> cursor` semantics.
    pub all_at_cursor: bool,
    /// Count of rows whose cursor == max_cursor (for tiebreaker offset).
    pub rows_at_max_cursor: u64,
}

/// Convert a slice of V3 query rows into Iggy messages.
///
/// Also detects whether all rows share the same cursor value as `ctx.current_cursor`
/// (the `all_at_cursor` flag). The caller uses this together with batch fullness
/// to decide whether to inflate the batch size for the next poll.
///
/// Unlike V2, V3 uses strict `> cursor` semantics, so there is no row-skipping.
/// All rows in the slice are emitted as messages.
pub(crate) fn process_rows(
    rows: &[Row],
    ctx: &RowContext<'_>,
) -> Result<RowProcessingResult, Error> {
    let mut messages = Vec::with_capacity(rows.len());
    let mut max_cursor: Option<String> = None;
    let mut max_cursor_parsed: Option<DateTime<Utc>> = None; // cache parsed form
    // Starts true for non-empty batches; flipped to false as soon as any row
    // either has a different cursor value or has no cursor field at all.
    let mut all_at_cursor = !rows.is_empty();
    // Generate the base UUID once per poll; derive per-message IDs by addition.
    // This is O(1) PRNG calls per batch instead of O(n), measurable at batch ≥ 100.
    let id_base = Uuid::new_v4().as_u128();
    for row in rows.iter() {
        if let Some(raw_cv) = row.get(ctx.cursor_field).and_then(|v| v.as_str()) {
            let cv_owned = normalize_v3_timestamp(raw_cv);
            let cv = cv_owned.as_str();
            if !timestamps_equal(cv, ctx.current_cursor) {
                all_at_cursor = false;
            }
            validate_cursor(cv)?;
            let cv_parsed = cv.parse::<DateTime<Utc>>().ok();
            match (cv_parsed, max_cursor_parsed) {
                (Some(new_dt), Some(cur_dt)) if new_dt > cur_dt => {
                    max_cursor = Some(cv.to_string());
                    max_cursor_parsed = Some(new_dt);
                }
                (Some(new_dt), None) => {
                    max_cursor = Some(cv.to_string());
                    max_cursor_parsed = Some(new_dt);
                }
                (None, _) if max_cursor_parsed.is_none() => {
                    // Unparsable cursor — still track it (string fallback) if no
                    // parsable cursor has been seen yet.
                    max_cursor = Some(cv.to_string());
                }
                _ => {}
            }
        } else {
            all_at_cursor = false;
        }

        let payload = build_payload(
            row,
            ctx.payload_col,
            ctx.payload_format,
            ctx.include_metadata,
            ctx.cursor_field,
        )?;
        messages.push(ProducedMessage {
            id: Some(id_base.wrapping_add(messages.len() as u128)),
            checksum: None,
            timestamp: Some(ctx.now_micros),
            origin_timestamp: Some(ctx.now_micros),
            headers: None,
            payload,
        });
    }

    let rows_at_max_cursor = rows
        .iter()
        .filter(|r| {
            max_cursor.as_deref().is_some_and(|mc| {
                r.get(ctx.cursor_field)
                    .and_then(|v| v.as_str())
                    .is_some_and(|cv| normalize_v3_timestamp(cv) == mc)
            })
        })
        .count() as u64;

    if !rows.is_empty() && max_cursor.is_none() {
        return Err(Error::InvalidRecordValue(format!(
            "No '{}' field found in any returned row — cursor cannot advance; \
             the connector would re-deliver the same rows on every poll. \
             Ensure your query selects the cursor column.",
            ctx.cursor_field
        )));
    }

    Ok(RowProcessingResult {
        messages,
        max_cursor,
        all_at_cursor,
        rows_at_max_cursor,
    })
}

pub(crate) async fn poll(
    client: &ClientWithMiddleware,
    config: &V3SourceConfig,
    auth: &str,
    state: &V3State,
    payload_format: PayloadFormat,
    include_metadata: bool,
) -> Result<PollResult, Error> {
    // Access config.initial_offset directly (not via the enum accessor) because
    // poll() receives &V3SourceConfig — the inner struct — already matched by the
    // caller in lib.rs. The enum accessor InfluxDbSourceConfig::initial_offset()
    // is not available here.
    let cursor = state
        .last_timestamp
        .clone()
        .or_else(|| config.initial_offset.clone())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());

    let base_batch = config.batch_size.unwrap_or(500);
    let effective_batch = if state.effective_batch_size == 0 {
        base_batch
    } else {
        state.effective_batch_size
    };

    let response_data = run_query(
        client,
        config,
        auth,
        &cursor,
        effective_batch,
        state.last_timestamp_row_offset,
    )
    .await?;
    let rows = parse_jsonl_rows(&response_data)?;

    let cap_factor = config
        .stuck_batch_cap_factor
        .unwrap_or(DEFAULT_STUCK_CAP_FACTOR);
    let ctx = RowContext {
        cursor_field: config
            .cursor_field
            .as_deref()
            .unwrap_or(DEFAULT_V3_CURSOR_FIELD),
        current_cursor: &cursor,
        include_metadata,
        payload_col: config.payload_column.as_deref(),
        payload_format,
        now_micros: iggy_common::Utc::now().timestamp_micros() as u64,
    };

    let result = process_rows(&rows, &ctx)?;

    // Stuck-timestamp detection: if every row is at the current cursor
    // and the batch was full, inflate and request more next time.
    let stuck = result.all_at_cursor && rows.len() >= effective_batch as usize;

    if stuck {
        return match next_stuck_batch_size(effective_batch, base_batch, cap_factor) {
            Some(next_batch) => {
                warn!(
                    "InfluxDB V3 source — all {} rows share timestamp {cursor:?}; \
                     inflating batch size {} → {} (cap={}×{}={})",
                    rows.len(),
                    effective_batch,
                    next_batch,
                    cap_factor,
                    base_batch,
                    base_batch.saturating_mul(cap_factor)
                );
                Ok(PollResult {
                    messages: vec![],
                    new_state: V3State {
                        last_timestamp: state.last_timestamp.clone(),
                        processed_rows: state.processed_rows,
                        effective_batch_size: next_batch,
                        last_timestamp_row_offset: result.rows_at_max_cursor,
                    },
                    schema: Schema::Json,
                    trip_circuit_breaker: false,
                })
            }
            None => {
                warn!(
                    "InfluxDB V3 source — stuck-timestamp cap reached at batch size {effective_batch}; \
                     tripping circuit breaker to prevent an infinite loop"
                );
                Ok(PollResult {
                    messages: vec![],
                    new_state: V3State {
                        last_timestamp: state.last_timestamp.clone(),
                        processed_rows: state.processed_rows,
                        effective_batch_size: effective_batch,
                        last_timestamp_row_offset: result.rows_at_max_cursor,
                    },
                    schema: Schema::Json,
                    trip_circuit_breaker: true,
                })
            }
        };
    }

    let processed_rows = state.processed_rows + result.messages.len() as u64;
    let old_dt = state
        .last_timestamp
        .as_deref()
        .and_then(|s| s.parse::<DateTime<Utc>>().ok());
    let advanced_cursor = match (
        result.max_cursor.as_deref(),
        state.last_timestamp.as_deref(),
    ) {
        (Some(new), Some(_)) if old_dt.is_some_and(|dt| is_timestamp_after(new, dt)) => {
            result.max_cursor
        }
        (Some(_), Some(_)) => {
            warn!("V3 source: max_cursor did not advance past saved cursor; keeping old value");
            state.last_timestamp.clone()
        }
        (Some(_), None) => result.max_cursor, // first poll
        _ => state.last_timestamp.clone(),    // empty batch
    };

    let new_state = V3State {
        last_timestamp: advanced_cursor,
        processed_rows,
        effective_batch_size: base_batch, // reset on successful advance
        last_timestamp_row_offset: result.rows_at_max_cursor,
    };

    let schema = if ctx.payload_col.is_some() {
        ctx.payload_format.schema()
    } else {
        Schema::Json
    };

    Ok(PollResult {
        messages: result.messages,
        new_state,
        schema,
        trip_circuit_breaker: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Row, RowContext};

    fn row(pairs: &[(&str, &str)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect()
    }

    const T1: &str = "2024-01-01T00:00:00Z";
    const T2: &str = "2024-01-01T00:00:01Z";
    const T3: &str = "2024-01-01T00:00:02Z";

    fn ctx(current_cursor: &str, now_micros: u64) -> RowContext<'_> {
        RowContext {
            cursor_field: "time",
            current_cursor,
            include_metadata: true,
            payload_col: None,
            payload_format: PayloadFormat::Json,
            now_micros,
        }
    }

    // ── process_rows ─────────────────────────────────────────────────────────

    #[test]
    fn process_rows_empty_returns_empty() {
        let result = process_rows(&[], &ctx(T1, 1000)).unwrap();
        assert!(result.messages.is_empty());
        assert!(result.max_cursor.is_none());
        assert!(
            !result.all_at_cursor,
            "empty slice must not be all_at_cursor"
        );
    }

    #[test]
    fn process_rows_single_row_advances_cursor() {
        let rows = vec![row(&[("time", T1), ("val", "1")])];
        let result = process_rows(&rows, &ctx(T1, 1000)).unwrap();
        assert_eq!(result.messages.len(), 1);
        assert_eq!(result.max_cursor.as_deref(), Some(T1));
    }

    #[test]
    fn process_rows_advances_to_latest_timestamp() {
        let rows = vec![
            row(&[("time", T1)]),
            row(&[("time", T3)]),
            row(&[("time", T2)]),
        ];
        let result = process_rows(&rows, &ctx(T1, 1000)).unwrap();
        assert_eq!(result.max_cursor.as_deref(), Some(T3));
        assert_eq!(result.messages.len(), 3);
    }

    #[test]
    fn process_rows_tied_timestamps_do_not_regress_cursor() {
        let rows = vec![
            row(&[("time", T2)]),
            row(&[("time", T1)]), // earlier — must not overwrite max
            row(&[("time", T2)]),
        ];
        let result = process_rows(&rows, &ctx(T1, 1000)).unwrap();
        assert_eq!(result.max_cursor.as_deref(), Some(T2));
    }

    #[test]
    fn process_rows_row_without_cursor_field_returns_error() {
        // A batch where no row has the cursor column must return Err rather than
        // silently re-delivering the same rows on every poll.
        let rows = vec![row(&[("val", "1")])]; // no "time" field
        let err = process_rows(&rows, &ctx(T1, 1000)).unwrap_err();
        assert!(
            matches!(err, Error::InvalidRecordValue(_)),
            "expected InvalidRecordValue when cursor column is absent, got {err:?}"
        );
    }

    #[test]
    fn process_rows_all_rows_missing_cursor_field_returns_error() {
        // When no row in the batch contains the cursor column, process_rows
        // returns Err. This trips the circuit breaker via poll()'s `?`, giving
        // the operator a visible failure rather than a silent infinite re-delivery.
        let rows = vec![
            row(&[("val", "1")]),
            row(&[("val", "2")]),
            row(&[("val", "3")]),
        ];
        let err = process_rows(&rows, &ctx(T1, 1000)).unwrap_err();
        assert!(
            matches!(err, Error::InvalidRecordValue(_)),
            "expected InvalidRecordValue when cursor column is absent, got {err:?}"
        );
    }

    #[test]
    fn process_rows_message_ids_are_some_and_unique() {
        let rows = vec![row(&[("time", T1)]), row(&[("time", T2)])];
        let result = process_rows(&rows, &ctx(T1, 1000)).unwrap();
        assert!(result.messages[0].id.is_some());
        assert!(result.messages[1].id.is_some());
        assert_ne!(result.messages[0].id, result.messages[1].id);
    }

    #[test]
    fn process_rows_message_timestamps_use_now_micros() {
        let rows = vec![row(&[("time", T1)])];
        let result = process_rows(&rows, &ctx(T1, 888_888)).unwrap();
        assert_eq!(result.messages[0].timestamp, Some(888_888));
        assert_eq!(result.messages[0].origin_timestamp, Some(888_888));
    }

    #[test]
    fn process_rows_text_payload_format() {
        use base64::{Engine as _, engine::general_purpose};
        let encoded = general_purpose::STANDARD.encode(b"hello");
        let rows = vec![row(&[("time", T1), ("payload", &encoded)])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("payload"),
                payload_format: PayloadFormat::Text,
                now_micros: 1000,
            },
        )
        .unwrap();
        assert_eq!(result.messages.len(), 1);
    }

    // ── all_at_cursor / stuck-batch ───────────────────────────────────────────

    #[test]
    fn process_rows_all_at_cursor_true_when_all_rows_match() {
        let rows = vec![row(&[("time", T1)]), row(&[("time", T1)])];
        let result = process_rows(&rows, &ctx(T1, 1000)).unwrap();
        assert!(result.all_at_cursor);
    }

    #[test]
    fn process_rows_all_at_cursor_false_when_any_row_advances() {
        let rows = vec![row(&[("time", T1)]), row(&[("time", T2)])];
        let result = process_rows(&rows, &ctx(T1, 1000)).unwrap();
        assert!(!result.all_at_cursor);
    }

    #[test]
    fn process_rows_all_at_cursor_false_for_empty_slice() {
        let result = process_rows(&[], &ctx(T1, 1000)).unwrap();
        assert!(!result.all_at_cursor);
    }

    // ── next_stuck_batch_size ────────────────────────────────────────────────

    #[test]
    fn next_stuck_batch_size_doubles_until_cap() {
        assert_eq!(next_stuck_batch_size(500, 500, 10), Some(1000));
        assert_eq!(next_stuck_batch_size(1000, 500, 10), Some(2000));
        assert_eq!(next_stuck_batch_size(4000, 500, 10), Some(5000));
        assert_eq!(next_stuck_batch_size(5000, 500, 10), None);
    }

    // ── normalize_v3_timestamp ────────────────────────────────────────────────

    #[test]
    fn normalize_already_valid_rfc3339_unchanged() {
        // Already valid RFC 3339 with Z and ms precision — must be returned as-is.
        assert_eq!(
            normalize_v3_timestamp("2024-01-01T00:00:00.123Z"),
            "2024-01-01T00:00:00.123Z"
        );
        // Second-precision with Z is also ≤ms, returned unchanged.
        assert_eq!(
            normalize_v3_timestamp("2024-01-01T00:00:00Z"),
            "2024-01-01T00:00:00Z"
        );
    }

    #[test]
    fn normalize_no_tz_nanoseconds_appends_z_only() {
        // InfluxDB 3 Core returns timestamps like this — 9 fractional digits, no Z.
        // Full nanosecond precision must be preserved (not truncated to ms).
        let result = normalize_v3_timestamp("2026-04-26T02:32:20.526360865");
        assert_eq!(result, "2026-04-26T02:32:20.526360865Z");
    }

    #[test]
    fn normalize_no_tz_milliseconds_appends_z() {
        // No timezone suffix, ms precision — just append Z.
        let result = normalize_v3_timestamp("2026-04-26T02:32:20.526");
        assert_eq!(result, "2026-04-26T02:32:20.526Z");
    }

    #[test]
    fn normalize_rfc3339_sub_ms_precision_returned_unchanged() {
        // Already valid RFC 3339 with Z and nanoseconds — returned as-is.
        let result = normalize_v3_timestamp("2026-04-26T02:32:20.526360865Z");
        assert_eq!(result, "2026-04-26T02:32:20.526360865Z");
    }

    #[test]
    fn normalize_invalid_returns_with_z_appended() {
        // Unparsable string — append Z and return (validate_cursor will reject it later).
        let result = normalize_v3_timestamp("not-a-timestamp");
        assert_eq!(result, "not-a-timestampZ");
    }

    #[test]
    fn process_rows_accepts_influxdb3_no_tz_timestamps() {
        // Regression test: process_rows must not return Err when timestamps lack Z suffix.
        // Full nanosecond precision must be preserved so the cursor is exact.
        let rows = vec![
            row(&[("time", "2026-04-26T02:32:20.526360865"), ("val", "1")]),
            row(&[("time", "2026-04-26T02:32:21.000000000"), ("val", "2")]),
        ];
        let c = ctx("2026-04-26T02:32:19.000Z", 0);
        let result = process_rows(&rows, &c).expect("should not fail on bare timestamps");
        assert_eq!(result.messages.len(), 2);
        assert_eq!(
            result.max_cursor.as_deref(),
            Some("2026-04-26T02:32:21.000000000Z")
        );
    }

    #[test]
    fn process_rows_sub_ms_timestamps_have_distinct_cursors() {
        // Regression: rows within the same millisecond must NOT get the same ms cursor,
        // which would cause re-delivery. Each row's nanosecond cursor must be preserved.
        let rows = vec![
            row(&[("time", "2026-04-26T02:32:20.526360000"), ("val", "a")]),
            row(&[("time", "2026-04-26T02:32:20.526361000"), ("val", "b")]),
            row(&[("time", "2026-04-26T02:32:20.526362000"), ("val", "c")]),
        ];
        let c = ctx("2026-04-26T02:32:19.000Z", 0);
        let result = process_rows(&rows, &c).expect("should succeed");
        // max_cursor must be the latest nanosecond timestamp (row 3), not a truncated ms.
        assert_eq!(
            result.max_cursor.as_deref(),
            Some("2026-04-26T02:32:20.526362000Z")
        );
        // Only row 3 is at max_cursor.
        assert_eq!(result.rows_at_max_cursor, 1);
    }
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

    fn make_config(url: &str) -> V3SourceConfig {
        V3SourceConfig {
            url: url.to_string(),
            db: "test_db".to_string(),
            token: SecretString::from("test_token"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit".to_string(),
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
            stuck_batch_cap_factor: None,
        }
    }

    const CURSOR: &str = "1970-01-01T00:00:00Z";

    // ── run_query ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn run_query_returns_jsonl_body_on_200() {
        let jsonl = r#"{"time":"2024-01-01T00:00:00Z","val":1}"#;
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await
        .unwrap();
        assert!(result.contains("val"));
        assert!(result.contains("2024-01-01"));
    }

    #[tokio::test]
    async fn run_query_empty_body_on_200() {
        let app = Router::new().route("/api/v3/query_sql", post(|| async { (StatusCode::OK, "") }));
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    /// V3-specific: 404 with body containing "database not found" must return
    /// an empty string rather than an error (namespace not yet written to).
    #[tokio::test]
    async fn run_query_404_database_not_found_returns_empty_string() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { (StatusCode::NOT_FOUND, "database not found") }),
        );
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    /// Any other 404 body must NOT be swallowed — it is a permanent error.
    #[tokio::test]
    async fn run_query_404_other_body_returns_permanent_error() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { (StatusCode::NOT_FOUND, "table not found") }),
        );
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await;
        assert!(matches!(result, Err(Error::PermanentHttpError(_))));
    }

    #[tokio::test]
    async fn run_query_500_returns_transient_error() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await;
        assert!(matches!(result, Err(Error::Storage(_))));
    }

    #[tokio::test]
    async fn run_query_400_returns_permanent_error() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { StatusCode::BAD_REQUEST }),
        );
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await;
        assert!(matches!(result, Err(Error::PermanentHttpError(_))));
    }

    #[tokio::test]
    async fn run_query_sends_bearer_authorization_header() {
        let captured: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured.clone();
        let app = Router::new().route(
            "/api/v3/query_sql",
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
            "Bearer my_token",
            CURSOR,
            10,
            0,
        )
        .await;
        assert_eq!(*captured.lock().await, "Bearer my_token");
    }

    #[tokio::test]
    async fn run_query_request_body_contains_db_and_substituted_cursor() {
        let captured_body: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured_body.clone();
        let app = Router::new().route(
            "/api/v3/query_sql",
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
        let cursor = "2024-06-01T00:00:00Z";
        let _ = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            cursor,
            10,
            0,
        )
        .await;
        let body = captured_body.lock().await;
        assert!(body.contains("test_db"), "body should include db: {body}");
        assert!(body.contains(cursor), "body should include cursor: {body}");
        assert!(
            !body.contains("$cursor"),
            "raw placeholder must not appear: {body}"
        );
    }

    // ── poll() end-to-end ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn poll_returns_messages_for_jsonl_response() {
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"val\":1}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"val\":2}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 2);
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some("2024-01-01T00:00:02Z")
        );
        assert!(!result.trip_circuit_breaker);
        assert_eq!(result.schema, Schema::Json);
    }

    #[tokio::test]
    async fn poll_advances_cursor_to_latest_out_of_order_timestamp() {
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"v\":1}\n\
                     {\"time\":\"2024-01-01T00:00:03Z\",\"v\":3}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"v\":2}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 3);
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some("2024-01-01T00:00:03Z")
        );
    }

    #[tokio::test]
    async fn poll_empty_jsonl_returns_no_messages() {
        let app = Router::new().route("/api/v3/query_sql", post(|| async { (StatusCode::OK, "") }));
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert!(result.messages.is_empty());
        assert!(!result.trip_circuit_breaker);
        // Cursor must not regress
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some("2024-01-01T00:00:00Z")
        );
    }

    #[tokio::test]
    async fn poll_detects_stuck_batch_and_doubles_batch_size() {
        // All batch_size rows share the same timestamp as the cursor → stuck.
        // Expected: no messages produced, effective_batch_size doubled.
        let t = "2024-01-01T00:00:00Z";
        let jsonl: String = (0..10)
            .map(|i| format!("{{\"time\":\"{t}\",\"val\":{i}}}\n"))
            .collect();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        // cursor = t so every row matches it
        let state = V3State {
            last_timestamp: Some(t.to_string()),
            effective_batch_size: 10,
            processed_rows: 0,
            last_timestamp_row_offset: 0,
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert!(
            result.messages.is_empty(),
            "stuck batch must produce no messages"
        );
        assert_eq!(result.new_state.effective_batch_size, 20, "should double");
        assert!(!result.trip_circuit_breaker);
        // Cursor must not change
        assert_eq!(result.new_state.last_timestamp.as_deref(), Some(t));
    }

    #[tokio::test]
    async fn poll_trips_circuit_breaker_when_stuck_cap_reached() {
        // cap_factor=1 → cap = batch_size × 1 = 10.
        // effective_batch_size is already 10 (= cap) → next_stuck_batch_size returns None.
        let t = "2024-01-01T00:00:00Z";
        let jsonl: String = (0..10)
            .map(|i| format!("{{\"time\":\"{t}\",\"val\":{i}}}\n"))
            .collect();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let config = V3SourceConfig {
            stuck_batch_cap_factor: Some(1),
            ..make_config(&base)
        };
        let state = V3State {
            last_timestamp: Some(t.to_string()),
            effective_batch_size: 10,
            processed_rows: 0,
            last_timestamp_row_offset: 0,
        };
        let result = poll(
            &make_client(),
            &config,
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert!(result.trip_circuit_breaker, "must trip when at cap");
        assert!(result.messages.is_empty());
    }

    #[tokio::test]
    async fn poll_resets_effective_batch_size_on_cursor_advance() {
        // State has an inflated batch size from a previous stuck run.
        // When the cursor advances the batch size must reset to the base value.
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"v\":1}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"v\":2}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            effective_batch_size: 5000,
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        // make_config has batch_size=10 → base_batch=10
        assert_eq!(
            result.new_state.effective_batch_size, 10,
            "should reset to base"
        );
        assert_eq!(result.messages.len(), 2);
    }

    #[tokio::test]
    async fn poll_accumulates_processed_rows_in_state() {
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"v\":1}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"v\":2}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            processed_rows: 7,
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.new_state.processed_rows, 9); // 7 prior + 2 new
    }

    #[tokio::test]
    async fn poll_propagates_transient_http_error() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let base = start_server(app).await;
        let state = V3State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await;
        assert!(matches!(result, Err(Error::Storage(_))));
    }

    #[tokio::test]
    async fn poll_permanent_http_error_propagates() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { StatusCode::BAD_REQUEST }),
        );
        let base = start_server(app).await;
        let state = V3State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await;
        assert!(matches!(result, Err(Error::PermanentHttpError(_))));
    }

    // ── build_query ──────────────────────────────────────────────────────────

    const BASE: &str = "http://localhost:8181";

    #[test]
    fn build_query_url_path_and_body_fields() {
        let (url, body) = build_query(BASE, "SELECT * FROM cpu LIMIT 10", "sensors").unwrap();
        assert!(
            url.path().ends_with("/api/v3/query_sql"),
            "wrong path: {}",
            url.path()
        );
        assert!(
            url.query().is_none_or(|q| !q.contains("org=")),
            "org must not appear in URL"
        );
        assert_eq!(body["db"].as_str(), Some("sensors"));
        assert_eq!(body["format"].as_str(), Some("jsonl"));
        assert!(body["q"].as_str().unwrap().contains("SELECT"));
    }

    #[test]
    fn build_query_format_is_always_jsonl() {
        let (_, body) = build_query(BASE, "SELECT 1", "db").unwrap();
        assert_eq!(body["format"].as_str(), Some("jsonl"));
    }

    #[test]
    fn build_query_invalid_base_returns_error() {
        assert!(build_query("not-a-url", "SELECT 1", "db").is_err());
    }
}
