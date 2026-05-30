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
//! approach is not safe here. If a full batch is returned and all rows share
//! the same timestamp, the cursor cannot advance — the effective batch size is
//! doubled each poll up to `stuck_batch_cap_factor × batch_size`. If the cap
//! is reached, the circuit breaker is tripped.

use crate::common::{
    DEFAULT_V3_CURSOR_FIELD, PayloadFormat, Row, RowContext, V3SourceConfig, V3State,
    apply_query_params, is_timestamp_after, parse_jsonl_rows, validate_cursor,
};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use iggy_connector_sdk::{Error, ProducedMessage, Schema};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde_json::json;
use tracing::warn;

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
        &offset.to_string(),
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

    // Serialize directly from borrowed references — avoids O(fields) String+Value
    // clones that the collect-into-Map approach required at high batch sizes.
    struct RowView<'a> {
        row: &'a Row,
        cursor_field: &'a str,
        include_metadata: bool,
    }
    impl serde::Serialize for RowView<'_> {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeMap;
            // serde_json ignores the length hint, so pass None and iterate directly
            // to avoid the intermediate Vec allocation.
            let mut map = s.serialize_map(None)?;
            for (k, v) in self
                .row
                .iter()
                .filter(|(k, _)| self.include_metadata || k.as_ref() != self.cursor_field)
            {
                map.serialize_entry(k, v)?;
            }
            map.end()
        }
    }
    serde_json::to_vec(&RowView {
        row,
        cursor_field,
        include_metadata,
    })
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
    /// Set to true when the poll detected a stuck batch or clock regression.
    /// Caller must NOT call `record_success()` - the query succeeded but the
    /// cursor did not advance; recording success would reset failure counters
    /// and mask genuine errors that accumulated earlier.
    pub is_stuck: bool,
}

// ── Row processing (pure, testable without HTTP) ──────────────────────────────

/// Normalize a raw timestamp from InfluxDB V3 JSONL into a cursor-safe RFC 3339 string
/// and its parsed `DateTime<Utc>`.
///
/// InfluxDB 3 Core returns timestamps without a timezone suffix and with nanosecond
/// precision (e.g. `"2026-04-26T02:32:20.526360865"`). The only required fix is
/// appending `"Z"` when no timezone suffix is present (InfluxDB always stores UTC).
///
/// Returns `(normalized_string, parsed_utc)` on success so the caller receives the
/// parsed `DateTime<Utc>` directly, avoiding a second `parse_from_rfc3339` call.
/// The string is borrowed when no `"Z"` was appended (zero allocation).
///
/// Returns `Err` if the value is not a valid RFC 3339 timestamp even after appending `"Z"`.
///
/// Full nanosecond precision is intentionally preserved — truncating to milliseconds
/// would place the cursor BEFORE the actual row timestamps within the same millisecond,
/// causing `WHERE time > '$cursor'` to re-deliver already-seen rows on subsequent polls.
fn normalize_v3_timestamp(ts: &str) -> Result<(std::borrow::Cow<'_, str>, DateTime<Utc>), Error> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) {
        return Ok((std::borrow::Cow::Borrowed(ts), dt.with_timezone(&Utc)));
    }
    let with_z = format!("{ts}Z");
    match chrono::DateTime::parse_from_rfc3339(&with_z) {
        Ok(dt) => Ok((std::borrow::Cow::Owned(with_z), dt.with_timezone(&Utc))),
        Err(_) => Err(Error::InvalidRecordValue(format!(
            "cursor field contains {ts:?} which is not a valid RFC 3339 timestamp \
             (tried appending 'Z' — still invalid)"
        ))),
    }
}

/// Result of processing a batch of V3 rows into Iggy messages.
#[derive(Debug)]
pub(crate) struct RowProcessingResult {
    pub messages: Vec<ProducedMessage>,
    pub max_cursor: Option<String>,
    /// Count of rows whose cursor == max_cursor.
    /// Used for stuck-batch detection: if this equals effective_batch, the cursor
    /// cannot advance and the batch size must be inflated.
    pub rows_at_max_cursor: u64,
    /// Highest timestamp strictly before `max_cursor`, if the batch contains two
    /// or more distinct timestamps. `None` when all rows share one timestamp.
    ///
    /// Used for mixed-timestamp full-batch detection: when a batch is completely
    /// full but `rows_at_max_cursor < effective_batch` (mixed timestamps), advancing
    /// the cursor to `max_cursor` would silently drop any rows at that timestamp
    /// that did not fit in this batch. Advancing to `penultimate_cursor` instead
    /// keeps those rows reachable on the next poll.
    pub penultimate_cursor: Option<String>,
    /// Number of messages with cursor timestamp strictly less than `max_cursor`.
    ///
    /// On a full mixed-timestamp batch, only `messages[..safe_message_count]` are
    /// emitted so that no rows at `max_cursor` are lost — the rest are re-fetched
    /// via `WHERE time > penultimate_cursor` on the next poll.
    pub safe_message_count: usize,
}

/// Converts a slice of V3 query rows into Iggy messages.
///
/// ## Cursor semantics
///
/// InfluxDB V3 queries use strict `WHERE time > '$cursor'` semantics, so every row
/// in the slice is strictly after the current cursor. No row-skipping or deduplication
/// is needed — unlike V2's inclusive `>= $cursor` query.
///
/// ## Timestamp normalization
///
/// InfluxDB 3 Core returns timestamps without a timezone suffix and with nanosecond
/// precision (e.g. `"2026-04-26T02:32:20.526360865"`). Each cursor value is passed
/// through [`normalize_v3_timestamp`] before parsing, which appends `"Z"` when no
/// timezone designator is present. Full nanosecond precision is preserved — truncating
/// to milliseconds would shift the cursor before rows that share a millisecond
/// boundary, causing re-delivery on the next poll.
///
/// ## Cursor tracking and stuck-batch detection
///
/// The highest timestamp seen across the batch becomes `max_cursor` in the result.
/// `rows_at_max_cursor` counts how many rows share that timestamp, computed in a
/// single pass with no extra allocations. The caller uses
/// `rows_at_max_cursor >= effective_batch_size` as the stuck-batch signal: when a
/// group of rows all carry the same timestamp and fill the entire batch, the cursor
/// cannot advance further, so the effective batch size is doubled (up to
/// `stuck_batch_cap_factor × base_batch_size`) to read past the tied rows.
///
/// ## Error conditions
///
/// - Any row whose cursor field value fails [`normalize_v3_timestamp`] (not a valid RFC 3339
///   timestamp even after appending `"Z"`) is an immediate error.
/// - If the batch is non-empty but *no* row contains the cursor field, an error is
///   returned — the cursor cannot advance and the connector would re-deliver the same
///   rows indefinitely. Individual rows that merely lack the cursor field (while at
///   least one other row has it) still produce messages without error.
///
/// ## Message identity
///
/// A single random UUID is generated per call; per-message IDs are derived by
/// adding the message's position to that base, keeping PRNG work O(1) per batch.
///
/// ## Parameters
///
/// - `rows`: Rows returned by the SQL query for this poll (already filtered by `> cursor`).
/// - `ctx`: Shared context (cursor field name, current cursor value, payload config,
///   wall-clock time in microseconds).
///
/// ## Returns
///
/// A [`RowProcessingResult`] containing:
/// - `messages`: One [`ProducedMessage`] per row.
/// - `max_cursor`: Highest cursor timestamp seen, if any row had a parsable cursor field.
/// - `rows_at_max_cursor`: Count of rows sharing `max_cursor`; used for stuck-batch detection.
pub(crate) fn process_rows(
    rows: &[Row],
    ctx: &RowContext<'_>,
    row_offset_base: u64,
) -> Result<RowProcessingResult, Error> {
    let mut messages = Vec::with_capacity(rows.len());
    let mut max_cursor: Option<String> = None;
    let mut max_cursor_parsed: Option<DateTime<Utc>> = None; // cache parsed form
    // Counted inline to avoid a second pass over rows (which would also
    // re-call normalize_v3_timestamp for each row — extra allocations).
    let mut rows_at_max_cursor = 0u64;
    let mut penultimate_cursor: Option<String> = None;
    // Snapshot of messages.len() at the moment max_cursor last changed.
    // All messages before this index have timestamps < max_cursor (safe to emit
    // on a full mixed-timestamp batch without risking loss at max_cursor).
    let mut safe_message_count = 0usize;
    for row in rows.iter() {
        // DB-absolute position: offset into the full result set for this cursor value.
        // Using batch-relative position (messages.len() alone) would assign the same ID
        // to different rows when OFFSET regresses after a serialization failure.
        let db_pos = row_offset_base as u128 + messages.len() as u128;
        let raw_cv = row
            .get(ctx.cursor_field)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::InvalidRecordValue(format!(
                    "Row missing '{}' cursor field — message ID would be non-deterministic \
                     on re-delivery, breaking deduplication. \
                     Ensure your query selects the cursor column.",
                    ctx.cursor_field
                ))
            })?;
        let (cv_owned, cv_dt) = normalize_v3_timestamp(raw_cv)?;
        // Stable ID using i128 arithmetic to avoid the i64 nanosecond overflow
        // that occurs for timestamps outside ~1678-2262 CE.
        let nanos_i128 =
            cv_dt.timestamp() as i128 * 1_000_000_000 + cv_dt.timestamp_subsec_nanos() as i128;
        let this_row_id = (nanos_i128 as u128).wrapping_add(db_pos);
        match max_cursor_parsed {
            Some(cur_dt) if cv_dt > cur_dt => {
                // New max: current max becomes penultimate. All messages pushed so
                // far are strictly before this new max timestamp.
                penultimate_cursor = max_cursor.take();
                max_cursor = Some(cv_owned.into_owned());
                max_cursor_parsed = Some(cv_dt);
                rows_at_max_cursor = 1;
                safe_message_count = messages.len();
            }
            Some(cur_dt) if cv_dt == cur_dt => {
                rows_at_max_cursor += 1;
            }
            Some(_) => {} // cv_dt < cur_dt: older row, no cursor update
            None => {
                max_cursor = Some(cv_owned.into_owned());
                max_cursor_parsed = Some(cv_dt);
                rows_at_max_cursor = 1;
            }
        }

        let payload = build_payload(
            row,
            ctx.payload_col,
            ctx.payload_format,
            ctx.include_metadata,
            ctx.cursor_field,
        )?;
        messages.push(ProducedMessage {
            id: Some(this_row_id),
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
        penultimate_cursor,
        safe_message_count,
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

    let base_batch = config.batch_size.unwrap_or(500).max(1);

    // NOTE: cap_factor must be computed BEFORE effective_batch so we can clamp the
    // persisted state value against the current config cap.
    let cap_factor = config
        .stuck_batch_cap_factor
        .unwrap_or(DEFAULT_STUCK_CAP_FACTOR);

    // FIX: clamp persisted inflated effective_batch_size against base_batch * cap_factor.
    //
    // If an operator lowers batch_size or stuck_batch_cap_factor between restarts,
    // a persisted inflated effective_batch_size can exceed the new cap. If we use
    // it directly, next_stuck_batch_size() may return None on the first stuck poll,
    // tripping the circuit breaker immediately with zero inflation attempt.
    //
    // cap_factor == 0 disables stuck detection; keep behavior consistent by not
    // clamping in that case.
    let effective_batch = if state.effective_batch_size == 0 {
        base_batch
    } else if cap_factor == 0 {
        state.effective_batch_size
    } else {
        state
            .effective_batch_size
            .min(base_batch.saturating_mul(cap_factor))
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

    let result = process_rows(&rows, &ctx, state.last_timestamp_row_offset)?;

    let schema = if ctx.payload_col.is_some() {
        ctx.payload_format.schema()
    } else {
        Schema::Json
    };

    let full_batch = rows.len() as u32 == effective_batch;
    let all_same_timestamp = result.rows_at_max_cursor >= effective_batch as u64;

    // Mixed full-batch: the batch is completely full but rows span more than one
    // timestamp. Advancing the cursor to max_cursor would silently discard any rows
    // at that timestamp that did not fit in this batch. Instead, emit only the rows
    // whose timestamps are strictly less than max_cursor (safe_message_count) and
    // advance the cursor to penultimate_cursor so the next poll re-fetches all rows
    // at max_cursor cleanly via WHERE time > penultimate_cursor.
    //
    // This path fires only when cap_factor > 0, because cap_factor == 0 disables
    // both stuck detection and this guard (consistent with open() allowing queries
    // without $offset when cap_factor == 0).
    if cap_factor > 0
        && full_batch
        && !all_same_timestamp
        && let Some(penultimate) = result.penultimate_cursor
    {
        let safe_count = result.safe_message_count;
        if safe_count == 0 {
            // Unreachable under the ORDER BY invariant: penultimate_cursor is only set
            // when a strictly-greater timestamp is observed, which requires at least one
            // prior message. If we reach here the result set violated ascending order
            // (e.g. a DESC sort slipped past validation). Trip the circuit breaker so
            // the operator sees a hard failure rather than silent data loss.
            return Err(Error::InvalidState);
        } else {
            let max_ts = result.max_cursor.as_deref().unwrap_or("unknown");
            warn!(
                "InfluxDB V3 source — full batch of {} rows has mixed timestamps; \
                     emitting {} safe rows and advancing cursor to {} \
                     (rows at {} deferred to next poll)",
                rows.len(),
                safe_count,
                penultimate,
                max_ts,
            );
            let mut messages = result.messages;
            messages.truncate(safe_count);
            let msg_count = messages.len() as u64;
            return Ok(PollResult {
                messages,
                new_state: V3State {
                    last_timestamp: Some(penultimate),
                    processed_rows: state.processed_rows + msg_count,
                    effective_batch_size: base_batch,
                    last_timestamp_row_offset: 0,
                    stuck_cursor: None,
                },
                schema,
                trip_circuit_breaker: false,
                is_stuck: false,
            });
        }
    }

    // Stuck-timestamp detection: when the batch fills up entirely with rows all sharing
    // the same max timestamp (rows_at_max_cursor >= effective_batch), there may be
    // more rows at that timestamp beyond the LIMIT. The cursor cannot safely advance
    // to that timestamp until all tied rows have been seen.
    // A partial batch (rows.len() < effective_batch) means the server returned all
    // available rows — the cursor can advance regardless of how many share the max timestamp.
    // cap_factor == 0 disables detection entirely: cursor advances even on a full same-timestamp
    // batch (consistent with open() skipping the $offset requirement when cap_factor == 0).
    let stuck = cap_factor > 0 && all_same_timestamp;

    if stuck {
        return match next_stuck_batch_size(effective_batch, base_batch, cap_factor) {
            Some(next_batch) => {
                warn!(
                    "InfluxDB V3 source — all {} rows share timestamp {}; \
                     inflating batch size {} → {} (cap={}×{}={})",
                    rows.len(),
                    result.max_cursor.as_deref().unwrap_or("unknown"),
                    effective_batch,
                    next_batch,
                    cap_factor,
                    base_batch,
                    base_batch.saturating_mul(cap_factor)
                );
                let msg_count = result.messages.len() as u64;
                Ok(PollResult {
                    messages: result.messages,
                    new_state: V3State {
                        last_timestamp: state.last_timestamp.clone(),
                        processed_rows: state.processed_rows + msg_count,
                        effective_batch_size: next_batch,
                        // Accumulate across consecutive stuck polls: each poll sees a superset of previous
                        // rows (inflated batch), so the total already-seen count must grow cumulatively.
                        last_timestamp_row_offset: state
                            .last_timestamp_row_offset
                            .saturating_add(result.rows_at_max_cursor),
                        // Remember the stuck timestamp so an empty follow-up poll can
                        // advance the cursor to it rather than looping back to T0.
                        stuck_cursor: result.max_cursor,
                    },
                    schema,
                    trip_circuit_breaker: false,
                    is_stuck: true,
                })
            }
            None => {
                warn!(
                    "InfluxDB V3 source — stuck-timestamp cap reached at batch size {effective_batch}; \
                     tripping circuit breaker to prevent an infinite loop"
                );
                // Reset effective_batch_size to base so the next poll after the
                // circuit-breaker cool-down restarts from the configured batch size
                // rather than re-entering at cap and immediately re-tripping.
                // Do NOT update last_timestamp_row_offset on the trip path — no
                // messages were emitted, so the offset tiebreaker is unchanged.
                Ok(PollResult {
                    messages: vec![],
                    new_state: V3State {
                        last_timestamp: state.last_timestamp.clone(),
                        processed_rows: state.processed_rows,
                        effective_batch_size: base_batch,
                        last_timestamp_row_offset: state.last_timestamp_row_offset,
                        // Preserve the stuck timestamp so an empty follow-up poll
                        // after CB cooldown can advance the cursor past this point
                        // instead of staying at last_timestamp forever.
                        stuck_cursor: result.max_cursor,
                    },
                    schema,
                    trip_circuit_breaker: true,
                    is_stuck: false,
                })
            }
        };
    }

    let old_dt = state.last_timestamp.as_deref().and_then(|s| {
        DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    // Cursor did not advance: preserve stuck-sequence progress so the
    // livelock-resolution arm can fire on the next empty-batch poll.
    if let (Some(new), Some(_)) = (
        result.max_cursor.as_deref(),
        state.last_timestamp.as_deref(),
    ) && !old_dt.is_some_and(|dt| is_timestamp_after(new, dt))
    {
        warn!("V3 source: max_cursor did not advance past saved cursor; keeping old value");
        // Mid-stuck-sequence: preserve the inflated batch so OFFSET pagination can
        // complete; treat as neutral (is_stuck=true skips both CB success and failure).
        //
        // Pure clock regression (no stuck_cursor): cursor cannot advance and the
        // same rows would be re-delivered on every subsequent poll. Suppress message
        // delivery and record as CB failure so repeated regressions eventually trip
        // the circuit breaker and halt the re-delivery loop.
        let (messages, effective_batch_size, trip_circuit_breaker, is_stuck) =
            if state.stuck_cursor.is_some() {
                (result.messages, state.effective_batch_size, false, true)
            } else {
                (vec![], base_batch, true, false)
            };
        // Count only actually emitted messages — on the pure-regression path
        // messages is vec![], so processed_rows must not include the suppressed rows.
        let processed_rows = state.processed_rows + messages.len() as u64;
        return Ok(PollResult {
            messages,
            new_state: V3State {
                last_timestamp: state.last_timestamp.clone(),
                processed_rows,
                effective_batch_size,
                last_timestamp_row_offset: state.last_timestamp_row_offset,
                stuck_cursor: state.stuck_cursor.clone(),
            },
            schema,
            trip_circuit_breaker,
            is_stuck,
        });
    }

    let processed_rows = state.processed_rows + result.messages.len() as u64;

    let advanced_cursor = match (
        result.max_cursor.as_deref(),
        state.last_timestamp.as_deref(),
    ) {
        (Some(_), _) => result.max_cursor,
        // Empty batch after stuck sequence: advance cursor to stuck_cursor.
        //
        // A zero-row response at the accumulated OFFSET is treated as confirmation
        // that all rows at stuck_cursor have been delivered. This is safe under the
        // assumption that InfluxDB receives writes in monotonically increasing time
        // order (the typical time-series pattern). If out-of-order or backdated
        // writes are possible in your workload, a concurrent writer could insert
        // rows at stuck_cursor AFTER this empty-batch poll, and those rows would
        // be silently skipped because the cursor advances past them here. In that
        // case, use stuck_batch_cap_factor = 0 to disable stuck detection and
        // accept potential re-delivery instead.
        _ if state.last_timestamp_row_offset > 0 && state.stuck_cursor.is_some() => {
            warn!(
                "Advancing cursor past stuck_cursor={:?} on empty follow-up batch. \
                 Any backdated writes at this timestamp inserted after this poll \
                 will be silently skipped. Set stuck_batch_cap_factor=0 to disable \
                 stuck detection if your workload has out-of-order ingestion.",
                state.stuck_cursor.as_deref()
            );
            state.stuck_cursor.clone()
        }
        _ => state.last_timestamp.clone(),
    };

    let new_state = V3State {
        last_timestamp: advanced_cursor,
        processed_rows,
        effective_batch_size: base_batch,
        // Reset to 0: new cursor excludes old rows via strict >; non-zero OFFSET
        // would skip valid rows on the next poll.
        last_timestamp_row_offset: 0,
        stuck_cursor: None,
    };

    Ok(PollResult {
        messages: result.messages,
        new_state,
        schema,
        trip_circuit_breaker: false,
        is_stuck: false,
    })
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
        let result = process_rows(&[], &ctx(T1, 1000), 0).unwrap();
        assert!(result.messages.is_empty());
        assert!(result.max_cursor.is_none());
        assert_eq!(
            result.rows_at_max_cursor, 0,
            "empty slice has no rows at max cursor"
        );
    }

    #[test]
    fn process_rows_single_row_advances_cursor() {
        let rows = vec![row(&[("time", T1), ("val", "1")])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
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
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
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
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.max_cursor.as_deref(), Some(T2));
    }

    #[test]
    fn process_rows_row_without_cursor_field_returns_error() {
        // A batch where no row has the cursor column must return Err rather than
        // silently re-delivering the same rows on every poll.
        let rows = vec![row(&[("val", "1")])]; // no "time" field
        let err = process_rows(&rows, &ctx(T1, 1000), 0).unwrap_err();
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
        let err = process_rows(&rows, &ctx(T1, 1000), 0).unwrap_err();
        assert!(
            matches!(err, Error::InvalidRecordValue(_)),
            "expected InvalidRecordValue when cursor column is absent, got {err:?}"
        );
    }

    #[test]
    fn process_rows_message_ids_are_some_and_unique() {
        let rows = vec![row(&[("time", T1)]), row(&[("time", T2)])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert!(result.messages[0].id.is_some());
        assert!(result.messages[1].id.is_some());
        assert_ne!(result.messages[0].id, result.messages[1].id);
    }

    #[test]
    fn process_rows_message_timestamps_use_now_micros() {
        let rows = vec![row(&[("time", T1)])];
        let result = process_rows(&rows, &ctx(T1, 888_888), 0).unwrap();
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
            0,
        )
        .unwrap();
        assert_eq!(result.messages.len(), 1);
    }

    // ── rows_at_max_cursor / stuck-batch ─────────────────────────────────────

    #[test]
    fn process_rows_rows_at_max_cursor_counts_rows_sharing_max_timestamp() {
        // Two rows at T1: both equal max_cursor → rows_at_max_cursor = 2.
        let rows = vec![row(&[("time", T1)]), row(&[("time", T1)])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.rows_at_max_cursor, 2);
    }

    #[test]
    fn process_rows_rows_at_max_cursor_resets_when_cursor_advances() {
        // T1 then T2: max_cursor = T2, only 1 row at T2 → rows_at_max_cursor = 1.
        let rows = vec![row(&[("time", T1)]), row(&[("time", T2)])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.rows_at_max_cursor, 1);
        assert_eq!(result.max_cursor.as_deref(), Some(T2));
    }

    #[test]
    fn process_rows_rows_at_max_cursor_zero_for_empty_slice() {
        let result = process_rows(&[], &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.rows_at_max_cursor, 0);
    }

    #[test]
    fn process_rows_penultimate_cursor_set_on_mixed_timestamps() {
        // [T1, T2]: T1 becomes penultimate when T2 becomes the new max.
        let rows = vec![row(&[("time", T1)]), row(&[("time", T2)])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.penultimate_cursor.as_deref(), Some(T1));
        assert_eq!(result.safe_message_count, 1);
        assert_eq!(result.max_cursor.as_deref(), Some(T2));
    }

    #[test]
    fn process_rows_penultimate_cursor_none_for_single_timestamp() {
        // All rows share one timestamp — penultimate does not exist.
        let rows = vec![row(&[("time", T1)]), row(&[("time", T1)])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert!(result.penultimate_cursor.is_none());
        assert_eq!(result.safe_message_count, 0);
    }

    #[test]
    fn process_rows_penultimate_tracks_second_highest_across_three_timestamps() {
        // [T1, T2, T3]: penultimate should be T2 (the one just before T3).
        let rows = vec![
            row(&[("time", T1)]),
            row(&[("time", T2)]),
            row(&[("time", T3)]),
        ];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.penultimate_cursor.as_deref(), Some(T2));
        assert_eq!(result.safe_message_count, 2); // T1 + T2 rows safe
        assert_eq!(result.max_cursor.as_deref(), Some(T3));
    }

    #[test]
    fn process_rows_penultimate_cursor_none_for_empty_slice() {
        let result = process_rows(&[], &ctx(T1, 1000), 0).unwrap();
        assert!(result.penultimate_cursor.is_none());
        assert_eq!(result.safe_message_count, 0);
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
            normalize_v3_timestamp("2024-01-01T00:00:00.123Z")
                .unwrap()
                .0,
            "2024-01-01T00:00:00.123Z"
        );
        // Second-precision with Z is also ≤ms, returned unchanged.
        assert_eq!(
            normalize_v3_timestamp("2024-01-01T00:00:00Z").unwrap().0,
            "2024-01-01T00:00:00Z"
        );
    }

    #[test]
    fn normalize_no_tz_nanoseconds_appends_z_only() {
        // InfluxDB 3 Core returns timestamps like this — 9 fractional digits, no Z.
        // Full nanosecond precision must be preserved (not truncated to ms).
        let (result, _) = normalize_v3_timestamp("2026-04-26T02:32:20.526360865").unwrap();
        assert_eq!(result, "2026-04-26T02:32:20.526360865Z");
    }

    #[test]
    fn normalize_no_tz_milliseconds_appends_z() {
        // No timezone suffix, ms precision — just append Z.
        let (result, _) = normalize_v3_timestamp("2026-04-26T02:32:20.526").unwrap();
        assert_eq!(result, "2026-04-26T02:32:20.526Z");
    }

    #[test]
    fn normalize_rfc3339_sub_ms_precision_returned_unchanged() {
        // Already valid RFC 3339 with Z and nanoseconds — returned as-is.
        let (result, _) = normalize_v3_timestamp("2026-04-26T02:32:20.526360865Z").unwrap();
        assert_eq!(result, "2026-04-26T02:32:20.526360865Z");
    }

    #[test]
    fn normalize_invalid_returns_err() {
        // Garbage input that is not valid RFC 3339 even after appending Z must return Err.
        assert!(normalize_v3_timestamp("not-a-timestamp").is_err());
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
        let result = process_rows(&rows, &c, 0).expect("should not fail on bare timestamps");
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
        let result = process_rows(&rows, &c, 0).expect("should succeed");
        // max_cursor must be the latest nanosecond timestamp (row 3), not a truncated ms.
        assert_eq!(
            result.max_cursor.as_deref(),
            Some("2026-04-26T02:32:20.526362000Z")
        );
        // Only row 3 is at max_cursor.
        assert_eq!(result.rows_at_max_cursor, 1);
    }

    #[test]
    fn process_rows_message_ids_stable_across_repoll() {
        // The same rows must produce identical message IDs on every call to process_rows.
        // Previously random UUIDs meant IDs changed on every re-poll, breaking dedup.
        let rows = vec![
            row(&[("time", T1), ("val", "a")]),
            row(&[("time", T2), ("val", "b")]),
        ];
        let c = ctx(T1, 1000);
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
    fn process_rows_rows_with_same_timestamp_get_distinct_stable_ids() {
        // Two rows sharing a timestamp must have different IDs (disambiguated by position).
        let rows = vec![
            row(&[("time", T1), ("val", "first")]),
            row(&[("time", T1), ("val", "second")]),
        ];
        let c = ctx("1970-01-01T00:00:00Z", 0);
        let result = process_rows(&rows, &c, 0).unwrap();
        assert_ne!(
            result.messages[0].id, result.messages[1].id,
            "two rows at the same timestamp must have distinct IDs"
        );
        // IDs must also be stable across re-polls
        let result2 = process_rows(&rows, &c, 0).unwrap();
        assert_eq!(result.messages[0].id, result2.messages[0].id);
        assert_eq!(result.messages[1].id, result2.messages[1].id);
    }

    #[test]
    fn process_rows_offset_base_prevents_id_collision_across_batches() {
        // Regression: when OFFSET regresses (e.g. a stuck-batch state is persisted but
        // the follow-up non-stuck state is not), a later re-poll uses a different offset.
        // Without row_offset_base, different physical rows at the same timestamp get the
        // same ID = nanos + batch_local_position. With row_offset_base, each row's ID
        // uses its DB-absolute position, so IDs remain distinct across batches.
        //
        // Simulate: batch A (offset=0) returns rows [R0, R1] at T1.
        //           batch B (offset=2) returns rows [R2, R3] at T1.
        // R0 and R2 are different rows but both appear at batch position 0.
        let rows_a = vec![
            row(&[("time", T1), ("val", "R0")]),
            row(&[("time", T1), ("val", "R1")]),
        ];
        let rows_b = vec![
            row(&[("time", T1), ("val", "R2")]),
            row(&[("time", T1), ("val", "R3")]),
        ];
        let c = ctx("1970-01-01T00:00:00Z", 0);

        let result_a = process_rows(&rows_a, &c, 0).unwrap();
        let result_b = process_rows(&rows_b, &c, 2).unwrap();

        // R0 (offset=0, pos=0) and R2 (offset=2, pos=0) must have different IDs.
        assert_ne!(
            result_a.messages[0].id, result_b.messages[0].id,
            "R0 and R2 share batch position 0 but differ by offset — must not collide"
        );
        // R1 (offset=0, pos=1) and R3 (offset=2, pos=1) must also not collide.
        assert_ne!(
            result_a.messages[1].id, result_b.messages[1].id,
            "R1 and R3 share batch position 1 but differ by offset — must not collide"
        );
        // IDs within each batch remain distinct.
        assert_ne!(result_a.messages[0].id, result_a.messages[1].id);
        assert_ne!(result_b.messages[0].id, result_b.messages[1].id);
    }

    // ── build_payload with payload_column ────────────────────────────────────

    #[test]
    fn process_rows_payload_column_json_format_serializes_value() {
        // payload_column + Json: the typed serde_json::Value is serialised directly.
        let rows = vec![row(&[("time", T1), ("data", r#"{"k":1}"#)])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("data"),
                payload_format: PayloadFormat::Json,
                now_micros: 0,
            },
            0,
        )
        .unwrap();
        assert_eq!(result.messages.len(), 1);
        // V3 stores row values as serde_json::Value::String for string columns.
        // The payload must contain the raw string representation.
        assert!(!result.messages[0].payload.is_empty());
    }

    #[test]
    fn process_rows_payload_column_raw_decodes_base64() {
        // payload_column + Raw: base64-encoded string value is decoded to bytes.
        use base64::{Engine as _, engine::general_purpose};
        let encoded = general_purpose::STANDARD.encode(b"raw-bytes");
        let rows = vec![row(&[("time", T1), ("blob", &encoded)])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("blob"),
                payload_format: PayloadFormat::Raw,
                now_micros: 0,
            },
            0,
        )
        .unwrap();
        assert_eq!(result.messages[0].payload, b"raw-bytes");
    }

    #[test]
    fn process_rows_payload_column_raw_non_string_value_returns_error() {
        // Raw format requires the column value to be a string (base64-encoded).
        // A non-string JSON value (e.g. a number) must return Err.
        use crate::common::Row;
        let mut row: Row = Row::default();
        row.insert(
            Arc::<str>::from("time"),
            serde_json::Value::String(T1.to_string()),
        );
        // Insert a numeric value for the payload column — not a base64 string.
        row.insert(
            Arc::<str>::from("blob"),
            serde_json::Value::Number(42.into()),
        );
        let err = process_rows(
            &[row],
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("blob"),
                payload_format: PayloadFormat::Raw,
                now_micros: 0,
            },
            0,
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::InvalidRecordValue(_)),
            "non-string value for Raw format must return InvalidRecordValue: {err:?}"
        );
    }

    #[test]
    fn process_rows_payload_column_raw_invalid_base64_returns_error() {
        let rows = vec![row(&[("time", T1), ("blob", "!!!invalid!!!")])];
        let err = process_rows(
            &rows,
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("blob"),
                payload_format: PayloadFormat::Raw,
                now_micros: 0,
            },
            0,
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidRecordValue(_)));
    }

    #[test]
    fn process_rows_missing_payload_column_returns_error() {
        // If the specified payload_column is absent from the row, return Err.
        let rows = vec![row(&[("time", T1), ("other", "value")])];
        let err = process_rows(
            &rows,
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("missing_col"),
                payload_format: PayloadFormat::Json,
                now_micros: 0,
            },
            0,
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidRecordValue(_)));
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
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset".to_string(),
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
            stuck_cursor: None,
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
        assert_eq!(
            result.messages.len(),
            10,
            "stuck batch must emit rows, not discard them"
        );
        assert_eq!(result.new_state.effective_batch_size, 20, "should double");
        assert!(!result.trip_circuit_breaker);
        assert!(result.is_stuck, "inflating batch must signal is_stuck");
        // Cursor must not change
        assert_eq!(result.new_state.last_timestamp.as_deref(), Some(t));
    }

    #[tokio::test]
    async fn poll_trips_circuit_breaker_when_stuck_cap_reached() {
        // cap_factor=2 → cap = batch_size × 2 = 20.
        // effective_batch_size is already 20 (= cap) → next_stuck_batch_size returns None → CB trips.
        // cap_factor=1 is rejected by open(); minimum valid cap that causes an immediate trip
        // at the start of inflation is cap=2 with effective_batch already at cap.
        let t = "2024-01-01T00:00:00Z";
        let jsonl: String = (0..20)
            .map(|i| format!("{{\"time\":\"{t}\",\"val\":{i}}}\n"))
            .collect();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let config = V3SourceConfig {
            stuck_batch_cap_factor: Some(2),
            ..make_config(&base)
        };
        let state = V3State {
            last_timestamp: Some(t.to_string()),
            effective_batch_size: 20,
            processed_rows: 0,
            last_timestamp_row_offset: 0,
            stuck_cursor: None,
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
    async fn poll_small_batch_all_same_timestamp_is_not_stuck() {
        // Regression: a batch smaller than batch_size where all rows share the
        // same timestamp must NOT trigger stuck detection. Previously the second
        // `||` condition in the stuck check caused data loss — no messages were
        // emitted, the offset advanced past those rows, and they were permanently
        // lost on restart.
        let t2 = "2024-01-01T00:00:01Z";
        let jsonl = format!(
            "{{\"time\":\"{t2}\",\"val\":0}}\n\
             {{\"time\":\"{t2}\",\"val\":1}}\n\
             {{\"time\":\"{t2}\",\"val\":2}}\n"
        );
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        // make_config has batch_size=10 but only 3 rows are returned.
        let state = V3State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            effective_batch_size: 10,
            last_timestamp_row_offset: 0,
            processed_rows: 0,
            stuck_cursor: None,
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
        assert_eq!(result.messages.len(), 3, "all 3 rows must be emitted");
        assert!(!result.trip_circuit_breaker);
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some(t2),
            "cursor must advance to t2"
        );
        assert_eq!(
            result.new_state.last_timestamp_row_offset, 0,
            "offset must reset to 0 after cursor advance"
        );
    }

    #[tokio::test]
    async fn poll_zero_cap_factor_full_batch_advances_cursor() {
        // stuck_batch_cap_factor=0 disables stuck detection: a full batch where every
        // row shares the same timestamp must still advance the cursor and deliver messages,
        // not trip the circuit breaker.
        let t1 = "2024-01-01T00:00:01Z";
        // Return exactly batch_size=10 rows, all at t1 — this would normally trigger stuck.
        let jsonl = (0..10)
            .map(|i| format!("{{\"time\":\"{t1}\",\"val\":{i}}}"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let mut cfg = make_config(&base);
        cfg.stuck_batch_cap_factor = Some(0);
        let state = V3State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            effective_batch_size: 10,
            last_timestamp_row_offset: 0,
            processed_rows: 0,
            stuck_cursor: None,
        };
        let result = poll(
            &make_client(),
            &cfg,
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 10, "all 10 rows must be emitted");
        assert!(
            !result.trip_circuit_breaker,
            "CB must not trip when cap_factor=0"
        );
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some(t1),
            "cursor must advance to t1"
        );
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

    #[tokio::test]
    async fn poll_non_stuck_advance_resets_row_offset_to_zero() {
        // Bug #1: when the cursor advances, last_timestamp_row_offset must reset to 0.
        // A non-zero offset on the next poll skips valid new rows — silent data loss.
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"v\":1}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"v\":2}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        // Start with a non-zero offset from a previous stuck batch.
        let state = V3State {
            last_timestamp: Some("1970-01-01T00:00:00Z".to_string()),
            last_timestamp_row_offset: 7, // leftover from previous stuck run
            stuck_cursor: Some("2024-01-01T00:00:01Z".to_string()), // simulate prior stuck
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
        // Cursor advanced: offset must reset to 0, not carry forward the old 7.
        assert_eq!(
            result.new_state.last_timestamp_row_offset, 0,
            "offset must reset to 0 after cursor advances"
        );
        assert_eq!(result.messages.len(), 2);
    }

    #[tokio::test]
    async fn poll_stuck_first_poll_sets_offset_and_inflation_resolves_stuck() {
        // First stuck poll (rows_at_max_cursor >= effective_batch) must set the row
        // offset and double the batch. On the next poll the server returns fewer rows
        // than the inflated limit at a NEWER timestamp — cursor advances and offset
        // resets to 0.
        let t0 = "2024-01-01T00:00:00Z";
        let t1 = "2024-01-01T00:00:01Z"; // strictly after t0
        let jsonl_stuck: String = (0..10)
            .map(|i| format!("{{\"time\":\"{t0}\",\"val\":{i}}}\n"))
            .collect();
        let jsonl_advance: String = (0..5)
            .map(|i| format!("{{\"time\":\"{t1}\",\"val\":{i}}}\n"))
            .collect();

        // First poll server: returns 10 rows at t0 (triggers stuck).
        let app1 = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl_stuck) }),
        );
        let base1 = start_server(app1).await;

        let state1 = V3State {
            last_timestamp: Some(t0.to_string()),
            effective_batch_size: 10,
            last_timestamp_row_offset: 0,
            processed_rows: 0,
            stuck_cursor: None,
        };
        let r1 = poll(
            &make_client(),
            &make_config(&base1),
            "Bearer tok",
            &state1,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(r1.messages.len(), 10, "first stuck poll must emit messages");
        assert!(
            r1.new_state.last_timestamp_row_offset > 0,
            "first stuck poll must set offset > 0"
        );
        assert_eq!(
            r1.new_state.effective_batch_size, 20,
            "batch must double on stuck"
        );

        // Second poll server: returns 5 rows at t1 > t0 (< effective_batch 20, not stuck).
        // Cursor advances to t1, offset resets to 0.
        let app2 = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl_advance) }),
        );
        let base2 = start_server(app2).await;
        let r2 = poll(
            &make_client(),
            &make_config(&base2),
            "Bearer tok",
            &r1.new_state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(r2.messages.len(), 5, "advancing poll must emit messages");
        assert_eq!(
            r2.new_state.last_timestamp_row_offset, 0,
            "offset must reset to 0 after cursor advances"
        );
        assert_eq!(
            r2.new_state.last_timestamp.as_deref(),
            Some(t1),
            "cursor must advance to t1"
        );
    }

    #[tokio::test]
    async fn poll_cursor_does_not_advance_when_new_is_not_after_saved() {
        // When the DB returns rows whose max timestamp equals the saved cursor
        // (which can happen if the clock skews or the same data is replayed),
        // the cursor must not regress and a warn is logged. The state cursor
        // value must be kept unchanged.
        let saved_ts = "2024-01-01T00:00:02Z"; // cursor already at T3
        // Server returns rows whose max timestamp == saved cursor (same, not newer).
        let jsonl = format!(
            "{{\"time\":\"{saved_ts}\",\"val\":1}}\n\
             {{\"time\":\"{saved_ts}\",\"val\":2}}\n"
        );
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some(saved_ts.to_string()),
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
        // Cursor must not move backwards: kept at saved value.
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some(saved_ts),
            "cursor must not regress when new max == saved cursor"
        );
    }

    #[tokio::test]
    async fn poll_cursor_non_advancing_preserves_stuck_sequence_state() {
        // When cursor doesn't advance mid-stuck-sequence, last_timestamp_row_offset
        // and stuck_cursor must be preserved - resetting them would re-deliver the
        // already-seen head rows and break livelock resolution on the next empty poll.
        let t0 = "2024-01-01T00:00:00Z";
        let t1 = "2024-01-01T00:00:01Z"; // stuck_cursor value from prior stuck polls
        let jsonl = format!(
            "{{\"time\":\"{t0}\",\"val\":1}}\n\
             {{\"time\":\"{t0}\",\"val\":2}}\n"
        );
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some(t0.to_string()),
            effective_batch_size: 20,
            last_timestamp_row_offset: 10,
            stuck_cursor: Some(t1.to_string()),
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
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some(t0),
            "cursor must stay at t0"
        );
        assert_eq!(
            result.new_state.last_timestamp_row_offset, 10,
            "offset must be preserved, not reset to 0"
        );
        assert_eq!(
            result.new_state.stuck_cursor.as_deref(),
            Some(t1),
            "stuck_cursor must be preserved for livelock resolution"
        );
        assert_eq!(
            result.new_state.effective_batch_size, 20,
            "inflated batch size must be preserved"
        );
    }

    // ── Bug regression tests ─────────────────────────────────────────────────

    /// Bug 1: stuck path was silently dropping all rows that triggered the stuck
    /// detection. The rows must be emitted; only cursor advancement is withheld.
    #[tokio::test]
    async fn poll_stuck_batch_emits_messages() {
        let t0 = "2024-01-01T00:00:00Z";
        let t1 = "2024-01-01T00:00:01Z"; // strictly after cursor
        let jsonl: String = (0..10)
            .map(|i| format!("{{\"time\":\"{t1}\",\"val\":{i}}}\n"))
            .collect();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some(t0.to_string()),
            effective_batch_size: 10,
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
        assert_eq!(
            result.messages.len(),
            10,
            "stuck batch must emit rows, not discard them"
        );
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some(t0),
            "cursor must not advance when stuck"
        );
        assert_eq!(
            result.new_state.effective_batch_size, 20,
            "batch must double"
        );
        assert!(!result.trip_circuit_breaker);
    }

    /// Bug 2: when exactly batch_size rows exist at T1, poll 1 is stuck and poll 2
    /// returns 0 rows at the inflated offset. Without the fix this livelocks because
    /// the cursor never advances past T0. The cursor must reach T1 after poll 2.
    #[tokio::test]
    async fn poll_livelock_resolved_when_empty_batch_follows_stuck() {
        let t0 = "2024-01-01T00:00:00Z";
        let t1 = "2024-01-01T00:00:01Z";
        let jsonl_t1: String = (0..10)
            .map(|i| format!("{{\"time\":\"{t1}\",\"val\":{i}}}\n"))
            .collect();
        let cc = Arc::new(Mutex::new(0u32));
        let cc2 = cc.clone();
        let rows2 = jsonl_t1.clone();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || {
                let counter = cc2.clone();
                let rows = rows2.clone();
                async move {
                    let mut n = counter.lock().await;
                    *n += 1;
                    if *n == 1 {
                        (StatusCode::OK, rows)
                    } else {
                        (StatusCode::OK, String::new())
                    }
                }
            }),
        );
        let base = start_server(app).await;
        let state1 = V3State {
            last_timestamp: Some(t0.to_string()),
            effective_batch_size: 10,
            ..V3State::default()
        };
        // Poll 1: 10 rows at T1 → stuck
        let r1 = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state1,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            r1.new_state.last_timestamp_row_offset, 10,
            "offset must accumulate after stuck poll"
        );
        // Poll 2: 0 rows at OFFSET=10 → cursor must advance to T1
        let r2 = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &r1.new_state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            r2.new_state.last_timestamp.as_deref(),
            Some(t1),
            "cursor must advance to T1 after empty follow-up confirms all rows at T1 seen; livelock not allowed"
        );
        assert_eq!(
            r2.new_state.last_timestamp_row_offset, 0,
            "offset must reset after cursor advance"
        );
        assert_eq!(
            r2.new_state.effective_batch_size, 10,
            "batch must reset to base"
        );
    }

    #[tokio::test]
    async fn poll_cb_trip_preserves_stuck_cursor() {
        // When CB trips (batch already at cap), stuck_cursor must be set to the
        // stuck timestamp so after cooldown an empty poll can advance past it.
        // Without this fix stuck_cursor=None causes the cursor to freeze after CB cooldown.
        let t0 = "2024-01-01T00:00:00Z"; // last saved cursor
        let t1 = "2024-01-01T00:00:01Z"; // stuck timestamp (strictly after t0)
        // cap_factor=2, batch_size=10 → cap=20. effective_batch=20 (= cap).
        // Return 20 rows all at t1: rows_at_max_cursor(20) >= effective_batch(20) → stuck.
        // next_stuck_batch_size(20, 10, 2) → 20 >= 20 → None → CB trips.
        let jsonl: String = (0..20)
            .map(|i| format!("{{\"time\":\"{t1}\",\"val\":{i}}}\n"))
            .collect();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let config = V3SourceConfig {
            stuck_batch_cap_factor: Some(2),
            ..make_config(&base)
        };
        let state = V3State {
            last_timestamp: Some(t0.to_string()),
            effective_batch_size: 20,
            last_timestamp_row_offset: 10,
            processed_rows: 10,
            stuck_cursor: None,
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
        assert!(result.trip_circuit_breaker, "must trip CB when at cap");
        assert!(result.messages.is_empty());
        assert_eq!(
            result.new_state.stuck_cursor.as_deref(),
            Some(t1),
            "stuck_cursor must be the stuck timestamp so cooldown poll can advance past it"
        );
        // CB resets batch to base
        assert_eq!(
            result.new_state.effective_batch_size, 10,
            "CB trip must reset batch to base"
        );
    }

    #[tokio::test]
    async fn poll_v3_zero_batch_size_is_floored_to_one() {
        // batch_size=Some(0) must floor to 1 inside poll().
        // Without .max(1): base_batch=0, cap=0, next_stuck_batch_size(0,0,cap)=None → CB trips.
        // With .max(1): base_batch=1, cap=cap_factor×1=10; 1 row at new timestamp is
        // not stuck (rows_at_max_cursor=1 >= effective_batch=1 is true but inflation gives
        // Some(2) → no CB trip, message emitted).
        let t1 = "2024-01-01T00:00:01Z";
        let jsonl = format!("{{\"time\":\"{t1}\",\"val\":1}}\n");
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let mut config = make_config(&base);
        config.batch_size = Some(0);
        let state = V3State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            effective_batch_size: 0,
            ..V3State::default()
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
        assert!(
            !result.trip_circuit_breaker,
            "batch_size=0 must floor to 1, not trip CB immediately"
        );
        assert_eq!(result.messages.len(), 1, "row must be emitted");
    }

    #[tokio::test]
    async fn poll_with_payload_column_returns_raw_schema() {
        // When payload_column is set the schema returned must match the format,
        // not always Json. This covers the payload_format.schema() path at v3.rs:570.
        // Two rows with distinct timestamps ensure the batch is not stuck
        // (rows_at_max_cursor < effective_batch) so messages are emitted.
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"data\":\"aGVsbG8=\"}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"data\":\"d29ybGQ=\"}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let config = V3SourceConfig {
            payload_column: Some("data".to_string()),
            ..make_config(&base)
        };
        let state = V3State::default();
        let result = poll(
            &make_client(),
            &config,
            "Bearer tok",
            &state,
            PayloadFormat::Raw,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 2, "both rows must produce messages");
        // Schema must reflect the payload format, not the default Json.
        assert_eq!(result.schema, Schema::Raw);
        // First row payload must be the decoded bytes of "hello" (base64 "aGVsbG8=").
        assert_eq!(&result.messages[0].payload, b"hello");
        // Second row payload must be the decoded bytes of "world" (base64 "d29ybGQ=").
        assert_eq!(&result.messages[1].payload, b"world");
    }

    // ── NEW regression test: clamp persisted effective_batch_size on read ─────

    #[tokio::test]
    async fn poll_clamps_persisted_effective_batch_size_to_new_cap() {
        // Persisted state can contain an inflated effective_batch_size from earlier stuck polling.
        // If the operator lowers batch_size or stuck_batch_cap_factor between restarts,
        // the new cap can be lower than the persisted effective_batch_size.
        //
        // Without clamping, next_stuck_batch_size() sees current >= cap and returns None
        // on the first stuck poll, tripping the circuit breaker immediately.
        //
        // This test captures the request body and checks the substituted SQL query uses
        // LIMIT = base_batch * cap_factor rather than the unbounded persisted value.
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
                    // Return empty OK body so poll completes quickly.
                    (StatusCode::OK, "")
                }
            }),
        );
        let base = start_server(app).await;

        let mut cfg = make_config(&base);
        cfg.batch_size = Some(10);
        cfg.stuck_batch_cap_factor = Some(2); // cap = 20

        // Persisted inflated value from a previous run.
        let state = V3State {
            last_timestamp: Some("1970-01-01T00:00:00Z".to_string()),
            effective_batch_size: 5000,
            last_timestamp_row_offset: 0,
            processed_rows: 0,
            stuck_cursor: None,
        };

        let _ = poll(
            &make_client(),
            &cfg,
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();

        let body = captured_body.lock().await;
        assert!(
            body.contains("LIMIT 20") || body.contains("limit 20"),
            "expected outbound query to be clamped to LIMIT 20, got body: {body}"
        );
        assert!(
            !body.contains("LIMIT 5000") && !body.contains("limit 5000"),
            "must not send un-clamped LIMIT 5000, got body: {body}"
        );
    }
}
