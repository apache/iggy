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
    PayloadFormat, Row, V3SourceConfig, V3State, is_timestamp_after, parse_jsonl_rows, parse_scalar,
    validate_cursor,
};
use base64::{Engine as _, engine::general_purpose};
use iggy_connector_sdk::{Error, ProducedMessage, Schema};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde_json::json;
use tracing::warn;
use uuid::Uuid;

const DEFAULT_STUCK_CAP_FACTOR: u32 = 10;

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

pub(crate) fn auth_header(token: &str) -> String {
    format!("Bearer {token}")
}

pub(crate) fn health_url(base: &str) -> Result<Url, Error> {
    Url::parse(&format!("{base}/health"))
        .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))
}

fn build_query_url(base: &str) -> Result<Url, Error> {
    Url::parse(&format!("{base}/api/v3/query_sql"))
        .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))
}

fn build_query_body(config: &V3SourceConfig, cursor: &str, effective_batch: u32) -> Result<serde_json::Value, Error> {
    validate_cursor(cursor)?;
    let mut q = config.query.clone();
    if q.contains("$cursor") {
        q = q.replace("$cursor", cursor);
    }
    if q.contains("$limit") {
        q = q.replace("$limit", &effective_batch.to_string());
    }
    Ok(json!({
        "db":     config.db,
        "q":      q,
        "format": "jsonl"
    }))
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

pub(crate) async fn run_query(
    client: &ClientWithMiddleware,
    config: &V3SourceConfig,
    auth: &str,
    cursor: &str,
    effective_batch: u32,
) -> Result<String, Error> {
    let base = config.url.trim_end_matches('/');
    let url = build_query_url(base)?;
    let body = build_query_body(config, cursor, effective_batch)?;

    let response = client
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
        return response
            .text()
            .await
            .map_err(|e| Error::Storage(format!("Failed to read V3 response: {e}")));
    }

    let body_text = response
        .text()
        .await
        .unwrap_or_else(|_| "failed to read response body".to_string());

    // 404 "database not found" means the namespace has not been written to yet;
    // treat it as empty rather than a failure so the circuit breaker stays healthy.
    if status.as_u16() == 404 && body_text.contains("database not found") {
        return Ok(String::new());
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

// ---------------------------------------------------------------------------
// Message building
// ---------------------------------------------------------------------------

fn build_payload(
    row: &Row,
    payload_column: Option<&str>,
    payload_format: PayloadFormat,
) -> Result<Vec<u8>, Error> {
    if let Some(col) = payload_column {
        let raw = row.get(col).cloned().ok_or_else(|| {
            Error::InvalidRecordValue(format!("Missing payload column '{col}'"))
        })?;
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

    // V3 rows are flat objects — emit them directly with all fields.
    let json_row: serde_json::Map<_, _> = row
        .iter()
        .map(|(k, v)| (k.clone(), parse_scalar(v)))
        .collect();
    serde_json::to_vec(&json_row)
        .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}")))
}

// ---------------------------------------------------------------------------
// Stuck-timestamp detection helpers
// ---------------------------------------------------------------------------

/// Returns `true` when every row in `rows` has `cursor_field == cursor`.
/// This means the batch is "stuck" — no rows have advanced beyond the current
/// timestamp, so we cannot move the cursor forward.
fn batch_is_stuck(rows: &[Row], cursor_field: &str, cursor: &str) -> bool {
    !rows.is_empty() && rows.iter().all(|r| r.get(cursor_field).map(String::as_str) == Some(cursor))
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

// ---------------------------------------------------------------------------
// Poll
// ---------------------------------------------------------------------------

pub(crate) struct PollResult {
    pub messages: Vec<ProducedMessage>,
    pub new_state: V3State,
    pub schema: Schema,
    /// Set to true when the stuck-timestamp cap was reached and the circuit
    /// breaker should be tripped by the caller.
    pub trip_circuit_breaker: bool,
}

pub(crate) async fn poll(
    client: &ClientWithMiddleware,
    config: &V3SourceConfig,
    auth: &str,
    state: &V3State,
    payload_format: PayloadFormat,
) -> Result<PollResult, Error> {
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

    let response_data = run_query(client, config, auth, &cursor, effective_batch).await?;
    let rows = parse_jsonl_rows(&response_data)?;

    let cursor_field = config.cursor_field.as_deref().unwrap_or("time");
    let payload_col = config.payload_column.as_deref();

    // Stuck-timestamp detection: if every row is at the current cursor
    // and the batch was full, inflate and request more next time.
    let cap_factor = config.stuck_batch_cap_factor.unwrap_or(DEFAULT_STUCK_CAP_FACTOR);
    let stuck = batch_is_stuck(&rows, cursor_field, &cursor)
        && rows.len() >= effective_batch as usize;

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
                    },
                    schema: Schema::Json,
                    trip_circuit_breaker: true,
                })
            }
        };
    }

    // Normal path: build messages, advance cursor.
    let mut messages = Vec::with_capacity(rows.len());
    let mut max_cursor: Option<String> = None;

    for row in &rows {
        if let Some(cv) = row.get(cursor_field) {
            match &max_cursor {
                None => max_cursor = Some(cv.clone()),
                Some(current) if is_timestamp_after(cv, current) => {
                    max_cursor = Some(cv.clone());
                }
                _ => {}
            }
        }

        let payload = build_payload(row, payload_col, payload_format)?;
        let now_micros = iggy_common::Utc::now().timestamp_micros() as u64;
        messages.push(ProducedMessage {
            id: Some(Uuid::new_v4().as_u128()),
            checksum: None,
            timestamp: Some(now_micros),
            origin_timestamp: Some(now_micros),
            headers: None,
            payload,
        });
    }

    let processed_rows = state.processed_rows + messages.len() as u64;
    let new_state = V3State {
        last_timestamp: max_cursor.or_else(|| state.last_timestamp.clone()),
        processed_rows,
        effective_batch_size: base_batch, // reset on successful advance
    };

    let schema = if payload_col.is_some() {
        payload_format.schema()
    } else {
        Schema::Json
    };

    Ok(PollResult {
        messages,
        new_state,
        schema,
        trip_circuit_breaker: false,
    })
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_stuck_batch_size_doubles_until_cap() {
        assert_eq!(next_stuck_batch_size(500, 500, 10), Some(1000));
        assert_eq!(next_stuck_batch_size(1000, 500, 10), Some(2000));
        assert_eq!(next_stuck_batch_size(4000, 500, 10), Some(5000));
        assert_eq!(next_stuck_batch_size(5000, 500, 10), None);
    }

    #[test]
    fn batch_is_stuck_all_same_timestamp() {
        let t = "2024-01-01T00:00:00Z";
        let rows: Vec<Row> = vec![
            [("time".to_string(), t.to_string())].into_iter().collect(),
            [("time".to_string(), t.to_string())].into_iter().collect(),
        ];
        assert!(batch_is_stuck(&rows, "time", t));
    }

    #[test]
    fn batch_is_stuck_mixed_timestamps() {
        let rows: Vec<Row> = vec![
            [("time".to_string(), "2024-01-01T00:00:00Z".to_string())]
                .into_iter()
                .collect(),
            [("time".to_string(), "2024-01-01T00:00:01Z".to_string())]
                .into_iter()
                .collect(),
        ];
        assert!(!batch_is_stuck(&rows, "time", "2024-01-01T00:00:00Z"));
    }

    #[test]
    fn batch_is_stuck_empty() {
        let rows: Vec<Row> = vec![];
        assert!(!batch_is_stuck(&rows, "time", "2024-01-01T00:00:00Z"));
    }
}
