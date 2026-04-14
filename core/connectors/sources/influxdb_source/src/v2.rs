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
    PayloadFormat, Row, V2SourceConfig, V2State, is_timestamp_after, parse_csv_rows, parse_scalar,
    validate_cursor,
};
use base64::{Engine as _, engine::general_purpose};
use iggy_connector_sdk::{Error, ProducedMessage, Schema};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde_json::json;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

pub(crate) fn auth_header(token: &str) -> String {
    format!("Token {token}")
}

pub(crate) fn health_url(base: &str) -> Result<Url, Error> {
    Url::parse(&format!("{base}/health"))
        .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))
}

fn build_query_url(base: &str, org: &str) -> Result<Url, Error> {
    let mut url = Url::parse(&format!("{base}/api/v2/query"))
        .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;
    if !org.is_empty() {
        url.query_pairs_mut().append_pair("org", org);
    }
    Ok(url)
}

fn build_query_body(query: &str) -> serde_json::Value {
    json!({
        "query": query,
        "dialect": {
            "annotations": [],
            "delimiter": ",",
            "header": true,
            "commentPrefix": "#"
        }
    })
}

fn query_with_params(config: &V2SourceConfig, cursor: &str, already_seen: u64) -> Result<String, Error> {
    validate_cursor(cursor)?;
    let batch = config.batch_size.unwrap_or(500) as u64;
    let limit = batch.saturating_add(already_seen).to_string();
    let mut q = config.query.clone();
    if q.contains("$cursor") {
        q = q.replace("$cursor", cursor);
    }
    if q.contains("$limit") {
        q = q.replace("$limit", &limit);
    }
    Ok(q)
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

pub(crate) async fn run_query(
    client: &ClientWithMiddleware,
    config: &V2SourceConfig,
    auth: &str,
    cursor: &str,
    already_seen: u64,
) -> Result<String, Error> {
    let query = query_with_params(config, cursor, already_seen)?;
    let base = config.url.trim_end_matches('/');
    let url = build_query_url(base, &config.org)?;
    let body = build_query_body(&query);

    let response = client
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

// ---------------------------------------------------------------------------
// Message building
// ---------------------------------------------------------------------------

fn build_payload(
    row: &Row,
    payload_column: Option<&str>,
    payload_format: PayloadFormat,
    include_metadata: bool,
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

    let mut json_row = serde_json::Map::new();
    for (key, value) in row {
        if include_metadata || key == "_value" || key == "_time" || key == "_measurement" {
            json_row.insert(key.clone(), parse_scalar(value));
        }
    }

    let wrapped = json!({
        "measurement": row.get("_measurement").cloned().unwrap_or_default(),
        "field":       row.get("_field").cloned().unwrap_or_default(),
        "timestamp":   row.get("_time").cloned().unwrap_or_default(),
        "value":       row.get("_value").map(|v| parse_scalar(v)).unwrap_or(serde_json::Value::Null),
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

    let mut messages = Vec::with_capacity(rows.len());
    let mut max_cursor: Option<String> = None;
    let mut rows_at_max_cursor = 0u64;
    let mut skipped = 0u64;

    for row in &rows {
        if let Some(cv) = row.get(cursor_field)
            && cv == &cursor
            && skipped < already_seen
        {
            skipped += 1;
            continue;
        }

        if let Some(cv) = row.get(cursor_field) {
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

    let schema = if payload_col.is_some() {
        payload_format.schema()
    } else {
        Schema::Json
    };

    Ok(PollResult {
        messages,
        max_cursor,
        rows_at_max_cursor,
        skipped,
        schema,
    })
}
