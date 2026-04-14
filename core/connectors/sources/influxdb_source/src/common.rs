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

use csv::StringRecord;
use iggy_common::serde_secret::serialize_secret;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::{Error, Schema};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::OnceLock;
use tracing::warn;

pub(crate) type Row = HashMap<String, String>;

// ---------------------------------------------------------------------------
// Config — tagged enum (no serde(flatten) to avoid deserialization issues)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum InfluxDbSourceConfig {
    #[serde(rename = "v2")]
    V2(V2SourceConfig),
    #[serde(rename = "v3")]
    V3(V3SourceConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2SourceConfig {
    pub url: String,
    pub org: String,
    #[serde(serialize_with = "serialize_secret")]
    pub token: SecretString,
    pub query: String,
    pub poll_interval: Option<String>,
    pub batch_size: Option<u32>,
    pub cursor_field: Option<String>,
    pub initial_offset: Option<String>,
    pub payload_column: Option<String>,
    pub payload_format: Option<String>,
    pub include_metadata: Option<bool>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub timeout: Option<String>,
    pub max_open_retries: Option<u32>,
    pub open_retry_max_delay: Option<String>,
    pub retry_max_delay: Option<String>,
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3SourceConfig {
    pub url: String,
    pub db: String,
    #[serde(serialize_with = "serialize_secret")]
    pub token: SecretString,
    pub query: String,
    pub poll_interval: Option<String>,
    pub batch_size: Option<u32>,
    pub cursor_field: Option<String>,
    pub initial_offset: Option<String>,
    pub payload_column: Option<String>,
    pub payload_format: Option<String>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub timeout: Option<String>,
    pub max_open_retries: Option<u32>,
    pub open_retry_max_delay: Option<String>,
    pub retry_max_delay: Option<String>,
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,
    /// Maximum factor by which batch_size may be inflated before the stuck-timestamp
    /// circuit breaker trips. Defaults to 10 (i.e. up to 10× the configured batch_size).
    pub stuck_batch_cap_factor: Option<u32>,
}

impl InfluxDbSourceConfig {
    pub fn url(&self) -> &str {
        match self {
            Self::V2(c) => &c.url,
            Self::V3(c) => &c.url,
        }
    }

    pub fn token_secret(&self) -> &SecretString {
        match self {
            Self::V2(c) => &c.token,
            Self::V3(c) => &c.token,
        }
    }

    pub fn poll_interval(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.poll_interval.as_deref(),
            Self::V3(c) => c.poll_interval.as_deref(),
        }
    }

    pub fn batch_size(&self) -> u32 {
        match self {
            Self::V2(c) => c.batch_size.unwrap_or(500),
            Self::V3(c) => c.batch_size.unwrap_or(500),
        }
    }

    pub fn cursor_field(&self) -> &str {
        match self {
            Self::V2(c) => c.cursor_field.as_deref().unwrap_or("_time"),
            Self::V3(c) => c.cursor_field.as_deref().unwrap_or("time"),
        }
    }

    pub fn initial_offset(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.initial_offset.as_deref(),
            Self::V3(c) => c.initial_offset.as_deref(),
        }
    }

    pub fn payload_column(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.payload_column.as_deref(),
            Self::V3(c) => c.payload_column.as_deref(),
        }
    }

    pub fn payload_format(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.payload_format.as_deref(),
            Self::V3(c) => c.payload_format.as_deref(),
        }
    }

    pub fn verbose_logging(&self) -> bool {
        match self {
            Self::V2(c) => c.verbose_logging.unwrap_or(false),
            Self::V3(c) => c.verbose_logging.unwrap_or(false),
        }
    }

    pub fn max_retries(&self) -> u32 {
        match self {
            Self::V2(c) => c.max_retries.unwrap_or(3).max(1),
            Self::V3(c) => c.max_retries.unwrap_or(3).max(1),
        }
    }

    pub fn retry_delay(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.retry_delay.as_deref(),
            Self::V3(c) => c.retry_delay.as_deref(),
        }
    }

    pub fn timeout(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.timeout.as_deref(),
            Self::V3(c) => c.timeout.as_deref(),
        }
    }

    pub fn max_open_retries(&self) -> u32 {
        match self {
            Self::V2(c) => c.max_open_retries.unwrap_or(10),
            Self::V3(c) => c.max_open_retries.unwrap_or(10),
        }
    }

    pub fn open_retry_max_delay(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.open_retry_max_delay.as_deref(),
            Self::V3(c) => c.open_retry_max_delay.as_deref(),
        }
    }

    pub fn retry_max_delay(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.retry_max_delay.as_deref(),
            Self::V3(c) => c.retry_max_delay.as_deref(),
        }
    }

    pub fn circuit_breaker_threshold(&self) -> u32 {
        match self {
            Self::V2(c) => c.circuit_breaker_threshold.unwrap_or(5),
            Self::V3(c) => c.circuit_breaker_threshold.unwrap_or(5),
        }
    }

    pub fn circuit_breaker_cool_down(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.circuit_breaker_cool_down.as_deref(),
            Self::V3(c) => c.circuit_breaker_cool_down.as_deref(),
        }
    }

    pub fn version_label(&self) -> &'static str {
        match self {
            Self::V2(_) => "v2",
            Self::V3(_) => "v3",
        }
    }
}

// ---------------------------------------------------------------------------
// Versioned persisted state
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum PersistedState {
    #[serde(rename = "v2")]
    V2(V2State),
    #[serde(rename = "v3")]
    V3(V3State),
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct V2State {
    pub last_timestamp: Option<String>,
    pub processed_rows: u64,
    /// Rows at `last_timestamp` already delivered; used to skip them when the
    /// Flux query uses `>= $cursor` and a batch boundary lands mid-timestamp.
    pub cursor_row_count: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct V3State {
    pub last_timestamp: Option<String>,
    pub processed_rows: u64,
    /// Current effective batch size after stuck-timestamp inflation.
    /// Reset to the configured base value when the cursor advances.
    pub effective_batch_size: u32,
}

// ---------------------------------------------------------------------------
// PayloadFormat
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    #[default]
    Json,
    Text,
    Raw,
}

impl PayloadFormat {
    pub fn from_config(value: Option<&str>) -> Self {
        match value.map(|v| v.to_ascii_lowercase()).as_deref() {
            Some("text") | Some("utf8") => PayloadFormat::Text,
            Some("raw") | Some("base64") => PayloadFormat::Raw,
            Some("json") => PayloadFormat::Json,
            other => {
                if other.is_some() {
                    warn!(
                        "Unrecognized payload_format {:?}, falling back to JSON",
                        other
                    );
                }
                PayloadFormat::Json
            }
        }
    }

    pub fn schema(self) -> Schema {
        match self {
            PayloadFormat::Json => Schema::Json,
            PayloadFormat::Text => Schema::Text,
            PayloadFormat::Raw => Schema::Raw,
        }
    }
}

// ---------------------------------------------------------------------------
// Cursor validation
// ---------------------------------------------------------------------------

static CURSOR_RE: OnceLock<regex::Regex> = OnceLock::new();

pub fn cursor_re() -> &'static regex::Regex {
    CURSOR_RE.get_or_init(|| {
        regex::Regex::new(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$",
        )
        .expect("hardcoded regex is valid")
    })
}

pub fn validate_cursor(cursor: &str) -> Result<(), Error> {
    if cursor_re().is_match(cursor) {
        Ok(())
    } else {
        Err(Error::InvalidConfigValue(format!(
            "cursor value {cursor:?} is not a valid RFC 3339 timestamp; \
             refusing substitution to prevent query injection"
        )))
    }
}

pub fn validate_cursor_field(field: &str) -> Result<(), Error> {
    match field {
        "_time" | "time" => Ok(()),
        other => Err(Error::InvalidConfigValue(format!(
            "cursor_field {other:?} is not supported — only \"_time\" (V2) and \"time\" (V3) \
             are valid timestamp cursor columns"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

pub fn is_timestamp_after(a: &str, b: &str) -> bool {
    match (a.parse::<DateTime<Utc>>(), b.parse::<DateTime<Utc>>()) {
        (Ok(dt_a), Ok(dt_b)) => dt_a > dt_b,
        _ => {
            warn!(
                "is_timestamp_after: could not parse timestamps as RFC 3339 \
                 ({a:?} vs {b:?}); falling back to lexicographic comparison"
            );
            a > b
        }
    }
}

// ---------------------------------------------------------------------------
// parse_scalar
// ---------------------------------------------------------------------------

pub fn parse_scalar(value: &str) -> serde_json::Value {
    if value.is_empty() {
        return serde_json::Value::Null;
    }
    if let Ok(v) = value.parse::<bool>() {
        return serde_json::Value::Bool(v);
    }
    if let Ok(v) = value.parse::<i64>() {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = value.parse::<f64>()
        && let Some(number) = serde_json::Number::from_f64(v)
    {
        return serde_json::Value::Number(number);
    }
    serde_json::Value::String(value.to_string())
}

// ---------------------------------------------------------------------------
// V2 annotated-CSV parser
// ---------------------------------------------------------------------------

fn is_header_record(record: &StringRecord) -> bool {
    record.iter().any(|v| v == "_time")
}

pub fn parse_csv_rows(csv_text: &str) -> Result<Vec<Row>, Error> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(csv_text.as_bytes());

    let mut headers: Option<StringRecord> = None;
    let mut rows = Vec::new();

    for result in reader.records() {
        let record =
            result.map_err(|e| Error::InvalidRecordValue(format!("Invalid CSV record: {e}")))?;

        if record.is_empty() {
            continue;
        }

        if let Some(first) = record.get(0)
            && first.starts_with('#')
        {
            continue;
        }

        if is_header_record(&record) {
            headers = Some(record.clone());
            continue;
        }

        let Some(active_headers) = headers.as_ref() else {
            continue;
        };

        if record == *active_headers {
            continue;
        }

        let mut mapped = Row::new();
        for (idx, key) in active_headers.iter().enumerate() {
            if key.is_empty() {
                continue;
            }
            let value = record.get(idx).unwrap_or("").to_string();
            mapped.insert(key.to_string(), value);
        }

        if !mapped.is_empty() {
            rows.push(mapped);
        }
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// V3 JSONL parser
// ---------------------------------------------------------------------------

pub fn parse_jsonl_rows(jsonl_text: &str) -> Result<Vec<Row>, Error> {
    let mut rows = Vec::new();

    for (line_no, line) in jsonl_text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(line).map_err(|e| {
                Error::InvalidRecordValue(format!(
                    "JSONL parse error on line {}: {e} — raw: {line:?}",
                    line_no + 1
                ))
            })?;

        let row: Row = obj
            .into_iter()
            .map(|(k, v)| {
                let s = match v {
                    serde_json::Value::String(s) => s,
                    serde_json::Value::Null => "null".to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Number(n) => n.to_string(),
                    other => other.to_string(),
                };
                (k, s)
            })
            .collect();

        rows.push(row);
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_cursor_accepts_rfc3339() {
        assert!(validate_cursor("2024-01-15T10:30:00Z").is_ok());
        assert!(validate_cursor("2024-01-15T10:30:00.123456789Z").is_ok());
        assert!(validate_cursor("2024-01-15T10:30:00+05:30").is_ok());
        assert!(validate_cursor("1970-01-01T00:00:00Z").is_ok());
        assert!(validate_cursor("2026-04-12T11:28:25.180749").is_ok());
    }

    #[test]
    fn validate_cursor_rejects_invalid() {
        assert!(validate_cursor(r#"") |> drop()"#).is_err());
        assert!(validate_cursor("2024-01-15 10:30:00Z").is_err());
        assert!(validate_cursor("not-a-timestamp").is_err());
        assert!(validate_cursor("").is_err());
        assert!(validate_cursor("2024-01-15").is_err());
    }

    #[test]
    fn validate_cursor_field_accepts_time_columns() {
        assert!(validate_cursor_field("_time").is_ok());
        assert!(validate_cursor_field("time").is_ok());
    }

    #[test]
    fn validate_cursor_field_rejects_others() {
        assert!(validate_cursor_field("_value").is_err());
        assert!(validate_cursor_field("").is_err());
    }

    #[test]
    fn parse_scalar_types() {
        assert_eq!(parse_scalar(""), serde_json::Value::Null);
        assert_eq!(parse_scalar("true"), serde_json::Value::Bool(true));
        assert_eq!(parse_scalar("42"), serde_json::Value::Number(42.into()));
        assert_eq!(
            parse_scalar("hello"),
            serde_json::Value::String("hello".to_string())
        );
    }

    #[test]
    fn is_timestamp_after_chronological() {
        let earlier = "2026-03-18T12:00:00.60952Z";
        let later = "2026-03-18T12:00:00.609521Z";
        assert!(is_timestamp_after(later, earlier));
        assert!(!is_timestamp_after(earlier, later));
        assert!(!is_timestamp_after(later, later));
    }

    #[test]
    fn parse_csv_rows_basics() {
        let csv = "#group,false\n#datatype,string\n_time,_value\n2024-01-01T00:00:00Z,42\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("_value").map(String::as_str), Some("42"));
    }

    #[test]
    fn parse_csv_rows_multi_table() {
        let csv = "_time,_value\n2024-01-01T00:00:00Z,1\n\n_time,_value\n2024-01-01T00:00:01Z,2\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn parse_jsonl_rows_basics() {
        let jsonl = r#"{"time":"2024-01-01T00:00:00Z","val":42}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("val").map(String::as_str), Some("42"));
    }

    #[test]
    fn parse_jsonl_rows_stringifies_types() {
        let jsonl = r#"{"b":true,"n":null,"f":1.5}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows[0].get("b").map(String::as_str), Some("true"));
        assert_eq!(rows[0].get("n").map(String::as_str), Some("null"));
    }

    #[test]
    fn parse_jsonl_invalid_returns_error() {
        assert!(parse_jsonl_rows("not json\n").is_err());
    }
}
