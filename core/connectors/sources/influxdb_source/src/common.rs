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

use iggy_common::serde_secret::serialize_secret;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::{Error, Schema};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use tracing::warn;

pub(crate) use crate::row::{Row, parse_csv_rows, parse_jsonl_rows};

// ── Config ────────────────────────────────────────────────────────────────────
//
// Uses `#[serde(tag = "version")]` instead of `#[serde(flatten)]` because
// serde's flatten interacts poorly with tagged enums — the tag field can be
// consumed before the variant content is parsed, causing deserialization to fail.

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

// Eliminates the repetitive "match self { V2(c) => …, V3(c) => … }" pattern for
// fields that are identical across all config variants. Methods with version-specific
// logic (cursor_field, max_retries, version_label) remain explicit.
macro_rules! delegate {
    // &T field reference  →  fn foo(&self) -> &T
    (ref $self:ident . $field:ident) => {
        match $self {
            Self::V2(c) => &c.$field,
            Self::V3(c) => &c.$field,
        }
    };
    // Option<String>  →  Option<&str>
    (opt $self:ident . $field:ident) => {
        match $self {
            Self::V2(c) => c.$field.as_deref(),
            Self::V3(c) => c.$field.as_deref(),
        }
    };
    // Option<T: Copy>  →  T with fallback
    (unwrap $self:ident . $field:ident, $default:expr) => {
        match $self {
            Self::V2(c) => c.$field.unwrap_or($default),
            Self::V3(c) => c.$field.unwrap_or($default),
        }
    };
}

impl InfluxDbSourceConfig {
    pub fn url(&self) -> &str {
        delegate!(ref    self.url)
    }
    pub fn token_secret(&self) -> &SecretString {
        delegate!(ref    self.token)
    }
    pub fn poll_interval(&self) -> Option<&str> {
        delegate!(opt    self.poll_interval)
    }
    pub fn batch_size(&self) -> u32 {
        delegate!(unwrap self.batch_size, 500)
    }
    pub fn initial_offset(&self) -> Option<&str> {
        delegate!(opt    self.initial_offset)
    }
    pub fn payload_column(&self) -> Option<&str> {
        delegate!(opt    self.payload_column)
    }
    pub fn payload_format(&self) -> Option<&str> {
        delegate!(opt    self.payload_format)
    }
    pub fn verbose_logging(&self) -> bool {
        delegate!(unwrap self.verbose_logging, false)
    }
    pub fn retry_delay(&self) -> Option<&str> {
        delegate!(opt    self.retry_delay)
    }
    pub fn timeout(&self) -> Option<&str> {
        delegate!(opt    self.timeout)
    }
    pub fn max_open_retries(&self) -> u32 {
        delegate!(unwrap self.max_open_retries, 10)
    }
    pub fn open_retry_max_delay(&self) -> Option<&str> {
        delegate!(opt  self.open_retry_max_delay)
    }
    pub fn retry_max_delay(&self) -> Option<&str> {
        delegate!(opt    self.retry_max_delay)
    }
    pub fn circuit_breaker_threshold(&self) -> u32 {
        delegate!(unwrap self.circuit_breaker_threshold, 5)
    }
    pub fn circuit_breaker_cool_down(&self) -> Option<&str> {
        delegate!(opt self.circuit_breaker_cool_down)
    }

    // V2 and V3 use different default cursor column names.
    pub fn cursor_field(&self) -> &str {
        match self {
            Self::V2(c) => c.cursor_field.as_deref().unwrap_or("_time"),
            Self::V3(c) => c.cursor_field.as_deref().unwrap_or("time"),
        }
    }

    // Enforces a minimum of 1 retry regardless of configuration.
    pub fn max_retries(&self) -> u32 {
        match self {
            Self::V2(c) => c.max_retries.unwrap_or(3).max(1),
            Self::V3(c) => c.max_retries.unwrap_or(3).max(1),
        }
    }

    pub fn version_label(&self) -> &'static str {
        match self {
            Self::V2(_) => "v2",
            Self::V3(_) => "v3",
        }
    }
}

// ── Persisted state ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum PersistedState {
    #[serde(rename = "v2")]
    V2(V2State),
    #[serde(rename = "v3")]
    V3(V3State),
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct V2State {
    pub last_timestamp: Option<String>,
    pub processed_rows: u64,
    /// Rows at `last_timestamp` already delivered; used to skip them when the
    /// Flux query uses `>= $cursor` and a batch boundary lands mid-timestamp.
    pub cursor_row_count: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct V3State {
    pub last_timestamp: Option<String>,
    pub processed_rows: u64,
    /// Current effective batch size after stuck-timestamp inflation.
    /// Reset to the configured base value when the cursor advances.
    pub effective_batch_size: u32,
}

// ── Payload format ────────────────────────────────────────────────────────────

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

// ── Cursor validation ─────────────────────────────────────────────────────────

static CURSOR_RE: OnceLock<regex::Regex> = OnceLock::new();

pub fn cursor_re() -> &'static regex::Regex {
    CURSOR_RE.get_or_init(|| {
        // Validates RFC 3339 timestamp structure with proper field ranges:
        // month 01-12, day 01-31, hour 00-23, minute/second 00-59.
        // Timezone suffix is required: a naive timestamp without Z or +HH:MM
        // is rejected to prevent silent UTC-vs-local ambiguity between V2 (Flux
        // always treats timestamps as UTC) and V3 (SQL engine timezone depends
        // on server config).
        // Note: day 29-31 validity for a given month is not checked by the regex;
        // chrono parsing inside validate_cursor handles that for tz-aware timestamps.
        regex::Regex::new(
            r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
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

// ── Timestamp helpers ─────────────────────────────────────────────────────────

/// Return `true` if timestamp `a` is strictly after `b`.
///
/// Parses both strings as RFC 3339 / chrono `DateTime<Utc>`. Falls back to
/// lexicographic comparison if either value fails to parse — this covers the
/// nanosecond-precision timestamps that InfluxDB 3 returns (e.g.
/// `"2026-03-18T12:00:00.609521Z"`), which chrono parses correctly when the
/// fractional-seconds component is six or fewer digits.
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

// ── Scalar parsing ────────────────────────────────────────────────────────────

/// Parse a string value from InfluxDB into the most specific JSON scalar type.
///
/// Tries `bool`, then `i64`, then `f64`; falls back to `String`. An empty
/// string becomes `null`. This is used when building the JSON payload for
/// messages produced by the source connector.
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

// ── Query template substitution ───────────────────────────────────────────────

/// Substitute `$cursor` and `$limit` placeholders in a query template in a
/// single pass, avoiding the two intermediate `String` allocations that
/// `clone() + replace() + replace()` would produce.
pub(crate) fn apply_query_params(template: &str, cursor: &str, limit: &str) -> String {
    let capacity = template.len() + cursor.len() + limit.len();
    let mut result = String::with_capacity(capacity);
    let mut remaining = template;
    while let Some(pos) = remaining.find('$') {
        result.push_str(&remaining[..pos]);
        let after = &remaining[pos..];
        if after.starts_with("$cursor") {
            result.push_str(cursor);
            remaining = &remaining[pos + "$cursor".len()..];
        } else if after.starts_with("$limit") {
            result.push_str(limit);
            remaining = &remaining[pos + "$limit".len()..];
        } else {
            result.push('$');
            remaining = &remaining[pos + 1..];
        }
    }
    result.push_str(remaining);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_cursor_accepts_rfc3339() {
        assert!(validate_cursor("2024-01-15T10:30:00Z").is_ok());
        assert!(validate_cursor("2024-01-15T10:30:00.123456789Z").is_ok());
        assert!(validate_cursor("2024-01-15T10:30:00+05:30").is_ok());
        assert!(validate_cursor("1970-01-01T00:00:00Z").is_ok());
    }

    #[test]
    fn validate_cursor_rejects_timezone_free_timestamp() {
        // A naive timestamp without a timezone suffix is rejected to prevent
        // silent UTC-vs-local ambiguity between V2 (always UTC) and V3
        // (SQL engine may apply a different default timezone).
        assert!(
            validate_cursor("2026-04-12T11:28:25.180749").is_err(),
            "no timezone suffix must be rejected"
        );
        assert!(
            validate_cursor("2024-01-15T10:30:00").is_err(),
            "bare datetime without tz must be rejected"
        );
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
    fn validate_cursor_rejects_out_of_range_date_parts() {
        assert!(validate_cursor("2024-13-01T00:00:00Z").is_err(), "month 13");
        assert!(validate_cursor("2024-00-01T00:00:00Z").is_err(), "month 0");
        assert!(validate_cursor("2024-01-00T00:00:00Z").is_err(), "day 0");
        assert!(validate_cursor("2024-01-32T00:00:00Z").is_err(), "day 32");
        assert!(validate_cursor("2024-01-01T24:00:00Z").is_err(), "hour 24");
        assert!(
            validate_cursor("2024-01-01T00:60:00Z").is_err(),
            "minute 60"
        );
        assert!(
            validate_cursor("2024-01-01T00:00:60Z").is_err(),
            "second 60"
        );
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
    fn apply_query_params_substitutes_both_placeholders() {
        let tmpl = "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit";
        let out = apply_query_params(tmpl, "2024-01-01T00:00:00Z", "100");
        assert_eq!(
            out,
            "SELECT * FROM t WHERE time > '2024-01-01T00:00:00Z' LIMIT 100"
        );
    }

    #[test]
    fn apply_query_params_no_placeholders() {
        let tmpl = "SELECT 1";
        assert_eq!(apply_query_params(tmpl, "ignored", "ignored"), "SELECT 1");
    }

    #[test]
    fn apply_query_params_repeated_placeholders() {
        let tmpl = "$cursor $cursor $limit";
        let out = apply_query_params(tmpl, "T", "5");
        assert_eq!(out, "T T 5");
    }

    // ── V2State / V3State ─────────────────────────────────────────────────

    #[test]
    fn v2_state_default_is_zeroed() {
        let s = V2State::default();
        assert!(s.last_timestamp.is_none());
        assert_eq!(s.processed_rows, 0);
        assert_eq!(s.cursor_row_count, 0);
    }

    #[test]
    fn v3_state_default_is_zeroed() {
        let s = V3State::default();
        assert!(s.last_timestamp.is_none());
        assert_eq!(s.processed_rows, 0);
        assert_eq!(s.effective_batch_size, 0);
    }

    #[test]
    fn v2_state_clone_preserves_all_fields() {
        let original = V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            processed_rows: 42,
            cursor_row_count: 3,
        };
        let cloned = original.clone();
        assert_eq!(cloned.last_timestamp, original.last_timestamp);
        assert_eq!(cloned.processed_rows, original.processed_rows);
        assert_eq!(cloned.cursor_row_count, original.cursor_row_count);
    }

    #[test]
    fn v3_state_clone_preserves_all_fields() {
        let original = V3State {
            last_timestamp: Some("2024-06-15T12:30:00Z".to_string()),
            processed_rows: 100,
            effective_batch_size: 1000,
        };
        let cloned = original.clone();
        assert_eq!(cloned.last_timestamp, original.last_timestamp);
        assert_eq!(cloned.processed_rows, original.processed_rows);
        assert_eq!(cloned.effective_batch_size, original.effective_batch_size);
    }

    #[test]
    fn v2_state_serde_round_trip() {
        let original = V2State {
            last_timestamp: Some("2024-06-15T12:30:00Z".to_string()),
            processed_rows: 999,
            cursor_row_count: 7,
        };
        let json = serde_json::to_string(&original).unwrap();
        let restored: V2State = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.last_timestamp, original.last_timestamp);
        assert_eq!(restored.processed_rows, original.processed_rows);
        assert_eq!(restored.cursor_row_count, original.cursor_row_count);
    }

    #[test]
    fn v3_state_serde_round_trip() {
        let original = V3State {
            last_timestamp: Some("2024-06-15T12:30:00Z".to_string()),
            processed_rows: 500,
            effective_batch_size: 2000,
        };
        let json = serde_json::to_string(&original).unwrap();
        let restored: V3State = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.last_timestamp, original.last_timestamp);
        assert_eq!(restored.processed_rows, original.processed_rows);
        assert_eq!(restored.effective_batch_size, original.effective_batch_size);
    }

    #[test]
    fn persisted_state_v2_serde_includes_version_tag() {
        let state = PersistedState::V2(V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            processed_rows: 1,
            cursor_row_count: 0,
        });
        let json = serde_json::to_string(&state).unwrap();
        assert!(json.contains(r#""version":"v2""#));
        let restored: PersistedState = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored, PersistedState::V2(_)));
    }

    #[test]
    fn persisted_state_v3_serde_includes_version_tag() {
        let state = PersistedState::V3(V3State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            processed_rows: 1,
            effective_batch_size: 500,
        });
        let json = serde_json::to_string(&state).unwrap();
        assert!(json.contains(r#""version":"v3""#));
        let restored: PersistedState = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored, PersistedState::V3(_)));
    }

    #[test]
    fn persisted_state_wrong_version_tag_fails_to_deserialize() {
        let json = r#"{"version":"v9","last_timestamp":null,"processed_rows":0}"#;
        let result: Result<PersistedState, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }
}
