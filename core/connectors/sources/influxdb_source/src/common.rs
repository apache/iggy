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

// ── Constants ─────────────────────────────────────────────────────────────────

/// Default cursor column for V2 (Flux annotated-CSV timestamp annotation).
pub(crate) const DEFAULT_V2_CURSOR_FIELD: &str = "_time";
/// Default cursor column for V3 (SQL timestamp column name).
pub(crate) const DEFAULT_V3_CURSOR_FIELD: &str = "time";

// ── Config ────────────────────────────────────────────────────────────────────
//
// Uses `#[serde(tag = "version")]` instead of `#[serde(flatten)]` because
// serde's flatten interacts poorly with tagged enums — the tag field can be
// consumed before the variant content is parsed, causing deserialization to fail.

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "version")]
pub enum InfluxDbSourceConfig {
    #[serde(rename = "v2")]
    V2(V2SourceConfig),
    #[serde(rename = "v3")]
    V3(V3SourceConfig),
}

/// Deserializes `InfluxDbSourceConfig` with backward-compatible version defaulting.
///
/// Existing V2 configs that omit the `version` field are treated as `"v2"` so
/// deployments can upgrade without touching their config files. Explicitly
/// unknown version strings are rejected with a clear error.
impl<'de> serde::Deserialize<'de> for InfluxDbSourceConfig {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = serde_json::Value::deserialize(d)?;
        let version = match raw.get("version") {
            None => "v2", // absent key → backward compat default
            Some(v) => v.as_str().ok_or_else(|| {
                serde::de::Error::custom(format!(
                    "\"version\" must be a string (e.g. \"v2\" or \"v3\"), got: {v}"
                ))
            })?,
        };
        match version {
            "v2" => serde_json::from_value::<V2SourceConfig>(raw)
                .map(Self::V2)
                .map_err(serde::de::Error::custom),
            "v3" => serde_json::from_value::<V3SourceConfig>(raw)
                .map(Self::V3)
                .map_err(serde::de::Error::custom),
            other => Err(serde::de::Error::custom(format!(
                "unknown InfluxDB version {other:?}; expected \"v2\" or \"v3\""
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2SourceConfig {
    pub(crate) url: String,
    pub(crate) org: String,
    #[serde(serialize_with = "serialize_secret")]
    pub(crate) token: SecretString,
    pub(crate) query: String,
    pub(crate) poll_interval: Option<String>,
    pub(crate) batch_size: Option<u32>,
    pub(crate) cursor_field: Option<String>,
    pub(crate) initial_offset: Option<String>,
    pub(crate) payload_column: Option<String>,
    pub(crate) payload_format: Option<String>,
    pub(crate) include_metadata: Option<bool>,
    pub(crate) verbose_logging: Option<bool>,
    pub(crate) max_retries: Option<u32>,
    pub(crate) retry_delay: Option<String>,
    pub(crate) timeout: Option<String>,
    pub(crate) max_open_retries: Option<u32>,
    pub(crate) open_retry_max_delay: Option<String>,
    pub(crate) retry_max_delay: Option<String>,
    pub(crate) circuit_breaker_threshold: Option<u32>,
    pub(crate) circuit_breaker_cool_down: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3SourceConfig {
    pub(crate) url: String,
    pub(crate) db: String,
    #[serde(serialize_with = "serialize_secret")]
    pub(crate) token: SecretString,
    pub(crate) query: String,
    pub(crate) poll_interval: Option<String>,
    pub(crate) batch_size: Option<u32>,
    pub(crate) cursor_field: Option<String>,
    pub(crate) initial_offset: Option<String>,
    pub(crate) payload_column: Option<String>,
    pub(crate) payload_format: Option<String>,
    /// When `false`, the cursor column (`time` by default) is excluded from the
    /// emitted JSON payload. Useful when consumers don't need the timestamp in
    /// the message body since it's available as message metadata.
    pub(crate) include_metadata: Option<bool>,
    pub(crate) verbose_logging: Option<bool>,
    pub(crate) max_retries: Option<u32>,
    pub(crate) retry_delay: Option<String>,
    pub(crate) timeout: Option<String>,
    pub(crate) max_open_retries: Option<u32>,
    pub(crate) open_retry_max_delay: Option<String>,
    pub(crate) retry_max_delay: Option<String>,
    pub(crate) circuit_breaker_threshold: Option<u32>,
    pub(crate) circuit_breaker_cool_down: Option<String>,
    /// Maximum factor by which batch_size may be inflated before the stuck-timestamp
    /// circuit breaker trips. Defaults to 10 (i.e. up to 10× the configured batch_size).
    /// Maximum accepted value is 100; higher values risk OOM-inducing queries.
    pub(crate) stuck_batch_cap_factor: Option<u32>,
}

// Eliminates the repetitive "match self { V2(c) => …, V3(c) => … }" pattern for
// fields that are identical across all config variants. Methods with version-specific
// logic (cursor_field, max_retries, version_label) remain explicit.
//
// Supported patterns:
//   delegate!(ref  self.url)                        →  &String (borrow)
//   delegate!(opt  self.poll_interval)              →  Option<&str>
//   delegate!(unwrap self.batch_size, 500)          →  T: Copy with value fallback
//
// Not supported (use explicit match arms instead):
//   Fields with version-specific defaults (e.g. cursor_field: "_time" vs "time")
//   Fields with chained transformations (e.g. max_retries + .max(1))
//   Fields that only exist on one variant (e.g. V3's stuck_batch_cap_factor)
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
        // Floor at 1 — callers build LIMIT $limit queries; LIMIT 0 stalls silently.
        // open() also rejects 0 explicitly, but defense-in-depth here costs nothing.
        delegate!(unwrap self.batch_size, 500).max(1)
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
            Self::V2(c) => c.cursor_field.as_deref().unwrap_or(DEFAULT_V2_CURSOR_FIELD),
            Self::V3(c) => c.cursor_field.as_deref().unwrap_or(DEFAULT_V3_CURSOR_FIELD),
        }
    }

    pub fn include_metadata(&self) -> bool {
        delegate!(unwrap self.include_metadata, true)
    }

    // Both arms are identical; `delegate!` is not used because the `.max(1)` chain
    // cannot be expressed in the macro without adding a new variant.
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

    /// URL with any trailing slash stripped — used as the base for all endpoint URLs.
    pub(crate) fn base_url(&self) -> &str {
        self.url().trim_end_matches('/')
    }
}

// ── Row processing context ────────────────────────────────────────────────────

/// Per-poll fields that are constant across all rows in a batch.
/// Passed by reference to `process_rows` so the function signature stays at ≤ 3 parameters.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RowContext<'a> {
    pub cursor_field: &'a str,
    pub current_cursor: &'a str,
    pub include_metadata: bool,
    pub payload_col: Option<&'a str>,
    pub payload_format: PayloadFormat,
    pub now_micros: u64,
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
    /// Row offset within the last timestamp group — used as a tiebreaker
    /// so that siblings at the same timestamp are not silently dropped.
    pub last_timestamp_row_offset: u64,
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
            r"(?-u)^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
        )
        .expect("hardcoded regex is valid")
    })
}

pub fn validate_cursor(cursor: &str) -> Result<(), Error> {
    if !cursor_re().is_match(cursor) {
        return Err(Error::InvalidConfigValue(format!(
            "cursor value {cursor:?} is not a valid RFC 3339 timestamp"
        )));
    }
    // Chain chrono parse to catch calendar-invalid dates (e.g. Feb 30)
    chrono::DateTime::parse_from_rfc3339(cursor).map_err(|e| {
        Error::InvalidConfigValue(format!(
            "cursor value {cursor:?} failed chrono validation: {e}"
        ))
    })?;
    Ok(())
}

/// Validate `cursor_field` for the given connector version.
///
/// `version` should be `"v2"` or `"v3"`. The function is version-strict: `"_time"`
/// is only valid for V2 (Flux annotation column) and `"time"` is only valid for V3
/// (SQL timestamp column). Swapping them silently would produce empty result sets
/// or query errors at the InfluxDB level.
pub fn validate_cursor_field(field: &str, version: &str) -> Result<(), Error> {
    if field.is_empty() {
        return Err(Error::InvalidConfigValue(format!(
            "cursor_field must not be empty for {version} — \
             use \"_time\" for v2 or \"time\" for v3"
        )));
    }
    match (field, version) {
        ("time", "v2") => Err(Error::InvalidConfigValue(
            "cursor_field \"time\" is not valid for v2 — use \"_time\" \
             (the Flux annotated-CSV timestamp column)"
                .into(),
        )),
        ("_time", "v3") => Err(Error::InvalidConfigValue(
            "cursor_field \"_time\" is not valid for v3 — use \"time\" \
             (the SQL timestamp column)"
                .into(),
        )),
        // Allow everything else — custom column names are valid
        _ => Ok(()),
    }
}

// ── Timestamp helpers ─────────────────────────────────────────────────────────

/// Return `true` if timestamp string `a` is strictly after the pre-parsed `b`.
///
/// `b` is accepted as an already-parsed `DateTime<Utc>` so callers that compare
/// against the same cursor on every row in a batch parse it once, not O(n) times.
/// `a` is parsed on each call. Returns `false` conservatively when `a` fails to
/// parse — do NOT advance the cursor when comparison is ambiguous. Lexicographic
/// comparison is incorrect for timestamps with different timezone offsets
/// (e.g. `+05:30` vs `Z`) and would silently produce wrong cursor advancement.
pub fn is_timestamp_after(a: &str, b_parsed: DateTime<Utc>) -> bool {
    match a.parse::<DateTime<Utc>>() {
        Ok(dt_a) => dt_a > b_parsed,
        Err(_) => {
            warn!(
                "is_timestamp_after: could not parse {a:?} as RFC 3339; \
                 refusing to advance cursor"
            );
            false
        }
    }
}

// ── Scalar parsing ────────────────────────────────────────────────────────────

/// Parse a string value from InfluxDB into the most specific JSON scalar type.
///
/// Tries `bool`, then `i64`, then `f64`; falls back to `String`. An empty
/// string becomes `null`. `NaN` and `±Infinity` are emitted as strings because
/// JSON has no representation for non-finite floats
/// (`serde_json::Number::from_f64` returns `None` for them).
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
pub(crate) fn apply_query_params(
    template: &str,
    cursor: &str,
    limit: &str,
    offset: &str,
) -> String {
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
        } else if after.starts_with("$offset") {
            result.push_str(offset);
            remaining = &remaining[pos + "$offset".len()..];
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
        assert!(validate_cursor_field("_time", "v2").is_ok());
        assert!(validate_cursor_field("time", "v3").is_ok());
    }

    #[test]
    fn validate_cursor_field_rejects_empty() {
        // Empty cursor field must always be rejected — it produces no results.
        assert!(validate_cursor_field("", "v2").is_err());
        assert!(validate_cursor_field("", "v3").is_err());
    }

    #[test]
    fn validate_cursor_field_is_version_strict() {
        // Swapping column names across versions is an error, not a silent passthrough:
        // using "time" with v2 or "_time" with v3 produces empty results at the DB level.
        assert!(
            validate_cursor_field("time", "v2").is_err(),
            "\"time\" must be rejected for v2 — correct column is \"_time\""
        );
        assert!(
            validate_cursor_field("_time", "v3").is_err(),
            "\"_time\" must be rejected for v3 — correct column is \"time\""
        );
    }

    #[test]
    fn validate_cursor_field_error_is_version_specific() {
        // The cross-version error messages reference the wrong column and hint at
        // the correct one, so users can fix config without reading the docs.
        let v2_err = validate_cursor_field("time", "v2").unwrap_err().to_string();
        assert!(
            v2_err.contains("v2"),
            "v2 error should mention v2, got: {v2_err}"
        );
        assert!(
            v2_err.contains("\"_time\""),
            "v2 error should suggest _time, got: {v2_err}"
        );

        let v3_err = validate_cursor_field("_time", "v3")
            .unwrap_err()
            .to_string();
        assert!(
            v3_err.contains("v3"),
            "v3 error should mention v3, got: {v3_err}"
        );
        assert!(
            v3_err.contains("\"time\""),
            "v3 error should suggest time, got: {v3_err}"
        );
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
        let earlier = "2026-03-18T12:00:00.60952Z"
            .parse::<DateTime<Utc>>()
            .unwrap();
        let later = "2026-03-18T12:00:00.609521Z"
            .parse::<DateTime<Utc>>()
            .unwrap();
        assert!(is_timestamp_after("2026-03-18T12:00:00.609521Z", earlier));
        assert!(!is_timestamp_after("2026-03-18T12:00:00.60952Z", later));
        assert!(!is_timestamp_after("2026-03-18T12:00:00.609521Z", later));
    }

    #[test]
    fn is_timestamp_after_fallback_is_conservative() {
        // Unparsable `a` must NOT advance the cursor.
        let sentinel = "2024-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert!(!is_timestamp_after("not-a-timestamp", sentinel));
        // Valid `a` that is older than `b` must also return false.
        assert!(!is_timestamp_after("2023-01-01T00:00:00Z", sentinel));
    }

    #[test]
    fn apply_query_params_substitutes_both_placeholders() {
        let tmpl = "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit";
        let out = apply_query_params(tmpl, "2024-01-01T00:00:00Z", "100", "");
        assert_eq!(
            out,
            "SELECT * FROM t WHERE time > '2024-01-01T00:00:00Z' LIMIT 100"
        );
    }

    #[test]
    fn apply_query_params_no_placeholders() {
        let tmpl = "SELECT 1";
        assert_eq!(
            apply_query_params(tmpl, "ignored", "ignored", ""),
            "SELECT 1"
        );
    }

    #[test]
    fn apply_query_params_repeated_placeholders() {
        let tmpl = "$cursor $cursor $limit";
        let out = apply_query_params(tmpl, "T", "5", "");
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
            last_timestamp_row_offset: 0,
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
            last_timestamp_row_offset: 0,
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
            last_timestamp_row_offset: 0,
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

    // ── InfluxDbSourceConfig backward-compat deserialization ─────────────────

    #[test]
    fn source_config_without_version_defaults_to_v2() {
        // Existing V2 configs that pre-date the version field must deserialize
        // as V2 without any modification to the config file.
        let json =
            r#"{"url":"http://localhost:8086","org":"myorg","token":"t","query":"SELECT 1"}"#;
        let cfg: InfluxDbSourceConfig = serde_json::from_str(json).unwrap();
        assert!(
            matches!(cfg, InfluxDbSourceConfig::V2(_)),
            "missing version must default to v2"
        );
    }

    #[test]
    fn source_config_with_explicit_v2_version_deserializes_v2() {
        let json = r#"{"version":"v2","url":"http://localhost:8086","org":"o","token":"t","query":"SELECT 1"}"#;
        let cfg: InfluxDbSourceConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(cfg, InfluxDbSourceConfig::V2(_)));
    }

    #[test]
    fn source_config_with_version_v3_deserializes_v3() {
        let json = r#"{"version":"v3","url":"http://localhost:8181","db":"d","token":"t","query":"SELECT 1"}"#;
        let cfg: InfluxDbSourceConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(cfg, InfluxDbSourceConfig::V3(_)));
    }

    #[test]
    fn source_config_unknown_version_returns_error() {
        let json =
            r#"{"version":"v9","url":"http://localhost","org":"o","token":"t","query":"SELECT 1"}"#;
        let result: Result<InfluxDbSourceConfig, _> = serde_json::from_str(json);
        assert!(result.is_err(), "unknown version must be rejected");
    }

    #[test]
    fn source_config_serializes_with_version_tag() {
        // Round-trip: serialize produces the version tag so the output can be
        // loaded back by a version-aware deserializer.
        let cfg = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost".to_string(),
            org: "o".to_string(),
            token: SecretString::from("t"),
            query: "q".to_string(),
            poll_interval: None,
            batch_size: None,
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
        });
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(
            json.contains(r#""version":"v2""#),
            "serialized form must include version tag"
        );
        let restored: InfluxDbSourceConfig = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored, InfluxDbSourceConfig::V2(_)));
    }

    #[test]
    fn source_config_toml_without_version_defaults_to_v2() {
        // Connectors load config from TOML files in production. Verify the
        // backward-compat path works with TOML, not just JSON.
        let toml_str = r#"
url   = "http://localhost:8086"
org   = "myorg"
token = "t"
query = "SELECT 1"
"#;
        let cfg: InfluxDbSourceConfig = toml::from_str(toml_str).unwrap();
        assert!(
            matches!(cfg, InfluxDbSourceConfig::V2(_)),
            "TOML config without version= must default to V2"
        );
    }

    #[test]
    fn source_config_toml_with_version_v3_deserializes_v3() {
        let toml_str = r#"
version = "v3"
url     = "http://localhost:8181"
db      = "mydb"
token   = "t"
query   = "SELECT 1"
"#;
        let cfg: InfluxDbSourceConfig = toml::from_str(toml_str).unwrap();
        assert!(matches!(cfg, InfluxDbSourceConfig::V3(_)));
    }

    // ── InfluxDbSourceConfig accessors ───────────────────────────────────────

    fn make_v2_cfg() -> InfluxDbSourceConfig {
        let json = r#"{"version":"v2","url":"http://host:8086/","org":"o","token":"t","query":"q",
            "poll_interval":"5s","batch_size":200,"cursor_field":"_time","initial_offset":"1970-01-01T00:00:00Z",
            "payload_column":"data","payload_format":"json","include_metadata":false,
            "verbose_logging":true,"retry_delay":"1s","timeout":"10s","max_open_retries":5,
            "open_retry_max_delay":"30s","retry_max_delay":"2s","circuit_breaker_threshold":3,
            "circuit_breaker_cool_down":"60s","max_retries":4}"#;
        serde_json::from_str(json).unwrap()
    }

    fn make_v3_cfg() -> InfluxDbSourceConfig {
        let json = r#"{"version":"v3","url":"http://host:8181/","db":"mydb","token":"t","query":"q",
            "batch_size":300,"payload_format":"text","include_metadata":true,"max_retries":2}"#;
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn config_accessors_v2_all_fields() {
        let cfg = make_v2_cfg();
        assert_eq!(cfg.url(), "http://host:8086/");
        assert_eq!(cfg.poll_interval(), Some("5s"));
        assert_eq!(cfg.batch_size(), 200);
        assert_eq!(cfg.initial_offset(), Some("1970-01-01T00:00:00Z"));
        assert_eq!(cfg.payload_column(), Some("data"));
        assert_eq!(cfg.payload_format(), Some("json"));
        assert!(!cfg.include_metadata());
        assert!(cfg.verbose_logging());
        assert_eq!(cfg.retry_delay(), Some("1s"));
        assert_eq!(cfg.timeout(), Some("10s"));
        assert_eq!(cfg.max_open_retries(), 5);
        assert_eq!(cfg.open_retry_max_delay(), Some("30s"));
        assert_eq!(cfg.retry_max_delay(), Some("2s"));
        assert_eq!(cfg.circuit_breaker_threshold(), 3);
        assert_eq!(cfg.circuit_breaker_cool_down(), Some("60s"));
        assert_eq!(cfg.max_retries(), 4);
        assert_eq!(cfg.base_url(), "http://host:8086");
        assert_eq!(cfg.version_label(), "v2");
        assert_eq!(cfg.cursor_field(), "_time");
    }

    #[test]
    fn config_accessors_v3_all_fields() {
        let cfg = make_v3_cfg();
        assert_eq!(cfg.url(), "http://host:8181/");
        assert_eq!(cfg.batch_size(), 300);
        assert_eq!(cfg.payload_format(), Some("text"));
        assert!(cfg.include_metadata());
        assert_eq!(cfg.max_retries(), 2);
        assert_eq!(cfg.base_url(), "http://host:8181");
        assert_eq!(cfg.version_label(), "v3");
        assert_eq!(cfg.cursor_field(), "time"); // V3 default
    }

    #[test]
    fn config_accessor_batch_size_zero_is_floored_to_one() {
        // batch_size: 0 would produce LIMIT 0 queries; the accessor floors it to 1.
        let json =
            r#"{"version":"v2","url":"http://h","org":"o","token":"t","query":"q","batch_size":0}"#;
        let cfg: InfluxDbSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.batch_size(), 1);
    }

    #[test]
    fn config_accessor_defaults_when_fields_absent() {
        let json = r#"{"version":"v2","url":"http://h","org":"o","token":"t","query":"q"}"#;
        let cfg: InfluxDbSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.batch_size(), 500);
        assert!(cfg.poll_interval().is_none());
        assert!(cfg.initial_offset().is_none());
        assert!(cfg.payload_column().is_none());
        assert!(cfg.payload_format().is_none());
        assert!(cfg.include_metadata()); // default true
        assert!(!cfg.verbose_logging()); // default false
        assert!(cfg.retry_delay().is_none());
        assert!(cfg.timeout().is_none());
        assert_eq!(cfg.max_open_retries(), 10);
        assert!(cfg.open_retry_max_delay().is_none());
        assert!(cfg.retry_max_delay().is_none());
        assert_eq!(cfg.circuit_breaker_threshold(), 5);
        assert!(cfg.circuit_breaker_cool_down().is_none());
        assert_eq!(cfg.max_retries(), 3);
    }

    #[test]
    fn source_config_version_not_a_string_returns_error() {
        // version must be a string — numeric or null version must be rejected.
        let json = r#"{"version":42,"url":"http://h","org":"o","token":"t","query":"q"}"#;
        let result: Result<InfluxDbSourceConfig, _> = serde_json::from_str(json);
        assert!(result.is_err(), "numeric version must be rejected");
    }

    // ── PayloadFormat ────────────────────────────────────────────────────────

    #[test]
    fn payload_format_from_config_all_variants() {
        assert_eq!(
            PayloadFormat::from_config(Some("text")),
            PayloadFormat::Text
        );
        assert_eq!(
            PayloadFormat::from_config(Some("utf8")),
            PayloadFormat::Text
        );
        assert_eq!(PayloadFormat::from_config(Some("raw")), PayloadFormat::Raw);
        assert_eq!(
            PayloadFormat::from_config(Some("base64")),
            PayloadFormat::Raw
        );
        assert_eq!(
            PayloadFormat::from_config(Some("json")),
            PayloadFormat::Json
        );
        assert_eq!(PayloadFormat::from_config(None), PayloadFormat::Json);
    }

    #[test]
    fn payload_format_from_config_unrecognized_falls_back_to_json() {
        assert_eq!(PayloadFormat::from_config(Some("xml")), PayloadFormat::Json);
    }

    #[test]
    fn payload_format_schema_all_variants() {
        use crate::common::Schema;
        assert_eq!(PayloadFormat::Json.schema(), Schema::Json);
        assert_eq!(PayloadFormat::Text.schema(), Schema::Text);
        assert_eq!(PayloadFormat::Raw.schema(), Schema::Raw);
    }

    // ── parse_scalar float ───────────────────────────────────────────────────

    #[test]
    fn parse_scalar_float_values() {
        // Finite f64 — can be represented as JSON number.
        assert_eq!(
            parse_scalar("1.23456"),
            serde_json::Value::Number(serde_json::Number::from_f64(1.23456).unwrap())
        );
        // NaN is not representable in JSON — falls back to String.
        assert_eq!(
            parse_scalar("NaN"),
            serde_json::Value::String("NaN".to_string())
        );
    }

    // ── apply_query_params $offset and unknown $ ─────────────────────────────

    #[test]
    fn apply_query_params_substitutes_offset() {
        let tmpl = "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset";
        let out = apply_query_params(tmpl, "T", "10", "5");
        assert_eq!(out, "SELECT * FROM t WHERE time > 'T' LIMIT 10 OFFSET 5");
    }

    #[test]
    fn apply_query_params_unknown_dollar_passthrough() {
        // An unrecognized $-placeholder is passed through literally.
        let tmpl = "SELECT $unknown FROM t";
        let out = apply_query_params(tmpl, "T", "10", "0");
        assert_eq!(out, "SELECT $unknown FROM t");
    }


}
