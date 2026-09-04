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

use std::collections::HashSet;

use iggy_connector_sdk::{ConsumedMessage, Payload};
use questdb::ingress::{Buffer, TimestampMicros, TimestampNanos};
use simd_json::OwnedValue;
use simd_json::prelude::{ValueAsArray, ValueAsObject};
use simd_json::value::StaticNode;

/// Where the designated timestamp comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampSource {
    /// The Apache Iggy message timestamp (microseconds).
    #[default]
    Message,
    /// The producer-supplied origin timestamp (microseconds).
    Origin,
    /// A field inside the payload, named by `timestamp_field`.
    Payload,
    /// Let QuestDB stamp the row on arrival.
    Server,
}

impl TimestampSource {
    pub fn parse(raw: Option<&str>) -> Option<Self> {
        match raw.map(str::to_ascii_lowercase).as_deref() {
            None => Some(Self::Message),
            Some("message") => Some(Self::Message),
            Some("origin") => Some(Self::Origin),
            Some("payload") => Some(Self::Payload),
            Some("server") => Some(Self::Server),
            Some(_) => None,
        }
    }
}

/// Unit of a payload-sourced timestamp. `Auto` guesses from magnitude, which
/// covers the common case of a producer that switched units between releases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampUnit {
    #[default]
    Auto,
    Seconds,
    Millis,
    Micros,
    Nanos,
}

impl TimestampUnit {
    pub fn parse(raw: Option<&str>) -> Option<Self> {
        match raw.map(str::to_ascii_lowercase).as_deref() {
            None => Some(Self::Auto),
            Some("auto") => Some(Self::Auto),
            Some("seconds") | Some("s") => Some(Self::Seconds),
            Some("millis") | Some("ms") => Some(Self::Millis),
            Some("micros") | Some("us") => Some(Self::Micros),
            Some("nanos") | Some("ns") => Some(Self::Nanos),
            Some(_) => None,
        }
    }

    fn to_nanos(self, value: i64) -> i64 {
        match self {
            Self::Seconds => value.saturating_mul(1_000_000_000),
            Self::Millis => value.saturating_mul(1_000_000),
            Self::Micros => value.saturating_mul(1_000),
            Self::Nanos => value,
            // Thresholds are the epoch value of roughly 2001 in each unit, so a
            // present-day timestamp lands in exactly one bucket.
            Self::Auto => match value.abs() {
                0..=99_999_999_999 => Self::Seconds.to_nanos(value),
                100_000_000_000..=99_999_999_999_999 => Self::Millis.to_nanos(value),
                100_000_000_000_000..=99_999_999_999_999_999 => Self::Micros.to_nanos(value),
                _ => value,
            },
        }
    }
}

/// A message that could not be turned into a row. The batch continues; the
/// caller counts and logs these.
#[derive(Debug)]
pub enum RowError {
    /// The payload was not a JSON object, or a required field was missing.
    Invalid(String),
    /// The QuestDB client rejected the row. Carries the underlying error so the
    /// caller can decide whether it is transient.
    Client(questdb::Error),
}

impl From<questdb::Error> for RowError {
    fn from(error: questdb::Error) -> Self {
        Self::Client(error)
    }
}

/// Everything needed to turn a `ConsumedMessage` into a QuestDB row. Held
/// behind an `Arc` so it can cross into `spawn_blocking` without cloning
/// per batch.
#[derive(Debug)]
pub struct Mapping {
    pub table: String,
    pub symbol_columns: HashSet<String>,
    pub uuid_columns: HashSet<String>,
    pub timestamp_source: TimestampSource,
    pub timestamp_field: Option<String>,
    pub timestamp_unit: TimestampUnit,
    pub include_stream_column: bool,
    pub include_topic_column: bool,
    pub include_partition_column: bool,
    pub include_offset_column: bool,
    pub include_headers: bool,
}

/// Per-batch context that is constant across every row.
#[derive(Debug, Clone, Copy)]
pub struct RowContext<'a> {
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition_id: u32,
}

impl Mapping {
    /// Appends one row. On `Err` the buffer is rewound to the row boundary, so
    /// a rejected message never leaves a half-written row behind.
    pub fn append_row(
        &self,
        buffer: &mut Buffer,
        message: &ConsumedMessage,
        context: RowContext<'_>,
    ) -> Result<(), RowError> {
        buffer.set_marker()?;
        match self.append_row_inner(buffer, message, context) {
            Ok(()) => {
                buffer.clear_marker();
                Ok(())
            }
            Err(error) => {
                // Best effort: if the rewind itself fails the buffer is
                // unusable and the flush will surface it.
                let _ = buffer.rewind_to_marker();
                Err(error)
            }
        }
    }

    fn append_row_inner(
        &self,
        buffer: &mut Buffer,
        message: &ConsumedMessage,
        context: RowContext<'_>,
    ) -> Result<(), RowError> {
        let fields = self.payload_object(message)?;

        buffer.table(self.table.as_str())?;

        // QuestDB requires every symbol to precede every non-symbol column, so
        // the payload is walked twice rather than once.
        if self.include_stream_column {
            buffer.symbol("stream", context.stream)?;
        }
        if self.include_topic_column {
            buffer.symbol("topic", context.topic)?;
        }
        if let Some(fields) = fields {
            for (name, value) in fields {
                if self.is_timestamp_field(name) || !self.symbol_columns.contains(name.as_str()) {
                    continue;
                }
                if let Some(text) = scalar_to_symbol(value) {
                    buffer.symbol(name.as_str(), text.as_str())?;
                }
            }
        }

        if self.include_partition_column {
            buffer.column_i64("partition_id", i64::from(context.partition_id))?;
        }
        if self.include_offset_column {
            buffer.column_i64("offset", message.offset as i64)?;
        }
        if self.include_headers {
            self.append_headers(buffer, message)?;
        }

        match fields {
            Some(fields) => {
                for (name, value) in fields {
                    if self.is_timestamp_field(name) || self.symbol_columns.contains(name.as_str())
                    {
                        continue;
                    }
                    self.append_column(buffer, name.as_str(), value)?;
                }
            }
            None => {
                // Raw / Text payloads have no field structure, so they land in
                // a single column rather than being silently dropped.
                let text = self.payload_text(message)?;
                buffer.column_str("payload", text.as_str())?;
            }
        }

        self.append_timestamp(buffer, message, fields)
    }

    fn is_timestamp_field(&self, name: &str) -> bool {
        self.timestamp_source == TimestampSource::Payload
            && self.timestamp_field.as_deref() == Some(name)
    }

    /// `Some` for JSON object payloads, `None` for payloads with no field
    /// structure.
    fn payload_object<'m>(
        &self,
        message: &'m ConsumedMessage,
    ) -> Result<Option<&'m simd_json::owned::Object>, RowError> {
        match &message.payload {
            Payload::Json(value) => value
                .as_object()
                .map(Some)
                .ok_or_else(|| RowError::Invalid("JSON payload is not an object".to_owned())),
            _ => Ok(None),
        }
    }

    fn payload_text(&self, message: &ConsumedMessage) -> Result<String, RowError> {
        match &message.payload {
            Payload::Text(text) | Payload::Proto(text) => Ok(text.clone()),
            Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
                String::from_utf8(bytes.clone())
                    .map_err(|_| RowError::Invalid("payload is not valid UTF-8".to_owned()))
            }
            Payload::Json(_) => unreachable!("JSON payloads take the field path"),
        }
    }

    fn append_headers(
        &self,
        buffer: &mut Buffer,
        message: &ConsumedMessage,
    ) -> Result<(), RowError> {
        let Some(headers) = message.headers.as_ref() else {
            return Ok(());
        };
        for (key, value) in headers {
            let column = format!("header_{}", key.to_string_value());
            buffer.column_str(column.as_str(), value.to_string_value().as_str())?;
        }
        Ok(())
    }

    fn append_column(
        &self,
        buffer: &mut Buffer,
        name: &str,
        value: &OwnedValue,
    ) -> Result<(), RowError> {
        match value {
            // Omitting the column is how QWP encodes a null.
            OwnedValue::Static(StaticNode::Null) => {}
            OwnedValue::Static(StaticNode::Bool(flag)) => {
                buffer.column_bool(name, *flag)?;
            }
            OwnedValue::Static(StaticNode::I64(number)) => {
                buffer.column_i64(name, *number)?;
            }
            OwnedValue::Static(StaticNode::U64(number)) => {
                // QuestDB has no unsigned 64-bit column; anything past i64::MAX
                // would wrap, so it degrades to DOUBLE rather than corrupting.
                match i64::try_from(*number) {
                    Ok(number) => buffer.column_i64(name, number)?,
                    Err(_) => buffer.column_f64(name, *number as f64)?,
                };
            }
            OwnedValue::Static(StaticNode::F64(number)) => {
                buffer.column_f64(name, *number)?;
            }
            OwnedValue::String(text) => {
                if self.uuid_columns.contains(name) {
                    let (lo, hi) = parse_uuid(text).ok_or_else(|| {
                        RowError::Invalid(format!("column {name} is not a valid UUID"))
                    })?;
                    buffer.column_uuid(name, lo, hi)?;
                } else {
                    buffer.column_str(name, text.as_str())?;
                }
            }
            OwnedValue::Array(items) => {
                append_array(buffer, name, items)?;
            }
            OwnedValue::Object(_) => {
                // Nested objects have no QuestDB column type; store the JSON
                // text so the data is preserved rather than dropped.
                let encoded = simd_json::to_string(value)
                    .map_err(|_| RowError::Invalid(format!("column {name} is not serializable")))?;
                buffer.column_str(name, encoded.as_str())?;
            }
        }
        Ok(())
    }

    fn append_timestamp(
        &self,
        buffer: &mut Buffer,
        message: &ConsumedMessage,
        fields: Option<&simd_json::owned::Object>,
    ) -> Result<(), RowError> {
        match self.timestamp_source {
            TimestampSource::Server => {
                buffer.at_now()?;
            }
            TimestampSource::Message => {
                buffer.at(micros_or_now(message.timestamp))?;
            }
            TimestampSource::Origin => {
                buffer.at(micros_or_now(message.origin_timestamp))?;
            }
            TimestampSource::Payload => {
                let field = self
                    .timestamp_field
                    .as_deref()
                    .ok_or_else(|| RowError::Invalid("timestamp_field is not set".to_owned()))?;
                let raw = fields.and_then(|fields| fields.get(field)).ok_or_else(|| {
                    RowError::Invalid(format!("timestamp field {field} is missing"))
                })?;
                let number = timestamp_number(raw).ok_or_else(|| {
                    RowError::Invalid(format!("timestamp field {field} is not a number"))
                })?;
                buffer.at(TimestampNanos::new(self.timestamp_unit.to_nanos(number)))?;
            }
        }
        Ok(())
    }
}

/// `timestamp == 0` means unset in Apache Iggy, so the row falls back to the
/// server clock rather than landing at the Unix epoch.
fn micros_or_now(micros: u64) -> TimestampMicros {
    if micros == 0 {
        TimestampMicros::now()
    } else {
        TimestampMicros::new(micros as i64)
    }
}

fn timestamp_number(value: &OwnedValue) -> Option<i64> {
    match value {
        OwnedValue::Static(StaticNode::I64(number)) => Some(*number),
        OwnedValue::Static(StaticNode::U64(number)) => i64::try_from(*number).ok(),
        OwnedValue::Static(StaticNode::F64(number)) => Some(*number as i64),
        _ => None,
    }
}

fn scalar_to_symbol(value: &OwnedValue) -> Option<String> {
    match value {
        OwnedValue::Static(StaticNode::Null) => None,
        OwnedValue::Static(StaticNode::Bool(flag)) => Some(flag.to_string()),
        OwnedValue::Static(StaticNode::I64(number)) => Some(number.to_string()),
        OwnedValue::Static(StaticNode::U64(number)) => Some(number.to_string()),
        OwnedValue::Static(StaticNode::F64(number)) => Some(number.to_string()),
        OwnedValue::String(text) => Some(text.clone()),
        _ => None,
    }
}

/// QuestDB only stores `DOUBLE` arrays, so integer input is widened rather than
/// rejected. Nesting is supported to three dimensions, which is the limit of
/// the client's slice-based array API.
fn append_array(buffer: &mut Buffer, name: &str, items: &[OwnedValue]) -> Result<(), RowError> {
    match array_depth(items) {
        1 => {
            let values = flat_array(items).ok_or_else(|| {
                RowError::Invalid(format!("column {name} is not a numeric array"))
            })?;
            buffer.column_arr(name, &values)?;
        }
        2 => {
            let mut rows = Vec::with_capacity(items.len());
            for item in items {
                let nested = item.as_array().and_then(|nested| flat_array(nested));
                rows.push(nested.ok_or_else(|| {
                    RowError::Invalid(format!("column {name} is not a numeric array"))
                })?);
            }
            buffer.column_arr(name, &rows)?;
        }
        3 => {
            let mut cubes = Vec::with_capacity(items.len());
            for item in items {
                let mut rows = Vec::new();
                let outer = item.as_array().ok_or_else(|| {
                    RowError::Invalid(format!("column {name} is not a numeric array"))
                })?;
                for nested in outer {
                    let values = nested.as_array().and_then(|values| flat_array(values));
                    rows.push(values.ok_or_else(|| {
                        RowError::Invalid(format!("column {name} is not a numeric array"))
                    })?);
                }
                cubes.push(rows);
            }
            buffer.column_arr(name, &cubes)?;
        }
        _ => {
            return Err(RowError::Invalid(format!(
                "column {name} exceeds the supported array nesting depth of 3"
            )));
        }
    }
    Ok(())
}

fn array_depth(items: &[OwnedValue]) -> usize {
    match items.first() {
        Some(OwnedValue::Array(nested)) => 1 + array_depth(nested),
        _ => 1,
    }
}

fn flat_array(items: &[OwnedValue]) -> Option<Vec<f64>> {
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        values.push(match item {
            OwnedValue::Static(StaticNode::I64(number)) => *number as f64,
            OwnedValue::Static(StaticNode::U64(number)) => *number as f64,
            OwnedValue::Static(StaticNode::F64(number)) => *number,
            _ => return None,
        });
    }
    Some(values)
}

/// Splits a canonical RFC-4122 UUID into the `(lo, hi)` pair the client expects.
///
/// `Buffer::column_uuid` follows the `java.util.UUID` convention: `hi` is the
/// big-endian most-significant half and `lo` the least-significant half. Its
/// doc comment describes the little-endian *wire* layout instead, and taking
/// that literally writes a byte-reversed UUID with no error.
fn parse_uuid(text: &str) -> Option<(u64, u64)> {
    let mut bytes = [0u8; 16];
    let mut written = 0usize;
    let mut nibble: Option<u8> = None;
    for character in text.chars() {
        if character == '-' {
            continue;
        }
        let value = character.to_digit(16)? as u8;
        match nibble {
            None => nibble = Some(value),
            Some(high) => {
                if written == bytes.len() {
                    return None;
                }
                bytes[written] = (high << 4) | value;
                written += 1;
                nibble = None;
            }
        }
    }
    if written != bytes.len() || nibble.is_some() {
        return None;
    }
    let hi = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let lo = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
    Some((lo, hi))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_canonical_uuid_when_parsed_should_use_java_uuid_halves() {
        let (lo, hi) = parse_uuid("123e4567-e89b-12d3-a456-426614174000").unwrap();
        assert_eq!(hi, 0x123e_4567_e89b_12d3);
        assert_eq!(lo, 0xa456_4266_1417_4000);
    }

    #[test]
    fn given_uuid_without_dashes_when_parsed_should_succeed() {
        let dashed = parse_uuid("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let plain = parse_uuid("123e4567e89b12d3a456426614174000").unwrap();
        assert_eq!(dashed, plain);
    }

    #[test]
    fn given_malformed_uuid_when_parsed_should_return_none() {
        assert!(parse_uuid("not-a-uuid").is_none());
        assert!(parse_uuid("123e4567-e89b-12d3-a456-42661417400").is_none());
        assert!(parse_uuid("123e4567-e89b-12d3-a456-4266141740000").is_none());
    }

    #[test]
    fn given_auto_unit_when_converting_should_pick_bucket_by_magnitude() {
        // 2026-09-04T12:00:00Z in each unit maps to the same nanosecond value.
        let nanos = 1_788_523_200_000_000_000i64;
        assert_eq!(TimestampUnit::Auto.to_nanos(1_788_523_200), nanos);
        assert_eq!(TimestampUnit::Auto.to_nanos(1_788_523_200_000), nanos);
        assert_eq!(TimestampUnit::Auto.to_nanos(1_788_523_200_000_000), nanos);
        assert_eq!(TimestampUnit::Auto.to_nanos(nanos), nanos);
    }

    #[test]
    fn given_explicit_unit_when_converting_should_ignore_magnitude() {
        assert_eq!(TimestampUnit::Seconds.to_nanos(1), 1_000_000_000);
        assert_eq!(TimestampUnit::Millis.to_nanos(1), 1_000_000);
        assert_eq!(TimestampUnit::Micros.to_nanos(1), 1_000);
        assert_eq!(TimestampUnit::Nanos.to_nanos(1), 1);
    }

    #[test]
    fn given_unknown_names_when_parsing_enums_should_return_none() {
        assert!(TimestampSource::parse(Some("nonsense")).is_none());
        assert!(TimestampUnit::parse(Some("fortnights")).is_none());
        assert_eq!(TimestampSource::parse(None), Some(TimestampSource::Message));
        assert_eq!(TimestampUnit::parse(None), Some(TimestampUnit::Auto));
    }

    #[test]
    fn given_nested_arrays_when_measuring_depth_should_count_levels() {
        let mut flat = br#"[1.0, 2.0]"#.to_vec();
        let flat: OwnedValue = simd_json::to_owned_value(&mut flat).unwrap();
        let mut nested = br#"[[1.0], [2.0]]"#.to_vec();
        let nested: OwnedValue = simd_json::to_owned_value(&mut nested).unwrap();

        assert_eq!(array_depth(flat.as_array().unwrap()), 1);
        assert_eq!(array_depth(nested.as_array().unwrap()), 2);
    }
}
