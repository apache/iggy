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
    use std::collections::BTreeMap;

    use iggy::prelude::{HeaderKey, HeaderKind, HeaderValue};
    use questdb::ingress::ProtocolVersion;

    use super::*;

    /// `Buffer::new` yields an ILP buffer, which is what makes these tests
    /// possible: ILP is inspectable through `as_bytes`, so the exact wire
    /// output can be asserted. The `Buffer` API, the symbol-before-column
    /// state machine and the marker/rewind path are shared with QWP. Only
    /// `column_uuid` and typed arrays are QWP-only, so those are covered by
    /// the integration suite instead.
    /// V1 is the all-text InfluxDB-compatible encoding. V2 and V3 write
    /// doubles as binary, which would make `as_bytes` unreadable here.
    fn buffer() -> Buffer {
        Buffer::new(ProtocolVersion::V1)
    }

    fn mapping() -> Mapping {
        Mapping {
            table: "events".to_owned(),
            symbol_columns: HashSet::new(),
            uuid_columns: HashSet::new(),
            timestamp_source: TimestampSource::Message,
            timestamp_field: None,
            timestamp_unit: TimestampUnit::Auto,
            include_stream_column: false,
            include_topic_column: false,
            include_partition_column: false,
            include_offset_column: false,
            include_headers: false,
        }
    }

    fn context() -> RowContext<'static> {
        RowContext {
            stream: "user_events",
            topic: "trades",
            partition_id: 3,
        }
    }

    fn json_message(json: &str) -> ConsumedMessage {
        let mut bytes = json.as_bytes().to_vec();
        ConsumedMessage {
            id: 7,
            offset: 42,
            checksum: 0,
            timestamp: 1_788_523_200_000_000,
            origin_timestamp: 1_700_000_000_000_000,
            headers: None,
            payload: Payload::Json(simd_json::to_owned_value(&mut bytes).unwrap()),
        }
    }

    fn rendered(buffer: &Buffer) -> String {
        String::from_utf8_lossy(buffer.as_bytes()).into_owned()
    }

    #[test]
    fn given_symbol_columns_when_appending_should_emit_symbols_before_columns() {
        let mut mapping = mapping();
        mapping.symbol_columns.insert("side".to_owned());
        let mut buffer = buffer();

        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"side":"buy","price":1.5}"#),
                context(),
            )
            .unwrap();

        let line = rendered(&buffer);
        let symbol_at = line.find("side=buy").expect("symbol missing");
        let column_at = line.find("price=").expect("column missing");
        assert!(
            symbol_at < column_at,
            "symbol must precede column, got: {line}"
        );
    }

    #[test]
    fn given_metadata_flags_when_appending_should_add_stream_topic_partition_offset() {
        let mut mapping = mapping();
        mapping.include_stream_column = true;
        mapping.include_topic_column = true;
        mapping.include_partition_column = true;
        mapping.include_offset_column = true;
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &json_message(r#"{"price":1.5}"#), context())
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("stream=user_events"), "{line}");
        assert!(line.contains("topic=trades"), "{line}");
        assert!(line.contains("partition_id=3i"), "{line}");
        assert!(line.contains("offset=42i"), "{line}");
    }

    #[test]
    fn given_null_field_when_appending_should_omit_the_column() {
        let mut buffer = buffer();
        mapping()
            .append_row(
                &mut buffer,
                &json_message(r#"{"price":1.5,"missing":null}"#),
                context(),
            )
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("price="), "{line}");
        assert!(
            !line.contains("missing"),
            "null must be omitted, got: {line}"
        );
    }

    #[test]
    fn given_nested_object_when_appending_should_store_json_text() {
        let mut buffer = buffer();
        mapping()
            .append_row(
                &mut buffer,
                &json_message(r#"{"meta":{"venue":"nyse"}}"#),
                context(),
            )
            .unwrap();

        assert!(rendered(&buffer).contains("venue"), "{}", rendered(&buffer));
    }

    #[test]
    fn given_message_timestamp_source_when_appending_should_use_message_timestamp() {
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &json_message(r#"{"price":1.5}"#), context())
            .unwrap();

        // Message timestamp is microseconds; ILP renders nanoseconds.
        assert!(
            rendered(&buffer).contains("1788523200000000000"),
            "{}",
            rendered(&buffer)
        );
    }

    #[test]
    fn given_origin_timestamp_source_when_appending_should_use_origin_timestamp() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Origin;
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &json_message(r#"{"price":1.5}"#), context())
            .unwrap();

        assert!(
            rendered(&buffer).contains("1700000000000000000"),
            "{}",
            rendered(&buffer)
        );
    }

    #[test]
    fn given_payload_timestamp_source_when_appending_should_consume_the_field() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("event_time".to_owned());
        mapping.timestamp_unit = TimestampUnit::Micros;
        let mut buffer = buffer();

        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"event_time":1788523200000000,"price":1.5}"#),
                context(),
            )
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("1788523200000000000"), "{line}");
        assert!(
            !line.contains("event_time="),
            "timestamp field must not double as a column, got: {line}"
        );
    }

    #[test]
    fn given_missing_payload_timestamp_when_appending_should_reject_row() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("event_time".to_owned());
        let mut buffer = buffer();

        let error = mapping
            .append_row(&mut buffer, &json_message(r#"{"price":1.5}"#), context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)));
        assert!(buffer.is_empty(), "rejected row must leave nothing behind");
    }

    #[test]
    fn given_non_object_payload_when_appending_should_reject_row() {
        let mut buffer = buffer();
        let error = mapping()
            .append_row(&mut buffer, &json_message("[1,2,3]"), context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)));
        assert!(buffer.is_empty());
    }

    #[test]
    fn given_mid_row_failure_when_appending_should_rewind_and_keep_batch_usable() {
        // The bad UUID is reached only after the table, a symbol and a column
        // are already encoded, so this exercises the rewind rather than an
        // up-front rejection.
        let mut mapping = mapping();
        mapping.symbol_columns.insert("side".to_owned());
        mapping.uuid_columns.insert("trade_id".to_owned());
        let mut buffer = buffer();

        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"side":"buy","price":1.0}"#),
                context(),
            )
            .unwrap();
        let after_good = rendered(&buffer);

        let error = mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"side":"sell","price":2.0,"trade_id":"not-a-uuid"}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)));
        assert_eq!(
            rendered(&buffer),
            after_good,
            "rejected row must be rewound exactly"
        );

        // The buffer is still usable for the next record.
        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"side":"buy","price":3.0}"#),
                context(),
            )
            .unwrap();
        assert_eq!(buffer.row_count(), 2);
    }

    #[test]
    fn given_headers_enabled_when_appending_should_prefix_header_columns() {
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"price":1.5}"#);
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_raw(HeaderKind::String, b"source").unwrap(),
            HeaderValue::from_raw(HeaderKind::String, b"gateway").unwrap(),
        );
        message.headers = Some(headers);
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &message, context())
            .unwrap();

        assert!(
            rendered(&buffer).contains("header_source="),
            "{}",
            rendered(&buffer)
        );
    }

    #[test]
    fn given_text_payload_when_appending_should_store_it_in_payload_column() {
        let mut buffer = buffer();
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Text("raw body".to_owned());

        mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap();

        assert!(
            rendered(&buffer).contains("payload="),
            "{}",
            rendered(&buffer)
        );
    }

    #[test]
    fn given_every_structureless_payload_when_appending_should_store_it_verbatim() {
        // Raw, Proto, FlatBuffer and Avro all reach QuestDB through the same
        // `payload` column, so each variant must round-trip its bytes rather
        // than being dropped for having no JSON fields.
        let body = "structureless body";
        let variants = [
            Payload::Raw(body.as_bytes().to_vec()),
            Payload::Proto(body.to_owned()),
            Payload::FlatBuffer(body.as_bytes().to_vec()),
            Payload::Avro(body.as_bytes().to_vec()),
        ];

        for payload in variants {
            let label = format!("{payload:?}");
            let mut buffer = buffer();
            let mut message = json_message(r#"{"unused":1}"#);
            message.payload = payload;

            mapping()
                .append_row(&mut buffer, &message, context())
                .unwrap();

            let line = rendered(&buffer);
            assert!(line.contains(body), "{label} did not round-trip: {line}");
        }
    }

    #[test]
    fn given_non_utf8_binary_payload_when_appending_should_reject_row() {
        // 0xFF is never valid UTF-8, and a VARCHAR column cannot hold it.
        let mut buffer = buffer();
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Raw(vec![0xFF, 0xFE, 0xFD]);

        let error = mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)));
        assert!(buffer.is_empty(), "rejected row must leave nothing behind");
    }

    #[test]
    fn given_structureless_payload_when_metadata_enabled_should_still_add_columns() {
        // A payload with no fields must not skip the stream/topic/offset
        // columns, which are the only way to trace such a row back.
        let mut mapping = mapping();
        mapping.include_stream_column = true;
        mapping.include_topic_column = true;
        mapping.include_offset_column = true;
        let mut buffer = buffer();
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Text("body".to_owned());

        mapping
            .append_row(&mut buffer, &message, context())
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("stream=user_events"), "{line}");
        assert!(line.contains("topic=trades"), "{line}");
        assert!(line.contains("offset=42i"), "{line}");
        assert!(line.contains("payload="), "{line}");
    }

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
