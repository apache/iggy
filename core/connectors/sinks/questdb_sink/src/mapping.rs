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

use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

use iggy_connector_sdk::{ConsumedMessage, Payload};
use questdb::ingress::{Buffer, ColumnName, TimestampMicros, TimestampNanos};
use simd_json::OwnedValue;
use simd_json::prelude::{ValueAsArray, ValueAsObject};
use simd_json::value::StaticNode;
use tracing::warn;
use uuid::Uuid;

use crate::CONNECTOR_NAME;

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

    /// `None` when the value cannot be expressed in nanoseconds, which for
    /// `Auto` also catches a magnitude that was bucketed as a coarser unit than
    /// it really is: multiplying it up overflows rather than quietly saturating
    /// to a timestamp centuries away.
    fn to_nanos(self, value: i64) -> Option<i64> {
        match self {
            Self::Seconds => value.checked_mul(1_000_000_000),
            Self::Millis => value.checked_mul(1_000_000),
            Self::Micros => value.checked_mul(1_000),
            Self::Nanos => Some(value),
            // Each threshold is the point above which a value cannot be the
            // smaller unit any more: 1e11 seconds is far past any plausible
            // date, so such a value is milliseconds, and so on. `unsigned_abs`
            // rather than `abs` because `i64::MIN` has no positive counterpart.
            Self::Auto => match value.unsigned_abs() {
                0..=99_999_999_999 => Self::Seconds.to_nanos(value),
                100_000_000_000..=99_999_999_999_999 => Self::Millis.to_nanos(value),
                100_000_000_000_000..=99_999_999_999_999_999 => Self::Micros.to_nanos(value),
                _ => Some(value),
            },
        }
    }
}

/// QuestDB's column-name limit. The client enforces it on the buffer from the
/// server's `cairo.max.file.name.length`, whose default this matches; a server
/// configured lower will refuse a name this accepts, which the caller recovers
/// from as an ordinary rejection.
pub(crate) const MAX_NAME_LEN: usize = 127;

/// Where a rounded integer came from, which decides what the warning tells the
/// operator to do. An array element and a value past `i64::MAX` have no exact
/// QuestDB form, so pointing them at `integer_columns` would be wrong: that
/// list refuses both. Each source warns once on its own, so an early array
/// rounding cannot hide the `integer_columns` remedy for a scalar column.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Rounding {
    Scalar,
    PastLong,
    ArrayElement,
}

impl Rounding {
    /// The match is exhaustive, so a new variant fails to compile here until
    /// it is counted.
    const COUNT: usize = match Self::Scalar {
        Self::Scalar | Self::PastLong | Self::ArrayElement => 3,
    };

    fn remedy(self) -> &'static str {
        match self {
            Self::Scalar => "list the column in integer_columns to keep it exact",
            Self::PastLong => "a value above i64::MAX has no exact QuestDB column",
            Self::ArrayElement => {
                "QuestDB stores only DOUBLE arrays, so an array element has no exact form"
            }
        }
    }
}

/// Widens an integer to the `DOUBLE` QuestDB stores, and says whether that
/// lost precision. The test goes through `i128`, because casting the double
/// back to the source width saturates at the extremes and would call
/// `i64::MAX` exact after rounding it to 2^63.
fn widen_exact(number: i128) -> (f64, bool) {
    let widened = number as f64;
    (widened, widened as i128 == number)
}

/// A message that could not be turned into a row. The batch continues; the
/// caller counts and logs these.
#[derive(Debug)]
pub enum RowError {
    /// The payload was not a JSON object, or a required field was missing.
    /// Caught before the first write, so the buffer is untouched and the batch
    /// continues.
    Invalid(String),
    /// The QuestDB client rejected the row. Carries the underlying error so the
    /// caller can decide whether it is transient.
    ///
    /// Every column and symbol setter rolls the half-written row back before
    /// returning, so the buffer is still usable and only this record is lost.
    Client(questdb::Error),
    /// `Buffer::at` refused the row. It is the one call that returns without
    /// rolling back, so the buffer can hold a partial row and the caller has to
    /// drop it. [`Mapping::prepare_timestamp`] rejects every value `at` would
    /// refuse, which is what keeps this unreachable.
    Unrecoverable(questdb::Error),
}

impl From<questdb::Error> for RowError {
    fn from(error: questdb::Error) -> Self {
        Self::Client(error)
    }
}

/// Configured column names, matched without regard to case.
///
/// QuestDB resolves a column name case-insensitively, so `uuid_columns =
/// ["trade_id"]` has to match a payload field named `Trade_Id`. Matching it
/// case-sensitively instead would write that field as a plain string column and
/// report nothing.
///
/// A short list with a linear scan beats a hash set here: the entries are one
/// per configured column, while the lookup runs for every field of every row,
/// and hashing would need a freshly lowercased copy of each field name.
#[derive(Debug, Default)]
pub struct ColumnNames(Vec<String>);

impl ColumnNames {
    pub fn contains(&self, name: &str) -> bool {
        self.0
            .iter()
            .any(|configured| configured.eq_ignore_ascii_case(name))
    }

    #[cfg(test)]
    pub fn insert(&mut self, name: String) {
        if !self.contains(&name) {
            self.0.push(name);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

impl FromIterator<String> for ColumnNames {
    fn from_iter<I: IntoIterator<Item = String>>(names: I) -> Self {
        Self(names.into_iter().collect())
    }
}

/// Everything needed to turn a `ConsumedMessage` into a QuestDB row. Held
/// behind an `Arc` so it can cross into `spawn_blocking` without cloning
/// per batch.
#[derive(Debug)]
pub struct Mapping {
    /// The connector ID, for the one log line this module writes.
    pub id: u32,
    pub table: String,
    pub symbol_columns: ColumnNames,
    pub uuid_columns: ColumnNames,
    /// Columns that stay `LONG` while [`Mapping::numbers_as_double`] is on.
    ///
    /// The list also pins the type when `numbers_as_double` is off: a
    /// fractional value in a listed column is rejected either way, because
    /// storing it as a `DOUBLE` would break the type the declaration pinned.
    pub integer_columns: ColumnNames,
    pub timestamp_source: TimestampSource,
    pub timestamp_field: Option<String>,
    pub timestamp_unit: TimestampUnit,
    pub include_stream_column: bool,
    pub include_topic_column: bool,
    pub include_partition_column: bool,
    pub include_offset_column: bool,
    pub include_headers: bool,
    /// Write every JSON number as a `DOUBLE`.
    ///
    /// JSON carries one number type and a serializer writes `2.0` as `2`, so a
    /// producer whose values are sometimes whole makes a column's type depend on
    /// which record reached the buffer first. The client pins that type for the
    /// rest of the buffer and refuses every record of that window which
    /// disagrees, so the records lost depend on where the flush boundary fell.
    ///
    /// On by default, so a column holding a measurement is always a `DOUBLE`.
    /// The cost is exactness above 2^53: a column holding a nanosecond epoch, a
    /// snowflake identifier or a hash goes in [`Mapping::integer_columns`]
    /// rather than turning this off. Turning it off is only safe when every
    /// numeric field is a genuine integer and the table was pre-created, since
    /// otherwise a whole-valued first window creates the column as a `LONG`.
    pub numbers_as_double: bool,
    /// One flag per [`Rounding`] source, set when that source first rounded an
    /// integer on its way to a `DOUBLE`, so each warning is logged once per
    /// connector rather than per row.
    pub rounding_warned: [AtomicBool; Rounding::COUNT],
}

/// Per-batch context that is constant across every row.
#[derive(Debug, Clone, Copy)]
pub struct RowContext<'a> {
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition_id: u32,
}

impl Mapping {
    /// Appends one row.
    ///
    /// The row is built in full before the first write, so a message this
    /// mapping rejects never touches the buffer. Building is what validates: a
    /// ragged array, a malformed UUID and an object that will not serialize all
    /// fail while the row is being assembled, which is why there is no separate
    /// validation pass to keep in step.
    ///
    /// That ordering is what the caller relies on, and the reason is narrower
    /// than it looks. The client rolls back a row it refuses itself, on every
    /// column and symbol setter, so a rejection from the client costs one record
    /// and leaves the buffer usable. It cannot roll back a rejection of this
    /// mapping's own: once an earlier column of the row is written, nothing
    /// removes it. The public `set_marker` API would, but on this transport it
    /// snapshots every buffered table, which makes building a batch quadratic in
    /// its length.
    pub fn append_row(
        &self,
        buffer: &mut Buffer,
        message: &ConsumedMessage,
        context: RowContext<'_>,
    ) -> Result<(), RowError> {
        // `json_document` parses proto text that carries a JSON document, so it
        // is resolved once here rather than per field.
        let document = message.payload.json_document();
        // A document that is not an object carries no fields to map, so it takes
        // the payload column rather than being rejected. That covers a JSON
        // array or scalar from the `json` decoder as well as proto text holding
        // one.
        let fields = document.as_deref().and_then(|value| value.as_object());
        let row = self.prepare_row(message, fields)?;
        self.write_row(buffer, &row, message, context)
    }

    /// Builds every value of one row, rejecting anything QuestDB would refuse.
    fn prepare_row<'m>(
        &self,
        message: &'m ConsumedMessage,
        fields: Option<&'m simd_json::owned::Object>,
    ) -> Result<PreparedRow<'m>, RowError> {
        // QuestDB resolves column names case-insensitively, so two fields that
        // differ only in case are one column and the second value would be
        // dropped without a word. The claimed names are borrowed and compared in
        // place: a row carries a handful of columns, so scanning that list costs
        // less than hashing a lowercased copy of every name, and it allocates
        // nothing on a path that runs for every field of every row.
        let mut seen: Vec<&str> = Vec::new();
        for reserved in self.reserved_columns() {
            seen.push(reserved);
        }

        let mut row = PreparedRow {
            symbols: Vec::new(),
            columns: Vec::new(),
            payload: None,
            headers: Vec::new(),
            timestamp: PreparedTimestamp::Now,
        };

        match fields {
            Some(fields) => {
                // The timestamp field is matched without regard to case, like
                // QuestDB's own column lookup, so two fields can both claim to
                // be the timestamp. Stamping the row from whichever was read
                // last would be a silent choice, so the row is rejected.
                let mut timestamp_fields = 0usize;
                for (name, value) in fields {
                    if self.is_timestamp_field(name) {
                        timestamp_fields += 1;
                        if timestamp_fields > 1 {
                            return Err(RowError::Invalid(format!(
                                "two fields match timestamp_field {}, so the row has no single timestamp",
                                self.timestamp_field.as_deref().unwrap_or_default()
                            )));
                        }
                        continue;
                    }
                    if self.symbol_columns.contains(name.as_str()) {
                        // A symbol has to be scalar. Dropping a structured
                        // value silently would also mean that listing a field
                        // in `symbol_columns` quietly discards data the default
                        // path would have kept as JSON text.
                        let Some(text) = scalar_to_symbol(value) else {
                            if matches!(value, OwnedValue::Static(StaticNode::Null)) {
                                continue;
                            }
                            return Err(RowError::Invalid(format!(
                                "column {name} is listed in symbol_columns but its value is not a scalar"
                            )));
                        };
                        self.claim_column(name.as_str(), &mut seen)?;
                        row.symbols.push((name.as_str(), text));
                        continue;
                    }
                    if matches!(value, OwnedValue::Static(StaticNode::Null)) {
                        // Encoded by omitting the column, so it claims nothing.
                        continue;
                    }
                    self.claim_column(name.as_str(), &mut seen)?;
                    row.columns
                        .push((name.as_str(), self.prepare_value(name.as_str(), value)?));
                }
            }
            None => {
                row.payload = Some(self.payload_text(message)?);
            }
        }

        if self.include_headers
            && let Some(headers) = message.headers.as_ref()
        {
            row.headers.reserve(headers.len());
            for (key, value) in headers {
                let column = format!("header_{}", key.to_string_value());
                // `to_string_value` Debug-formats a `Raw` value, which would
                // store the literal text `b"\x01\x02"`. Base64 keeps the bytes
                // recoverable, and matches what the HTTP sink writes for that
                // kind.
                let text = match value.as_raw() {
                    Ok(bytes) => BASE64_STANDARD.encode(bytes),
                    Err(_) => value.to_string_value(),
                };
                row.headers.push((column, text));
            }
            // The prefixed names are owned, so they are claimed from the vector
            // that already holds them rather than being built a second time.
            for (column, _) in &row.headers {
                self.claim_column(column, &mut seen)?;
            }
        }

        // Every QuestDB row needs at least one symbol or column before its
        // designated timestamp; `at` is refused otherwise. `seen` holds every
        // claimed name, reserved columns and headers included, and the payload
        // column is the one write that claims no name.
        if seen.is_empty() && row.payload.is_none() {
            return Err(RowError::Invalid(
                "row would have no columns, so QuestDB cannot accept it".to_owned(),
            ));
        }

        row.timestamp = self.prepare_timestamp(message, fields)?;
        Ok(row)
    }

    /// Column names this connector emits itself for every row.
    pub(crate) fn reserved_columns(&self) -> impl Iterator<Item = &'static str> {
        self.reserved_columns_with_flags().map(|(name, _)| name)
    }

    /// The enabled reserved columns, each with the configuration flag that
    /// enables it, so an error message can name the flag an operator has to
    /// turn off. The flag names are not derivable from the column names:
    /// `partition_id` is enabled by `include_partition_column`.
    pub(crate) fn reserved_columns_with_flags(
        &self,
    ) -> impl Iterator<Item = (&'static str, &'static str)> {
        [
            (
                "stream",
                "include_stream_column",
                self.include_stream_column,
            ),
            ("topic", "include_topic_column", self.include_topic_column),
            (
                "partition_id",
                "include_partition_column",
                self.include_partition_column,
            ),
            (
                "offset",
                "include_offset_column",
                self.include_offset_column,
            ),
        ]
        .into_iter()
        .filter_map(|(name, flag, included)| included.then_some((name, flag)))
    }

    /// Records that `name` will occupy a column, rejecting a case-insensitive
    /// collision with one already claimed.
    fn claim_column<'n>(&self, name: &'n str, seen: &mut Vec<&'n str>) -> Result<(), RowError> {
        validate_name(name)?;
        if seen
            .iter()
            .any(|claimed| claimed.eq_ignore_ascii_case(name))
        {
            return Err(RowError::Invalid(format!(
                "column {name} collides with another column of this row; QuestDB matches column names case-insensitively"
            )));
        }
        seen.push(name);
        Ok(())
    }

    /// Turns one JSON value into the column the write pass will set.
    ///
    /// The caller skips nulls and has already claimed the name, so neither is
    /// rechecked here.
    fn prepare_value<'v>(
        &self,
        name: &str,
        value: &'v OwnedValue,
    ) -> Result<PreparedValue<'v>, RowError> {
        // A column declared as a UUID has to hold one. A number or a boolean
        // there creates a column of the wrong type, which the client accepts and
        // the server then refuses for every later row.
        if self.uuid_columns.contains(name) {
            let OwnedValue::String(text) = value else {
                return Err(RowError::Invalid(format!(
                    "column {name} is listed in uuid_columns but its value is not a string"
                )));
            };
            let (lo, hi) = parse_uuid(text)
                .ok_or_else(|| RowError::Invalid(format!("column {name} is not a valid UUID")))?;
            return Ok(PreparedValue::Uuid(lo, hi));
        }
        // A column declared an integer has to hold a number, for the same
        // reason as the UUID guard above: any other type would create or pin
        // the column as something the declaration ruled out.
        let declared_integer = self.integer_columns.contains(name);
        if declared_integer
            && !matches!(
                value,
                OwnedValue::Static(StaticNode::I64(_) | StaticNode::U64(_) | StaticNode::F64(_))
            )
        {
            return Err(RowError::Invalid(format!(
                "column {name} is listed in integer_columns but its value is not a number"
            )));
        }
        // The rejection reasons below name the column and never the value: the
        // reason reaches the error log whatever `log_rejected_payload` says.
        let as_double = self.numbers_as_double && !declared_integer;
        match value {
            OwnedValue::Static(StaticNode::Bool(flag)) => Ok(PreparedValue::Bool(*flag)),
            OwnedValue::Static(StaticNode::I64(number)) if as_double => Ok(PreparedValue::F64(
                self.widen(name, i128::from(*number), Rounding::Scalar),
            )),
            OwnedValue::Static(StaticNode::U64(number)) if as_double => Ok(PreparedValue::F64(
                self.widen(name, i128::from(*number), Rounding::Scalar),
            )),
            OwnedValue::Static(StaticNode::I64(number)) => Ok(PreparedValue::I64(*number)),
            // QuestDB has no unsigned 64-bit column, so anything past `i64::MAX`
            // would wrap. A column the operator declared an integer is refused
            // rather than silently sent as a `DOUBLE`, which would change the
            // type the declaration pinned. Otherwise it degrades to `DOUBLE`,
            // which is what `numbers_as_double` would have done anyway, and
            // the remedy differs: no QuestDB column holds such a value exactly.
            OwnedValue::Static(StaticNode::U64(number)) => match i64::try_from(*number) {
                Ok(number) => Ok(PreparedValue::I64(number)),
                Err(_) if declared_integer => Err(RowError::Invalid(format!(
                    "column {name} is listed in integer_columns but its value is larger than a QuestDB LONG"
                ))),
                Err(_) => Ok(PreparedValue::F64(self.widen(
                    name,
                    i128::from(*number),
                    Rounding::PastLong,
                ))),
            },
            // A declared integer column takes a float whose value is whole,
            // because JSON gives no way to tell `20` from `20.0`. A genuinely
            // fractional value is refused: rounding it would store something the
            // producer did not send, and sending it as a `DOUBLE` would break
            // the type the declaration pinned. The upper bound is strict:
            // `i64::MAX as f64` rounds up to 2^63, which `as i64` would
            // saturate one short of the value sent.
            OwnedValue::Static(StaticNode::F64(number)) if declared_integer => {
                let number = *number;
                if number.fract() == 0.0 && number >= i64::MIN as f64 && number < i64::MAX as f64 {
                    Ok(PreparedValue::I64(number as i64))
                } else {
                    Err(RowError::Invalid(format!(
                        "column {name} is listed in integer_columns but its value is not a whole number in range"
                    )))
                }
            }
            OwnedValue::Static(StaticNode::F64(number)) => Ok(PreparedValue::F64(*number)),
            OwnedValue::String(text) => Ok(PreparedValue::Str(text.as_str())),
            OwnedValue::Array(items) => {
                let (values, rounded) = parse_array(name, items)?;
                if rounded {
                    self.warn_rounding(name, Rounding::ArrayElement);
                }
                Ok(PreparedValue::Array(values))
            }
            // Nested objects have no QuestDB column type; store the JSON text so
            // the data is preserved rather than dropped.
            OwnedValue::Object(_) => simd_json::to_string(value)
                .map(PreparedValue::Json)
                .map_err(|_| RowError::Invalid(format!("column {name} is not serializable"))),
            // The caller skips a null before reaching this.
            OwnedValue::Static(StaticNode::Null) => Err(RowError::Invalid(format!(
                "column {name} holds a null that should have been skipped"
            ))),
        }
    }

    /// Hands back the widened number, and says so once if the widening lost
    /// precision.
    ///
    /// A `DOUBLE` carries integers exactly only up to 2^53. The README states
    /// the limit, but an operator who missed it would otherwise get rounded
    /// identifiers with no signal at all, so the first lossy conversion is
    /// logged, once per connector and source, naming the column and the remedy.
    fn widen(&self, name: &str, number: i128, source: Rounding) -> f64 {
        let (widened, exact) = widen_exact(number);
        if !exact {
            self.warn_rounding(name, source);
        }
        widened
    }

    fn warn_rounding(&self, name: &str, source: Rounding) {
        if !self.rounding_warned[source as usize].swap(true, Ordering::Relaxed) {
            warn!(
                "{CONNECTOR_NAME} ID: {} column {name} holds an integer a DOUBLE cannot carry exactly, so it was rounded; {}. This is reported once per connector for this kind of value.",
                self.id,
                source.remedy()
            );
        }
    }

    /// Resolves the designated timestamp, rejecting a value `Buffer::at` would
    /// refuse.
    ///
    /// Every source is checked, not just the payload one: `origin_timestamp`
    /// comes from the producer, and a value above `i64::MAX` wraps negative in
    /// the cast that follows. `at` is the one call that refuses a value without
    /// rolling the row back, so a timestamp it would refuse is the only input
    /// that could leave a partial row.
    fn prepare_timestamp(
        &self,
        message: &ConsumedMessage,
        fields: Option<&simd_json::owned::Object>,
    ) -> Result<PreparedTimestamp, RowError> {
        match self.timestamp_source {
            TimestampSource::Server => Ok(PreparedTimestamp::Now),
            TimestampSource::Message => Ok(PreparedTimestamp::Micros(micros_timestamp(
                "message timestamp",
                message.timestamp,
            )?)),
            TimestampSource::Origin => Ok(PreparedTimestamp::Micros(micros_timestamp(
                "origin timestamp",
                message.origin_timestamp,
            )?)),
            TimestampSource::Payload => {
                let field = self
                    .timestamp_field
                    .as_deref()
                    .ok_or_else(|| RowError::Invalid("timestamp_field is not set".to_owned()))?;
                let raw = fields
                    .and_then(|fields| field_value(fields, field))
                    .ok_or_else(|| {
                        RowError::Invalid(format!("timestamp field {field} is missing"))
                    })?;
                let number = timestamp_number(raw).ok_or_else(|| {
                    RowError::Invalid(format!("timestamp field {field} is not a number"))
                })?;
                // The conversion to nanoseconds happens here so the sign is the
                // one the buffer will see.
                let nanos = self.timestamp_unit.to_nanos(number).ok_or_else(|| {
                    RowError::Invalid(format!(
                        "timestamp field {field} is out of range for its unit"
                    ))
                })?;
                if nanos < 0 {
                    return Err(RowError::Invalid(format!(
                        "timestamp field {field} is before the Unix epoch"
                    )));
                }
                Ok(PreparedTimestamp::Nanos(TimestampNanos::new(nanos)))
            }
        }
    }

    /// Writes a prepared row. Every failure here belongs to the client.
    fn write_row(
        &self,
        buffer: &mut Buffer,
        row: &PreparedRow<'_>,
        message: &ConsumedMessage,
        context: RowContext<'_>,
    ) -> Result<(), RowError> {
        buffer.table(self.table.as_str())?;

        // QuestDB requires every symbol to precede every non-symbol column.
        if self.include_stream_column {
            buffer.symbol("stream", context.stream)?;
        }
        if self.include_topic_column {
            buffer.symbol("topic", context.topic)?;
        }
        for (name, text) in &row.symbols {
            buffer.symbol(*name, text.as_ref())?;
        }

        if self.include_partition_column {
            buffer.column_i64("partition_id", i64::from(context.partition_id))?;
        }
        if self.include_offset_column {
            buffer.column_i64("offset", message.offset as i64)?;
        }
        for (column, text) in &row.headers {
            buffer.column_str(column.as_str(), text.as_str())?;
        }
        for (name, value) in &row.columns {
            match value {
                PreparedValue::Bool(flag) => {
                    buffer.column_bool(*name, *flag)?;
                }
                PreparedValue::I64(number) => {
                    buffer.column_i64(*name, *number)?;
                }
                PreparedValue::F64(number) => {
                    buffer.column_f64(*name, *number)?;
                }
                PreparedValue::Str(text) => {
                    buffer.column_str(*name, *text)?;
                }
                PreparedValue::Uuid(lo, hi) => {
                    buffer.column_uuid(*name, *lo, *hi)?;
                }
                PreparedValue::Array(values) => write_array(buffer, name, values)?,
                PreparedValue::Json(text) => {
                    buffer.column_str(*name, text.as_str())?;
                }
            }
        }
        // A record with no field structure lands in a single column rather than
        // being silently dropped.
        if let Some(payload) = &row.payload {
            buffer.column_str("payload", payload.as_ref())?;
        }

        // `at` is the one call that refuses a value without rolling the row
        // back, so its failure leaves a partial row and the caller has to drop
        // the buffer. `prepare_timestamp` covers every value that would be
        // refused, which is what keeps this unreachable.
        match row.timestamp {
            PreparedTimestamp::Now => buffer.at_now(),
            PreparedTimestamp::Micros(micros) => buffer.at(micros),
            PreparedTimestamp::Nanos(nanos) => buffer.at(nanos),
        }
        .map_err(RowError::Unrecoverable)
    }

    fn is_timestamp_field(&self, name: &str) -> bool {
        self.timestamp_source == TimestampSource::Payload
            && self
                .timestamp_field
                .as_deref()
                .is_some_and(|field| field.eq_ignore_ascii_case(name))
    }

    /// The payload of a record that carries no field structure.
    ///
    /// Nothing copies a payload that is already text. A `Payload::Json` reaches
    /// this only when the document is not an object, which has no fields to map,
    /// and is rendered back to JSON text.
    fn payload_text<'m>(&self, message: &'m ConsumedMessage) -> Result<Cow<'m, str>, RowError> {
        match &message.payload {
            Payload::Text(text) | Payload::Proto(text) => Ok(Cow::Borrowed(text.as_str())),
            Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
                std::str::from_utf8(bytes)
                    .map(Cow::Borrowed)
                    .map_err(|_| RowError::Invalid("payload is not valid UTF-8".to_owned()))
            }
            Payload::Json(value) => simd_json::to_string(value)
                .map(Cow::Owned)
                .map_err(|error| {
                    RowError::Invalid(format!("payload cannot be rendered as JSON text: {error}"))
                }),
        }
    }
}

/// One row with every value built, ready to write.
struct PreparedRow<'m> {
    symbols: Vec<(&'m str, Cow<'m, str>)>,
    columns: Vec<(&'m str, PreparedValue<'m>)>,
    payload: Option<Cow<'m, str>>,
    headers: Vec<(String, String)>,
    timestamp: PreparedTimestamp,
}

/// A column value in the shape the client's setter takes.
enum PreparedValue<'m> {
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(&'m str),
    Uuid(u64, u64),
    Array(ArrayValues),
    Json(String),
}

/// Which `Buffer::at` call closes the row.
enum PreparedTimestamp {
    Now,
    Micros(TimestampMicros),
    Nanos(TimestampNanos),
}

/// Turns an Apache Iggy microsecond timestamp into one the buffer will accept.
///
/// Apache Iggy carries these as `u64` and the client takes an `i64`, so a value
/// above `i64::MAX` wraps negative. `Buffer::at` refuses a negative value
/// without rolling the row back, which is the one input that leaves a partial
/// row, so it is caught before anything is written. `0` means unset in Apache
/// Iggy, so the row falls back to the server clock rather than landing at the
/// Unix epoch.
fn micros_timestamp(label: &str, micros: u64) -> Result<TimestampMicros, RowError> {
    if micros > i64::MAX as u64 {
        return Err(RowError::Invalid(format!(
            "{label} {micros} is too large for a QuestDB timestamp"
        )));
    }
    if micros == 0 {
        Ok(TimestampMicros::now())
    } else {
        Ok(TimestampMicros::new(micros as i64))
    }
}

/// Looks a payload field up without regard to case, which is how QuestDB
/// resolves a column name and therefore how `timestamp_field` has to match.
fn field_value<'m>(fields: &'m simd_json::owned::Object, name: &str) -> Option<&'m OwnedValue> {
    fields.get(name).or_else(|| {
        fields
            .iter()
            .find(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value)
    })
}

/// `None` for anything that is not a usable timestamp.
///
/// A float needs the range test: `as i64` turns `NaN` into zero, which would
/// stamp the row at the epoch, and saturates a large value, which would stamp it
/// centuries away. Both would pass validation and store a wrong time.
fn timestamp_number(value: &OwnedValue) -> Option<i64> {
    match value {
        OwnedValue::Static(StaticNode::I64(number)) => Some(*number),
        OwnedValue::Static(StaticNode::U64(number)) => i64::try_from(*number).ok(),
        OwnedValue::Static(StaticNode::F64(number)) => {
            let number = *number;
            // Strict on the upper side: `i64::MAX as f64` is 2^63, which `as
            // i64` would saturate to a timestamp one short of the one sent.
            (number.is_finite() && number >= i64::MIN as f64 && number < i64::MAX as f64)
                .then_some(number as i64)
        }
        _ => None,
    }
}

/// Borrows a string value and renders the rest, so the common case of a string
/// symbol copies nothing on a path that runs for every symbol of every row.
fn scalar_to_symbol(value: &OwnedValue) -> Option<Cow<'_, str>> {
    match value {
        OwnedValue::Static(StaticNode::Null) => None,
        OwnedValue::Static(StaticNode::Bool(flag)) => Some(Cow::Owned(flag.to_string())),
        OwnedValue::Static(StaticNode::I64(number)) => Some(Cow::Owned(number.to_string())),
        OwnedValue::Static(StaticNode::U64(number)) => Some(Cow::Owned(number.to_string())),
        OwnedValue::Static(StaticNode::F64(number)) => Some(Cow::Owned(number.to_string())),
        OwnedValue::String(text) => Some(Cow::Borrowed(text.as_str())),
        _ => None,
    }
}

/// Defers to the client's own character rule so the two cannot drift, and adds
/// the length limit, which `ColumnName` does not carry: it lives on the buffer,
/// so without this an over-long name is only refused once the row is half
/// written.
fn validate_name(name: &str) -> Result<(), RowError> {
    if name.len() > MAX_NAME_LEN {
        return Err(RowError::Invalid(format!(
            "column {name} is longer than the {MAX_NAME_LEN} bytes QuestDB allows"
        )));
    }
    ColumnName::new(name)
        .map(|_| ())
        .map_err(|error| RowError::Invalid(format!("column {name} is not a valid name: {error}")))
}

/// A numeric array shaped for the client, by nesting depth.
///
/// QuestDB stores only `DOUBLE` arrays, so integer input is widened. Nesting is
/// supported to three dimensions, which is the limit of the client's
/// slice-based array API.
enum ArrayValues {
    One(Vec<f64>),
    Two(Vec<Vec<f64>>),
    Three(Vec<Vec<Vec<f64>>>),
}

/// Builds the array the write pass will hand to the client.
///
/// Building it is what checks its shape, so the rules cannot be held in two
/// places that have to agree by hand.
fn parse_array(name: &str, items: &[OwnedValue]) -> Result<(ArrayValues, bool), RowError> {
    let not_numeric = || RowError::Invalid(format!("column {name} is not a numeric array"));
    // QuestDB arrays are rectangular: every sibling must have the same length,
    // or the client refuses the whole frame.
    let ragged = || RowError::Invalid(format!("column {name} is not a rectangular array"));
    // An empty array carries no values and no shape, so there is nothing to
    // store and nothing to infer a column type from.
    if items.is_empty() {
        return Err(RowError::Invalid(format!(
            "column {name} is an empty array"
        )));
    }
    // Whether any element lost precision on its way to a `DOUBLE`, so the
    // caller can say so once.
    let mut rounded = false;
    match array_depth(items) {
        1 => Ok((
            ArrayValues::One(flat_array(items, &mut rounded).ok_or_else(not_numeric)?),
            rounded,
        )),
        2 => {
            let mut rows = Vec::with_capacity(items.len());
            let mut width = None;
            for item in items {
                let values = item
                    .as_array()
                    .and_then(|values| flat_array(values, &mut rounded))
                    .ok_or_else(not_numeric)?;
                if *width.get_or_insert(values.len()) != values.len() {
                    return Err(ragged());
                }
                rows.push(values);
            }
            Ok((ArrayValues::Two(rows), rounded))
        }
        3 => {
            let mut cubes = Vec::with_capacity(items.len());
            let mut plane = None;
            let mut width = None;
            for item in items {
                let outer = item.as_array().ok_or_else(not_numeric)?;
                if *plane.get_or_insert(outer.len()) != outer.len() {
                    return Err(ragged());
                }
                let mut rows = Vec::with_capacity(outer.len());
                for nested in outer {
                    let values = nested
                        .as_array()
                        .and_then(|values| flat_array(values, &mut rounded))
                        .ok_or_else(not_numeric)?;
                    if *width.get_or_insert(values.len()) != values.len() {
                        return Err(ragged());
                    }
                    rows.push(values);
                }
                cubes.push(rows);
            }
            Ok((ArrayValues::Three(cubes), rounded))
        }
        _ => Err(RowError::Invalid(format!(
            "column {name} exceeds the supported array nesting depth of 3"
        ))),
    }
}

fn write_array(buffer: &mut Buffer, name: &str, values: &ArrayValues) -> Result<(), RowError> {
    match values {
        ArrayValues::One(values) => buffer.column_arr(name, values)?,
        ArrayValues::Two(rows) => buffer.column_arr(name, rows)?,
        ArrayValues::Three(cubes) => buffer.column_arr(name, cubes)?,
    };
    Ok(())
}

fn array_depth(items: &[OwnedValue]) -> usize {
    match items.first() {
        Some(OwnedValue::Array(nested)) => 1 + array_depth(nested),
        _ => 1,
    }
}

/// Widens one level of a numeric array, setting `rounded` when an integer
/// element lost precision.
fn flat_array(items: &[OwnedValue], rounded: &mut bool) -> Option<Vec<f64>> {
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        let number = match item {
            OwnedValue::Static(StaticNode::I64(number)) => i128::from(*number),
            OwnedValue::Static(StaticNode::U64(number)) => i128::from(*number),
            OwnedValue::Static(StaticNode::F64(number)) => {
                values.push(*number);
                continue;
            }
            _ => return None,
        };
        let (widened, exact) = widen_exact(number);
        *rounded |= !exact;
        values.push(widened);
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
    // `as_u64_pair` is `(most significant, least significant)`, so the halves
    // are swapped into the `(lo, hi)` order `column_uuid` takes.
    let (hi, lo) = Uuid::parse_str(text).ok()?.as_u64_pair();
    Some((lo, hi))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use iggy::prelude::{HeaderKey, HeaderKind, HeaderValue};
    use questdb::ingress::ProtocolVersion;

    use super::*;

    /// The buffer production actually writes into. Prefer it for anything that
    /// could depend on columnar behaviour: it pins one type per column for the
    /// buffer's lifetime, resolves names case-insensitively and silently keeps
    /// the first write when a column is repeated in a row. The ILP buffer has
    /// none of those rules, so a test that needs them cannot use `buffer()`.
    fn qwp_buffer() -> Buffer {
        Buffer::qwp_ws_with_max_name_len(MAX_NAME_LEN)
    }

    /// `Buffer::new` yields an ILP buffer, which is what makes the rendering
    /// assertions possible: ILP is inspectable through `as_bytes`, so the exact
    /// wire output can be asserted, while the QWP buffer exposes nothing. Use it
    /// only where the assertion is about what was written rather than about
    /// columnar rules, and see `qwp_buffer` for the rest.
    /// V1 is the all-text InfluxDB-compatible encoding. V2 and V3 write
    /// doubles as binary, which would make `as_bytes` unreadable here.
    fn buffer() -> Buffer {
        Buffer::new(ProtocolVersion::V1)
    }

    fn mapping() -> Mapping {
        Mapping {
            id: 1,
            table: "events".to_owned(),
            symbol_columns: ColumnNames::default(),
            uuid_columns: ColumnNames::default(),
            integer_columns: ColumnNames::default(),
            timestamp_source: TimestampSource::Message,
            timestamp_field: None,
            timestamp_unit: TimestampUnit::Auto,
            include_stream_column: false,
            include_topic_column: false,
            include_partition_column: false,
            include_offset_column: false,
            include_headers: false,
            // The production default, so the common path is what the tests
            // exercise. The tests that pin integer typing set it back.
            numbers_as_double: true,
            rounding_warned: Default::default(),
        }
    }

    /// Whether any rounding warning fired.
    fn warned(mapping: &Mapping) -> bool {
        mapping
            .rounding_warned
            .iter()
            .any(|flag| flag.load(Ordering::Relaxed))
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
    fn given_non_object_json_payload_when_appending_should_take_the_payload_column() {
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &json_message("[1,2,3]"), context())
            .expect("a JSON array has no fields to map, so it takes the payload column");

        let line = rendered(&buffer);
        assert!(line.contains("payload="), "{line}");
        assert!(line.contains("[1,2,3]"), "{line}");
    }

    #[test]
    fn given_scalar_json_payload_when_appending_should_take_the_payload_column() {
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &json_message("42"), context())
            .expect("a JSON scalar has no fields to map, so it takes the payload column");

        assert!(rendered(&buffer).contains("payload=\"42\""));
    }

    #[test]
    fn given_two_fields_matching_the_timestamp_field_when_appending_should_reject_the_row() {
        // QuestDB resolves a column name without regard to case, so `ts` and
        // `TS` are one column and one of the two values would be dropped.
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("ts".to_owned());
        let mut buffer = qwp_buffer();

        let error = mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"ts":1700000000000000,"TS":1800000000000000,"price":1.5}"#),
                context(),
            )
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "{error:?}");
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_message_timestamp_above_the_client_range_when_appending_should_reject_the_row() {
        // `Buffer::at` refuses a negative value without rolling the row back, so
        // this is the one input that could leave a partial row behind.
        let mut message = json_message(r#"{"price":1.5}"#);
        message.timestamp = i64::MAX as u64 + 1;
        let mut buffer = qwp_buffer();

        let error = mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "{error:?}");
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_origin_timestamp_above_the_client_range_when_appending_should_reject_the_row() {
        // `origin_timestamp` is producer-supplied, so this one is reachable from
        // outside the server.
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Origin;
        let mut message = json_message(r#"{"price":1.5}"#);
        message.origin_timestamp = u64::MAX;
        let mut buffer = qwp_buffer();

        let error = mapping
            .append_row(&mut buffer, &message, context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "{error:?}");
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_mid_row_failure_when_appending_should_leave_the_buffer_untouched() {
        // The bad UUID sits after fields that would already have been encoded,
        // so this pins the guarantee that validation runs before any write
        // rather than being rolled back afterwards. It runs on the buffer
        // production uses: the ILP buffer rolls a refused row back by itself, so
        // it cannot tell whether the guarantee holds.
        let mut mapping = mapping();
        mapping.symbol_columns.insert("side".to_owned());
        mapping.uuid_columns.insert("trade_id".to_owned());
        let mut buffer = qwp_buffer();

        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"side":"buy","price":1.0}"#),
                context(),
            )
            .unwrap();
        assert_eq!(buffer.row_count(), 1);

        let error = mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"side":"sell","price":2.0,"trade_id":"not-a-uuid"}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)));
        assert_eq!(
            buffer.row_count(),
            1,
            "rejected row must not reach the buffer"
        );

        // The buffer is still usable for the next record, which it would not be
        // if the rejection had left a half-written row behind.
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
    fn given_field_name_the_client_rejects_when_appending_should_reject_the_row() {
        // A dot is illegal in a column name. Validating names up front turns
        // what the client would raise mid-row into an ordinary row rejection,
        // so the rest of the batch still flushes.
        let mapping = mapping();
        let mut buffer = buffer();

        let error = mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"price.usd":1.0}"#),
                context(),
            )
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
        assert_eq!(buffer.row_count(), 0);

        mapping
            .append_row(&mut buffer, &json_message(r#"{"price":2.0}"#), context())
            .unwrap();
        assert_eq!(buffer.row_count(), 1);
    }

    /// Each of these reached the QuestDB client as a write-time failure before
    /// validation covered it, and a client failure costs the batch a rebuild
    /// rather than just the row. They are grouped so a regression in any one of
    /// them is visible as a change of error kind, not only of message.
    #[test]
    fn given_inputs_the_client_would_refuse_when_appending_should_reject_the_row() {
        let long_name = "a".repeat(MAX_NAME_LEN + 1);
        let cases: &[(&str, Mapping, String)] = &[
            (
                "name longer than the column limit",
                mapping(),
                format!(r#"{{"{long_name}":1}}"#),
            ),
            (
                "array rows of differing length",
                mapping(),
                r#"{"m":[[1,2],[3]]}"#.to_owned(),
            ),
            (
                "array planes of differing length",
                mapping(),
                r#"{"m":[[[1,2]],[[3,4],[5,6]]]}"#.to_owned(),
            ),
        ];

        for (label, mapping, payload) in cases {
            let mut buffer = qwp_buffer();
            let error = mapping
                .append_row(&mut buffer, &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(error, RowError::Invalid(_)),
                "{label} should be an up-front rejection, got {error:?}"
            );
            assert_eq!(buffer.row_count(), 0, "{label} must not write a row");
        }
    }

    #[test]
    fn given_payload_timestamp_out_of_range_when_appending_should_reject_the_row() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("ts".to_owned());

        // Negative, and the extreme that used to panic in `abs`.
        for payload in [
            r#"{"ts":-1,"price":1.0}"#,
            r#"{"ts":-9223372036854775808,"price":1.0}"#,
        ] {
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
        }

        // 1972 in milliseconds looks like seconds, and scaling it up overflows.
        // Reported rather than saturated to a date centuries away.
        let error = mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"ts":63072000000,"price":1.0}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
    }

    #[test]
    fn given_column_name_collision_when_appending_should_reject_the_row() {
        // QuestDB matches column names case-insensitively, so each of these is
        // one column receiving two values. The client keeps the first and
        // discards the second without reporting it.
        let with_stream_column = || {
            let mut mapping = mapping();
            mapping.include_stream_column = true;
            mapping
        };
        let against_the_stream_symbol = || {
            let mut mapping = with_stream_column();
            mapping.symbol_columns.insert("stream".to_owned());
            mapping
        };

        let cases: &[(&str, Mapping, &str)] = &[
            (
                "payload field against the stream column",
                with_stream_column(),
                r#"{"stream":"web","price":1.0}"#,
            ),
            (
                "payload field against the stream symbol",
                against_the_stream_symbol(),
                r#"{"stream":"web"}"#,
            ),
            (
                "two fields differing only in case",
                with_stream_column(),
                r#"{"Price":1.0,"price":2.0}"#,
            ),
        ];

        for (label, mapping, payload) in cases {
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(error, RowError::Invalid(_)),
                "{label} should be rejected rather than silently deduped, got {error:?}"
            );
        }
    }

    #[test]
    fn given_non_scalar_in_a_symbol_column_when_appending_should_reject_the_row() {
        // Without this the value is dropped and the row still counts as written,
        // so declaring a field as a symbol would quietly discard data the
        // default path keeps as JSON text.
        let mut mapping = mapping();
        mapping.symbol_columns.insert("region".to_owned());

        for payload in [
            r#"{"region":{"code":"eu"},"price":1.0}"#,
            r#"{"region":["eu","west"],"price":1.0}"#,
        ] {
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
        }

        // A null symbol stays an omission rather than becoming a rejection.
        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"region":null,"price":1.0}"#),
                context(),
            )
            .unwrap();
    }

    #[test]
    fn given_configured_column_differing_in_case_when_appending_should_still_match() {
        // QuestDB resolves a column name without regard to case, so a
        // configured name has to match a field that differs only in case.
        // Matching case-sensitively would write the field as a plain column and
        // report nothing.
        let mut mapping = mapping();
        mapping.symbol_columns.insert("region".to_owned());
        mapping.uuid_columns.insert("trade_id".to_owned());

        // The uuid column matches, so a bad value there is rejected.
        let error = mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"Trade_Id":"not-a-uuid"}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");

        // The symbol column matches, so a non-scalar there is rejected.
        let error = mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"REGION":{"a":1},"price":1.0}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
    }

    #[test]
    fn given_payload_timestamp_field_differing_in_case_when_appending_should_still_match() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("event_time".to_owned());

        // Found despite the case difference, so the row is written and the
        // field is not also emitted as a data column.
        let mut buffer = qwp_buffer();
        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"Event_Time":1788523200000000,"price":1.0}"#),
                context(),
            )
            .unwrap();
        assert_eq!(buffer.row_count(), 1);
    }

    #[test]
    fn given_non_string_in_a_uuid_column_when_appending_should_reject_the_row() {
        // Without this the value is written as a LONG or a BOOLEAN into a column
        // the operator declared as a UUID.
        let mut mapping = mapping();
        mapping.uuid_columns.insert("trade_id".to_owned());

        for payload in [
            r#"{"trade_id":7,"price":1.0}"#,
            r#"{"trade_id":true,"price":1.0}"#,
            r#"{"trade_id":[1,2],"price":1.0}"#,
        ] {
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(error, RowError::Invalid(_)),
                "{payload} should be rejected, got {error:?}"
            );
        }

        // A null stays an omission.
        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"trade_id":null,"price":1.0}"#),
                context(),
            )
            .unwrap();
    }

    #[test]
    fn given_unusable_float_timestamp_when_appending_should_reject_the_row() {
        // `as i64` turns NaN into zero and saturates a huge value, so both would
        // store a wrong time with no error.
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(f64::NAN))),
            None
        );
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(f64::INFINITY))),
            None
        );
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(1e30))),
            None
        );
        // An ordinary float still works, truncated to whole nanoseconds.
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(1_788_523_200.5))),
            Some(1_788_523_200)
        );
    }

    #[test]
    fn given_array_shapes_when_appending_should_accept_rectangular_and_reject_the_rest() {
        let mapping = mapping();
        let rejected = [
            (r#"{"m":[]}"#, "empty"),
            (r#"{"m":[[1,2],[3]]}"#, "ragged rows"),
            (r#"{"m":[[[1,2]],[[3,4],[5,6]]]}"#, "ragged planes"),
            (r#"{"m":[[[[1]]]]}"#, "depth four"),
            (r#"{"m":[1,"two"]}"#, "mixed types"),
            (r#"{"m":[1,[2]]}"#, "mixed nesting"),
        ];
        for (payload, label) in rejected {
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(error, RowError::Invalid(_)),
                "{label} should be rejected, got {error:?}"
            );
        }

        let accepted = [
            (r#"{"m":[1,2,3]}"#, "one dimension"),
            (r#"{"m":[[1,2],[3,4]]}"#, "two dimensions"),
            (r#"{"m":[[[1,2]],[[3,4]]]}"#, "three dimensions"),
        ];
        for (payload, label) in accepted {
            let mut buffer = qwp_buffer();
            mapping
                .append_row(&mut buffer, &json_message(payload), context())
                .unwrap_or_else(|error| panic!("{label} was rejected: {error:?}"));
            assert_eq!(buffer.row_count(), 1, "{label} did not write a row");
        }
    }

    #[test]
    fn given_u64_above_i64_max_when_appending_should_widen_to_double() {
        // QuestDB has no unsigned 64-bit column, so a value past `i64::MAX`
        // degrades to a double rather than wrapping into a negative.
        let mut buffer = buffer();
        mapping()
            .append_row(
                &mut buffer,
                &json_message(r#"{"fingerprint":18446744073709551615}"#),
                context(),
            )
            .unwrap();
        let line = rendered(&buffer);
        assert!(
            line.contains("fingerprint=1.8446744073709552e19"),
            "expected a double rendering, got {line}"
        );
    }

    #[test]
    fn given_unset_message_timestamp_when_appending_should_use_the_server_clock() {
        // Apache Iggy writes zero for an unset timestamp, which would otherwise
        // stamp the row at the Unix epoch.
        let before = TimestampMicros::now();
        let mut message = json_message(r#"{"price":1.0}"#);
        message.timestamp = 0;
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap();
        let rendered = rendered(&buffer);
        let stamped: i64 = rendered
            .rsplit(' ')
            .next()
            .expect("no timestamp")
            .trim()
            .parse()
            .expect("timestamp is not a number");
        assert!(
            stamped >= before.as_i64() * 1000,
            "expected the server clock, got {stamped}"
        );
    }

    #[test]
    fn given_non_string_scalars_in_a_symbol_column_when_appending_should_render_them() {
        let mut mapping = mapping();
        mapping.symbol_columns.insert("flag".to_owned());
        mapping.symbol_columns.insert("count".to_owned());
        let mut buffer = buffer();
        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"flag":true,"count":7,"price":1.0}"#),
                context(),
            )
            .unwrap();
        let line = rendered(&buffer);
        assert!(line.contains("flag=true"), "{line}");
        assert!(line.contains("count=7"), "{line}");
    }

    #[test]
    fn given_a_client_rejection_when_appending_should_leave_the_buffer_usable() {
        // The caller keeps the same buffer after a client rejection, which only
        // holds because the client rolls the half-written row back itself before
        // returning the error. Pin that: the rejected row leaves no trace and the
        // next record still writes.
        let mut mapping = mapping();
        // Mixed numbers are the cheapest way to make the client refuse a row,
        // and the default resolves them, so this opts out of that.
        mapping.numbers_as_double = false;
        let mut buffer = qwp_buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21.5}"#), context())
            .unwrap();
        assert_eq!(buffer.row_count(), 1);

        // Conflicts with the type the first row pinned.
        let error = mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21}"#), context())
            .unwrap_err();
        assert!(matches!(error, RowError::Client(_)), "got {error:?}");
        assert_eq!(
            buffer.row_count(),
            1,
            "the refused row must not be left half written"
        );

        // The same buffer still takes a well-formed record.
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":22.5}"#), context())
            .unwrap();
        assert_eq!(buffer.row_count(), 2, "the buffer must still be usable");
    }

    #[test]
    fn given_numbers_as_double_when_appending_should_make_the_type_follow_the_name() {
        // Without this, an integer and a decimal for the same field pin two
        // different column types, and the client refuses whichever came second.
        let mut mapping = mapping();
        mapping.numbers_as_double = true;
        let mut buffer = qwp_buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21}"#), context())
            .unwrap();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21.5}"#), context())
            .unwrap();
        assert_eq!(
            buffer.row_count(),
            2,
            "both rows should share one column type"
        );
    }

    #[test]
    fn given_the_default_when_appending_mixed_numbers_should_accept_both() {
        // JSON writes 21.0 as 21, so one field arriving whole and fractional is
        // ordinary data rather than a producer mistake. The default has to take
        // both, because which one the buffer saw first is an accident of
        // batching.
        let mapping = mapping();
        let mut buffer = qwp_buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21}"#), context())
            .unwrap();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21.5}"#), context())
            .unwrap();
        assert_eq!(buffer.row_count(), 2);
    }

    #[test]
    fn given_numbers_as_double_off_when_appending_mixed_numbers_should_conflict() {
        // Opting out is what reintroduces the conflict, which is the cost the
        // README names for keeping integers exact without naming the columns.
        let mut mapping = mapping();
        mapping.numbers_as_double = false;
        let mut buffer = qwp_buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21}"#), context())
            .unwrap();
        let error = mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21.5}"#), context())
            .unwrap_err();
        assert!(matches!(error, RowError::Client(_)), "got {error:?}");
    }

    #[test]
    fn given_an_integer_column_when_appending_should_keep_it_exact() {
        // A column named in integer_columns stays LONG while every other number
        // is a DOUBLE, so an identifier past 2^53 survives the round trip.
        let mut mapping = mapping();
        mapping.integer_columns.insert("trade_id".to_owned());
        let mut buffer = buffer();
        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"trade_id":9007199254740993,"price":1.5}"#),
                context(),
            )
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("trade_id=9007199254740993i"), "{line}");
        assert!(line.contains("price=1.5"), "{line}");
    }

    #[test]
    fn given_an_integer_column_holding_a_whole_float_when_appending_should_accept_it() {
        // JSON gives no way to tell 20 from 20.0, so a whole float in a declared
        // integer column is the same value and must not cost the record.
        let mut mapping = mapping();
        mapping.integer_columns.insert("count".to_owned());
        let mut buffer = buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"count":20.0}"#), context())
            .unwrap();

        assert!(rendered(&buffer).contains("count=20i"));
    }

    #[test]
    fn given_an_integer_column_holding_a_non_number_when_appending_should_reject_the_row() {
        // Any other type would create or pin the declared LONG column as
        // something else, which is the same harm the uuid_columns guard
        // prevents, so every non-numeric variant is refused before a write.
        for payload in [
            r#"{"count":"20"}"#,
            r#"{"count":true}"#,
            r#"{"count":[1,2]}"#,
            r#"{"count":{"n":1}}"#,
        ] {
            let mut mapping = mapping();
            mapping.integer_columns.insert("count".to_owned());
            let mut buffer = qwp_buffer();
            let error = mapping
                .append_row(&mut buffer, &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(&error, RowError::Invalid(reason) if reason.contains("not a number")),
                "{payload}: {error:?}"
            );
            assert_eq!(buffer.row_count(), 0, "{payload}");
        }
    }

    #[test]
    fn given_an_integer_column_rejection_when_logged_should_not_carry_the_value() {
        // The reason reaches the error log whatever log_rejected_payload says,
        // so it names the column and never the payload value.
        for (payload, value) in [
            (r#"{"count":20.5}"#, "20.5"),
            (r#"{"count":18446744073709551615}"#, "18446744073709551615"),
            (r#"{"count":"20"}"#, "\"20\""),
        ] {
            let mut mapping = mapping();
            mapping.integer_columns.insert("count".to_owned());
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            let RowError::Invalid(reason) = error else {
                panic!("{payload}: expected a mapping rejection, got {error:?}");
            };
            assert!(
                reason.contains("count") && !reason.contains(value),
                "{payload}: the reason must name the column and not the value: {reason}"
            );
        }
    }

    #[test]
    fn given_a_number_past_2_to_53_when_widened_should_set_the_warned_flag() {
        // The README states the limit; this is the runtime signal for an
        // operator who missed it. The crate has no `tracing` subscriber in its
        // dev-dependencies, so the flag is what the test can observe: it
        // proves the lossy conversion was detected, not that the line was
        // written exactly once.
        let mapping = mapping();
        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"small":42,"temp":20.5}"#),
                context(),
            )
            .unwrap();
        assert!(!warned(&mapping), "an exact widening must not warn");

        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"big":9007199254740993}"#),
                context(),
            )
            .unwrap();
        assert!(warned(&mapping));

        // A declared integer column is never widened, so it never trips it.
        let mut declared = Mapping {
            rounding_warned: Default::default(),
            ..mapping
        };
        declared.integer_columns.insert("big".to_owned());
        declared
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"big":9007199254740993}"#),
                context(),
            )
            .unwrap();
        assert!(!warned(&declared));
    }

    #[test]
    fn given_the_extreme_integers_when_widened_should_still_count_as_rounded() {
        // `i64::MAX as f64` is 2^63 and `as i64` saturates back to `i64::MAX`,
        // so a check through the source width would call that exact. The
        // check goes through `i128`, where nothing saturates.
        for payload in [
            r#"{"big":9223372036854775807}"#,
            r#"{"big":18446744073709551615}"#,
        ] {
            let mapping = mapping();
            mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap();
            assert!(warned(&mapping), "{payload} must count as rounded");
        }
    }

    #[test]
    fn given_an_array_element_past_2_to_53_when_appending_should_set_the_warned_flag() {
        // The array path widens too, so it has to report the loss as well.
        let lossy = mapping();
        lossy
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"ids":[1,9007199254740993]}"#),
                context(),
            )
            .unwrap();
        assert!(warned(&lossy));

        let exact = mapping();
        exact
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"ids":[1,2,3]}"#),
                context(),
            )
            .unwrap();
        assert!(!warned(&exact));
    }

    #[test]
    fn given_an_array_rounding_first_when_a_scalar_rounds_later_should_still_warn_for_the_scalar() {
        // Each source has its own flag, so the array warning cannot use up the
        // one that points a scalar column at integer_columns.
        let mapping = mapping();
        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"ids":[9007199254740993]}"#),
                context(),
            )
            .unwrap();
        assert!(mapping.rounding_warned[Rounding::ArrayElement as usize].load(Ordering::Relaxed));
        assert!(!mapping.rounding_warned[Rounding::Scalar as usize].load(Ordering::Relaxed));

        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"big":9007199254740993}"#),
                context(),
            )
            .unwrap();
        assert!(mapping.rounding_warned[Rounding::Scalar as usize].load(Ordering::Relaxed));
    }

    #[test]
    fn given_an_integer_column_holding_2_to_63_as_a_float_when_appending_should_reject_the_row() {
        // `i64::MAX as f64` rounds up to 2^63, so an inclusive bound would admit
        // it and `as i64` would store one less than the producer sent. The
        // same value as an integer literal is refused, and the two must agree.
        let mut mapping = mapping();
        mapping.integer_columns.insert("count".to_owned());
        let mut buffer = qwp_buffer();
        let error = mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"count":9223372036854775808.0}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)), "{error:?}");
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_a_float_timestamp_of_2_to_63_when_resolved_should_be_out_of_range() {
        // The same strict bound as the integer column, for the same reason.
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(9223372036854775808.0))),
            None
        );
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(1.0))),
            Some(1)
        );
    }

    #[test]
    fn given_a_raw_header_when_appending_should_write_it_as_base64() {
        // `to_string_value` Debug-formats a Raw value, which would store the
        // literal text b"\x01\x02\xff"; base64 keeps the bytes recoverable.
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"price":1.5}"#);
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_raw(HeaderKind::String, b"bin").unwrap(),
            HeaderValue::from_raw(HeaderKind::Raw, &[1, 2, 255]).unwrap(),
        );
        message.headers = Some(headers);
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &message, context())
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("header_bin=\"AQL/\""), "{line}");
    }

    #[test]
    fn given_an_over_long_column_name_when_appending_should_say_bytes() {
        // The check counts bytes, which matches the client and the server
        // limit, so the message says bytes rather than characters.
        let name = "é".repeat(64);
        let payload = format!(r#"{{"{name}":1}}"#);
        let error = mapping()
            .append_row(&mut qwp_buffer(), &json_message(&payload), context())
            .unwrap_err();
        let RowError::Invalid(reason) = error else {
            panic!("expected a mapping rejection, got {error:?}");
        };
        assert!(reason.contains("bytes QuestDB allows"), "{reason}");
    }

    #[test]
    fn given_an_integer_column_holding_a_fraction_when_appending_should_reject_the_row() {
        // Rounding would store a value the producer never sent, and sending a
        // DOUBLE would break the type the declaration pinned.
        let mut mapping = mapping();
        mapping.integer_columns.insert("count".to_owned());
        let mut buffer = qwp_buffer();
        let error = mapping
            .append_row(&mut buffer, &json_message(r#"{"count":20.5}"#), context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_proto_payload_holding_json_when_appending_should_take_the_field_path() {
        // A transform that cannot encode against its descriptor hands the sink
        // proto-tagged JSON text. Treating that as one opaque column would lose
        // the field structure the document carries.
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Proto(r#"{"side":"buy","price":1.5}"#.to_owned());
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap();
        let line = rendered(&buffer);
        assert!(line.contains("price=1.5"), "{line}");
        assert!(!line.contains("payload="), "{line}");
    }

    #[test]
    fn given_proto_payload_without_json_when_appending_should_use_the_payload_column() {
        // Text that is not a document still belongs in the single column.
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Proto("not a document".to_owned());
        let mut buffer = buffer();
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
    fn given_mixed_case_enum_names_when_parsing_should_be_accepted() {
        // Both parsers lowercase the input, so an operator writing "Payload" or
        // "MILLIS" gets what they meant rather than a configuration error.
        assert_eq!(
            TimestampSource::parse(Some("PAYLOAD")),
            Some(TimestampSource::Payload)
        );
        assert_eq!(
            TimestampSource::parse(Some("Origin")),
            Some(TimestampSource::Origin)
        );
        assert_eq!(
            TimestampUnit::parse(Some("MILLIS")),
            Some(TimestampUnit::Millis)
        );
        assert_eq!(
            TimestampUnit::parse(Some("Nanos")),
            Some(TimestampUnit::Nanos)
        );
    }

    #[test]
    fn given_row_with_no_columns_when_appending_should_reject_the_row() {
        // Every QuestDB row needs a column before its designated timestamp.
        let mut mapping = mapping();
        mapping.include_stream_column = false;
        mapping.include_topic_column = false;
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("ts".to_owned());

        let error = mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"ts":1700000000}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
    }

    #[test]
    fn given_ordinary_rows_when_appending_to_the_production_buffer_should_succeed() {
        // The guard for the checks above: they must not reject valid rows.
        let mut mapping = mapping();
        mapping.symbol_columns.insert("region".to_owned());
        mapping.include_stream_column = true;
        mapping.include_topic_column = true;

        for payload in [
            r#"{"region":"eu","price":1.0}"#,
            r#"{"price":1.0,"missing":null}"#,
            r#"{"m":[[1,2],[3,4]]}"#,
            r#"{"nested":{"a":1},"price":2.0}"#,
        ] {
            let mut buffer = qwp_buffer();
            mapping
                .append_row(&mut buffer, &json_message(payload), context())
                .unwrap_or_else(|error| panic!("{payload} was rejected: {error:?}"));
            assert_eq!(buffer.row_count(), 1, "{payload} did not write a row");
        }
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
    fn given_structureless_payload_with_bad_header_name_when_appending_should_reject_the_row() {
        // Headers are written for structureless payloads too, so validation
        // has to reach them on that path as well as the field path.
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Text("body".to_owned());
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_raw(HeaderKind::String, b"a.b").unwrap(),
            HeaderValue::from_raw(HeaderKind::String, b"v").unwrap(),
        );
        message.headers = Some(headers);
        let mut buffer = buffer();

        let error = mapping
            .append_row(&mut buffer, &message, context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
        assert_eq!(buffer.row_count(), 0);
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
        assert_eq!(TimestampUnit::Auto.to_nanos(1_788_523_200), Some(nanos));
        assert_eq!(TimestampUnit::Auto.to_nanos(1_788_523_200_000), Some(nanos));
        assert_eq!(
            TimestampUnit::Auto.to_nanos(1_788_523_200_000_000),
            Some(nanos)
        );
        assert_eq!(TimestampUnit::Auto.to_nanos(nanos), Some(nanos));
    }

    #[test]
    fn given_explicit_unit_when_converting_should_ignore_magnitude() {
        assert_eq!(TimestampUnit::Seconds.to_nanos(1), Some(1_000_000_000));
        assert_eq!(TimestampUnit::Millis.to_nanos(1), Some(1_000_000));
        assert_eq!(TimestampUnit::Micros.to_nanos(1), Some(1_000));
        assert_eq!(TimestampUnit::Nanos.to_nanos(1), Some(1));
    }

    #[test]
    fn given_i64_min_when_converting_should_not_panic() {
        // `abs` has no positive counterpart for `i64::MIN` and panics under
        // overflow checks, so the bucket test uses `unsigned_abs`. The value
        // falls through as nanoseconds and is refused later for being negative.
        assert_eq!(TimestampUnit::Auto.to_nanos(i64::MIN), Some(i64::MIN));
    }

    #[test]
    fn given_millis_below_the_seconds_threshold_when_auto_should_not_saturate() {
        // 1972-01-01 in milliseconds is small enough to look like seconds.
        // Multiplying it up overflows, which is reported rather than saturating
        // to a timestamp centuries in the future.
        assert_eq!(TimestampUnit::Auto.to_nanos(63_072_000_000), None);
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
