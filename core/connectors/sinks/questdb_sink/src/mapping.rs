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

use iggy_connector_sdk::{ConsumedMessage, Payload};
use questdb::ingress::{Buffer, ColumnName, TimestampMicros, TimestampNanos};
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
const MAX_COLUMN_NAME_LEN: usize = 127;

/// A message that could not be turned into a row. The batch continues; the
/// caller counts and logs these.
#[derive(Debug)]
pub enum RowError {
    /// The payload was not a JSON object, or a required field was missing.
    /// Caught before the first write, so the buffer is untouched and the batch
    /// continues.
    Invalid(String),
    /// The write pass refused a record that validation accepted, so the two
    /// passes disagree and the buffer can hold a partial row. Unreachable while
    /// they agree, and recovered the same way as a client rejection.
    Drift(String),
    /// The QuestDB client rejected the row. Carries the underlying error so the
    /// caller can decide whether it is transient.
    Client(questdb::Error),
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

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

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
    pub table: String,
    pub symbol_columns: ColumnNames,
    pub uuid_columns: ColumnNames,
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
    /// JSON carries one number type, so a producer that emits `2` for a whole
    /// value and `2.5` for a fractional one makes the column type depend on
    /// whichever record defined the column first. The client pins that type for
    /// the rest of the buffer and refuses the records that disagree. Turning
    /// this on makes the type a function of the column name instead, at the cost
    /// of storing integers as doubles, which loses exactness above 2^53.
    pub numbers_as_double: bool,
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
    /// The message is validated in full before the first write, so a rejected
    /// message is skipped without ever having touched the buffer. Rolling a
    /// half-written row back instead would mean a marker per row, and on the
    /// QWP/WebSocket path setting a marker clones every table buffered so far,
    /// which makes building a batch quadratic in its length.
    pub fn append_row(
        &self,
        buffer: &mut Buffer,
        message: &ConsumedMessage,
        context: RowContext<'_>,
    ) -> Result<(), RowError> {
        // Resolved once and shared by both passes. `json_document` parses proto
        // text that carries a JSON document, so doing this per pass would parse
        // such a payload twice.
        let document = message.payload.json_document();
        let fields =
            match document.as_deref() {
                Some(value) => Some(value.as_object().ok_or_else(|| {
                    RowError::Invalid("JSON payload is not an object".to_owned())
                })?),
                None => None,
            };
        self.validate_row(message, fields)?;
        self.append_row_inner(buffer, message, fields, context)
            .map_err(|error| match error {
                RowError::Invalid(reason) => RowError::Drift(reason),
                other => other,
            })
    }

    /// Rejects every record the write path would refuse, on the same terms, so
    /// that `append_row_inner` can only fail on a client error. Keep the two in
    /// step: a check that exists only in the write path can abandon a partial
    /// row.
    fn validate_row(
        &self,
        message: &ConsumedMessage,
        fields: Option<&simd_json::owned::Object>,
    ) -> Result<(), RowError> {
        // QuestDB resolves column names case-insensitively, so two fields that
        // differ only in case are one column and the second value would be
        // dropped without a word. The claimed names are borrowed and compared in
        // place: a row carries a handful of columns, so scanning that list costs
        // less than hashing a lowercased copy of every name, and it allocates
        // nothing on a path that runs for every field of every row.
        let mut seen: Vec<&str> = Vec::new();
        let mut columns = 0usize;
        for reserved in self.reserved_columns() {
            seen.push(reserved);
            columns += 1;
        }

        match fields {
            Some(fields) => {
                for (name, value) in fields {
                    if self.is_timestamp_field(name) {
                        continue;
                    }
                    if self.symbol_columns.contains(name.as_str()) {
                        // A symbol has to be scalar. Dropping a structured
                        // value silently would also mean that listing a field
                        // in `symbol_columns` quietly discards data the default
                        // path would have kept as JSON text.
                        if !is_symbol_scalar(value) {
                            if matches!(value, OwnedValue::Static(StaticNode::Null)) {
                                continue;
                            }
                            return Err(RowError::Invalid(format!(
                                "column {name} is listed in symbol_columns but its value is not a scalar"
                            )));
                        }
                        self.claim_column(name.as_str(), &mut seen, &mut columns)?;
                        continue;
                    }
                    if matches!(value, OwnedValue::Static(StaticNode::Null)) {
                        // Encoded by omitting the column, so it claims nothing.
                        continue;
                    }
                    self.claim_column(name.as_str(), &mut seen, &mut columns)?;
                    self.validate_column(name.as_str(), value)?;
                }
            }
            None => {
                self.payload_text(message)?;
                columns += 1;
            }
        }

        // The prefixed names are owned, so they are built into a local that
        // outlives the scan above. Only built when headers are enabled, and the
        // write pass needs the same strings anyway.
        let header_columns: Vec<String> = if self.include_headers {
            message
                .headers
                .as_ref()
                .map(|headers| {
                    headers
                        .keys()
                        .map(|key| format!("header_{}", key.to_string_value()))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        for column in &header_columns {
            self.claim_column(column, &mut seen, &mut columns)?;
        }

        // Every QuestDB row needs at least one symbol or column before its
        // designated timestamp; `at` is refused otherwise.
        if columns == 0 {
            return Err(RowError::Invalid(
                "row would have no columns, so QuestDB cannot accept it".to_owned(),
            ));
        }

        self.validate_timestamp(fields)
    }

    /// Column names this connector emits itself for every row.
    fn reserved_columns(&self) -> impl Iterator<Item = &'static str> {
        [
            ("stream", self.include_stream_column),
            ("topic", self.include_topic_column),
            ("partition_id", self.include_partition_column),
            ("offset", self.include_offset_column),
        ]
        .into_iter()
        .filter_map(|(name, included)| included.then_some(name))
    }

    /// Records that `name` will occupy a column, rejecting a case-insensitive
    /// collision with one already claimed.
    fn claim_column<'n>(
        &self,
        name: &'n str,
        seen: &mut Vec<&'n str>,
        columns: &mut usize,
    ) -> Result<(), RowError> {
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
        *columns += 1;
        Ok(())
    }

    /// The caller skips nulls and has already validated the name, so neither is
    /// rechecked here.
    fn validate_column(&self, name: &str, value: &OwnedValue) -> Result<(), RowError> {
        // A column declared as a UUID has to hold one. Writing a number or a
        // boolean there instead creates a column of the wrong type, which the
        // server then refuses for every later row, or silently stores the wrong
        // shape when it creates the column itself.
        if self.uuid_columns.contains(name) {
            let OwnedValue::String(text) = value else {
                return Err(RowError::Invalid(format!(
                    "column {name} is listed in uuid_columns but its value is not a string"
                )));
            };
            return parse_uuid(text)
                .map(|_| ())
                .ok_or_else(|| RowError::Invalid(format!("column {name} is not a valid UUID")));
        }
        match value {
            OwnedValue::Array(items) => parse_array(name, items).map(|_| ()),
            OwnedValue::Object(_) => simd_json::to_string(value)
                .map(|_| ())
                .map_err(|_| RowError::Invalid(format!("column {name} is not serializable"))),
            _ => Ok(()),
        }
    }

    fn validate_timestamp(
        &self,
        fields: Option<&simd_json::owned::Object>,
    ) -> Result<(), RowError> {
        if self.timestamp_source != TimestampSource::Payload {
            return Ok(());
        }
        let field = self
            .timestamp_field
            .as_deref()
            .ok_or_else(|| RowError::Invalid("timestamp_field is not set".to_owned()))?;
        let raw = fields
            .and_then(|fields| field_value(fields, field))
            .ok_or_else(|| RowError::Invalid(format!("timestamp field {field} is missing")))?;
        let number = timestamp_number(raw)
            .ok_or_else(|| RowError::Invalid(format!("timestamp field {field} is not a number")))?;
        // The designated timestamp is refused below zero, and the conversion to
        // nanoseconds has to happen here for the sign to be the one the buffer
        // will see.
        match self.timestamp_unit.to_nanos(number) {
            None => Err(RowError::Invalid(format!(
                "timestamp field {field} is out of range for its unit"
            ))),
            Some(nanos) if nanos < 0 => Err(RowError::Invalid(format!(
                "timestamp field {field} is before the Unix epoch"
            ))),
            Some(_) => Ok(()),
        }
    }

    fn append_row_inner(
        &self,
        buffer: &mut Buffer,
        message: &ConsumedMessage,
        fields: Option<&simd_json::owned::Object>,
        context: RowContext<'_>,
    ) -> Result<(), RowError> {
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
                buffer.column_str("payload", text)?;
            }
        }

        self.append_timestamp(buffer, message, fields)
    }

    fn is_timestamp_field(&self, name: &str) -> bool {
        self.timestamp_source == TimestampSource::Payload
            && self
                .timestamp_field
                .as_deref()
                .is_some_and(|field| field.eq_ignore_ascii_case(name))
    }

    /// `Some` for JSON object payloads, `None` for payloads with no field
    /// structure.
    /// The payload of a record that carries no field structure, borrowed.
    ///
    /// Both passes call this, so the UTF-8 rule lives in one place and neither
    /// pass copies the payload. A JSON payload never reaches it, because the
    /// caller takes the field path for those, and it is reported rather than
    /// asserted so that a future caller cannot turn a mistake into a panic
    /// crossing the plugin boundary.
    fn payload_text<'m>(&self, message: &'m ConsumedMessage) -> Result<&'m str, RowError> {
        match &message.payload {
            Payload::Text(text) | Payload::Proto(text) => Ok(text.as_str()),
            Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
                std::str::from_utf8(bytes)
                    .map_err(|_| RowError::Invalid("payload is not valid UTF-8".to_owned()))
            }
            Payload::Json(_) => Err(RowError::Invalid(
                "a JSON payload takes the field path, not the payload column".to_owned(),
            )),
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
            OwnedValue::Static(StaticNode::I64(number)) if self.numbers_as_double => {
                buffer.column_f64(name, *number as f64)?;
            }
            OwnedValue::Static(StaticNode::U64(number)) if self.numbers_as_double => {
                buffer.column_f64(name, *number as f64)?;
            }
            OwnedValue::Static(StaticNode::I64(number)) => {
                buffer.column_i64(name, *number)?;
            }
            OwnedValue::Static(StaticNode::U64(number)) => {
                // QuestDB has no unsigned 64-bit column; anything past i64::MAX
                // would wrap, so it degrades to DOUBLE rather than corrupting.
                // That makes the column type depend on the value, which
                // `numbers_as_double` exists to avoid.
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
                let raw = fields
                    .and_then(|fields| field_value(fields, field))
                    .ok_or_else(|| {
                        RowError::Invalid(format!("timestamp field {field} is missing"))
                    })?;
                let number = timestamp_number(raw).ok_or_else(|| {
                    RowError::Invalid(format!("timestamp field {field} is not a number"))
                })?;
                let nanos = self.timestamp_unit.to_nanos(number).ok_or_else(|| {
                    RowError::Invalid(format!(
                        "timestamp field {field} is out of range for its unit"
                    ))
                })?;
                buffer.at(TimestampNanos::new(nanos))?;
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

/// `None` for anything that is not a usable timestamp.
///
/// A float needs the range test: `as i64` turns `NaN` into zero, which would
/// stamp the row at the epoch, and saturates a large value, which would stamp it
/// centuries away. Both would pass validation and store a wrong time.
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

fn timestamp_number(value: &OwnedValue) -> Option<i64> {
    match value {
        OwnedValue::Static(StaticNode::I64(number)) => Some(*number),
        OwnedValue::Static(StaticNode::U64(number)) => i64::try_from(*number).ok(),
        OwnedValue::Static(StaticNode::F64(number)) => {
            let number = *number;
            (number.is_finite() && number >= i64::MIN as f64 && number <= i64::MAX as f64)
                .then_some(number as i64)
        }
        _ => None,
    }
}

/// Whether `value` is something [`scalar_to_symbol`] would render, without
/// rendering it. Validation only needs the answer, and this runs per symbol
/// field per row.
fn is_symbol_scalar(value: &OwnedValue) -> bool {
    matches!(
        value,
        OwnedValue::Static(
            StaticNode::Bool(_) | StaticNode::I64(_) | StaticNode::U64(_) | StaticNode::F64(_)
        ) | OwnedValue::String(_)
    )
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

/// Defers to the client's own character rule so the two cannot drift, and adds
/// the length limit, which `ColumnName` does not carry: it lives on the buffer,
/// so without this an over-long name is only refused once the row is half
/// written.
fn validate_name(name: &str) -> Result<(), RowError> {
    if name.len() > MAX_COLUMN_NAME_LEN {
        return Err(RowError::Invalid(format!(
            "column {name} is longer than the {MAX_COLUMN_NAME_LEN} characters QuestDB allows"
        )));
    }
    ColumnName::new(name)
        .map(|_| ())
        .map_err(|error| RowError::Invalid(format!("column {name} is not a valid name: {error}")))
}

/// Mirrors the shape checks in [`append_array`] without writing anything.
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

/// Builds the array once, for both passes.
///
/// Validation throws the value away and the write pass keeps it, so the shape
/// rules live in one place. Holding them twice meant the two copies had to agree
/// by hand, and the rectangularity rule was missing from one of them.
fn parse_array(name: &str, items: &[OwnedValue]) -> Result<ArrayValues, RowError> {
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
    match array_depth(items) {
        1 => Ok(ArrayValues::One(flat_array(items).ok_or_else(not_numeric)?)),
        2 => {
            let mut rows = Vec::with_capacity(items.len());
            let mut width = None;
            for item in items {
                let values = item
                    .as_array()
                    .and_then(|values| flat_array(values))
                    .ok_or_else(not_numeric)?;
                if *width.get_or_insert(values.len()) != values.len() {
                    return Err(ragged());
                }
                rows.push(values);
            }
            Ok(ArrayValues::Two(rows))
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
                        .and_then(|values| flat_array(values))
                        .ok_or_else(not_numeric)?;
                    if *width.get_or_insert(values.len()) != values.len() {
                        return Err(ragged());
                    }
                    rows.push(values);
                }
                cubes.push(rows);
            }
            Ok(ArrayValues::Three(cubes))
        }
        _ => Err(RowError::Invalid(format!(
            "column {name} exceeds the supported array nesting depth of 3"
        ))),
    }
}

fn append_array(buffer: &mut Buffer, name: &str, items: &[OwnedValue]) -> Result<(), RowError> {
    match parse_array(name, items)? {
        ArrayValues::One(values) => buffer.column_arr(name, &values)?,
        ArrayValues::Two(rows) => buffer.column_arr(name, &rows)?,
        ArrayValues::Three(cubes) => buffer.column_arr(name, &cubes)?,
    };
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

    /// The buffer production actually writes into. Prefer it for anything that
    /// could depend on columnar behaviour: it pins one type per column for the
    /// buffer's lifetime, resolves names case-insensitively and silently keeps
    /// the first write when a column is repeated in a row. The ILP buffer has
    /// none of those rules, so a test that needs them cannot use `buffer()`.
    fn qwp_buffer() -> Buffer {
        Buffer::qwp_ws_with_max_name_len(MAX_COLUMN_NAME_LEN)
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
            table: "events".to_owned(),
            symbol_columns: ColumnNames::default(),
            uuid_columns: ColumnNames::default(),
            timestamp_source: TimestampSource::Message,
            timestamp_field: None,
            timestamp_unit: TimestampUnit::Auto,
            include_stream_column: false,
            include_topic_column: false,
            include_partition_column: false,
            include_offset_column: false,
            include_headers: false,
            numbers_as_double: false,
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
    fn given_mid_row_failure_when_appending_should_leave_the_buffer_untouched() {
        // The bad UUID sits after fields that would already have been encoded,
        // so this pins the guarantee that validation runs before any write
        // rather than being rolled back afterwards.
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
            "rejected row must not reach the buffer"
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
        let long_name = "a".repeat(MAX_COLUMN_NAME_LEN + 1);
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
    fn given_the_default_when_appending_mixed_numbers_should_still_conflict() {
        // The guard above is opt-in, so the default keeps integers as LONG and
        // the conflict stays visible as a client rejection the caller recovers
        // from.
        let mapping = mapping();
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
