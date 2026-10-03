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

//! Turns a run of `ConsumedMessage`s into Arrow record batches that fit in
//! one `AppendRows` request each.
//!
//! A message that cannot become a row (wrong payload type, a value that does
//! not fit its column, a row larger than the request budget) is rejected on
//! its own and reported back with its offset. The rest of the batch is still
//! written.
//!
//! Mapped mode decodes JSON objects with `arrow-json`. The fast path decodes
//! the whole run at once. When that fails, the run is decoded again row by
//! row so that only the offending rows are dropped.
//!
//! The writer schema of a request only contains the columns that at least one
//! row in the run sets. Columns no row mentions are left out, so BigQuery
//! fills them according to `missing_value` (the column default, or NULL).

use crate::schema::{BqType, Column, MetaColumn, Mode, RawKind, TableLayout, UTC};
use arrow::array::{
    ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message,
};
use arrow::json::ReaderBuilder;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, Payload, TopicMetadata};
use simd_json::{OwnedValue, StaticNode};
use std::fmt::Write as _;
use std::sync::Arc;

const HEADER_ENCODING_BASE64: &str = "base64";
const UTC_OFFSET: &str = "+00:00";

/// One `AppendRows` request worth of rows.
#[derive(Debug)]
pub(crate) struct Chunk {
    pub batch: RecordBatch,
    /// Iggy offset of each row, same order as `batch`.
    pub offsets: Vec<u64>,
    pub schema_bytes: Vec<u8>,
    pub batch_bytes: Vec<u8>,
}

/// A message that was not turned into a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rejected {
    pub offset: u64,
    pub reason: String,
}

#[derive(Debug, Default)]
pub(crate) struct Encoded {
    pub chunks: Vec<Chunk>,
    pub rejected: Vec<Rejected>,
}

/// Per-run context shared by every row.
pub(crate) struct RunContext<'a> {
    pub layout: &'a TableLayout,
    pub topic: &'a TopicMetadata,
    pub messages: &'a MessagesMetadata,
    pub max_request_bytes: usize,
}

/// Encode a run of messages. Fails only on an internal Arrow error, which
/// means the batch as a whole cannot be built. Per-row problems end up in
/// `Encoded::rejected`.
pub(crate) fn encode(
    ctx: &RunContext<'_>,
    messages: Vec<ConsumedMessage>,
) -> Result<Encoded, ArrowError> {
    let mut rejected = Vec::new();
    let batch = match &ctx.layout.raw {
        Some((kind, field)) => encode_raw(ctx, *kind, field, messages, &mut rejected)?,
        None => encode_mapped(ctx, messages, &mut rejected)?,
    };
    let mut chunks = Vec::new();
    if let Some((batch, offsets)) = batch {
        split_into_chunks(
            batch,
            offsets,
            ctx.max_request_bytes,
            &mut chunks,
            &mut rejected,
        )?;
    }
    Ok(Encoded { chunks, rejected })
}

/// Build a new chunk holding only the rows of `chunk` whose index is not in
/// `drop`. Used after BigQuery reports row-level errors.
pub(crate) fn without_rows(chunk: &Chunk, drop: &[usize]) -> Result<Option<Chunk>, ArrowError> {
    let keep: Vec<bool> = (0..chunk.batch.num_rows())
        .map(|row| !drop.contains(&row))
        .collect();
    let offsets: Vec<u64> = chunk
        .offsets
        .iter()
        .zip(&keep)
        .filter_map(|(offset, keep)| keep.then_some(*offset))
        .collect();
    if offsets.is_empty() {
        return Ok(None);
    }
    let mask = arrow::array::BooleanArray::from(keep);
    let batch = arrow::compute::filter_record_batch(&chunk.batch, &mask)?;
    let (schema_bytes, batch_bytes) = encode_ipc(&batch)?;
    Ok(Some(Chunk {
        batch,
        offsets,
        schema_bytes,
        batch_bytes,
    }))
}

// ─── Mapped mode ─────────────────────────────────────────────────────────────

fn encode_mapped(
    ctx: &RunContext<'_>,
    messages: Vec<ConsumedMessage>,
    rejected: &mut Vec<Rejected>,
) -> Result<Option<(RecordBatch, Vec<u64>)>, ArrowError> {
    let layout = ctx.layout;
    let mut rows = Vec::with_capacity(messages.len());
    let mut kept = Vec::with_capacity(messages.len());
    for mut message in messages {
        let payload = std::mem::replace(&mut message.payload, Payload::Raw(Vec::new()));
        match json_object(payload).and_then(|mut row| {
            normalize_row(&mut row, layout)?;
            Ok(row)
        }) {
            Ok(row) => {
                rows.push(row);
                kept.push(message);
            }
            Err(reason) => rejected.push(Rejected {
                offset: message.offset,
                reason,
            }),
        }
    }
    if rows.is_empty() {
        return Ok(None);
    }

    let present = present_columns(&rows, layout);
    if present.is_empty() && layout.metadata.is_empty() {
        rejected.extend(kept.iter().map(|m| Rejected {
            offset: m.offset,
            reason: "payload has no field matching a table column".to_owned(),
        }));
        return Ok(None);
    }
    let payload_fields: Vec<FieldRef> = present.iter().map(|&i| layout.fields[i].clone()).collect();

    let (decoded, survivors) = decode_rows(&payload_fields, &rows, kept.len(), |row, reason| {
        rejected.push(Rejected {
            offset: kept[row].offset,
            reason,
        });
    })?;
    if survivors.is_empty() {
        return Ok(None);
    }
    let kept: Vec<&ConsumedMessage> = survivors.iter().map(|&row| &kept[row]).collect();
    let offsets = kept.iter().map(|m| m.offset).collect();

    let mut fields = payload_fields;
    let mut columns: Vec<ArrayRef> = decoded.map(|b| b.columns().to_vec()).unwrap_or_default();
    append_metadata(ctx, &kept, &mut fields, &mut columns)?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    Ok(Some((batch, offsets)))
}

/// Take the JSON object out of a payload. Text and proto text must parse as
/// a JSON object.
fn json_object(payload: Payload) -> Result<OwnedValue, String> {
    let value = match payload {
        Payload::Json(value) => value,
        Payload::Text(text) | Payload::Proto(text) => {
            let mut bytes = text.into_bytes();
            simd_json::to_owned_value(&mut bytes)
                .map_err(|e| format!("payload text is not valid JSON: {e}"))?
        }
        other => {
            return Err(format!(
                "{} payload is not supported in mapped mode",
                payload_kind(&other)
            ));
        }
    };
    match value {
        OwnedValue::Object(_) => Ok(value),
        _ => Err("payload is not a JSON object".to_owned()),
    }
}

/// Prepare one row for `arrow-json`:
/// - a REQUIRED column without a default must be present and non-null,
/// - REPEATED columns that are absent or null become `[]`,
/// - JSON column values become their JSON text,
/// - BYTES column values are base64 in JSON and become hex, which is what
///   `arrow-json` decodes into binary.
fn normalize_row(row: &mut OwnedValue, layout: &TableLayout) -> Result<(), String> {
    let OwnedValue::Object(object) = row else {
        return Err("payload is not a JSON object".to_owned());
    };
    for column in &layout.columns {
        let value = object.get_mut(column.name.as_str());
        match (value, column.mode) {
            (None | Some(OwnedValue::Static(StaticNode::Null)), Mode::Repeated) => {
                object.insert(column.name.clone(), empty_array());
            }
            (None | Some(OwnedValue::Static(StaticNode::Null)), Mode::Required)
                if !column.has_default =>
            {
                return Err(format!(
                    "missing value for REQUIRED column '{}'",
                    column.name
                ));
            }
            (Some(value), _) => normalize_value(value, column)?,
            (None, _) => {}
        }
    }
    Ok(())
}

fn normalize_value(value: &mut OwnedValue, column: &Column) -> Result<(), String> {
    if column.mode == Mode::Repeated {
        let OwnedValue::Array(items) = value else {
            return Err(format!("column '{}' expects an array", column.name));
        };
        for item in items.iter_mut() {
            normalize_scalar(item, column)?;
        }
        return Ok(());
    }
    normalize_scalar(value, column)
}

fn normalize_scalar(value: &mut OwnedValue, column: &Column) -> Result<(), String> {
    if matches!(value, OwnedValue::Static(StaticNode::Null)) {
        return Ok(());
    }
    match &column.bq_type {
        BqType::Json => {
            let text = simd_json::to_string(value)
                .map_err(|e| format!("column '{}': cannot serialize JSON: {e}", column.name))?;
            *value = OwnedValue::String(text);
        }
        BqType::Bytes => {
            let OwnedValue::String(text) = value else {
                return Err(format!("column '{}' expects a base64 string", column.name));
            };
            let bytes = BASE64
                .decode(text.as_bytes())
                .map_err(|e| format!("column '{}': invalid base64: {e}", column.name))?;
            *value = OwnedValue::String(to_hex(&bytes));
        }
        BqType::Record(children) => {
            let OwnedValue::Object(object) = value else {
                return Ok(());
            };
            for child in children {
                match (object.get_mut(child.name.as_str()), child.mode) {
                    (None | Some(OwnedValue::Static(StaticNode::Null)), Mode::Repeated) => {
                        object.insert(child.name.clone(), empty_array());
                    }
                    (Some(child_value), _) => normalize_value(child_value, child)?,
                    (None, _) => {}
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Indexes of the layout columns that at least one row sets, in table order.
fn present_columns(rows: &[OwnedValue], layout: &TableLayout) -> Vec<usize> {
    let mut present = vec![false; layout.columns.len()];
    for row in rows {
        if let OwnedValue::Object(object) = row {
            for key in object.keys() {
                if let Some(&index) = layout.index.get(key.as_str()) {
                    present[index] = true;
                }
            }
        }
    }
    present
        .iter()
        .enumerate()
        .filter_map(|(index, set)| set.then_some(index))
        .collect()
}

/// Decode rows into one batch. Returns the batch (`None` when there are no
/// payload fields) and the indexes of the rows it holds.
fn decode_rows(
    fields: &[FieldRef],
    rows: &[OwnedValue],
    row_count: usize,
    mut on_reject: impl FnMut(usize, String),
) -> Result<(Option<RecordBatch>, Vec<usize>), ArrowError> {
    if fields.is_empty() {
        return Ok((None, (0..row_count).collect()));
    }
    let target: SchemaRef = Arc::new(Schema::new(fields.to_vec()));
    let schema: SchemaRef = Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| {
                f.as_ref()
                    .clone()
                    .with_data_type(decode_type(f.data_type()))
            })
            .collect::<Vec<_>>(),
    ));
    if let Ok(batch) = decode(&schema, rows) {
        return Ok((Some(to_target(batch, &target)?), (0..row_count).collect()));
    }

    let mut batches = Vec::with_capacity(rows.len());
    let mut survivors = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        match decode(&schema, std::slice::from_ref(row)) {
            Ok(batch) => {
                batches.push(batch);
                survivors.push(index);
            }
            Err(e) => on_reject(index, format!("row does not match the table schema: {e}")),
        }
    }
    if batches.is_empty() {
        return Ok((None, survivors));
    }
    let batch = concat_batches(&schema, &batches)?;
    Ok((Some(to_target(batch, &target)?), survivors))
}

/// The type `arrow-json` decodes into. Without the `chrono-tz` feature it
/// only parses offset timezones, so `"UTC"` is decoded as `"+00:00"` and
/// relabelled afterwards. Both name the same instant, so the relabel is a
/// metadata change.
fn decode_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Timestamp(unit, Some(_)) => DataType::Timestamp(*unit, Some(UTC_OFFSET.into())),
        DataType::List(item) => DataType::List(Arc::new(
            item.as_ref()
                .clone()
                .with_data_type(decode_type(item.data_type())),
        )),
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|f| {
                    f.as_ref()
                        .clone()
                        .with_data_type(decode_type(f.data_type()))
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

fn to_target(batch: RecordBatch, target: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let columns = batch
        .columns()
        .iter()
        .zip(target.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(column.clone())
            } else {
                cast(column, field.data_type())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(target.clone(), columns)
}

fn decode(schema: &SchemaRef, rows: &[OwnedValue]) -> Result<RecordBatch, ArrowError> {
    let mut decoder = ReaderBuilder::new(schema.clone())
        .with_batch_size(rows.len().max(1))
        .build_decoder()?;
    decoder.serialize(rows)?;
    decoder
        .flush()?
        .ok_or_else(|| ArrowError::JsonError("decoder produced no rows".to_owned()))
}

// ─── Raw mode ────────────────────────────────────────────────────────────────

fn encode_raw(
    ctx: &RunContext<'_>,
    kind: RawKind,
    payload_field: &FieldRef,
    messages: Vec<ConsumedMessage>,
    rejected: &mut Vec<Rejected>,
) -> Result<Option<(RecordBatch, Vec<u64>)>, ArrowError> {
    let mut kept = Vec::with_capacity(messages.len());
    let payload_column: ArrayRef = match kind {
        RawKind::Bytes => {
            let mut values = Vec::with_capacity(messages.len());
            for message in messages {
                match message.payload.try_to_bytes() {
                    Ok(bytes) => {
                        values.push(bytes);
                        kept.push(message);
                    }
                    Err(e) => rejected.push(Rejected {
                        offset: message.offset,
                        reason: e.to_string(),
                    }),
                }
            }
            Arc::new(BinaryArray::from_iter_values(values))
        }
        RawKind::Json | RawKind::String => {
            let mut values = Vec::with_capacity(messages.len());
            for mut message in messages {
                let payload = std::mem::replace(&mut message.payload, Payload::Raw(Vec::new()));
                match raw_text(payload, kind) {
                    Ok(text) => {
                        values.push(text);
                        kept.push(message);
                    }
                    Err(reason) => rejected.push(Rejected {
                        offset: message.offset,
                        reason,
                    }),
                }
            }
            Arc::new(StringArray::from_iter_values(values))
        }
    };
    if kept.is_empty() {
        return Ok(None);
    }
    let kept: Vec<&ConsumedMessage> = kept.iter().collect();
    let offsets = kept.iter().map(|m| m.offset).collect();
    let mut fields = vec![payload_field.clone()];
    let mut columns = vec![payload_column];
    append_metadata(ctx, &kept, &mut fields, &mut columns)?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    Ok(Some((batch, offsets)))
}

/// Payload as text for a JSON or STRING column. A JSON column only takes
/// text that parses as JSON.
fn raw_text(payload: Payload, kind: RawKind) -> Result<String, String> {
    let text = match payload {
        Payload::Json(value) => {
            return simd_json::to_string(&value).map_err(|e| format!("cannot serialize JSON: {e}"));
        }
        Payload::Text(text) | Payload::Proto(text) => text,
        Payload::Raw(bytes) => {
            String::from_utf8(bytes).map_err(|_| "raw payload is not valid UTF-8".to_owned())?
        }
        other => {
            return Err(format!(
                "{} payload cannot be written to a text column",
                payload_kind(&other)
            ));
        }
    };
    if kind == RawKind::Json {
        let mut bytes = text.as_bytes().to_vec();
        simd_json::to_owned_value(&mut bytes)
            .map_err(|e| format!("payload is not valid JSON: {e}"))?;
    }
    Ok(text)
}

// ─── Metadata ────────────────────────────────────────────────────────────────

fn append_metadata(
    ctx: &RunContext<'_>,
    messages: &[&ConsumedMessage],
    fields: &mut Vec<FieldRef>,
    columns: &mut Vec<ArrayRef>,
) -> Result<(), ArrowError> {
    let rows = messages.len();
    for (meta, field) in &ctx.layout.metadata {
        let column: ArrayRef = match meta {
            MetaColumn::Stream => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                ctx.topic.stream.as_str(),
                rows,
            ))),
            MetaColumn::Topic => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                ctx.topic.topic.as_str(),
                rows,
            ))),
            MetaColumn::PartitionId => Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                i64::from(ctx.messages.partition_id),
                rows,
            ))),
            MetaColumn::Offset => Arc::new(Int64Array::from_iter_values(
                messages.iter().map(|m| saturating_i64(m.offset)),
            )),
            MetaColumn::Timestamp => Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    messages.iter().map(|m| saturating_i64(m.timestamp)),
                )
                .with_timezone(UTC),
            ),
            MetaColumn::Id => Arc::new(StringArray::from_iter_values(
                messages.iter().map(|m| m.id.to_string()),
            )),
            MetaColumn::Headers => {
                let required = !field.is_nullable();
                Arc::new(StringArray::from_iter(
                    messages.iter().map(|m| headers_json(m, required)),
                ))
            }
        };
        fields.push(field.clone());
        columns.push(column);
    }
    Ok(())
}

/// Headers as a JSON object. Text values are written as strings, binary
/// values as `{"data": <base64>, "iggy_header_encoding": "base64"}`, the same
/// shape `http_sink` uses.
fn headers_json(message: &ConsumedMessage, required: bool) -> Option<String> {
    let headers = message.headers.as_ref().filter(|h| !h.is_empty());
    let Some(headers) = headers else {
        return required.then(|| "{}".to_owned());
    };
    let mut object = serde_json::Map::with_capacity(headers.len());
    for (key, value) in headers {
        let encoded = match value.as_raw() {
            Ok(raw) => serde_json::json!({
                "data": BASE64.encode(raw),
                "iggy_header_encoding": HEADER_ENCODING_BASE64,
            }),
            Err(_) => serde_json::Value::String(value.to_string_value()),
        };
        object.insert(key.to_string_value(), encoded);
    }
    Some(serde_json::Value::Object(object).to_string())
}

// ─── Request sizing ──────────────────────────────────────────────────────────

/// Encode `batch` and halve it until every piece fits `max_bytes`. A single
/// row that does not fit is rejected.
fn split_into_chunks(
    batch: RecordBatch,
    offsets: Vec<u64>,
    max_bytes: usize,
    chunks: &mut Vec<Chunk>,
    rejected: &mut Vec<Rejected>,
) -> Result<(), ArrowError> {
    let (schema_bytes, batch_bytes) = encode_ipc(&batch)?;
    let size = schema_bytes.len() + batch_bytes.len();
    if size <= max_bytes {
        chunks.push(Chunk {
            batch,
            offsets,
            schema_bytes,
            batch_bytes,
        });
        return Ok(());
    }
    if batch.num_rows() == 1 {
        rejected.push(Rejected {
            offset: offsets[0],
            reason: format!("row is {size} bytes encoded, above max_request_bytes ({max_bytes})"),
        });
        return Ok(());
    }
    let half = batch.num_rows() / 2;
    let (left_offsets, right_offsets) = offsets.split_at(half);
    split_into_chunks(
        batch.slice(0, half),
        left_offsets.to_vec(),
        max_bytes,
        chunks,
        rejected,
    )?;
    split_into_chunks(
        batch.slice(half, batch.num_rows() - half),
        right_offsets.to_vec(),
        max_bytes,
        chunks,
        rejected,
    )
}

/// Serialize the schema and the batch as Arrow IPC stream messages, the
/// format `AppendRowsRequest.arrow_rows` carries.
fn encode_ipc(batch: &RecordBatch) -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
    let options = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(true);
    let mut compression = CompressionContext::default();

    let schema = generator.schema_to_bytes_with_dictionary_tracker(
        batch.schema_ref(),
        &mut tracker,
        &options,
    );
    let mut schema_bytes = Vec::new();
    write_message(&mut schema_bytes, schema, &options)?;

    let (dictionaries, encoded) =
        generator.encode(batch, &mut tracker, &options, &mut compression)?;
    let mut batch_bytes = Vec::new();
    for dictionary in dictionaries {
        write_message(&mut batch_bytes, dictionary, &options)?;
    }
    write_message(&mut batch_bytes, encoded, &options)?;
    Ok((schema_bytes, batch_bytes))
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn payload_kind(payload: &Payload) -> &'static str {
    match payload {
        Payload::Json(_) => "JSON",
        Payload::Raw(_) => "raw",
        Payload::Text(_) => "text",
        Payload::Proto(_) => "proto",
        Payload::FlatBuffer(_) => "FlatBuffer",
        Payload::Avro(_) => "Avro",
    }
}

fn empty_array() -> OwnedValue {
    OwnedValue::Array(Box::default())
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WriteMode;
    use crate::schema::parse_table_schema;
    use crate::test_support::settings;
    use arrow::array::{Array, AsArray, ListArray};
    use arrow::datatypes::Int64Type;
    use iggy_common::{HeaderKey, HeaderValue};
    use iggy_connector_sdk::Schema as PayloadSchema;
    use std::collections::BTreeMap;
    use std::str::FromStr;

    const METADATA_FIELDS: &str = r#"
        {"name":"iggy_stream","type":"STRING"},
        {"name":"iggy_topic","type":"STRING"},
        {"name":"iggy_partition_id","type":"INTEGER"},
        {"name":"iggy_offset","type":"INTEGER"},
        {"name":"iggy_timestamp","type":"TIMESTAMP"},
        {"name":"iggy_id","type":"STRING"}"#;

    fn layout(fields: &str, mode: WriteMode, include_metadata: bool) -> TableLayout {
        let body = format!(r#"{{"schema":{{"fields":[{fields}]}}}}"#);
        let columns = parse_table_schema(body.as_bytes()).unwrap();
        TableLayout::build(columns, &settings(mode, include_metadata)).unwrap()
    }

    fn topic() -> TopicMetadata {
        TopicMetadata {
            stream: "orders".into(),
            topic: "created".into(),
        }
    }

    fn messages_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 3,
            current_offset: 100,
            schema: PayloadSchema::Json,
        }
    }

    fn message(offset: u64, payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: u128::from(offset) + 1000,
            offset,
            checksum: 0,
            timestamp: 1_700_000_000_000_000 + offset,
            origin_timestamp: 0,
            headers: None,
            payload,
        }
    }

    fn json(offset: u64, text: &str) -> ConsumedMessage {
        let mut bytes = text.as_bytes().to_vec();
        message(
            offset,
            Payload::Json(simd_json::to_owned_value(&mut bytes).unwrap()),
        )
    }

    fn run(layout: &TableLayout, messages: Vec<ConsumedMessage>) -> Encoded {
        run_with_budget(layout, messages, 8 * 1024 * 1024)
    }

    fn run_with_budget(
        layout: &TableLayout,
        messages: Vec<ConsumedMessage>,
        max_request_bytes: usize,
    ) -> Encoded {
        let topic = topic();
        let metadata = messages_metadata();
        let ctx = RunContext {
            layout,
            topic: &topic,
            messages: &metadata,
            max_request_bytes,
        };
        encode(&ctx, messages).unwrap()
    }

    fn column_names(batch: &RecordBatch) -> Vec<String> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    #[test]
    fn given_json_rows_should_encode_payload_and_metadata_columns() {
        let layout = layout(
            &format!(
                r#"{{"name":"user_id","type":"INT64"}},{{"name":"event","type":"STRING"}},{METADATA_FIELDS}"#
            ),
            WriteMode::Mapped,
            true,
        );
        let encoded = run(
            &layout,
            vec![
                json(10, r#"{"user_id": 1, "event": "signup", "ignored": true}"#),
                json(11, r#"{"user_id": "2", "event": "login"}"#),
            ],
        );
        assert!(encoded.rejected.is_empty());
        assert_eq!(encoded.chunks.len(), 1);
        let chunk = &encoded.chunks[0];
        assert_eq!(chunk.offsets, vec![10, 11]);
        assert_eq!(
            column_names(&chunk.batch),
            vec![
                "user_id",
                "event",
                "iggy_stream",
                "iggy_topic",
                "iggy_partition_id",
                "iggy_offset",
                "iggy_timestamp",
                "iggy_id"
            ]
        );
        let user_ids = chunk.batch.column(0).as_primitive::<Int64Type>();
        assert_eq!(user_ids.values(), &[1, 2]);
        let stream = chunk.batch.column(2).as_string::<i32>();
        assert_eq!(stream.value(1), "orders");
        let partition = chunk.batch.column(4).as_primitive::<Int64Type>();
        assert_eq!(partition.value(0), 3);
        let offset = chunk.batch.column(5).as_primitive::<Int64Type>();
        assert_eq!(offset.values(), &[10, 11]);
        let id = chunk.batch.column(7).as_string::<i32>();
        assert_eq!(id.value(0), "1010");
        assert!(!chunk.schema_bytes.is_empty());
        assert!(!chunk.batch_bytes.is_empty());
    }

    #[test]
    fn given_row_with_wrong_type_should_reject_only_that_row() {
        let layout = layout(
            r#"{"name":"user_id","type":"INT64"}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(
            &layout,
            vec![
                json(1, r#"{"user_id": 1}"#),
                json(2, r#"{"user_id": "not-a-number"}"#),
                json(3, r#"{"user_id": 3}"#),
            ],
        );
        assert_eq!(encoded.rejected.len(), 1);
        assert_eq!(encoded.rejected[0].offset, 2);
        let chunk = &encoded.chunks[0];
        assert_eq!(chunk.offsets, vec![1, 3]);
        assert_eq!(
            chunk.batch.column(0).as_primitive::<Int64Type>().values(),
            &[1, 3]
        );
    }

    #[test]
    fn given_text_payload_should_parse_as_json_and_reject_non_json() {
        let layout = layout(
            r#"{"name":"name","type":"STRING"}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(
            &layout,
            vec![
                message(1, Payload::Text(r#"{"name":"a"}"#.into())),
                message(2, Payload::Text("plain text".into())),
                message(3, Payload::Raw(vec![1, 2, 3])),
                json(4, r#"[1, 2]"#),
            ],
        );
        let rejected: Vec<u64> = encoded.rejected.iter().map(|r| r.offset).collect();
        assert_eq!(rejected, vec![2, 3, 4]);
        assert_eq!(encoded.chunks[0].offsets, vec![1]);
    }

    #[test]
    fn given_column_absent_from_every_row_should_leave_it_out_of_writer_schema() {
        let layout = layout(
            r#"{"name":"a","type":"STRING"},{"name":"b","type":"STRING"},{"name":"c","type":"STRING"}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(
            &layout,
            vec![json(1, r#"{"c": "x"}"#), json(2, r#"{"a": "y"}"#)],
        );
        assert_eq!(column_names(&encoded.chunks[0].batch), vec!["a", "c"]);
    }

    #[test]
    fn given_required_column_missing_should_reject_unless_it_has_default() {
        let layout = layout(
            r#"{"name":"id","type":"INT64","mode":"REQUIRED"},
               {"name":"created","type":"TIMESTAMP","mode":"REQUIRED","defaultValueExpression":"CURRENT_TIMESTAMP()"}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(&layout, vec![json(1, r#"{"id": 7}"#), json(2, r#"{}"#)]);
        assert_eq!(encoded.rejected.len(), 1);
        assert_eq!(encoded.rejected[0].offset, 2);
        assert!(encoded.rejected[0].reason.contains("REQUIRED column 'id'"));
        assert_eq!(column_names(&encoded.chunks[0].batch), vec!["id"]);
    }

    #[test]
    fn given_null_repeated_column_should_write_empty_array() {
        let layout = layout(
            r#"{"name":"tags","type":"STRING","mode":"REPEATED"}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(
            &layout,
            vec![
                json(1, r#"{"tags": ["a", "b"]}"#),
                json(2, r#"{"tags": null}"#),
            ],
        );
        assert!(encoded.rejected.is_empty());
        let tags = encoded.chunks[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .clone();
        assert_eq!(tags.value_length(0), 2);
        assert_eq!(tags.value_length(1), 0);
        assert!(!tags.is_null(1));
    }

    #[test]
    fn given_json_and_bytes_columns_should_convert_values() {
        let layout = layout(
            r#"{"name":"attrs","type":"JSON"},{"name":"blob","type":"BYTES"}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(
            &layout,
            vec![
                json(1, r#"{"attrs": {"k": [1, 2]}, "blob": "aGVsbG8="}"#),
                json(2, r#"{"blob": "not base64!"}"#),
            ],
        );
        assert_eq!(encoded.rejected.len(), 1);
        assert_eq!(encoded.rejected[0].offset, 2);
        let batch = &encoded.chunks[0].batch;
        assert_eq!(
            batch.column(0).as_string::<i32>().value(0),
            r#"{"k":[1,2]}"#
        );
        assert_eq!(batch.column(1).as_binary::<i32>().value(0), b"hello");
    }

    #[test]
    fn given_timestamp_and_numeric_strings_should_decode() {
        let layout = layout(
            r#"{"name":"at","type":"TIMESTAMP"},{"name":"amount","type":"NUMERIC"},{"name":"day","type":"DATE"}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(
            &layout,
            vec![json(
                1,
                r#"{"at": "2024-01-02T03:04:05.123456Z", "amount": "12.345", "day": "2024-01-02"}"#,
            )],
        );
        assert!(encoded.rejected.is_empty(), "{:?}", encoded.rejected);
        let batch = &encoded.chunks[0].batch;
        assert_eq!(
            batch.column(0).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(UTC.into()))
        );
        let at = batch
            .column(0)
            .as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
        assert_eq!(at.value(0), 1_704_164_645_123_456);
    }

    #[test]
    fn given_nested_record_with_missing_required_child_should_reject_row() {
        let layout = layout(
            r#"{"name":"customer","type":"RECORD","fields":[
                {"name":"id","type":"INT64","mode":"REQUIRED"},
                {"name":"tags","type":"STRING","mode":"REPEATED"}
            ]}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(
            &layout,
            vec![
                json(1, r#"{"customer": {"id": 1}}"#),
                json(2, r#"{"customer": {"tags": ["x"]}}"#),
            ],
        );
        assert_eq!(encoded.rejected.len(), 1);
        assert_eq!(encoded.rejected[0].offset, 2);
        assert_eq!(encoded.chunks[0].offsets, vec![1]);
    }

    #[test]
    fn given_raw_json_column_should_accept_json_and_reject_invalid_text() {
        let layout = layout(
            &format!(r#"{{"name":"payload","type":"JSON"}},{METADATA_FIELDS}"#),
            WriteMode::Raw,
            true,
        );
        let encoded = run(
            &layout,
            vec![
                json(1, r#"{"a": 1}"#),
                message(2, Payload::Text("not json".into())),
                message(3, Payload::Raw(br#"{"b":2}"#.to_vec())),
            ],
        );
        assert_eq!(encoded.rejected.len(), 1);
        assert_eq!(encoded.rejected[0].offset, 2);
        let batch = &encoded.chunks[0].batch;
        assert_eq!(column_names(batch)[0], "payload");
        let payload = batch.column(0).as_string::<i32>();
        assert_eq!(payload.value(0), r#"{"a":1}"#);
        assert_eq!(payload.value(1), r#"{"b":2}"#);
    }

    #[test]
    fn given_raw_bytes_column_should_accept_every_payload_type() {
        let layout = layout(
            r#"{"name":"payload","type":"BYTES"}"#,
            WriteMode::Raw,
            false,
        );
        let encoded = run(
            &layout,
            vec![
                message(1, Payload::Raw(vec![0, 159, 146, 150])),
                message(2, Payload::Text("hi".into())),
            ],
        );
        assert!(encoded.rejected.is_empty());
        let payload = encoded.chunks[0].batch.column(0).as_binary::<i32>();
        assert_eq!(payload.value(0), &[0, 159, 146, 150]);
        assert_eq!(payload.value(1), b"hi");
    }

    #[test]
    fn given_raw_string_column_should_reject_non_utf8_bytes() {
        let layout = layout(
            r#"{"name":"payload","type":"STRING"}"#,
            WriteMode::Raw,
            false,
        );
        let encoded = run(&layout, vec![message(1, Payload::Raw(vec![0xff, 0xfe]))]);
        assert_eq!(encoded.rejected.len(), 1);
        assert!(encoded.chunks.is_empty());
    }

    #[test]
    fn given_batch_over_budget_should_split_and_keep_row_order() {
        let layout = layout(
            r#"{"name":"payload","type":"STRING"}"#,
            WriteMode::Raw,
            false,
        );
        let messages = (0..40)
            .map(|offset| message(offset, Payload::Text("x".repeat(4096))))
            .collect();
        let encoded = run_with_budget(&layout, messages, 64 * 1024);
        assert!(encoded.rejected.is_empty());
        assert!(encoded.chunks.len() > 1);
        for chunk in &encoded.chunks {
            assert!(chunk.schema_bytes.len() + chunk.batch_bytes.len() <= 64 * 1024);
            assert_eq!(chunk.batch.num_rows(), chunk.offsets.len());
        }
        let offsets: Vec<u64> = encoded
            .chunks
            .iter()
            .flat_map(|c| c.offsets.clone())
            .collect();
        assert_eq!(offsets, (0..40).collect::<Vec<_>>());
    }

    #[test]
    fn given_single_row_over_budget_should_reject_it() {
        let layout = layout(
            r#"{"name":"payload","type":"STRING"}"#,
            WriteMode::Raw,
            false,
        );
        let encoded = run_with_budget(
            &layout,
            vec![
                message(1, Payload::Text("small".into())),
                message(2, Payload::Text("y".repeat(200 * 1024))),
            ],
            64 * 1024,
        );
        assert_eq!(encoded.rejected.len(), 1);
        assert_eq!(encoded.rejected[0].offset, 2);
        assert_eq!(encoded.chunks.len(), 1);
        assert_eq!(encoded.chunks[0].offsets, vec![1]);
    }

    #[test]
    fn given_row_errors_should_rebuild_chunk_without_those_rows() {
        let layout = layout(
            r#"{"name":"payload","type":"STRING"}"#,
            WriteMode::Raw,
            false,
        );
        let encoded = run(
            &layout,
            (0..4)
                .map(|offset| message(offset, Payload::Text(format!("row-{offset}"))))
                .collect(),
        );
        let chunk = without_rows(&encoded.chunks[0], &[1, 3]).unwrap().unwrap();
        assert_eq!(chunk.offsets, vec![0, 2]);
        let payload = chunk.batch.column(0).as_string::<i32>();
        assert_eq!(payload.value(1), "row-2");
        assert!(without_rows(&chunk, &[0, 1]).unwrap().is_none());
    }

    #[test]
    fn given_headers_should_encode_text_and_binary_values() {
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_str("trace").unwrap(),
            HeaderValue::from_str("abc").unwrap(),
        );
        headers.insert(
            HeaderKey::from_str("blob").unwrap(),
            HeaderValue::try_from(&[1u8, 2][..]).unwrap(),
        );
        let mut msg = message(1, Payload::Text("x".into()));
        msg.headers = Some(headers);
        let text = headers_json(&msg, false).unwrap();
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["trace"], "abc");
        assert_eq!(value["blob"]["data"], "AQI=");
        assert_eq!(value["blob"]["iggy_header_encoding"], "base64");

        let empty = message(2, Payload::Text("x".into()));
        assert_eq!(headers_json(&empty, false), None);
        assert_eq!(headers_json(&empty, true).as_deref(), Some("{}"));
    }
}
