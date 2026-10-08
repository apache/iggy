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
//! Mapped mode decodes JSON objects with `arrow-json`. Rows are first grouped
//! by adjacent default-column presence. Each group is decoded at once, and
//! bisection isolates offending rows when a group fails.
//!
//! The writer schema of a request only contains the columns that at least one
//! row in its group sets. Columns no row mentions are left out, so BigQuery
//! fills them according to `missing_value` (the column default, or NULL).

use std::fmt::Write as _;
use std::io;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, Int64Array, ListArray, RecordBatch, StringArray, StringBuilder,
    StructArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, FieldRef, Fields, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message,
};
use arrow::json::ReaderBuilder;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, Payload, TopicMetadata};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use simd_json::{OwnedValue, StaticNode};

use crate::schema::{BqType, Column, MetaColumn, Mode, RawKind, TableLayout, UTC};

const HEADER_ENCODING_BASE64: &str = "base64";
const UTC_OFFSET: &str = "+00:00";
const IPC_BATCH_OVERHEAD: usize = 1024;
const IPC_COLUMN_OVERHEAD: usize = 128;
const GRPC_FRAME_HEADER_BYTES: usize = 5;

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
    pub write_stream: &'a str,
    pub missing_value: i32,
}

/// Encode a run of messages. Fails only on an internal Arrow error, which
/// means the batch as a whole cannot be built. Per-row problems end up in
/// `Encoded::rejected`.
pub(crate) fn encode(
    ctx: &RunContext<'_>,
    messages: Vec<ConsumedMessage>,
) -> Result<Encoded, ArrowError> {
    let mut rejected = Vec::new();
    let mut chunks = Vec::new();
    for messages in group_messages(ctx, messages, &mut rejected) {
        let batches = match &ctx.layout.raw {
            Some((kind, field)) => encode_raw(ctx, *kind, field, messages, &mut rejected)?
                .into_iter()
                .collect(),
            None => encode_mapped(ctx, messages, &mut rejected)?,
        };
        for (batch, offsets) in batches {
            split_into_chunks(
                batch,
                offsets,
                ctx.max_request_bytes,
                ctx.write_stream,
                ctx.missing_value,
                &mut chunks,
                &mut rejected,
            )?;
        }
    }
    Ok(Encoded { chunks, rejected })
}

/// Build a new chunk holding only the rows of `chunk` whose index is not in
/// `drop`. Used after BigQuery reports row-level errors.
pub(crate) fn without_rows(chunk: &Chunk, drop: &[usize]) -> Result<Option<Chunk>, ArrowError> {
    let mut keep = vec![true; chunk.batch.num_rows()];
    for &row in drop {
        let Some(value) = keep.get_mut(row) else {
            return Err(ArrowError::ComputeError(format!(
                "cannot remove row {row} from a {}-row batch",
                keep.len()
            )));
        };
        *value = false;
    }
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

/// Keep the amount handed to any Arrow builder below the request budget.
/// A single mapped row is inspected again after normalization because fields
/// that do not exist in the table are not written.
fn group_messages(
    ctx: &RunContext<'_>,
    messages: Vec<ConsumedMessage>,
    rejected: &mut Vec<Rejected>,
) -> Vec<Vec<ConsumedMessage>> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for message in messages {
        if let Some(reason) = invalid_metadata(ctx.layout, &message) {
            rejected.push(Rejected {
                offset: message.offset,
                reason,
            });
            continue;
        }

        let estimated = estimated_message_size(ctx, &message).max(1);
        if ctx.layout.raw.is_some() && estimated > ctx.max_request_bytes {
            rejected.push(Rejected {
                offset: message.offset,
                reason: format!(
                    "row input is at least {estimated} bytes, above max_request_bytes ({})",
                    ctx.max_request_bytes
                ),
            });
            continue;
        }
        if !current.is_empty() && current_bytes.saturating_add(estimated) > ctx.max_request_bytes {
            groups.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(estimated);
        current.push(message);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

fn invalid_metadata(layout: &TableLayout, message: &ConsumedMessage) -> Option<String> {
    for (meta, _) in &layout.metadata {
        let (name, value) = match meta {
            MetaColumn::Offset => (meta.name(), message.offset),
            MetaColumn::Timestamp => (meta.name(), message.timestamp),
            _ => continue,
        };
        if i64::try_from(value).is_err() {
            return Some(format!("{name} value {value} does not fit INT64"));
        }
    }
    None
}

fn estimated_message_size(ctx: &RunContext<'_>, message: &ConsumedMessage) -> usize {
    let payload = match &message.payload {
        Payload::Json(value) => estimated_json_size(value),
        Payload::Raw(value) | Payload::FlatBuffer(value) | Payload::Avro(value) => value.len(),
        Payload::Text(value) | Payload::Proto(value) => value.len(),
    };
    payload.saturating_add(estimated_metadata_size(ctx, message))
}

fn estimated_metadata_size(ctx: &RunContext<'_>, message: &ConsumedMessage) -> usize {
    ctx.layout.metadata.iter().fold(0usize, |size, (meta, _)| {
        size.saturating_add(match meta {
            MetaColumn::Stream => ctx.topic.stream.len(),
            MetaColumn::Topic => ctx.topic.topic.len(),
            MetaColumn::PartitionId | MetaColumn::Offset | MetaColumn::Timestamp => 8,
            MetaColumn::Id => 39,
            MetaColumn::Headers => estimated_headers_size(message),
        })
    })
}

fn estimated_headers_size(message: &ConsumedMessage) -> usize {
    message.headers.as_ref().map_or(0, |headers| {
        headers.iter().fold(0usize, |size, (key, value)| {
            let value_size = value
                .as_raw()
                .map_or_else(|_| value.to_string_value().len(), |raw| raw.len());
            size.saturating_add(key.to_string_value().len())
                .saturating_add(value_size.saturating_mul(2))
        })
    })
}

fn estimated_json_size(value: &OwnedValue) -> usize {
    match value {
        OwnedValue::Static(_) => 24,
        OwnedValue::String(value) => value.len(),
        OwnedValue::Array(values) => values.iter().fold(2usize, |size, value| {
            size.saturating_add(estimated_json_size(value).saturating_add(1))
        }),
        OwnedValue::Object(values) => values.iter().fold(2usize, |size, (key, value)| {
            size.saturating_add(key.len())
                .saturating_add(estimated_json_size(value))
                .saturating_add(4)
        }),
    }
}

// ─── Mapped mode ─────────────────────────────────────────────────────────────

fn encode_mapped(
    ctx: &RunContext<'_>,
    messages: Vec<ConsumedMessage>,
    rejected: &mut Vec<Rejected>,
) -> Result<Vec<(RecordBatch, Vec<u64>)>, ArrowError> {
    let layout = ctx.layout;
    let mut rows = Vec::with_capacity(messages.len());
    let mut kept = Vec::with_capacity(messages.len());
    for mut message in messages {
        let payload = std::mem::replace(&mut message.payload, Payload::Raw(Vec::new()));
        match json_object(payload).and_then(|mut row| {
            normalize_row(&mut row, layout)?;
            let estimated = estimated_mapped_row_size(&row, layout, ctx, &message);
            if estimated > ctx.max_request_bytes {
                return Err(format!(
                    "row input is at least {estimated} bytes, above max_request_bytes ({})",
                    ctx.max_request_bytes
                ));
            }
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
        return Ok(Vec::new());
    }

    let mut groups = Vec::<(Vec<usize>, Vec<OwnedValue>, Vec<ConsumedMessage>)>::new();
    for (row, message) in rows.into_iter().zip(kept) {
        let signature = default_signature(&row, layout);
        if let Some((last_signature, rows, messages)) = groups.last_mut()
            && *last_signature == signature
        {
            rows.push(row);
            messages.push(message);
        } else {
            groups.push((signature, vec![row], vec![message]));
        }
    }

    let mut batches = Vec::with_capacity(groups.len());
    for (_, rows, kept) in groups {
        if let Some(batch) = encode_mapped_group(ctx, rows, kept, rejected)? {
            batches.push(batch);
        }
    }
    Ok(batches)
}

fn estimated_mapped_row_size(
    row: &OwnedValue,
    layout: &TableLayout,
    ctx: &RunContext<'_>,
    message: &ConsumedMessage,
) -> usize {
    let OwnedValue::Object(object) = row else {
        return 0;
    };
    let payload = layout.columns.iter().fold(0usize, |size, column| {
        object.get(column.name.as_str()).map_or(size, |value| {
            size.saturating_add(estimated_json_size(value))
                .saturating_add(column.name.len())
        })
    });
    payload.saturating_add(estimated_metadata_size(ctx, message))
}

fn encode_mapped_group(
    ctx: &RunContext<'_>,
    rows: Vec<OwnedValue>,
    kept: Vec<ConsumedMessage>,
    rejected: &mut Vec<Rejected>,
) -> Result<Option<(RecordBatch, Vec<u64>)>, ArrowError> {
    let layout = ctx.layout;

    let present = present_columns(&rows, layout);
    if present.is_empty() && layout.metadata.is_empty() {
        rejected.extend(kept.iter().map(|m| Rejected {
            offset: m.offset,
            reason: "payload has no field matching a table column".to_owned(),
        }));
        return Ok(None);
    }
    let payload_fields: Vec<FieldRef> = present.iter().map(|&i| layout.fields[i].clone()).collect();

    let (decoded, survivors) = decode_rows(&payload_fields, &rows, |row, reason| {
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

fn default_signature(row: &OwnedValue, layout: &TableLayout) -> Vec<usize> {
    let OwnedValue::Object(object) = row else {
        return Vec::new();
    };
    layout
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            (column.has_default && object.contains_key(column.name.as_str())).then_some(index)
        })
        .collect()
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
                other.schema()
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
/// - null in a REQUIRED column with a default is treated as absent,
/// - REPEATED columns that are absent or null become `[]`,
/// - JSON column values become their JSON text,
/// - BYTES column values remain base64 until they are decoded directly into
///   Arrow binary buffers.
fn normalize_row(row: &mut OwnedValue, layout: &TableLayout) -> Result<(), String> {
    let OwnedValue::Object(object) = row else {
        return Err("payload is not a JSON object".to_owned());
    };
    for column in &layout.columns {
        let absent_or_null = matches!(
            object.get(column.name.as_str()),
            None | Some(OwnedValue::Static(StaticNode::Null))
        );
        if absent_or_null && column.mode == Mode::Repeated {
            object.insert(column.name.clone(), empty_array());
            continue;
        }
        if absent_or_null && column.mode == Mode::Required {
            if column.has_default {
                object.remove(column.name.as_str());
            } else {
                return Err(format!(
                    "missing value for REQUIRED column '{}'",
                    column.name
                ));
            }
            continue;
        }
        if let Some(value) = object.get_mut(column.name.as_str()) {
            normalize_value(value, column)?;
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
            let OwnedValue::String(_) = value else {
                return Err(format!("column '{}' expects a base64 string", column.name));
            };
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
        BqType::Int64 | BqType::Timestamp | BqType::Date | BqType::Time => {
            validate_integral_number(value, column)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_integral_number(value: &OwnedValue, column: &Column) -> Result<(), String> {
    match value {
        OwnedValue::Static(StaticNode::F64(number))
            if number.fract() != 0.0
                || *number < i64::MIN as f64
                || *number >= -(i64::MIN as f64) =>
        {
            Err(format!(
                "column '{}' expects an integral value in the INT64 range",
                column.name
            ))
        }
        OwnedValue::Static(StaticNode::U64(number)) if *number > i64::MAX as u64 => Err(format!(
            "column '{}' value {number} does not fit INT64",
            column.name
        )),
        _ => Ok(()),
    }
}

/// Serialize a row for `arrow-json` while substituting empty strings for BYTES.
/// The decoder still builds the correct null and list structure, and the binary
/// leaves are replaced from the original base64 values afterwards.
struct ArrowObject<'a> {
    value: &'a OwnedValue,
    fields: &'a Fields,
}

impl Serialize for ArrowObject<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let OwnedValue::Object(object) = self.value else {
            return self.value.serialize(serializer);
        };
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for field in self.fields {
            if let Some(value) = object.get(field.name().as_str()) {
                map.serialize_entry(
                    field.name(),
                    &ArrowValue {
                        value,
                        data_type: field.data_type(),
                    },
                )?;
            }
        }
        map.end()
    }
}

struct ArrowValue<'a> {
    value: &'a OwnedValue,
    data_type: &'a DataType,
}

impl Serialize for ArrowValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if matches!(self.value, OwnedValue::Static(StaticNode::Null)) {
            return self.value.serialize(serializer);
        }
        match (self.data_type, self.value) {
            (DataType::Binary, OwnedValue::String(_)) => serializer.serialize_str(""),
            (DataType::List(field), OwnedValue::Array(items)) => {
                let mut sequence = serializer.serialize_seq(Some(items.len()))?;
                for value in items.iter() {
                    sequence.serialize_element(&ArrowValue {
                        value,
                        data_type: field.data_type(),
                    })?;
                }
                sequence.end()
            }
            (DataType::Struct(fields), OwnedValue::Object(_)) => ArrowObject {
                value: self.value,
                fields,
            }
            .serialize(serializer),
            _ => self.value.serialize(serializer),
        }
    }
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
    on_reject: impl FnMut(usize, String),
) -> Result<(Option<RecordBatch>, Vec<usize>), ArrowError> {
    decode_rows_with(fields, rows, on_reject, decode)
}

fn decode_rows_with<'a>(
    fields: &[FieldRef],
    rows: &'a [OwnedValue],
    mut on_reject: impl FnMut(usize, String),
    mut decoder: impl FnMut(&SchemaRef, &[&'a OwnedValue]) -> Result<RecordBatch, ArrowError>,
) -> Result<(Option<RecordBatch>, Vec<usize>), ArrowError> {
    if fields.is_empty() || rows.is_empty() {
        return Ok((None, (0..rows.len()).collect()));
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
    let row_refs: Vec<&OwnedValue> = rows.iter().collect();
    let first_error = match decoder(&schema, &row_refs) {
        Ok(batch) => {
            return Ok((Some(to_target(batch, &target)?), (0..rows.len()).collect()));
        }
        Err(error) => error,
    };

    let mut valid = vec![true; rows.len()];
    isolate_invalid_rows(
        &schema,
        &row_refs,
        0,
        first_error,
        &mut valid,
        &mut on_reject,
        &mut decoder,
    )?;

    let mut valid_rows = Vec::with_capacity(rows.len());
    let mut survivors = Vec::with_capacity(rows.len());
    for (index, row) in row_refs.into_iter().enumerate() {
        if valid[index] {
            valid_rows.push(row);
            survivors.push(index);
        }
    }
    if valid_rows.is_empty() {
        return Ok((None, survivors));
    }
    let batch = decoder(&schema, &valid_rows)?;
    Ok((Some(to_target(batch, &target)?), survivors))
}

fn isolate_invalid_rows<'a>(
    schema: &SchemaRef,
    rows: &[&'a OwnedValue],
    first_index: usize,
    error: ArrowError,
    valid: &mut [bool],
    on_reject: &mut impl FnMut(usize, String),
    decoder: &mut impl FnMut(&SchemaRef, &[&'a OwnedValue]) -> Result<RecordBatch, ArrowError>,
) -> Result<(), ArrowError> {
    if rows.len() == 1 {
        valid[first_index] = false;
        on_reject(
            first_index,
            format!("row does not match the table schema: {error}"),
        );
        return Ok(());
    }

    let midpoint = rows.len() / 2;
    let (left, right) = rows.split_at(midpoint);
    if let Err(error) = decoder(schema, left) {
        isolate_invalid_rows(schema, left, first_index, error, valid, on_reject, decoder)?;
    }
    if let Err(error) = decoder(schema, right) {
        isolate_invalid_rows(
            schema,
            right,
            first_index + midpoint,
            error,
            valid,
            on_reject,
            decoder,
        )?;
    }
    Ok(())
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

fn decode(schema: &SchemaRef, rows: &[&OwnedValue]) -> Result<RecordBatch, ArrowError> {
    let mut decoder = ReaderBuilder::new(schema.clone())
        .with_batch_size(rows.len().max(1))
        .build_decoder()?;
    let arrow_rows: Vec<_> = rows
        .iter()
        .map(|row| ArrowObject {
            value: row,
            fields: schema.fields(),
        })
        .collect();
    decoder.serialize(&arrow_rows)?;
    let batch = decoder
        .flush()?
        .ok_or_else(|| ArrowError::JsonError("decoder produced no rows".to_owned()))?;
    replace_binary_columns(batch, rows)
}

fn replace_binary_columns(
    batch: RecordBatch,
    rows: &[&OwnedValue],
) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        if contains_binary(field.data_type()) {
            let values = rows
                .iter()
                .map(|row| object_value(Some(row), field.name()))
                .collect::<Result<Vec<_>, _>>()?;
            columns.push(replace_binary_values(field, array, &values)?);
        } else {
            columns.push(array.clone());
        }
    }
    RecordBatch::try_new(schema, columns)
}

fn replace_binary_values(
    field: &FieldRef,
    array: &ArrayRef,
    values: &[Option<&OwnedValue>],
) -> Result<ArrayRef, ArrowError> {
    if array.len() != values.len() {
        return Err(ArrowError::ComputeError(format!(
            "column '{}' has {} Arrow values for {} JSON values",
            field.name(),
            array.len(),
            values.len()
        )));
    }
    match field.data_type() {
        DataType::Binary => binary_array(field.name(), values),
        DataType::List(item) if contains_binary(item.data_type()) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| type_mismatch(field, array))?;
            let mut child_values = Vec::with_capacity(list.values().len());
            for value in values {
                match value {
                    None | Some(OwnedValue::Static(StaticNode::Null)) => {}
                    Some(OwnedValue::Array(items)) => {
                        child_values.extend(items.iter().map(Some));
                    }
                    Some(_) => {
                        return Err(ArrowError::JsonError(format!(
                            "column '{}' expects an array",
                            field.name()
                        )));
                    }
                }
            }
            let child = replace_binary_values(item, list.values(), &child_values)?;
            Ok(Arc::new(ListArray::try_new(
                item.clone(),
                list.offsets().clone(),
                child,
                list.nulls().cloned(),
            )?))
        }
        DataType::Struct(fields) if contains_binary(field.data_type()) => {
            let structure = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| type_mismatch(field, array))?;
            let mut children = Vec::with_capacity(fields.len());
            for (index, child_field) in fields.iter().enumerate() {
                if contains_binary(child_field.data_type()) {
                    let child_values = values
                        .iter()
                        .map(|value| object_value(*value, child_field.name()))
                        .collect::<Result<Vec<_>, _>>()?;
                    children.push(replace_binary_values(
                        child_field,
                        structure.column(index),
                        &child_values,
                    )?);
                } else {
                    children.push(structure.column(index).clone());
                }
            }
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                children,
                structure.nulls().cloned(),
            )?))
        }
        _ => Ok(array.clone()),
    }
}

fn binary_array(name: &str, values: &[Option<&OwnedValue>]) -> Result<ArrayRef, ArrowError> {
    let data_capacity = values
        .iter()
        .filter_map(|value| match value {
            Some(OwnedValue::String(text)) => Some(text.len().saturating_mul(3) / 4),
            _ => None,
        })
        .sum();
    let mut builder = BinaryBuilder::with_capacity(values.len(), data_capacity);
    for value in values {
        match value {
            None | Some(OwnedValue::Static(StaticNode::Null)) => builder.append_null(),
            Some(OwnedValue::String(text)) => {
                let mut decoder = base64::read::DecoderReader::new(text.as_bytes(), &BASE64);
                io::copy(&mut decoder, &mut builder).map_err(|error| {
                    ArrowError::JsonError(format!("column '{name}': invalid base64: {error}"))
                })?;
                builder.append_value(&[] as &[u8]);
            }
            Some(_) => {
                return Err(ArrowError::JsonError(format!(
                    "column '{name}' expects a base64 string"
                )));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn object_value<'a>(
    value: Option<&'a OwnedValue>,
    name: &str,
) -> Result<Option<&'a OwnedValue>, ArrowError> {
    match value {
        None | Some(OwnedValue::Static(StaticNode::Null)) => Ok(None),
        Some(OwnedValue::Object(object)) => Ok(object.get(name)),
        Some(_) => Err(ArrowError::JsonError(format!(
            "column '{name}' has a non-object parent"
        ))),
    }
}

fn contains_binary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Binary => true,
        DataType::List(field) => contains_binary(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_binary(field.data_type())),
        _ => false,
    }
}

fn type_mismatch(field: &FieldRef, array: &ArrayRef) -> ArrowError {
    ArrowError::ComputeError(format!(
        "column '{}' has Arrow type {}, expected {}",
        field.name(),
        array.data_type(),
        field.data_type()
    ))
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
            let mut builder = BinaryBuilder::with_capacity(messages.len(), 0);
            for mut message in messages {
                let payload = std::mem::replace(&mut message.payload, Payload::Raw(Vec::new()));
                match payload.try_into_vec() {
                    Ok(bytes) => {
                        builder.append_value(bytes);
                        kept.push(message);
                    }
                    Err(e) => rejected.push(Rejected {
                        offset: message.offset,
                        reason: e.to_string(),
                    }),
                }
            }
            Arc::new(builder.finish())
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
                other.schema()
            ));
        }
    };
    if kind == RawKind::Json {
        let mut bytes = text.as_bytes().to_vec();
        simd_json::to_borrowed_value(&mut bytes)
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
            MetaColumn::Offset => {
                let values = messages
                    .iter()
                    .map(|message| checked_i64(message.offset, MetaColumn::Offset.name()))
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(Int64Array::from(values))
            }
            MetaColumn::Timestamp => {
                let values = messages
                    .iter()
                    .map(|message| checked_i64(message.timestamp, MetaColumn::Timestamp.name()))
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(TimestampMicrosecondArray::from(values).with_timezone(UTC))
            }
            MetaColumn::Id => {
                let mut builder = StringBuilder::with_capacity(rows, rows.saturating_mul(39));
                for message in messages {
                    write!(&mut builder, "{}", message.id).map_err(|_| {
                        ArrowError::ComputeError("cannot format message ID".to_owned())
                    })?;
                    builder.append_value("");
                }
                Arc::new(builder.finish())
            }
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

fn checked_i64(value: u64, name: &str) -> Result<i64, ArrowError> {
    i64::try_from(value)
        .map_err(|_| ArrowError::ComputeError(format!("{name} value {value} does not fit INT64")))
}

// ─── Request sizing ──────────────────────────────────────────────────────────

/// Estimate the rows per request from the Arrow allocation, then encode each
/// candidate chunk. A single row that does not fit is rejected.
fn split_into_chunks(
    batch: RecordBatch,
    offsets: Vec<u64>,
    max_bytes: usize,
    write_stream: &str,
    missing_value: i32,
    chunks: &mut Vec<Chunk>,
    rejected: &mut Vec<Rejected>,
) -> Result<(), ArrowError> {
    let schema_bytes = encode_schema(batch.schema_ref())?;
    let limits = RequestLimits {
        max_bytes,
        write_stream,
        missing_value,
    };
    split_into_chunks_with(
        batch,
        offsets,
        schema_bytes,
        limits,
        chunks,
        rejected,
        encode_batch,
    )
}

#[derive(Clone, Copy)]
struct RequestLimits<'a> {
    max_bytes: usize,
    write_stream: &'a str,
    missing_value: i32,
}

fn split_into_chunks_with(
    batch: RecordBatch,
    offsets: Vec<u64>,
    schema_bytes: Vec<u8>,
    limits: RequestLimits<'_>,
    chunks: &mut Vec<Chunk>,
    rejected: &mut Vec<Rejected>,
    mut encoder: impl FnMut(&RecordBatch) -> Result<Vec<u8>, ArrowError>,
) -> Result<(), ArrowError> {
    let rows_per_chunk = estimated_rows_per_chunk(
        &batch,
        schema_bytes.len(),
        limits.max_bytes,
        limits.write_stream,
        limits.missing_value,
    )?;
    let mut start = 0;
    while start < batch.num_rows() {
        let remaining = batch.num_rows() - start;
        let mut row_count = rows_per_chunk.min(remaining);
        loop {
            let candidate = batch.slice(start, row_count);
            let batch_bytes = encoder(&candidate)?;
            let size = encoded_append_request_size(
                limits.write_stream,
                limits.missing_value,
                schema_bytes.len(),
                batch_bytes.len(),
            );
            if size <= limits.max_bytes {
                chunks.push(Chunk {
                    batch: candidate,
                    offsets: offsets[start..start + row_count].to_vec(),
                    schema_bytes: schema_bytes.clone(),
                    batch_bytes,
                });
                start += row_count;
                break;
            }
            if row_count == 1 {
                rejected.push(Rejected {
                    offset: offsets[start],
                    reason: format!(
                        "row is {size} bytes encoded, above max_request_bytes ({})",
                        limits.max_bytes
                    ),
                });
                start += 1;
                break;
            }

            let batch_budget = limits.max_bytes.saturating_sub(schema_bytes.len());
            let smaller = row_count.saturating_mul(batch_budget) / batch_bytes.len().max(1);
            row_count = smaller.clamp(1, row_count - 1);
        }
    }
    Ok(())
}

fn estimated_rows_per_chunk(
    batch: &RecordBatch,
    schema_bytes: usize,
    max_bytes: usize,
    write_stream: &str,
    missing_value: i32,
) -> Result<usize, ArrowError> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let overhead =
        IPC_BATCH_OVERHEAD.saturating_add(batch.num_columns().saturating_mul(IPC_COLUMN_OVERHEAD));
    let request_overhead =
        encoded_append_request_size(write_stream, missing_value, schema_bytes, 0);
    let batch_budget = max_bytes.saturating_sub(request_overhead.saturating_add(overhead));
    let array_bytes = batch.columns().iter().try_fold(0usize, |size, array| {
        Ok::<_, ArrowError>(size.saturating_add(array.to_data().get_slice_memory_size()?))
    })?;
    let bytes_per_row = array_bytes.div_ceil(batch.num_rows()).max(1);
    Ok((batch_budget / bytes_per_row).max(1).min(batch.num_rows()))
}

fn encoded_append_request_size(
    write_stream: &str,
    missing_value: i32,
    schema_bytes: usize,
    batch_bytes: usize,
) -> usize {
    let schema = encoded_len_delimited_field(schema_bytes);
    let record_batch = encoded_len_delimited_field(batch_bytes);
    let arrow_data =
        encoded_len_message_field(schema).saturating_add(encoded_len_message_field(record_batch));
    let missing_value_size = if missing_value != 0 {
        1 + encoded_len_varint(missing_value as usize)
    } else {
        0
    };
    GRPC_FRAME_HEADER_BYTES
        .saturating_add(encoded_len_delimited_field(write_stream.len()))
        .saturating_add(missing_value_size)
        .saturating_add(encoded_len_message_field(arrow_data))
}

fn encoded_len_delimited_field(value_len: usize) -> usize {
    if value_len == 0 {
        0
    } else {
        1usize
            .saturating_add(encoded_len_varint(value_len))
            .saturating_add(value_len)
    }
}

fn encoded_len_message_field(message_len: usize) -> usize {
    1usize
        .saturating_add(encoded_len_varint(message_len))
        .saturating_add(message_len)
}

fn encoded_len_varint(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

/// Serialize the schema and the batch as Arrow IPC stream messages, the
/// format `AppendRowsRequest.arrow_rows` carries.
fn encode_ipc(batch: &RecordBatch) -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
    Ok((encode_schema(batch.schema_ref())?, encode_batch(batch)?))
}

fn encode_schema(schema: &SchemaRef) -> Result<Vec<u8>, ArrowError> {
    let options = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(true);
    let schema = generator.schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &options);
    let mut schema_bytes = Vec::new();
    write_message(&mut schema_bytes, schema, &options)?;
    Ok(schema_bytes)
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let options = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(true);
    let mut compression = CompressionContext::default();
    let (dictionaries, encoded) =
        generator.encode(batch, &mut tracker, &options, &mut compression)?;
    if !dictionaries.is_empty() {
        return Err(ArrowError::IpcError(
            "BigQuery writer schema unexpectedly produced dictionary batches".to_owned(),
        ));
    }
    let mut batch_bytes = Vec::new();
    write_message(&mut batch_bytes, encoded, &options)?;
    Ok(batch_bytes)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn empty_array() -> OwnedValue {
    OwnedValue::Array(Box::default())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use arrow::array::{Array, AsArray, ListArray};
    use arrow::datatypes::Int64Type;
    use gcloud_googleapis::cloud::bigquery::storage::v1::append_rows_request::{ArrowData, Rows};
    use gcloud_googleapis::cloud::bigquery::storage::v1::{
        AppendRowsRequest, ArrowRecordBatch, ArrowSchema,
    };
    use iggy_common::{HeaderKey, HeaderValue};
    use iggy_connector_sdk::Schema as PayloadSchema;
    use prost::Message;

    use super::*;
    use crate::WriteMode;
    use crate::schema::parse_table_schema;
    use crate::test_support::settings;

    const METADATA_FIELDS: &str = r#"
        {"name":"iggy_stream","type":"STRING"},
        {"name":"iggy_topic","type":"STRING"},
        {"name":"iggy_partition_id","type":"INTEGER"},
        {"name":"iggy_offset","type":"INTEGER"},
        {"name":"iggy_timestamp","type":"TIMESTAMP"},
        {"name":"iggy_id","type":"STRING"}"#;
    const WRITE_STREAM: &str = "projects/proj/datasets/ds/tables/events/streams/_default";

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
            write_stream: WRITE_STREAM,
            missing_value: 2,
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
    fn given_one_invalid_row_should_isolate_it_with_logarithmic_decodes() {
        let layout = layout(
            r#"{"name":"user_id","type":"INT64"}"#,
            WriteMode::Mapped,
            false,
        );
        let rows: Vec<OwnedValue> = (0..32)
            .map(|index| {
                let text = if index == 17 {
                    r#"{"user_id":"not-a-number"}"#.to_owned()
                } else {
                    format!(r#"{{"user_id":{index}}}"#)
                };
                let mut bytes = text.into_bytes();
                simd_json::to_owned_value(&mut bytes).unwrap()
            })
            .collect();
        let mut rejected = Vec::new();
        let mut decode_calls = 0;

        let (batch, survivors) = decode_rows_with(
            &layout.fields,
            &rows,
            |index, _| rejected.push(index),
            |schema, rows| {
                decode_calls += 1;
                decode(schema, rows)
            },
        )
        .unwrap();

        assert_eq!(rejected, vec![17]);
        assert_eq!(survivors.len(), 31);
        assert_eq!(batch.unwrap().num_rows(), 31);
        assert!(decode_calls <= 12, "used {decode_calls} decode calls");
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
    fn given_defaulted_column_present_in_some_rows_should_use_separate_writer_schemas() {
        let layout = layout(
            r#"{"name":"id","type":"INT64","mode":"REQUIRED"},
               {"name":"created","type":"TIMESTAMP","mode":"REQUIRED","defaultValueExpression":"CURRENT_TIMESTAMP()"}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(
            &layout,
            vec![
                json(1, r#"{"id":1,"created":"2024-01-02T03:04:05Z"}"#),
                json(2, r#"{"id":2}"#),
                json(3, r#"{"id":3,"created":"2024-01-03T03:04:05Z"}"#),
            ],
        );
        assert!(encoded.rejected.is_empty(), "{:?}", encoded.rejected);
        assert_eq!(encoded.chunks.len(), 3);
        assert_eq!(encoded.chunks[0].offsets, vec![1]);
        assert_eq!(
            column_names(&encoded.chunks[0].batch),
            vec!["id", "created"]
        );
        assert_eq!(encoded.chunks[1].offsets, vec![2]);
        assert_eq!(column_names(&encoded.chunks[1].batch), vec!["id"]);
        assert_eq!(encoded.chunks[2].offsets, vec![3]);
        assert_eq!(
            column_names(&encoded.chunks[2].batch),
            vec!["id", "created"]
        );
    }

    #[test]
    fn given_null_required_defaulted_column_should_omit_it() {
        let layout = layout(
            r#"{"name":"id","type":"INT64","mode":"REQUIRED"},
               {"name":"created","type":"TIMESTAMP","mode":"REQUIRED","defaultValueExpression":"CURRENT_TIMESTAMP()"}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(&layout, vec![json(1, r#"{"id":1,"created":null}"#)]);
        assert!(encoded.rejected.is_empty(), "{:?}", encoded.rejected);
        assert_eq!(column_names(&encoded.chunks[0].batch), vec!["id"]);
    }

    #[test]
    fn given_fractional_integral_values_should_reject_each_row() {
        let layout = layout(
            r#"{"name":"id","type":"INT64"},{"name":"day","type":"DATE"},{"name":"at","type":"TIME"}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(
            &layout,
            vec![
                json(1, r#"{"id":1.5,"day":"2024-01-01","at":"12:00:00"}"#),
                json(2, r#"{"id":2,"day":1.5,"at":"12:00:00"}"#),
                json(3, r#"{"id":3,"day":"2024-01-01","at":1.5}"#),
                json(4, r#"{"id":4,"day":"2024-01-01","at":"12:00:00"}"#),
            ],
        );
        assert_eq!(
            encoded
                .rejected
                .iter()
                .map(|row| row.offset)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(encoded.chunks[0].offsets, vec![4]);
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
    fn given_repeated_and_nested_bytes_should_decode_values() {
        let layout = layout(
            r#"{"name":"blobs","type":"BYTES","mode":"REPEATED"},
               {"name":"details","type":"RECORD","fields":[
                   {"name":"blob","type":"BYTES"}
               ]}"#,
            WriteMode::Mapped,
            false,
        );
        let encoded = run(
            &layout,
            vec![
                json(1, r#"{"blobs":["AQI=","AwQ="],"details":{"blob":"aGk="}}"#),
                json(2, r#"{"blobs":[],"details":{}}"#),
                json(3, r#"{"blobs":["not base64!"],"details":{}}"#),
            ],
        );

        assert_eq!(encoded.rejected.len(), 1);
        assert_eq!(encoded.rejected[0].offset, 3);
        let batch = &encoded.chunks[0].batch;
        let blobs = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let blob_values = blobs.values().as_binary::<i32>();
        assert_eq!(blob_values.value(0), &[1, 2]);
        assert_eq!(blob_values.value(1), &[3, 4]);
        assert_eq!(blobs.value_length(1), 0);

        let details = batch.column(1).as_struct();
        let nested_blob = details.column(0).as_binary::<i32>();
        assert_eq!(nested_blob.value(0), b"hi");
        assert!(nested_blob.is_null(1));
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
        let mut encoded = run(&layout, messages);
        let source = encoded.chunks.pop().unwrap();
        let schema_bytes = encode_schema(source.batch.schema_ref()).unwrap();
        let mut chunks = Vec::new();
        let mut rejected = Vec::new();
        let mut encode_calls = 0;
        split_into_chunks_with(
            source.batch,
            source.offsets,
            schema_bytes,
            RequestLimits {
                max_bytes: 64 * 1024,
                write_stream: WRITE_STREAM,
                missing_value: 2,
            },
            &mut chunks,
            &mut rejected,
            |batch| {
                encode_calls += 1;
                encode_batch(batch)
            },
        )
        .unwrap();

        assert!(rejected.is_empty());
        assert!(chunks.len() > 1);
        assert_eq!(encode_calls, chunks.len());
        for chunk in &chunks {
            assert!(
                encoded_append_request_size(
                    WRITE_STREAM,
                    2,
                    chunk.schema_bytes.len(),
                    chunk.batch_bytes.len()
                ) <= 64 * 1024
            );
            assert_eq!(chunk.batch.num_rows(), chunk.offsets.len());
        }
        let offsets: Vec<u64> = chunks.iter().flat_map(|c| c.offsets.clone()).collect();
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
    fn given_metadata_outside_int64_should_reject_the_row() {
        let layout = layout(
            &format!(r#"{{"name":"value","type":"STRING"}},{METADATA_FIELDS}"#),
            WriteMode::Mapped,
            true,
        );
        let mut overflow = json(1, r#"{"value":"x"}"#);
        overflow.offset = u64::MAX;
        let encoded = run(&layout, vec![overflow]);
        assert!(encoded.chunks.is_empty());
        assert_eq!(encoded.rejected.len(), 1);
        assert!(encoded.rejected[0].reason.contains("does not fit INT64"));
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
        assert!(without_rows(&chunk, &[2]).is_err());
    }

    #[test]
    fn given_sliced_batch_should_estimate_only_visible_array_memory() {
        let values = StringArray::from_iter_values(
            (0..100).map(|index| format!("{index}-{}", "x".repeat(4096))),
        );
        let batch = RecordBatch::try_from_iter(vec![("value", Arc::new(values) as ArrayRef)])
            .expect("batch should build")
            .slice(0, 10);
        let schema_bytes = encode_schema(batch.schema_ref()).expect("schema should encode");
        let rows = estimated_rows_per_chunk(&batch, schema_bytes.len(), 64 * 1024, WRITE_STREAM, 2)
            .expect("memory estimate should succeed");
        assert!(
            rows > 1,
            "slice backing capacity must not dominate the estimate"
        );
    }

    #[test]
    fn given_append_payload_should_account_for_protobuf_and_grpc_framing() {
        let schema = vec![1; 130];
        let batch = vec![2; 16_400];
        let request = AppendRowsRequest {
            write_stream: WRITE_STREAM.into(),
            default_missing_value_interpretation: 2,
            rows: Some(Rows::ArrowRows(ArrowData {
                writer_schema: Some(ArrowSchema {
                    serialized_schema: schema.clone(),
                }),
                rows: Some(ArrowRecordBatch {
                    serialized_record_batch: batch.clone(),
                    #[allow(deprecated)]
                    row_count: 0,
                }),
            })),
            ..Default::default()
        };
        assert_eq!(
            encoded_append_request_size(WRITE_STREAM, 2, schema.len(), batch.len()),
            GRPC_FRAME_HEADER_BYTES + request.encoded_len()
        );
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
