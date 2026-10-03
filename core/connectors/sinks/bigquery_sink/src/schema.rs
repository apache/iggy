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

//! BigQuery table schema model and its mapping to Arrow.
//!
//! The schema comes from the `tables.get` REST call as JSON. It is parsed
//! with local serde types rather than a client library model so that every
//! BigQuery type (GEOGRAPHY, RANGE, ...) deserializes, and the unsupported
//! ones can be rejected with a clear message in `open()`.
//!
//! Arrow types follow the Storage Write API Arrow mapping:
//!
//! | BigQuery   | Arrow                          |
//! |------------|--------------------------------|
//! | STRING     | Utf8                           |
//! | BYTES      | Binary                         |
//! | INT64      | Int64                          |
//! | FLOAT64    | Float64                        |
//! | BOOL       | Boolean                        |
//! | NUMERIC    | Decimal128(38, 9)              |
//! | BIGNUMERIC | Decimal256(76, 38)             |
//! | TIMESTAMP  | Timestamp(Microsecond, "UTC")  |
//! | DATETIME   | Timestamp(Microsecond, none)   |
//! | DATE       | Date32                         |
//! | TIME       | Time64(Microsecond)            |
//! | GEOGRAPHY  | Utf8 (WKT)                     |
//! | JSON       | Utf8 (JSON text)               |
//! | RECORD     | Struct                         |
//! | REPEATED   | List                           |

use crate::{Settings, WriteMode};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub(crate) const UTC: &str = "UTC";
const LIST_ITEM: &str = "item";

/// One table column, recursively for RECORD types.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Column {
    pub name: String,
    pub bq_type: BqType,
    pub mode: Mode,
    pub has_default: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BqType {
    String,
    Bytes,
    Int64,
    Float64,
    Bool,
    Numeric,
    BigNumeric,
    Timestamp,
    Datetime,
    Date,
    Time,
    Geography,
    Json,
    Record(Vec<Column>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Nullable,
    Required,
    Repeated,
}

/// Iggy metadata written next to the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetaColumn {
    Stream,
    Topic,
    PartitionId,
    Offset,
    Timestamp,
    Id,
    Headers,
}

/// What the `raw` mode payload column holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawKind {
    Json,
    String,
    Bytes,
}

/// The validated mapping from messages to the target table, built once in
/// `open()`.
#[derive(Debug)]
pub(crate) struct TableLayout {
    /// Payload columns in table order. Empty in `raw` mode.
    pub columns: Vec<Column>,
    /// Arrow field for each entry of `columns`, same index.
    pub fields: Vec<FieldRef>,
    /// Column name to index into `columns`.
    pub index: HashMap<String, usize>,
    /// Enabled metadata columns with their Arrow fields.
    pub metadata: Vec<(MetaColumn, FieldRef)>,
    /// `raw` mode payload column.
    pub raw: Option<(RawKind, FieldRef)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchemaError {
    Malformed(String),
    EmptySchema,
    UnsupportedType {
        column: String,
        bq_type: String,
    },
    MissingColumn {
        column: String,
        hint: String,
    },
    WrongType {
        column: String,
        expected: String,
        actual: String,
    },
    UnfillableColumn(String),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaError::Malformed(reason) => write!(f, "malformed table schema: {reason}"),
            SchemaError::EmptySchema => write!(f, "table has no columns"),
            SchemaError::UnsupportedType { column, bq_type } => write!(
                f,
                "column '{column}' has type {bq_type}, which the BigQuery sink does not support"
            ),
            SchemaError::MissingColumn { column, hint } => {
                write!(f, "column '{column}' is missing from the table. {hint}")
            }
            SchemaError::WrongType {
                column,
                expected,
                actual,
            } => write!(f, "column '{column}' must be {expected}, found {actual}"),
            SchemaError::UnfillableColumn(column) => write!(
                f,
                "column '{column}' is REQUIRED without a default value and raw mode never writes it"
            ),
        }
    }
}

impl MetaColumn {
    pub(crate) const ALL: [MetaColumn; 7] = [
        MetaColumn::Stream,
        MetaColumn::Topic,
        MetaColumn::PartitionId,
        MetaColumn::Offset,
        MetaColumn::Timestamp,
        MetaColumn::Id,
        MetaColumn::Headers,
    ];

    pub(crate) fn name(self) -> &'static str {
        match self {
            MetaColumn::Stream => "iggy_stream",
            MetaColumn::Topic => "iggy_topic",
            MetaColumn::PartitionId => "iggy_partition_id",
            MetaColumn::Offset => "iggy_offset",
            MetaColumn::Timestamp => "iggy_timestamp",
            MetaColumn::Id => "iggy_id",
            MetaColumn::Headers => "iggy_headers",
        }
    }

    fn ddl_type(self) -> &'static str {
        match self {
            MetaColumn::Stream | MetaColumn::Topic | MetaColumn::Id => "STRING",
            MetaColumn::PartitionId | MetaColumn::Offset => "INT64",
            MetaColumn::Timestamp => "TIMESTAMP",
            MetaColumn::Headers => "JSON",
        }
    }

    fn accepts(self, bq_type: &BqType) -> bool {
        match self {
            MetaColumn::Stream | MetaColumn::Topic | MetaColumn::Id => {
                matches!(bq_type, BqType::String)
            }
            MetaColumn::PartitionId | MetaColumn::Offset => matches!(bq_type, BqType::Int64),
            MetaColumn::Timestamp => matches!(bq_type, BqType::Timestamp),
            MetaColumn::Headers => matches!(bq_type, BqType::Json | BqType::String),
        }
    }

    fn enabled(self, settings: &Settings) -> bool {
        match self {
            MetaColumn::Headers => settings.include_headers,
            _ => settings.include_metadata,
        }
    }
}

/// Parse the body of a `tables.get` response.
pub(crate) fn parse_table_schema(body: &[u8]) -> Result<Vec<Column>, SchemaError> {
    let resource: TableResource =
        serde_json::from_slice(body).map_err(|e| SchemaError::Malformed(e.to_string()))?;
    let fields = resource
        .schema
        .map(|schema| schema.fields)
        .unwrap_or_default();
    if fields.is_empty() {
        return Err(SchemaError::EmptySchema);
    }
    fields.iter().map(Column::try_from).collect()
}

impl TableLayout {
    pub(crate) fn build(columns: Vec<Column>, settings: &Settings) -> Result<Self, SchemaError> {
        let mut by_name: HashMap<&str, &Column> =
            columns.iter().map(|c| (c.name.as_str(), c)).collect();

        let mut metadata = Vec::new();
        for meta in MetaColumn::ALL {
            if !meta.enabled(settings) {
                continue;
            }
            let column = by_name
                .remove(meta.name())
                .ok_or_else(|| SchemaError::MissingColumn {
                    column: meta.name().to_owned(),
                    hint: format!(
                        "Add `{} {}` to the table or disable it in the connector config.",
                        meta.name(),
                        meta.ddl_type()
                    ),
                })?;
            if column.mode == Mode::Repeated || !meta.accepts(&column.bq_type) {
                return Err(SchemaError::WrongType {
                    column: column.name.clone(),
                    expected: meta.ddl_type().to_owned(),
                    actual: column.type_label(),
                });
            }
            metadata.push((meta, Arc::new(meta.arrow_field(column.mode))));
        }

        match settings.mode {
            WriteMode::Raw => Self::build_raw(&columns, by_name, metadata, settings),
            WriteMode::Mapped => Ok(Self::build_mapped(&columns, &metadata)),
        }
    }

    fn build_raw(
        columns: &[Column],
        mut remaining: HashMap<&str, &Column>,
        metadata: Vec<(MetaColumn, FieldRef)>,
        settings: &Settings,
    ) -> Result<Self, SchemaError> {
        let name = settings.payload_column.as_str();
        let column = remaining
            .remove(name)
            .ok_or_else(|| SchemaError::MissingColumn {
                column: name.to_owned(),
                hint: "Add a JSON, STRING or BYTES column for the payload or set payload_column."
                    .to_owned(),
            })?;
        let kind = match (&column.bq_type, column.mode) {
            (BqType::Json, Mode::Nullable | Mode::Required) => RawKind::Json,
            (BqType::String, Mode::Nullable | Mode::Required) => RawKind::String,
            (BqType::Bytes, Mode::Nullable | Mode::Required) => RawKind::Bytes,
            _ => {
                return Err(SchemaError::WrongType {
                    column: column.name.clone(),
                    expected: "JSON, STRING or BYTES".to_owned(),
                    actual: column.type_label(),
                });
            }
        };

        // Every other column is left out of the writer schema, so BigQuery
        // fills it from its default. A REQUIRED column without one would
        // reject every row.
        if let Some(unfillable) = columns.iter().find(|c| {
            remaining.contains_key(c.name.as_str()) && c.mode == Mode::Required && !c.has_default
        }) {
            return Err(SchemaError::UnfillableColumn(unfillable.name.clone()));
        }

        let field = Arc::new(column.arrow_field());
        Ok(TableLayout {
            columns: Vec::new(),
            fields: Vec::new(),
            index: HashMap::new(),
            metadata,
            raw: Some((kind, field)),
        })
    }

    fn build_mapped(columns: &[Column], metadata: &[(MetaColumn, FieldRef)]) -> Self {
        let payload_columns: Vec<Column> = columns
            .iter()
            .filter(|c| !metadata.iter().any(|(meta, _)| meta.name() == c.name))
            .cloned()
            .collect();
        let fields = payload_columns
            .iter()
            .map(|c| Arc::new(c.arrow_field()))
            .collect();
        let index = payload_columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        TableLayout {
            columns: payload_columns,
            fields,
            index,
            metadata: metadata.to_vec(),
            raw: None,
        }
    }
}

impl Column {
    /// Arrow field as the Storage Write API expects it. REQUIRED maps to a
    /// non-nullable field. REPEATED maps to a list whose items are
    /// non-nullable, because BigQuery arrays cannot hold NULL.
    pub(crate) fn arrow_field(&self) -> Field {
        let element = self.bq_type.arrow_type();
        match self.mode {
            Mode::Nullable => Field::new(&self.name, element, true),
            Mode::Required => Field::new(&self.name, element, false),
            Mode::Repeated => Field::new(
                &self.name,
                DataType::List(Arc::new(Field::new(LIST_ITEM, element, false))),
                true,
            ),
        }
    }

    fn type_label(&self) -> String {
        let base = self.bq_type.label();
        match self.mode {
            Mode::Repeated => format!("REPEATED {base}"),
            _ => base.to_owned(),
        }
    }
}

impl BqType {
    fn arrow_type(&self) -> DataType {
        match self {
            BqType::String | BqType::Geography | BqType::Json => DataType::Utf8,
            BqType::Bytes => DataType::Binary,
            BqType::Int64 => DataType::Int64,
            BqType::Float64 => DataType::Float64,
            BqType::Bool => DataType::Boolean,
            BqType::Numeric => DataType::Decimal128(38, 9),
            BqType::BigNumeric => DataType::Decimal256(76, 38),
            BqType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            BqType::Datetime => DataType::Timestamp(TimeUnit::Microsecond, None),
            BqType::Date => DataType::Date32,
            BqType::Time => DataType::Time64(TimeUnit::Microsecond),
            BqType::Record(children) => DataType::Struct(Fields::from(
                children.iter().map(Column::arrow_field).collect::<Vec<_>>(),
            )),
        }
    }

    fn label(&self) -> &'static str {
        match self {
            BqType::String => "STRING",
            BqType::Bytes => "BYTES",
            BqType::Int64 => "INT64",
            BqType::Float64 => "FLOAT64",
            BqType::Bool => "BOOL",
            BqType::Numeric => "NUMERIC",
            BqType::BigNumeric => "BIGNUMERIC",
            BqType::Timestamp => "TIMESTAMP",
            BqType::Datetime => "DATETIME",
            BqType::Date => "DATE",
            BqType::Time => "TIME",
            BqType::Geography => "GEOGRAPHY",
            BqType::Json => "JSON",
            BqType::Record(_) => "RECORD",
        }
    }
}

impl MetaColumn {
    fn arrow_field(self, mode: Mode) -> Field {
        let data_type = match self {
            MetaColumn::Stream | MetaColumn::Topic | MetaColumn::Id | MetaColumn::Headers => {
                DataType::Utf8
            }
            MetaColumn::PartitionId | MetaColumn::Offset => DataType::Int64,
            MetaColumn::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
        };
        Field::new(self.name(), data_type, mode != Mode::Required)
    }
}

// ─── REST representation ─────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct TableResource {
    schema: Option<RestSchema>,
}

#[derive(Debug, Deserialize)]
struct RestSchema {
    #[serde(default)]
    fields: Vec<RestField>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestField {
    name: String,
    #[serde(rename = "type")]
    field_type: String,
    mode: Option<String>,
    #[serde(default)]
    fields: Vec<RestField>,
    default_value_expression: Option<String>,
}

impl TryFrom<&RestField> for Column {
    type Error = SchemaError;

    fn try_from(field: &RestField) -> Result<Self, Self::Error> {
        let bq_type = match field.field_type.to_ascii_uppercase().as_str() {
            "STRING" => BqType::String,
            "BYTES" => BqType::Bytes,
            "INTEGER" | "INT64" => BqType::Int64,
            "FLOAT" | "FLOAT64" => BqType::Float64,
            "BOOLEAN" | "BOOL" => BqType::Bool,
            "NUMERIC" | "DECIMAL" => BqType::Numeric,
            "BIGNUMERIC" | "BIGDECIMAL" => BqType::BigNumeric,
            "TIMESTAMP" => BqType::Timestamp,
            "DATETIME" => BqType::Datetime,
            "DATE" => BqType::Date,
            "TIME" => BqType::Time,
            "GEOGRAPHY" => BqType::Geography,
            "JSON" => BqType::Json,
            "RECORD" | "STRUCT" => {
                if field.fields.is_empty() {
                    return Err(SchemaError::Malformed(format!(
                        "RECORD column '{}' has no fields",
                        field.name
                    )));
                }
                BqType::Record(
                    field
                        .fields
                        .iter()
                        .map(Column::try_from)
                        .collect::<Result<_, _>>()?,
                )
            }
            other => {
                return Err(SchemaError::UnsupportedType {
                    column: field.name.clone(),
                    bq_type: other.to_owned(),
                });
            }
        };
        let mode = match field
            .mode
            .as_deref()
            .map(str::to_ascii_uppercase)
            .as_deref()
        {
            None | Some("NULLABLE") => Mode::Nullable,
            Some("REQUIRED") => Mode::Required,
            Some("REPEATED") => Mode::Repeated,
            Some(other) => {
                return Err(SchemaError::Malformed(format!(
                    "column '{}' has unknown mode {other}",
                    field.name
                )));
            }
        };
        Ok(Column {
            name: field.name.clone(),
            bq_type,
            mode,
            has_default: field.default_value_expression.is_some(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::settings;

    fn schema_json(fields: &str) -> Vec<u8> {
        format!(r#"{{"schema":{{"fields":[{fields}]}}}}"#).into_bytes()
    }

    const METADATA_FIELDS: &str = r#"
        {"name":"iggy_stream","type":"STRING"},
        {"name":"iggy_topic","type":"STRING"},
        {"name":"iggy_partition_id","type":"INTEGER"},
        {"name":"iggy_offset","type":"INTEGER","mode":"REQUIRED"},
        {"name":"iggy_timestamp","type":"TIMESTAMP"},
        {"name":"iggy_id","type":"STRING"}"#;

    #[test]
    fn given_every_scalar_type_should_map_to_storage_write_arrow_types() {
        let body = schema_json(
            r#"
            {"name":"s","type":"STRING"},
            {"name":"b","type":"BYTES"},
            {"name":"i","type":"INTEGER"},
            {"name":"f","type":"FLOAT"},
            {"name":"ok","type":"BOOLEAN"},
            {"name":"n","type":"NUMERIC"},
            {"name":"bn","type":"BIGNUMERIC"},
            {"name":"ts","type":"TIMESTAMP"},
            {"name":"dt","type":"DATETIME"},
            {"name":"d","type":"DATE"},
            {"name":"tm","type":"TIME"},
            {"name":"g","type":"GEOGRAPHY"},
            {"name":"j","type":"JSON"}"#,
        );
        let columns = parse_table_schema(&body).unwrap();
        let types: Vec<DataType> = columns
            .iter()
            .map(|c| c.arrow_field().data_type().clone())
            .collect();
        assert_eq!(
            types,
            vec![
                DataType::Utf8,
                DataType::Binary,
                DataType::Int64,
                DataType::Float64,
                DataType::Boolean,
                DataType::Decimal128(38, 9),
                DataType::Decimal256(76, 38),
                DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Date32,
                DataType::Time64(TimeUnit::Microsecond),
                DataType::Utf8,
                DataType::Utf8,
            ]
        );
    }

    #[test]
    fn given_nested_repeated_record_should_map_to_list_of_struct() {
        let body = schema_json(
            r#"{"name":"items","type":"RECORD","mode":"REPEATED","fields":[
                {"name":"sku","type":"STRING","mode":"REQUIRED"},
                {"name":"qty","type":"INT64"}
            ]}"#,
        );
        let columns = parse_table_schema(&body).unwrap();
        let field = columns[0].arrow_field();
        let DataType::List(item) = field.data_type() else {
            panic!("expected list, got {:?}", field.data_type());
        };
        assert!(!item.is_nullable());
        let DataType::Struct(children) = item.data_type() else {
            panic!("expected struct item");
        };
        assert_eq!(children[0].name(), "sku");
        assert!(!children[0].is_nullable());
        assert!(children[1].is_nullable());
    }

    #[test]
    fn given_required_column_should_be_non_nullable() {
        let columns = parse_table_schema(&schema_json(
            r#"{"name":"id","type":"INT64","mode":"REQUIRED"}"#,
        ))
        .unwrap();
        assert!(!columns[0].arrow_field().is_nullable());
    }

    #[test]
    fn given_interval_column_should_be_rejected() {
        let result = parse_table_schema(&schema_json(r#"{"name":"span","type":"INTERVAL"}"#));
        assert!(matches!(
            result,
            Err(SchemaError::UnsupportedType { ref column, .. }) if column == "span"
        ));
    }

    #[test]
    fn given_nested_range_column_should_be_rejected() {
        let result = parse_table_schema(&schema_json(
            r#"{"name":"r","type":"RECORD","fields":[{"name":"window","type":"RANGE"}]}"#,
        ));
        assert!(matches!(
            result,
            Err(SchemaError::UnsupportedType { ref column, .. }) if column == "window"
        ));
    }

    #[test]
    fn given_table_without_schema_should_fail() {
        assert_eq!(parse_table_schema(b"{}"), Err(SchemaError::EmptySchema));
    }

    #[test]
    fn given_default_value_expression_should_mark_column_with_default() {
        let columns = parse_table_schema(&schema_json(
            r#"{"name":"created","type":"TIMESTAMP","mode":"REQUIRED","defaultValueExpression":"CURRENT_TIMESTAMP()"}"#,
        ))
        .unwrap();
        assert!(columns[0].has_default);
    }

    #[test]
    fn given_mapped_mode_with_metadata_should_split_payload_and_metadata_columns() {
        let body = schema_json(&format!(
            r#"{{"name":"user_id","type":"INT64"}},{METADATA_FIELDS},{{"name":"event","type":"STRING"}}"#
        ));
        let layout = TableLayout::build(
            parse_table_schema(&body).unwrap(),
            &settings(WriteMode::Mapped, true),
        )
        .unwrap();
        let payload: Vec<&str> = layout.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(payload, vec!["user_id", "event"]);
        assert_eq!(layout.metadata.len(), 6);
        let offset = layout
            .metadata
            .iter()
            .find(|(meta, _)| *meta == MetaColumn::Offset)
            .unwrap();
        assert!(!offset.1.is_nullable());
        assert_eq!(layout.index["event"], 1);
    }

    #[test]
    fn given_metadata_enabled_but_column_missing_should_fail_with_hint() {
        let body = schema_json(r#"{"name":"user_id","type":"INT64"}"#);
        let result = TableLayout::build(
            parse_table_schema(&body).unwrap(),
            &settings(WriteMode::Mapped, true),
        );
        let Err(SchemaError::MissingColumn { column, hint }) = result else {
            panic!("expected MissingColumn");
        };
        assert_eq!(column, "iggy_stream");
        assert!(hint.contains("iggy_stream STRING"));
    }

    #[test]
    fn given_metadata_column_with_wrong_type_should_fail() {
        let body = schema_json(
            r#"{"name":"iggy_stream","type":"STRING"},
               {"name":"iggy_topic","type":"STRING"},
               {"name":"iggy_partition_id","type":"STRING"}"#,
        );
        let result = TableLayout::build(
            parse_table_schema(&body).unwrap(),
            &settings(WriteMode::Mapped, true),
        );
        assert!(matches!(
            result,
            Err(SchemaError::WrongType { ref column, .. }) if column == "iggy_partition_id"
        ));
    }

    #[test]
    fn given_metadata_disabled_should_treat_iggy_columns_as_payload() {
        let body = schema_json(r#"{"name":"iggy_offset","type":"INT64"}"#);
        let layout = TableLayout::build(
            parse_table_schema(&body).unwrap(),
            &settings(WriteMode::Mapped, false),
        )
        .unwrap();
        assert!(layout.metadata.is_empty());
        assert_eq!(layout.columns.len(), 1);
    }

    #[test]
    fn given_raw_mode_with_json_payload_column_should_build() {
        let body = schema_json(&format!(
            r#"{{"name":"payload","type":"JSON"}},{METADATA_FIELDS}"#
        ));
        let layout = TableLayout::build(
            parse_table_schema(&body).unwrap(),
            &settings(WriteMode::Raw, true),
        )
        .unwrap();
        let (kind, field) = layout.raw.as_ref().unwrap();
        assert_eq!(*kind, RawKind::Json);
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(layout.columns.is_empty());
    }

    #[test]
    fn given_raw_mode_without_payload_column_should_fail() {
        let body = schema_json(r#"{"name":"data","type":"BYTES"}"#);
        let result = TableLayout::build(
            parse_table_schema(&body).unwrap(),
            &settings(WriteMode::Raw, false),
        );
        assert!(matches!(
            result,
            Err(SchemaError::MissingColumn { ref column, .. }) if column == "payload"
        ));
    }

    #[test]
    fn given_raw_mode_with_numeric_payload_column_should_fail() {
        let body = schema_json(r#"{"name":"payload","type":"INT64"}"#);
        let result = TableLayout::build(
            parse_table_schema(&body).unwrap(),
            &settings(WriteMode::Raw, false),
        );
        assert!(matches!(result, Err(SchemaError::WrongType { .. })));
    }

    #[test]
    fn given_raw_mode_with_required_column_without_default_should_fail() {
        let body = schema_json(
            r#"{"name":"payload","type":"BYTES"},
               {"name":"tenant","type":"STRING","mode":"REQUIRED"}"#,
        );
        let result = TableLayout::build(
            parse_table_schema(&body).unwrap(),
            &settings(WriteMode::Raw, false),
        );
        assert_eq!(
            result.unwrap_err(),
            SchemaError::UnfillableColumn("tenant".into())
        );
    }

    #[test]
    fn given_raw_mode_with_required_column_with_default_should_build() {
        let body = schema_json(
            r#"{"name":"payload","type":"BYTES"},
               {"name":"tenant","type":"STRING","mode":"REQUIRED","defaultValueExpression":"'acme'"}"#,
        );
        let layout = TableLayout::build(
            parse_table_schema(&body).unwrap(),
            &settings(WriteMode::Raw, false),
        )
        .unwrap();
        assert_eq!(layout.raw.unwrap().0, RawKind::Bytes);
    }
}
