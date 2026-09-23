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

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, Decimal128Builder, Int64Array, StringArray, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use fluss::{
    error::Error as FlussError,
    metadata::{Column, TableDescriptor},
};
use iggy_connector_sdk::{ConsumedMessage, Error as IggyError, Payload};
use thiserror::Error;

use simd_json::Error as SimdJsonError;

use crate::{PayloadFormat, config::TableConfig};

const MESSAGE_ID_COLUMN_NAME: &str = "id";
const CHECKSUM_COLUMN_NAME: &str = "checksum";
const MESSAGE_TIMESTAMP_COLUMN_NAME: &str = "iggy_timestamp";
const ORIGIN_TIMESTAMP_COLUMN_NAME: &str = "iggy_origin_timestamp";
const MESSAGE_OFFSET_COLUMN_NAME: &str = "iggy_offset";
const STREAM_COLUMN_NAME: &str = "iggy_stream";
const TOPIC_COLUMN_NAME: &str = "iggy_topic";
const PARTITION_ID_COLUMN_NAME: &str = "iggy_partition_id";
const PAYLOAD_COLUMN_NAME: &str = "payload";
const UNSIGNED_64_DECIMAL_PRECISION: u8 = 20;
const TIMESTAMP_PRECISION: u32 = 6;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Fluss(Box<FlussError>),
    #[error(
        "Failed to convert Iggy message ID {message_id} field '{field}' with value {value} to Fluss TIMESTAMP_LTZ(6): {reason}"
    )]
    TimestampConversion {
        message_id: u128,
        field: &'static str,
        value: u64,
        reason: String,
    },
    #[error(
        "Payload from Iggy message ID {message_id} is not valid UTF-8 for a Fluss STRING column: {source}"
    )]
    InvalidPayloadUtf8 {
        message_id: u128,
        #[source]
        source: SimdJsonError,
    },
    #[error("Failed to convert payload from Iggy message ID {message_id} to bytes: {source}")]
    PayloadBytesConversion {
        message_id: u128,
        #[source]
        source: IggyError,
    },

    #[error("Payload format is not supported for Iggy Message ID {message_id}: {reason}")]
    PayloadFormatNotSupported {
        message_id: u128,
        reason: &'static str,
    },
    #[error("Failed to create Arrow record batch: {source}")]
    RecordBatchCreation {
        #[source]
        source: ArrowError,
    },
    #[error("Unsupported static table column: {name}")]
    UnsupportedColumn { name: String },
}

impl From<FlussError> for Error {
    fn from(error: FlussError) -> Self {
        Self::Fluss(Box::new(error))
    }
}

impl From<Error> for IggyError {
    fn from(error: Error) -> Self {
        let message = error.to_string();
        match error {
            Error::Fluss(_) => Self::CannotStoreData(message),
            Error::InvalidPayloadUtf8 { .. }
            | Error::PayloadFormatNotSupported { .. }
            | Error::RecordBatchCreation { .. } => Self::Serialization(message),
            Error::TimestampConversion { .. }
            | Error::PayloadBytesConversion { .. }
            | Error::UnsupportedColumn { .. } => Self::InvalidRecordValue(message),
        }
    }
}

struct ConvertedMessage {
    message_id: u128,
    id: String,
    offset: i128,
    checksum: i128,
    timestamp: i64,
    origin_timestamp: i64,
    payload: Payload,
}

impl TryFrom<ConsumedMessage> for ConvertedMessage {
    type Error = Error;
    fn try_from(message: ConsumedMessage) -> Result<Self, Self::Error> {
        let message_id = message.id;
        Ok(Self {
            message_id,
            id: string_from_id(message.id),
            offset: i128::from(message.offset),
            checksum: i128::from(message.checksum),
            timestamp: i64::try_from(message.timestamp).map_err(|error| {
                Error::TimestampConversion {
                    message_id,
                    field: "timestamp",
                    value: message.timestamp,
                    reason: error.to_string(),
                }
            })?,
            origin_timestamp: i64::try_from(message.origin_timestamp).map_err(|error| {
                Error::TimestampConversion {
                    message_id,
                    field: "origin_timestamp",
                    value: message.origin_timestamp,
                    reason: error.to_string(),
                }
            })?,
            payload: message.payload,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColumnKind {
    MessageId,
    Checksum,
    MessageTimestamp,
    OriginTimestamp,
    MessageOffset,
    Stream,
    Topic,
    PartitionId,
}

impl ColumnKind {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::MessageId => MESSAGE_ID_COLUMN_NAME,
            Self::Checksum => CHECKSUM_COLUMN_NAME,
            Self::MessageTimestamp => MESSAGE_TIMESTAMP_COLUMN_NAME,
            Self::OriginTimestamp => ORIGIN_TIMESTAMP_COLUMN_NAME,
            Self::MessageOffset => MESSAGE_OFFSET_COLUMN_NAME,
            Self::Stream => STREAM_COLUMN_NAME,
            Self::Topic => TOPIC_COLUMN_NAME,
            Self::PartitionId => PARTITION_ID_COLUMN_NAME,
        }
    }
}

impl From<ColumnKind> for Column {
    fn from(column: ColumnKind) -> Self {
        let name = column.name();
        match column {
            ColumnKind::MessageId => Column::new(name, fluss::metadata::DataTypes::string())
                .with_comment("Apache Iggy message ID"),
            ColumnKind::Checksum => Column::new(
                name,
                fluss::metadata::DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION as u32, 0),
            )
            .with_comment("Apache Iggy message checksum"),
            ColumnKind::MessageTimestamp => Column::new(
                name,
                fluss::metadata::DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
            )
            .with_comment("Apache Iggy message timestamp"),
            ColumnKind::OriginTimestamp => Column::new(
                name,
                fluss::metadata::DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
            )
            .with_comment("Apache Iggy message origin timestamp"),
            ColumnKind::MessageOffset => Column::new(
                name,
                fluss::metadata::DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION as u32, 0),
            )
            .with_comment("Apache Iggy message offset"),
            ColumnKind::Stream => Column::new(name, fluss::metadata::DataTypes::string())
                .with_comment("Apache Iggy stream name"),
            ColumnKind::Topic => Column::new(name, fluss::metadata::DataTypes::string())
                .with_comment("Apache Iggy topic name"),
            ColumnKind::PartitionId => Column::new(name, fluss::metadata::DataTypes::bigint())
                .with_comment("Apache Iggy partition ID"),
        }
    }
}

impl TryFrom<&Field> for ColumnKind {
    type Error = Error;

    fn try_from(value: &Field) -> Result<Self, Self::Error> {
        match value.name().as_str() {
            MESSAGE_ID_COLUMN_NAME => Ok(Self::MessageId),
            CHECKSUM_COLUMN_NAME => Ok(Self::Checksum),
            MESSAGE_TIMESTAMP_COLUMN_NAME => Ok(Self::MessageTimestamp),
            ORIGIN_TIMESTAMP_COLUMN_NAME => Ok(Self::OriginTimestamp),
            MESSAGE_OFFSET_COLUMN_NAME => Ok(Self::MessageOffset),
            STREAM_COLUMN_NAME => Ok(Self::Stream),
            TOPIC_COLUMN_NAME => Ok(Self::Topic),
            PARTITION_ID_COLUMN_NAME => Ok(Self::PartitionId),
            name => Err(Error::UnsupportedColumn {
                name: name.to_owned(),
            }),
        }
    }
}

#[derive(Debug)]
pub struct RowContext<'a> {
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition_id: u32,
}

pub struct StaticTableBatchBuilder<'a> {
    id_builder: StringBuilder,
    checksum_builder: Decimal128Builder,
    message_offset_builder: Decimal128Builder,
    message_timestamp_builder: TimestampMicrosecondBuilder,
    origin_timestamp_builder: TimestampMicrosecondBuilder,
    payload_string_builder: StringBuilder,
    payload_binary_builder: BinaryBuilder,
    payload_format: PayloadFormat,
    context: RowContext<'a>,
    schema: Arc<Schema>,
    len: usize,
}

impl<'a> StaticTableBatchBuilder<'a> {
    pub(crate) fn new(
        payload_format: PayloadFormat,
        context: RowContext<'a>,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            id_builder: StringBuilder::new(),
            checksum_builder: Decimal128Builder::new()
                .with_data_type(DataType::Decimal128(UNSIGNED_64_DECIMAL_PRECISION, 0)),
            message_offset_builder: Decimal128Builder::new()
                .with_data_type(DataType::Decimal128(UNSIGNED_64_DECIMAL_PRECISION, 0)),
            message_timestamp_builder: TimestampMicrosecondBuilder::new().with_timezone("UTC"),
            origin_timestamp_builder: TimestampMicrosecondBuilder::new().with_timezone("UTC"),
            payload_binary_builder: BinaryBuilder::new(),
            payload_string_builder: StringBuilder::new(),
            payload_format,
            context,
            schema,
            len: 0,
        }
    }

    pub(crate) fn append(&mut self, message: ConsumedMessage) -> Result<(), Error> {
        let ConvertedMessage {
            message_id,
            id,
            offset,
            checksum,
            timestamp,
            origin_timestamp,
            payload,
        } = message.try_into()?;

        match &self.payload_format {
            PayloadFormat::Bytea => {
                let payload = payload
                    .try_into_vec()
                    .map_err(|source| Error::PayloadBytesConversion { message_id, source })?;
                self.payload_binary_builder.append_value(payload);
            }
            PayloadFormat::Json | PayloadFormat::Text => match payload {
                Payload::Json(value) => self.payload_string_builder.append_value(
                    simd_json::to_string(&value)
                        .map_err(|source| Error::InvalidPayloadUtf8 { message_id, source })?,
                ),
                Payload::Text(str) => self.payload_string_builder.append_value(str),
                Payload::Proto(str) => self.payload_string_builder.append_value(str),
                Payload::Avro { .. } | Payload::Raw { .. } | Payload::FlatBuffer { .. } => {
                    return Err(Error::PayloadFormatNotSupported {
                        message_id,
                        reason: "Avro, Raw, and FlatBuffer are not supported for Payload type for Payload Format JSON, or Text. Use Bytea instead: [payload_format = bytea]",
                    });
                }
            },
        };

        for field in self.schema.fields.iter() {
            let name = field.name().as_str();
            match name {
                MESSAGE_ID_COLUMN_NAME => self.id_builder.append_value(&id),
                CHECKSUM_COLUMN_NAME => self.checksum_builder.append_value(checksum),
                MESSAGE_OFFSET_COLUMN_NAME => self.message_offset_builder.append_value(offset),
                MESSAGE_TIMESTAMP_COLUMN_NAME => {
                    self.message_timestamp_builder.append_value(timestamp)
                }
                ORIGIN_TIMESTAMP_COLUMN_NAME => {
                    self.origin_timestamp_builder.append_value(origin_timestamp)
                }
                _ => continue, // skipping unknown columns
            }
        }
        self.len += 1;
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch, Error> {
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields.len());

        for field in self.schema.fields.iter() {
            let name = field.name().as_str();
            let array: ArrayRef = match name {
                MESSAGE_ID_COLUMN_NAME => Arc::new(self.id_builder.finish()),
                CHECKSUM_COLUMN_NAME => Arc::new(self.checksum_builder.finish()),
                MESSAGE_OFFSET_COLUMN_NAME => Arc::new(self.message_offset_builder.finish()),
                MESSAGE_TIMESTAMP_COLUMN_NAME => Arc::new(self.message_timestamp_builder.finish()),
                ORIGIN_TIMESTAMP_COLUMN_NAME => Arc::new(self.origin_timestamp_builder.finish()),
                STREAM_COLUMN_NAME => {
                    Arc::new(StringArray::new_repeated(self.context.stream, self.len))
                }
                TOPIC_COLUMN_NAME => {
                    Arc::new(StringArray::new_repeated(self.context.topic, self.len))
                }
                PARTITION_ID_COLUMN_NAME => Arc::new(Int64Array::from_value(
                    i64::from(self.context.partition_id),
                    self.len,
                )),
                _ => continue, // skipping unknown columns
            };
            cols.push(array);
        }

        let payload: ArrayRef = match self.payload_format {
            PayloadFormat::Bytea => Arc::new(self.payload_binary_builder.finish()),
            PayloadFormat::Json | PayloadFormat::Text => {
                Arc::new(self.payload_string_builder.finish())
            }
        };
        cols.push(payload);
        self.len = 0;
        RecordBatch::try_new(Arc::clone(&self.schema), cols)
            .map_err(|source| Error::RecordBatchCreation { source })
    }
}

#[derive(Debug, Default)]
pub struct StaticTableDescriptorBuilder<'a> {
    extra_columns: &'a [ColumnKind],
    payload_format: PayloadFormat,
    table_config: Option<&'a TableConfig>,
}

impl<'a> StaticTableDescriptorBuilder<'a> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_table_config(mut self, table_config: &'a TableConfig) -> Self {
        self.table_config = Some(table_config);
        self
    }

    pub(crate) fn with_extra_columns(mut self, extra_columns: &'a [ColumnKind]) -> Self {
        self.extra_columns = extra_columns;
        self
    }

    pub(crate) fn with_payload_format(mut self, payload_format: PayloadFormat) -> Self {
        self.payload_format = payload_format;
        self
    }

    fn build_schema(&self) -> Result<fluss::metadata::Schema, Error> {
        let payload_column = match self.payload_format {
            PayloadFormat::Bytea => {
                Column::new(PAYLOAD_COLUMN_NAME, fluss::metadata::DataTypes::bytes())
                    .with_comment("Apache Iggy message payload")
            }

            PayloadFormat::Json | PayloadFormat::Text => {
                Column::new(PAYLOAD_COLUMN_NAME, fluss::metadata::DataTypes::string())
                    .with_comment("Apache Iggy message payload")
            }
        };

        let columns: Vec<Column> = self
            .extra_columns
            .iter()
            .copied()
            .map(Into::into)
            .chain(std::iter::once(payload_column))
            .collect();

        let mut builder = fluss::metadata::Schema::builder().with_columns(columns);

        if let Some(table_config) = &self.table_config {
            builder = table_config.enrich_schema_builder(builder);
        }
        Ok(builder.build()?)
    }

    pub(crate) fn build(self) -> Result<TableDescriptor, Error> {
        let schema = self.build_schema()?;
        let mut builder = TableDescriptor::builder()
            .comment("Stores Apache Iggy messages written by the Fluss sink connector")
            .schema(schema);

        if let Some(table_config) = self.table_config {
            builder = table_config.enrich_descriptor_builder(builder);
        }

        builder.build().map_err(Into::into)
    }
}

fn string_from_id(id: u128) -> String {
    format!("{:032x}", id)
}

pub(crate) fn create_extra_columns(
    include_metadata: bool,
    include_checksum: bool,
    include_origin_timestamp: bool,
) -> Vec<ColumnKind> {
    let mut columns: Vec<ColumnKind> = Vec::with_capacity(10);
    columns.push(ColumnKind::MessageId);

    if include_checksum {
        columns.push(ColumnKind::Checksum);
    };

    if include_metadata {
        columns.extend([
            ColumnKind::MessageOffset,
            ColumnKind::MessageTimestamp,
            ColumnKind::Stream,
            ColumnKind::Topic,
            ColumnKind::PartitionId,
        ]);
    };

    if include_origin_timestamp {
        columns.push(ColumnKind::OriginTimestamp);
    }

    columns
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use fluss::metadata::{Column, DataTypes};
    use iggy_connector_sdk::{ConsumedMessage, Payload, Schema};

    use super::{
        CHECKSUM_COLUMN_NAME, ColumnKind, ConvertedMessage, Error as SchemaError,
        MESSAGE_ID_COLUMN_NAME, MESSAGE_OFFSET_COLUMN_NAME, MESSAGE_TIMESTAMP_COLUMN_NAME,
        ORIGIN_TIMESTAMP_COLUMN_NAME, PARTITION_ID_COLUMN_NAME, PAYLOAD_COLUMN_NAME, RowContext,
        STREAM_COLUMN_NAME, StaticTableBatchBuilder, StaticTableDescriptorBuilder,
        TIMESTAMP_PRECISION, TOPIC_COLUMN_NAME, UNSIGNED_64_DECIMAL_PRECISION,
        create_extra_columns,
    };
    use crate::{PayloadFormat, ResolvedFlussSinkConfig, config::TableConfig};

    const MESSAGE_TIMESTAMP: i64 = 1_700_000_000_123_456;
    const ORIGIN_TIMESTAMP: u64 = 1_700_000_000_120_789;

    fn test_config(payload_format: PayloadFormat) -> ResolvedFlussSinkConfig {
        ResolvedFlussSinkConfig {
            payload_format,
            ..ResolvedFlussSinkConfig::default()
        }
    }

    fn config_without_optional_columns(payload_format: PayloadFormat) -> ResolvedFlussSinkConfig {
        ResolvedFlussSinkConfig {
            include_checksum: false,
            include_metadata: false,
            include_origin_timestamp: false,
            ..test_config(payload_format)
        }
    }

    fn test_message(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 101,
            offset: 202,
            checksum: 303,
            timestamp: MESSAGE_TIMESTAMP as u64,
            origin_timestamp: ORIGIN_TIMESTAMP,
            headers: None,
            payload,
        }
    }

    fn test_context() -> RowContext<'static> {
        RowContext {
            stream: "orders",
            topic: "created",
            partition_id: 7,
        }
    }

    fn arrow_column<T: Array + 'static>(columns: &[ArrayRef], index: usize) -> &T {
        columns[index]
            .as_any()
            .downcast_ref::<T>()
            .expect("Arrow column should have the expected type")
    }

    fn extra_columns(config: &ResolvedFlussSinkConfig) -> Vec<ColumnKind> {
        create_extra_columns(
            config.include_metadata,
            config.include_checksum,
            config.include_origin_timestamp,
        )
    }

    fn arrow_schema(
        payload_format: PayloadFormat,
        columns: &[ColumnKind],
    ) -> Arc<arrow::datatypes::Schema> {
        let descriptor = StaticTableDescriptorBuilder::new()
            .with_extra_columns(columns)
            .with_payload_format(payload_format)
            .build()
            .expect("Table descriptor should build");

        fluss::record::to_arrow_schema(descriptor.schema().row_type())
            .expect("Fluss schema should convert to Arrow schema")
    }

    fn build_arrow_rows(
        config: &ResolvedFlussSinkConfig,
        messages: Vec<ConsumedMessage>,
    ) -> RecordBatch {
        let columns = extra_columns(config);
        let context = test_context();
        let mut builder = StaticTableBatchBuilder::new(
            config.payload_format,
            context,
            arrow_schema(config.payload_format, &columns),
        );
        for message in messages {
            builder.append(message).expect("Message should append");
        }
        builder.finish().expect("Record batch should build")
    }

    #[test]
    fn given_i64_max_timestamps_when_encoding_message_should_preserve_values() {
        let mut message = test_message(Payload::Raw(vec![1]));
        message.timestamp = i64::MAX as u64;
        message.origin_timestamp = i64::MAX as u64;

        let encoded_message = ConvertedMessage::try_from(message)
            .expect("Timestamps within the i64 range should encode");

        assert_eq!(encoded_message.timestamp, i64::MAX);
        assert_eq!(encoded_message.origin_timestamp, i64::MAX);
    }

    #[test]
    fn given_all_extra_columns_when_converting_messages_should_preserve_array_contents() {
        let config = test_config(PayloadFormat::Text);
        let first_message = test_message(Payload::Text("first".to_string()));
        let mut second_message = test_message(Payload::Text("second".to_string()));
        second_message.id = 102;
        second_message.checksum = 304;
        second_message.offset = 203;
        second_message.timestamp = MESSAGE_TIMESTAMP as u64 + 1;
        second_message.origin_timestamp = ORIGIN_TIMESTAMP + 1;

        let rows = build_arrow_rows(&config, vec![first_message, second_message]);
        let columns = rows.columns();

        assert_eq!(columns.len(), extra_columns(&config).len() + 1);

        let ids = arrow_column::<StringArray>(columns, 0);
        assert_eq!(
            ids.iter().collect::<Vec<_>>(),
            vec![
                Some("00000000000000000000000000000065"),
                Some("00000000000000000000000000000066"),
            ]
        );

        let checksums = arrow_column::<Decimal128Array>(columns, 1);
        assert_eq!(
            checksums.iter().collect::<Vec<_>>(),
            vec![Some(303), Some(304)]
        );

        let offsets = arrow_column::<Decimal128Array>(columns, 2);
        assert_eq!(
            offsets.iter().collect::<Vec<_>>(),
            vec![Some(202), Some(203)]
        );

        let timestamps = arrow_column::<TimestampMicrosecondArray>(columns, 3);
        assert_eq!(
            timestamps.iter().collect::<Vec<_>>(),
            vec![Some(MESSAGE_TIMESTAMP), Some(MESSAGE_TIMESTAMP + 1)]
        );

        let streams = arrow_column::<StringArray>(columns, 4);
        assert_eq!(
            streams.iter().collect::<Vec<_>>(),
            vec![Some("orders"), Some("orders")]
        );

        let topics = arrow_column::<StringArray>(columns, 5);
        assert_eq!(
            topics.iter().collect::<Vec<_>>(),
            vec![Some("created"), Some("created")]
        );

        let partition_ids = arrow_column::<Int64Array>(columns, 6);
        assert_eq!(
            partition_ids.iter().collect::<Vec<_>>(),
            vec![Some(7), Some(7)]
        );

        let origin_timestamps = arrow_column::<TimestampMicrosecondArray>(columns, 7);
        assert_eq!(
            origin_timestamps.iter().collect::<Vec<_>>(),
            vec![
                Some(ORIGIN_TIMESTAMP as i64),
                Some(ORIGIN_TIMESTAMP as i64 + 1),
            ]
        );

        let payloads = arrow_column::<StringArray>(columns, 8);
        assert_eq!(
            payloads.iter().collect::<Vec<_>>(),
            vec![Some("first"), Some("second")]
        );
    }

    #[test]
    fn given_binary_messages_when_building_minimal_arrow_columns_should_preserve_payloads() {
        let config = config_without_optional_columns(PayloadFormat::Bytea);
        let first_message = test_message(Payload::Raw(vec![0, 127, 255]));
        let mut second_message = test_message(Payload::Raw(vec![1, 2, 3]));
        second_message.id = 102;

        let rows = build_arrow_rows(&config, vec![first_message, second_message]);
        let columns = rows.columns();

        assert_eq!(columns.len(), 2);

        let ids = arrow_column::<StringArray>(columns, 0);
        assert_eq!(ids.value(0), "00000000000000000000000000000065");
        assert_eq!(ids.value(1), "00000000000000000000000000000066");

        let payloads = arrow_column::<BinaryArray>(columns, 1);
        assert_eq!(payloads.value(0), &[0, 127, 255]);
        assert_eq!(payloads.value(1), &[1, 2, 3]);
    }

    #[test]
    fn given_invalid_utf8_before_valid_message_when_encoding_arrow_rows_should_skip_invalid_row() {
        let config = config_without_optional_columns(PayloadFormat::Text);
        let columns = extra_columns(&config);
        let context = test_context();

        let mut builder = StaticTableBatchBuilder::new(
            config.payload_format,
            context,
            arrow_schema(config.payload_format, &columns),
        );
        let mut valid_message = test_message(Payload::Text("valid".to_string()));
        valid_message.id = 102;
        let messages = [test_message(Payload::Raw(vec![0xff])), valid_message];
        let mut errors = 0;
        for message in messages {
            if let Err(error) = builder.append(message) {
                assert!(matches!(
                    error,
                    SchemaError::PayloadFormatNotSupported {
                        message_id: 101,
                        ..
                    }
                ));
                errors += 1;
            }
        }

        let rows = builder.finish().expect("Record batch should build");
        assert_eq!(errors, 1);
        assert_eq!(rows.num_rows(), 1);
        let ids = arrow_column::<StringArray>(rows.columns(), 0);
        assert_eq!(ids.value(0), "00000000000000000000000000000066");
        let payloads = arrow_column::<StringArray>(rows.columns(), 1);
        assert_eq!(payloads.value(0), "valid");
    }

    #[test]
    fn given_timestamp_above_i64_max_when_encoding_message_should_return_conversion_error() {
        let mut message = test_message(Payload::Text(PAYLOAD_COLUMN_NAME.to_owned()));
        message.timestamp = i64::MAX as u64 + 1;

        let error = ConvertedMessage::try_from(message)
            .err()
            .expect("Timestamp above the i64 range should fail");

        assert!(matches!(
            error,
            SchemaError::TimestampConversion {
                message_id: 101,
                field: "timestamp",
                value,
                ..
            } if value == i64::MAX as u64 + 1
        ));
    }

    #[test]
    fn given_default_config_when_building_layout_should_include_all_columns_in_order() {
        let columns = extra_columns(&ResolvedFlussSinkConfig::default());

        assert_eq!(
            columns,
            [
                ColumnKind::MessageId,
                ColumnKind::Checksum,
                ColumnKind::MessageOffset,
                ColumnKind::MessageTimestamp,
                ColumnKind::Stream,
                ColumnKind::Topic,
                ColumnKind::PartitionId,
                ColumnKind::OriginTimestamp,
            ]
        );
    }

    #[test]
    fn given_supported_arrow_fields_when_converting_should_return_matching_column_kinds() {
        for expected in [
            ColumnKind::MessageId,
            ColumnKind::Checksum,
            ColumnKind::MessageTimestamp,
            ColumnKind::OriginTimestamp,
            ColumnKind::MessageOffset,
            ColumnKind::Stream,
            ColumnKind::Topic,
            ColumnKind::PartitionId,
        ] {
            let field = Field::new(expected.name(), DataType::Null, false);

            assert_eq!(
                ColumnKind::try_from(&field).expect("Arrow field should be supported"),
                expected
            );
        }
    }

    #[test]
    fn given_unsupported_arrow_field_when_converting_should_return_error() {
        let field = Field::new(PAYLOAD_COLUMN_NAME, DataType::Null, false);

        let error = ColumnKind::try_from(&field)
            .expect_err("Unsupported Arrow field should return an error");

        assert!(matches!(
            error,
            SchemaError::UnsupportedColumn { name } if name == PAYLOAD_COLUMN_NAME
        ));
    }

    #[test]
    fn given_optional_columns_disabled_when_building_layout_should_only_include_id_and_payload() {
        let config = config_without_optional_columns(PayloadFormat::Bytea);

        assert_eq!(extra_columns(&config), [ColumnKind::MessageId]);
    }

    #[test]
    fn given_payload_formats_when_building_schema_should_use_matching_payload_types() {
        for (payload_format, expected_data_type) in [
            (PayloadFormat::Bytea, DataTypes::bytes()),
            (PayloadFormat::Json, DataTypes::string()),
            (PayloadFormat::Text, DataTypes::string()),
        ] {
            let config = config_without_optional_columns(payload_format);
            let columns = extra_columns(&config);
            let schema = StaticTableDescriptorBuilder::new()
                .with_extra_columns(&columns)
                .with_payload_format(config.payload_format)
                .build_schema()
                .expect("Schema should build");
            let payload_column = schema
                .columns()
                .last()
                .expect("Schema should contain a payload column");

            assert_eq!(payload_column.name(), PAYLOAD_COLUMN_NAME);
            assert_eq!(payload_column.data_type(), &expected_data_type);
        }
    }

    #[test]
    fn given_default_layout_when_building_descriptor_should_include_schema_metadata() {
        let config = ResolvedFlussSinkConfig::default();
        let columns = extra_columns(&config);
        let descriptor = StaticTableDescriptorBuilder::new()
            .with_extra_columns(&columns)
            .with_payload_format(config.payload_format)
            .build()
            .expect("Table descriptor should build");

        assert_eq!(
            descriptor.schema().columns(),
            [
                Column::new(MESSAGE_ID_COLUMN_NAME, DataTypes::string())
                    .with_comment("Apache Iggy message ID")
                    .with_id(0),
                Column::new(
                    CHECKSUM_COLUMN_NAME,
                    DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION as u32, 0),
                )
                .with_comment("Apache Iggy message checksum")
                .with_id(1),
                Column::new(
                    MESSAGE_OFFSET_COLUMN_NAME,
                    DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION as u32, 0),
                )
                .with_comment("Apache Iggy message offset")
                .with_id(2),
                Column::new(
                    MESSAGE_TIMESTAMP_COLUMN_NAME,
                    DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
                )
                .with_comment("Apache Iggy message timestamp")
                .with_id(3),
                Column::new(STREAM_COLUMN_NAME, DataTypes::string())
                    .with_comment("Apache Iggy stream name")
                    .with_id(4),
                Column::new(TOPIC_COLUMN_NAME, DataTypes::string())
                    .with_comment("Apache Iggy topic name")
                    .with_id(5),
                Column::new(PARTITION_ID_COLUMN_NAME, DataTypes::bigint())
                    .with_comment("Apache Iggy partition ID")
                    .with_id(6),
                Column::new(
                    ORIGIN_TIMESTAMP_COLUMN_NAME,
                    DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
                )
                .with_comment("Apache Iggy message origin timestamp")
                .with_id(7),
                Column::new(PAYLOAD_COLUMN_NAME, DataTypes::string())
                    .with_comment("Apache Iggy message payload")
                    .with_id(8)
            ]
        );
        assert_eq!(
            descriptor.comment(),
            Some("Stores Apache Iggy messages written by the Fluss sink connector")
        );
        assert!(!descriptor.has_primary_key());
    }

    #[test]
    fn given_primary_key_columns_when_building_schema_should_set_primary_key() {
        let table_config = TableConfig {
            primary_keys: Some(vec![MESSAGE_ID_COLUMN_NAME.to_owned()]),
            ..TableConfig::default()
        };
        let columns = vec![ColumnKind::MessageId];
        let builder = StaticTableDescriptorBuilder::new()
            .with_table_config(&table_config)
            .with_extra_columns(&columns)
            .with_payload_format(PayloadFormat::Text);

        let schema = builder.build_schema().expect("Schema should build");

        assert_eq!(schema.primary_key_column_names(), [MESSAGE_ID_COLUMN_NAME]);
    }

    #[test]
    fn given_table_config_when_building_descriptor_should_apply_table_creation_settings() {
        let table_config = TableConfig {
            primary_keys: Some(vec![
                MESSAGE_ID_COLUMN_NAME.to_owned(),
                STREAM_COLUMN_NAME.to_owned(),
            ]),
            partitioned_by: Some(vec![STREAM_COLUMN_NAME.to_owned()]),
            bucket_keys: Some(vec![MESSAGE_ID_COLUMN_NAME.to_owned()]),
            bucket_count: Some(3),
            properties: Some(HashMap::from([
                ("table.datalake.enabled".to_string(), "true".to_string()),
                ("table.replication.factor".to_string(), "2".to_string()),
            ])),
        };
        let columns = extra_columns(&ResolvedFlussSinkConfig::default());
        let descriptor = StaticTableDescriptorBuilder::new()
            .with_table_config(&table_config)
            .with_extra_columns(&columns)
            .with_payload_format(PayloadFormat::Json)
            .build()
            .expect("Table descriptor should build");

        assert_eq!(
            descriptor.schema().primary_key_column_names(),
            [MESSAGE_ID_COLUMN_NAME, STREAM_COLUMN_NAME]
        );
        assert_eq!(descriptor.partition_keys(), [STREAM_COLUMN_NAME]);
        assert_eq!(descriptor.bucket_keys(), [MESSAGE_ID_COLUMN_NAME]);
        assert_eq!(
            descriptor
                .table_distribution()
                .and_then(|distribution| distribution.bucket_count()),
            Some(3)
        );
        assert_eq!(
            descriptor.properties().get("table.datalake.enabled"),
            Some(&"true".to_string())
        );
        assert_eq!(
            descriptor.properties().get("table.replication.factor"),
            Some(&"2".to_string())
        );
    }

    #[test]
    fn given_json_payload_when_building_arrow_batch_should_serialize_payload_as_string() {
        let config = config_without_optional_columns(PayloadFormat::Json);
        let payload = Schema::Json
            .try_into_payload(br#"{"event":"created"}"#.to_vec())
            .expect("JSON payload should decode");
        let rows = build_arrow_rows(&config, vec![test_message(payload)]);

        let payloads = arrow_column::<StringArray>(rows.columns(), 1);
        assert_eq!(payloads.value(0), r#"{"event":"created"}"#);
    }

    #[test]
    fn given_max_unsigned_values_when_building_arrow_batch_should_preserve_values() {
        let config = test_config(PayloadFormat::Bytea);
        let mut message = test_message(Payload::Raw(vec![1]));
        message.id = u128::MAX;
        message.offset = u64::MAX;
        message.checksum = u64::MAX;
        let rows = build_arrow_rows(&config, vec![message]);

        let ids = arrow_column::<StringArray>(rows.columns(), 0);
        assert_eq!(ids.value(0), "ffffffffffffffffffffffffffffffff");
        let checksums = arrow_column::<Decimal128Array>(rows.columns(), 1);
        assert_eq!(checksums.value(0), i128::from(u64::MAX));
        let offsets = arrow_column::<Decimal128Array>(rows.columns(), 2);
        assert_eq!(offsets.value(0), i128::from(u64::MAX));
    }

    #[test]
    fn given_origin_timestamp_above_i64_max_when_encoding_message_should_return_conversion_error() {
        let mut message = test_message(Payload::Raw(vec![1]));
        message.origin_timestamp = i64::MAX as u64 + 1;

        let error = ConvertedMessage::try_from(message)
            .err()
            .expect("Origin timestamp above the i64 range should fail");

        assert!(matches!(
            error,
            SchemaError::TimestampConversion {
                message_id: 101,
                field: "origin_timestamp",
                value,
                ..
            } if value == i64::MAX as u64 + 1
        ));
    }
}
