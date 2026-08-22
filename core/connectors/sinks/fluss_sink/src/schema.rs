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

use std::{string::FromUtf8Error, sync::Arc};

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, Decimal128Builder, Int64Array, StringArray,
    StringBuilder, StructArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field};
use fluss::{
    error::Error as FlussError,
    metadata::{Column, TableDescriptor},
    row::{Datum, GenericRow},
};
use iggy_connector_sdk::{ConsumedMessage, Error as IggyError};
use thiserror::Error;

use crate::{FlussSinkConfig, PayloadFormat};

const UNSIGNED_64_DECIMAL_PRECISION: u8 = 20;
const TIMESTAMP_PRECISION: u32 = 6;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Fluss(Box<FlussError>),
    #[error(
        "Failed to convert Iggy message ID {message_id} field '{field}' with value {value} to Fluss DECIMAL(20, 0): {source}"
    )]
    DecimalConversion {
        message_id: u128,
        field: &'static str,
        value: i128,
        #[source]
        source: Box<FlussError>,
    },
    #[error(
        "Failed to convert Iggy message ID {message_id} field '{field}' with value {value} to Fluss TIMESTAMP_LTZ(6): {reason}"
    )]
    TimestampConversion {
        message_id: u128,
        field: &'static str,
        value: i128,
        reason: String,
    },
    #[error(
        "Payload from Iggy message ID {message_id} is not valid UTF-8 for a Fluss STRING column: {source}"
    )]
    InvalidPayloadUtf8 {
        message_id: u128,
        #[source]
        source: FromUtf8Error,
    },
    #[error("Failed to convert payload from Iggy message ID {message_id} to bytes: {source}")]
    PayloadBytesConversion {
        message_id: u128,
        #[source]
        source: IggyError,
    },
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
            Error::InvalidPayloadUtf8 { .. } => Self::Serialization(message),
            Error::DecimalConversion { .. }
            | Error::TimestampConversion { .. }
            | Error::PayloadBytesConversion { .. } => Self::InvalidRecordValue(message),
        }
    }
}

enum EncodedPayload {
    String(String),
    Binary(Vec<u8>),
}

pub struct EncodedMessage {
    id: String,
    offset: i128,
    checksum: i128,
    timestamp: i64,
    origin_timestamp: i64,
    payload: EncodedPayload,
}

impl EncodedMessage {
    fn try_into(message: ConsumedMessage, payload_format: PayloadFormat) -> Result<Self, Error> {
        let message_id = message.id;
        Ok(Self {
            id: string_from_id(message.id),
            offset: i128::from(message.offset),
            checksum: i128::from(message.checksum),
            timestamp: i64::try_from(message.timestamp).map_err(|error| {
                Error::TimestampConversion {
                    message_id,
                    field: "timestamp",
                    value: i128::from(message.timestamp),
                    reason: error.to_string(),
                }
            })?,
            origin_timestamp: i64::try_from(message.origin_timestamp).map_err(|error| {
                Error::TimestampConversion {
                    message_id,
                    field: "origin_timestamp",
                    value: i128::from(message.origin_timestamp),
                    reason: error.to_string(),
                }
            })?,
            payload: match payload_format {
                PayloadFormat::Bytea => {
                    let payload = message
                        .payload
                        .try_into_vec()
                        .map_err(|source| Error::PayloadBytesConversion { message_id, source })?;
                    EncodedPayload::Binary(payload)
                }
                PayloadFormat::Json | PayloadFormat::Text => {
                    let payload_binary = message
                        .payload
                        .try_into_vec()
                        .map_err(|source| Error::PayloadBytesConversion { message_id, source })?;
                    let payload_string = String::from_utf8(payload_binary)
                        .map_err(|source| Error::InvalidPayloadUtf8 { message_id, source })?;
                    EncodedPayload::String(payload_string)
                }
            },
        })
    }
    pub fn encode_all(
        messages: impl IntoIterator<Item = ConsumedMessage>,
        payload_format: PayloadFormat,
    ) -> impl Iterator<Item = Result<Self, Error>> {
        messages
            .into_iter()
            .map(move |m| Self::try_into(m, payload_format))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    MessageId(&'static str),
    Checksum(&'static str),
    MessageTimestamp(&'static str),
    OriginTimestamp(&'static str),
    MessageOffset(&'static str),
    Stream(&'static str),
    Topic(&'static str),
    PartitionId(&'static str),
}

impl From<ColumnKind> for Column {
    fn from(column: ColumnKind) -> Self {
        match column {
            ColumnKind::MessageId(name) => Column::new(name, fluss::metadata::DataTypes::string())
                .with_comment("Apache Iggy message ID"),
            ColumnKind::Checksum(name) => Column::new(
                name,
                fluss::metadata::DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION as u32, 0),
            )
            .with_comment("Apache Iggy message checksum"),
            ColumnKind::MessageTimestamp(name) => Column::new(
                name,
                fluss::metadata::DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
            )
            .with_comment("Apache Iggy message timestamp"),
            ColumnKind::OriginTimestamp(name) => Column::new(
                name,
                fluss::metadata::DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
            )
            .with_comment("Apache Iggy message origin timestamp"),
            ColumnKind::MessageOffset(name) => Column::new(
                name,
                fluss::metadata::DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION as u32, 0),
            )
            .with_comment("Apache Iggy message offset"),
            ColumnKind::Stream(name) => Column::new(name, fluss::metadata::DataTypes::string())
                .with_comment("Apache Iggy stream name"),
            ColumnKind::Topic(name) => Column::new(name, fluss::metadata::DataTypes::string())
                .with_comment("Apache Iggy topic name"),
            ColumnKind::PartitionId(name) => {
                Column::new(name, fluss::metadata::DataTypes::bigint())
                    .with_comment("Apache Iggy partition ID")
            }
        }
    }
}

#[derive(Debug)]
pub struct RowContext {
    pub stream: String,
    pub topic: String,
    pub partition_id: u32,
}

fn string_from_id(id: u128) -> String {
    format!("{:032x}", id)
}

fn timestamp_from_micros(epoch_micros: i64) -> Result<fluss::row::TimestampLtz, FlussError> {
    let epoch_millis = epoch_micros / 1_000;
    let nanos_of_millisecond = ((epoch_micros % 1_000) * 1_000) as i32;
    fluss::row::TimestampLtz::from_millis_nanos(epoch_millis, nanos_of_millisecond)
}

pub struct ArrowRowBuilder {
    id_builder: StringBuilder,
    checksum_builder: Decimal128Builder,
    message_offset_builder: Decimal128Builder,
    message_timestamp_builder: TimestampMicrosecondBuilder,
    origin_timestamp_builder: TimestampMicrosecondBuilder,
    payload_string_builder: StringBuilder,
    payload_binary_builder: BinaryBuilder,
    payload_format: PayloadFormat,
    extra_columns: Vec<ColumnKind>,
    context: RowContext,
}

impl ArrowRowBuilder {
    fn new(
        payload_format: PayloadFormat,
        extra_columns: Vec<ColumnKind>,
        context: RowContext,
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
            extra_columns,
            context,
        }
    }

    pub(crate) fn append(&mut self, message: EncodedMessage) {
        let EncodedMessage {
            id,
            offset,
            checksum,
            timestamp,
            origin_timestamp,
            payload,
        } = message;

        for column in &self.extra_columns {
            match column {
                ColumnKind::MessageId(_) => self.id_builder.append_value(&id),
                ColumnKind::Checksum(_) => self.checksum_builder.append_value(checksum),
                ColumnKind::MessageOffset(_) => self.message_offset_builder.append_value(offset),
                ColumnKind::MessageTimestamp(_) => {
                    self.message_timestamp_builder.append_value(timestamp)
                }
                ColumnKind::OriginTimestamp(_) => {
                    self.origin_timestamp_builder.append_value(origin_timestamp)
                }
                ColumnKind::Stream(_) | ColumnKind::Topic(_) | ColumnKind::PartitionId(_) => {}
            }
        }

        match payload {
            EncodedPayload::Binary(payload) => self.payload_binary_builder.append_value(payload),
            EncodedPayload::String(payload) => self.payload_string_builder.append_value(payload),
        }
    }

    pub(crate) fn finish(&mut self) -> StructArray {
        let mut cols: Vec<(Arc<Field>, ArrayRef)> =
            Vec::with_capacity(self.extra_columns.len() + 1);
        let len = match self.payload_format {
            PayloadFormat::Bytea => self.payload_binary_builder.len(),
            PayloadFormat::Json | PayloadFormat::Text => self.payload_string_builder.len(),
        };

        for column in &self.extra_columns {
            let (name, array): (&str, ArrayRef) = match column {
                ColumnKind::MessageId(name) => (name, Arc::new(self.id_builder.finish())),
                ColumnKind::Checksum(name) => (name, Arc::new(self.checksum_builder.finish())),
                ColumnKind::MessageOffset(name) => {
                    (name, Arc::new(self.message_offset_builder.finish()))
                }
                ColumnKind::MessageTimestamp(name) => {
                    (name, Arc::new(self.message_timestamp_builder.finish()))
                }
                ColumnKind::OriginTimestamp(name) => {
                    (name, Arc::new(self.origin_timestamp_builder.finish()))
                }
                ColumnKind::Stream(name) => (
                    name,
                    Arc::new(StringArray::new_repeated(self.context.stream.as_str(), len)),
                ),
                ColumnKind::Topic(name) => (
                    name,
                    Arc::new(StringArray::new_repeated(self.context.topic.as_str(), len)),
                ),
                ColumnKind::PartitionId(name) => (
                    name,
                    Arc::new(Int64Array::from_value(
                        i64::from(self.context.partition_id),
                        len,
                    )),
                ),
            };
            let field = Arc::new(Field::new(name, array.data_type().clone(), false));
            cols.push((field, array));
        }

        let payload: ArrayRef = match self.payload_format {
            PayloadFormat::Bytea => Arc::new(self.payload_binary_builder.finish()),
            PayloadFormat::Json | PayloadFormat::Text => {
                Arc::new(self.payload_string_builder.finish())
            }
        };
        let payload_field = Arc::new(Field::new("payload", payload.data_type().clone(), true));
        cols.push((payload_field, payload));

        StructArray::from(cols)
    }
}

#[derive(Debug)]
pub struct FlussTableLayout {
    extra_columns: Vec<ColumnKind>,
    payload_format: PayloadFormat,
    primary_key_columns: Vec<String>,
}

impl FlussTableLayout {
    pub fn from_config(config: &FlussSinkConfig) -> Self {
        let mut columns: Vec<ColumnKind> = Vec::with_capacity(10);
        columns.push(ColumnKind::MessageId("id"));

        if config.include_checksum {
            columns.push(ColumnKind::Checksum("checksum"));
        };

        if config.include_metadata {
            columns.extend([
                ColumnKind::MessageOffset("iggy_offset"),
                ColumnKind::MessageTimestamp("iggy_timestamp"),
                ColumnKind::Stream("iggy_stream"),
                ColumnKind::Topic("iggy_topic"),
                ColumnKind::PartitionId("iggy_partition_id"),
            ]);
        };

        if config.include_origin_timestamp {
            columns.push(ColumnKind::OriginTimestamp("iggy_origin_timestamp"));
        }

        Self {
            extra_columns: columns,
            payload_format: config.payload_format,
            primary_key_columns: Vec::new(),
        }
    }

    fn build_schema(&self) -> Result<fluss::metadata::Schema, Error> {
        let payload_column = match self.payload_format {
            PayloadFormat::Bytea => Column::new("payload", fluss::metadata::DataTypes::bytes())
                .with_comment("Apache Iggy message payload"),

            PayloadFormat::Json | PayloadFormat::Text => {
                Column::new("payload", fluss::metadata::DataTypes::string())
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

        let mut schema_builder = fluss::metadata::Schema::builder().with_columns(columns);
        if !self.primary_key_columns.is_empty() {
            schema_builder = schema_builder.primary_key(self.primary_key_columns.clone());
        }
        Ok(schema_builder.build()?)
    }

    pub(crate) fn build_table_descriptor(&self) -> Result<TableDescriptor, Error> {
        let schema = self.build_schema()?;
        Ok(TableDescriptor::builder()
            .comment("Stores Apache Iggy messages written by the Fluss sink connector")
            .schema(schema)
            .build()?)
    }

    pub(crate) fn convert_to_generic_row<'a>(
        &self,
        message: ConsumedMessage,
        context: &'a RowContext,
    ) -> Result<GenericRow<'a>, Error> {
        let message_id = message.id;
        let mut values: Vec<fluss::row::Datum> = Vec::with_capacity(self.extra_columns.len());
        let encoded_message = EncodedMessage::try_into(message, self.payload_format)?;
        for column in &self.extra_columns {
            let value: Datum = match column {
                ColumnKind::MessageId(_) => encoded_message.id.clone().into(),
                ColumnKind::Checksum(_) => fluss::row::Decimal::from_arrow_decimal128(
                    encoded_message.checksum,
                    0,
                    UNSIGNED_64_DECIMAL_PRECISION as u32,
                    0,
                )
                .map_err(|source| Error::DecimalConversion {
                    message_id,
                    field: "checksum",
                    value: encoded_message.checksum,
                    source: Box::new(source),
                })
                .map(Into::into)?,
                ColumnKind::MessageTimestamp(_) => timestamp_from_micros(encoded_message.timestamp)
                    .map_err(|source| Error::TimestampConversion {
                        message_id,
                        field: "timestamp",
                        value: i128::from(encoded_message.timestamp),
                        reason: source.to_string(),
                    })
                    .map(Into::into)?,

                ColumnKind::OriginTimestamp(_) => {
                    timestamp_from_micros(encoded_message.origin_timestamp)
                        .map_err(|source| Error::TimestampConversion {
                            message_id,
                            field: "origin_timestamp",
                            value: i128::from(encoded_message.origin_timestamp),
                            reason: source.to_string(),
                        })
                        .map(Into::into)?
                }
                ColumnKind::MessageOffset(_) => fluss::row::Decimal::from_arrow_decimal128(
                    encoded_message.offset,
                    0,
                    UNSIGNED_64_DECIMAL_PRECISION as u32,
                    0,
                )
                .map_err(|source| Error::DecimalConversion {
                    message_id,
                    field: "offset",
                    value: encoded_message.offset,
                    source: Box::new(source),
                })
                .map(Into::into)?,
                ColumnKind::Stream(_) => context.stream.as_str().into(),
                ColumnKind::Topic(_) => context.topic.as_str().into(),
                ColumnKind::PartitionId(_) => i64::from(context.partition_id).into(),
            };
            values.push(value);
        }

        let payload = match encoded_message.payload {
            EncodedPayload::String(s) => Datum::from(s),
            EncodedPayload::Binary(b) => Datum::from(b),
        };

        values.push(payload);

        Ok(GenericRow::from_data(values))
    }

    pub(crate) fn create_arrow_builder(&self, context: RowContext) -> ArrowRowBuilder {
        ArrowRowBuilder::new(self.payload_format, self.extra_columns.clone(), context)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, Int64Array, StringArray, StructArray,
        TimestampMicrosecondArray,
    };
    use fluss::{
        metadata::{Column, DataTypes},
        row::Datum,
    };
    use iggy_connector_sdk::{ConsumedMessage, Error, Payload, Schema};

    use super::{
        ColumnKind, EncodedMessage, Error as SchemaError, FlussTableLayout, RowContext,
        TIMESTAMP_PRECISION, UNSIGNED_64_DECIMAL_PRECISION, timestamp_from_micros,
    };
    use crate::{FlussSinkConfig, PayloadFormat};

    const MESSAGE_TIMESTAMP: i64 = 1_700_000_000_123_456;
    const ORIGIN_TIMESTAMP: u64 = 1_700_000_000_120_789;

    fn test_config(payload_format: PayloadFormat) -> FlussSinkConfig {
        FlussSinkConfig {
            payload_format,
            ..FlussSinkConfig::default()
        }
    }

    fn config_without_optional_columns(payload_format: PayloadFormat) -> FlussSinkConfig {
        FlussSinkConfig {
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

    fn test_context() -> RowContext {
        RowContext {
            stream: "orders".to_string(),
            topic: "created".to_string(),
            partition_id: 7,
        }
    }

    fn arrow_column<T: Array + 'static>(columns: &[ArrayRef], index: usize) -> &T {
        columns[index]
            .as_any()
            .downcast_ref::<T>()
            .expect("Arrow column should have the expected type")
    }

    fn build_arrow_rows(layout: &FlussTableLayout, messages: Vec<ConsumedMessage>) -> StructArray {
        let mut builder = layout.create_arrow_builder(test_context());
        for message in EncodedMessage::encode_all(messages, layout.payload_format) {
            let message = message.expect("Message should encode");
            builder.append(message);
        }
        builder.finish()
    }

    #[test]
    fn given_microsecond_values_when_converting_to_timestamp_should_preserve_precision() {
        let cases = [
            (0, 0, 0),
            (999, 0, 999_000),
            (1_000, 1, 0),
            (1_001, 1, 1_000),
            (MESSAGE_TIMESTAMP, 1_700_000_000_123, 456_000),
            (i64::MAX, 9_223_372_036_854_775, 807_000),
        ];

        for (value, expected_millis, expected_nanos) in cases {
            let timestamp = timestamp_from_micros(value)
                .expect("microsecond value within the i64 range should convert");

            assert_eq!(timestamp.get_epoch_millisecond(), expected_millis);
            assert_eq!(timestamp.get_nano_of_millisecond(), expected_nanos);
        }
    }

    #[test]
    fn given_i64_max_timestamps_when_encoding_message_should_preserve_values() {
        let mut message = test_message(Payload::Raw(vec![1]));
        message.timestamp = i64::MAX as u64;
        message.origin_timestamp = i64::MAX as u64;

        let encoded_message = EncodedMessage::try_into(message, PayloadFormat::Json)
            .expect("Timestamps within the i64 range should encode");

        assert_eq!(encoded_message.timestamp, i64::MAX);
        assert_eq!(encoded_message.origin_timestamp, i64::MAX);
    }

    #[test]
    fn given_all_extra_columns_when_converting_messages_should_preserve_array_contents() {
        let layout = FlussTableLayout::from_config(&test_config(PayloadFormat::Text));
        let first_message = test_message(Payload::Text("first".to_string()));
        let mut second_message = test_message(Payload::Text("second".to_string()));
        second_message.id = 102;
        second_message.checksum = 304;
        second_message.offset = 203;
        second_message.timestamp = MESSAGE_TIMESTAMP as u64 + 1;
        second_message.origin_timestamp = ORIGIN_TIMESTAMP + 1;

        let rows = build_arrow_rows(&layout, vec![first_message, second_message]);
        let columns = rows.columns();

        assert_eq!(columns.len(), layout.extra_columns.len() + 1);

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
        let layout = FlussTableLayout::from_config(&config);
        let first_message = test_message(Payload::Raw(vec![0, 127, 255]));
        let mut second_message = test_message(Payload::Raw(vec![1, 2, 3]));
        second_message.id = 102;

        let rows = build_arrow_rows(&layout, vec![first_message, second_message]);
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
        let layout = FlussTableLayout::from_config(&config);
        let mut builder = layout.create_arrow_builder(test_context());
        let mut valid_message = test_message(Payload::Text("valid".to_string()));
        valid_message.id = 102;
        let messages = [test_message(Payload::Raw(vec![0xff])), valid_message];
        let mut errors = 0;
        for message in EncodedMessage::encode_all(messages, layout.payload_format) {
            match message {
                Ok(message) => builder.append(message),
                Err(error) => {
                    assert!(matches!(
                        error,
                        SchemaError::InvalidPayloadUtf8 {
                            message_id: 101,
                            ..
                        }
                    ));
                    errors += 1;
                }
            }
        }

        let rows = builder.finish();
        assert_eq!(errors, 1);
        assert_eq!(rows.len(), 1);
        let ids = arrow_column::<StringArray>(rows.columns(), 0);
        assert_eq!(ids.value(0), "00000000000000000000000000000066");
        let payloads = arrow_column::<StringArray>(rows.columns(), 1);
        assert_eq!(payloads.value(0), "valid");
    }

    #[test]
    fn given_timestamp_above_i64_max_when_encoding_message_should_return_conversion_error() {
        let mut message = test_message(Payload::Text("payload".to_string()));
        message.timestamp = i64::MAX as u64 + 1;

        let error = EncodedMessage::try_into(message, PayloadFormat::Text)
            .err()
            .expect("Timestamp above the i64 range should fail");

        assert!(matches!(
            error,
            SchemaError::TimestampConversion {
                message_id: 101,
                field: "timestamp",
                value,
                ..
            } if value == i128::from(i64::MAX) + 1
        ));
    }

    #[test]
    fn given_default_config_when_building_layout_should_include_all_columns_in_order() {
        let layout = FlussTableLayout::from_config(&FlussSinkConfig::default());

        assert_eq!(
            layout.extra_columns,
            [
                ColumnKind::MessageId("id"),
                ColumnKind::Checksum("checksum"),
                ColumnKind::MessageOffset("iggy_offset"),
                ColumnKind::MessageTimestamp("iggy_timestamp"),
                ColumnKind::Stream("iggy_stream"),
                ColumnKind::Topic("iggy_topic"),
                ColumnKind::PartitionId("iggy_partition_id"),
                ColumnKind::OriginTimestamp("iggy_origin_timestamp"),
            ]
        );
    }

    #[test]
    fn given_optional_columns_disabled_when_building_layout_should_only_include_id_and_payload() {
        let config = config_without_optional_columns(PayloadFormat::Bytea);
        let layout = FlussTableLayout::from_config(&config);

        assert_eq!(layout.extra_columns, [ColumnKind::MessageId("id")]);
    }

    #[test]
    fn given_payload_formats_when_building_schema_should_use_matching_payload_types() {
        for (payload_format, expected_data_type) in [
            (PayloadFormat::Bytea, DataTypes::bytes()),
            (PayloadFormat::Json, DataTypes::string()),
            (PayloadFormat::Text, DataTypes::string()),
        ] {
            let config = config_without_optional_columns(payload_format);
            let layout = FlussTableLayout::from_config(&config);
            let schema = layout.build_schema().expect("Schema should build");
            let payload_column = schema
                .columns()
                .last()
                .expect("Schema should contain a payload column");

            assert_eq!(payload_column.name(), "payload");
            assert_eq!(payload_column.data_type(), &expected_data_type);
        }
    }

    #[test]
    fn given_default_layout_when_building_descriptor_should_include_schema_metadata() {
        let layout = FlussTableLayout::from_config(&FlussSinkConfig::default());
        let descriptor = layout
            .build_table_descriptor()
            .expect("Table descriptor should build");

        assert_eq!(
            descriptor.schema().columns(),
            [
                Column::new("id", DataTypes::string())
                    .with_comment("Apache Iggy message ID")
                    .with_id(0),
                Column::new(
                    "checksum",
                    DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION as u32, 0),
                )
                .with_comment("Apache Iggy message checksum")
                .with_id(1),
                Column::new(
                    "iggy_offset",
                    DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION as u32, 0),
                )
                .with_comment("Apache Iggy message offset")
                .with_id(2),
                Column::new(
                    "iggy_timestamp",
                    DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
                )
                .with_comment("Apache Iggy message timestamp")
                .with_id(3),
                Column::new("iggy_stream", DataTypes::string())
                    .with_comment("Apache Iggy stream name")
                    .with_id(4),
                Column::new("iggy_topic", DataTypes::string())
                    .with_comment("Apache Iggy topic name")
                    .with_id(5),
                Column::new("iggy_partition_id", DataTypes::bigint())
                    .with_comment("Apache Iggy partition ID")
                    .with_id(6),
                Column::new(
                    "iggy_origin_timestamp",
                    DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
                )
                .with_comment("Apache Iggy message origin timestamp")
                .with_id(7),
                Column::new("payload", DataTypes::string())
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
        let layout = FlussTableLayout {
            extra_columns: vec![ColumnKind::MessageId("id")],
            payload_format: PayloadFormat::Text,
            primary_key_columns: vec!["id".to_string()],
        };

        let schema = layout.build_schema().expect("Schema should build");

        assert_eq!(schema.primary_key_column_names(), ["id"]);
    }

    #[test]
    fn given_text_payload_when_building_row_should_store_payload_as_string() {
        let config = config_without_optional_columns(PayloadFormat::Text);
        let layout = FlussTableLayout::from_config(&config);
        let message = test_message(Payload::Text("hello Fluss".to_string()));

        let context = test_context();
        let row = layout
            .convert_to_generic_row(message, &context)
            .expect("Text row should build");

        assert_eq!(
            row.values,
            [
                Datum::from("00000000000000000000000000000065".to_string()),
                Datum::from("hello Fluss".to_string()),
            ]
        );
    }

    #[test]
    fn given_json_payload_when_building_row_should_serialize_payload_as_string() {
        let config = config_without_optional_columns(PayloadFormat::Json);
        let layout = FlussTableLayout::from_config(&config);
        let payload = Schema::Json
            .try_into_payload(br#"{"event":"created"}"#.to_vec())
            .expect("JSON payload should decode");
        let message = test_message(payload);
        let context = test_context();
        let row = layout
            .convert_to_generic_row(message, &context)
            .expect("JSON row should build");

        assert_eq!(
            row.values,
            [
                Datum::from("00000000000000000000000000000065".to_string()),
                Datum::from(r#"{"event":"created"}"#.to_string()),
            ]
        );
    }

    #[test]
    fn given_max_unsigned_values_when_building_row_should_preserve_id_offset_and_checksum() {
        let layout = FlussTableLayout::from_config(&test_config(PayloadFormat::Bytea));
        let mut message = test_message(Payload::Raw(vec![1]));
        let context = test_context();
        message.id = u128::MAX;
        message.offset = u64::MAX;
        message.checksum = u64::MAX;

        let row = layout
            .convert_to_generic_row(message, &context)
            .expect("Unsigned values should build");

        assert_eq!(row.values[0].as_str(), "ffffffffffffffffffffffffffffffff");
        assert_eq!(row.values[1].as_decimal().to_string(), u64::MAX.to_string());
        assert_eq!(row.values[2].as_decimal().to_string(), u64::MAX.to_string());
    }

    #[test]
    fn given_origin_timestamp_above_i64_max_when_encoding_message_should_return_conversion_error() {
        let mut message = test_message(Payload::Raw(vec![1]));
        message.origin_timestamp = i64::MAX as u64 + 1;

        let error = EncodedMessage::try_into(message, PayloadFormat::Bytea)
            .err()
            .expect("Origin timestamp above the i64 range should fail");

        assert!(matches!(
            error,
            SchemaError::TimestampConversion {
                message_id: 101,
                field: "origin_timestamp",
                value,
                ..
            } if value == i128::from(i64::MAX) + 1
        ));
    }

    #[test]
    fn given_invalid_utf8_when_building_string_row_should_return_serialization_error() {
        let config = config_without_optional_columns(PayloadFormat::Text);
        let layout = FlussTableLayout::from_config(&config);
        let message = test_message(Payload::Raw(vec![0xFF]));

        let error = layout
            .convert_to_generic_row(message, &test_context())
            .expect_err("Invalid UTF-8 payload should fail");

        assert!(matches!(
            &error,
            SchemaError::InvalidPayloadUtf8 {
                message_id: 101,
                ..
            }
        ));

        let error: Error = error.into();

        assert!(matches!(error, Error::Serialization(_)));
    }
}
