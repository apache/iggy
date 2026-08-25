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

use std::mem::size_of;

use fluss::{
    error::Error as FlussError,
    metadata::{Column, DataTypes, Schema, TableDescriptor},
    row::{Datum, Decimal, GenericRow, TimestampLtz},
};
use iggy_connector_sdk::{ConsumedMessage, Error};

use crate::{FlussSinkConfig, PayloadFormat};

const UNSIGNED_64_DECIMAL_PRECISION: u32 = 20;
const TIMESTAMP_PRECISION: u32 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    MessageId,
    Checksum,
    MessageTimestamp,
    OriginTimestamp,
    MessageOffset,
    Stream,
    Topic,
    PartitionId,
    BinaryPayload,
    StringPayload,
}

impl From<ColumnKind> for Column {
    fn from(column: ColumnKind) -> Self {
        match column {
            ColumnKind::MessageId => {
                Column::new("id", DataTypes::string()).with_comment("Apache Iggy message ID")
            }
            ColumnKind::Checksum => Column::new(
                "checksum",
                DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION, 0),
            )
            .with_comment("Apache Iggy message checksum"),
            ColumnKind::MessageTimestamp => Column::new(
                "iggy_timestamp",
                DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
            )
            .with_comment("Apache Iggy message timestamp"),
            ColumnKind::OriginTimestamp => Column::new(
                "iggy_origin_timestamp",
                DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
            )
            .with_comment("Apache Iggy message origin timestamp"),
            ColumnKind::MessageOffset => Column::new(
                "iggy_offset",
                DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION, 0),
            )
            .with_comment("Apache Iggy message offset"),
            ColumnKind::Stream => Column::new("iggy_stream", DataTypes::string())
                .with_comment("Apache Iggy stream name"),
            ColumnKind::Topic => Column::new("iggy_topic", DataTypes::string())
                .with_comment("Apache Iggy topic name"),
            ColumnKind::PartitionId => Column::new("iggy_partition_id", DataTypes::bigint())
                .with_comment("Apache Iggy partition ID"),
            ColumnKind::BinaryPayload => Column::new("payload", DataTypes::bytes())
                .with_comment("Apache Iggy message payload"),
            ColumnKind::StringPayload => Column::new("payload", DataTypes::string())
                .with_comment("Apache Iggy message payload"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RowContext<'a> {
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition_id: u32,
}

impl ColumnKind {
    fn datum<'a>(
        self,
        message: &'a ConsumedMessage,
        context: RowContext<'a>,
    ) -> Result<Datum<'a>, Error> {
        match self {
            Self::MessageId => Ok(format!("{:032x}", message.id).into()),
            Self::Checksum => decimal_from_u64(message.checksum, "checksum").map(Into::into),
            Self::MessageTimestamp => {
                timestamp_from_micros(message.timestamp, "timestamp").map(Into::into)
            }
            Self::OriginTimestamp => {
                timestamp_from_micros(message.origin_timestamp, "origin_timestamp").map(Into::into)
            }
            Self::MessageOffset => decimal_from_u64(message.offset, "offset").map(Into::into),
            Self::Stream => Ok(context.stream.into()),
            Self::Topic => Ok(context.topic.into()),
            Self::PartitionId => Ok(i64::from(context.partition_id).into()),
            Self::BinaryPayload => {
                message
                    .payload
                    .try_to_bytes()
                    .map(Into::into)
                    .map_err(|error| {
                        Error::Serialization(format!(
                            "Failed to serialize payload for Fluss BYTES column: {error}"
                        ))
                    })
            }
            Self::StringPayload => {
                let payload_bytes = message.payload.try_to_bytes().map_err(|error| {
                    Error::Serialization(format!(
                        "Failed to serialize payload for Fluss STRING column: {error}"
                    ))
                })?;
                let payload_string = String::from_utf8(payload_bytes).map_err(|error| {
                    Error::Serialization(format!(
                        "Payload is not valid UTF-8 for the Fluss STRING column: {error}"
                    ))
                })?;
                Ok(payload_string.into())
            }
        }
    }
}

fn decimal_from_u64(value: u64, field: &str) -> Result<Decimal, Error> {
    // Fluss reads signed two's-complement bytes, so the zero prefix preserves the high bit.
    let mut unscaled_bytes = [0_u8; size_of::<u64>() + 1];
    unscaled_bytes[1..].copy_from_slice(&value.to_be_bytes());

    Decimal::from_unscaled_bytes(&unscaled_bytes, UNSIGNED_64_DECIMAL_PRECISION, 0).map_err(
        |error| {
            Error::InvalidRecordValue(format!(
                "Failed to convert Iggy {field} value {value} to Fluss DECIMAL(20, 0): {error}"
            ))
        },
    )
}

fn timestamp_from_micros(value: u64, field: &str) -> Result<TimestampLtz, Error> {
    let epoch_micros = i64::try_from(value).map_err(|_| {
        Error::InvalidRecordValue(format!(
            "Iggy {field} value {value} exceeds the Fluss TIMESTAMP_LTZ(6) range"
        ))
    })?;
    let epoch_millis = epoch_micros / 1_000;
    let nanos_of_millisecond = ((epoch_micros % 1_000) * 1_000) as i32;

    TimestampLtz::from_millis_nanos(epoch_millis, nanos_of_millisecond).map_err(|error| {
        Error::InvalidRecordValue(format!(
            "Failed to convert Iggy {field} value {value} to Fluss TIMESTAMP_LTZ(6): {error}"
        ))
    })
}

#[derive(Debug)]
pub struct FlussTableLayout {
    columns: Vec<ColumnKind>,
    primary_key_columns: Vec<String>,
}

impl FlussTableLayout {
    pub fn from_config(config: &FlussSinkConfig) -> Result<Self, Error> {
        let mut columns: Vec<ColumnKind> = Vec::with_capacity(10);
        columns.push(ColumnKind::MessageId);

        if config.include_checksum {
            columns.push(ColumnKind::Checksum);
        };

        if config.include_metadata {
            columns.extend([
                ColumnKind::MessageOffset,
                ColumnKind::MessageTimestamp,
                ColumnKind::Stream,
                ColumnKind::Topic,
                ColumnKind::PartitionId,
            ]);
        };

        if config.include_origin_timestamp {
            columns.push(ColumnKind::OriginTimestamp);
        }

        match config.payload_format {
            PayloadFormat::Bytea => columns.push(ColumnKind::BinaryPayload),
            PayloadFormat::Json | PayloadFormat::Text => {
                columns.push(ColumnKind::StringPayload);
            }
        }

        Ok(Self {
            columns,
            primary_key_columns: Vec::new(),
        })
    }

    fn build_schema(&self) -> Result<Schema, FlussError> {
        let columns: Vec<Column> = self.columns.iter().copied().map(Into::into).collect();
        let mut schema_builder = Schema::builder().with_columns(columns);

        if !self.primary_key_columns.is_empty() {
            schema_builder = schema_builder.primary_key(self.primary_key_columns.clone());
        }

        schema_builder.build()
    }

    pub fn build_table_descriptor(&self) -> Result<TableDescriptor, FlussError> {
        let schema = self.build_schema()?;

        TableDescriptor::builder()
            .comment("Stores Apache Iggy messages written by the Fluss sink connector")
            .schema(schema)
            .build()
    }

    pub fn row_from_message<'a>(
        &self,
        message: &'a ConsumedMessage,
        context: RowContext<'a>,
    ) -> Result<GenericRow<'a>, Error> {
        let mut values: Vec<Datum> = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            values.push(column.datum(message, context)?);
        }
        Ok(GenericRow::from_data(values))
    }
}

#[cfg(test)]
mod tests {
    use fluss::{
        metadata::{Column, DataTypes},
        row::{Datum, TimestampLtz},
    };
    use iggy_connector_sdk::{ConsumedMessage, Error, Payload, Schema};

    use super::{
        ColumnKind, FlussTableLayout, RowContext, TIMESTAMP_PRECISION,
        UNSIGNED_64_DECIMAL_PRECISION, decimal_from_u64,
    };
    use crate::{FlussSinkConfig, PayloadFormat};

    const MESSAGE_TIMESTAMP: u64 = 1_700_000_000_123_456;
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
            timestamp: MESSAGE_TIMESTAMP,
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

    #[test]
    fn given_default_config_when_building_layout_should_include_all_columns_in_order() {
        let layout = FlussTableLayout::from_config(&FlussSinkConfig::default())
            .expect("Default config should build a table layout");

        assert_eq!(
            layout.columns,
            [
                ColumnKind::MessageId,
                ColumnKind::Checksum,
                ColumnKind::MessageOffset,
                ColumnKind::MessageTimestamp,
                ColumnKind::Stream,
                ColumnKind::Topic,
                ColumnKind::PartitionId,
                ColumnKind::OriginTimestamp,
                ColumnKind::StringPayload,
            ]
        );
    }

    #[test]
    fn given_optional_columns_disabled_when_building_layout_should_only_include_id_and_payload() {
        let config = config_without_optional_columns(PayloadFormat::Bytea);
        let layout = FlussTableLayout::from_config(&config)
            .expect("Config without optional columns should build a table layout");

        assert_eq!(
            layout.columns,
            [ColumnKind::MessageId, ColumnKind::BinaryPayload]
        );
    }

    #[test]
    fn given_payload_formats_when_building_schema_should_use_matching_payload_types() {
        for (payload_format, expected_data_type) in [
            (PayloadFormat::Bytea, DataTypes::bytes()),
            (PayloadFormat::Json, DataTypes::string()),
            (PayloadFormat::Text, DataTypes::string()),
        ] {
            let config = config_without_optional_columns(payload_format);
            let layout = FlussTableLayout::from_config(&config)
                .expect("Payload format should build a table layout");
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
        let layout = FlussTableLayout::from_config(&FlussSinkConfig::default())
            .expect("Default config should build a table layout");
        let descriptor = layout
            .build_table_descriptor()
            .expect("Table descriptor should build");

        assert_eq!(
            descriptor.schema().columns(),
            [
                Column::new("id", DataTypes::string()).with_comment("Apache Iggy message ID"),
                Column::new(
                    "checksum",
                    DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION, 0),
                )
                .with_comment("Apache Iggy message checksum"),
                Column::new(
                    "iggy_offset",
                    DataTypes::decimal(UNSIGNED_64_DECIMAL_PRECISION, 0),
                )
                .with_comment("Apache Iggy message offset"),
                Column::new(
                    "iggy_timestamp",
                    DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
                )
                .with_comment("Apache Iggy message timestamp"),
                Column::new("iggy_stream", DataTypes::string())
                    .with_comment("Apache Iggy stream name"),
                Column::new("iggy_topic", DataTypes::string())
                    .with_comment("Apache Iggy topic name"),
                Column::new("iggy_partition_id", DataTypes::bigint())
                    .with_comment("Apache Iggy partition ID"),
                Column::new(
                    "iggy_origin_timestamp",
                    DataTypes::timestamp_ltz_with_precision(TIMESTAMP_PRECISION),
                )
                .with_comment("Apache Iggy message origin timestamp"),
                Column::new("payload", DataTypes::string())
                    .with_comment("Apache Iggy message payload"),
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
            columns: vec![ColumnKind::MessageId, ColumnKind::BinaryPayload],
            primary_key_columns: vec!["id".to_string()],
        };

        let schema = layout.build_schema().expect("Schema should build");

        assert_eq!(schema.primary_key_column_names(), ["id"]);
    }

    #[test]
    fn given_binary_payload_when_building_row_should_preserve_column_order_and_values() {
        let layout = FlussTableLayout::from_config(&test_config(PayloadFormat::Bytea))
            .expect("Bytea config should build a table layout");
        let message = test_message(Payload::Raw(vec![0, 127, 255]));

        let row = layout
            .row_from_message(&message, test_context())
            .expect("Binary row should build");

        assert_eq!(
            row.values,
            [
                Datum::from("00000000000000000000000000000065".to_string()),
                Datum::from(
                    decimal_from_u64(303, "checksum").expect("Checksum should convert to decimal")
                ),
                Datum::from(
                    decimal_from_u64(202, "offset").expect("Offset should convert to decimal")
                ),
                Datum::from(
                    TimestampLtz::from_millis_nanos(1_700_000_000_123, 456_000)
                        .expect("Timestamp should build")
                ),
                Datum::from("orders"),
                Datum::from("created"),
                Datum::from(7_i64),
                Datum::from(
                    TimestampLtz::from_millis_nanos(1_700_000_000_120, 789_000)
                        .expect("Origin timestamp should build")
                ),
                Datum::from(vec![0, 127, 255]),
            ]
        );
    }

    #[test]
    fn given_text_payload_when_building_row_should_store_payload_as_string() {
        let config = config_without_optional_columns(PayloadFormat::Text);
        let layout = FlussTableLayout::from_config(&config)
            .expect("Text config should build a table layout");
        let message = test_message(Payload::Text("hello Fluss".to_string()));

        let row = layout
            .row_from_message(&message, test_context())
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
        let layout = FlussTableLayout::from_config(&config)
            .expect("JSON config should build a table layout");
        let payload = Schema::Json
            .try_into_payload(br#"{"event":"created"}"#.to_vec())
            .expect("JSON payload should decode");
        let message = test_message(payload);

        let row = layout
            .row_from_message(&message, test_context())
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
        let layout = FlussTableLayout::from_config(&test_config(PayloadFormat::Bytea))
            .expect("Bytea config should build a table layout");
        let mut message = test_message(Payload::Raw(vec![1]));
        message.id = u128::MAX;
        message.offset = u64::MAX;
        message.checksum = u64::MAX;

        let row = layout
            .row_from_message(&message, test_context())
            .expect("Unsigned values should build");

        assert_eq!(row.values[0].as_str(), "ffffffffffffffffffffffffffffffff");
        assert_eq!(row.values[1].as_decimal().to_string(), u64::MAX.to_string());
        assert_eq!(row.values[2].as_decimal().to_string(), u64::MAX.to_string());
    }

    #[test]
    fn given_timestamp_above_fluss_range_when_building_row_should_return_invalid_record_value() {
        let layout = FlussTableLayout::from_config(&test_config(PayloadFormat::Bytea))
            .expect("Bytea config should build a table layout");
        let mut message = test_message(Payload::Raw(vec![1]));
        message.timestamp = i64::MAX as u64 + 1;

        let error = layout
            .row_from_message(&message, test_context())
            .expect_err("Out-of-range timestamp should fail");

        assert!(matches!(
            error,
            Error::InvalidRecordValue(message)
                if message.contains("timestamp") && message.contains("TIMESTAMP_LTZ(6) range")
        ));
    }

    #[test]
    fn given_invalid_utf8_when_building_string_row_should_return_serialization_error() {
        let config = config_without_optional_columns(PayloadFormat::Text);
        let layout = FlussTableLayout::from_config(&config)
            .expect("Text config should build a table layout");
        let message = test_message(Payload::Raw(vec![0xFF]));

        let error = layout
            .row_from_message(&message, test_context())
            .expect_err("Invalid UTF-8 payload should fail");

        assert!(matches!(
            error,
            Error::Serialization(message)
                if message.contains("not valid UTF-8 for the Fluss STRING column")
        ));
    }
}
