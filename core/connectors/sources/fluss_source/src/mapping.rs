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

use base64::display::Base64Display;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use fluss::metadata::{DataField, DataType};
use fluss::row::InternalRow;
use iggy_connector_sdk::Error;
use serde::ser::{Error as _, SerializeMap};
use serde::{Serialize, Serializer};

const MILLIS_PER_SECOND: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;

/// One row written as a JSON object straight into the payload, with the `metadata` entries
/// after the columns. Names and values are written from the schema and the Arrow batch as
/// they are read, without building a `serde_json::Value` or copying a column name per row.
///
/// Temporal values are formatted with their full fractional precision, the way the
/// PostgreSQL source formats them. A number of milliseconds would drop the microseconds of
/// the default `TIMESTAMP(6)`. `TIMESTAMP` carries no timezone and is written without one,
/// while `TIMESTAMP_LTZ` is an instant and is written in UTC.
pub(crate) struct JsonRow<'a> {
    row: &'a dyn InternalRow,
    fields: &'a [DataField],
    metadata: &'a [(&'a str, i64)],
}

impl<'a> JsonRow<'a> {
    pub(crate) fn new(
        row: &'a dyn InternalRow,
        fields: &'a [DataField],
        metadata: &'a [(&'a str, i64)],
    ) -> Self {
        Self {
            row,
            fields,
            metadata,
        }
    }
}

impl Serialize for JsonRow<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut object = serializer.serialize_map(Some(self.fields.len() + self.metadata.len()))?;
        for (position, field) in self.fields.iter().enumerate() {
            let value = JsonField {
                row: self.row,
                position,
                field,
            };
            object.serialize_entry(field.name(), &value)?;
        }
        for (name, value) in self.metadata {
            object.serialize_entry(name, value)?;
        }
        object.end()
    }
}

/// Rejects column types with no JSON representation before the first poll, so a table with
/// an unsupported column fails at startup instead of once per batch.
pub(crate) fn ensure_supported_types(fields: &[DataField]) -> Result<(), Error> {
    for field in fields {
        if !is_supported(field.data_type()) {
            return Err(Error::SchemaMismatch(format!(
                "column '{}' has type {:?}, which the Apache Fluss source cannot map to JSON",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

fn is_supported(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Array(_) | DataType::Map(_) | DataType::Row(_)
    )
}

struct JsonField<'a> {
    row: &'a dyn InternalRow,
    position: usize,
    field: &'a DataField,
}

impl Serialize for JsonField<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let row = self.row;
        let position = self.position;
        let name = self.field.name();
        let data_type = self.field.data_type();
        let read_error = |error: fluss::error::Error| {
            S::Error::custom(format_args!("failed to read column '{name}': {error}"))
        };
        let out_of_range = || {
            S::Error::custom(format_args!(
                "column '{name}' holds a {data_type:?} value outside the range that can be \
                 formatted"
            ))
        };

        if row.is_null_at(position).map_err(read_error)? {
            return serializer.serialize_none();
        }

        match data_type {
            DataType::Boolean(_) => {
                serializer.serialize_bool(row.get_boolean(position).map_err(read_error)?)
            }
            DataType::TinyInt(_) => {
                serializer.serialize_i8(row.get_byte(position).map_err(read_error)?)
            }
            DataType::SmallInt(_) => {
                serializer.serialize_i16(row.get_short(position).map_err(read_error)?)
            }
            DataType::Int(_) => {
                serializer.serialize_i32(row.get_int(position).map_err(read_error)?)
            }
            DataType::BigInt(_) => {
                serializer.serialize_i64(row.get_long(position).map_err(read_error)?)
            }
            DataType::Float(_) => serialize_float(
                serializer,
                f64::from(row.get_float(position).map_err(read_error)?),
            ),
            DataType::Double(_) => {
                serialize_float(serializer, row.get_double(position).map_err(read_error)?)
            }
            DataType::Char(inner) => serializer.serialize_str(
                row.get_char(position, inner.length() as usize)
                    .map_err(read_error)?,
            ),
            DataType::String(_) => {
                serializer.serialize_str(row.get_string(position).map_err(read_error)?)
            }
            DataType::Decimal(inner) => {
                let decimal = row
                    .get_decimal(position, inner.precision() as usize, inner.scale() as usize)
                    .map_err(read_error)?;
                serializer.collect_str(&decimal.to_big_decimal())
            }
            DataType::Date(_) => {
                let days = row.get_date(position).map_err(read_error)?.get_inner();
                serializer.collect_str(&NaiveDate::from_epoch_days(days).ok_or_else(out_of_range)?)
            }
            DataType::Time(_) => {
                let millis = row.get_time(position).map_err(read_error)?.get_inner();
                serializer.collect_str(&time_of_day(millis).ok_or_else(out_of_range)?)
            }
            DataType::Timestamp(inner) => {
                let timestamp = row
                    .get_timestamp_ntz(position, inner.precision())
                    .map_err(read_error)?;
                let instant = instant(
                    timestamp.get_millisecond(),
                    timestamp.get_nano_of_millisecond(),
                )
                .ok_or_else(out_of_range)?;
                serializer.collect_str(&instant.naive_utc())
            }
            DataType::TimestampLTz(inner) => {
                let timestamp = row
                    .get_timestamp_ltz(position, inner.precision())
                    .map_err(read_error)?;
                let instant = instant(
                    timestamp.get_epoch_millisecond(),
                    timestamp.get_nano_of_millisecond(),
                )
                .ok_or_else(out_of_range)?;
                serializer.serialize_str(&instant.to_rfc3339())
            }
            DataType::Bytes(_) => serializer.collect_str(&Base64Display::new(
                row.get_bytes(position).map_err(read_error)?,
                &BASE64,
            )),
            DataType::Binary(inner) => serializer.collect_str(&Base64Display::new(
                row.get_binary(position, inner.length())
                    .map_err(read_error)?,
                &BASE64,
            )),
            DataType::Array(_) | DataType::Map(_) | DataType::Row(_) => {
                Err(S::Error::custom(format_args!(
                    "column '{name}' has nested type {data_type:?}, which the Apache Fluss \
                     source does not support"
                )))
            }
        }
    }
}

/// JSON has no encoding for NaN or infinity, so those collapse to null rather than
/// failing the whole batch over one degenerate float.
fn serialize_float<S: Serializer>(serializer: S, value: f64) -> Result<S::Ok, S::Error> {
    if value.is_finite() {
        serializer.serialize_f64(value)
    } else {
        serializer.serialize_none()
    }
}

/// Fluss keeps a timestamp as epoch milliseconds plus the nanoseconds within that
/// millisecond. Euclidean division keeps a pre-epoch value's fraction positive, so -1 ms is
/// 23:59:59.999 of the previous day rather than an invalid negative fraction.
fn instant(epoch_millis: i64, nano_of_millisecond: i32) -> Option<DateTime<Utc>> {
    let seconds = epoch_millis.div_euclid(MILLIS_PER_SECOND);
    let nanos = epoch_millis.rem_euclid(MILLIS_PER_SECOND) * NANOS_PER_MILLI
        + i64::from(nano_of_millisecond);
    DateTime::from_timestamp(seconds, u32::try_from(nanos).ok()?)
}

/// Fluss keeps `TIME` as milliseconds since midnight.
fn time_of_day(millis: i32) -> Option<NaiveTime> {
    let millis = i64::from(millis);
    NaiveTime::from_num_seconds_from_midnight_opt(
        u32::try_from(millis.div_euclid(MILLIS_PER_SECOND)).ok()?,
        u32::try_from(millis.rem_euclid(MILLIS_PER_SECOND) * NANOS_PER_MILLI).ok()?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use fluss::metadata::DataTypes;
    use fluss::row::{Date, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz};
    use serde_json::Value;

    fn field(name: &str, data_type: DataType) -> DataField {
        DataField::new(name, data_type, None)
    }

    fn to_json(row: &GenericRow, fields: &[DataField]) -> Value {
        let payload =
            serde_json::to_vec(&JsonRow::new(row, fields, &[])).expect("Failed to map row");
        serde_json::from_slice(&payload).expect("The payload should be JSON")
    }

    #[test]
    fn given_temporal_columns_when_mapped_should_keep_full_precision() {
        let fields = vec![
            field("date", DataTypes::date()),
            field("time", DataTypes::time_with_precision(3)),
            field("timestamp", DataTypes::timestamp()),
            field("timestamp_nanos", DataTypes::timestamp_with_precision(9)),
            field("timestamp_ltz", DataTypes::timestamp_ltz()),
        ];
        let mut row = GenericRow::new(5);
        row.set_field(0, Date::new(19_782));
        row.set_field(1, Time::new(45_296_789));
        row.set_field(
            2,
            TimestampNtz::from_millis_nanos(1_700_000_000_123, 456_000)
                .expect("Failed to build timestamp"),
        );
        row.set_field(
            3,
            TimestampNtz::from_millis_nanos(1_700_000_000_123, 456_789)
                .expect("Failed to build timestamp"),
        );
        row.set_field(
            4,
            TimestampLtz::from_millis_nanos(1_700_000_000_123, 456_000)
                .expect("Failed to build timestamp"),
        );

        let object = to_json(&row, &fields);

        assert_eq!(object["date"], Value::from("2024-02-29"));
        assert_eq!(object["time"], Value::from("12:34:56.789"));
        assert_eq!(
            object["timestamp"],
            Value::from("2023-11-14 22:13:20.123456")
        );
        assert_eq!(
            object["timestamp_nanos"],
            Value::from("2023-11-14 22:13:20.123456789")
        );
        assert_eq!(
            object["timestamp_ltz"],
            Value::from("2023-11-14T22:13:20.123456+00:00")
        );
    }

    #[test]
    fn given_timestamp_before_the_epoch_when_mapped_should_borrow_from_the_previous_second() {
        let fields = vec![field("timestamp", DataTypes::timestamp_with_precision(3))];
        let mut row = GenericRow::new(1);
        row.set_field(
            0,
            TimestampNtz::from_millis_nanos(-1, 0).expect("Failed to build timestamp"),
        );

        let object = to_json(&row, &fields);

        assert_eq!(object["timestamp"], Value::from("1969-12-31 23:59:59.999"));
    }

    #[test]
    fn given_date_beyond_what_can_be_formatted_should_fail_naming_the_column() {
        let fields = vec![field("shipped_on", DataTypes::date())];
        let mut row = GenericRow::new(1);
        row.set_field(0, Date::new(i32::MAX));

        let error = serde_json::to_vec(&JsonRow::new(&row, &fields, &[]))
            .expect_err("A date chrono cannot hold should fail");

        assert!(error.to_string().contains("'shipped_on'"), "{error}");
    }

    #[test]
    fn given_decimal_and_fixed_width_columns_when_mapped_should_produce_strings() {
        let fields = vec![
            field("amount", DataTypes::decimal(10, 2)),
            field("code", DataTypes::char(5)),
            field("digest", DataTypes::binary(3)),
        ];
        let mut row = GenericRow::new(3);
        row.set_field(
            0,
            Decimal::from_unscaled_long(12_345, 10, 2).expect("Failed to build decimal"),
        );
        row.set_field(1, "abcde");
        row.set_field(2, [4u8, 5, 6].as_slice());

        let object = to_json(&row, &fields);

        assert_eq!(object["amount"], Value::from("123.45"));
        assert_eq!(object["code"], Value::from("abcde"));
        assert_eq!(object["digest"], Value::from(BASE64.encode([4u8, 5, 6])));
    }

    #[test]
    fn given_scalar_columns_when_mapped_should_produce_json_object() {
        let fields = vec![
            field("id", DataTypes::int()),
            field("name", DataTypes::string()),
            field("active", DataTypes::boolean()),
            field("ratio", DataTypes::double()),
            field("total", DataTypes::bigint()),
        ];
        let mut row = GenericRow::new(5);
        row.set_field(0, 7i32);
        row.set_field(1, "alice");
        row.set_field(2, true);
        row.set_field(3, 1.5f64);
        row.set_field(4, 90i64);

        let object = to_json(&row, &fields);

        assert_eq!(object["id"], Value::from(7));
        assert_eq!(object["name"], Value::from("alice"));
        assert_eq!(object["active"], Value::from(true));
        assert_eq!(object["ratio"], Value::from(1.5));
        assert_eq!(object["total"], Value::from(90));
    }

    #[test]
    fn given_unset_column_when_mapped_should_produce_null() {
        let fields = vec![
            field("id", DataTypes::int()),
            field("name", DataTypes::string()),
        ];
        let mut row = GenericRow::new(2);
        row.set_field(0, 1i32);

        let object = to_json(&row, &fields);

        assert_eq!(object["id"], Value::from(1));
        assert_eq!(object["name"], Value::Null);
    }

    #[test]
    fn given_binary_column_when_mapped_should_produce_base64() {
        let fields = vec![field("blob", DataTypes::bytes())];
        let mut row = GenericRow::new(1);
        row.set_field(0, [1u8, 2, 3].as_slice());

        let object = to_json(&row, &fields);

        assert_eq!(object["blob"], Value::from(BASE64.encode([1u8, 2, 3])));
    }

    #[test]
    fn given_scalar_columns_when_validated_should_be_accepted() {
        let fields = vec![
            field("a", DataTypes::string()),
            field("b", DataTypes::timestamp()),
            field("c", DataTypes::decimal(10, 2)),
        ];

        assert!(ensure_supported_types(&fields).is_ok());
    }

    #[test]
    fn given_nested_column_when_validated_should_be_rejected() {
        let fields = vec![
            field("id", DataTypes::int()),
            field("tags", DataTypes::array(DataTypes::string())),
        ];

        let error = ensure_supported_types(&fields).expect_err("Nested column should be rejected");

        assert!(matches!(error, Error::SchemaMismatch(message) if message.contains("tags")));
    }

    #[test]
    fn given_non_finite_floats_should_map_to_null() {
        let fields = vec![
            field("nan", DataTypes::double()),
            field("infinite", DataTypes::float()),
            field("finite", DataTypes::double()),
        ];
        let mut row = GenericRow::new(3);
        row.set_field(0, f64::NAN);
        row.set_field(1, f32::INFINITY);
        row.set_field(2, 2.5f64);

        let object = to_json(&row, &fields);

        assert_eq!(object["nan"], Value::Null);
        assert_eq!(object["infinite"], Value::Null);
        assert_eq!(object["finite"], Value::from(2.5));
    }
}
