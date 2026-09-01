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

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use fluss::metadata::{DataField, DataType};
use fluss::row::InternalRow;
use iggy_connector_sdk::Error;
use serde_json::{Map, Number, Value};

/// Temporal values are emitted as their Fluss-native integer representation rather than
/// formatted strings: `Date` as days since the Unix epoch, `Time` as milliseconds since
/// midnight, and both timestamp kinds as milliseconds since the epoch. Formatting would
/// force a timezone policy on downstream consumers that Fluss itself does not carry.
pub(crate) fn row_to_json(
    row: &dyn InternalRow,
    fields: &[DataField],
) -> Result<Map<String, Value>, Error> {
    let mut object = Map::with_capacity(fields.len());
    for (position, field) in fields.iter().enumerate() {
        let value = read_field(row, position, field.data_type())?;
        object.insert(field.name().to_owned(), value);
    }
    Ok(object)
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

fn read_field(
    row: &dyn InternalRow,
    position: usize,
    data_type: &DataType,
) -> Result<Value, Error> {
    if row.is_null_at(position).map_err(read_error)? {
        return Ok(Value::Null);
    }

    let value = match data_type {
        DataType::Boolean(_) => Value::Bool(row.get_boolean(position).map_err(read_error)?),
        DataType::TinyInt(_) => Value::from(row.get_byte(position).map_err(read_error)?),
        DataType::SmallInt(_) => Value::from(row.get_short(position).map_err(read_error)?),
        DataType::Int(_) => Value::from(row.get_int(position).map_err(read_error)?),
        DataType::BigInt(_) => Value::from(row.get_long(position).map_err(read_error)?),
        DataType::Float(_) => float_value(f64::from(row.get_float(position).map_err(read_error)?)),
        DataType::Double(_) => float_value(row.get_double(position).map_err(read_error)?),
        DataType::Char(inner) => Value::String(
            row.get_char(position, inner.length() as usize)
                .map_err(read_error)?
                .to_owned(),
        ),
        DataType::String(_) => {
            Value::String(row.get_string(position).map_err(read_error)?.to_owned())
        }
        DataType::Decimal(inner) => {
            let decimal = row
                .get_decimal(position, inner.precision() as usize, inner.scale() as usize)
                .map_err(read_error)?;
            Value::String(decimal.to_big_decimal().to_string())
        }
        DataType::Date(_) => Value::from(row.get_date(position).map_err(read_error)?.get_inner()),
        DataType::Time(_) => Value::from(row.get_time(position).map_err(read_error)?.get_inner()),
        DataType::Timestamp(inner) => Value::from(
            row.get_timestamp_ntz(position, inner.precision())
                .map_err(read_error)?
                .get_millisecond(),
        ),
        DataType::TimestampLTz(inner) => Value::from(
            row.get_timestamp_ltz(position, inner.precision())
                .map_err(read_error)?
                .get_epoch_millisecond(),
        ),
        DataType::Bytes(_) => {
            Value::String(BASE64.encode(row.get_bytes(position).map_err(read_error)?))
        }
        DataType::Binary(inner) => Value::String(
            BASE64.encode(
                row.get_binary(position, inner.length())
                    .map_err(read_error)?,
            ),
        ),
        DataType::Array(_) | DataType::Map(_) | DataType::Row(_) => {
            return Err(Error::SchemaMismatch(format!(
                "nested type {data_type:?} is not supported by the Apache Fluss source"
            )));
        }
    };
    Ok(value)
}

/// JSON has no encoding for NaN or infinity, so those collapse to null rather than
/// failing the whole batch over one degenerate float.
fn float_value(value: f64) -> Value {
    Number::from_f64(value).map_or(Value::Null, Value::Number)
}

fn read_error(error: fluss::error::Error) -> Error {
    Error::InvalidRecordValue(format!("failed to read Apache Fluss column: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::DataTypes;
    use fluss::row::GenericRow;

    fn field(name: &str, data_type: DataType) -> DataField {
        DataField::new(name, data_type, None)
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

        let object = row_to_json(&row, &fields).expect("Failed to map row");

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

        let object = row_to_json(&row, &fields).expect("Failed to map row");

        assert_eq!(object["id"], Value::from(1));
        assert_eq!(object["name"], Value::Null);
    }

    #[test]
    fn given_binary_column_when_mapped_should_produce_base64() {
        let fields = vec![field("blob", DataTypes::bytes())];
        let mut row = GenericRow::new(1);
        row.set_field(0, [1u8, 2, 3].as_slice());

        let object = row_to_json(&row, &fields).expect("Failed to map row");

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
    fn given_non_finite_float_should_map_to_null() {
        assert_eq!(float_value(f64::NAN), Value::Null);
        assert_eq!(float_value(f64::INFINITY), Value::Null);
        assert_eq!(float_value(2.5), Value::from(2.5));
    }
}
