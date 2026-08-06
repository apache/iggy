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

use pgwire::api::Type as PgWireType;
use pgwire::api::portal::Portal;
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use tokio_postgres::{
    Row,
    types::{ToSql, Type as PgType},
};

use crate::connectors::fixtures::redshift::redshift_mock::parser::ParsedStatement;

/// Building wire-level column descriptors from a prepared statement's real
/// result metadata (from `Client::prepare_typed`),
pub fn columns_to_field_info(columns: &[tokio_postgres::Column]) -> Vec<FieldInfo> {
    columns
        .iter()
        .map(|c| {
            FieldInfo::new(
                c.name().to_owned(),
                None,
                None,
                tokio_type_to_wire(c.type_()),
                pgwire::api::results::FieldFormat::Text,
            )
        })
        .collect()
}

pub fn tokio_type_to_wire(t: &PgType) -> PgWireType {
    match *t {
        PgType::BOOL => PgWireType::BOOL,
        PgType::INT2 => PgWireType::INT2,
        PgType::INT4 => PgWireType::INT4,
        PgType::INT8 => PgWireType::INT8,
        PgType::FLOAT4 => PgWireType::FLOAT4,
        PgType::FLOAT8 => PgWireType::FLOAT8,
        PgType::TEXT => PgWireType::TEXT,
        PgType::VARCHAR => PgWireType::VARCHAR,
        PgType::TIMESTAMP => PgWireType::TIMESTAMP,
        PgType::TIMESTAMPTZ => PgWireType::TIMESTAMPTZ,
        PgType::NUMERIC => PgWireType::NUMERIC,
        PgType::UUID => PgWireType::UUID,
        PgType::JSONB => PgWireType::JSONB,
        PgType::BYTEA => PgWireType::BYTEA,
        _ => PgWireType::UNKNOWN,
    }
}

/// Convert one backend row into a wire DataRow, column by column, by OID.
pub fn row_to_data_row(row: &Row, fields: &[FieldInfo]) -> PgWireResult<DataRow> {
    let cols = Arc::new(fields.to_vec());
    let mut encoder = DataRowEncoder::new(cols);

    for (i, field) in fields.iter().enumerate() {
        match field.datatype() {
            &PgWireType::BOOL => {
                encoder.encode_field(&row.try_get::<_, Option<bool>>(i).map_err(backend_err)?)?
            }
            &PgWireType::INT2 => {
                encoder.encode_field(&row.try_get::<_, Option<i16>>(i).map_err(backend_err)?)?
            }
            &PgWireType::INT4 => {
                encoder.encode_field(&row.try_get::<_, Option<i32>>(i).map_err(backend_err)?)?
            }
            &PgWireType::INT8 => {
                encoder.encode_field(&row.try_get::<_, Option<i64>>(i).map_err(backend_err)?)?
            }
            &PgWireType::FLOAT4 => {
                encoder.encode_field(&row.try_get::<_, Option<f32>>(i).map_err(backend_err)?)?
            }
            &PgWireType::FLOAT8 => {
                encoder.encode_field(&row.try_get::<_, Option<f64>>(i).map_err(backend_err)?)?
            }
            &PgWireType::TEXT | &PgWireType::VARCHAR => {
                encoder.encode_field(&row.try_get::<_, Option<String>>(i).map_err(backend_err)?)?
            }
            &PgWireType::NUMERIC => {
                encoder.encode_field(&row.try_get::<_, Option<String>>(i).map_err(backend_err)?)?
            }
            &PgWireType::TIMESTAMP => encoder.encode_field(
                &row.try_get::<_, Option<chrono::NaiveDateTime>>(i)
                    .map_err(backend_err)?,
            )?,
            &PgWireType::BYTEA => {
                encoder.encode_field(&row.try_get::<_, Option<Vec<u8>>>(i).map_err(backend_err)?)?
            }
            _ => {
                // Unknown/unhandled OID: fall back to text representation via
                // an explicit cast rather than guessing at binary layout.
                let v: Option<String> = row.try_get(i).ok();
                encoder.encode_field(&v)?
            }
        }
    }

    Ok(encoder.take_row())
}

pub fn backend_err(e: tokio_postgres::Error) -> PgWireError {
    tracing::error!("{}", e);
    PgWireError::ApiError(Box::new(e))
}

pub fn user_err(msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "42601".into(),
        msg.into(),
    )))
}

/// Decode one Bind parameter into a boxed ToSql, given the OID Describe
pub fn decode_param(
    portal: &Portal<ParsedStatement>,
    oid: &PgType,
    index: usize,
) -> PgWireResult<Box<dyn ToSql + Sync + Send>> {
    macro_rules! decode {
        ($t:ty) => {{
            let value: Option<$t> = portal.parameter(index, oid)?;
            Box::new(value) as Box<dyn ToSql + Sync + Send>
        }};
    }

    Ok(match *oid {
        PgType::BOOL => decode!(bool),
        PgType::INT2 => decode!(i16),
        PgType::INT4 => decode!(i32),
        PgType::INT8 => decode!(i64),
        PgType::FLOAT4 => decode!(f32),
        PgType::FLOAT8 => decode!(f64),
        PgType::TEXT | PgType::VARCHAR => decode!(String),
        PgType::BYTEA => decode!(Vec<u8>),
        PgType::TIMESTAMP => decode!(chrono::NaiveDateTime),
        _ => decode!(String), // last resort: treat as text
    })
}
