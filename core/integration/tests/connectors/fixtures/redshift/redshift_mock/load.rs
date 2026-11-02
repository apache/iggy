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

use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
        StringArray,
    },
    datatypes::DataType,
};

use bytes::Bytes;
use futures::pin_mut;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use s3::{Bucket, Region, creds::Credentials};
use tokio_postgres::{
    Client as PgClient,
    binary_copy::BinaryCopyInWriter,
    types::{ToSql, Type as PgType},
};

pub async fn fetch_table_columns(
    pg: &PgClient,
    table: &str,
) -> Result<Option<Vec<ColumnDef>>, String> {
    let query = format!(
        "SELECT column_name, udt_name FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position",
        table.replace('"', "")
    );

    let rows = pg.query(&query, &[]).await.map_err(|e| {
        tracing::error!("{:?}", e);
        e.to_string()
    })?;

    if rows.is_empty() {
        return Ok(None);
    }

    let rows: Result<Vec<ColumnDef>, String> = rows
        .into_iter()
        .map(|row| {
            let name: String = row.get(0);
            let udt: String = row.get(1);
            Ok(ColumnDef {
                name,
                pg_type: udt_name_to_type(&udt)?,
            })
        })
        .collect();

    Ok(Some(rows?))
}

pub async fn load_one_object(
    pg: &PgClient,
    table: &str,
    columns: &[ColumnDef],
    bytes: Bytes,
) -> Result<usize, String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| e.to_string())?
        .build()
        .map_err(|e| e.to_string())?;

    let col_list = columns
        .iter()
        .map(|v| format!("\"{}\"", v.name))
        .collect::<Vec<String>>()
        .join(", ");

    let types = columns
        .iter()
        .map(|v| v.pg_type.clone())
        .collect::<Vec<PgType>>();

    let copy_sql = format!("COPY {table} ({col_list}) FROM STDIN BINARY");
    let sink = pg.copy_in(&copy_sql).await.map_err(|e| {
        tracing::error!("{:?}", e.as_db_error());

        e.to_string()
    })?;

    tracing::info!("COPY FROM STDIN started");

    let writer = BinaryCopyInWriter::new(sink, &types);

    pin_mut!(writer);

    let mut n = 0usize;

    for batch in reader {
        let batch = batch.map_err(|e| e.to_string())?;

        for row_idx in 0..batch.num_rows() {
            let row_values = extract_row(&batch, row_idx, columns)?;

            let refs: Vec<&(dyn ToSql + Sync)> = row_values
                .iter()
                .map(|v| v.as_ref() as &(dyn ToSql + Sync))
                .collect();

            writer.as_mut().write(&refs).await.map_err(|e| {
                tracing::error!("{:?}", e.as_db_error());

                e.to_string()
            })?;

            n += 1;
        }
    }

    writer.finish().await.map_err(|e| {
        tracing::error!("{:?}", e.as_db_error());

        e.to_string()
    })?;

    Ok(n)
}

#[derive(Debug)]
pub struct ColumnDef {
    pub name: String,
    pub pg_type: PgType,
}

macro_rules! scalar_column {
    ($array:expr, $arr_ty:ty, $val_ty:ty, $row:expr, $conv:expr) => {{
        let a = $array
            .as_any()
            .downcast_ref::<$arr_ty>()
            .ok_or_else(|| format!("expected {} array", stringify!($arr_ty)))?;

        if a.is_null($row) {
            Box::new(None::<$val_ty>) as Box<dyn ToSql + Sync + Send>
        } else {
            let conv: fn(_) -> $val_ty = $conv;
            Box::new(conv(a.value($row))) as Box<dyn ToSql + Sync + Send>
        }
    }};
}

/// Only covers common scalar types. Extend as your Parquet exports need more —
/// this deliberately doesn't try to handle structs, lists, or decimals up front.
fn extract_row<'a>(
    batch: &'a RecordBatch,
    row: usize,
    columns: &'a [ColumnDef],
) -> Result<Vec<Box<dyn ToSql + Send + Sync + 'a>>, String> {
    let mut out = Vec::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        let array = batch.column(i);

        let value: Box<dyn ToSql + Sync + Send> = match array.data_type() {
            DataType::Utf8 => {
                scalar_column!(array, StringArray, String, row, |v: &str| v.to_string())
            }
            DataType::Int64 => {
                scalar_column!(array, Int64Array, i64, row, |v: i64| v)
            }
            DataType::Int32 => scalar_column!(array, Int32Array, i32, row, |v: i32| v),
            DataType::Float64 => scalar_column!(array, Float64Array, f64, row, |v: f64| v),
            DataType::Boolean => scalar_column!(array, BooleanArray, bool, row, |v: bool| v),
            DataType::Binary => {
                scalar_column!(array, BinaryArray, Vec<u8>, row, |v: &[u8]| v.to_vec())
            }
            other => Err(format!(
                "unsupported parquet column type {other:?} for column {}",
                col.name
            ))?,
        };
        out.push(value);
    }

    Ok(out)
}

fn udt_name_to_type(udt: &str) -> Result<PgType, String> {
    Ok(match udt {
        "int2" => PgType::INT2,
        "int4" => PgType::INT4,
        "int8" => PgType::INT8,
        "float4" => PgType::FLOAT4,
        "float8" => PgType::FLOAT8,
        // Numeric serialiation requires extra work
        // Safe to use VARCHAR
        "numeric" => PgType::VARCHAR,
        "bool" => PgType::BOOL,
        "text" | "varchar" | "bpchar" => PgType::TEXT,
        "timestamp" => PgType::TIMESTAMP,
        "timestamptz" => PgType::TIMESTAMPTZ,
        "date" => PgType::DATE,
        "jsonb" => PgType::JSONB,
        "bytea" => PgType::BYTEA,
        other => Err(format!("unsupported column type for COPY target: {other}"))?,
    })
}

fn arrow_to_type(a_type: &DataType) -> Result<PgType, String> {
    match a_type {
        DataType::Boolean => Ok(PgType::BOOL),
        DataType::Binary | DataType::FixedSizeBinary(_) => Ok(PgType::BYTEA),
        DataType::Float64 => Ok(PgType::FLOAT8),
        DataType::Float32 | DataType::Float16 => Ok(PgType::FLOAT4),
        DataType::Int64 => Ok(PgType::INT8),
        DataType::Int32 => Ok(PgType::INT4),
        DataType::Decimal128(_, _) => Ok(PgType::VARCHAR),
        DataType::Decimal256(_, _) => Ok(PgType::VARCHAR),
        DataType::Utf8 => Ok(PgType::TEXT),
        other => Err(format!("Unsuppoerted type: {}", other)),
    }
}

pub fn infer_parquet_schema(bytes: Bytes) -> Result<Vec<ColumnDef>, String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| e.to_string())?;

    reader
        .schema()
        .fields()
        .iter()
        .map(|v| {
            Ok(ColumnDef {
                name: v.name().into(),
                pg_type: arrow_to_type(v.data_type())?,
            })
        })
        .collect()
}

/// S3
#[allow(unused)]
#[derive(Clone)]
pub struct S3Client {
    bucket_name: String,
    inner: Box<Bucket>,
}

impl S3Client {
    pub async fn new(
        bucket_name: &str,
        s3_endpoint: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> Result<Self, String> {
        let region = Region::Custom {
            region: region.into(),
            endpoint: s3_endpoint.into(),
        };

        let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None)
            .map_err(|e| e.to_string())?;

        let bucket = Bucket::new(bucket_name, region, credentials)
            .map_err(|e| format!("failed to setup bucket: {e}"))?
            .with_path_style();

        Ok(S3Client {
            bucket_name: bucket_name.into(),
            inner: bucket,
        })
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>, String> {
        tracing::info!(
            "Downloading object '{}' from bucket '{}'",
            key,
            self.bucket_name
        );

        let response = self
            .inner
            .get_object(key)
            .await
            .map_err(|e| e.to_string())?;

        if response.status_code() != 200 {
            tracing::error!(
                "S3 get object returned status {}: {}",
                response.status_code(),
                String::from_utf8_lossy(response.as_slice())
            );
            return Err(format!(
                "S3 get_object failed with status {}",
                response.status_code()
            ));
        }

        tracing::info!(
            "Retrieved {} bytes to s3://{}/{}",
            response.bytes().len(),
            self.inner.name(),
            key
        );

        Ok(response.bytes().to_vec())
    }
}
