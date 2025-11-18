/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::router::arrow_streamer::JsonArrowReader;
use crate::slice_user_table;
use arrow_json::ReaderBuilder;
use async_trait::async_trait;
use iceberg::TableIdent;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{Literal, PrimitiveLiteral, PrimitiveType, Struct, StructType};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog,
    writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, Schema};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tracing::{error, warn};
use uuid::Uuid;

mod arrow_streamer;
pub mod dynamic_router;
pub mod static_router;

pub fn is_valid_namespaced_table(input: &str) -> bool {
    let parts: Vec<&str> = input.split('.').collect();
    parts.len() >= 2 && parts.iter().all(|part| !part.is_empty())
}

async fn table_exists(route_field_val: &str, catalog: &dyn Catalog) -> Option<Table> {
    let sliced_table = slice_user_table(route_field_val);
    let table_ident = TableIdent::from_strs(&sliced_table).ok()?;

    catalog.load_table(&table_ident).await.ok()
}

pub fn primitive_type_to_literal(pt: &PrimitiveType) -> Result<PrimitiveLiteral, Error> {
    match pt {
        PrimitiveType::Boolean => Ok(PrimitiveLiteral::Boolean(false)),
        PrimitiveType::Int => Ok(PrimitiveLiteral::Int(0)),
        PrimitiveType::Long => Ok(PrimitiveLiteral::Long(0)),
        PrimitiveType::Decimal { .. } => Ok(PrimitiveLiteral::Int128(0)),
        PrimitiveType::Date => Ok(PrimitiveLiteral::Int(0)), // e.g. days since epoch
        PrimitiveType::Time => Ok(PrimitiveLiteral::Long(0)), // microseconds since midnight
        PrimitiveType::Timestamp => Ok(PrimitiveLiteral::Long(0)), // microseconds since epoch
        PrimitiveType::Timestamptz => Ok(PrimitiveLiteral::Long(0)),
        PrimitiveType::TimestampNs => Ok(PrimitiveLiteral::Long(0)),
        PrimitiveType::TimestamptzNs => Ok(PrimitiveLiteral::Long(0)),
        PrimitiveType::String => Ok(PrimitiveLiteral::String(String::new())),
        PrimitiveType::Uuid => Ok(PrimitiveLiteral::Binary(vec![0; 16])),
        PrimitiveType::Fixed(len) => Ok(PrimitiveLiteral::Binary(vec![0; *len as usize])),
        PrimitiveType::Binary => Ok(PrimitiveLiteral::Binary(Vec::new())),
        _ => {
            error!("Partition type not supported");
            Err(Error::InvalidConfig)
        }
    }
}

fn get_partition_type_value(default_partition_type: &StructType) -> Result<Option<Struct>, Error> {
    let mut fields: Vec<Option<Literal>> = Vec::new();

    if default_partition_type.fields().is_empty() {
        return Ok(None);
    };

    for field in default_partition_type.fields() {
        let field_type = field.field_type.as_primitive_type().ok_or_else(|| {
            error!("The partition type of the configured iceberg table is not a primitive type");
            Error::InvalidConfig
        })?;

        let value = Some(Literal::Primitive(primitive_type_to_literal(field_type)?));

        fields.push(value);
    }
    Ok(Some(Struct::from_iter(fields)))
}

async fn write_data(
    messages: &[Payload],
    table: &Table,
    catalog: &dyn Catalog,
    messages_schema: Schema,
) -> Result<(), Error> {
    let location = DefaultLocationGenerator::new(table.metadata().clone()).map_err(|err| {
        error!(
            "Failed to get location on table: {}. Error: {}",
            table.metadata().uuid(),
            err
        );
        Error::InvalidConfig
    })?;

    let file_name_gen = DefaultFileNameGenerator::new(
        Uuid::new_v4().to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location,
        file_name_gen,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(
        parquet_writer_builder,
        get_partition_type_value(table.metadata().default_partition_type())?,
        table.metadata().default_partition_spec_id(),
    );

    let mut writer = data_file_writer_builder.build().await.map_err(|err| {
        error!("Error while constructing data file writer: {}", err);
        Error::InitError(err.to_string())
    })?;

    let msgs: Vec<&simd_json::OwnedValue> = messages
        .iter()
        .filter_map(|payload| match payload {
            Payload::Json(value) => Some(value),
            _ => {
                warn!(
                    "Unsupported type of payload, expected JSON, got {}",
                    messages_schema.to_string()
                );
                None
            }
        })
        .collect();

    let cursor = JsonArrowReader::new(msgs.as_slice());
    let reader = ReaderBuilder::new(Arc::new(
        schema_to_arrow_schema(&table.metadata().current_schema().clone()).map_err(|err| {
            error!(
                "Error while mapping records to Iceberg table with uuid: {}. Error {}",
                table.metadata().uuid(),
                err
            );
            Error::InvalidRecord
        })?,
    ))
    .build(cursor)
    .map_err(|err| {
        error!(
            "Error while building Iceberg reader from message payload: {}",
            err
        );
        Error::InitError(err.to_string())
    })?;

    for batch in reader {
        let batch_data = batch.map_err(|err| {
            error!("Error while getting record batch: {}", err);
            Error::InvalidRecord
        })?;
        writer.write(batch_data).await.map_err(|err| {
            error!("Error while writing record batch: {}", err);
            Error::InvalidRecord
        })?;
    }

    let data_files = writer.close().await.map_err(|err| {
        error!("Error while writing data records to Parquet file: {}", err);
        Error::InvalidRecord
    })?;

    let table_commit = Transaction::new(table);

    let action = table_commit.fast_append().add_data_files(data_files);

    let tx = action.apply(table_commit).map_err(|err| {
        error!(
            "Failed to apply transaction on table with UUID: {}, Error: {}",
            table.metadata().uuid(),
            err
        );
        Error::InvalidRecord
    })?;

    let _table = tx.commit(catalog).await.map_err(|err| {
        error!(
            "Failed to commit transaction on table with UUID: {}, Error: {}",
            table.metadata().uuid(),
            err
        );
        Error::InvalidRecord
    })?;
    Ok(())
}

#[async_trait]
pub trait Router: std::fmt::Debug + Sync + Send {
    async fn route_data(
        &self,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), crate::Error>;
}
