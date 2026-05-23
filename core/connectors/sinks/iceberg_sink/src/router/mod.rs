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
use iceberg::spec::{
    Literal, PartitionKey, PartitionSpec, PrimitiveLiteral, PrimitiveType, Struct, StructType,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
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

fn format_error_chain(err: &dyn std::error::Error) -> String {
    let mut chain = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        chain.push_str(": ");
        chain.push_str(&cause.to_string());
        source = cause.source();
    }
    chain
}

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
    );

    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location.clone(),
        file_name_gen.clone(),
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);

    let partition_spec = PartitionSpec::builder(table.current_schema_ref());

    let partition_type = get_partition_type_value(table.metadata().default_partition_type())?;

    let mut writer = data_file_writer_builder
        .build(match partition_type {
            None => None,
            Some(p_type) => Some(PartitionKey::new(
                partition_spec
                    .build()
                    .map_err(|err| Error::InitError(err.to_string()))?,
                table.current_schema_ref(),
                p_type,
            )),
        })
        .await
        .map_err(|err| {
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
            let chain = format_error_chain(&err);
            error!(
                "Error while mapping records to Iceberg table with uuid: {}. Error {}",
                table.metadata().uuid(),
                chain
            );
            Error::SchemaMismatch(chain)
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

    let write_result: Result<(), Error> = async {
        for batch in reader {
            let batch_data = batch.map_err(|err| {
                let chain = format_error_chain(&err);
                error!("Error while getting record batch: {}", chain);
                Error::InvalidRecordValue(chain)
            })?;
            writer.write(batch_data).await.map_err(|err| {
                let chain = format_error_chain(&err);
                error!("Error while writing record batch: {}", chain);
                Error::WriteFailure(chain)
            })?;
        }
        Ok(())
    }
    .await;

    if let Err(e) = &write_result {
        error!(
            "Batch loop failed ({}), closing writer to release resources",
            e
        );
        if let Err(close_err) = writer.close().await {
            error!("Failed to close writer after batch error: {}", close_err);
        }
        return Err(write_result.unwrap_err());
    }

    let data_files = writer.close().await.map_err(|err| {
        let chain = format_error_chain(&err);
        error!(
            "Error while writing data records to Parquet file: {}",
            chain
        );
        Error::WriteFailure(chain)
    })?;

    let table_commit = Transaction::new(table);

    let action = table_commit.fast_append().add_data_files(data_files);

    let tx = action.apply(table_commit).map_err(|err| {
        let chain = format_error_chain(&err);
        error!(
            "Failed to apply transaction on table with UUID: {}, Error: {}",
            table.metadata().uuid(),
            chain
        );
        Error::TransactionApplyError(chain)
    })?;

    let _table = tx.commit(catalog).await.map_err(|err| {
        let chain = format_error_chain(&err);
        error!(
            "Failed to commit transaction on table with UUID: {}, Error: {}",
            table.metadata().uuid(),
            chain
        );
        Error::CatalogCommitError(chain)
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
