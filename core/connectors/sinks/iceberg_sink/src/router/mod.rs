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

use crate::router::arrow_streamer::JsonArrowReader;
use crate::slice_user_table;
use arrow_array::RecordBatch;
use arrow_json::ReaderBuilder;
use async_trait::async_trait;
use iceberg::TableIdent;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, PartitionKey, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog,
    writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, Schema};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use tracing::{error, warn};
use uuid::Uuid;

type ParquetDataFileWriterBuilder =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;
type ParquetDataFileWriter =
    DataFileWriter<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

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

async fn write_data(
    messages: &[Payload],
    table: &Table,
    catalog: &dyn Catalog,
    messages_schema: Schema,
) -> Result<(), Error> {
    let data_files = write_data_files(messages, table, messages_schema).await?;

    // Data files are kept on commit failure: the catalog may have applied the
    // commit before the error surfaced, and retries must check that first.
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

/// Writes the JSON payloads as Parquet data files under the table location,
/// laid out by the table's default partition spec. Nothing is committed here.
async fn write_data_files(
    messages: &[Payload],
    table: &Table,
    messages_schema: Schema,
) -> Result<Vec<DataFile>, Error> {
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

    let mut writer = TableWriter::new(table, data_file_writer_builder).map_err(|err| {
        let chain = format_error_chain(&err);
        error!("Error while constructing data file writer: {}", chain);
        Error::InitError(chain)
    })?;

    let write_result: Result<(), Error> = async {
        for batch in reader {
            let batch_data = batch.map_err(|err| {
                let chain = format_error_chain(&err);
                error!("Error while getting record batch: {}", chain);
                Error::InvalidRecordValue(chain)
            })?;
            writer.write(batch_data).await?;
        }
        Ok(())
    }
    .await;

    let (data_files, close_error) = writer.close().await;
    if let Some(err) = write_result.err().or(close_error) {
        error!(
            "Writing data files failed ({}), deleting the uncommitted ones",
            err
        );
        delete_uncommitted_files(table.file_io(), &data_files).await;
        return Err(err);
    }
    Ok(data_files)
}

/// Files finalized by a failed batch are never committed, so they are removed
/// instead of being left as orphans on the object store.
async fn delete_uncommitted_files(file_io: &FileIO, data_files: &[DataFile]) {
    for data_file in data_files {
        if let Err(err) = file_io.delete(data_file.file_path()).await {
            warn!(
                "Failed to delete uncommitted data file {}: {}",
                data_file.file_path(),
                err
            );
        }
    }
}

/// Data file writer bound to the table's default partition spec.
///
/// Every batch is mapped to partition keys and fanned out to one data file
/// writer per key, so a mixed batch lands in the right partitions. An
/// unpartitioned table maps every batch to the single key of its spec.
///
/// The per-partition writers are owned here rather than by iceberg's
/// `FanoutWriter` because its `close` stops at the first failing writer and
/// loses the files the other writers already finalized.
struct TableWriter {
    partitioner: Partitioner,
    builder: ParquetDataFileWriterBuilder,
    writers: HashMap<Struct, ParquetDataFileWriter>,
}

enum Partitioner {
    Unpartitioned(PartitionKey),
    /// Boxed to keep the enum small (`clippy::large_enum_variant`).
    Partitioned(Box<RecordBatchPartitionSplitter>),
}

impl TableWriter {
    fn new(table: &Table, builder: ParquetDataFileWriterBuilder) -> iceberg::Result<Self> {
        let partition_spec = table.metadata().default_partition_spec();
        let schema = table.metadata().current_schema().clone();
        let partitioner = if partition_spec.is_unpartitioned() {
            // A spec whose fields are all `Transform::Void` counts as unpartitioned,
            // but its partition type still has one field per void field and the
            // commit rejects data files whose partition value has a different arity.
            let void_field_count = table.metadata().default_partition_type().fields().len();
            Partitioner::Unpartitioned(PartitionKey::new(
                partition_spec.as_ref().clone(),
                schema,
                Struct::from_iter(vec![None; void_field_count]),
            ))
        } else {
            let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
                schema,
                partition_spec.clone(),
            )?;
            Partitioner::Partitioned(Box::new(splitter))
        };
        Ok(Self {
            partitioner,
            builder,
            writers: HashMap::new(),
        })
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<(), Error> {
        let partitioned_batches = match &self.partitioner {
            Partitioner::Unpartitioned(partition_key) => vec![(partition_key.clone(), batch)],
            Partitioner::Partitioned(splitter) => splitter.split(&batch).map_err(|err| {
                let chain = format_error_chain(&err);
                error!("Error while splitting record batch by partition: {}", chain);
                Error::InvalidRecordValue(chain)
            })?,
        };
        for (partition_key, partition_batch) in partitioned_batches {
            let writer = match self.writers.entry(partition_key.data().clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let writer = self
                        .builder
                        .build(Some(partition_key))
                        .await
                        .map_err(write_failure)?;
                    entry.insert(writer)
                }
            };
            writer.write(partition_batch).await.map_err(write_failure)?;
        }
        Ok(())
    }

    /// Closes every partition writer even after one of them fails, so the
    /// caller learns about all finalized files and can commit or delete them.
    /// Returns the first close failure alongside the files.
    async fn close(self) -> (Vec<DataFile>, Option<Error>) {
        let mut data_files = Vec::new();
        let mut first_error = None;
        for mut writer in self.writers.into_values() {
            match writer.close().await {
                Ok(files) => data_files.extend(files),
                Err(err) => {
                    let close_error = write_failure(err);
                    if first_error.is_none() {
                        first_error = Some(close_error);
                    }
                }
            }
        }
        (data_files, first_error)
    }
}

fn write_failure(err: iceberg::Error) -> Error {
    let chain = format_error_chain(&err);
    error!("Error while writing data file: {}", chain);
    Error::WriteFailure(chain)
}

#[async_trait]
pub trait Router: std::fmt::Debug + Sync + Send {
    async fn route_data(
        &self,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), crate::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{
        FormatVersion, Literal, NestedField, PrimitiveLiteral, PrimitiveType,
        Schema as IcebergSchema, SortOrder, TableMetadataBuilder, Transform, Type,
        UnboundPartitionSpec,
    };
    use std::collections::HashMap;

    const REGION_FIELD_ID: i32 = 2;

    fn in_memory_table(partition_spec: UnboundPartitionSpec) -> Table {
        let schema = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(
                    REGION_FIELD_ID,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .expect("Failed to build schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            partition_spec,
            SortOrder::unsorted_order(),
            "memory://warehouse/test/events".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("Failed to create table metadata")
        .build()
        .expect("Failed to build table metadata")
        .metadata;

        Table::builder()
            .identifier(TableIdent::from_strs(["test", "events"]).expect("Failed to build ident"))
            .metadata(metadata)
            .file_io(FileIO::new_with_memory())
            .build()
            .expect("Failed to build table")
    }

    fn json_payloads(rows: &[(i64, &str)]) -> Vec<Payload> {
        rows.iter()
            .map(|(id, region)| Payload::Json(simd_json::json!({ "id": *id, "region": *region })))
            .collect()
    }

    fn write(table: &Table, payloads: &[Payload]) -> Vec<DataFile> {
        tokio::runtime::Runtime::new()
            .expect("Failed to create runtime")
            .block_on(write_data_files(payloads, table, Schema::Json))
            .expect("Failed to write data files")
    }

    fn partition_region(data_file: &DataFile) -> String {
        match &data_file.partition()[0] {
            Some(Literal::Primitive(PrimitiveLiteral::String(region))) => region.clone(),
            other => panic!("Expected string partition value, got {other:?}"),
        }
    }

    #[test]
    fn given_partitioned_table_should_write_one_data_file_per_partition_value() {
        let partition_spec = UnboundPartitionSpec::builder()
            .add_partition_field(REGION_FIELD_ID, "region", Transform::Identity)
            .expect("Failed to add partition field")
            .build();
        let table = in_memory_table(partition_spec);
        let payloads = json_payloads(&[(1, "eu"), (2, "us"), (3, "eu")]);

        let data_files = write(&table, &payloads);

        assert_eq!(data_files.len(), 2);
        let by_region: HashMap<String, &DataFile> = data_files
            .iter()
            .map(|data_file| (partition_region(data_file), data_file))
            .collect();
        assert_eq!(by_region["eu"].record_count(), 2);
        assert!(by_region["eu"].file_path().contains("/data/region=eu/"));
        assert_eq!(by_region["us"].record_count(), 1);
        assert!(by_region["us"].file_path().contains("/data/region=us/"));
    }

    #[test]
    fn given_void_only_partition_spec_should_write_null_partition_values() {
        let partition_spec = UnboundPartitionSpec::builder()
            .add_partition_field(REGION_FIELD_ID, "region_void", Transform::Void)
            .expect("Failed to add partition field")
            .build();
        let table = in_memory_table(partition_spec);
        let payloads = json_payloads(&[(1, "eu"), (2, "us")]);

        let data_files = write(&table, &payloads);

        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].partition().fields(), &[None]);
        assert!(!data_files[0].file_path().contains("region_void="));
    }

    #[test]
    fn given_uncommitted_files_should_be_deleted_from_store() {
        let table = in_memory_table(UnboundPartitionSpec::builder().build());
        let payloads = json_payloads(&[(1, "eu")]);
        let data_files = write(&table, &payloads);
        let file_path = data_files[0].file_path().to_string();

        let exists_after_delete = tokio::runtime::Runtime::new()
            .expect("Failed to create runtime")
            .block_on(async {
                assert!(table.file_io().exists(&file_path).await.expect("exists"));
                delete_uncommitted_files(table.file_io(), &data_files).await;
                table.file_io().exists(&file_path).await.expect("exists")
            });

        assert!(!exists_after_delete);
    }

    #[test]
    fn given_unpartitioned_table_should_write_single_data_file_without_partition() {
        let table = in_memory_table(UnboundPartitionSpec::builder().build());
        let payloads = json_payloads(&[(1, "eu"), (2, "us")]);

        let data_files = write(&table, &payloads);

        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].record_count(), 2);
        assert!(data_files[0].partition().fields().is_empty());
        assert!(data_files[0].file_path().contains("/data/"));
        assert!(!data_files[0].file_path().contains("region="));
    }
}
