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

use crate::InvalidRows;
use crate::router::arrow_streamer::JsonArrowReader;
use crate::router::commit::commit_data_files;
use crate::router::redelivery::{HeldMessages, LastMessage, snapshot_properties};
use crate::slice_user_table;
use arrow_array::RecordBatch;
use arrow_json::ReaderBuilder;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use iceberg::TableIdent;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, PartitionKey, Struct};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog,
    writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
};
use iggy_connector_sdk::retry::RetryPolicy;
use iggy_connector_sdk::{ConsumedMessage, Error, Schema};
use parquet::file::properties::WriterProperties;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use tracing::{error, warn};
use uuid::Uuid;

type ParquetDataFileWriterBuilder =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;
type ParquetDataFileWriter =
    DataFileWriter<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

/// Rows decoded and written at a time, so the decoded Arrow data held in memory is one batch and
/// not the whole run. Matches the default batch size of the Arrow JSON reader.
const ROWS_PER_BATCH: usize = 1024;

/// Where a run of messages was consumed from, named in the warning of every row the sink skips.
pub struct BatchOrigin<'a> {
    pub connector_id: u32,
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition_id: u32,
}

impl Display for BatchOrigin<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "connector ID: {}, stream: {}, topic: {}, partition: {}",
            self.connector_id, self.stream, self.topic, self.partition_id
        )
    }
}

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
mod commit;
pub mod dynamic_router;
mod redelivery;
pub mod static_router;

pub fn is_valid_namespaced_table(input: &str) -> bool {
    let parts: Vec<&str> = input.split('.').collect();
    parts.len() >= 2 && parts.iter().all(|part| !part.is_empty())
}

async fn load_table(route_field_val: &str, catalog: &dyn Catalog) -> iceberg::Result<Table> {
    let table_ident = TableIdent::from_strs(slice_user_table(route_field_val))?;
    catalog.load_table(&table_ident).await
}

/// A message that a table rejects with `invalid_rows = "fail"`. Every message of the call before
/// it is committed, so that storing its offset, as an operator does to move past it, gives up
/// this message alone.
#[derive(Debug)]
pub(crate) struct Rejection {
    pub(crate) offset: u64,
    pub(crate) error: Error,
}

/// What a write of a call's messages to one table did.
#[derive(Debug, Default)]
pub(crate) struct Written {
    /// The rows committed, which leaves out the rows the sink skipped.
    pub(crate) committed_rows: u64,
    pub(crate) rejection: Option<Rejection>,
}

/// Rejections of several tables stop the call at the first rejected message.
fn first_rejection(rejection: Option<Rejection>, other: Option<Rejection>) -> Option<Rejection> {
    match (rejection, other) {
        (Some(rejection), Some(other)) if other.offset < rejection.offset => Some(other),
        (rejection, other) => rejection.or(other),
    }
}

/// The messages before `rejection`, which a call still has to deliver.
fn before<'m>(
    messages: &'m [ConsumedMessage],
    rejection: Option<&Rejection>,
) -> &'m [ConsumedMessage] {
    match rejection {
        Some(rejection) => {
            &messages[..messages.partition_point(|message| message.offset < rejection.offset)]
        }
        None => messages,
    }
}

/// Writes and commits `messages` to `table`. With `held`, the sink delivers at least once: the
/// messages that `table` already holds are not appended again, and the commit records the last
/// message it holds. `current` is a copy of `table` just loaded from the catalog.
#[allow(clippy::too_many_arguments)]
async fn write_data(
    origin: &BatchOrigin<'_>,
    messages: &[ConsumedMessage],
    table: &Table,
    current: Option<&Table>,
    catalog: &dyn Catalog,
    messages_schema: Schema,
    invalid_rows: InvalidRows,
    commit_retry: RetryPolicy,
    held: Option<&HeldMessages>,
) -> Result<Written, Error> {
    if messages.is_empty() {
        return Ok(Written::default());
    }
    let messages = match held {
        Some(held) => {
            held.unheld(origin, table, current, catalog, messages)
                .await?
        }
        None => messages,
    };
    if messages.is_empty() {
        return Ok(Written::default());
    }
    let (data_files, rejection) =
        write_data_files(origin, messages, table, messages_schema, invalid_rows).await?;
    let handled = before(messages, rejection.as_ref());
    let (Some(last), false) = (handled.last(), data_files.is_empty()) else {
        // Every row was skipped or rejected, and an empty append would still add a snapshot.
        return Ok(Written {
            committed_rows: 0,
            rejection,
        });
    };
    let committed_rows = data_files.iter().map(DataFile::record_count).sum();
    let recorded = held.map(|held| held.last_to_record(origin, table, LastMessage::from(last)));
    let properties = recorded
        .map(|last| snapshot_properties(origin, last))
        .unwrap_or_default();
    let committed = commit_data_files(table, catalog, data_files, properties, commit_retry).await;
    if let (Some(held), Some(recorded)) = (held, recorded) {
        match &committed {
            Ok(()) => held.committed(origin, table, recorded),
            Err(_) => held.forget(origin, table),
        }
    }
    committed?;
    Ok(Written {
        committed_rows,
        rejection,
    })
}

/// Writes the JSON payloads as Parquet data files under the table location,
/// laid out by the table's default partition spec. Nothing is committed here. With
/// `invalid_rows = "fail"`, the first message that the table cannot take ends the write, and the
/// data files hold the messages before it.
async fn write_data_files(
    origin: &BatchOrigin<'_>,
    messages: &[ConsumedMessage],
    table: &Table,
    messages_schema: Schema,
    invalid_rows: InvalidRows,
) -> Result<(Vec<DataFile>, Option<Rejection>), Error> {
    let location = DefaultLocationGenerator::new(table.metadata()).map_err(|err| {
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

    // Proto text holding JSON is the descriptor-less `proto_convert` fallback
    // and is written as the document it holds. Proto text that is not JSON holds
    // no document, so it is an invalid row like any other payload without one.
    let mut rejection = None;
    let mut documents: Vec<(u64, Cow<'_, simd_json::OwnedValue>)> =
        Vec::with_capacity(messages.len());
    for message in messages {
        if let Some(document) = message.payload.json_document() {
            documents.push((message.offset, document));
            continue;
        }
        let reason = format!(
            "The message at offset: {} ({origin}) holds no JSON document for table: {}, and the Iceberg sink requires schema = json, got {messages_schema}",
            message.offset,
            table.identifier()
        );
        if invalid_rows == InvalidRows::Skip {
            warn!("Skipping a row. {reason}");
            continue;
        }
        error!("{reason}");
        rejection = Some(Rejection {
            offset: message.offset,
            error: Error::InvalidRecordValue(reason),
        });
        break;
    }

    if documents.is_empty() {
        return Ok((Vec::new(), rejection));
    }

    let arrow_schema = Arc::new(
        schema_to_arrow_schema(&table.metadata().current_schema().clone()).map_err(|err| {
            let chain = format_error_chain(&err);
            error!(
                "Error while mapping records to Iceberg table with uuid: {}. Error {}",
                table.metadata().uuid(),
                chain
            );
            Error::SchemaMismatch(chain)
        })?,
    );

    // The table's partition spec does not change between attempts, so a writer that cannot be
    // built fails the same way every time.
    let mut writer = TableWriter::new(table, data_file_writer_builder).map_err(|err| {
        let chain = format_error_chain(&err);
        error!("Error while constructing data file writer: {}", chain);
        Error::SchemaMismatch(chain)
    })?;

    let write_result: Result<(), Error> = async {
        for chunk in documents.chunks(ROWS_PER_BATCH) {
            let (batch, rejected) = decode_rows(
                origin,
                table.identifier(),
                &arrow_schema,
                chunk,
                invalid_rows,
            )?;
            if let Some(batch) = batch {
                writer.write(batch).await?;
            }
            if rejected.is_some() {
                rejection = first_rejection(rejection.take(), rejected);
                break;
            }
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
    Ok((data_files, rejection))
}

/// Decodes the JSON documents into a record batch of `schema`. A row that does not match is
/// logged with its offset, origin and table. `invalid_rows = "skip"` leaves it out and decodes
/// the others. `invalid_rows = "fail"` decodes the rows before it and rejects it. The batch is
/// `None` when no row is left.
fn decode_rows(
    origin: &BatchOrigin<'_>,
    table: &TableIdent,
    schema: &SchemaRef,
    documents: &[(u64, Cow<'_, simd_json::OwnedValue>)],
    invalid_rows: InvalidRows,
) -> Result<(Option<RecordBatch>, Option<Rejection>), Error> {
    let rows: Vec<&simd_json::OwnedValue> = documents
        .iter()
        .map(|(_, document)| document.as_ref())
        .collect();
    if let Ok(batch) = read_batch(schema, &rows) {
        return Ok((batch, None));
    }
    // The batch read only says that some row does not match, so each row is read on its own.
    let mut valid = Vec::with_capacity(rows.len());
    let mut rejection = None;
    for (offset, document) in documents {
        let row_error = match read_batch(schema, &[document.as_ref()]) {
            Ok(_) => {
                valid.push(document.as_ref());
                continue;
            }
            Err(row_error) => row_error,
        };
        let reason = format!(
            "The row at offset: {offset} ({origin}) does not match the schema of table: {table}. {row_error}"
        );
        if invalid_rows == InvalidRows::Skip {
            warn!("Skipping a row. {reason}");
            continue;
        }
        error!("{reason}");
        rejection = Some(Rejection {
            offset: *offset,
            error: Error::InvalidRecordValue(reason),
        });
        break;
    }
    if valid.is_empty() {
        return Ok((None, rejection));
    }
    let batch = read_batch(schema, &valid).inspect_err(|error| {
        error!("Error while getting record batch: {error}");
    })?;
    Ok((batch, rejection))
}

/// The reader allocates its buffers for a full batch, so the batch size is the row count: a row
/// checked on its own is read without the buffers of a default-sized batch, and every row lands
/// in the one batch the reader yields.
fn read_batch(
    schema: &SchemaRef,
    rows: &[&simd_json::OwnedValue],
) -> Result<Option<RecordBatch>, Error> {
    let mut reader = ReaderBuilder::new(Arc::clone(schema))
        .with_batch_size(rows.len())
        .build(JsonArrowReader::new(rows))
        .map_err(|err| {
            // The reader is built from the table schema alone, so it fails the same way every time.
            error!("Error while building Iceberg reader from message payload: {err}");
            Error::SchemaMismatch(err.to_string())
        })?;
    reader
        .next()
        .transpose()
        .map_err(|err| Error::InvalidRecordValue(format_error_chain(&err)))
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
        let Self {
            partitioner,
            builder,
            writers,
        } = self;
        match partitioner {
            Partitioner::Unpartitioned(partition_key) => {
                write_partition(writers, builder, partition_key, batch).await
            }
            Partitioner::Partitioned(splitter) => {
                let partitioned_batches = splitter.split(&batch).map_err(|err| {
                    let chain = format_error_chain(&err);
                    error!("Error while splitting record batch by partition: {}", chain);
                    Error::InvalidRecordValue(chain)
                })?;
                for (partition_key, partition_batch) in partitioned_batches {
                    write_partition(writers, builder, &partition_key, partition_batch).await?;
                }
                Ok(())
            }
        }
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

/// The partition key is cloned only when a writer is created, not on every batch.
async fn write_partition(
    writers: &mut HashMap<Struct, ParquetDataFileWriter>,
    builder: &ParquetDataFileWriterBuilder,
    partition_key: &PartitionKey,
    batch: RecordBatch,
) -> Result<(), Error> {
    if let Some(writer) = writers.get_mut(partition_key.data()) {
        return writer.write(batch).await.map_err(write_failure);
    }
    let writer = builder
        .build(Some(partition_key.clone()))
        .await
        .map_err(write_failure)?;
    writers
        .entry(partition_key.data().clone())
        .or_insert(writer)
        .write(batch)
        .await
        .map_err(write_failure)
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
        origin: &BatchOrigin<'_>,
        messages_schema: Schema,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), crate::Error>;

    /// From now on, fails a batch instead of dropping a message it cannot route or a table that
    /// does not load, and appends only the messages a table does not hold yet when a batch is
    /// delivered again. Refuses when the configuration cannot keep that promise.
    fn enable_at_least_once(&mut self) -> Result<(), Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::Runtime as IcebergRuntime;
    use iceberg::spec::{
        FormatVersion, Literal, NestedField, PrimitiveLiteral, PrimitiveType,
        Schema as IcebergSchema, SortOrder, TableMetadataBuilder, Transform, Type,
        UnboundPartitionSpec,
    };
    use iggy_connector_sdk::Payload;
    use std::collections::HashMap;
    use std::sync::OnceLock;

    const REGION_FIELD_ID: i32 = 2;

    const ORIGIN: BatchOrigin<'static> = BatchOrigin {
        connector_id: 1,
        stream: "stream",
        topic: "topic",
        partition_id: 1,
    };

    // Table keeps only tokio handles, so the runtime must outlive every table.
    fn test_runtime() -> &'static tokio::runtime::Runtime {
        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("Failed to create runtime"))
    }

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
            .runtime(IcebergRuntime::new(test_runtime()))
            .build()
            .expect("Failed to build table")
    }

    fn json_payloads(rows: &[(i64, &str)]) -> Vec<Payload> {
        rows.iter()
            .map(|(id, region)| Payload::Json(simd_json::json!({ "id": *id, "region": *region })))
            .collect()
    }

    /// Messages carrying `payloads`, with offsets counting from 0.
    fn messages(payloads: &[Payload]) -> Vec<ConsumedMessage> {
        payloads
            .iter()
            .enumerate()
            .map(|(offset, payload)| ConsumedMessage {
                id: offset as u128,
                offset: offset as u64,
                checksum: 0,
                timestamp: 0,
                origin_timestamp: 0,
                headers: None,
                payload: payload.clone(),
            })
            .collect()
    }

    fn write_with(
        table: &Table,
        payloads: &[Payload],
        invalid_rows: InvalidRows,
    ) -> Result<(Vec<DataFile>, Option<Rejection>), Error> {
        test_runtime().block_on(write_data_files(
            &ORIGIN,
            &messages(payloads),
            table,
            Schema::Json,
            invalid_rows,
        ))
    }

    fn write(table: &Table, payloads: &[Payload]) -> Vec<DataFile> {
        let (data_files, rejection) =
            write_with(table, payloads, InvalidRows::Fail).expect("Failed to write data files");
        assert!(rejection.is_none(), "{rejection:?}");
        data_files
    }

    fn rows(data_files: &[DataFile]) -> u64 {
        data_files.iter().map(DataFile::record_count).sum()
    }

    /// Two rows that match the table and two that do not: a wrong type and a missing required
    /// field.
    fn rows_with_two_invalid() -> Vec<Payload> {
        vec![
            Payload::Json(simd_json::json!({ "id": 1, "region": "eu" })),
            Payload::Json(simd_json::json!({ "id": "not a number", "region": "us" })),
            Payload::Json(simd_json::json!({ "id": 3 })),
            Payload::Json(simd_json::json!({ "id": 4, "region": "us" })),
        ]
    }

    fn partition_region(data_file: &DataFile) -> String {
        match &data_file.partition()[0] {
            Some(Literal::Primitive(PrimitiveLiteral::String(region))) => region.clone(),
            other => panic!("Expected string partition value, got {other:?}"),
        }
    }

    #[test]
    fn given_proto_text_payloads_holding_json_should_write_them_as_rows() {
        let table = in_memory_table(UnboundPartitionSpec::builder().build());
        let payloads = vec![
            Payload::Json(simd_json::json!({ "id": 1, "region": "eu" })),
            Payload::Proto(r#"{"id": 2, "region": "us"}"#.to_owned()),
        ];

        let data_files = write(&table, &payloads);

        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].record_count(), 2);
    }

    #[test]
    fn given_only_proto_text_that_is_not_json_when_failing_rows_should_reject_its_message() {
        let table = in_memory_table(UnboundPartitionSpec::builder().build());
        let payloads = vec![Payload::Proto("id: 1".to_owned())];

        let (data_files, rejection) = test_runtime()
            .block_on(write_data_files(
                &ORIGIN,
                &messages(&payloads),
                &table,
                Schema::Proto,
                InvalidRows::Fail,
            ))
            .expect("a rejected message is not a failed write");

        assert!(data_files.is_empty());
        let rejection = rejection.expect("proto text that is not JSON has no document to write");
        assert_eq!(rejection.offset, 0);
        assert!(rejection.error.is_permanent(), "{}", rejection.error);
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

        let exists_after_delete = test_runtime().block_on(async {
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

    #[test]
    fn given_a_text_payload_when_skipping_rows_should_write_nothing_and_reject_nothing() {
        let table = in_memory_table(UnboundPartitionSpec::builder().build());
        let payloads = vec![Payload::Text("not json".to_string())];

        let (data_files, rejection) = test_runtime()
            .block_on(write_data_files(
                &ORIGIN,
                &messages(&payloads),
                &table,
                Schema::Text,
                InvalidRows::Skip,
            ))
            .expect("skipping every row is not an error");

        assert!(data_files.is_empty());
        assert!(rejection.is_none());
    }

    #[test]
    fn given_rows_that_do_not_match_when_invalid_rows_fail_should_write_the_rows_before_the_first()
    {
        let table = in_memory_table(UnboundPartitionSpec::builder().build());

        let (data_files, rejection) =
            write_with(&table, &rows_with_two_invalid(), InvalidRows::Fail)
                .expect("a rejected row is not a failed write");

        assert_eq!(rows(&data_files), 1);
        let rejection = rejection.expect("the first row that does not match is rejected");
        assert_eq!(rejection.offset, 1);
        assert!(
            matches!(rejection.error, Error::InvalidRecordValue(_)),
            "got {:?}",
            rejection.error
        );
    }

    #[test]
    fn given_rows_that_do_not_match_when_invalid_rows_skip_should_write_the_other_rows() {
        let table = in_memory_table(UnboundPartitionSpec::builder().build());

        let (data_files, rejection) =
            write_with(&table, &rows_with_two_invalid(), InvalidRows::Skip)
                .expect("the matching rows should be written");

        assert_eq!(rows(&data_files), 2);
        assert!(rejection.is_none());
    }

    #[test]
    fn given_only_rows_that_do_not_match_when_invalid_rows_skip_should_write_no_data_file() {
        let table = in_memory_table(UnboundPartitionSpec::builder().build());
        let payloads = vec![Payload::Json(
            simd_json::json!({ "id": "not a number", "region": "eu" }),
        )];

        let (data_files, rejection) = write_with(&table, &payloads, InvalidRows::Skip)
            .expect("skipping every row is not an error");

        assert!(data_files.is_empty());
        assert!(rejection.is_none());
    }

    #[test]
    fn given_an_invalid_row_in_a_later_batch_when_invalid_rows_skip_should_write_the_other_rows() {
        let table = in_memory_table(UnboundPartitionSpec::builder().build());
        let mut payloads = json_payloads(&[(1, "eu"); ROWS_PER_BATCH + 10]);
        payloads[ROWS_PER_BATCH + 5] = Payload::Json(simd_json::json!({ "id": "not a number" }));

        let (data_files, _) = write_with(&table, &payloads, InvalidRows::Skip)
            .expect("the matching rows should be written");

        assert_eq!(rows(&data_files), (ROWS_PER_BATCH + 9) as u64);
    }

    #[test]
    fn given_an_invalid_row_in_a_later_batch_when_invalid_rows_fail_should_write_the_rows_before_it()
     {
        let table = in_memory_table(UnboundPartitionSpec::builder().build());
        let mut payloads = json_payloads(&[(1, "eu"); ROWS_PER_BATCH + 10]);
        payloads[ROWS_PER_BATCH + 5] = Payload::Json(simd_json::json!({ "id": "not a number" }));

        let (data_files, rejection) = write_with(&table, &payloads, InvalidRows::Fail)
            .expect("a rejected row is not a failed write");

        assert_eq!(rows(&data_files), (ROWS_PER_BATCH + 5) as u64);
        assert_eq!(
            rejection.map(|rejection| rejection.offset),
            Some((ROWS_PER_BATCH + 5) as u64)
        );
    }
}
