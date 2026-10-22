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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arrow::{
    datatypes::{DataType, Schema},
    error::ArrowError,
    json::reader::infer_json_schema_from_iterator,
};
use fluss::{
    metadata::{DataField, RowType, SchemaBuilder, TableDescriptor, TablePath},
    record::from_arrow_field,
};
use iggy_connector_sdk::{ConsumedMessage, Payload};
use thiserror::Error;
use tracing::warn;

use crate::{
    config::TableConfig,
    writer::{self, TableWriter},
};

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Writer(#[from] writer::WriterError),
    #[error("Sampling has failed for messages. Can not create arrow schema because of [{reason}]")]
    SampleFailed { reason: String },
    #[error("Failed to build Fluss table descriptor for table {table_path}: {reason}")]
    BuildTableDescriptor {
        table_path: TablePath,
        reason: String,
    },
    #[error("Failed to infer schema for table: {table_path}: {reason}")]
    InferSchemaFailed {
        table_path: TablePath,
        reason: String,
    },
    #[error(
        "Failed to convert arrow Schema to fluss RowType for table {table_path} because of: {reason}"
    )]
    ArrowSchemaToFlussRowType {
        table_path: TablePath,
        reason: String,
    },
    #[error("Failed to convert Fluss schema to Arrow schema because: {reason}")]
    FlussToArrowSchemaFailed { reason: String },
    #[error("Fluss table {table_path} was not found right after it was created")]
    TableMissingAfterCreate { table_path: TablePath },
    #[error(
        "No column type could be inferred for table {table_path}, every sampled field was null or empty"
    )]
    NoInferableColumns { table_path: TablePath },
}

#[derive(Debug)]
pub(crate) struct SchemaEntry {
    pub(crate) schema: Arc<Schema>,
    pub(crate) table_descriptor: TableDescriptor,
}

#[derive(Debug, Default)]
pub(crate) struct SchemaCatalog {
    connector_id: u32,
    table_to_entry: Mutex<HashMap<TablePath, Arc<SchemaEntry>>>,
    table_config: HashMap<String, TableConfig>,
}

impl SchemaCatalog {
    pub(crate) fn new(connector_id: u32, table_config: HashMap<String, TableConfig>) -> Self {
        Self {
            connector_id,
            table_to_entry: Mutex::default(),
            table_config,
        }
    }

    pub(crate) fn get_schema_entry(&self, table: &TablePath) -> Option<Arc<SchemaEntry>> {
        self.table_to_entry
            .lock()
            .expect("schema catalog mutex poisoned")
            .get(table)
            .cloned()
    }

    pub(crate) fn load_from_table_descriptor(
        &self,
        table: &TablePath,
        table_descriptor: TableDescriptor,
    ) -> Result<Arc<SchemaEntry>, Error> {
        let row_type = table_descriptor.schema().row_type();

        let schema = fluss::record::to_arrow_schema(row_type).map_err(|error| {
            Error::FlussToArrowSchemaFailed {
                reason: error.to_string(),
            }
        })?;

        let entry = Arc::new(SchemaEntry {
            schema,
            table_descriptor,
        });
        self.store(table, Arc::clone(&entry));
        Ok(entry)
    }

    pub(crate) async fn load_from_table(
        &self,
        table: &TablePath,
        writer: &impl TableWriter,
    ) -> Result<Option<Arc<SchemaEntry>>, Error> {
        let descriptor = match writer.get_table_descriptor(table).await {
            Ok(descriptor) => descriptor,
            Err(writer::WriterError::TableNotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        let entry = self.load_from_table_descriptor(table, descriptor)?;
        Ok(Some(entry))
    }

    pub(crate) fn store(&self, table: &TablePath, entry: Arc<SchemaEntry>) {
        self.table_to_entry
            .lock()
            .expect("schema catalog mutex poisoned")
            .insert(table.to_owned(), entry);
    }

    pub(crate) async fn infer_and_create_table(
        &self,
        table: &TablePath,
        messages: &[ConsumedMessage],
        writer: &impl TableWriter,
    ) -> Result<Arc<SchemaEntry>, Error> {
        let table_descriptor = sample_and_infer_schema(messages)
            .and_then(|schema| drop_null_fields(self.connector_id, schema, table))
            .and_then(|schema| {
                table_descriptor_from_schema(
                    &schema,
                    table,
                    self.table_config.get(&table.to_string()),
                )
            })
            .map_err(|err| Error::InferSchemaFailed {
                table_path: table.to_owned(),
                reason: err.to_string(),
            })?;
        writer
            .create_table_if_not_exists(table, &table_descriptor)
            .await?;

        self.load_from_table(table, writer)
            .await?
            .ok_or_else(|| Error::TableMissingAfterCreate {
                table_path: table.to_owned(),
            })
    }
}

fn table_descriptor_from_schema(
    schema: &Schema,
    table: &TablePath,
    table_config: Option<&TableConfig>,
) -> Result<TableDescriptor, Error> {
    let row_type = schema_to_row_type(schema, table)?;

    let mut schema_builder =
        SchemaBuilder::new().with_row_type(&fluss::metadata::DataType::Row(row_type));

    if let Some(table_config) = table_config {
        schema_builder = table_config
            .enrich_schema_builder(schema_builder)
            .map_err(|error| Error::BuildTableDescriptor {
                table_path: table.clone(),
                reason: error.to_string(),
            })?;
    }

    let fluss_schema = schema_builder
        .build()
        .map_err(|error| Error::BuildTableDescriptor {
            table_path: table.clone(),
            reason: error.to_string(),
        })?;

    let mut builder = TableDescriptor::builder()
        .comment("Automatically created table")
        .schema(fluss_schema);

    if let Some(table_config) = table_config {
        builder = table_config.enrich_descriptor_builder(builder);
    }

    builder
        .build()
        .map_err(|error| Error::BuildTableDescriptor {
            table_path: table.clone(),
            reason: error.to_string(),
        })
}

fn schema_to_row_type(schema: &Schema, table: &TablePath) -> Result<RowType, Error> {
    schema
        .fields()
        .iter()
        .map(|field| {
            from_arrow_field(field)
                .map(|data_type| DataField::new(field.name().clone(), data_type, None))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(RowType::new)
        .map_err(|error| Error::ArrowSchemaToFlussRowType {
            table_path: table.clone(),
            reason: error.to_string(),
        })
}

fn drop_null_fields(connector_id: u32, schema: Schema, table: &TablePath) -> Result<Schema, Error> {
    let (null_fields, fields): (Vec<_>, Vec<_>) = schema
        .fields()
        .iter()
        .cloned()
        .partition(|field| contains_null_type(field.data_type()));

    if !null_fields.is_empty() {
        let names = null_fields
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        warn!(
            "Fluss sink ID {}: Leaving out columns [{names}] of table '{table}', every sampled value was null or empty",
            connector_id
        );
    }

    if fields.is_empty() {
        return Err(Error::NoInferableColumns {
            table_path: table.clone(),
        });
    }

    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn contains_null_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null => true,
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => contains_null_type(item.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_null_type(field.data_type())),
        _ => false,
    }
}

fn sample_and_infer_schema(messages: &[ConsumedMessage]) -> Result<Schema, Error> {
    let sampler = messages.iter().map(|message| {
        let id = message.id;
        match &message.payload {
            Payload::Json(value) => {
                simd_json::serde::from_refowned_value::<serde_json::Value>(value)
                    .map_err(|error| ArrowError::ExternalError(Box::new(error)))
            }
            _ => Err(ArrowError::ExternalError(Box::new(Error::SampleFailed {
                reason: format!("Only JSON Payload is supported for sampling and infer [{id}]"),
            }))),
        }
    });

    infer_json_schema_from_iterator(sampler).map_err(|error| Error::SampleFailed {
        reason: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        future::Future,
        sync::{Arc, Mutex},
    };

    use arrow::datatypes::{DataType, Field, Schema};
    use fluss::metadata::{TableDescriptor, TablePath};
    use iggy_connector_sdk::{ConsumedMessage, Schema as IggySchema};

    use super::{Error, SchemaCatalog, sample_and_infer_schema, table_descriptor_from_schema};
    use crate::{
        config::TableConfig,
        writer::{Stat, TableWriter, WriterError},
    };

    #[derive(Default)]
    struct RecordingWriter {
        created: Mutex<Vec<(TablePath, TableDescriptor)>>,
        existing: Mutex<HashMap<TablePath, TableDescriptor>>,
        fail_create: bool,
        fail_get: bool,
    }

    impl TableWriter for RecordingWriter {
        async fn write_to_table(
            &self,
            _table_path: &TablePath,
            _table_descriptor: &TableDescriptor,
            _batch: arrow::record_batch::RecordBatch,
        ) -> Result<Stat, WriterError> {
            unreachable!("schema catalog does not write rows")
        }

        async fn create_table_if_not_exists(
            &self,
            table_path: &TablePath,
            table_descriptor: &TableDescriptor,
        ) -> Result<(), WriterError> {
            if self.fail_create {
                return Err(WriterError::ConnectionNotInitialized);
            }
            self.created
                .lock()
                .expect("created tables lock should not be poisoned")
                .push((table_path.clone(), table_descriptor.clone()));
            self.existing
                .lock()
                .expect("existing tables lock should not be poisoned")
                .entry(table_path.clone())
                .or_insert_with(|| table_descriptor.clone());
            Ok(())
        }

        async fn get_table_descriptor(
            &self,
            table_path: &TablePath,
        ) -> Result<TableDescriptor, WriterError> {
            if self.fail_get {
                return Err(WriterError::ConnectionNotInitialized);
            }
            self.existing
                .lock()
                .expect("existing tables lock should not be poisoned")
                .get(table_path)
                .cloned()
                .ok_or_else(|| WriterError::TableNotFound {
                    table_path: table_path.clone(),
                })
        }
    }

    fn run_async<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Runtime::new()
            .expect("Tokio runtime should build")
            .block_on(future)
    }

    fn json_message(id: u128, value: &str) -> ConsumedMessage {
        ConsumedMessage {
            id,
            offset: 0,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: IggySchema::Json
                .try_into_payload(value.as_bytes().to_vec())
                .expect("Test JSON should decode"),
        }
    }

    #[test]
    fn given_table_descriptor_when_loading_should_store_descriptor_and_arrow_schema() {
        let table = TablePath::new("fluss", "orders");
        let arrow_schema = Schema::new(vec![Field::new("id", DataType::Utf8, false)]);
        let table_descriptor = table_descriptor_from_schema(&arrow_schema, &table, None)
            .expect("Table descriptor should build");
        let expected_table_descriptor = table_descriptor.clone();
        let catalog = SchemaCatalog::default();

        let loaded_entry = catalog
            .load_from_table_descriptor(&table, table_descriptor)
            .expect("Schema entry should load");
        let cached_entry = catalog
            .get_schema_entry(&table)
            .expect("Schema entry should be cached");

        assert_eq!(loaded_entry.table_descriptor, expected_table_descriptor);
        assert_eq!(loaded_entry.schema.fields().len(), 1);
        assert_eq!(loaded_entry.schema.field(0).name(), "id");
        assert!(Arc::ptr_eq(&loaded_entry, &cached_entry));
    }

    #[test]
    fn given_json_messages_when_sampling_should_infer_fields() {
        let messages = [
            json_message(1, r#"{"id":1,"name":"first"}"#),
            json_message(2, r#"{"id":2,"name":"second"}"#),
        ];

        let schema = sample_and_infer_schema(&messages).expect("JSON schema should infer");

        assert_eq!(
            schema.field_with_name("id").expect("ID field").data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema
                .field_with_name("name")
                .expect("Name field")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn given_non_json_message_when_sampling_should_return_sample_error() {
        let mut message = json_message(7, r#"{"id":7}"#);
        message.payload = iggy_connector_sdk::Payload::Text("not JSON".to_owned());

        let error = sample_and_infer_schema(&[message]).expect_err("Non-JSON should fail");

        assert!(matches!(error, Error::SampleFailed { reason } if reason.contains("[7]")));
    }

    #[test]
    fn given_unsupported_arrow_field_when_building_descriptor_should_return_conversion_error() {
        let table = TablePath::new("fluss", "orders");
        let schema = Schema::new(vec![Field::new("unsupported", DataType::Null, true)]);

        let error = table_descriptor_from_schema(&schema, &table, None)
            .expect_err("Null Arrow field should not convert to Fluss");

        assert!(
            matches!(error, Error::ArrowSchemaToFlussRowType { table_path, .. } if table_path == table)
        );
    }

    #[test]
    fn given_missing_table_when_loading_should_return_none() {
        let catalog = SchemaCatalog::default();
        let table = TablePath::new("fluss", "orders");

        let entry = run_async(catalog.load_from_table(&table, &RecordingWriter::default()))
            .expect("Missing table should not fail");

        assert!(entry.is_none());
        assert!(catalog.get_schema_entry(&table).is_none());
    }

    #[test]
    fn given_writer_failure_when_loading_should_preserve_error() {
        let catalog = SchemaCatalog::default();
        let table = TablePath::new("fluss", "orders");
        let writer = RecordingWriter {
            fail_get: true,
            ..RecordingWriter::default()
        };

        let error = run_async(catalog.load_from_table(&table, &writer))
            .expect_err("Writer failure should propagate");

        assert!(matches!(
            error,
            Error::Writer(WriterError::ConnectionNotInitialized)
        ));
    }

    #[test]
    fn given_json_messages_when_inferring_should_create_and_cache_table() {
        let catalog = SchemaCatalog::default();
        let table = TablePath::new("fluss", "orders");
        let writer = RecordingWriter::default();
        let messages = [json_message(1, r#"{"id":1,"name":"first"}"#)];

        let entry = run_async(catalog.infer_and_create_table(&table, &messages, &writer))
            .expect("Table should be inferred and created");
        let cached = catalog
            .get_schema_entry(&table)
            .expect("Table should be cached");
        let created = writer
            .created
            .lock()
            .expect("created tables lock should not be poisoned");

        assert!(Arc::ptr_eq(&entry, &cached));
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].0, table);
        assert_eq!(created[0].1, entry.table_descriptor);
        assert_eq!(entry.schema.fields().len(), 2);
    }

    #[test]
    fn given_table_created_concurrently_when_inferring_should_cache_existing_table_schema() {
        let catalog = SchemaCatalog::default();
        let table = TablePath::new("fluss", "orders");
        let winner_schema = Schema::new(vec![Field::new("id", DataType::Utf8, true)]);
        let winner_descriptor = table_descriptor_from_schema(&winner_schema, &table, None)
            .expect("Winner descriptor should build");
        let writer = RecordingWriter::default();
        writer
            .existing
            .lock()
            .expect("existing tables lock should not be poisoned")
            .insert(table.clone(), winner_descriptor.clone());
        let messages = [json_message(1, r#"{"id":1}"#)];

        let entry = run_async(catalog.infer_and_create_table(&table, &messages, &writer))
            .expect("Existing table should be loaded");
        let cached = catalog
            .get_schema_entry(&table)
            .expect("Table should be cached");

        assert_eq!(entry.table_descriptor, winner_descriptor);
        assert_eq!(cached.table_descriptor, winner_descriptor);
        assert_eq!(
            entry
                .schema
                .field_with_name("id")
                .expect("id field should exist")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn given_null_and_empty_fields_when_inferring_should_create_table_without_them() {
        let catalog = SchemaCatalog::default();
        let table = TablePath::new("fluss", "orders");
        let writer = RecordingWriter::default();
        let messages = [json_message(
            1,
            r#"{"id":1,"note":null,"tags":[],"meta":{"source":null}}"#,
        )];

        let entry = run_async(catalog.infer_and_create_table(&table, &messages, &writer))
            .expect("Table should be created from the remaining fields");
        let names = entry
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, ["id"]);
    }

    #[test]
    fn given_only_null_fields_when_inferring_should_not_create_or_cache_table() {
        let catalog = SchemaCatalog::default();
        let table = TablePath::new("fluss", "orders");
        let writer = RecordingWriter::default();
        let messages = [json_message(1, r#"{"note":null,"tags":[]}"#)];

        let error = run_async(catalog.infer_and_create_table(&table, &messages, &writer))
            .expect_err("Inference should fail without any typed field");

        assert!(matches!(
            error,
            Error::InferSchemaFailed { table_path, reason }
                if table_path == table && reason.contains("null or empty")
        ));
        assert!(
            writer
                .created
                .lock()
                .expect("created tables lock should not be poisoned")
                .is_empty()
        );
        assert!(catalog.get_schema_entry(&table).is_none());
    }

    #[test]
    fn given_non_json_message_when_inferring_should_not_create_or_cache_table() {
        let catalog = SchemaCatalog::default();
        let table = TablePath::new("fluss", "orders");
        let writer = RecordingWriter::default();
        let mut message = json_message(7, r#"{"id":7}"#);
        message.payload = iggy_connector_sdk::Payload::Text("not JSON".to_owned());

        let error = run_async(catalog.infer_and_create_table(&table, &[message], &writer))
            .expect_err("Inference should fail");

        assert!(
            matches!(error, Error::InferSchemaFailed { table_path, .. } if table_path == table)
        );
        assert!(
            writer
                .created
                .lock()
                .expect("created tables lock should not be poisoned")
                .is_empty()
        );
        assert!(catalog.get_schema_entry(&table).is_none());
    }

    #[test]
    fn given_invalid_primary_key_when_inferring_should_not_create_or_cache_table() {
        let table = TablePath::new("fluss", "orders");
        let catalog = SchemaCatalog::new(
            1,
            HashMap::from([(
                table.to_string(),
                TableConfig {
                    primary_keys: Some(vec!["missing".to_owned()]),
                    ..TableConfig::default()
                },
            )]),
        );
        let writer = RecordingWriter::default();
        let messages = [json_message(1, r#"{"id":1}"#)];

        let error = run_async(catalog.infer_and_create_table(&table, &messages, &writer))
            .expect_err("Invalid primary key should prevent table creation");

        assert!(
            matches!(error, Error::InferSchemaFailed { table_path, .. } if table_path == table)
        );
        assert!(
            writer
                .created
                .lock()
                .expect("created tables lock should not be poisoned")
                .is_empty()
        );
        assert!(catalog.get_schema_entry(&table).is_none());
    }

    #[test]
    fn given_create_failure_when_inferring_should_not_cache_table() {
        let catalog = SchemaCatalog::default();
        let table = TablePath::new("fluss", "orders");
        let writer = RecordingWriter {
            fail_create: true,
            ..RecordingWriter::default()
        };
        let messages = [json_message(1, r#"{"id":1}"#)];

        let error = run_async(catalog.infer_and_create_table(&table, &messages, &writer))
            .expect_err("Create failure should propagate");

        assert!(matches!(
            error,
            Error::Writer(WriterError::ConnectionNotInitialized)
        ));
        assert!(catalog.get_schema_entry(&table).is_none());
    }
}
