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

use std::{collections::HashMap, sync::Arc, sync::Mutex};

use arrow::{array::RecordBatch, datatypes::Schema, json::ReaderBuilder};
use fluss::metadata::TablePath;
use iggy_connector_sdk::{ConsumedMessage, Payload};
use simd_json::prelude::*;
use tracing::warn;

use super::Error;
use crate::{
    ResolvedFlussSinkConfig,
    schema_catalog::{SchemaCatalog, SchemaEntry},
    writer::{Op, Stat, TableWriter},
};

#[derive(Debug)]
pub(crate) struct MultiTableRouter {
    schema_catalog: SchemaCatalog,
    auto_create_tables: bool,
    route_key: String,
}

impl MultiTableRouter {
    pub(crate) fn new(config: &ResolvedFlussSinkConfig) -> Self {
        let schema_catalog =
            SchemaCatalog::new(Mutex::new(HashMap::new()), config.tables.to_owned());

        Self {
            schema_catalog,
            auto_create_tables: config.auto_create_table,
            route_key: config.route_key.clone(),
        }
    }

    async fn resolve_schema_entry(
        &self,
        table: &TablePath,
        writer: &impl TableWriter,
        messages: &[ConsumedMessage],
    ) -> Result<Arc<SchemaEntry>, Error> {
        Ok(match self.schema_catalog.get_schema_entry(table) {
            Some(schema) => schema,
            None => match self.schema_catalog.load_from_table(table, writer).await? {
                Some(schema) => schema,
                None if self.auto_create_tables => {
                    self.schema_catalog
                        .infer_and_create_table(table, messages, writer)
                        .await?
                }
                None => {
                    return Err(Error::FailedToResolveSchema {
                        table_path: table.to_owned(),
                        reason: "schema could not found in the fluss cluster and auto_create_tables is disabled".to_owned(),
                    });
                }
            },
        })
    }

    pub(crate) async fn route(
        &self,
        writer: &impl TableWriter,
        messages: Vec<ConsumedMessage>,
    ) -> Result<Stat, Error> {
        let mut result = Stat::default();
        let (partitioned_messages, stat) =
            partition_messages_by_route_key(messages, &self.route_key);
        result = result.add(stat);
        for (table, messages) in partitioned_messages {
            let maybe_entry = self
                .resolve_schema_entry(&table, writer, messages.as_slice())
                .await;

            let entry = match maybe_entry {
                Ok(entry) => entry,
                Err(Error::FailedToResolveSchema {
                    table_path: table,
                    reason,
                }) => {
                    warn!(
                        "FlussSink: Skipping messages for table [{}] because schema could not be resolve because of [{reason}]",
                        table
                    );
                    result.inc_err_by(messages.len() as u64);
                    continue;
                }
                // Retryable errors
                Err(e) => return Err(e),
            };

            let (batch, stat) = to_record_batch(&messages, Arc::clone(&entry.schema), &table)?;
            result = result.add(stat);
            if let Some(batch) = batch {
                let op = if entry.table_descriptor.has_primary_key() {
                    Op::UpsertOrDelete
                } else {
                    Op::Append
                };
                let stat = writer
                    .write_to_table(&table, &entry.table_descriptor, op, batch)
                    .await?;
                result = result.add(stat);
            }
        }

        Ok(result)
    }
}

fn to_record_batch(
    messages: &[ConsumedMessage],
    schema: Arc<Schema>,
    table: &TablePath,
) -> Result<(Option<RecordBatch>, Stat), Error> {
    let mut decoder = ReaderBuilder::new(schema)
        .with_strict_mode(false)
        .with_ignore_type_conflicts(false)
        .build_decoder()
        .map_err(|err| Error::FailedToCreateRecordBatch {
            reason: err.to_string(),
        })?;

    let mut stat = Stat::default();
    let payloads: Vec<&simd_json::OwnedValue> = messages
        .iter()
        .filter_map(|message| match &message.payload {
            Payload::Json(payload) => Some(payload),
            _ => {
                warn!(
                    "FlussSink: Skipping message [{}] for table [{}] because only JSON payloads are supported",
                    message.id, table
                );
                stat.inc_err();
                None
            }
        })
        .collect();

    decoder
        .serialize(payloads.as_slice())
        .map_err(|err| Error::FailedToCreateRecordBatch {
            reason: err.to_string(),
        })?;

    let batch = decoder
        .flush()
        .map_err(|err| Error::FailedToCreateRecordBatch {
            reason: err.to_string(),
        })?;
    Ok((batch, stat))
}

fn to_table_path(table: &str) -> Result<TablePath, Error> {
    let parts = table.split('.').collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(Error::FailedToExtractTablePath {
            reason: format!("Invalid table format, expected 'database.table', got {table}"),
        });
    }
    let database = parts[0];
    let table_name = parts[1];

    if let Some(reason) = TablePath::detect_invalid_name(database) {
        return Err(Error::FailedToExtractTablePath {
            reason: format!("Invalid name detected, for {database} : {reason}"),
        });
    }

    if let Some(reason) = TablePath::detect_invalid_name(table_name) {
        return Err(Error::FailedToExtractTablePath {
            reason: format!("Invalid name detected, for {table_name} : {reason}"),
        });
    }

    if let Some(reason) = TablePath::validate_prefix(database) {
        return Err(Error::FailedToExtractTablePath {
            reason: format!("Invalid name detected, for {database} : {reason}"),
        });
    }

    if let Some(reason) = TablePath::validate_prefix(table_name) {
        return Err(Error::FailedToExtractTablePath {
            reason: format!("Invalid name detected, for {table_name} : {reason}"),
        });
    }

    Ok(TablePath::new(database, table_name))
}

fn extract_string_field(message: &ConsumedMessage, route_key: &str) -> Result<String, Error> {
    let id = message.id;
    match &message.payload {
        Payload::Json(payload) => payload
            .as_object()
            .and_then(|obj| obj.get(route_key))
            .and_then(|value| value.as_str())
            .map(str::to_owned)
            .ok_or(Error::ExtractStringField {
                id,
                key: route_key.to_owned(),
                reason: "Value not found".to_owned(),
            }),
        _ => Err(Error::ExtractStringField {
            id,
            key: route_key.to_owned(),
            reason: "Payload type is not supported, only Json type is supported.".to_owned(),
        }),
    }
}

fn partition_messages_by_route_key<I>(
    messages: I,
    route_key: &str,
) -> (HashMap<TablePath, Vec<ConsumedMessage>>, Stat)
where
    I: IntoIterator<Item = ConsumedMessage>,
{
    let mut table_to_message = HashMap::<TablePath, Vec<ConsumedMessage>>::new();
    let mut extract = Stat::default();
    let mut path_convert = Stat::default();

    messages
        .into_iter()
        .filter_map(|message| match extract_string_field(&message, route_key) {
            Ok(route_target) => Some((route_target, message)),
            Err(error) => {
                extract.inc_err();
                warn!(
                    "FlussSink: Skipping message [{}] because route key extraction failed: {}",
                    message.id, error
                );
                None
            }
        })
        .filter_map(|(table, message)| match to_table_path(&table) {
            Ok(table_path) => Some((table_path, message)),
            Err(error) => {
                path_convert.inc_err();
                warn!(
                    "FlussSink: Skipping message [{}] because table path extraction failed: {}",
                    message.id, error
                );
                None
            }
        })
        .for_each(|(table_path, message)| {
            table_to_message
                .entry(table_path)
                .or_default()
                .push(message)
        });

    (table_to_message, extract.add(path_convert))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        future::Future,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use arrow::{
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use fluss::{
        client::FlussTable,
        metadata::{TableDescriptor, TablePath},
    };
    use iggy_connector_sdk::{ConsumedMessage, Payload, Schema as IggySchema};

    use super::{
        Error, MultiTableRouter, extract_string_field, partition_messages_by_route_key,
        to_record_batch, to_table_path,
    };
    use crate::{
        ResolvedFlussSinkConfig,
        config::TableConfig,
        writer::{Op, Stat, TableWriter, WriterError},
    };

    struct TableWrite {
        table_path: TablePath,
        op: Op,
        rows: usize,
    }

    #[derive(Default)]
    struct RecordingWriter {
        get_calls: AtomicUsize,
        created: Mutex<Vec<TablePath>>,
        writes: Mutex<Vec<TableWrite>>,
        fail_create: bool,
        fail_write: bool,
    }

    impl TableWriter for RecordingWriter {
        async fn write_to_table(
            &self,
            table_path: &TablePath,
            _table_descriptor: &TableDescriptor,
            op: Op,
            batch: RecordBatch,
        ) -> Result<Stat, WriterError> {
            if self.fail_write {
                return Err(WriterError::ConnectionNotInitialized);
            }
            let rows = batch.num_rows();
            self.writes
                .lock()
                .expect("table writes lock should not be poisoned")
                .push(TableWrite {
                    table_path: table_path.clone(),
                    op,
                    rows,
                });
            let mut stat = Stat::default();
            stat.inc_appended_by(rows as u64);
            Ok(stat)
        }

        async fn create_table_if_not_exists(
            &self,
            table_path: &TablePath,
            _table_descriptor: &TableDescriptor,
        ) -> Result<(), WriterError> {
            if self.fail_create {
                return Err(WriterError::ConnectionNotInitialized);
            }
            self.created
                .lock()
                .expect("created tables lock should not be poisoned")
                .push(table_path.clone());
            Ok(())
        }

        async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>, WriterError> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            Err(WriterError::TableNotFound {
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
    fn given_valid_and_invalid_table_paths_when_parsing_should_validate_both_segments() {
        assert_eq!(
            to_table_path("fluss.orders").expect("Qualified table should parse"),
            TablePath::new("fluss", "orders")
        );
        for table in [
            "orders",
            "fluss.orders.extra",
            ".orders",
            "fluss.",
            "bad!.orders",
            "fluss.bad!",
            "__internal.orders",
            "fluss.__internal",
        ] {
            assert!(
                matches!(
                    to_table_path(table),
                    Err(Error::FailedToExtractTablePath { .. })
                ),
                "Expected invalid path: {table}"
            );
        }
    }

    #[test]
    fn given_missing_or_non_string_route_when_extracting_should_return_message_context() {
        let valid = json_message(1, r#"{"table":"fluss.orders"}"#);
        assert_eq!(
            extract_string_field(&valid, "table").expect("Route should extract"),
            "fluss.orders"
        );

        for message in [
            json_message(2, r#"{"other":"fluss.orders"}"#),
            json_message(3, r#"{"table":42}"#),
            json_message(4, r#"["fluss.orders"]"#),
        ] {
            let id = message.id;
            assert!(matches!(
                extract_string_field(&message, "table"),
                Err(Error::ExtractStringField { id: error_id, key, .. })
                    if error_id == id && key == "table"
            ));
        }
    }

    #[test]
    fn given_text_payload_when_extracting_route_should_return_message_context() {
        let message = ConsumedMessage {
            payload: Payload::Text("fluss.orders".to_owned()),
            ..json_message(5, r#"{"table":"fluss.orders"}"#)
        };

        assert!(matches!(
            extract_string_field(&message, "table"),
            Err(Error::ExtractStringField { id, key, .. })
                if id == message.id && key == "table"
        ));
    }

    #[test]
    fn given_mixed_routes_when_partitioning_should_group_valid_and_count_rejections() {
        let messages = vec![
            json_message(1, r#"{"table":"fluss.orders","id":1}"#),
            json_message(2, r#"{"table":"fluss.events","id":2}"#),
            json_message(3, r#"{"table":"fluss.orders","id":3}"#),
            json_message(4, r#"{"table":"invalid","id":4}"#),
            json_message(5, r#"{"id":5}"#),
        ];

        let (groups, stat) = partition_messages_by_route_key(messages, "table");

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[&TablePath::new("fluss", "orders")].len(), 2);
        assert_eq!(groups[&TablePath::new("fluss", "events")].len(), 1);
        assert_eq!(stat.errors, 2);
    }

    #[test]
    fn given_json_and_non_json_payloads_when_decoding_should_count_skipped_messages() {
        let table = TablePath::new("fluss", "orders");
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let mut non_json = json_message(2, r#"{"id":2}"#);
        non_json.payload = Payload::Text("not JSON".to_owned());

        let (batch, stat) = to_record_batch(
            &[json_message(1, r#"{"id":1}"#), non_json],
            std::sync::Arc::clone(&schema),
            &table,
        )
        .expect("Valid JSON should decode");
        let mut non_json = json_message(3, r#"{"id":3}"#);
        non_json.payload = Payload::Text("not JSON".to_owned());
        let (empty_batch, empty_stat) =
            to_record_batch(&[non_json], schema, &table).expect("Non-JSON should be skipped");

        assert_eq!(batch.expect("JSON batch should exist").num_rows(), 1);
        assert_eq!(stat.errors, 1);
        assert!(empty_batch.is_none());
        assert_eq!(empty_stat.errors, 1);
    }

    #[test]
    fn given_invalid_json_field_type_when_decoding_should_return_batch_error() {
        let table = TablePath::new("fluss", "orders");
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));

        let error = to_record_batch(
            &[json_message(1, r#"{"id":"not an integer"}"#)],
            schema,
            &table,
        )
        .err()
        .expect("Type conflict should fail");

        assert!(matches!(error, Error::FailedToCreateRecordBatch { .. }));
    }

    #[test]
    fn given_valid_and_invalid_messages_when_routing_should_cache_schema_and_count_results() {
        let router = MultiTableRouter::new(&ResolvedFlussSinkConfig::default());
        let writer = RecordingWriter::default();
        let mut non_json = json_message(5, r#"{"table":"fluss.orders","id":5}"#);
        non_json.payload = Payload::Text("not JSON".to_owned());
        let messages = vec![
            json_message(1, r#"{"table":"fluss.orders","id":1}"#),
            json_message(2, r#"{"table":"fluss.orders","id":2}"#),
            json_message(3, r#"{"table":"invalid","id":3}"#),
            json_message(4, r#"{"id":4}"#),
            non_json,
        ];

        let stat = run_async(router.route(&writer, messages)).expect("Valid messages should route");
        let second_stat = run_async(router.route(
            &writer,
            vec![json_message(6, r#"{"table":"fluss.orders","id":6}"#)],
        ))
        .expect("Cached table should route");
        let created = writer
            .created
            .lock()
            .expect("created tables lock should not be poisoned");
        let writes = writer
            .writes
            .lock()
            .expect("table writes lock should not be poisoned");

        assert_eq!((stat.appended, stat.errors), (2, 3));
        assert_eq!((second_stat.appended, second_stat.errors), (1, 0));
        assert_eq!(writer.get_calls.load(Ordering::Relaxed), 1);
        assert_eq!(created.as_slice(), &[TablePath::new("fluss", "orders")]);
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].table_path, TablePath::new("fluss", "orders"));
        assert_eq!(writes[0].op, Op::Append);
        assert_eq!(writes[0].rows, 2);
        assert_eq!(writes[1].rows, 1);
    }

    #[test]
    fn given_multiple_tables_when_routing_should_accumulate_written_and_skipped_counts() {
        let router = MultiTableRouter::new(&ResolvedFlussSinkConfig::default());
        let writer = RecordingWriter::default();
        let messages = vec![
            json_message(1, r#"{"table":"fluss.orders","id":1}"#),
            json_message(2, r#"{"table":"fluss.events","id":2}"#),
            json_message(3, r#"{"table":"fluss.events","id":3}"#),
            json_message(4, r#"{"table":"bad path","id":4}"#),
        ];

        let stat = run_async(router.route(&writer, messages)).expect("Valid tables should route");
        let writes = writer
            .writes
            .lock()
            .expect("table writes lock should not be poisoned");

        assert_eq!((stat.appended, stat.errors), (3, 1));
        assert_eq!(writes.len(), 2);
        assert_eq!(writes.iter().map(|write| write.rows).sum::<usize>(), 3);
    }

    #[test]
    fn given_auto_create_disabled_when_routing_should_skip_missing_table() {
        let config = ResolvedFlussSinkConfig {
            auto_create_table: false,
            ..ResolvedFlussSinkConfig::default()
        };
        let router = MultiTableRouter::new(&config);
        let writer = RecordingWriter::default();

        let stat = run_async(router.route(
            &writer,
            vec![json_message(1, r#"{"table":"fluss.orders","id":1}"#)],
        ))
        .expect("Missing table should be skipped");

        assert_eq!((stat.appended, stat.errors), (0, 1));
        assert!(
            writer
                .created
                .lock()
                .expect("created tables lock should not be poisoned")
                .is_empty()
        );
        assert!(
            writer
                .writes
                .lock()
                .expect("table writes lock should not be poisoned")
                .is_empty()
        );
    }

    #[test]
    fn given_create_failure_when_routing_should_return_catalog_error() {
        let router = MultiTableRouter::new(&ResolvedFlussSinkConfig::default());
        let writer = RecordingWriter {
            fail_create: true,
            ..RecordingWriter::default()
        };

        let error = run_async(router.route(
            &writer,
            vec![json_message(1, r#"{"table":"fluss.orders","id":1}"#)],
        ))
        .err()
        .expect("Create failure should propagate");

        assert!(matches!(error, Error::SchemaCatalog(_)));
        assert!(
            writer
                .writes
                .lock()
                .expect("table writes lock should not be poisoned")
                .is_empty()
        );
    }

    #[test]
    fn given_write_failure_when_routing_should_return_writer_error() {
        let router = MultiTableRouter::new(&ResolvedFlussSinkConfig::default());
        let writer = RecordingWriter {
            fail_write: true,
            ..RecordingWriter::default()
        };

        let error = run_async(router.route(
            &writer,
            vec![json_message(1, r#"{"table":"fluss.orders","id":1}"#)],
        ))
        .err()
        .expect("Write failure should propagate");

        assert!(matches!(
            error,
            Error::WriterError(WriterError::ConnectionNotInitialized)
        ));
    }

    #[test]
    fn given_primary_key_table_when_routing_should_choose_upsert_operation() {
        let config = ResolvedFlussSinkConfig {
            tables: HashMap::from([(
                "fluss.orders".to_owned(),
                TableConfig {
                    primary_keys: Some(vec!["id".to_owned()]),
                    ..TableConfig::default()
                },
            )]),
            ..ResolvedFlussSinkConfig::default()
        };
        let router = MultiTableRouter::new(&config);
        let writer = RecordingWriter::default();

        let stat = run_async(router.route(
            &writer,
            vec![json_message(1, r#"{"table":"fluss.orders","id":1}"#)],
        ))
        .expect("Primary-key message should route");
        let writes = writer
            .writes
            .lock()
            .expect("table writes lock should not be poisoned");

        assert_eq!(stat.appended, 1);
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].op, Op::UpsertOrDelete);
    }
}
