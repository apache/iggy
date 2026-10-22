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

use std::{collections::HashMap, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema, json::ReaderBuilder};
use fluss::metadata::TablePath;
use iggy_connector_sdk::{ConsumedMessage, Payload};
use simd_json::prelude::*;
use tracing::warn;

use super::Error;
use crate::{
    ResolvedFlussSinkConfig,
    router::RouteOutcome,
    schema_catalog::{SchemaCatalog, SchemaEntry},
    writer::{Stat, TableWriter},
};

#[derive(Debug)]
pub(crate) struct MultiTableRouter {
    schema_catalog: SchemaCatalog,
    auto_create_table: bool,
    route_key: String,
}

impl MultiTableRouter {
    pub(crate) fn new(config: &ResolvedFlussSinkConfig) -> Self {
        let schema_catalog = SchemaCatalog::new(config.tables.to_owned());

        Self {
            schema_catalog,
            auto_create_table: config.auto_create_table,
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
                None if self.auto_create_table => {
                    self.schema_catalog
                        .infer_and_create_table(table, messages, writer)
                        .await?
                }
                None => {
                    return Err(Error::FailedToResolveSchema {
                        table_path: table.to_owned(),
                        reason: "table was not found in the Fluss cluster and auto_create_table is disabled".to_owned(),
                    });
                }
            },
        })
    }

    pub(crate) async fn route(
        &self,
        writer: &impl TableWriter,
        messages: Vec<ConsumedMessage>,
    ) -> RouteOutcome {
        let mut result = Stat::default();
        let (partitioned_messages, stat) =
            partition_messages_by_route_key(messages, &self.route_key);
        result += stat;
        let mut last_error: Option<Error> = None;

        for (table, messages) in partitioned_messages {
            let maybe_entry = self
                .resolve_schema_entry(&table, writer, messages.as_slice())
                .await;
            let message_count = messages.len();

            let entry = match maybe_entry {
                Ok(entry) => entry,
                Err(error) => {
                    warn!(
                        "FlussSink: Skipping {message_count} messages for table '{table}', schema could not be resolved: {error}"
                    );
                    result.errors += message_count as u64;
                    last_error = Some(error);
                    continue;
                }
            };

            let (batch, stat) = match to_record_batch(&messages, Arc::clone(&entry.schema), &table)
            {
                Ok(converted) => converted,
                Err(error) => {
                    result.errors += message_count as u64;
                    warn!(
                        "FlussSink: Skipping {message_count} messages for table '{table}', record batch conversion failed: {error}"
                    );
                    last_error = Some(error);
                    continue;
                }
            };
            result += stat;

            if let Some(batch) = batch {
                let batch_len = batch.num_rows();
                match writer
                    .write_to_table(&table, &entry.table_descriptor, batch)
                    .await
                {
                    Ok(stat) => result += stat,
                    Err(error) => {
                        result.errors += batch_len as u64;
                        warn!(
                            "FlussSink: Skipping {batch_len} rows for table '{table}', write failed: {error}"
                        );
                        last_error = Some(error.into());
                    }
                }
            }
        }

        RouteOutcome {
            stat: result,
            last_error,
        }
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
                stat.errors += 1;
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

fn to_table_path(route: &str) -> Result<TablePath, Error> {
    let (database, table_name) =
        route
            .split_once('.')
            .ok_or_else(|| Error::FailedToExtractTablePath {
                reason: format!("Invalid table format, expected 'database.table', got {route}"),
            })?;

    for name in [database, table_name] {
        if let Some(reason) =
            TablePath::detect_invalid_name(name).or_else(|| TablePath::validate_prefix(name))
        {
            return Err(Error::FailedToExtractTablePath {
                reason: format!("Invalid name detected, for {name} : {reason}"),
            });
        }
    }

    Ok(TablePath::new(database, table_name))
}

fn extract_string_field<'a>(
    message: &'a ConsumedMessage,
    route_key: &str,
) -> Result<&'a str, Error> {
    let id = message.id;
    match &message.payload {
        Payload::Json(payload) => payload
            .as_object()
            .and_then(|obj| obj.get(route_key))
            .and_then(|value| value.as_str())
            .ok_or_else(|| Error::ExtractStringField {
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

fn partition_messages_by_route_key(
    messages: Vec<ConsumedMessage>,
    route_key: &str,
) -> (HashMap<TablePath, Vec<ConsumedMessage>>, Stat) {
    let mut stat = Stat::default();
    let mut by_route = HashMap::<String, Vec<ConsumedMessage>>::new();

    for message in messages {
        let route = match extract_string_field(&message, route_key) {
            Ok(route) => route,
            Err(error) => {
                stat.errors += 1;
                warn!(
                    "FlussSink: Skipping message [{}] because route key extraction failed: {error}",
                    message.id
                );
                continue;
            }
        };

        if let Some(group) = by_route.get_mut(route) {
            group.push(message);
        } else {
            let route = route.to_owned();
            by_route.insert(route, vec![message]);
        }
    }

    let mut by_table = HashMap::<TablePath, Vec<ConsumedMessage>>::with_capacity(by_route.len());
    for (route, messages) in by_route {
        match to_table_path(&route) {
            Ok(table_path) => by_table.entry(table_path).or_default().extend(messages),
            Err(error) => {
                stat.errors += messages.len() as u64;
                warn!(
                    "FlussSink: Skipping {} messages because route '{route}' is not a valid table path: {error}",
                    messages.len()
                );
            }
        }
    }

    (by_table, stat)
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
    use fluss::metadata::{TableDescriptor, TablePath};
    use iggy_connector_sdk::{ConsumedMessage, Payload, Schema as IggySchema};

    use super::{
        Error, MultiTableRouter, extract_string_field, partition_messages_by_route_key,
        to_record_batch, to_table_path,
    };
    use crate::{
        ResolvedFlussSinkConfig,
        config::TableConfig,
        router::RouteOutcome,
        writer::{Stat, TableWriter, WriterError},
    };

    struct TableWrite {
        table_path: TablePath,
        has_primary_key: bool,
        rows: usize,
    }

    #[derive(Default)]
    struct RecordingWriter {
        get_calls: AtomicUsize,
        created: Mutex<Vec<TablePath>>,
        existing: Mutex<HashMap<TablePath, TableDescriptor>>,
        writes: Mutex<Vec<TableWrite>>,
        fail_create: bool,
        failing_writes: Vec<TablePath>,
    }

    impl TableWriter for RecordingWriter {
        async fn write_to_table(
            &self,
            table_path: &TablePath,
            table_descriptor: &TableDescriptor,
            batch: RecordBatch,
        ) -> Result<Stat, WriterError> {
            if self.failing_writes.contains(table_path) {
                return Err(WriterError::ConnectionNotInitialized);
            }
            let rows = batch.num_rows();
            self.writes
                .lock()
                .expect("table writes lock should not be poisoned")
                .push(TableWrite {
                    table_path: table_path.clone(),
                    has_primary_key: table_descriptor.has_primary_key(),
                    rows,
                });
            let mut stat = Stat::default();
            stat.appended += rows as u64;
            Ok(stat)
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
                .push(table_path.clone());
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
            self.get_calls.fetch_add(1, Ordering::Relaxed);
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

    fn auto_create_config() -> ResolvedFlussSinkConfig {
        ResolvedFlussSinkConfig {
            auto_create_table: true,
            ..ResolvedFlussSinkConfig::default()
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
        let router = MultiTableRouter::new(&auto_create_config());
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

        let RouteOutcome { stat, last_error } = run_async(router.route(&writer, messages));
        let RouteOutcome {
            stat: second_stat,
            last_error: second_last_error,
        } = run_async(router.route(
            &writer,
            vec![json_message(6, r#"{"table":"fluss.orders","id":6}"#)],
        ));
        let created = writer
            .created
            .lock()
            .expect("created tables lock should not be poisoned");
        let writes = writer
            .writes
            .lock()
            .expect("table writes lock should not be poisoned");

        assert_eq!((stat.appended, stat.errors), (2, 3));
        assert!(last_error.is_none());
        assert_eq!((second_stat.appended, second_stat.errors), (1, 0));
        assert!(second_last_error.is_none());
        // One lookup before the create and one reload after it; the second batch hits the cache.
        assert_eq!(writer.get_calls.load(Ordering::Relaxed), 2);
        assert_eq!(created.as_slice(), &[TablePath::new("fluss", "orders")]);
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].table_path, TablePath::new("fluss", "orders"));
        assert!(!writes[0].has_primary_key);
        assert_eq!(writes[0].rows, 2);
        assert_eq!(writes[1].rows, 1);
    }

    #[test]
    fn given_multiple_tables_when_routing_should_accumulate_written_and_skipped_counts() {
        let router = MultiTableRouter::new(&auto_create_config());
        let writer = RecordingWriter::default();
        let messages = vec![
            json_message(1, r#"{"table":"fluss.orders","id":1}"#),
            json_message(2, r#"{"table":"fluss.events","id":2}"#),
            json_message(3, r#"{"table":"fluss.events","id":3}"#),
            json_message(4, r#"{"table":"bad path","id":4}"#),
        ];

        let RouteOutcome { stat, last_error } = run_async(router.route(&writer, messages));
        let writes = writer
            .writes
            .lock()
            .expect("table writes lock should not be poisoned");

        assert_eq!((stat.appended, stat.errors), (3, 1));
        assert!(last_error.is_none());
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

        let RouteOutcome { stat, last_error } = run_async(router.route(
            &writer,
            vec![json_message(1, r#"{"table":"fluss.orders","id":1}"#)],
        ));

        assert_eq!((stat.appended, stat.errors), (0, 1));
        assert!(matches!(
            last_error,
            Some(Error::FailedToResolveSchema { .. })
        ));
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
        let router = MultiTableRouter::new(&auto_create_config());
        let writer = RecordingWriter {
            fail_create: true,
            ..RecordingWriter::default()
        };

        let error = run_async(router.route(
            &writer,
            vec![json_message(1, r#"{"table":"fluss.orders","id":1}"#)],
        ))
        .last_error
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
        let router = MultiTableRouter::new(&auto_create_config());
        let writer = RecordingWriter {
            failing_writes: vec![TablePath::new("fluss", "orders")],
            ..RecordingWriter::default()
        };

        let error = run_async(router.route(
            &writer,
            vec![json_message(1, r#"{"table":"fluss.orders","id":1}"#)],
        ))
        .last_error
        .expect("Write failure should propagate");

        assert!(matches!(
            error,
            Error::WriterError(WriterError::ConnectionNotInitialized)
        ));
    }

    #[test]
    fn given_one_failing_table_when_routing_should_still_write_other_tables() {
        let router = MultiTableRouter::new(&auto_create_config());
        let writer = RecordingWriter {
            failing_writes: vec![TablePath::new("fluss", "orders")],
            ..RecordingWriter::default()
        };
        let messages = vec![
            json_message(1, r#"{"table":"fluss.orders","id":1}"#),
            json_message(2, r#"{"table":"fluss.events","id":2}"#),
            json_message(3, r#"{"table":"fluss.events","id":3}"#),
        ];

        let RouteOutcome { stat, last_error } = run_async(router.route(&writer, messages));
        let writes = writer
            .writes
            .lock()
            .expect("table writes lock should not be poisoned");

        assert_eq!((stat.appended, stat.errors), (2, 1));
        assert!(matches!(
            last_error,
            Some(Error::WriterError(WriterError::ConnectionNotInitialized))
        ));
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].table_path, TablePath::new("fluss", "events"));
        assert_eq!(writes[0].rows, 2);
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
            ..auto_create_config()
        };
        let router = MultiTableRouter::new(&config);
        let writer = RecordingWriter::default();

        let RouteOutcome { stat, last_error } = run_async(router.route(
            &writer,
            vec![json_message(1, r#"{"table":"fluss.orders","id":1}"#)],
        ));
        let writes = writer
            .writes
            .lock()
            .expect("table writes lock should not be poisoned");

        assert_eq!(stat.appended, 1);
        assert!(last_error.is_none());
        assert_eq!(writes.len(), 1);
        assert!(writes[0].has_primary_key);
    }
}
