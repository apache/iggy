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

use fluss::metadata::{TableDescriptor, TablePath};
use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, TopicMetadata};
use tracing::error;

use super::Error;
use crate::{
    PayloadFormat, ResolvedFlussSinkConfig,
    config::TableConfig,
    router::RouteOutcome,
    schema_catalog::{SchemaCatalog, SchemaEntry},
    static_schema::{
        self, ColumnKind, RowContext, StaticTableBatchBuilder, StaticTableDescriptorBuilder,
        StaticTableLayout, create_extra_columns,
    },
    writer::{Stat, TableWriter},
};

#[derive(Debug)]
pub(crate) struct StaticTableRouter {
    connector_id: u32,
    table_path: TablePath,
    auto_create_table: bool,
    extra_columns: Vec<ColumnKind>,
    payload_format: PayloadFormat,
    table_config: Option<TableConfig>,
    layout: Option<StaticTableLayout>,
    entry: Option<Arc<SchemaEntry>>,
}

impl StaticTableRouter {
    pub(crate) fn new(connector_id: u32, config: &ResolvedFlussSinkConfig) -> Self {
        let table_path =
            TablePath::new(config.target_database.clone(), config.target_table.clone());
        let table_config = config.tables.get(&table_path.to_string()).cloned();
        Self {
            connector_id,
            table_path,
            auto_create_table: config.auto_create_table,
            extra_columns: create_extra_columns(
                config.include_metadata,
                config.include_checksum,
                config.include_origin_timestamp,
            ),
            payload_format: config.payload_format,
            table_config,
            layout: None,
            entry: None,
        }
    }

    pub(crate) async fn init(&mut self, writer: &impl TableWriter) -> Result<(), Error> {
        let schema_catalog = SchemaCatalog::default();

        let entry = match schema_catalog
            .load_from_table(&self.table_path, writer)
            .await?
        {
            Some(entry) => entry,
            None if self.auto_create_table => {
                let table_descriptor =
                    self.build_table_descriptor().map_err(|err| Error::Init {
                        reason: err.to_string(),
                    })?;
                writer
                    .create_table_if_not_exists(&self.table_path, &table_descriptor)
                    .await?;
                // Another sink can win the create with a different schema, so use what Fluss has.
                schema_catalog
                    .load_from_table(&self.table_path, writer)
                    .await?
                    .ok_or_else(|| Error::Init {
                        reason: format!(
                            "Table {} was not found right after it was created",
                            self.table_path
                        ),
                    })?
            }
            None => {
                return Err(Error::Init {
                    reason: format!(
                        "Table {} was not found and auto_create_table is disabled",
                        self.table_path
                    ),
                });
            }
        };

        let layout = StaticTableLayout::new(Arc::clone(&entry.schema), self.payload_format)
            .map_err(|error| Error::Init {
                reason: format!(
                    "Table {} does not match the static table layout: {error}",
                    self.table_path
                ),
            })?;
        self.layout = Some(layout);
        self.entry = Some(entry);
        Ok(())
    }

    pub(crate) async fn route(
        &self,
        writer: &impl TableWriter,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> RouteOutcome {
        let context = RowContext {
            topic: &topic_metadata.topic,
            stream: &topic_metadata.stream,
            partition_id: messages_metadata.partition_id,
        };

        let mut last_error: Option<Error> = None;
        let message_count = messages.len();

        let mut stat = Stat::default();
        let (Some(schema_entry), Some(layout)) = (self.entry.as_ref(), self.layout.as_ref()) else {
            stat.errors += message_count as u64;
            return RouteOutcome {
                stat,
                last_error: Some(Error::Init {
                    reason: format!("Router for table {} is not initialized", self.table_path),
                }),
            };
        };

        let mut builder = StaticTableBatchBuilder::new(self.payload_format, context, layout);
        for message in messages {
            if let Err(error) = builder.append(message) {
                error!(
                    "Fluss sink ID {}: Can not add record to batch builder, skipping message because of error: [{error}]",
                    self.connector_id
                );
                stat.errors += 1;
                last_error = Some(Error::FailedToAddToRecordBatch {
                    reason: error.to_string(),
                });
            }
        }

        let record_batch = match builder.finish() {
            Ok(batch) => batch,
            Err(error) => {
                stat.errors += message_count as u64 - stat.errors;
                return RouteOutcome {
                    stat,
                    last_error: Some(Error::FailedToCreateRecordBatch {
                        reason: error.to_string(),
                    }),
                };
            }
        };

        let batch_rows = record_batch.num_rows();
        if batch_rows == 0 {
            return RouteOutcome { stat, last_error };
        }

        let write_stat = match writer
            .write_to_table(
                &self.table_path,
                &schema_entry.table_descriptor,
                record_batch,
            )
            .await
        {
            Ok(write_stat) => write_stat,
            Err(error) => {
                stat.errors += batch_rows as u64;
                return RouteOutcome {
                    stat,
                    last_error: Some(error.into()),
                };
            }
        };

        stat += write_stat;
        RouteOutcome { stat, last_error }
    }

    fn build_table_descriptor(&self) -> Result<TableDescriptor, static_schema::Error> {
        let mut builder = StaticTableDescriptorBuilder::new()
            .with_extra_columns(&self.extra_columns)
            .with_payload_format(self.payload_format);

        if let Some(table_config) = &self.table_config {
            builder = builder.with_table_config(table_config);
        }

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use arrow::{
        array::{Int64Array, StringArray},
        record_batch::RecordBatch,
    };
    use fluss::metadata::{TableDescriptor, TablePath};
    use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, Payload, Schema, TopicMetadata};

    use super::{Error as RouterError, StaticTableRouter};
    use crate::{
        PayloadFormat, ResolvedFlussSinkConfig,
        router::RouteOutcome,
        writer::{Stat, TableWriter, WriterError},
    };

    const MESSAGE_TIMESTAMP: u64 = 1_700_000_000_123_456;
    const ORIGIN_TIMESTAMP: u64 = 1_700_000_000_120_789;

    struct TableCreation {
        table_path: TablePath,
        table_descriptor: TableDescriptor,
    }

    struct TableWrite {
        table_path: TablePath,
        batch: RecordBatch,
        has_primary_key: bool,
    }

    #[derive(Default)]
    struct RecordingWriter {
        table_creations: Mutex<Vec<TableCreation>>,
        table_writes: Mutex<Vec<TableWrite>>,
        fail_table_creation: bool,
        fail_table_write: bool,
        skipped_rows: u64,
    }

    impl TableWriter for RecordingWriter {
        async fn write_to_table(
            &self,
            table_path: &TablePath,
            table_descriptor: &TableDescriptor,
            batch: RecordBatch,
        ) -> Result<Stat, WriterError> {
            if self.fail_table_write {
                return Err(WriterError::ConnectionNotInitialized);
            }

            let row_count = batch.num_rows();
            self.table_writes
                .lock()
                .expect("Table writes lock should not be poisoned")
                .push(TableWrite {
                    table_path: table_path.clone(),
                    batch,
                    has_primary_key: table_descriptor.has_primary_key(),
                });
            let mut stat = Stat::default();
            stat.appended += row_count as u64 - self.skipped_rows;
            stat.errors += self.skipped_rows;
            Ok(stat)
        }

        async fn create_table_if_not_exists(
            &self,
            table_path: &TablePath,
            table_descriptor: &TableDescriptor,
        ) -> Result<(), WriterError> {
            if self.fail_table_creation {
                return Err(WriterError::ConnectionNotInitialized);
            }

            self.table_creations
                .lock()
                .expect("Table creations lock should not be poisoned")
                .push(TableCreation {
                    table_path: table_path.clone(),
                    table_descriptor: table_descriptor.clone(),
                });
            Ok(())
        }

        async fn get_table_descriptor(
            &self,
            table_path: &TablePath,
        ) -> Result<TableDescriptor, WriterError> {
            self.table_creations
                .lock()
                .expect("Table creations lock should not be poisoned")
                .iter()
                .find(|creation| &creation.table_path == table_path)
                .map(|creation| creation.table_descriptor.clone())
                .ok_or_else(|| WriterError::TableNotFound {
                    table_path: table_path.to_owned(),
                })
        }
    }

    fn test_config() -> ResolvedFlussSinkConfig {
        ResolvedFlussSinkConfig {
            target_database: "analytics".to_string(),
            target_table: "events".to_string(),
            payload_format: PayloadFormat::Text,
            auto_create_table: true,
            ..ResolvedFlussSinkConfig::default()
        }
    }

    fn test_topic_metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "orders".to_string(),
            topic: "created".to_string(),
        }
    }

    fn test_messages_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 7,
            current_offset: 202,
            schema: Schema::Text,
        }
    }

    fn test_message(id: u128, payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id,
            offset: id as u64,
            checksum: id as u64 + 100,
            timestamp: MESSAGE_TIMESTAMP,
            origin_timestamp: ORIGIN_TIMESTAMP,
            headers: None,
            payload,
        }
    }

    fn run_async<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Runtime::new()
            .expect("Tokio runtime should build")
            .block_on(future)
    }

    fn assert_stats(
        stats: &Stat,
        expected_messages_processed: u64,
        expected_insertion_errors: u64,
    ) {
        assert_eq!(stats.appended, expected_messages_processed);
        assert_eq!(stats.errors, expected_insertion_errors);
    }

    #[test]
    fn given_target_config_when_creating_router_should_preserve_routing_settings() {
        let config = ResolvedFlussSinkConfig {
            auto_create_table: false,
            ..test_config()
        };

        let router = StaticTableRouter::new(1, &config);

        assert_eq!(router.table_path, TablePath::new("analytics", "events"));
        assert!(!router.auto_create_table);
    }

    #[test]
    fn given_auto_create_enabled_when_initializing_should_create_expected_table() {
        let config = test_config();
        let mut router = StaticTableRouter::new(1, &config);
        let writer = RecordingWriter::default();

        run_async(router.init(&writer)).expect("Router should initialize");

        let table_creations = writer
            .table_creations
            .lock()
            .expect("Table creations lock should not be poisoned");
        assert_eq!(table_creations.len(), 1);
        assert_eq!(table_creations[0].table_path, router.table_path);
        assert_eq!(
            table_creations[0].table_descriptor,
            router
                .build_table_descriptor()
                .expect("Expected table descriptor should build")
        );
    }

    #[test]
    fn given_missing_table_and_auto_create_disabled_when_initializing_should_return_init_error() {
        let config = ResolvedFlussSinkConfig {
            auto_create_table: false,
            ..test_config()
        };
        let mut router = StaticTableRouter::new(1, &config);
        let writer = RecordingWriter::default();

        let error = run_async(router.init(&writer)).expect_err("Router initialization should fail");

        assert!(matches!(error, RouterError::Init { .. }));
        assert!(
            writer
                .table_creations
                .lock()
                .expect("Table creations lock should not be poisoned")
                .is_empty()
        );
    }

    #[test]
    fn given_table_creation_failure_when_initializing_should_return_writer_error() {
        let mut router = StaticTableRouter::new(1, &test_config());
        let writer = RecordingWriter {
            fail_table_creation: true,
            ..RecordingWriter::default()
        };

        let error = run_async(router.init(&writer)).expect_err("Table creation should fail");

        assert!(matches!(
            error,
            RouterError::WriterError(WriterError::ConnectionNotInitialized)
        ));
    }

    #[test]
    fn given_valid_messages_when_routing_should_write_batch_with_context_and_return_success_stats()
    {
        let mut router = StaticTableRouter::new(1, &test_config());
        let writer = RecordingWriter::default();
        let messages = vec![
            test_message(101, Payload::Text("first".to_string())),
            test_message(102, Payload::Text("second".to_string())),
        ];

        run_async(router.init(&writer)).expect("Router should initialize");
        let RouteOutcome {
            stat: stats,
            last_error,
        } = run_async(router.route(
            &writer,
            &test_topic_metadata(),
            test_messages_metadata(),
            messages,
        ));

        assert_stats(&stats, 2, 0);
        assert!(last_error.is_none());
        let table_writes = writer
            .table_writes
            .lock()
            .expect("Table writes lock should not be poisoned");
        assert_eq!(table_writes.len(), 1);
        assert_eq!(table_writes[0].table_path, router.table_path);
        assert_eq!(table_writes[0].batch.num_rows(), 2);

        let streams = table_writes[0]
            .batch
            .column_by_name("iggy_stream")
            .expect("Stream column should exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Stream column should contain strings");
        let topics = table_writes[0]
            .batch
            .column_by_name("iggy_topic")
            .expect("Topic column should exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Topic column should contain strings");
        let partition_ids = table_writes[0]
            .batch
            .column_by_name("iggy_partition_id")
            .expect("Partition ID column should exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Partition ID column should contain integers");

        assert_eq!(streams.iter().collect::<Vec<_>>(), vec![Some("orders"); 2]);
        assert_eq!(topics.iter().collect::<Vec<_>>(), vec![Some("created"); 2]);
        assert_eq!(partition_ids.iter().collect::<Vec<_>>(), vec![Some(7); 2]);
    }

    #[test]
    fn given_only_invalid_messages_when_routing_should_not_write_empty_batch() {
        let mut router = StaticTableRouter::new(1, &test_config());
        let writer = RecordingWriter::default();
        let messages = vec![
            test_message(101, Payload::Raw(vec![0xff])),
            test_message(102, Payload::Raw(vec![0xfe])),
        ];

        run_async(router.init(&writer)).expect("Router should initialize");
        let RouteOutcome {
            stat: stats,
            last_error,
        } = run_async(router.route(
            &writer,
            &test_topic_metadata(),
            test_messages_metadata(),
            messages,
        ));

        assert_stats(&stats, 0, 2);
        assert!(matches!(
            last_error,
            Some(RouterError::FailedToAddToRecordBatch { .. })
        ));
        assert!(
            writer
                .table_writes
                .lock()
                .expect("Table writes lock should not be poisoned")
                .is_empty()
        );
    }

    #[test]
    fn given_invalid_and_valid_messages_when_routing_should_skip_invalid_message() {
        let mut router = StaticTableRouter::new(1, &test_config());
        let writer = RecordingWriter::default();
        let messages = vec![
            test_message(101, Payload::Raw(vec![0xff])),
            test_message(102, Payload::Text("valid".to_string())),
        ];

        run_async(router.init(&writer)).expect("Router should initialize");
        let RouteOutcome {
            stat: stats,
            last_error,
        } = run_async(router.route(
            &writer,
            &test_topic_metadata(),
            test_messages_metadata(),
            messages,
        ));

        assert_stats(&stats, 1, 1);
        assert!(matches!(
            last_error,
            Some(RouterError::FailedToAddToRecordBatch { .. })
        ));
        let table_writes = writer
            .table_writes
            .lock()
            .expect("Table writes lock should not be poisoned");
        assert_eq!(table_writes.len(), 1);
        assert_eq!(table_writes[0].batch.num_rows(), 1);
        let payloads = table_writes[0]
            .batch
            .column_by_name("payload")
            .expect("Payload column should exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Payload column should contain strings");
        assert_eq!(payloads.value(0), "valid");
    }

    #[test]
    fn given_writer_skips_row_when_routing_should_report_writer_stats() {
        let mut router = StaticTableRouter::new(1, &test_config());
        let writer = RecordingWriter {
            skipped_rows: 1,
            ..RecordingWriter::default()
        };
        let messages = vec![
            test_message(101, Payload::Text("first".to_string())),
            test_message(102, Payload::Text("second".to_string())),
        ];

        run_async(router.init(&writer)).expect("Router should initialize");
        let RouteOutcome {
            stat: stats,
            last_error,
        } = run_async(router.route(
            &writer,
            &test_topic_metadata(),
            test_messages_metadata(),
            messages,
        ));

        assert_stats(&stats, 1, 1);
        assert!(last_error.is_none());
    }

    #[test]
    fn given_write_messages_router_should_write_with_append_mode() {
        let mut router = StaticTableRouter::new(1, &test_config());
        let writer = RecordingWriter::default();
        let messages = vec![test_message(102, Payload::Text("valid".to_string()))];

        run_async(router.init(&writer)).expect("Router should initialize");
        let RouteOutcome { last_error, .. } = run_async(router.route(
            &writer,
            &test_topic_metadata(),
            test_messages_metadata(),
            messages,
        ));

        assert!(last_error.is_none());
        let table_writes = writer
            .table_writes
            .lock()
            .expect("Table writes lock should not be poisoned");
        assert_eq!(table_writes.len(), 1);

        assert!(!table_writes[0].has_primary_key);
    }

    #[test]
    fn given_table_write_failure_when_routing_should_return_writer_error() {
        let mut router = StaticTableRouter::new(1, &test_config());
        let writer = RecordingWriter {
            fail_table_write: true,
            ..RecordingWriter::default()
        };

        run_async(router.init(&writer)).expect("Router should initialize");
        let RouteOutcome { stat, last_error } = run_async(router.route(
            &writer,
            &test_topic_metadata(),
            test_messages_metadata(),
            vec![test_message(101, Payload::Text("payload".to_string()))],
        ));
        let error = last_error.expect("Table write should fail");

        assert!(matches!(error, RouterError::WriterError { .. }));
        assert_eq!(stat.errors, 1);
    }
}
