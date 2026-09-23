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
    fmt::{self, Display, Formatter},
    sync::Arc,
    time::Duration,
};

use arrow::array::{RecordBatch, StringArray};
use fluss::{
    client::{AppendWriter, FlussConnection, FlussTable, UpsertWriter},
    error::{Error as FlussError, FlussError as FlussApiError},
    metadata::{TableDescriptor, TablePath},
    row::ColumnarRow,
};
use iggy_connector_sdk::Error as ConnectorError;
use thiserror::Error;
use tracing::warn;

use crate::ResolvedFlussSinkConfig;

#[derive(Debug, Error)]
pub(crate) enum WriterError {
    #[error(transparent)]
    Connector(ConnectorError),
    #[error("Fluss connection is not initialized")]
    ConnectionNotInitialized,
    #[error("Failed to connect to Fluss: {source}")]
    Connect {
        #[source]
        source: Box<FlussError>,
    },
    #[error("Invalid Fluss writer configuration: {source}")]
    InvalidWriterConfig {
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to close Fluss connection: {source}")]
    CloseConnection {
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to get Fluss admin client: {source}")]
    GetAdminClient {
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to create Fluss table '{table_path}': {source}")]
    CreateTable {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to get Fluss table, table not found: '{table_path}'")]
    TableNotFound { table_path: TablePath },
    #[error("Failed to get Fluss table '{table_path}' because of error: {source}")]
    GetTableFailed {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to create appender for Fluss table '{table_path}': {source}")]
    CreateAppender {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to create writer for Fluss table '{table_path}': {source}")]
    CreateWriter {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to append Arrow batch to Fluss table '{table_path}': {source}")]
    AppendArrowBatch {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
    #[error(
        "Failed to flush rows to Fluss table, data can be partially stored, append operation can duplicate data '{table_path}': {source}"
    )]
    FlushRows {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },

    #[error(
        "Failed to create row reader for arrow batch record for table: '{table_path}': {source}"
    )]
    CreateColumnarRow {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
}

impl From<ConnectorError> for WriterError {
    fn from(error: ConnectorError) -> Self {
        Self::Connector(error)
    }
}

impl From<WriterError> for ConnectorError {
    fn from(error: WriterError) -> Self {
        let message = error.to_string();
        match error {
            WriterError::Connector(source) => source,
            WriterError::ConnectionNotInitialized | WriterError::Connect { .. } => {
                Self::InitError(message)
            }
            WriterError::InvalidWriterConfig { .. } => Self::InvalidConfigValue(message),
            WriterError::CloseConnection { .. } => Self::Connection(message),
            WriterError::TableNotFound { .. } => Self::SchemaMismatch(message),
            WriterError::GetAdminClient { source }
            | WriterError::CreateTable { source, .. }
            | WriterError::GetTableFailed { source, .. }
            | WriterError::CreateAppender { source, .. }
            | WriterError::CreateWriter { source, .. }
            | WriterError::AppendArrowBatch { source, .. }
            | WriterError::FlushRows { source, .. }
            | WriterError::CreateColumnarRow { source, .. } => {
                WriterError::classify_source(&source, message)
            }
        }
    }
}

impl WriterError {
    fn classify_source(source: &FlussError, message: String) -> ConnectorError {
        match source {
            FlussError::RowConvertError { .. } | FlussError::ArrowError { .. } => {
                ConnectorError::SchemaMismatch(message)
            }
            FlussError::JsonSerdeError { .. } => ConnectorError::InvalidRecordValue(message),
            FlussError::IllegalArgument { .. }
            | FlussError::IoUnsupported { .. }
            | FlussError::UnsupportedOperation { .. }
            | FlussError::UnsupportedVersion { .. }
            | FlussError::InvalidServerType { .. } => ConnectorError::InvalidConfigValue(message),
            FlussError::RpcError { .. } | FlussError::IoUnexpectedError { .. } => {
                ConnectorError::Connection(message)
            }
            FlussError::FlussAPIError { .. } => match source.api_error() {
                Some(
                    FlussApiError::TableNotExist
                    | FlussApiError::SchemaNotExist
                    | FlussApiError::InvalidTableException
                    | FlussApiError::NonPrimaryKeyTableException
                    | FlussApiError::InvalidColumnProjection
                    | FlussApiError::InvalidTargetColumn
                    | FlussApiError::TableNotPartitionedException
                    | FlussApiError::PartitionSpecInvalidException
                    | FlussApiError::InvalidAlterTableException,
                ) => ConnectorError::SchemaMismatch(message),
                Some(
                    FlussApiError::CorruptMessage
                    | FlussApiError::RecordTooLargeException
                    | FlussApiError::CorruptRecordException
                    | FlussApiError::InvalidTimestampException,
                ) => ConnectorError::InvalidRecordValue(message),
                Some(
                    FlussApiError::UnsupportedVersion
                    | FlussApiError::DatabaseNotExist
                    | FlussApiError::InvalidDatabaseException
                    | FlussApiError::InvalidReplicationFactor
                    | FlussApiError::InvalidRequiredAcks
                    | FlussApiError::InvalidConfigException
                    | FlussApiError::LakeStorageNotConfiguredException
                    | FlussApiError::AuthenticateException
                    | FlussApiError::SecurityDisabledException
                    | FlussApiError::AuthorizationException
                    | FlussApiError::DeletionDisabledException,
                ) => ConnectorError::InvalidConfigValue(message),
                _ => ConnectorError::CannotStoreData(message),
            },
            _ => ConnectorError::CannotStoreData(message),
        }
    }
}

#[derive(Default)]
pub(crate) struct Stat {
    pub(crate) errors: u64,
    pub(crate) appended: u64,
    pub(crate) inserted: u64,
}

impl Stat {
    pub(crate) fn add(self, other: Self) -> Self {
        Self {
            errors: self.errors + other.errors,
            appended: self.appended + other.appended,
            inserted: self.inserted + other.inserted,
        }
    }

    pub(crate) fn inc_err(&mut self) {
        self.errors += 1;
    }

    pub(crate) fn inc_err_by(&mut self, count: u64) {
        self.errors += count;
    }

    pub(crate) fn inc_appended(&mut self) {
        self.appended += 1;
    }

    pub(crate) fn inc_appended_by(&mut self, count: u64) {
        self.appended += count;
    }
}

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum Op {
    Append,
    UpsertOrDelete,
}

pub(crate) trait TableWriter {
    async fn write_to_table(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        op: Op,
        batch: RecordBatch,
    ) -> Result<Stat, WriterError>;

    async fn create_table_if_not_exists(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
    ) -> Result<(), WriterError>;

    async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>, WriterError>;
}

pub struct FlussWriter {
    connection: Option<FlussConnection>,
    config: ResolvedFlussSinkConfig,
}

impl Display for FlussWriter {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "FlussWriter")
    }
}

impl fmt::Debug for FlussWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FlussWriter")
            .finish_non_exhaustive()
    }
}

impl FlussWriter {
    pub fn new(config: ResolvedFlussSinkConfig) -> Self {
        Self {
            config,
            connection: None,
        }
    }

    pub(crate) async fn connect(&mut self) -> Result<(), WriterError> {
        let config = fluss::config::Config::try_from(&self.config).map_err(|source| {
            WriterError::InvalidWriterConfig {
                source: Box::new(source),
            }
        })?;
        let connection =
            FlussConnection::new(config)
                .await
                .map_err(|source| WriterError::Connect {
                    source: Box::new(source),
                })?;
        connection.get_or_create_writer_client().map_err(|source| {
            WriterError::InvalidWriterConfig {
                source: Box::new(source),
            }
        })?;
        self.connection = Some(connection);
        Ok(())
    }

    pub(crate) async fn close(&mut self) -> Result<(), WriterError> {
        let connection = self
            .connection
            .take()
            .ok_or(WriterError::ConnectionNotInitialized)?;

        connection
            .close(Duration::from_secs(30))
            .await
            .map_err(|source| WriterError::CloseConnection {
                source: Box::new(source),
            })
    }
    async fn append_record_batch(
        &self,
        table_path: &TablePath,
        batch: RecordBatch,
    ) -> Result<Stat, WriterError> {
        let rows = batch.num_rows();
        let writer = self.create_append_writer(table_path).await?;
        writer
            .append_arrow_batch(batch)
            .map_err(|source| WriterError::AppendArrowBatch {
                table_path: table_path.clone(),
                source: Box::new(source),
            })?;
        self.flush_appender(&writer, table_path).await?;
        let mut stat = Stat::default();
        stat.inc_appended_by(rows as u64);
        Ok(stat)
    }
    async fn append_record_batch_as_rows(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        batch: RecordBatch,
    ) -> Result<Stat, WriterError> {
        let writer = self.create_append_writer(table_path).await?;
        let (rows, mut row) = to_columnal_row(table_path, table_descriptor, batch)?;
        let mut stat = Stat::default();
        for i in 0..rows {
            row.set_row_id(i);
            match writer.append(&row) {
                Ok(_) => stat.inc_appended(),
                Err(error) => {
                    warn!("FlussSink: Failed to append row {i} to table '{table_path}': {error}");
                    stat.inc_err();
                }
            }
        }
        self.flush_appender(&writer, table_path).await?;
        Ok(stat)
    }

    async fn upsert_or_delete_arrow_batch_as_rows(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        batch: RecordBatch,
    ) -> Result<Stat, WriterError> {
        let op = batch
            .column_by_name("op")
            .and_then(|col| col.as_any().downcast_ref::<StringArray>())
            .cloned();

        let writer = self.create_upsert_writer(table_path).await?;
        let (rows, mut row) = to_columnal_row(table_path, table_descriptor, batch)?;
        let mut stat = Stat::default();
        for i in 0..rows {
            row.set_row_id(i);
            let (operation, write_result) = if op.as_ref().is_some_and(|op| op.value(i) == "d") {
                ("delete", writer.delete(&row))
            } else {
                ("upsert", writer.upsert(&row))
            };
            match write_result {
                Ok(_) => stat.inc_appended(),
                Err(error) => {
                    warn!(
                        "FlussSink: Failed to {operation} row {i} in table '{table_path}': {error}"
                    );
                    stat.inc_err();
                }
            }
        }
        self.flush_upserter(&writer, table_path).await?;
        Ok(stat)
    }

    fn get_connection(&self) -> Result<&FlussConnection, WriterError> {
        self.connection
            .as_ref()
            .ok_or(WriterError::ConnectionNotInitialized)
    }

    async fn get_table_by_path(
        &self,
        table_path: &TablePath,
    ) -> Result<FlussTable<'_>, WriterError> {
        let connection = self.get_connection()?;

        // The fluss get table API at the moment, doesn't return a specific error for table not
        // found, so we need to check if the table exists first.
        let table_id = connection
            .get_metadata()
            .fetch_table_id(table_path)
            .await
            .map_err(|source| WriterError::GetTableFailed {
                table_path: table_path.clone(),
                source: Box::new(source),
            })?;

        if table_id.is_none() {
            return Err(WriterError::TableNotFound {
                table_path: table_path.clone(),
            });
        }

        connection
            .get_table(table_path)
            .await
            .map_err(|source| match source {
                FlussError::FlussAPIError { api_error }
                    if api_error.code == fluss::rpc::FlussError::TableNotExist.code() =>
                {
                    WriterError::TableNotFound {
                        table_path: table_path.clone(),
                    }
                }
                _ => WriterError::GetTableFailed {
                    table_path: table_path.clone(),
                    source: Box::new(source),
                },
            })
    }

    async fn create_append_writer(
        &self,
        table_path: &TablePath,
    ) -> Result<AppendWriter, WriterError> {
        let table = self.get_table_by_path(table_path).await?;
        table
            .new_append()
            .map_err(|source| WriterError::CreateAppender {
                table_path: table_path.clone(),
                source: Box::new(source),
            })?
            .create_writer()
            .map_err(|source| WriterError::CreateWriter {
                table_path: table_path.clone(),
                source: Box::new(source),
            })
    }

    async fn create_upsert_writer(
        &self,
        table_path: &TablePath,
    ) -> Result<UpsertWriter, WriterError> {
        let table = self.get_table_by_path(table_path).await?;
        table
            .new_upsert()
            .map_err(|source| WriterError::CreateWriter {
                table_path: table_path.clone(),
                source: Box::new(source),
            })?
            .create_writer()
            .map_err(|source| WriterError::CreateWriter {
                table_path: table_path.clone(),
                source: Box::new(source),
            })
    }

    async fn flush_appender(
        &self,
        writer: &AppendWriter,
        table_path: &TablePath,
    ) -> Result<(), WriterError> {
        writer
            .flush()
            .await
            .map_err(|source| WriterError::FlushRows {
                table_path: table_path.clone(),
                source: Box::new(source),
            })
    }

    async fn flush_upserter(
        &self,
        writer: &UpsertWriter,
        table_path: &TablePath,
    ) -> Result<(), WriterError> {
        writer
            .flush()
            .await
            .map_err(|source| WriterError::FlushRows {
                table_path: table_path.clone(),
                source: Box::new(source),
            })
    }
}

impl TableWriter for FlussWriter {
    async fn create_table_if_not_exists(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
    ) -> Result<(), WriterError> {
        self.get_connection()?
            .get_admin()
            .map_err(|source| WriterError::GetAdminClient {
                source: Box::new(source),
            })?
            .create_table(table_path, table_descriptor, true)
            .await
            .map_err(|source| WriterError::CreateTable {
                table_path: table_path.clone(),
                source: Box::new(source),
            })
    }

    async fn write_to_table(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        op: Op,
        batch: RecordBatch,
    ) -> Result<Stat, WriterError> {
        match op {
            Op::Append => {
                if table_descriptor.is_partitioned() {
                    // fallback to rows if table is partitioned
                    self.append_record_batch_as_rows(table_path, table_descriptor, batch)
                        .await
                } else {
                    self.append_record_batch(table_path, batch).await
                }
            }
            Op::UpsertOrDelete => {
                self.upsert_or_delete_arrow_batch_as_rows(table_path, table_descriptor, batch)
                    .await
            }
        }
    }

    async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>, WriterError> {
        self.get_table_by_path(table_path).await
    }
}

fn to_columnal_row(
    table: &TablePath,
    table_descriptor: &TableDescriptor,
    batch: RecordBatch,
) -> Result<(usize, ColumnarRow), WriterError> {
    let row_type = table_descriptor.schema().row_type().clone();
    let rows = batch.num_rows();
    let row = ColumnarRow::new(Arc::new(batch), Arc::new(row_type), 0, None).map_err(|source| {
        WriterError::CreateColumnarRow {
            table_path: table.clone(),
            source: Box::new(source),
        }
    })?;
    Ok((rows, row))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use fluss::{
        error::Error as FlussError,
        metadata::{Column, DataTypes, TableDescriptor, TablePath},
    };
    use iggy_connector_sdk::Error as ConnectorError;

    use super::{WriterError, to_columnal_row};

    type ErrorConstructor = fn(String) -> ConnectorError;

    fn string_table_descriptor() -> TableDescriptor {
        let schema = fluss::metadata::Schema::builder()
            .with_columns(vec![Column::new("id", DataTypes::string())])
            .build()
            .expect("Fluss schema should build");
        TableDescriptor::builder()
            .schema(schema)
            .build()
            .expect("Table descriptor should build")
    }

    #[test]
    fn given_matching_batch_when_creating_columnar_row_should_preserve_rows() {
        let table = TablePath::new("fluss", "orders");
        let descriptor = string_table_descriptor();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["first", "second"]))],
        )
        .expect("Record batch should build");

        let (rows, row) = to_columnal_row(&table, &descriptor, batch)
            .expect("Matching batch should create a columnar row");

        assert_eq!(rows, 2);
        assert_eq!(row.get_row_id(), 0);
        assert_eq!(
            row.get_record_batch()
                .expect("Batch should be available")
                .num_rows(),
            2
        );
    }

    #[test]
    fn given_mismatched_batch_when_creating_columnar_row_should_return_table_context() {
        let table = TablePath::new("fluss", "orders");
        let descriptor = string_table_descriptor();
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));

        let error =
            to_columnal_row(&table, &descriptor, batch).expect_err("Mismatched batch should fail");

        assert!(
            matches!(error, WriterError::CreateColumnarRow { table_path, .. } if table_path == table)
        );
    }

    #[test]
    fn given_connector_error_when_converting_should_preserve_original_variant() {
        let expected = ConnectorError::InvalidConfigValue("invalid value".to_string());

        let actual: ConnectorError = WriterError::Connector(expected.clone()).into();

        assert_eq!(actual, expected);
    }

    #[test]
    fn given_missing_connection_when_converting_should_return_init_error() {
        let error: ConnectorError = WriterError::ConnectionNotInitialized.into();

        assert_eq!(
            error,
            ConnectorError::InitError("Fluss connection is not initialized".to_string())
        );
    }

    #[test]
    fn given_arrow_append_failure_when_converting_should_return_cannot_store_data() {
        let error: ConnectorError = WriterError::AppendArrowBatch {
            table_path: TablePath::new("fluss", "iggy_messages"),
            source: Box::new(FlussError::WriterClosed {
                message: "writer closed".to_string(),
            }),
        }
        .into();

        assert!(matches!(
            error,
            ConnectorError::CannotStoreData(message)
                if message.contains("fluss.iggy_messages") && message.contains("writer closed")
        ));
    }
    #[test]
    fn given_permanent_schema_failure_when_converting_should_return_schema_mismatch() {
        for source in [
            FlussError::invalid_table("invalid primary key"),
            FlussError::invalid_partition("invalid partition columns"),
            FlussError::RowConvertError {
                message: "column type mismatch".into(),
            },
            FlussError::from(arrow::error::ArrowError::SchemaError(
                "incompatible fields".into(),
            )),
        ] {
            let error = WriterError::AppendArrowBatch {
                table_path: TablePath::new("fluss", "orders"),
                source: Box::new(source),
            };
            let expected = ConnectorError::SchemaMismatch(error.to_string());
            assert_eq!(ConnectorError::from(error), expected);
        }
    }

    #[test]
    fn given_transient_failure_when_converting_should_return_cannot_store_data() {
        for source in [
            FlussError::leader_not_available("leader election"),
            FlussError::BufferExhausted {
                message: "buffer full".into(),
            },
        ] {
            let error = WriterError::FlushRows {
                table_path: TablePath::new("fluss", "orders"),
                source: Box::new(source),
            };
            assert!(matches!(
                ConnectorError::from(error),
                ConnectorError::CannotStoreData(_)
            ));
        }
    }

    #[test]
    fn given_invalid_api_request_when_converting_should_classify_record_and_config_errors() {
        let cases: [(fluss::error::FlussError, ErrorConstructor); 4] = [
            (
                fluss::error::FlussError::CorruptMessage,
                ConnectorError::InvalidRecordValue,
            ),
            (
                fluss::error::FlussError::RecordTooLargeException,
                ConnectorError::InvalidRecordValue,
            ),
            (
                fluss::error::FlussError::InvalidConfigException,
                ConnectorError::InvalidConfigValue,
            ),
            (
                fluss::error::FlussError::AuthorizationException,
                ConnectorError::InvalidConfigValue,
            ),
        ];
        for (kind, expected) in cases {
            let error = WriterError::FlushRows {
                table_path: TablePath::new("fluss", "orders"),
                source: Box::new(FlussError::from(fluss::error::ApiError {
                    code: kind.code(),
                    message: "rejected request".into(),
                })),
            };
            let expected = expected(error.to_string());
            assert_eq!(ConnectorError::from(error), expected);
        }
    }
}
