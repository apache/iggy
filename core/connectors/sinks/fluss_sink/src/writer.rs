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

use std::fmt::{self, Display, Formatter};

use fluss::{
    client::FlussConnection,
    metadata::{TableDescriptor, TablePath},
};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, TopicMetadata};
use tracing::error;

use crate::{
    FlussSinkConfig,
    schema::{FlussTableLayout, RowContext},
};

pub struct TableWriteResult {
    pub insertion_errors: u64,
    pub messages_processed: u64,
}

pub struct FlussWriter {
    connection: Option<FlussConnection>,
    config: FlussSinkConfig,
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
    fn get_connection(&self) -> Result<&FlussConnection, Error> {
        self.connection
            .as_ref()
            .ok_or_else(|| Error::InitError("Fluss connection is not initialized".to_string()))
    }

    pub async fn connect(&mut self) -> Result<(), Error> {
        let config = fluss::config::Config::try_from(&self.config)?;
        let connection = FlussConnection::new(config)
            .await
            .map_err(|error| Error::InitError(format!("Failed to connect to Fluss: {error}")))?;
        // creating writer and cache it
        connection.get_or_create_writer_client().map_err(|error| {
            Error::InvalidConfigValue(format!("Invalid Fluss writer configuration: {error}"))
        })?;
        self.connection = Some(connection);
        Ok(())
    }

    pub fn new(config: FlussSinkConfig) -> Self {
        Self {
            config,
            connection: None,
        }
    }
    async fn create_table_if_not_exists(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
    ) -> Result<(), Error> {
        self.get_connection()?
            .get_admin()
            .map_err(|error| {
                Error::CannotStoreData(format!("Failed to get Fluss admin client: {error}"))
            })?
            .create_table(table_path, table_descriptor, true)
            .await
            .map_err(|error| {
                Error::CannotStoreData(format!(
                    "Failed to create Fluss table '{table_path}': {error}"
                ))
            })
    }

    pub async fn ensure_table_exists(
        &self,
        table_path: &TablePath,
        table_layout: &FlussTableLayout,
    ) -> Result<(), Error> {
        if self.config.auto_create_table {
            let table_descriptor = table_layout.build_table_descriptor().map_err(|error| {
                Error::SchemaMismatch(format!("Failed to build Fluss table descriptor: {error}"))
            })?;

            self.create_table_if_not_exists(table_path, &table_descriptor)
                .await?;
        }
        Ok(())
    }

    pub async fn write_to_table(
        &self,
        table_path: &TablePath,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
        topic_metadata: &TopicMetadata,
        table_layout: &FlussTableLayout,
    ) -> Result<TableWriteResult, Error> {
        let mut result = TableWriteResult {
            insertion_errors: 0,
            messages_processed: 0,
        };

        let table = self
            .get_connection()?
            .get_table(table_path)
            .await
            .map_err(|error| {
                Error::CannotStoreData(format!("Failed to get Fluss table '{table_path}': {error}"))
            })?;

        let writer = table
            .new_append()
            .map_err(|error| {
                Error::CannotStoreData(format!(
                    "Failed to create appender for Fluss table '{table_path}': {error}"
                ))
            })?
            .create_writer()
            .map_err(|error| {
                Error::CannotStoreData(format!(
                    "Failed to create writer for Fluss table '{table_path}': {error}"
                ))
            })?;

        let context = RowContext {
            topic: &topic_metadata.topic,
            stream: &topic_metadata.stream,
            partition_id: messages_metadata.partition_id,
        };
        for message in messages {
            let row = match table_layout.row_from_message(&message, context) {
                Ok(row) => row,
                Err(e) => {
                    error!(
                        "Can not convert iggy message to row, skipping message id: [{}] because of error: [{}]",
                        message.id, e
                    );
                    result.insertion_errors += 1;
                    continue;
                }
            };
            writer.append(&row).map_err(|error| {
                Error::CannotStoreData(format!(
                    "Failed to append message {} to Fluss table '{table_path}': {error}",
                    message.id
                ))
            })?;
            result.messages_processed += 1;
        }

        writer.flush().await.map_err(|error| {
            Error::CannotStoreData(format!(
                "Failed to flush rows to Fluss table '{table_path}': {error}"
            ))
        })?;

        Ok(result)
    }
}
