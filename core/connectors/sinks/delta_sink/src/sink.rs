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

use crate::DeltaSink;
use crate::SinkState;
use crate::coercions::{coerce, create_coercion_tree};
use crate::storage::build_storage_options;
use async_trait::async_trait;
use deltalake::writer::{DeltaWriter, JsonWriter};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
    owned_value_to_serde_json,
};
use tracing::{debug, error, info};

// TODO: Expose metrics for observability purposes

#[async_trait]
impl Sink for DeltaSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Delta Lake sink connector with ID: {} for table: {}",
            self.id, self.config.table_uri
        );

        let table_url = url::Url::parse(&self.config.table_uri).map_err(|e| {
            error!(
                "Connector configuration: failed to parse table_uri = '{}': {e}.",
                self.config.table_uri
            );
            Error::InvalidConfigValue(format!("table_uri: {e}"))
        })?;
        let table_url_parsed = &self.config.table_uri;

        info!("Parsed table URI: {}", table_url_parsed);

        let storage_options = build_storage_options(&self.config).map_err(|e| {
            error!("Connector configuration: invalid storage configuration. Error message: {e}");
            Error::InitError(format!(
                "Connector configuration: invalid storage configuration. Error message: {e}"
            ))
        })?;

        info!("Successfully composed the storage options for accessing the storage backend");

        let builder = deltalake::DeltaTableBuilder::from_url(table_url)
            .map_err(|e| {
                error!("deltalake-rs interface: failed to configure with table_uri = '{table_url_parsed}'. Check deltalake::DeltaTableBuilder::from_url docs and code to correct your table_uri. Error message: {e}");
                Error::InvalidConfigValue(format!("table_uri = '{table_url_parsed}' caused an error in deltalake-rs interface. Check deltalake::DeltaTableBuilder::from_url docs and code to correct your table_uri. Error message: {e}"))
            })?
            .with_storage_options(storage_options);
        let mut table = builder.build().map_err(|e| {
            error!("deltalake-rs interface: failed to configure with provided storage configuration. Error message: {e}");
            Error::InitError(format!("deltalake-rs interface: failed to configure with provided storage configuration. Error message: {e}"))
        })?;
        let table_exists = table
            .verify_deltatable_existence()
            .await
            .map_err(
                |e| {
                    error!("deltalake-rs interface: failed to list table_url '{table_url_parsed}' directory to verify delta table existence. Make sure the destination exists and the access to the destination is set up correctly - read the Iggy delta connector docs for more information. Error message: {e}");
                    Error::InitError(format!("deltalake-rs interface: failed to list table_url '{table_url_parsed}' directory to verify delta table existence. Make sure the destination exists and the access to the destination is set up correctly - read the Iggy delta connector docs for more information. Error message: {e}"))
                }
            )?;
        if !table_exists {
            error!(
                "No delta table found in '{table_url_parsed}. Make sure to create the delta table in the destination manually or verify the validity of such table."
            );
            return Err(Error::InitError(format!(
                "No delta table found in '{table_url_parsed}. Make sure to create the delta table in the destination manually or verify the validity of such table."
            )));
        }

        table
            .load()
            .await
            .map_err(|e| {
                error!("deltalake-rs interface: failed to load the table's latest snapshot. See deltalake::DeltaTable::load for more information. Error message: {e}");
                Error::InitError(format!("deltalake-rs interface: failed to load the table's latest snapshot. See deltalake::DeltaTable::load for more information. Error message: {e}"))
            })?;

        let kernel_schema = table
            .snapshot()
            .map_err(|e| {
                error!("deltalake-rs interface: failed to get the table's latest snapshot. See deltalake::DeltaTable::snapshot for more information. Error message: {e}");
                Error::InitError(format!("deltalake-rs interface: failed to get the table's latest snapshot. See deltalake::DeltaTable::snapshot for more information. Error message: {e}"))
            })?
            .schema();

        // TODO: coercion tree is never refreshed if the schema changes concurrently,
        // leading to opaque errors downstream.
        let coercion_tree = create_coercion_tree(&kernel_schema);

        let writer = JsonWriter::for_table(&table).map_err(|e| {
            error!("Failed to create JsonWriter: {e}");
            Error::InitError(format!("Failed to create JsonWriter: {e}"))
        })?;

        *self.state.lock().await = Some(SinkState {
            table,
            writer,
            coercion_tree,
        });

        info!(
            "Delta Lake sink connector with ID: {} opened successfully.",
            self.id
        );
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        debug!(
            "Delta sink with ID: {} received: {} messages, partition: {}, offset: {}",
            self.id,
            messages.len(),
            messages_metadata.partition_id,
            messages_metadata.current_offset,
        );

        // Extract JSON values from consumed messages
        let mut json_values: Vec<serde_json::Value> = Vec::with_capacity(messages.len());
        for msg in &messages {
            match &msg.payload {
                Payload::Json(simd_value) => {
                    json_values.push(owned_value_to_serde_json(simd_value));
                }
                other => {
                    error!(
                        "Unsupported payload type: {other}. Delta sink only supports JSON payloads."
                    );
                    return Err(Error::InvalidPayloadType);
                }
            }
        }

        if json_values.is_empty() {
            debug!("No JSON values to write");
            return Ok(());
        }

        let mut state_guard = self.state.lock().await;
        let state = state_guard.as_mut().ok_or_else(|| {
            error!("Delta sink state not initialized — was open() called?");
            Error::InvalidState
        })?;

        // Apply coercions to match Delta table schema
        for value in &mut json_values {
            coerce(value, &state.coercion_tree).map_err(Error::InvalidRecordValue)?;
        }

        // Write JSON values to internal Parquet buffers
        // TODO: Add retry mechanism if write fails.
        if let Err(e) = state.writer.write(json_values).await {
            state.writer.reset();
            error!("Failed to write to Delta writer: {e}");
            return Err(Error::Storage(format!(
                "Failed to write to Delta writer: {e}"
            )));
        }

        // Flush buffers to object store and commit to Delta log
        let version = match state.writer.flush_and_commit(&mut state.table).await {
            Ok(v) => v,
            Err(e) => {
                state.writer.reset();
                error!("Failed to flush and commit to Delta table: {e}");
                return Err(Error::Storage(format!("Failed to flush and commit: {e}")));
            }
        };

        debug!(
            "Delta sink with ID: {} committed version {}",
            self.id, version
        );

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(mut state) = self.state.lock().await.take()
            && let Err(e) = state.writer.flush_and_commit(&mut state.table).await
        {
            error!(
                "Delta sink with ID: {} failed to flush on close: {e}",
                self.id
            );
            return Err(Error::Storage(format!("Failed to flush on close: {e}")));
        }
        info!("Delta Lake sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}
