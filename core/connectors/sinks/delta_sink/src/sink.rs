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

use crate::DeltaSink;
use crate::SinkState;
use crate::coercions::{coerce, create_coercion_tree};
use crate::storage::build_storage_options;
use crate::utils::parse_schema;
use async_trait::async_trait;
use deltalake::DeltaTable;
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::{DeltaWriter, JsonWriter};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata};
use std::collections::HashMap;
use tracing::{debug, error, info};

#[async_trait]
impl Sink for DeltaSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Delta Lake sink connector with ID: {} for table: {}",
            self.id, self.config.table_uri
        );

        let table_url = url::Url::parse(&self.config.table_uri).map_err(|e| {
            error!("Failed to parse table URI '{}': {e}", self.config.table_uri);
            Error::InitError(format!("Invalid table URI: {e}"))
        })?;

        info!("Parsed table URI: {}", table_url);

        let storage_options = build_storage_options(&self.config).map_err(|e| {
            error!("Invalid storage configuration: {e}");
            Error::InitError(format!("Invalid storage configuration: {e}"))
        })?;

        let table =
            match deltalake::open_table_with_storage_options(table_url, storage_options.clone())
                .await
            {
                Ok(table) => table,
                Err(_) if !self.config.schema.is_empty() => {
                    info!("Table does not exist, creating from configured schema...");
                    create_table(&self.config.table_uri, storage_options, &self.config.schema)
                        .await?
                }
                Err(e) => {
                    error!("Failed to load Delta table: {e}");
                    return Err(Error::InitError(format!("Failed to load Delta table: {e}")));
                }
            };

        let kernel_schema = table
            .snapshot()
            .map_err(|e| {
                error!("Failed to get table snapshot: {e}");
                Error::InitError(format!("Failed to get table snapshot: {e}"))
            })?
            .schema();
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
                    let json_bytes = simd_json::to_vec(simd_value).map_err(|e| {
                        error!("Failed to serialize JSON payload: {e}");
                        Error::InvalidJsonPayload
                    })?;
                    let value: serde_json::Value =
                        serde_json::from_slice(&json_bytes).map_err(|e| {
                            error!("Failed to parse JSON payload: {e}");
                            Error::InvalidJsonPayload
                        })?;
                    json_values.push(value);
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
            Error::InvalidConfig
        })?;

        // Apply coercions to match Delta table schema
        for value in &mut json_values {
            coerce(value, &state.coercion_tree);
        }

        // Write JSON values to internal Parquet buffers
        state.writer.write(json_values).await.map_err(|e| {
            error!("Failed to write to Delta writer: {e}");
            Error::Storage(format!("Failed to write to Delta writer: {e}"))
        })?;

        // Flush buffers to object store and commit to Delta log
        let version = state
            .writer
            .flush_and_commit(&mut state.table)
            .await
            .map_err(|e| {
                error!("Failed to flush and commit to Delta table: {e}");
                Error::Storage(format!("Failed to flush and commit: {e}"))
            })?;

        debug!(
            "Delta sink with ID: {} committed version {}",
            self.id, version
        );

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Delta Lake sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}

async fn create_table(
    table_uri: &str,
    storage_options: HashMap<String, String>,
    schema: &[String],
) -> Result<DeltaTable, Error> {
    let columns = parse_schema(schema)?;
    let table = CreateBuilder::new()
        .with_location(table_uri)
        .with_storage_options(storage_options)
        .with_columns(columns)
        .await
        .map_err(|e| {
            error!("Failed to create Delta table: {e}");
            Error::InitError(format!("Failed to create Delta table: {e}"))
        })?;
    info!("Created new Delta table at {table_uri}");
    Ok(table)
}
