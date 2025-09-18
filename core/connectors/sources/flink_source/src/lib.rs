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

use async_trait::async_trait;
use chrono::Utc;
use iggy_connector_sdk::{
    source_connector, ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

mod config;
mod config_loader;
mod flink_reader;
mod state;

use config::FlinkSourceConfig;
use flink_reader::{FlinkMessage, FlinkReader};
use state::SourceState;

source_connector!(FlinkSource);

#[derive(Debug)]
pub struct FlinkSource {
    id: u32,
    config: FlinkSourceConfig,
    reader: Arc<Mutex<FlinkReader>>,
    state: Arc<RwLock<SourceState>>,
    buffer: Arc<Mutex<Vec<FlinkMessage>>>,
}

impl FlinkSource {
    pub fn new(id: u32, config: FlinkSourceConfig, state: Option<ConnectorState>) -> Self {
        let reader = Arc::new(Mutex::new(FlinkReader::new(config.clone())));

        let source_state = if let Some(connector_state) = state {
            SourceState::from_bytes(&connector_state.0).unwrap_or_else(|| {
                warn!("Failed to deserialize state, creating new");
                SourceState::new()
            })
        } else {
            SourceState::new()
        };

        let buffer = Arc::new(Mutex::new(Vec::with_capacity(config.batch_size)));

        FlinkSource {
            id,
            config,
            reader,
            state: Arc::new(RwLock::new(source_state)),
            buffer,
        }
    }

    async fn fetch_messages(&self) -> Result<Vec<FlinkMessage>, Error> {
        let state = self.state.read().await;
        let offset = state.last_offset;
        drop(state);

        let reader = self.reader.lock().await;
        match reader.fetch_batch(offset, self.config.batch_size).await {
            Ok(messages) => {
                debug!("Fetched {} messages from Flink", messages.len());
                Ok(messages)
            }
            Err(e) => {
                error!("Failed to fetch messages from Flink: {}", e);
                Err(Error::HttpRequestFailed(e.to_string()))
            }
        }
    }

    async fn convert_message(&self, msg: FlinkMessage) -> Result<ProducedMessage, Error> {
        let payload = match self.config.output_schema {
            Schema::Json => serde_json::to_vec(&msg.data).map_err(|_| Error::InvalidJsonPayload)?,
            Schema::Text => msg.data.to_string().into_bytes(),
            Schema::Raw => {
                // For raw, we'll serialize the JSON as bytes
                serde_json::to_vec(&msg.data).unwrap_or_default()
            }
            _ => {
                warn!("Unsupported schema type: {:?}", self.config.output_schema);
                return Err(Error::InvalidPayloadType);
            }
        };

        Ok(ProducedMessage {
            id: Some(msg.id.as_u128()),
            checksum: Some(msg.checksum),
            timestamp: Some(msg.timestamp),
            origin_timestamp: msg.origin_timestamp,
            // TODO: Convert headers properly once HeaderKey/HeaderValue API is understood
            headers: None,
            payload,
        })
    }

    async fn start_background_fetch(&self) {
        let reader = self.reader.clone();
        let buffer = self.buffer.clone();
        let state = self.state.clone();
        let batch_size = self.config.batch_size;
        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);

        tokio::spawn(async move {
            loop {
                sleep(poll_interval).await;

                let current_state = state.read().await;
                let offset = current_state.last_offset;
                drop(current_state);

                let reader_guard = reader.lock().await;
                match reader_guard.fetch_batch(offset, batch_size).await {
                    Ok(messages) if !messages.is_empty() => {
                        drop(reader_guard); // Release reader lock before locking buffer
                        let mut buf = buffer.lock().await;
                        buf.extend(messages);
                        debug!("Added messages to buffer, current size: {}", buf.len());
                    }
                    Ok(_) => {
                        debug!("No new messages from Flink");
                    }
                    Err(e) => {
                        error!("Background fetch error: {}", e);
                    }
                }
            }
        });
    }
}

#[async_trait]
impl Source for FlinkSource {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Flink source connector with ID: {} for cluster: {}",
            self.id, self.config.flink_cluster_url
        );

        // Verify connection to Flink cluster
        {
            let reader = self.reader.lock().await;
            reader.check_connection().await.map_err(|e| {
                error!("Failed to connect to Flink cluster: {}", e);
                Error::InitError(format!("Cannot connect to Flink: {}", e))
            })?;
        }

        // Subscribe to the configured source
        {
            let mut reader = self.reader.lock().await;
            if let Err(e) = reader.subscribe_to_source().await {
                error!("Failed to subscribe to Flink source: {}", e);
                return Err(Error::InitError(e.to_string()));
            }

            // Get initial metrics if available
            match reader.get_source_metrics().await {
                Ok(metrics) => {
                    info!("Initial source metrics: {:?}", metrics);
                }
                Err(e) => {
                    debug!("Could not fetch initial metrics: {}", e);
                }
            }
        }

        // Restore state if we have a checkpoint
        let state = self.state.read().await;
        if let Some(checkpoint_id) = &state.checkpoint_id {
            info!("Restoring from checkpoint: {}", checkpoint_id);
            let reader = self.reader.lock().await;
            if let Err(e) = reader.restore_from_checkpoint(checkpoint_id).await {
                warn!("Failed to restore from checkpoint: {}", e);
            }
        }
        drop(state);

        // Start background message fetching if configured
        if self.config.enable_background_fetch {
            self.start_background_fetch().await;
        }

        info!("Flink source connector initialized successfully");
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        // Wait for the polling interval
        sleep(Duration::from_millis(self.config.poll_interval_ms)).await;

        let mut messages = Vec::new();

        // Check buffer first if background fetch is enabled
        if self.config.enable_background_fetch {
            let mut buffer = self.buffer.lock().await;
            if !buffer.is_empty() {
                let drain_count = buffer.len().min(self.config.batch_size);
                let drained: Vec<_> = buffer.drain(..drain_count).collect();

                for msg in drained {
                    if let Ok(produced_msg) = self.convert_message(msg).await {
                        messages.push(produced_msg);
                    }
                }
            }
        }

        // If buffer was empty or background fetch disabled, fetch directly
        if messages.is_empty() {
            let fetched = self.fetch_messages().await?;
            for msg in fetched {
                if let Ok(produced_msg) = self.convert_message(msg).await {
                    messages.push(produced_msg);
                }
            }
        }

        // Update state
        if !messages.is_empty() {
            let mut state = self.state.write().await;
            state.messages_produced += messages.len() as u64;
            state.last_poll_time = Utc::now();

            // Update offset based on the last message
            if let Some(last_msg) = messages.last() {
                if let Some(id) = last_msg.id {
                    state.last_offset = id as u64;
                }
            }

            info!(
                "Produced {} messages from Flink source {}",
                messages.len(),
                self.id
            );
        }

        // Prepare state for persistence
        let state = self.state.read().await;
        let connector_state = Some(ConnectorState(state.to_bytes()));

        Ok(ProducedMessages {
            schema: self.config.output_schema,
            messages,
            state: connector_state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing Flink source connector with ID: {}", self.id);

        // Unsubscribe from source
        let reader = self.reader.lock().await;
        if let Err(e) = reader.unsubscribe().await {
            warn!("Error during unsubscribe: {}", e);
        }
        drop(reader);

        // Save final state
        let state = self.state.read().await;
        info!(
            "Flink source closed. Total messages produced: {}",
            state.messages_produced
        );

        Ok(())
    }
}
