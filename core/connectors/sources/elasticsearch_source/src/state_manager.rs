/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use crate::{ElasticsearchSource, StateConfig};
use iggy_connector_sdk::{Error, FileStateStorage, Source, SourceState, StateStorage};
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{Duration, interval};
use tracing::{error, info, warn};

/// State manager for Elasticsearch source connector
pub struct StateManager {
    storage: Arc<dyn StateStorage>,
    config: StateConfig,
    auto_save_interval: Option<Duration>,
}

impl StateManager {
    pub fn new(config: StateConfig) -> Result<Self, Error> {
        let storage = Self::create_storage(&config)?;
        let auto_save_interval = config
            .auto_save_interval
            .as_deref()
            .and_then(|interval_str| {
                humantime::Duration::from_str(interval_str)
                    .ok()
                    .map(|d| Duration::from_secs(d.as_secs()))
            });

        Ok(Self {
            storage,
            config,
            auto_save_interval,
        })
    }

    fn create_storage(config: &StateConfig) -> Result<Arc<dyn StateStorage>, Error> {
        match config.storage_type.as_deref() {
            Some("file") | None => {
                let base_path = config
                    .storage_config
                    .as_ref()
                    .and_then(|c| c.get("base_path"))
                    .and_then(|p| p.as_str())
                    .unwrap_or("./connector_states");

                Ok(Arc::new(FileStateStorage::new(base_path)))
            }
            Some("elasticsearch") => {
                // TODO: Implement Elasticsearch-based state storage
                warn!(
                    "Elasticsearch state storage not yet implemented, falling back to file storage"
                );
                Ok(Arc::new(FileStateStorage::new("./connector_states")))
            }
            Some(storage_type) => {
                warn!(
                    "Unknown state storage type: {}, falling back to file storage",
                    storage_type
                );
                Ok(Arc::new(FileStateStorage::new("./connector_states")))
            }
        }
    }

    /// Start auto-save background task
    pub async fn start_auto_save(&self, connector: std::sync::Arc<ElasticsearchSource>) {
        let interval_duration = self
            .auto_save_interval
            .unwrap_or_else(|| Duration::from_secs(60));
        let storage = self.storage.clone();
        let state_id = self.config.state_id.clone();
        tokio::spawn(async move {
            let mut interval = interval(interval_duration);
            loop {
                interval.tick().await;
                if let Ok(Some(state)) = Source::get_state(&*connector).await {
                    if let Err(e) = storage.save_source_state(&state).await {
                        error!(
                            "Failed to auto-save state for {}: {}",
                            state_id.as_deref().unwrap_or("unknown"),
                            e
                        );
                    } else {
                        info!(
                            "Auto-saved state for {}",
                            state_id.as_deref().unwrap_or("unknown")
                        );
                    }
                }
            }
        });
    }

    /// Get state statistics
    pub async fn get_state_stats(&self) -> Result<StateStats, Error> {
        let state_ids = self.storage.list_states().await?;
        let mut stats = StateStats {
            total_states: state_ids.len(),
            states: Vec::new(),
        };

        for state_id in state_ids {
            if let Some(state) = self.storage.load_source_state(&state_id).await? {
                stats.states.push(StateInfo {
                    id: state.id,
                    last_updated: state.last_updated,
                    version: state.version,
                    connector_type: state
                        .metadata
                        .as_ref()
                        .and_then(|m| m.get("connector_type"))
                        .and_then(|t| t.as_str())
                        .unwrap_or("unknown")
                        .to_string(),
                });
            }
        }

        Ok(stats)
    }

    /// Clean up old states
    pub async fn cleanup_old_states(&self, older_than_days: u32) -> Result<usize, Error> {
        let state_ids = self.storage.list_states().await?;
        let cutoff_time = chrono::Utc::now() - chrono::Duration::days(older_than_days as i64);
        let mut deleted_count = 0;

        for state_id in state_ids {
            if let Some(state) = self.storage.load_source_state(&state_id).await?
                && state.last_updated < cutoff_time
            {
                if let Err(e) = self.storage.delete_state(&state_id).await {
                    warn!("Failed to delete old state {}: {}", state_id, e);
                } else {
                    deleted_count += 1;
                    info!("Deleted old state: {}", state_id);
                }
            }
        }

        Ok(deleted_count)
    }

    pub fn auto_save_interval(&self) -> Option<Duration> {
        self.auto_save_interval
    }
}

#[derive(Debug)]
pub struct StateStats {
    pub total_states: usize,
    pub states: Vec<StateInfo>,
}

#[derive(Debug)]
pub struct StateInfo {
    pub id: String,
    pub last_updated: chrono::DateTime<chrono::Utc>,
    pub version: u32,
    pub connector_type: String,
}

/// Extension trait for ElasticsearchSource to add state management utilities
#[allow(async_fn_in_trait)]
pub trait StateManagerExt {
    /// Get state manager for this connector
    fn get_state_manager(&self) -> Option<StateManager>;

    /// Export current state to JSON
    async fn export_state(&self) -> Result<Value, Error>;

    /// Import state from JSON
    async fn import_state(&mut self, state_json: Value) -> Result<(), Error>;

    /// Reset state (clear all state data)
    async fn reset_state(&mut self) -> Result<(), Error>;
}

impl StateManagerExt for ElasticsearchSource {
    fn get_state_manager(&self) -> Option<StateManager> {
        self.config.state.as_ref().and_then(|config| {
            if config.enabled {
                StateManager::new(config.clone()).ok()
            } else {
                None
            }
        })
    }

    async fn export_state(&self) -> Result<Value, Error> {
        let state = Source::get_state(self).await?;
        serde_json::to_value(state).map_err(|e| Error::Storage(e.to_string()))
    }

    async fn import_state(&mut self, state_json: Value) -> Result<(), Error> {
        let source_state: SourceState =
            serde_json::from_value(state_json).map_err(|e| Error::Storage(e.to_string()))?;
        Source::set_state(self, source_state).await
    }

    async fn reset_state(&mut self) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        *state = crate::State {
            last_poll_timestamp: None,
            total_documents_fetched: 0,
            poll_count: 0,
            last_document_id: None,
            last_scroll_id: None,
            last_offset: None,
            error_count: 0,
            last_error: None,
            processing_stats: crate::ProcessingStats {
                total_bytes_processed: 0,
                avg_batch_processing_time_ms: 0.0,
                last_successful_poll: None,
                empty_polls_count: 0,
                successful_polls_count: 0,
            },
        };
        drop(state);

        // Save the reset state
        Source::save_state(self).await
    }
}
