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
use async_trait::async_trait;
use iggy_connector_sdk::Error;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{Duration, interval};
use tracing::{error, info, warn};

impl ElasticsearchSource {
    async fn get_state(&self) -> Result<Option<SourceState>, Error> {
        if self
            .config
            .state
            .as_ref()
            .map(|s| s.enabled)
            .unwrap_or(false)
        {
            Ok(Some(self.internal_state_to_source_state().await?))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn save_state(&self) -> Result<(), Error> {
        if !self
            .config
            .state
            .as_ref()
            .map(|s| s.enabled)
            .unwrap_or(false)
        {
            return Ok(());
        }

        let storage = self
            .create_state_storage()
            .ok_or_else(|| Error::Storage("State storage not configured".to_string()))?;

        let source_state = self.internal_state_to_source_state().await?;
        storage.save_source_state(&source_state).await?;

        info!(
            "Saved state for Elasticsearch source connector with ID: {}",
            self.id
        );
        Ok(())
    }

    pub(super) async fn load_state(&mut self) -> Result<(), Error> {
        if !self
            .config
            .state
            .as_ref()
            .map(|s| s.enabled)
            .unwrap_or(false)
        {
            return Ok(());
        }

        let storage = self
            .create_state_storage()
            .ok_or_else(|| Error::Storage("State storage not configured".to_string()))?;

        let state_id = self.get_state_id();
        if let Some(source_state) = storage.load_source_state(&state_id).await? {
            self.source_state_to_internal_state(source_state).await?;

            let state = self.state.lock().await;
            info!(
                "Loaded state for Elasticsearch source connector with ID: {} - last poll: {:?}, total docs: {}, polls: {}",
                self.id, state.last_poll_timestamp, state.total_documents_fetched, state.poll_count
            );
        } else {
            info!(
                "No existing state found for Elasticsearch source connector with ID: {}, starting fresh",
                self.id
            );
        }

        Ok(())
    }
}

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
    pub async fn start_auto_save(&self, connector: Arc<ElasticsearchSource>) {
        let interval_duration = self
            .auto_save_interval
            .unwrap_or_else(|| Duration::from_secs(60));
        let storage = self.storage.clone();
        let state_id = self.config.state_id.clone();
        tokio::spawn(async move {
            let mut interval = interval(interval_duration);
            loop {
                interval.tick().await;
                if let Ok(Some(state)) = connector.get_state().await {
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

/// State management for source connectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceState {
    /// Unique identifier for this state
    pub id: String,
    /// Timestamp when this state was last updated
    pub last_updated: chrono::DateTime<chrono::Utc>,
    /// Version of the state format
    pub version: u32,
    /// Generic state data as JSON
    pub data: serde_json::Value,
    /// Optional metadata
    pub metadata: Option<serde_json::Value>,
}

/// State storage backend trait
#[async_trait]
pub trait StateStorage: Send + Sync {
    /// Save source state to storage
    async fn save_source_state(&self, state: &SourceState) -> Result<(), Error>;

    /// Load source state from storage
    async fn load_source_state(&self, id: &str) -> Result<Option<SourceState>, Error>;

    /// Delete state from storage
    async fn delete_state(&self, id: &str) -> Result<(), Error>;

    /// List all state IDs
    async fn list_states(&self) -> Result<Vec<String>, Error>;
}

/// File-based state storage implementation
pub struct FileStateStorage {
    base_path: std::path::PathBuf,
}

impl FileStateStorage {
    pub fn new<P: AsRef<std::path::Path>>(base_path: P) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    fn get_state_path(&self, id: &str) -> std::path::PathBuf {
        self.base_path.join(format!("{id}.json"))
    }
}

#[async_trait]
impl StateStorage for FileStateStorage {
    async fn save_source_state(&self, state: &SourceState) -> Result<(), Error> {
        use tokio::fs;

        // Ensure directory exists
        if let Some(parent) = self.base_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::Storage(format!("Failed to create state directory: {e}")))?;
        }

        let path = self.get_state_path(&state.id);
        let json = serde_json::to_string_pretty(state)
            .map_err(|e| Error::Serialization(format!("Failed to serialize source state: {e}")))?;

        fs::write(path, json)
            .await
            .map_err(|e| Error::Storage(format!("Failed to write state file: {e}")))?;

        Ok(())
    }

    async fn load_source_state(&self, id: &str) -> Result<Option<SourceState>, Error> {
        use tokio::fs;

        let path = self.get_state_path(id);
        if !path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(path)
            .await
            .map_err(|e| Error::Storage(format!("Failed to read state file: {e}")))?;

        let state: SourceState = serde_json::from_str(&content).map_err(|e| {
            Error::Serialization(format!("Failed to deserialize source state: {e}"))
        })?;

        Ok(Some(state))
    }

    async fn delete_state(&self, id: &str) -> Result<(), Error> {
        use tokio::fs;

        let path = self.get_state_path(id);
        if path.exists() {
            fs::remove_file(path)
                .await
                .map_err(|e| Error::Storage(format!("Failed to delete state file: {e}")))?;
        }

        Ok(())
    }

    async fn list_states(&self) -> Result<Vec<String>, Error> {
        use tokio::fs;

        let mut states = Vec::new();

        if !self.base_path.exists() {
            return Ok(states);
        }

        let mut entries = fs::read_dir(&self.base_path)
            .await
            .map_err(|e| Error::Storage(format!("Failed to read state directory: {e}")))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::Storage(format!("Failed to read directory entry: {e}")))?
        {
            if let Some(extension) = entry.path().extension()
                && extension == "json"
                && let Some(stem) = entry.path().file_stem()
                && let Some(id) = stem.to_str()
            {
                states.push(id.to_string());
            }
        }

        Ok(states)
    }
}
