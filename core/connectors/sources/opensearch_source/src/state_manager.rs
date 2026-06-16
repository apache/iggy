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

use crate::{OpenSearchSource, StateConfig};
use async_trait::async_trait;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::Error;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

impl OpenSearchSource {
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

        let storage = create_state_storage(
            self.config
                .state
                .as_ref()
                .expect("state.enabled implies Some(state)"),
        )?;

        let source_state = self.internal_state_to_source_state().await?;
        storage.save_source_state(&source_state).await?;

        info!(
            "Saved state for OpenSearch source connector with ID: {}",
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

        let storage = create_state_storage(
            self.config
                .state
                .as_ref()
                .expect("state.enabled implies Some(state)"),
        )?;

        let state_id = self.get_state_id();
        if let Some(source_state) = storage.load_source_state(&state_id).await? {
            self.source_state_to_internal_state(source_state).await?;

            let (last_poll_timestamp, total_documents_fetched, poll_count) = {
                let state = self.state.lock().await;
                (
                    state.last_poll_timestamp,
                    state.total_documents_fetched,
                    state.poll_count,
                )
            };
            info!(
                "Loaded state for OpenSearch source connector with ID: {} - last poll: {:?}, total docs: {}, polls: {}",
                self.id, last_poll_timestamp, total_documents_fetched, poll_count
            );
        } else {
            info!(
                "No existing state found for OpenSearch source connector with ID: {}, starting fresh",
                self.id
            );
        }

        Ok(())
    }
}

pub(crate) fn create_state_storage(config: &StateConfig) -> Result<Arc<dyn StateStorage>, Error> {
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
        Some(storage_type) => Err(Error::InvalidConfigValue(format!(
            "state storage_type {storage_type:?} is not supported; only \"file\" is implemented"
        ))),
    }
}

/// State management for source connectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceState {
    /// Unique identifier for this state
    pub id: String,
    /// Timestamp when this state was last updated
    pub last_updated: DateTime<Utc>,
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
    async fn save_source_state(&self, state: &SourceState) -> Result<(), Error>;

    async fn load_source_state(&self, id: &str) -> Result<Option<SourceState>, Error>;
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

        fs::create_dir_all(&self.base_path)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create state directory: {e}")))?;

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
}
