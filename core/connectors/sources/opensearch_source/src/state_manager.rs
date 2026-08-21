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

use crate::{OpenSearchSource, StateConfig};
use async_trait::async_trait;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::Error;
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::sync::Arc;
use tracing::info;

pub(crate) const SOURCE_STATE_VERSION: u32 = 1;

impl OpenSearchSource {
    pub(super) async fn save_state(&self) -> Result<(), Error> {
        let Some(state_config) = self.config.state.as_ref().filter(|s| s.enabled) else {
            return Ok(());
        };
        let storage = create_state_storage(state_config)?;

        let source_state = self.internal_state_to_source_state().await?;
        storage.save_source_state(&source_state).await?;

        info!(
            "Saved state for OpenSearch source connector with ID: {}",
            self.id
        );
        Ok(())
    }

    pub(super) async fn load_state(&mut self) -> Result<(), Error> {
        let Some(state_config) = self.config.state.as_ref().filter(|s| s.enabled) else {
            return Ok(());
        };
        let storage = create_state_storage(state_config)?;

        let state_id = self.get_state_id();
        if let Some(source_state) = storage.load_source_state(&state_id).await? {
            self.source_state_to_internal_state(source_state).await?;

            let (last_poll_timestamp, total_documents_published, poll_count) = {
                let state = self.state.lock().await;
                (
                    state.last_poll_timestamp,
                    state.total_documents_published,
                    state.poll_count,
                )
            };
            info!(
                "Loaded state for OpenSearch source connector with ID: {} - last poll: {:?}, total docs: {}, polls: {}",
                self.id, last_poll_timestamp, total_documents_published, poll_count
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

pub(crate) fn validate_state_storage_config(config: &StateConfig) -> Result<(), Error> {
    match config.storage_type.as_deref() {
        Some("file") | None => Ok(()),
        Some(storage_type) => Err(Error::InvalidConfigValue(format!(
            "state storage_type {storage_type:?} is not supported; only \"file\" is implemented"
        ))),
    }
}

pub(crate) fn create_state_storage(config: &StateConfig) -> Result<Arc<dyn StateStorage>, Error> {
    let base_path = config
        .storage_config
        .as_ref()
        .and_then(|c| c.get("base_path"))
        .and_then(|p| p.as_str())
        .unwrap_or("./connector_states");

    Ok(Arc::new(FileStateStorage::new(base_path)))
}

/// Optional file-backed mirror of connector state. Runtime `ConnectorState` msgpack is authoritative.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SourceState {
    pub id: String,
    pub last_updated: DateTime<Utc>,
    #[serde(default)]
    pub version: u32,
    pub data: serde_json::Value,
    pub metadata: Option<serde_json::Value>,
}

#[async_trait]
pub(crate) trait StateStorage: Send + Sync {
    async fn save_source_state(&self, state: &SourceState) -> Result<(), Error>;

    async fn load_source_state(&self, id: &str) -> Result<Option<SourceState>, Error>;
}

pub(crate) struct FileStateStorage {
    base_path: std::path::PathBuf,
}

impl FileStateStorage {
    pub(crate) fn new<P: AsRef<std::path::Path>>(base_path: P) -> Self {
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
        use tokio::fs::{self, OpenOptions};
        use tokio::io::AsyncWriteExt;

        fs::create_dir_all(&self.base_path)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create state directory: {e}")))?;

        let path = self.get_state_path(&state.id);
        // PID in the suffix avoids races when two processes write the same state_id.
        let tmp_path = path.with_extension(format!("json.{}.tmp", std::process::id()));
        let json = serde_json::to_string(state)
            .map_err(|e| Error::Serialization(format!("Failed to serialize source state: {e}")))?;

        let mut tmp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .await
            .map_err(|e| Error::Storage(format!("Failed to open state temp file: {e}")))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600); // Similar to core/connectors/runtime/src/state.rs open_options_for_state
            std::fs::set_permissions(&tmp_path, perms).map_err(|e| {
                Error::Storage(format!("Failed to set state temp file permissions: {e}"))
            })?;
        }

        if let Err(e) = tmp_file.write_all(json.as_bytes()).await {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(Error::Storage(format!(
                "Failed to write state temp file: {e}"
            )));
        }
        if let Err(e) = tmp_file.sync_all().await {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(Error::Storage(format!(
                "Failed to sync state temp file: {e}"
            )));
        }
        drop(tmp_file);

        if let Err(e) = fs::rename(&tmp_path, &path).await {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(Error::Storage(format!("Failed to rename state file: {e}")));
        }

        // Flush the parent directory entry so the rename is durable on crash.
        if let Some(parent) = path.parent() {
            let dir = tokio::fs::File::open(parent)
                .await
                .map_err(|e| Error::Storage(format!("Failed to open state directory: {e}")))?;
            dir.sync_all()
                .await
                .map_err(|e| Error::Storage(format!("Failed to sync state directory: {e}")))?;
        }

        Ok(())
    }

    async fn load_source_state(&self, id: &str) -> Result<Option<SourceState>, Error> {
        use tokio::fs;

        let path = self.get_state_path(id);
        let content = match fs::read_to_string(&path).await {
            Ok(content) => content,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(Error::Storage(format!(
                    "Failed to read state file: {error}"
                )));
            }
        };

        let state: SourceState = serde_json::from_str(&content).map_err(|e| {
            Error::Serialization(format!("Failed to deserialize source state: {e}"))
        })?;

        Ok(Some(state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::Utc;
    use serde_json::json;
    use tempfile::TempDir;

    fn file_state_config(base_path: &str) -> StateConfig {
        StateConfig {
            enabled: true,
            storage_type: Some("file".to_string()),
            storage_config: Some(json!({ "base_path": base_path })),
            state_id: Some("opensearch_unit_state".to_string()),
        }
    }

    #[test]
    fn given_unknown_storage_type_should_fail() {
        let config = StateConfig {
            enabled: true,
            storage_type: Some("s3".to_string()),
            storage_config: None,
            state_id: None,
        };
        let error = validate_state_storage_config(&config);
        assert!(matches!(error, Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_file_storage_should_save_and_load_source_state() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let temp_dir = TempDir::new().expect("tempdir");
            let config = file_state_config(&temp_dir.path().to_string_lossy());
            let storage = create_state_storage(&config).expect("file storage");

            let source_state = SourceState {
                id: "opensearch_unit_state".to_string(),
                last_updated: Utc::now(),
                version: SOURCE_STATE_VERSION,
                data: json!({
                    "total_documents_published": 7,
                    "poll_count": 2,
                    "search_after": ["2024-01-01T00:00:00Z", "doc-7"]
                }),
                metadata: None,
            };

            storage
                .save_source_state(&source_state)
                .await
                .expect("save state");
            let loaded = storage
                .load_source_state("opensearch_unit_state")
                .await
                .expect("load state")
                .expect("state file should exist");
            assert_eq!(loaded.data["total_documents_published"], 7);
            assert_eq!(loaded.data["poll_count"], 2);
        });
    }

    #[test]
    fn given_missing_state_file_when_load_should_return_none() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let temp_dir = TempDir::new().expect("tempdir");
            let config = file_state_config(&temp_dir.path().to_string_lossy());
            let storage = create_state_storage(&config).expect("file storage");
            let loaded = storage
                .load_source_state("missing_state_id")
                .await
                .expect("load should not error");
            assert!(loaded.is_none());
        });
    }

    #[test]
    fn given_default_storage_type_should_use_file_backend() {
        let temp_dir = TempDir::new().expect("tempdir");
        let config = StateConfig {
            enabled: true,
            storage_type: None,
            storage_config: Some(
                json!({ "base_path": temp_dir.path().to_string_lossy().as_ref() }),
            ),
            state_id: None,
        };
        let storage = create_state_storage(&config).expect("default file storage");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let source_state = SourceState {
                id: "opensearch_default".to_string(),
                last_updated: Utc::now(),
                version: SOURCE_STATE_VERSION,
                data: json!({ "poll_count": 1 }),
                metadata: None,
            };
            storage
                .save_source_state(&source_state)
                .await
                .expect("save");
        });
    }
}
